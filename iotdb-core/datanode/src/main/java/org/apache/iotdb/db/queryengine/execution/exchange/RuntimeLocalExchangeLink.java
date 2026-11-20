/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.exchange;

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.db.queryengine.execution.operator.process.RuntimeCompactOutputOperator;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeEndpointFrontier;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation.BlockCredit;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.Objects;
import java.util.function.Function;

import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;

/**
 * One in-process edge with independent, already admitted OUTPUT and INPUT owners.
 *
 * <p>This class never reserves pool bytes, closes either reservation, or runs a consumer/future
 * callback under its monitor. The single slot stays occupied through compaction, consumption and
 * both credit returns. Cancellation seals the edge immediately but its receipt waits for actual
 * owners to exit. Returned typed blocks follow ExchangeOperator's existing decoded-return budget.
 * Final-result consumers with a legacy INPUT account need a separate adapter; they cannot bind an
 * OUTPUT frontier as INPUT here.
 */
public final class RuntimeLocalExchangeLink {
  public record Edge(
      FragmentInstanceId producer,
      int channel,
      FragmentInstanceId consumer,
      String consumerPlanNode) {
    public Edge {
      Objects.requireNonNull(producer);
      Objects.requireNonNull(consumer);
      Objects.requireNonNull(consumerPlanNode);
      if (channel < 0
          || consumerPlanNode.isBlank()
          || !producer.getQueryId().equals(consumer.getQueryId())) {
        throw new IllegalArgumentException(
            DataNodeQueryMessages
                .EXCEPTION_RUNTIME_LOCAL_EXCHANGE_EDGE_HAS_INCONSISTENT_IDENTITIES_AAD9C714);
      }
    }
  }

  private enum Phase {
    COPYING,
    BUFFERED,
    CONSUMING,
    RETURNING
  }

  private static final class Slot {
    private Phase phase = Phase.COPYING;
    private TsBlock block;
    private BlockCredit outputCredit;
    private BlockCredit inputCredit;
  }

  private final Edge edge;
  private final MemoryPool pool;
  private final Object outputOwner = new Object();
  private final Object inputOwner = new Object();
  private final SettableFuture<Void> closed = SettableFuture.create();
  private final SettableFuture<Void> drained = SettableFuture.create();
  private SettableFuture<Void> writable = SettableFuture.create();
  private SettableFuture<Void> readable = SettableFuture.create();
  private RuntimeEndpointFrontier output;
  private RuntimeEndpointFrontier input;
  private Slot slot;
  private boolean noMore;
  private boolean closeRequested;
  private Throwable failure;
  private Throwable settlementFailure;

  public RuntimeLocalExchangeLink(Edge edge, MemoryPool pool) {
    this.edge = Objects.requireNonNull(edge);
    this.pool = Objects.requireNonNull(pool);
  }

  public Edge getEdge() {
    return edge;
  }

  public void bindOutput(RuntimeEndpointFrontier frontier) {
    bind(frontier, true);
  }

  public void bindInput(RuntimeEndpointFrontier frontier) {
    bind(frontier, false);
  }

  private void bind(RuntimeEndpointFrontier frontier, boolean producer) {
    SettableFuture<Void> wake;
    synchronized (this) {
      checkOpen();
      Objects.requireNonNull(frontier);
      if ((producer ? output : input) != null) {
        throw new IllegalStateException(
            DataNodeQueryMessages
                .EXCEPTION_RUNTIME_LOCAL_EXCHANGE_ENDPOINT_IS_ALREADY_BOUND_B8068663);
      }
      RuntimeEndpointFrontier other = producer ? input : output;
      if (other != null && !other.bounds().equals(frontier.bounds())) {
        throw new IllegalArgumentException(
            DataNodeQueryMessages
                .EXCEPTION_RUNTIME_LOCAL_EXCHANGE_ENDPOINTS_REQUIRE_IDENTICAL_ADMITTED_BOUNDS_29FA3C48);
      }
      frontier.bind(
          pool,
          (producer ? edge.producer() : edge.consumer()).toThrift(),
          producer ? MemorySide.OUTPUT : MemorySide.INPUT,
          producer ? outputOwner : inputOwner);
      if (producer) output = frontier;
      else input = frontier;
      wake = output != null && input != null && slot == null ? writable : null;
    }
    if (wake != null) wake.set(null);
  }

  public synchronized ListenableFuture<Void> whenWritable() {
    return nonCancellationPropagating(writable);
  }

  public synchronized ListenableFuture<Void> whenReadable() {
    return nonCancellationPropagating(readable);
  }

  public void send(TsBlock block) {
    Objects.requireNonNull(block);
    Slot current;
    synchronized (this) {
      checkOpen();
      if (noMore) return;
      if (output == null || input == null || slot != null) {
        throw new IllegalStateException(
            DataNodeQueryMessages.EXCEPTION_RUNTIME_LOCAL_EXCHANGE_IS_NOT_WRITABLE_E53C002E);
      }
      current = new Slot();
      current.outputCredit = output.reservation().acquireEnvelope(output.slot(), outputOwner);
      slot = current;
      writable = SettableFuture.create();
    }
    TsBlock compact = null;
    Throwable thrown = null;
    boolean publish = false;
    SettableFuture<Void> wake = null;
    try {
      compact =
          RuntimeCompactOutputOperator.copyAllRowsBounded(
              block,
              Math.multiplyExact(4L, output.bounds().maxRetainedBytes()),
              output.bounds().maxRetainedBytes(),
              output.bounds().schema());
      RuntimeExchangeBlockCodec.inspect(compact, output.bounds());
      synchronized (this) {
        if (!closeRequested) {
          current.block = compact;
          current.phase = Phase.BUFFERED;
          wake = readable;
          publish = true;
        } else current.phase = Phase.RETURNING;
      }
    } catch (RuntimeException | Error error) {
      thrown = error;
      synchronized (this) {
        current.phase = Phase.RETURNING;
        closeRequested = true;
        if (failure == null) failure = error;
      }
      throw error;
    } finally {
      compact = null;
      if (!publish) {
        try {
          settle(current);
        } catch (RuntimeException | Error cleanup) {
          if (thrown == null) throw cleanup;
          if (thrown != cleanup) thrown.addSuppressed(cleanup);
        }
      }
    }
    if (wake != null) wake.set(null);
  }

  /** Keep both owners alive until the typed consumer or serializer actually exits. */
  public <T> T consume(Function<TsBlock, T> consumer) {
    Objects.requireNonNull(consumer);
    Slot current;
    TsBlock block;
    synchronized (this) {
      checkOpen();
      current = slot;
      if (current == null && noMore) return null;
      if (current == null || current.phase != Phase.BUFFERED) {
        throw new IllegalStateException(
            DataNodeQueryMessages.EXCEPTION_RUNTIME_LOCAL_EXCHANGE_IS_NOT_READABLE_6AF0FD07);
      }
      current.inputCredit = input.reservation().acquireEnvelope(input.slot(), inputOwner);
      current.phase = Phase.CONSUMING;
      block = current.block;
      current.block = null;
      readable = SettableFuture.create();
    }
    Throwable thrown = null;
    try {
      return consumer.apply(block);
    } catch (RuntimeException | Error error) {
      thrown = error;
      synchronized (this) {
        closeRequested = true;
        if (failure == null) failure = error;
      }
      throw error;
    } finally {
      block = null;
      synchronized (this) {
        current.phase = Phase.RETURNING;
      }
      try {
        settle(current);
      } catch (RuntimeException | Error cleanup) {
        if (thrown == null) throw cleanup;
        if (thrown != cleanup) thrown.addSuppressed(cleanup);
      }
    }
  }

  public void setNoMore() {
    SettableFuture<Void> wake;
    synchronized (this) {
      if (closeRequested || noMore) return;
      noMore = true;
      wake = slot == null || slot.phase == Phase.BUFFERED ? readable : null;
    }
    if (wake != null) wake.set(null);
  }

  /** Idempotent tombstone. A close during copy/consume must not free its live backing. */
  public ListenableFuture<Void> requestClose(Throwable cause) {
    Slot discard = null;
    SettableFuture<Void> writer;
    SettableFuture<Void> reader;
    boolean done;
    synchronized (this) {
      if (closeRequested) return nonCancellationPropagating(closed);
      closeRequested = true;
      failure = cause;
      if (slot != null && slot.phase == Phase.BUFFERED) {
        discard = slot;
        discard.block = null;
        discard.phase = Phase.RETURNING;
      }
      writer = writable;
      reader = readable;
      done = slot == null;
    }
    // Complete every notification even if a listener reenters or fails.
    try {
      signal(writer, cause);
    } finally {
      try {
        signal(reader, cause);
      } finally {
        if (discard != null) settle(discard);
        else if (done) completeClose();
      }
    }
    return nonCancellationPropagating(closed);
  }

  private static void signal(SettableFuture<Void> future, Throwable cause) {
    if (cause == null) future.set(null);
    else future.setException(cause);
  }

  private void settle(Slot current) {
    Throwable error = null;
    try {
      if (current.inputCredit != null) input.reservation().release(current.inputCredit);
    } catch (Throwable t) {
      error = t;
    }
    try {
      output.reservation().release(current.outputCredit);
    } catch (Throwable t) {
      if (error == null) error = t;
      else if (error != t) error.addSuppressed(t);
    }
    current.inputCredit = null;
    current.outputCredit = null;
    current.block = null;
    SettableFuture<Void> writer;
    SettableFuture<Void> reader;
    boolean terminal;
    synchronized (this) {
      slot = null;
      if (error != null) {
        settlementFailure = error;
        closeRequested = true;
        if (failure == null) failure = error;
      }
      terminal = closeRequested;
      writer = writable;
      reader = terminal || noMore ? readable : null;
    }
    try {
      signal(writer, terminal ? failure : null);
    } finally {
      try {
        if (reader != null) signal(reader, terminal ? failure : null);
      } finally {
        if (terminal) completeClose();
      }
    }
    if (error instanceof Error) throw (Error) error;
    if (error instanceof RuntimeException) throw (RuntimeException) error;
    if (error != null)
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_RUNTIME_LOCAL_EXCHANGE_CREDIT_SETTLEMENT_FAILED_AFA14B18,
          error);
  }

  private void completeClose() {
    Throwable error;
    synchronized (this) {
      error = settlementFailure;
    }
    if (error == null) closed.set(null);
    else closed.setException(error);
  }

  private void checkOpen() {
    if (closeRequested) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_RUNTIME_LOCAL_EXCHANGE_IS_CLOSED_7FBCB963, failure);
    }
  }

  public synchronized boolean isFinished() {
    return noMore && slot == null;
  }

  public synchronized boolean isCloseRequested() {
    return closeRequested;
  }

  public synchronized long getBufferedRetainedBytes() {
    return slot == null || slot.block == null ? 0L : slot.block.getRetainedSizeInBytes();
  }

  public synchronized int getActiveConsumerCount() {
    return slot != null && slot.phase == Phase.CONSUMING ? 1 : 0;
  }

  public ListenableFuture<Void> getClosedFuture() {
    return nonCancellationPropagating(closed);
  }

  public void acknowledgeFinished() {
    boolean finished;
    synchronized (this) {
      finished = noMore && slot == null && !closeRequested;
    }
    if (finished) drained.set(null);
  }

  public ListenableFuture<Void> getDrainedFuture() {
    return nonCancellationPropagating(drained);
  }

  public synchronized Throwable getFailure() {
    return failure;
  }

  public synchronized boolean isFullyBound() {
    return output != null && input != null;
  }

  public synchronized int getCopyingCount() {
    return slot != null && slot.phase == Phase.COPYING ? 1 : 0;
  }

  public synchronized int getReturningCount() {
    return slot != null && slot.phase == Phase.RETURNING ? 1 : 0;
  }

  public synchronized int getBufferedBlockCount() {
    return slot != null && slot.phase == Phase.BUFFERED ? 1 : 0;
  }

  public synchronized long getBufferedLogicalBytes() {
    return slot == null || slot.block == null ? 0L : slot.block.getSizeInBytes();
  }
}
