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

import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeEndpointFrontier;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/** Per-endpoint completion is distinct from the single physical link and its two accounts. */
public abstract class RuntimeLocalEndpoint implements RuntimeExchangeEndpoint {
  protected final RuntimeLocalExchangeLink link;
  private final RuntimeEndpointFrontier frontier;
  private final FragmentInstanceId fragment;
  private final String planNode;
  private final SettableFuture<Void> closure = SettableFuture.create();
  protected final AtomicBoolean terminalNotified = new AtomicBoolean();

  protected RuntimeLocalEndpoint(
      RuntimeLocalExchangeLink link,
      RuntimeEndpointFrontier frontier,
      FragmentInstanceId fragment,
      String planNode) {
    this.link = Objects.requireNonNull(link);
    this.frontier = Objects.requireNonNull(frontier);
    this.fragment = Objects.requireNonNull(fragment);
    this.planNode = Objects.requireNonNull(planNode);
  }

  // Subclasses call only after their listeners are initialized and their frontier is bound.
  protected final void observeClosure() {
    link.getClosedFuture()
        .addListener(
            () -> {
              Throwable failure = null;
              try {
                Futures.getDone(link.getClosedFuture());
              } catch (ExecutionException e) {
                failure = e.getCause() == null ? e : e.getCause();
              } catch (Throwable e) {
                failure = e;
              }
              try {
                notifyTerminated(link.getFailure());
              } catch (Throwable e) {
                if (failure == null) failure = e;
                else if (failure != e) failure.addSuppressed(e);
              }
              if (failure == null) closure.set(null);
              else closure.setException(failure);
            },
            MoreExecutors.directExecutor());
  }

  protected abstract void notifyTerminated(Throwable failure);

  public RuntimeLocalExchangeLink getLink() {
    return link;
  }

  @Override
  public RuntimeEndpointFrontier getRuntimeFrontier() {
    return frontier;
  }

  @Override
  public TFragmentInstanceId getLocalFragmentInstanceId() {
    return fragment.toThrift();
  }

  @Override
  public String getLocalPlanNodeId() {
    return planNode;
  }

  @Override
  public SharedTsBlockQueue.MemoryReservationKey getActualAccountKey() {
    return frontier.reservation().getMemoryReservationKey();
  }

  @Override
  public ListenableFuture<Void> requestCoordinatedClose() {
    link.requestClose(null);
    return Futures.nonCancellationPropagating(closure);
  }

  @Override
  public ListenableFuture<Void> requestCoordinatedAbort(Throwable cause) {
    link.requestClose(cause == null ? new CancellationException() : cause);
    return Futures.nonCancellationPropagating(closure);
  }

  @Override
  public ListenableFuture<Void> getCoordinatedCloseFuture() {
    return link.isCloseRequested() ? Futures.nonCancellationPropagating(closure) : null;
  }

  public boolean isClosed() {
    return closure.isDone() && link.getFailure() == null;
  }

  public boolean isAborted() {
    return link.isCloseRequested() && link.getFailure() != null;
  }

  public long getBufferRetainedSizeInBytes() {
    return frontier.reservation().getMemorySide()
            == org.apache.iotdb.db.queryengine.execution.memory.MemorySide.OUTPUT
        ? link.getBufferedRetainedBytes()
        : 0L;
  }

  @Override
  public int getRuntimeInFlightTaskCount() {
    return link.getCopyingCount();
  }

  @Override
  public int getRuntimeConsumerCount() {
    return link.getActiveConsumerCount();
  }

  @Override
  public int getRuntimeResponsePinCount() {
    return 0;
  }

  @Override
  public int getRuntimePendingCreditReturnCount() {
    return link.getReturningCount();
  }

  @Override
  public Long getRuntimeBufferedPayloadBytes() {
    return frontier.reservation().getMemorySide()
            == org.apache.iotdb.db.queryengine.execution.memory.MemorySide.OUTPUT
        ? link.getBufferedLogicalBytes()
        : 0L;
  }

  public void setMaxBytesCanReserve(long ignored) {
    // Both FI-side totals were admitted before endpoint creation. Publishing a quota never
    // reserves again and cannot enlarge the bound of the original reservation.
  }

  protected final void checkFailure() {
    Throwable cause = link.getFailure();
    if (cause != null) throw new IllegalStateException(cause);
  }

  @Override
  public String getTransportMode() {
    return "LOCAL_TYPED";
  }
}
