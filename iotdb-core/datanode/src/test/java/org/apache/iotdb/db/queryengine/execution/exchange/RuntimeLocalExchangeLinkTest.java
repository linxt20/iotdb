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

import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.exchange.RuntimeLocalExchangeLink.Edge;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue.MemoryReservationKey;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeEndpointFrontier;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.Assert.*;

public class RuntimeLocalExchangeLinkTest {
  private static final RuntimeExchangeBlockCodec.Bounds BOUNDS =
      new RuntimeExchangeBlockCodec.Bounds(
          16, 1, 4096, 4096, 4096, 8192, 8192, List.of(TSDataType.INT64));

  private static FragmentInstanceId fi(String query, int fragment) {
    return FragmentInstanceId.fromThrift(new TFragmentInstanceId(query, fragment, "0"));
  }

  private static TsBlock block(long value) {
    return new TsBlock(
        1,
        new TimeColumn(1, new long[] {7}),
        new Column[] {new LongColumn(1, Optional.empty(), new long[] {value})});
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static final class Fixture implements AutoCloseable {
    final FragmentInstanceId producer = fi("local_query", 1);
    final FragmentInstanceId consumer = fi("local_query", 2);
    final MemoryPool pool = new MemoryPool("runtime-local", new MemoryManager(1L << 22), 1L << 21);
    final RuntimeFrontierReservation out =
        create(producer, MemorySide.OUTPUT, "out", BOUNDS.outputEnvelopeBytes());
    final RuntimeFrontierReservation in =
        create(consumer, MemorySide.INPUT, "in", BOUNDS.inputEnvelopeBytes());
    final RuntimeEndpointFrontier output = new RuntimeEndpointFrontier(out, 0, BOUNDS);
    final RuntimeEndpointFrontier input = new RuntimeEndpointFrontier(in, 0, BOUNDS);
    final RuntimeLocalExchangeLink link =
        new RuntimeLocalExchangeLink(new Edge(producer, 0, consumer, "exchange"), pool);

    RuntimeFrontierReservation create(
        FragmentInstanceId id, MemorySide side, String name, long bytes) {
      return RuntimeFrontierReservation.tryCreate(
              pool,
              new MemoryReservationKey(id.getQueryId().getId(), id.getFragmentInstanceId(), name),
              side,
              1,
              bytes,
              1L << 21)
          .get();
    }

    void bind() {
      link.bindOutput(output);
      link.bindInput(input);
    }

    @Override
    public void close() throws Exception {
      link.requestClose(null).get(5, TimeUnit.SECONDS);
      out.requestClose().get(5, TimeUnit.SECONDS);
      in.requestClose().get(5, TimeUnit.SECONDS);
      assertEquals(0, pool.getReservedBytes());
      pool.deRegisterFragmentInstanceFromQueryMemoryMap(
          "local_query", producer.getFragmentInstanceId(), false);
      pool.deRegisterFragmentInstanceFromQueryMemoryMap(
          "local_query", consumer.getFragmentInstanceId(), false);
      assertEquals(0, pool.getQueryMemoryReservationSize());
    }
  }

  @Test
  public void eitherEndpointMayBindFirstWithoutGrantingEarlyWrite() throws Exception {
    for (boolean reverse : new boolean[] {false, true}) {
      try (Fixture f = new Fixture()) {
        ListenableFuture<Void> waiting = f.link.whenWritable();
        assertFalse(waiting.isDone());
        if (reverse) f.link.bindInput(f.input);
        else f.link.bindOutput(f.output);
        assertFalse(waiting.isDone());
        assertThrows(IllegalStateException.class, () -> f.link.send(block(1)));
        if (reverse) f.link.bindOutput(f.output);
        else f.link.bindInput(f.input);
        waiting.get(5, TimeUnit.SECONDS);
        assertEquals(0, f.out.getLeasedCreditCount());
        assertEquals(0, f.in.getLeasedCreditCount());
      }
    }
  }

  @Test
  public void oneSlotPreservesFifoAndIndependentPrepaidAccounts() throws Exception {
    try (Fixture f = new Fixture()) {
      f.bind();
      long reserved = f.pool.getReservedBytes();
      for (int i = 0; i < 8; i++) {
        final long value = i;
        assertFalse(f.link.whenReadable().isDone());
        f.link.send(block(value));
        assertTrue(f.link.whenReadable().isDone());
        assertFalse(f.link.whenWritable().isDone());
        assertEquals(1, f.out.getLeasedCreditCount());
        assertEquals(0, f.in.getLeasedCreditCount());
        assertThrows(IllegalStateException.class, () -> f.link.send(block(99)));
        TsBlock result =
            f.link.consume(
                b -> {
                  assertFalse(Thread.holdsLock(f.link));
                  assertEquals(1, f.in.getLeasedCreditCount());
                  assertEquals(1, f.out.getLeasedCreditCount());
                  assertEquals(1, f.link.getActiveConsumerCount());
                  assertEquals(value, b.getColumn(0).getLong(0));
                  return b;
                });
        assertEquals(value, result.getColumn(0).getLong(0));
        assertEquals(0, f.out.getLeasedCreditCount());
        assertEquals(0, f.in.getLeasedCreditCount());
        assertEquals(reserved, f.pool.getReservedBytes());
        assertTrue(f.link.whenWritable().isDone());
      }
      f.link.setNoMore();
      assertTrue(f.link.isFinished());
      assertNull(f.link.consume(Function.identity()));
      f.link.send(block(10));
      assertEquals(0, f.link.getBufferedRetainedBytes());
    }
  }

  @Test
  public void outputHasIndependentBackingBeforePublisherMayReuseInput() throws Exception {
    try (Fixture f = new Fixture()) {
      f.bind();
      long[] values = new long[] {42};
      TsBlock original =
          new TsBlock(
              1,
              new TimeColumn(1, new long[] {7}),
              new Column[] {new LongColumn(1, Optional.empty(), values)});
      f.link.send(original);
      values[0] = -1;
      TsBlock result = f.link.consume(Function.identity());
      assertNotSame(original, result);
      assertEquals(42, result.getColumn(0).getLong(0));
    }
  }

  @Test
  public void eosWaitsForLastActiveConsumerRatherThanOnlyEmptyQueue() throws Exception {
    try (Fixture f = new Fixture()) {
      f.bind();
      f.link.send(block(3));
      f.link.setNoMore();
      assertFalse(f.link.isFinished());
      f.link.consume(
          b -> {
            assertFalse(f.link.isFinished());
            return b;
          });
      assertTrue(f.link.isFinished());
      assertTrue(f.link.whenReadable().isDone());
    }
  }

  @Test
  public void cancelBufferedBlockSettlesOutputAndRejectsLateBind() throws Exception {
    try (Fixture f = new Fixture()) {
      f.bind();
      f.link.send(block(3));
      RuntimeException expected = new RuntimeException("original cancellation");
      f.link.requestClose(expected).get(5, TimeUnit.SECONDS);
      assertEquals(0, f.out.getLeasedCreditCount());
      IllegalStateException error =
          assertThrows(IllegalStateException.class, () -> f.link.consume(Function.identity()));
      assertSame(expected, error.getCause());
      assertSame(
          expected,
          assertThrows(IllegalStateException.class, () -> f.link.bindInput(f.input)).getCause());
      f.link.requestClose(new RuntimeException("later")).get(5, TimeUnit.SECONDS);
      assertSame(
          expected,
          assertThrows(IllegalStateException.class, () -> f.link.send(block(4))).getCause());
    }
  }

  @Test
  public void cancelledUnboundLinkCannotBeResurrectedByConstructionOrder() throws Exception {
    try (Fixture f = new Fixture()) {
      f.link.requestClose(null).get(5, TimeUnit.SECONDS);
      assertThrows(IllegalStateException.class, () -> f.link.bindOutput(f.output));
      assertThrows(IllegalStateException.class, () -> f.link.bindInput(f.input));
    }
  }

  @Test
  public void cancellationWaitsForSerializerAndBothPhysicalReservations() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    try (Fixture f = new Fixture()) {
      try {
        f.bind();
        f.link.send(block(42));
        Future<ByteBuffer> serialized =
            executor.submit(
                () ->
                    f.link.consume(
                        b -> {
                          entered.countDown();
                          await(release);
                          try {
                            return new TsBlockSerde().serialize(b);
                          } catch (Exception e) {
                            throw new AssertionError(e);
                          }
                        }));
        await(entered);
        ListenableFuture<Void> close =
            f.link.requestClose(new RuntimeException("cancel during serialization"));
        ListenableFuture<Void> inputClose = f.in.requestClose(), outputClose = f.out.requestClose();
        assertFalse(close.isDone());
        assertFalse(inputClose.isDone());
        assertFalse(outputClose.isDone());
        assertEquals(1, f.in.getLeasedCreditCount());
        assertEquals(1, f.out.getLeasedCreditCount());
        release.countDown();
        TsBlock decoded = new TsBlockSerde().deserialize(serialized.get(5, TimeUnit.SECONDS));
        assertEquals(42, decoded.getColumn(0).getLong(0));
        close.get(5, TimeUnit.SECONDS);
        inputClose.get(5, TimeUnit.SECONDS);
        outputClose.get(5, TimeUnit.SECONDS);
        assertEquals(0, f.pool.getReservedBytes());
      } finally {
        release.countDown();
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void originalConsumerFailurePropagatesAndCreditsAreReturned() throws Exception {
    try (Fixture f = new Fixture()) {
      f.bind();
      f.link.send(block(4));
      RuntimeException expected = new RuntimeException("cast failure");
      assertSame(
          expected,
          assertThrows(
              RuntimeException.class,
              () ->
                  f.link.consume(
                      b -> {
                        throw expected;
                      })));
      f.link.getClosedFuture().get(5, TimeUnit.SECONDS);
      assertSame(
          expected,
          assertThrows(IllegalStateException.class, () -> f.link.send(block(5))).getCause());
      assertEquals(0, f.in.getLeasedCreditCount());
      assertEquals(0, f.out.getLeasedCreditCount());
    }
  }

  @Test
  public void malformedBlockNeverEscapesOrLeavesOutstandingCredit() throws Exception {
    try (Fixture f = new Fixture()) {
      f.bind();
      TsBlock wrong = new TsBlock(1, new TimeColumn(1, new long[] {0}), new Column[0]);
      assertThrows(RuntimeException.class, () -> f.link.send(wrong));
      assertEquals(0, f.out.getLeasedCreditCount());
      assertEquals(0, f.link.getBufferedRetainedBytes());
      assertTrue(f.link.isCloseRequested());
      f.link.getClosedFuture().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void frontierBindingRejectsWrongFiPoolSideAndDuplicateOwner() throws Exception {
    try (Fixture f = new Fixture()) {
      assertThrows(IllegalArgumentException.class, () -> f.link.bindInput(f.output));
      assertThrows(IllegalArgumentException.class, () -> f.link.bindOutput(f.input));
      RuntimeLocalExchangeLink wrongFi =
          new RuntimeLocalExchangeLink(
              new Edge(fi("local_query", 3), 0, f.consumer, "exchange"), f.pool);
      assertThrows(IllegalArgumentException.class, () -> wrongFi.bindOutput(f.output));
      RuntimeLocalExchangeLink wrongPool =
          new RuntimeLocalExchangeLink(
              f.link.getEdge(), new MemoryPool("other", new MemoryManager(1L << 22), 1L << 21));
      assertThrows(IllegalArgumentException.class, () -> wrongPool.bindOutput(f.output));
      f.bind();
      assertThrows(IllegalStateException.class, () -> f.link.bindOutput(f.output));
      assertThrows(IllegalStateException.class, () -> f.link.bindInput(f.input));
      assertThrows(IllegalStateException.class, () -> f.out.acquireEnvelope(0, new Object()));
      RuntimeLocalExchangeLink second = new RuntimeLocalExchangeLink(f.link.getEdge(), f.pool);
      assertThrows(IllegalStateException.class, () -> second.bindOutput(f.output));
      second.requestClose(null).get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void boundsMismatchDoesNotConsumeTheUnboundInputSlot() throws Exception {
    try (Fixture f = new Fixture()) {
      f.link.bindOutput(f.output);
      RuntimeExchangeBlockCodec.Bounds different =
          new RuntimeExchangeBlockCodec.Bounds(
              8, 1, 4096, 4096, 4096, 8192, 8192, List.of(TSDataType.INT64));
      assertThrows(
          IllegalArgumentException.class,
          () -> f.link.bindInput(new RuntimeEndpointFrontier(f.in, 0, different)));
      f.link.bindInput(f.input);
      f.link.send(block(7));
      assertEquals(7, f.link.consume(Function.identity()).getColumn(0).getLong(0));
    }
  }

  @Test
  public void edgeUsesFullQueryFiChannelAndConsumerIdentity() {
    Edge base = new Edge(fi("q", 1), 0, fi("q", 2), "n");
    assertNotEquals(base, new Edge(fi("q", 1), 1, fi("q", 2), "n"));
    assertNotEquals(base, new Edge(fi("other", 1), 0, fi("other", 2), "n"));
    assertNotEquals(base, new Edge(fi("q", 1), 0, fi("q", 3), "n"));
    assertNotEquals(base, new Edge(fi("q", 1), 0, fi("q", 2), "other"));
    assertThrows(IllegalArgumentException.class, () -> new Edge(fi("q", 1), -1, fi("q", 2), "n"));
    assertThrows(
        IllegalArgumentException.class, () -> new Edge(fi("q", 1), 0, fi("other", 2), "n"));
  }

  @Test
  public void futureCallbacksRunOutsideMonitorAndMayCloseReentrantly() throws Exception {
    try (Fixture f = new Fixture()) {
      f.bind();
      AtomicBoolean underLock = new AtomicBoolean();
      AtomicInteger callbacks = new AtomicInteger();
      f.link
          .whenReadable()
          .addListener(
              () -> {
                underLock.set(Thread.holdsLock(f.link));
                callbacks.incrementAndGet();
                f.link.requestClose(null);
              },
              MoreExecutors.directExecutor());
      f.link.send(block(4));
      assertFalse(underLock.get());
      assertEquals(1, callbacks.get());
      f.link.getClosedFuture().get(5, TimeUnit.SECONDS);
      assertEquals(0, f.out.getLeasedCreditCount());
    }
  }

  @Test
  public void concurrentSendAndCloseNeverReleaseLiveCreditTwice() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      for (int n = 0; n < 40; n++) {
        try (Fixture f = new Fixture()) {
          f.bind();
          CyclicBarrier barrier = new CyclicBarrier(2);
          Future<?> sender =
              executor.submit(
                  () -> {
                    try {
                      barrier.await(5, TimeUnit.SECONDS);
                    } catch (Exception e) {
                      throw new AssertionError(e);
                    }
                    try {
                      f.link.send(block(4));
                    } catch (IllegalStateException e) {
                      assertTrue(f.link.isCloseRequested());
                    }
                  });
          Future<?> closer =
              executor.submit(
                  () -> {
                    try {
                      barrier.await(5, TimeUnit.SECONDS);
                    } catch (Exception e) {
                      throw new AssertionError(e);
                    }
                    f.link.requestClose(null);
                  });
          sender.get(5, TimeUnit.SECONDS);
          closer.get(5, TimeUnit.SECONDS);
          f.link.getClosedFuture().get(5, TimeUnit.SECONDS);
          assertEquals(0, f.out.getLeasedCreditCount());
          assertEquals(0, f.in.getLeasedCreditCount());
          assertEquals(0, f.link.getBufferedRetainedBytes());
        }
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void insufficientScratchRemainsRejectedBeforePublication() throws Exception {
    try (Fixture f = new Fixture()) {
      RuntimeExchangeBlockCodec.Bounds tooSmall =
          new RuntimeExchangeBlockCodec.Bounds(
              16, 1, 4096, 4096, 4096, 8192, 4096, List.of(TSDataType.INT64));
      f.link.bindOutput(new RuntimeEndpointFrontier(f.out, 0, tooSmall));
      f.link.bindInput(new RuntimeEndpointFrontier(f.in, 0, tooSmall));
      assertThrows(IllegalArgumentException.class, () -> f.link.send(block(1)));
      assertEquals(0, f.out.getLeasedCreditCount());
      assertEquals(0, f.in.getLeasedCreditCount());
      assertEquals(0, f.link.getBufferedRetainedBytes());
    }
  }

  @Test
  public void cancellingNotificationDoesNotCancelUnderlyingLink() throws Exception {
    try (Fixture f = new Fixture()) {
      assertTrue(f.link.whenWritable().cancel(true));
      f.bind();
      assertTrue(f.link.whenWritable().isDone());
      assertTrue(f.link.whenReadable().cancel(true));
      assertThrows(IllegalStateException.class, () -> f.link.consume(Function.identity()));
      f.link.send(block(9));
      assertEquals(9, f.link.consume(Function.identity()).getColumn(0).getLong(0));
      ListenableFuture<Void> receipt = f.link.getClosedFuture();
      assertTrue(receipt.cancel(true));
      f.link.requestClose(null).get(5, TimeUnit.SECONDS);
    }
  }
}
