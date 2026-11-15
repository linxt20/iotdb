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
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue.MemoryReservationKey;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool.MemoryReservationResult;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

public class SharedTsBlockQueueTest {

  /**
   * Test that when add() goes into the async listener path (memory blocked) and the queue is
   * aborted before the listener fires, the listener does NOT add the TsBlock to the closed queue.
   * This reproduces the race condition that caused NPE in MemoryPool.free().
   */
  @Test
  public void testAsyncListenerAfterAbortDoesNotAddTsBlock() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L;
    final TFragmentInstanceId fragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String planNodeId = "test";

    // Use a SettableFuture to manually control when the blocked-on-memory future
    // completes.
    SettableFuture<Void> manualFuture = SettableFuture.create();

    // Create a mock MemoryPool that returns the manually-controlled future
    // (simulating blocked).
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Mockito.mock(MemoryPool.class);
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);

    // reserveWithPriority() returns blocked future and reserve failure.
    Mockito.when(
            mockMemoryPool.reserveWithPriority(
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.anyLong(),
                Mockito.anyLong(),
                Mockito.anyBoolean()))
        .thenReturn(new MemoryReservationResult(manualFuture, false, 1024L));
    // tryCancel returns 0 — simulating future already completed (can't cancel)
    Mockito.when(mockMemoryPool.tryCancel(Mockito.any())).thenReturn(0L);

    // Use a direct executor so that when we complete manualFuture, the listener
    // runs immediately.
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            fragmentInstanceId, planNodeId, mockLocalMemoryManager, newDirectExecutorService());
    queue.getCanAddTsBlock().set(null);
    queue.setMaxBytesCanReserve(Long.MAX_VALUE);

    TsBlock mockTsBlock = Utils.createMockTsBlock(mockTsBlockSize);

    // Step 1: add() goes into async path — listener is registered on manualFuture.
    // reserve() returns (manualFuture, false), so the TsBlock is NOT yet added to
    // the queue.
    ListenableFuture<Void> addFuture;
    synchronized (queue) {
      addFuture = queue.add(mockTsBlock);
    }
    // The addFuture (channelBlocked) should not be done yet
    Assert.assertFalse(addFuture.isDone());
    // Queue should be empty — TsBlock is waiting for memory
    Assert.assertTrue(queue.isEmpty());

    // Step 2: Abort the queue (simulates upstream FI state change listener calling
    // abort)
    synchronized (queue) {
      queue.abort();
    }
    Assert.assertTrue(queue.isClosed());

    // Step 3: Now complete the manualFuture — this triggers the async listener.
    // Before the fix, this would add the TsBlock to the closed queue.
    // After the fix, the listener detects closed==true and returns without adding.
    manualFuture.set(null);

    // Verify: queue should still be empty (TsBlock was NOT added to the closed
    // queue)
    Assert.assertTrue(queue.isEmpty());
    // The channelBlocked future should be completed (no hang)
    Assert.assertTrue(addFuture.isDone());
  }

  @Test
  public void concurrencyTest() throws Exception {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;

    // Construct a mock LocalMemoryManager with capacity 5 * mockTsBlockSize per
    // query.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryManager memoryManager = Mockito.spy(new MemoryManager(10 * mockTsBlockSize));
    MemoryPool spyMemoryPool =
        Mockito.spy(new MemoryPool("test", memoryManager, 5 * mockTsBlockSize));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            new TFragmentInstanceId(queryId, 0, "0"),
            "test",
            mockLocalMemoryManager,
            newDirectExecutorService());
    queue.getCanAddTsBlock().set(null);
    queue.setMaxBytesCanReserve(Long.MAX_VALUE);

    // Reuse an immutable block so mock generation is outside the concurrency deadline.
    TsBlock block = Utils.createMockTsBlock(mockTsBlockSize);
    int blockCount = 1000;
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CompletionService<Void> completedTasks = new ExecutorCompletionService<>(executor);
    try {
      completedTasks.submit(
          () -> {
            for (int i = 0; i < blockCount; i++) {
              ListenableFuture<Void> blocked;
              synchronized (queue) {
                blocked = queue.add(block);
              }
              blocked.get(10, TimeUnit.SECONDS);
            }
            synchronized (queue) {
              queue.setNoMoreTsBlocks(true);
            }
            return null;
          });
      completedTasks.submit(
          () -> {
            for (int i = 0; i < blockCount; i++) {
              ListenableFuture<Void> blocked;
              synchronized (queue) {
                blocked = queue.isBlocked();
              }
              blocked.get(10, TimeUnit.SECONDS);
              synchronized (queue) {
                Assert.assertSame(block, queue.remove());
              }
            }
            return null;
          });
      for (int i = 0; i < 2; i++) {
        Future<Void> completed = completedTasks.poll(10, TimeUnit.SECONDS);
        Assert.assertNotNull(completed);
        completed.get();
      }
      Assert.assertTrue(queue.hasNoMoreTsBlocks());
      Assert.assertTrue(queue.isEmpty());
      Assert.assertEquals(0, spyMemoryPool.getReservedBytes());
    } finally {
      executor.shutdownNow();
      try {
        Assert.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      } finally {
        synchronized (queue) {
          queue.close();
        }
      }
    }
  }

  @Test
  public void testDependentQueuesKeepIndependentBudgetsByDefault() {
    MemoryPool pool = new MemoryPool("pipeline-chain", new MemoryManager(1024), 512);
    LocalMemoryManager manager = Mockito.mock(LocalMemoryManager.class);
    Mockito.when(manager.getQueryPool()).thenReturn(pool);
    ExecutorService executor = newDirectExecutorService();
    TFragmentInstanceId id = new TFragmentInstanceId("pipeline-chain", 0, "0");
    SharedTsBlockQueue upstream = new SharedTsBlockQueue(id, "a", manager, executor);
    SharedTsBlockQueue downstream = new SharedTsBlockQueue(id, "b", manager, executor);
    upstream.setMaxBytesCanReserve(256);
    downstream.setMaxBytesCanReserve(256);
    TsBlock block = Utils.createMockTsBlock(64);
    try {
      for (int i = 0; i < 4; i++) Assert.assertTrue(upstream.add(block).isDone());
      ListenableFuture<Void> fifth = upstream.add(block);
      Assert.assertFalse(fifth.isDone());
      TsBlock forwarded = upstream.remove();
      Assert.assertTrue(fifth.isDone());
      Assert.assertTrue(downstream.add(forwarded).isDone());
      Assert.assertEquals(4, upstream.getNumOfBufferedTsBlocks());
      Assert.assertEquals(1, downstream.getNumOfBufferedTsBlocks());
      Assert.assertEquals(320, pool.getReservedBytes());
      // Both queues are producer-side OUTPUT buffers of the same fragment instance.
      String fullFragmentInstanceId =
          FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(id);
      Assert.assertEquals(
          320,
          pool.getFragmentMemoryReservedBytes(
              id.queryId, fullFragmentInstanceId, MemorySide.OUTPUT));
      Assert.assertEquals(
          0,
          pool.getFragmentMemoryReservedBytes(
              id.queryId, fullFragmentInstanceId, MemorySide.INPUT));
    } finally {
      downstream.close();
      upstream.close();
      executor.shutdownNow();
    }
    Assert.assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void testFrontierPrepaidAccountAndQueueIdentities() throws Exception {
    try (FrontierFixture fixture = new FrontierFixture(2, realBlock().getRetainedSizeInBytes())) {
      SharedTsBlockQueue first = fixture.queues[0];
      SharedTsBlockQueue second = fixture.queues[1];
      Assert.assertTrue(first.isFrontierMode());
      Assert.assertEquals(0, first.getFrontierLaneIndex());
      Assert.assertEquals(1, second.getFrontierLaneIndex());
      Assert.assertNotEquals(first.getQueueIdentityKey(), second.getQueueIdentityKey());
      Assert.assertEquals(first.getQueueIdentityKey(), first.getMemoryReservationKey());
      Assert.assertEquals(fixture.key, first.getActualAccountKey());
      Assert.assertEquals(first.getActualAccountKey(), second.getActualAccountKey());
      Assert.assertEquals(fixture.totalBytes, fixture.pool.getReservedBytes());
      Assert.assertEquals(3 * fixture.totalBytes, fixture.pool.getRemainingBytes());
      Assert.assertEquals(
          fixture.totalBytes,
          fixture.pool.getPlanNodeMemoryReservedBytes(
              fixture.key.queryId(), fixture.key.fragmentInstanceId(), fixture.key.planNodeId()));
      Assert.assertEquals(
          0,
          fixture.pool.getPlanNodeMemoryReservedBytes(
              fixture.key.queryId(), fixture.key.fragmentInstanceId(), "queue-0"));
      Assert.assertEquals(
          fixture.totalBytes,
          fixture.pool.getFragmentMemoryReservedBytes(
              fixture.key.queryId(), fixture.key.fragmentInstanceId(), MemorySide.OUTPUT));
      Assert.assertEquals(
          0,
          fixture.pool.getFragmentMemoryReservedBytes(
              fixture.key.queryId(), fixture.key.fragmentInstanceId(), MemorySide.INPUT));
      Mockito.verify(fixture.pool, Mockito.times(1))
          .tryReserve(
              Mockito.anyString(),
              Mockito.anyString(),
              Mockito.anyString(),
              Mockito.anyLong(),
              Mockito.anyLong());
      Assert.assertEquals(0, first.getBufferRetainedSizeInBytes());
      Assert.assertEquals(0, second.getBufferRetainedSizeInBytes());
    }
  }

  @Test
  public void testFrontierCreditUsesRetainedBytesAndReturnsInternally() throws Exception {
    TsBlock block = realBlock();
    try (FrontierFixture fixture = new FrontierFixture(1, block.getRetainedSizeInBytes())) {
      SharedTsBlockQueue queue = fixture.queues[0];
      ListenableFuture<Void> readable = queue.isBlocked();
      AtomicInteger notifications = new AtomicInteger();
      readable.addListener(
          () -> {
            Assert.assertFalse(Thread.holdsLock(queue));
            notifications.incrementAndGet();
          },
          Runnable::run);
      ListenableFuture<Void> writable = queue.add(block);
      writable.addListener(
          () -> {
            Assert.assertFalse(Thread.holdsLock(queue));
            notifications.incrementAndGet();
          },
          Runnable::run);
      Assert.assertTrue(readable.isDone());
      Assert.assertFalse(writable.isDone());
      Assert.assertEquals(1, queue.getNumOfBufferedTsBlocks());
      Assert.assertEquals(block.getRetainedSizeInBytes(), queue.getBufferRetainedSizeInBytes());
      Assert.assertEquals(
          block.getRetainedSizeInBytes(), fixture.reservation.getInternalUsedBytes());
      Assert.assertEquals(1, fixture.reservation.getLeasedCreditCount());
      Assert.assertThrows(IllegalStateException.class, () -> queue.add(block));
      Assert.assertEquals(1, fixture.reservation.getLeasedCreditCount());
      Assert.assertSame(block, queue.remove());
      writable.get(5, TimeUnit.SECONDS);
      Assert.assertEquals(2, notifications.get());
      Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
      Assert.assertEquals(0, fixture.reservation.getInternalUsedBytes());
      Assert.assertEquals(0, queue.getBufferRetainedSizeInBytes());
      Assert.assertEquals(fixture.totalBytes, fixture.pool.getReservedBytes());
      Assert.assertFalse(queue.isBlocked().isDone());
      Mockito.verify(fixture.pool, Mockito.never())
          .reserveWithPriority(
              Mockito.anyString(),
              Mockito.anyString(),
              Mockito.anyString(),
              Mockito.anyLong(),
              Mockito.anyLong(),
              Mockito.anyBoolean());
      Mockito.verify(fixture.pool, Mockito.never())
          .free(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyLong());
      Mockito.verify(fixture.pool, Mockito.never()).tryCancel(Mockito.any());
      Mockito.verify(fixture.pool, Mockito.times(1))
          .registerPlanNodeIdToQueryMemoryMap(
              fixture.key.queryId(),
              fixture.key.fragmentInstanceId(),
              fixture.key.planNodeId(),
              MemorySide.OUTPUT);
    }
  }

  @Test
  public void testFrontierRejectsRetainedSizeLargerThanLaneBound() throws Exception {
    TsBlock block = realBlock();
    Assert.assertTrue(block.getRetainedSizeInBytes() > block.getSizeInBytes());
    long bound = block.getRetainedSizeInBytes() - 1;
    try (FrontierFixture fixture = new FrontierFixture(2, bound)) {
      Assert.assertTrue(block.getRetainedSizeInBytes() < fixture.totalBytes);
      Assert.assertThrows(IllegalArgumentException.class, () -> fixture.queues[0].add(block));
      Assert.assertTrue(fixture.queues[0].isEmpty());
      Assert.assertEquals(0, fixture.reservation.getInternalUsedBytes());
      Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
      Assert.assertEquals(fixture.totalBytes, fixture.pool.getReservedBytes());
    }
  }

  @Test
  public void testFrontierLanesCannotBorrowEachOthersSingleCredit() throws Exception {
    TsBlock block = realBlock();
    try (FrontierFixture fixture = new FrontierFixture(2, block.getRetainedSizeInBytes())) {
      ListenableFuture<Void> first = fixture.queues[0].add(block);
      Assert.assertThrows(IllegalStateException.class, () -> fixture.queues[0].add(block));
      Assert.assertEquals(1, fixture.reservation.getLeasedCreditCount());
      ListenableFuture<Void> second = fixture.queues[1].add(block);
      Assert.assertFalse(first.isDone());
      Assert.assertFalse(second.isDone());
      Assert.assertEquals(2, fixture.reservation.getLeasedCreditCount());
      fixture.queues[0].remove();
      Assert.assertTrue(first.isDone());
      Assert.assertFalse(second.isDone());
      Assert.assertEquals(1, fixture.reservation.getLeasedCreditCount());
      Assert.assertEquals(
          block.getRetainedSizeInBytes(), fixture.reservation.getInternalUsedBytes());
    }
  }

  @Test
  public void testFrontierProducerCancellationDoesNotReturnCredit() throws Exception {
    TsBlock block = realBlock();
    try (FrontierFixture fixture = new FrontierFixture(1, block.getRetainedSizeInBytes())) {
      ListenableFuture<Void> writable = fixture.queues[0].add(block);
      Assert.assertTrue(writable.cancel(true));
      Assert.assertEquals(1, fixture.reservation.getLeasedCreditCount());
      Assert.assertFalse(fixture.reservation.whenWritable(0).isDone());
      Assert.assertThrows(IllegalStateException.class, () -> fixture.queues[0].add(block));
      fixture.queues[0].remove();
      Assert.assertTrue(fixture.reservation.whenWritable(0).isDone());
      Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
      Assert.assertEquals(fixture.totalBytes, fixture.pool.getReservedBytes());
    }
  }

  @Test
  public void testFrontierQueueCloseDoesNotCloseGroupReservation() throws Exception {
    TsBlock block = realBlock();
    try (FrontierFixture fixture = new FrontierFixture(2, block.getRetainedSizeInBytes())) {
      ListenableFuture<Void> first = fixture.queues[0].add(block);
      fixture.queues[0].close();
      fixture.queues[0].abort();
      fixture.queues[0].abort(new RuntimeException());
      fixture.queues[0].close();
      Assert.assertTrue(first.isDone());
      Assert.assertTrue(fixture.queues[0].isClosed());
      Assert.assertEquals(0, fixture.queues[0].getNumOfBufferedTsBlocks());
      Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
      Assert.assertFalse(fixture.reservation.isCloseRequested());
      Assert.assertEquals(fixture.totalBytes, fixture.pool.getReservedBytes());
      Assert.assertFalse(fixture.queues[1].add(block).isDone());
    }
  }

  @Test
  public void testFrontierGroupCloseWaitsForLastQueuedCredit() throws Exception {
    TsBlock block = realBlock();
    try (FrontierFixture fixture = new FrontierFixture(2, block.getRetainedSizeInBytes())) {
      fixture.queues[0].add(block);
      fixture.queues[1].add(block);
      ListenableFuture<Void> closed = fixture.reservation.requestClose();
      Assert.assertFalse(closed.isDone());
      fixture.queues[0].abort();
      Assert.assertFalse(closed.isDone());
      Assert.assertEquals(fixture.totalBytes, fixture.pool.getReservedBytes());
      fixture.queues[1].remove();
      closed.get(5, TimeUnit.SECONDS);
      Assert.assertEquals(0, fixture.pool.getReservedBytes());
      Assert.assertEquals(4 * fixture.totalBytes, fixture.pool.getRemainingBytes());
      fixture.queues[1].close();
      fixture.reservation.requestClose().get(5, TimeUnit.SECONDS);
      Mockito.verify(fixture.pool, Mockito.times(1))
          .free(
              fixture.key.queryId(),
              fixture.key.fragmentInstanceId(),
              fixture.key.planNodeId(),
              fixture.totalBytes);
    }
  }

  @Test
  public void testFrontierAbortCauseAndCancellationSettleEmptyReader() throws Exception {
    long bytes = realBlock().getRetainedSizeInBytes();
    RuntimeException cause = new RuntimeException("frontier abort");
    try (FrontierFixture fixture = new FrontierFixture(1, bytes)) {
      ListenableFuture<Void> reader = fixture.queues[0].isBlocked();
      fixture.queues[0].abort(cause);
      ExecutionException failure =
          Assert.assertThrows(ExecutionException.class, () -> reader.get(5, TimeUnit.SECONDS));
      Assert.assertSame(cause, failure.getCause());
      Assert.assertSame(cause, fixture.queues[0].getAbortedCause().get());
      IllegalStateException removeFailure =
          Assert.assertThrows(IllegalStateException.class, fixture.queues[0]::remove);
      Assert.assertSame(cause, removeFailure.getCause());
      Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
    }
    try (FrontierFixture fixture = new FrontierFixture(1, bytes)) {
      ListenableFuture<Void> reader = fixture.queues[0].isBlocked();
      fixture.queues[0].abort();
      Assert.assertTrue(reader.isCancelled());
      Assert.assertTrue(fixture.queues[0].getCanAddTsBlock().isDone());
    }
  }

  @Test
  public void testFrontierBindingRejectsForeignPoolFragmentAndOwner() throws Exception {
    long bytes = realBlock().getRetainedSizeInBytes();
    try (FrontierFixture fixture = new FrontierFixture(1, bytes)) {
      Assert.assertThrows(IllegalStateException.class, () -> fixture.reservation.acquire(0, bytes));
      Assert.assertThrows(
          IllegalStateException.class, () -> fixture.reservation.acquire(0, new Object(), bytes));
      Assert.assertThrows(
          IllegalStateException.class,
          () ->
              new SharedTsBlockQueue(
                  fixture.id,
                  "second-owner",
                  fixture.manager,
                  fixture.executor,
                  false,
                  fixture.reservation,
                  0));
      Assert.assertThrows(
          IllegalArgumentException.class,
          () ->
              new SharedTsBlockQueue(
                  fixture.id,
                  "bad-lane",
                  fixture.manager,
                  fixture.executor,
                  false,
                  fixture.reservation,
                  1));
      LocalMemoryManager otherManager = Mockito.mock(LocalMemoryManager.class);
      Mockito.when(otherManager.getQueryPool())
          .thenReturn(new MemoryPool("foreign", new MemoryManager(4 * bytes), 4 * bytes));
      Assert.assertThrows(
          IllegalArgumentException.class,
          () ->
              new SharedTsBlockQueue(
                  fixture.id,
                  "foreign-pool",
                  otherManager,
                  fixture.executor,
                  false,
                  fixture.reservation,
                  0));
      Assert.assertThrows(
          IllegalArgumentException.class,
          () ->
              new SharedTsBlockQueue(
                  new TFragmentInstanceId("foreign-query", 0, "0"),
                  "foreign-query",
                  fixture.manager,
                  fixture.executor,
                  false,
                  fixture.reservation,
                  0));
      Assert.assertThrows(
          IllegalArgumentException.class,
          () ->
              new SharedTsBlockQueue(
                  new TFragmentInstanceId(fixture.id.queryId, 1, "1"),
                  "foreign-fi",
                  fixture.manager,
                  fixture.executor,
                  false,
                  fixture.reservation,
                  0));
      Assert.assertEquals(fixture.totalBytes, fixture.pool.getReservedBytes());
    }
  }

  @Test
  public void testLegacyConstructorsRetainQueueAccountIdentity() throws Exception {
    long bytes = realBlock().getRetainedSizeInBytes();
    try (FrontierFixture fixture = new FrontierFixture(1, bytes)) {
      SharedTsBlockQueue legacy =
          new SharedTsBlockQueue(fixture.id, "legacy", fixture.manager, fixture.executor);
      try {
        Assert.assertFalse(legacy.isFrontierMode());
        Assert.assertEquals(-1, legacy.getFrontierLaneIndex());
        Assert.assertEquals(legacy.getMemoryReservationKey(), legacy.getActualAccountKey());
        Assert.assertEquals(legacy.getMemoryReservationKey(), legacy.getQueueIdentityKey());
      } finally {
        legacy.close();
      }
    }
  }

  @Test
  public void testFrontierNotificationsWaitForExternalQueueMonitorExit() throws Exception {
    TsBlock block = realBlock();
    try (FrontierFixture fixture = new FrontierFixture(1, block.getRetainedSizeInBytes())) {
      SharedTsBlockQueue queue = fixture.queues[0];
      ListenableFuture<Void> reader;
      ListenableFuture<Void> writer;
      synchronized (queue) {
        reader = queue.isBlocked();
        writer = queue.add(block);
        Assert.assertFalse(reader.isDone());
        Assert.assertFalse(writer.isDone());
        queue.flushNotifications();
        Assert.assertFalse(reader.isDone());
      }
      queue.flushNotifications();
      Assert.assertTrue(reader.isDone());
      Assert.assertFalse(writer.isDone());
      synchronized (queue) {
        queue.remove();
        Assert.assertFalse(writer.isDone());
      }
      queue.flushNotifications();
      writer.get(5, TimeUnit.SECONDS);
      Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
    }
  }

  @Test
  public void testFrontierOutboxReentrancyAndFailuresDoNotSkipCleanup() throws Exception {
    TsBlock block = realBlock();
    try (FrontierFixture fixture = new FrontierFixture(1, block.getRetainedSizeInBytes())) {
      SharedTsBlockQueue queue = fixture.queues[0];
      ListenableFuture<Void> writer = queue.add(block);
      AtomicInteger reentrantNotifications = new AtomicInteger();
      RuntimeException first = new RuntimeException("first callback");
      RuntimeException second = new RuntimeException("second callback");
      queue.deferNotification(
          () -> {
            Assert.assertFalse(Thread.holdsLock(queue));
            queue.deferNotification(reentrantNotifications::incrementAndGet);
            queue.flushNotifications();
            throw first;
          });
      queue.deferNotification(
          () -> {
            throw second;
          });
      queue.deferNotification(queue::close);
      RuntimeException failure =
          Assert.assertThrows(RuntimeException.class, queue::flushNotifications);
      Assert.assertSame(first, failure);
      Assert.assertArrayEquals(new Throwable[] {second}, failure.getSuppressed());
      Assert.assertEquals(1, reentrantNotifications.get());
      Assert.assertTrue(writer.isDone());
      Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
      Assert.assertEquals(fixture.totalBytes, fixture.pool.getReservedBytes());
    }
  }

  @Test
  public void testFrontierThrowingAbortSupplierStillReturnsCredit() throws Exception {
    TsBlock block = realBlock();
    try (FrontierFixture fixture = new FrontierFixture(1, block.getRetainedSizeInBytes())) {
      SharedTsBlockQueue queue = fixture.queues[0];
      ListenableFuture<Void> writer = queue.add(block);
      RuntimeException cause = new RuntimeException("abort listener");
      RuntimeException failure =
          Assert.assertThrows(
              RuntimeException.class,
              () ->
                  queue.abortWithCauseSupplier(
                      () -> {
                        Assert.assertFalse(Thread.holdsLock(queue));
                        throw cause;
                      }));
      Assert.assertSame(cause, failure);
      Assert.assertSame(cause, queue.getAbortedCause().get());
      Assert.assertTrue(writer.isDone());
      Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
      Assert.assertTrue(queue.isClosed());
      Assert.assertEquals(0, queue.getBufferRetainedSizeInBytes());
    }
  }

  @Test
  public void testFrontierThrowingCreditListenerStillFreesClosingGroup() throws Exception {
    TsBlock block = realBlock();
    try (FrontierFixture fixture = new FrontierFixture(1, block.getRetainedSizeInBytes())) {
      SharedTsBlockQueue queue = fixture.queues[0];
      ListenableFuture<Void> writer = queue.add(block);
      AssertionError cause = new AssertionError("producer listener");
      writer.addListener(
          () -> {
            throw cause;
          },
          Runnable::run);
      ListenableFuture<Void> closed = fixture.reservation.requestClose();
      AssertionError failure = Assert.assertThrows(AssertionError.class, queue::close);
      Assert.assertSame(cause, failure);
      closed.get(5, TimeUnit.SECONDS);
      Assert.assertEquals(0, fixture.pool.getReservedBytes());
      Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
      Assert.assertTrue(queue.getCanAddTsBlock().isDone());
    }
  }

  @Test
  public void testFrontierCloseRacesAddWithoutLateInsertion() throws Exception {
    TsBlock block = realBlock();
    ExecutorService workers = Executors.newFixedThreadPool(2);
    try {
      for (int iteration = 0; iteration < 25; iteration++) {
        try (FrontierFixture fixture = new FrontierFixture(1, block.getRetainedSizeInBytes())) {
          SharedTsBlockQueue queue = fixture.queues[0];
          CountDownLatch start = new CountDownLatch(1);
          Future<ListenableFuture<Void>> added =
              workers.submit(
                  () -> {
                    start.await();
                    return queue.add(block);
                  });
          Future<?> closed =
              workers.submit(
                  () -> {
                    start.await();
                    queue.close();
                    return null;
                  });
          start.countDown();
          ListenableFuture<Void> writer = added.get(5, TimeUnit.SECONDS);
          closed.get(5, TimeUnit.SECONDS);
          writer.get(5, TimeUnit.SECONDS);
          Assert.assertTrue(queue.isClosed());
          Assert.assertTrue(queue.isEmpty());
          Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
          Assert.assertEquals(0, fixture.reservation.getInternalUsedBytes());
          Assert.assertEquals(fixture.totalBytes, fixture.pool.getReservedBytes());
        }
      }
    } finally {
      workers.shutdownNow();
      Assert.assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testFrontierCloseRacesRemoveWithoutDoubleReturn() throws Exception {
    TsBlock block = realBlock();
    ExecutorService workers = Executors.newFixedThreadPool(2);
    try {
      for (int iteration = 0; iteration < 25; iteration++) {
        try (FrontierFixture fixture = new FrontierFixture(1, block.getRetainedSizeInBytes())) {
          SharedTsBlockQueue queue = fixture.queues[0];
          ListenableFuture<Void> writer = queue.add(block);
          CountDownLatch start = new CountDownLatch(1);
          Future<?> removed =
              workers.submit(
                  () -> {
                    start.await();
                    try {
                      Assert.assertSame(block, queue.remove());
                    } catch (IllegalStateException expected) {
                      Assert.assertTrue(queue.isClosed());
                    }
                    return null;
                  });
          Future<?> closed =
              workers.submit(
                  () -> {
                    start.await();
                    queue.close();
                    return null;
                  });
          start.countDown();
          removed.get(5, TimeUnit.SECONDS);
          closed.get(5, TimeUnit.SECONDS);
          writer.get(5, TimeUnit.SECONDS);
          Assert.assertTrue(queue.isEmpty());
          Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
          fixture.reservation.requestClose().get(5, TimeUnit.SECONDS);
          Assert.assertEquals(0, fixture.pool.getReservedBytes());
          Mockito.verify(fixture.pool, Mockito.times(1))
              .free(
                  fixture.key.queryId(),
                  fixture.key.fragmentInstanceId(),
                  fixture.key.planNodeId(),
                  fixture.totalBytes);
        }
      }
    } finally {
      workers.shutdownNow();
      Assert.assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testFrontierLogicalEofPreservesPendingDataAndCannotBeRevoked() throws Exception {
    TsBlock block = realBlock();
    try (FrontierFixture fixture = new FrontierFixture(1, block.getRetainedSizeInBytes())) {
      SharedTsBlockQueue queue = fixture.queues[0];
      ListenableFuture<Void> writable = queue.add(block);
      queue.setNoMoreTsBlocks(true);
      queue.setNoMoreTsBlocks(false);
      Assert.assertTrue(queue.hasNoMoreTsBlocks());
      Assert.assertFalse(writable.isDone());
      Assert.assertEquals(1, fixture.reservation.getLeasedCreditCount());
      Assert.assertSame(block, queue.remove());
      writable.get(5, TimeUnit.SECONDS);
      Assert.assertNull(queue.remove());
      Assert.assertTrue(queue.isBlocked().isDone());
      Assert.assertThrows(IllegalStateException.class, () -> queue.add(block));
      Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
      Assert.assertEquals(fixture.totalBytes, fixture.pool.getReservedBytes());
    }
  }

  @Test
  public void testFrontierFirstTerminalPreservesAbortKindAndCause() throws Exception {
    TsBlock block = realBlock();
    RuntimeException first = new RuntimeException("first abort");
    try (FrontierFixture fixture = new FrontierFixture(1, block.getRetainedSizeInBytes())) {
      SharedTsBlockQueue queue = fixture.queues[0];
      ListenableFuture<Void> writer = queue.add(block);
      queue.abort(first);
      queue.close();
      queue.abort(new IllegalArgumentException("late abort"));
      queue.abortWithCauseSupplier(
          () -> {
            throw new AssertionError("Terminal supplier must not run again");
          });
      Assert.assertTrue(queue.wasFrontierAborted());
      Assert.assertSame(first, queue.getAbortedCause().get());
      Assert.assertTrue(writer.isDone());
      Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
    }
    try (FrontierFixture fixture = new FrontierFixture(1, block.getRetainedSizeInBytes())) {
      SharedTsBlockQueue queue = fixture.queues[0];
      ListenableFuture<Void> reader = queue.isBlocked();
      queue.close();
      queue.abort(first);
      Assert.assertFalse(queue.wasFrontierAborted());
      Assert.assertFalse(queue.getAbortedCause().isPresent());
      reader.get(5, TimeUnit.SECONDS);
    }
    try (FrontierFixture fixture = new FrontierFixture(1, block.getRetainedSizeInBytes())) {
      SharedTsBlockQueue queue = fixture.queues[0];
      queue.abort();
      queue.close();
      queue.abort(first);
      Assert.assertTrue(queue.wasFrontierAborted());
      Assert.assertFalse(queue.getAbortedCause().isPresent());
    }
  }

  @Test
  public void testInputFrontierCannotBackAnOutputQueue() throws Exception {
    TFragmentInstanceId id = new TFragmentInstanceId("input_frontier_queue", 0, "0");
    MemoryReservationKey key =
        new MemoryReservationKey(
            id.queryId,
            FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(id),
            "input_owner");
    MemoryPool pool = new MemoryPool("input-owner-pool", new MemoryManager(4096), 4096);
    LocalMemoryManager manager = Mockito.mock(LocalMemoryManager.class);
    Mockito.when(manager.getQueryPool()).thenReturn(pool);
    RuntimeFrontierReservation owner =
        RuntimeFrontierReservation.tryCreate(pool, key, MemorySide.INPUT, 1, 1024, 1024).get();
    ExecutorService executor = newDirectExecutorService();
    try {
      Assert.assertThrows(
          IllegalArgumentException.class,
          () -> new SharedTsBlockQueue(id, "invalid_output", manager, executor, false, owner, 0));
      Assert.assertEquals(1024, pool.getReservedBytes());
      Assert.assertEquals(0, owner.getLeasedCreditCount());
    } finally {
      owner.requestClose().get(5, TimeUnit.SECONDS);
      executor.shutdownNow();
    }
    Assert.assertEquals(0, pool.getReservedBytes());
  }

  private static TsBlock realBlock() {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT64));
    builder.getTimeColumnBuilder().writeLong(1L);
    builder.getColumnBuilder(0).writeLong(7L);
    builder.declarePosition();
    return builder.build();
  }

  private static final class FrontierFixture implements AutoCloseable {
    private final TFragmentInstanceId id = new TFragmentInstanceId("frontier-queue", 0, "0");
    private final MemoryReservationKey key =
        new MemoryReservationKey(
            id.queryId,
            FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(id),
            "frontier-group");
    private final long totalBytes;
    private final MemoryPool pool;
    private final LocalMemoryManager manager = Mockito.mock(LocalMemoryManager.class);
    private final ExecutorService executor = newDirectExecutorService();
    private final RuntimeFrontierReservation reservation;
    private final SharedTsBlockQueue[] queues;

    private FrontierFixture(int lanes, long bytesPerLane) {
      totalBytes = lanes * bytesPerLane;
      pool =
          Mockito.spy(
              new MemoryPool(
                  "frontier-queue-pool", new MemoryManager(4 * totalBytes), 4 * totalBytes));
      Mockito.when(manager.getQueryPool()).thenReturn(pool);
      reservation =
          RuntimeFrontierReservation.tryCreate(pool, key, lanes, bytesPerLane, totalBytes).get();
      queues = new SharedTsBlockQueue[lanes];
      for (int lane = 0; lane < lanes; lane++) {
        queues[lane] =
            new SharedTsBlockQueue(
                id, "queue-" + lane, manager, executor, false, reservation, lane);
      }
    }

    @Override
    public void close() throws Exception {
      try {
        for (SharedTsBlockQueue queue : queues) {
          queue.close();
        }
        reservation.requestClose().get(5, TimeUnit.SECONDS);
        Assert.assertEquals(0, pool.getReservedBytes());
      } finally {
        executor.shutdownNow();
      }
    }
  }
}
