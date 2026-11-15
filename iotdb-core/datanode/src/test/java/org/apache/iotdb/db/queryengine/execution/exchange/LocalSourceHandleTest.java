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
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SourceHandleListener;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

public class LocalSourceHandleTest {
  @Test
  public void testReceive() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final String localPlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");

    // Construct a mock LocalMemoryManager that do not block any reservation.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a shared TsBlock queue.
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            localFragmentInstanceId,
            localPlanNodeId,
            mockLocalMemoryManager,
            newDirectExecutorService());

    LocalSourceHandle localSourceHandle =
        new LocalSourceHandle(
            localFragmentInstanceId, localPlanNodeId, queue, mockSourceHandleListener);
    Assert.assertFalse(localSourceHandle.isBlocked().isDone());
    Assert.assertFalse(localSourceHandle.isAborted());
    Assert.assertFalse(localSourceHandle.isFinished());
    Assert.assertEquals(0L, localSourceHandle.getBufferRetainedSizeInBytes());

    // Local sink handle produces tsblocks.
    queue.add(Utils.createMockTsBlock(mockTsBlockSize));
    queue.setNoMoreTsBlocks(true);
    Assert.assertTrue(localSourceHandle.isBlocked().isDone());
    Assert.assertFalse(localSourceHandle.isAborted());
    Assert.assertFalse(localSourceHandle.isFinished());
    Assert.assertEquals(mockTsBlockSize, localSourceHandle.getBufferRetainedSizeInBytes());

    // Consume tsblocks.
    Assert.assertTrue(localSourceHandle.isBlocked().isDone());
    localSourceHandle.receive();
    ListenableFuture<?> blocked = localSourceHandle.isBlocked();
    Assert.assertTrue(blocked.isDone());
    Assert.assertFalse(localSourceHandle.isAborted());
    Assert.assertTrue(localSourceHandle.isFinished());
    Mockito.verify(mockSourceHandleListener, Mockito.times(1)).onFinished(localSourceHandle);
  }

  @Test
  public void testAbort() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final String localPlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");

    // Construct a mock LocalMemoryManager that do not block any reservation.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a shared tsblock queue.
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            localFragmentInstanceId,
            localPlanNodeId,
            mockLocalMemoryManager,
            newDirectExecutorService());

    LocalSourceHandle localSourceHandle =
        new LocalSourceHandle(
            localFragmentInstanceId, localPlanNodeId, queue, mockSourceHandleListener);
    ListenableFuture<?> future = localSourceHandle.isBlocked();
    Assert.assertFalse(future.isDone());
    Assert.assertFalse(localSourceHandle.isAborted());
    Assert.assertFalse(localSourceHandle.isFinished());
    Assert.assertEquals(0L, localSourceHandle.getBufferRetainedSizeInBytes());

    // Close the local source handle.
    localSourceHandle.abort();
    Assert.assertTrue(future.isDone());
    Assert.assertTrue(localSourceHandle.isAborted());
    Assert.assertFalse(localSourceHandle.isFinished());
    Mockito.verify(mockSourceHandleListener, Mockito.times(1)).onAborted(localSourceHandle);
  }

  @Test
  public void testFrontierReceiveReturnsCreditBeforeFinishing() {
    try (FrontierFixture f = new FrontierFixture()) {
      Mockito.doAnswer(
              invocation -> {
                f.assertNoHandleLocksHeld();
                Assert.assertEquals(0, f.reservation.getLeasedCreditCount());
                Assert.assertEquals(0, f.pool.getReservedBytes());
                return null;
              })
          .when(f.listener)
          .onFinished(f.source);

      ListenableFuture<Void> writable = f.queue.add(f.block);
      AtomicBoolean writableOutsideLocks = new AtomicBoolean();
      writable.addListener(
          () -> writableOutsideLocks.set(!Thread.holdsLock(f.queue) && !Thread.holdsLock(f.source)),
          MoreExecutors.directExecutor());
      Assert.assertFalse(writable.isDone());
      f.queue.setNoMoreTsBlocks(true);
      f.source.checkAndInvokeOnFinished();
      Mockito.verify(f.listener, Mockito.never()).onFinished(f.source);
      Assert.assertFalse(f.source.isFinished());
      ListenableFuture<Void> closed = f.reservation.requestClose();
      Assert.assertFalse(closed.isDone());

      Assert.assertSame(f.block, f.source.receive());
      Assert.assertTrue(writable.isDone());
      Assert.assertTrue(writableOutsideLocks.get());
      Assert.assertTrue(closed.isDone());
      Assert.assertTrue(f.source.isFinished());
      Assert.assertNull(f.source.receive());
      f.source.checkAndInvokeOnFinished();
      f.source.close();
      Mockito.verify(f.listener, Mockito.times(1)).onFinished(f.source);
      Mockito.verify(f.listener, Mockito.never()).onAborted(f.source);
    }
  }

  @Test
  public void testFrontierReceiveKeepsGroupPrepaymentAndLogicalEof() {
    try (FrontierFixture f = new FrontierFixture()) {
      ListenableFuture<Void> first = f.queue.add(f.block);
      Assert.assertSame(f.block, f.source.receive());
      Assert.assertTrue(first.isDone());
      Assert.assertEquals(0, f.reservation.getLeasedCreditCount());
      Assert.assertEquals(f.block.getRetainedSizeInBytes(), f.pool.getReservedBytes());
      Assert.assertFalse(f.source.isFinished());
      Mockito.verify(f.listener, Mockito.never()).onFinished(f.source);

      ListenableFuture<Void> second = f.queue.add(f.block);
      Assert.assertFalse(second.isDone());
      f.source.close();
      Assert.assertTrue(second.isDone());
      Assert.assertEquals(0, f.reservation.getInternalUsedBytes());
      Assert.assertEquals(f.block.getRetainedSizeInBytes(), f.pool.getReservedBytes());
      f.source.close();
      f.source.abort();
      Mockito.verify(f.listener, Mockito.times(1)).onFinished(f.source);
      Mockito.verify(f.listener, Mockito.never()).onAborted(f.source);
      Assert.assertTrue(f.reservation.requestClose().isDone());
      Assert.assertEquals(0, f.pool.getReservedBytes());
    }
  }

  @Test
  public void testFrontierAbortReturnsCreditAndNotifiesOnceOutsideLocks() {
    try (FrontierFixture f = new FrontierFixture()) {
      Mockito.doAnswer(
              invocation -> {
                f.assertNoHandleLocksHeld();
                Assert.assertEquals(0, f.reservation.getLeasedCreditCount());
                return null;
              })
          .when(f.listener)
          .onAborted(f.source);
      ListenableFuture<Void> writable = f.queue.add(f.block);
      ListenableFuture<Void> closed = f.reservation.requestClose();

      f.source.abort();
      f.source.abort();
      f.source.abort(new IllegalStateException("late abort"));
      f.source.close();

      Assert.assertTrue(f.source.isAborted());
      Assert.assertTrue(writable.isDone());
      Assert.assertTrue(closed.isDone());
      Assert.assertEquals(0, f.pool.getReservedBytes());
      Mockito.verify(f.listener, Mockito.times(1)).onAborted(f.source);
      Mockito.verify(f.listener, Mockito.never()).onFinished(f.source);
    }
  }

  @Test
  public void testFrontierAbortCauseReachesPendingConsumer() throws Exception {
    try (FrontierFixture f = new FrontierFixture()) {
      IllegalArgumentException cause = new IllegalArgumentException("frontier consumer failure");
      ListenableFuture<?> blocked = f.source.isBlocked();
      Assert.assertFalse(blocked.isDone());
      Mockito.doAnswer(
              invocation -> {
                f.assertNoHandleLocksHeld();
                Assert.assertSame(cause, f.queue.getAbortedCause().get());
                return null;
              })
          .when(f.listener)
          .onAborted(f.source);

      f.source.abort(cause);
      Assert.assertTrue(blocked.isDone());
      try {
        blocked.get();
        Assert.fail("The pending consumer must observe the abort cause");
      } catch (ExecutionException expected) {
        Assert.assertSame(cause, expected.getCause());
      }
      try {
        f.source.receive();
        Assert.fail("An aborted source must reject reads");
      } catch (IllegalStateException expected) {
        Assert.assertSame(cause, expected.getCause());
      }
      f.source.abort(cause);
      Mockito.verify(f.listener, Mockito.times(1)).onAborted(f.source);
    }
  }

  @Test
  public void testFrontierThrowingAbortListenerCannotLeakCredit() {
    try (FrontierFixture f = new FrontierFixture()) {
      IllegalStateException failure = new IllegalStateException("abort listener failure");
      Mockito.doAnswer(
              invocation -> {
                f.assertNoHandleLocksHeld();
                throw failure;
              })
          .when(f.listener)
          .onAborted(f.source);
      ListenableFuture<Void> writable = f.queue.add(f.block);
      ListenableFuture<Void> closed = f.reservation.requestClose();

      try {
        f.source.abort();
        Assert.fail("Listener failure must be reported after draining notifications");
      } catch (IllegalStateException expected) {
        Assert.assertSame(failure, expected);
      }
      Assert.assertTrue(writable.isDone());
      Assert.assertTrue(closed.isDone());
      Assert.assertEquals(0, f.reservation.getLeasedCreditCount());
      Assert.assertEquals(0, f.pool.getReservedBytes());
      f.source.abort();
      Mockito.verify(f.listener, Mockito.times(1)).onAborted(f.source);
    }
  }

  @Test
  public void testFrontierThrowingFinishListenerCannotLeakCredit() {
    try (FrontierFixture f = new FrontierFixture()) {
      IllegalStateException failure = new IllegalStateException("finish listener failure");
      Mockito.doAnswer(
              invocation -> {
                f.assertNoHandleLocksHeld();
                throw failure;
              })
          .when(f.listener)
          .onFinished(f.source);
      ListenableFuture<Void> writable = f.queue.add(f.block);
      f.queue.setNoMoreTsBlocks(true);
      ListenableFuture<Void> closed = f.reservation.requestClose();

      try {
        f.source.receive();
        Assert.fail("Listener failure must be reported after draining notifications");
      } catch (IllegalStateException expected) {
        Assert.assertSame(failure, expected);
      }
      Assert.assertTrue(writable.isDone());
      Assert.assertTrue(closed.isDone());
      Assert.assertEquals(0, f.reservation.getInternalUsedBytes());
      Assert.assertEquals(0, f.pool.getReservedBytes());
      f.source.checkAndInvokeOnFinished();
      f.source.close();
      Mockito.verify(f.listener, Mockito.times(1)).onFinished(f.source);
    }
  }

  @Test
  public void testFrontierReentrantFinishListenerDoesNotRepeatNotification() {
    try (FrontierFixture f = new FrontierFixture()) {
      Mockito.doAnswer(
              invocation -> {
                f.assertNoHandleLocksHeld();
                f.source.close();
                f.source.checkAndInvokeOnFinished();
                f.queue.flushNotifications();
                return null;
              })
          .when(f.listener)
          .onFinished(f.source);
      f.queue.add(f.block);
      f.queue.setNoMoreTsBlocks(true);
      Assert.assertSame(f.block, f.source.receive());
      Assert.assertTrue(f.source.isClosed());
      Assert.assertEquals(0, f.reservation.getLeasedCreditCount());
      Mockito.verify(f.listener, Mockito.times(1)).onFinished(f.source);
    }
  }

  @Test(timeout = 15000)
  public void testFrontierConcurrentCloseAndReceiveReturnCreditOnce() throws Exception {
    try (FrontierFixture f = new FrontierFixture()) {
      f.queue.add(f.block);
      ListenableFuture<Void> closed = f.reservation.requestClose();
      ExecutorService workers = Executors.newFixedThreadPool(2);
      CountDownLatch start = new CountDownLatch(1);
      try {
        Future<?> receive =
            workers.submit(
                () -> {
                  awaitStart(start);
                  try {
                    f.source.receive();
                  } catch (IllegalStateException expected) {
                    Assert.assertTrue(f.source.isClosed());
                  }
                });
        Future<?> close =
            workers.submit(
                () -> {
                  awaitStart(start);
                  f.source.close();
                });
        start.countDown();
        receive.get(10, TimeUnit.SECONDS);
        close.get(10, TimeUnit.SECONDS);
        f.queue.flushNotifications();
        Assert.assertTrue(closed.isDone());
        Assert.assertEquals(0, f.reservation.getLeasedCreditCount());
        Assert.assertEquals(0, f.pool.getReservedBytes());
        Mockito.verify(f.listener, Mockito.times(1)).onFinished(f.source);
      } finally {
        workers.shutdownNow();
      }
    }
  }

  @Test
  public void testFrontierQueuedEofCannotFinishAfterDirectQueueAbort() {
    try (FrontierFixture f = new FrontierFixture()) {
      IllegalArgumentException cause =
          new IllegalArgumentException("abort before EOF notification");
      synchronized (f.queue) {
        f.queue.setNoMoreTsBlocks(true);
        f.queue.abort(cause);
      }
      f.queue.flushNotifications();
      Assert.assertTrue(f.queue.isClosed());
      Assert.assertSame(cause, f.queue.getAbortedCause().get());
      f.source.checkAndInvokeOnFinished();
      Mockito.verify(f.listener, Mockito.never()).onFinished(f.source);

      // Settle the source endpoint separately; the queue already owns the original failure.
      f.source.abort(cause);
      Mockito.verify(f.listener, Mockito.times(1)).onAborted(f.source);
      Mockito.verify(f.listener, Mockito.never()).onFinished(f.source);
      Assert.assertSame(cause, f.queue.getAbortedCause().get());
    }
  }

  @Test
  public void testFrontierClosePropagatesDirectQueueAbortWithoutCause() {
    try (FrontierFixture f = new FrontierFixture()) {
      Mockito.doAnswer(
              invocation -> {
                f.assertNoHandleLocksHeld();
                return null;
              })
          .when(f.listener)
          .onAborted(f.source);
      ListenableFuture<Void> writable = f.queue.add(f.block);
      ListenableFuture<Void> closed = f.reservation.requestClose();
      f.queue.abort();
      f.source.close();
      f.source.close();

      Assert.assertTrue(f.source.isAborted());
      Assert.assertFalse(f.source.isClosed());
      Assert.assertTrue(f.queue.wasFrontierAborted());
      Assert.assertFalse(f.queue.getAbortedCause().isPresent());
      Assert.assertTrue(writable.isDone());
      Assert.assertTrue(closed.isDone());
      Assert.assertEquals(0, f.pool.getReservedBytes());
      Mockito.verify(f.listener, Mockito.times(1)).onAborted(f.source);
      Mockito.verify(f.listener, Mockito.never()).onFinished(f.source);
    }
  }

  @Test
  public void testFrontierClosePropagatesDirectQueueAbortCause() {
    try (FrontierFixture f = new FrontierFixture()) {
      IllegalArgumentException cause = new IllegalArgumentException("direct queue failure");
      Mockito.doAnswer(
              invocation -> {
                f.assertNoHandleLocksHeld();
                Assert.assertSame(cause, f.queue.getAbortedCause().get());
                return null;
              })
          .when(f.listener)
          .onAborted(f.source);
      ListenableFuture<Void> writable = f.queue.add(f.block);
      ListenableFuture<Void> closed = f.reservation.requestClose();
      f.queue.abort(cause);
      f.source.close();
      f.source.abort();

      Assert.assertTrue(f.source.isAborted());
      Assert.assertSame(cause, f.queue.getAbortedCause().get());
      Assert.assertTrue(writable.isDone());
      Assert.assertTrue(closed.isDone());
      Assert.assertEquals(0, f.pool.getReservedBytes());
      Mockito.verify(f.listener, Mockito.times(1)).onAborted(f.source);
      Mockito.verify(f.listener, Mockito.never()).onFinished(f.source);
    }
  }

  @Test
  public void testFrontierBufferedEofAndQueueAbortCannotFinishDuringSourceClose() {
    try (FrontierFixture f = new FrontierFixture()) {
      IllegalArgumentException cause = new IllegalArgumentException("buffered EOF failure");
      ListenableFuture<Void> writable = f.queue.add(f.block);
      ListenableFuture<Void> closed = f.reservation.requestClose();
      synchronized (f.queue) {
        f.queue.setNoMoreTsBlocks(true);
        Assert.assertFalse(f.source.isFinished());
        f.queue.abort(cause);
        f.source.close();
      }
      f.queue.flushNotifications();

      Assert.assertTrue(f.source.isAborted());
      Assert.assertSame(cause, f.queue.getAbortedCause().get());
      Assert.assertTrue(writable.isDone());
      Assert.assertTrue(closed.isDone());
      Assert.assertEquals(0, f.reservation.getLeasedCreditCount());
      Assert.assertEquals(0, f.pool.getReservedBytes());
      f.source.close();
      Mockito.verify(f.listener, Mockito.times(1)).onAborted(f.source);
      Mockito.verify(f.listener, Mockito.never()).onFinished(f.source);
    }
  }

  @Test
  public void testFrontierClaimedNaturalEofIsNotNotifiedAgainAfterQueueAbort() {
    try (FrontierFixture f = new FrontierFixture()) {
      f.queue.setNoMoreTsBlocks(true);
      f.queue.flushNotifications();
      f.source.checkAndInvokeOnFinished();
      Mockito.verify(f.listener, Mockito.times(1)).onFinished(f.source);

      f.queue.abort(new IllegalArgumentException("after claimed EOF"));
      f.source.close();
      f.source.close();
      Mockito.verify(f.listener, Mockito.times(1)).onFinished(f.source);
      Mockito.verify(f.listener, Mockito.never()).onAborted(f.source);
    }
  }

  @Test
  public void testFrontierNormalQueueCloseWinsBeforeSourceAbort() {
    try (FrontierFixture f = new FrontierFixture()) {
      Mockito.doAnswer(
              invocation -> {
                f.assertNoHandleLocksHeld();
                return null;
              })
          .when(f.listener)
          .onFinished(f.source);
      ListenableFuture<Void> writable = f.queue.add(f.block);
      ListenableFuture<Void> closed = f.reservation.requestClose();
      f.queue.close();
      f.source.abort();
      f.source.abort(new IllegalArgumentException("late abort"));
      f.source.close();

      Assert.assertTrue(f.source.isClosed());
      Assert.assertFalse(f.source.isAborted());
      Assert.assertFalse(f.queue.wasFrontierAborted());
      Assert.assertFalse(f.queue.getAbortedCause().isPresent());
      Assert.assertTrue(writable.isDone());
      Assert.assertTrue(closed.isDone());
      Assert.assertEquals(0, f.pool.getReservedBytes());
      Mockito.verify(f.listener, Mockito.times(1)).onFinished(f.source);
      Mockito.verify(f.listener, Mockito.never()).onAborted(f.source);
    }
  }

  @Test
  public void testConstructorValidatesListenerBeforePublishingSource() {
    SharedTsBlockQueue queue = Mockito.mock(SharedTsBlockQueue.class);
    try {
      new LocalSourceHandle(queue, null, "unpublished");
      Assert.fail("A null listener must be rejected");
    } catch (NullPointerException expected) {
      // No partially initialized handle may be visible through the queue.
    }
    Mockito.verify(queue, Mockito.never()).setSourceHandle(Mockito.any(LocalSourceHandle.class));

    try {
      new LocalSourceHandle(new TFragmentInstanceId("invalid-listener", 0, "0"), "n", queue, null);
      Assert.fail("A null listener must be rejected");
    } catch (NullPointerException expected) {
      // This constructor has the same publication guarantee.
    }
    Mockito.verify(queue, Mockito.never()).setSourceHandle(Mockito.any(LocalSourceHandle.class));
  }

  private static void awaitStart(CountDownLatch start) {
    try {
      Assert.assertTrue(start.await(10, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static final class FrontierFixture implements AutoCloseable {
    private final TsBlock block;
    private final MemoryPool pool;
    private final RuntimeFrontierReservation reservation;
    private final ExecutorService notificationExecutor = newDirectExecutorService();
    private final SharedTsBlockQueue queue;
    private final SourceHandleListener listener = Mockito.mock(SourceHandleListener.class);
    private final LocalSourceHandle source;

    private FrontierFixture() {
      TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT64));
      for (int i = 0; i < 3; i++) {
        builder.getTimeColumnBuilder().writeLong(i);
        builder.getColumnBuilder(0).writeLong(i * 10L);
        builder.declarePosition();
      }
      block = builder.build();
      long retained = block.getRetainedSizeInBytes();
      pool = new MemoryPool("frontier-source", new MemoryManager(retained * 32), retained * 16);
      LocalMemoryManager manager = Mockito.mock(LocalMemoryManager.class);
      Mockito.when(manager.getQueryPool()).thenReturn(pool);
      TFragmentInstanceId id = new TFragmentInstanceId("frontier-source", 0, "0");
      SharedTsBlockQueue.MemoryReservationKey key =
          new SharedTsBlockQueue.MemoryReservationKey(
              id.getQueryId(),
              FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(id),
              "frontier-group");
      reservation =
          RuntimeFrontierReservation.tryCreate(pool, key, 1, retained, retained * 16).get();
      queue =
          new SharedTsBlockQueue(
              id, "frontier-queue", manager, notificationExecutor, false, reservation, 0);
      source = new LocalSourceHandle(id, "frontier-source", queue, listener);
    }

    private void assertNoHandleLocksHeld() {
      Assert.assertFalse(Thread.holdsLock(queue));
      Assert.assertFalse(Thread.holdsLock(source));
    }

    @Override
    public void close() {
      try {
        source.close();
      } finally {
        reservation.requestClose();
        notificationExecutor.shutdownNow();
      }
      Assert.assertEquals(0, pool.getReservedBytes());
    }
  }
}
