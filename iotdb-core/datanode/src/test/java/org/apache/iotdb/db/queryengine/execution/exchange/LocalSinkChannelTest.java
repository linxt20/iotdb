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
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SinkListener;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;

public class LocalSinkChannelTest {
  @Test
  public void testSend() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L;
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");

    // Construct a mock LocalMemoryManager with capacity 5 * mockTsBlockSize per query.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryManager memoryManager = Mockito.spy(new MemoryManager(10 * mockTsBlockSize));
    MemoryPool spyMemoryPool =
        Mockito.spy(new MemoryPool("test", memoryManager, 5 * mockTsBlockSize));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);
    // Construct a mock SinkListener.
    SinkListener mockSinkListener = Mockito.mock(SinkListener.class);
    // Construct a shared TsBlock queue.
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            remoteFragmentInstanceId,
            remotePlanNodeId,
            mockLocalMemoryManager,
            newDirectExecutorService());

    // Construct Sink.
    LocalSinkChannel localSinkChannel =
        new LocalSinkChannel(localFragmentInstanceId, queue, mockSinkListener);

    final long queueCap = queue.getMaxBytesCanReserve();
    Assert.assertTrue(queueCap >= 10 * mockTsBlockSize);

    // Construct SourceHandle
    LocalSourceHandle localSourceHandle =
        new LocalSourceHandle(
            localFragmentInstanceId,
            remotePlanNodeId,
            queue,
            Mockito.mock(MPPDataExchangeManager.SourceHandleListener.class));

    Assert.assertFalse(localSinkChannel.isFull().isDone());
    localSourceHandle.isBlocked();
    // blocked of LocalSinkChannel should be completed after calling isBlocked() of corresponding
    // LocalSourceHandle
    Assert.assertTrue(localSinkChannel.isFull().isDone());
    Assert.assertFalse(localSinkChannel.isFinished());
    Assert.assertFalse(localSinkChannel.isAborted());
    Assert.assertEquals(0L, localSinkChannel.getBufferRetainedSizeInBytes());

    // Send TsBlocks.
    int numOfSentTsblocks = 0;
    while (localSinkChannel.isFull().isDone()) {
      localSinkChannel.send(Utils.createMockTsBlock(mockTsBlockSize));
      numOfSentTsblocks += 1;
    }
    Assert.assertEquals(11, numOfSentTsblocks);
    Assert.assertFalse(localSinkChannel.isFull().isDone());
    Assert.assertFalse(localSinkChannel.isFinished());
    Assert.assertEquals(11 * mockTsBlockSize, localSinkChannel.getBufferRetainedSizeInBytes());
    Mockito.verify(spyMemoryPool, Mockito.times(11))
        .reserveWithPriority(
            queryId,
            FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                remoteFragmentInstanceId),
            remotePlanNodeId,
            mockTsBlockSize,
            queueCap,
            false);

    // Receive TsBlocks.
    int numOfReceivedTsblocks = 0;
    while (!queue.isEmpty()) {
      queue.remove();
      numOfReceivedTsblocks += 1;
    }
    Assert.assertEquals(11, numOfReceivedTsblocks);
    Assert.assertTrue(localSinkChannel.isFull().isDone());
    Assert.assertFalse(localSinkChannel.isFinished());
    Assert.assertEquals(0L, localSinkChannel.getBufferRetainedSizeInBytes());
    Mockito.verify(spyMemoryPool, Mockito.times(11))
        .free(
            queryId,
            FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                remoteFragmentInstanceId),
            remotePlanNodeId,
            mockTsBlockSize);

    // Set no-more-TsBlocks.
    localSinkChannel.setNoMoreTsBlocks();
    Assert.assertTrue(localSinkChannel.isFull().isDone());
    Assert.assertTrue(localSinkChannel.isFinished());
    Mockito.verify(mockSinkListener, Mockito.times(1)).onEndOfBlocks(localSinkChannel);
    Mockito.verify(mockSinkListener, Mockito.times(1)).onFinish(localSinkChannel);
  }

  @Test
  public void testAbort() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L;
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");

    // Construct a mock LocalMemoryManager with capacity 5 * mockTsBlockSize per read.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryManager memoryManager = Mockito.spy(new MemoryManager(10 * mockTsBlockSize));
    MemoryPool spyMemoryPool =
        Mockito.spy(new MemoryPool("test", memoryManager, 5 * mockTsBlockSize));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);
    // Construct a mock SinkListener.
    SinkListener mockSinkListener = Mockito.mock(SinkListener.class);
    // Construct a shared tsblock queue.
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            remoteFragmentInstanceId,
            remotePlanNodeId,
            mockLocalMemoryManager,
            newDirectExecutorService());

    // Construct SinkChannel.
    LocalSinkChannel localSinkChannel =
        new LocalSinkChannel(localFragmentInstanceId, queue, mockSinkListener);

    final long queueCap = queue.getMaxBytesCanReserve();
    Assert.assertTrue(queueCap >= 10 * mockTsBlockSize);

    // Construct SourceHandle
    LocalSourceHandle localSourceHandle =
        new LocalSourceHandle(
            localFragmentInstanceId,
            remotePlanNodeId,
            queue,
            Mockito.mock(MPPDataExchangeManager.SourceHandleListener.class));

    Assert.assertFalse(localSinkChannel.isFull().isDone());
    localSourceHandle.isBlocked();
    // blocked of LocalSinkChannel should be completed after calling isBlocked() of corresponding
    // LocalSourceHandle
    Assert.assertTrue(localSinkChannel.isFull().isDone());
    Assert.assertFalse(localSinkChannel.isFinished());
    Assert.assertFalse(localSinkChannel.isAborted());
    Assert.assertEquals(0L, localSinkChannel.getBufferRetainedSizeInBytes());

    // Send TsBlocks.
    int numOfSentTsblocks = 0;
    while (localSinkChannel.isFull().isDone()) {
      localSinkChannel.send(Utils.createMockTsBlock(mockTsBlockSize));
      numOfSentTsblocks += 1;
    }
    Assert.assertEquals(11, numOfSentTsblocks);
    ListenableFuture<?> blocked = localSinkChannel.isFull();
    Assert.assertFalse(blocked.isDone());
    Assert.assertFalse(localSinkChannel.isFinished());
    Assert.assertEquals(11 * mockTsBlockSize, localSinkChannel.getBufferRetainedSizeInBytes());
    Mockito.verify(spyMemoryPool, Mockito.times(11))
        .reserveWithPriority(
            queryId,
            FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                remoteFragmentInstanceId),
            remotePlanNodeId,
            mockTsBlockSize,
            queueCap,
            false);

    // Abort.
    localSinkChannel.abort();
    Assert.assertTrue(blocked.isDone());
    Assert.assertFalse(localSinkChannel.isFinished());
    Assert.assertTrue(localSinkChannel.isAborted());
    Mockito.verify(mockSinkListener, Mockito.times(1)).onAborted(localSinkChannel);
  }

  @Test
  public void testFrontierSendAndReceiveCompleteFuturesOutsideHandleLocks() {
    try (FrontierFixture fixture = new FrontierFixture(Mockito.mock(SinkListener.class))) {
      AtomicInteger consumerNotifications = new AtomicInteger();
      fixture
          .source
          .isBlocked()
          .addListener(
              () -> {
                assertFrontierLocksReleased(fixture);
                consumerNotifications.incrementAndGet();
              },
              directExecutor());
      Assert.assertTrue(fixture.sink.isFull().isDone());

      fixture.sink.send(fixture.block);
      Assert.assertEquals(1, consumerNotifications.get());
      Assert.assertEquals(fixture.laneBytes, fixture.pool.getReservedBytes());
      Assert.assertEquals(1, fixture.reservation.getLeasedCreditCount());
      Assert.assertEquals(
          fixture.block.getRetainedSizeInBytes(), fixture.sink.getBufferRetainedSizeInBytes());

      AtomicInteger producerNotifications = new AtomicInteger();
      ListenableFuture<?> producer = fixture.sink.isFull();
      Assert.assertFalse(producer.isDone());
      producer.addListener(
          () -> {
            assertFrontierLocksReleased(fixture);
            producerNotifications.incrementAndGet();
          },
          directExecutor());

      Assert.assertSame(fixture.block, fixture.source.receive());
      Assert.assertTrue(producer.isDone());
      Assert.assertEquals(1, producerNotifications.get());
      Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
      Assert.assertEquals(0L, fixture.sink.getBufferRetainedSizeInBytes());
      Assert.assertEquals(fixture.laneBytes, fixture.pool.getReservedBytes());
      // Releasing execution capacity is not logical producer EOF.
      Assert.assertFalse(fixture.sink.isFinished());
      Assert.assertFalse(fixture.sink.isNoMoreTsBlocks());
    }
  }

  @Test
  public void testFrontierEofPreservesBufferedDataAndNotifiesEndBeforeFinishOnce() {
    final FrontierFixture[] holder = new FrontierFixture[1];
    List<String> events = new ArrayList<>();
    SinkListener listener = Mockito.mock(SinkListener.class);
    Mockito.doAnswer(
            invocation -> {
              assertFrontierLocksReleased(holder[0]);
              events.add("end");
              return null;
            })
        .when(listener)
        .onEndOfBlocks(Mockito.any(LocalSinkChannel.class));
    Mockito.doAnswer(
            invocation -> {
              assertFrontierLocksReleased(holder[0]);
              events.add("finish");
              return null;
            })
        .when(listener)
        .onFinish(Mockito.any(LocalSinkChannel.class));
    try (FrontierFixture fixture = new FrontierFixture(listener)) {
      holder[0] = fixture;
      fixture.source.isBlocked();
      fixture.sink.send(fixture.block);
      fixture.sink.setNoMoreTsBlocks();
      fixture.sink.setNoMoreTsBlocks();

      Assert.assertEquals(Collections.singletonList("end"), events);
      Assert.assertFalse(fixture.sink.isFinished());
      Assert.assertEquals(1, fixture.sink.getNumOfBufferedTsBlocks());
      Assert.assertSame(fixture.block, fixture.source.receive());

      fixture.sink.checkAndInvokeOnFinished();
      fixture.sink.checkAndInvokeOnFinished();
      Assert.assertEquals(Arrays.asList("end", "finish"), events);
      Assert.assertTrue(fixture.sink.isFinished());
      Mockito.verify(listener, Mockito.times(1)).onEndOfBlocks(fixture.sink);
      Mockito.verify(listener, Mockito.times(1)).onFinish(fixture.sink);
    }
  }

  @Test
  public void testFrontierEmptyEofReentrantCallbacksDrainWithoutDuplicates() {
    final FrontierFixture[] holder = new FrontierFixture[1];
    List<String> events = new ArrayList<>();
    SinkListener listener = Mockito.mock(SinkListener.class);
    Mockito.doAnswer(
            invocation -> {
              FrontierFixture fixture = holder[0];
              assertFrontierLocksReleased(fixture);
              events.add("end");
              fixture.sink.setNoMoreTsBlocks();
              fixture.sink.checkAndInvokeOnFinished();
              return null;
            })
        .when(listener)
        .onEndOfBlocks(Mockito.any(LocalSinkChannel.class));
    Mockito.doAnswer(
            invocation -> {
              FrontierFixture fixture = holder[0];
              assertFrontierLocksReleased(fixture);
              events.add("finish");
              fixture.sink.close();
              return null;
            })
        .when(listener)
        .onFinish(Mockito.any(LocalSinkChannel.class));

    try (FrontierFixture fixture = new FrontierFixture(listener)) {
      holder[0] = fixture;
      fixture.sink.setNoMoreTsBlocks();
      fixture.sink.setNoMoreTsBlocks();
      fixture.sink.checkAndInvokeOnFinished();
      Assert.assertEquals(Arrays.asList("end", "finish"), events);
      Assert.assertTrue(fixture.queue.isClosed());
      Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
    }
  }

  @Test
  public void testFrontierThrowingEndListenerStillDeliversClaimedFinish() {
    final FrontierFixture[] holder = new FrontierFixture[1];
    RuntimeException expected = new RuntimeException("end-listener-failure");
    AtomicInteger finishes = new AtomicInteger();
    SinkListener listener = Mockito.mock(SinkListener.class);
    Mockito.doAnswer(
            invocation -> {
              assertFrontierLocksReleased(holder[0]);
              throw expected;
            })
        .when(listener)
        .onEndOfBlocks(Mockito.any(LocalSinkChannel.class));
    Mockito.doAnswer(
            invocation -> {
              assertFrontierLocksReleased(holder[0]);
              finishes.incrementAndGet();
              return null;
            })
        .when(listener)
        .onFinish(Mockito.any(LocalSinkChannel.class));

    try (FrontierFixture fixture = new FrontierFixture(listener)) {
      holder[0] = fixture;
      try {
        fixture.sink.setNoMoreTsBlocks();
        Assert.fail("Expected listener failure");
      } catch (RuntimeException e) {
        Assert.assertSame(expected, e);
      }
      fixture.sink.setNoMoreTsBlocks();
      fixture.sink.checkAndInvokeOnFinished();
      Assert.assertEquals(1, finishes.get());
      Mockito.verify(listener, Mockito.times(1)).onEndOfBlocks(fixture.sink);
    }
  }

  @Test
  public void testFrontierCloseReleasesCreditEvenWhenFinishListenerThrows() {
    final FrontierFixture[] holder = new FrontierFixture[1];
    RuntimeException expected = new RuntimeException("finish-listener-failure");
    SinkListener listener = Mockito.mock(SinkListener.class);
    Mockito.doAnswer(
            invocation -> {
              assertFrontierLocksReleased(holder[0]);
              throw expected;
            })
        .when(listener)
        .onFinish(Mockito.any(LocalSinkChannel.class));

    try (FrontierFixture fixture = new FrontierFixture(listener)) {
      holder[0] = fixture;
      fixture.source.isBlocked();
      fixture.sink.send(fixture.block);
      ListenableFuture<?> producer = fixture.sink.isFull();
      AtomicInteger wakeups = new AtomicInteger();
      producer.addListener(
          () -> {
            assertFrontierLocksReleased(fixture);
            wakeups.incrementAndGet();
          },
          directExecutor());
      ListenableFuture<Void> groupClosed = fixture.reservation.requestClose();
      Assert.assertFalse(groupClosed.isDone());

      try {
        fixture.sink.close();
        Assert.fail("Expected listener failure");
      } catch (RuntimeException e) {
        Assert.assertSame(expected, e);
      }
      Assert.assertTrue(producer.isDone());
      Assert.assertEquals(1, wakeups.get());
      Assert.assertTrue(groupClosed.isDone());
      Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
      Assert.assertEquals(0L, fixture.pool.getReservedBytes());
      Assert.assertTrue(fixture.queue.isEmpty());
      Assert.assertFalse(fixture.sink.close());
      Assert.assertFalse(fixture.sink.abort());
      Mockito.verify(listener, Mockito.times(1)).onFinish(fixture.sink);
    }
  }

  @Test
  public void testFrontierAbortPublishesListenerCauseOutsideHandleLocks() throws Exception {
    final FrontierFixture[] holder = new FrontierFixture[1];
    RuntimeException cause = new RuntimeException("abort-cause");
    SinkListener listener = Mockito.mock(SinkListener.class);
    Mockito.doAnswer(
            invocation -> {
              assertFrontierLocksReleased(holder[0]);
              return Optional.of(cause);
            })
        .when(listener)
        .onAborted(Mockito.any(LocalSinkChannel.class));

    try (FrontierFixture fixture = new FrontierFixture(listener)) {
      holder[0] = fixture;
      ListenableFuture<?> consumer = fixture.source.isBlocked();
      Assert.assertFalse(consumer.isDone());
      AtomicInteger wakeups = new AtomicInteger();
      consumer.addListener(
          () -> {
            assertFrontierLocksReleased(fixture);
            wakeups.incrementAndGet();
          },
          directExecutor());

      Assert.assertTrue(fixture.sink.abort());
      Assert.assertTrue(consumer.isDone());
      Assert.assertEquals(1, wakeups.get());
      try {
        consumer.get();
        Assert.fail("Expected the supplied abort cause");
      } catch (ExecutionException e) {
        Assert.assertSame(cause, e.getCause());
      }
      try {
        fixture.sink.isFull();
        Assert.fail("Expected aborted sink");
      } catch (IllegalStateException e) {
        Assert.assertSame(cause, e.getCause());
      }
      Assert.assertFalse(fixture.sink.abort());
      Assert.assertFalse(fixture.sink.close());
      Mockito.verify(listener, Mockito.times(1)).onAborted(fixture.sink);
      Mockito.verify(listener, Mockito.never()).onFinish(fixture.sink);
    }
  }

  @Test
  public void testFrontierThrowingAbortListenerCannotRetainCredit() {
    final FrontierFixture[] holder = new FrontierFixture[1];
    RuntimeException expected = new RuntimeException("abort-listener-failure");
    SinkListener listener = Mockito.mock(SinkListener.class);
    Mockito.doAnswer(
            invocation -> {
              assertFrontierLocksReleased(holder[0]);
              throw expected;
            })
        .when(listener)
        .onAborted(Mockito.any(LocalSinkChannel.class));
    try (FrontierFixture fixture = new FrontierFixture(listener)) {
      holder[0] = fixture;
      fixture.source.isBlocked();
      fixture.sink.send(fixture.block);
      ListenableFuture<?> producer = fixture.sink.isFull();
      ListenableFuture<Void> groupClosed = fixture.reservation.requestClose();

      try {
        fixture.sink.abort();
        Assert.fail("Expected listener failure");
      } catch (RuntimeException e) {
        Assert.assertSame(expected, e);
      }
      Assert.assertTrue(producer.isDone());
      Assert.assertTrue(groupClosed.isDone());
      Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
      Assert.assertEquals(0L, fixture.pool.getReservedBytes());
      Assert.assertTrue(fixture.queue.isEmpty());
      Assert.assertFalse(fixture.sink.abort());
      Mockito.verify(listener, Mockito.times(1)).onAborted(fixture.sink);
    }
  }

  @Test
  public void testFrontierProducerFutureCancellationDoesNotReleaseCredit() {
    try (FrontierFixture fixture = new FrontierFixture(Mockito.mock(SinkListener.class))) {
      fixture.source.isBlocked();
      fixture.sink.send(fixture.block);
      ListenableFuture<?> producer = fixture.sink.isFull();
      Assert.assertTrue(producer.cancel(true));
      Assert.assertEquals(1, fixture.reservation.getLeasedCreditCount());
      Assert.assertEquals(fixture.laneBytes, fixture.pool.getReservedBytes());
      Assert.assertEquals(1, fixture.sink.getNumOfBufferedTsBlocks());
      Assert.assertFalse(fixture.sink.isFull().isDone());

      Assert.assertSame(fixture.block, fixture.source.receive());
      Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
      Assert.assertTrue(fixture.sink.isFull().isDone());
      Assert.assertEquals(fixture.laneBytes, fixture.pool.getReservedBytes());
    }
  }

  @Test
  public void testFrontierProducerWakeupCanReenterSendWithoutLostNotification() {
    try (FrontierFixture fixture = new FrontierFixture(Mockito.mock(SinkListener.class))) {
      fixture.source.isBlocked();
      fixture.sink.send(fixture.block);
      AtomicInteger sends = new AtomicInteger();
      fixture
          .sink
          .isFull()
          .addListener(
              () -> {
                assertFrontierLocksReleased(fixture);
                fixture.sink.send(fixture.block);
                sends.incrementAndGet();
              },
              directExecutor());

      Assert.assertSame(fixture.block, fixture.source.receive());
      Assert.assertEquals(1, sends.get());
      Assert.assertEquals(1, fixture.sink.getNumOfBufferedTsBlocks());
      Assert.assertEquals(1, fixture.reservation.getLeasedCreditCount());
      Assert.assertSame(fixture.block, fixture.source.receive());
      Assert.assertTrue(fixture.queue.isEmpty());
      Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
    }
  }

  @Test
  public void testFrontierSendCloseRaceHasNoLateInsertionOrCreditLeak() throws Exception {
    ExecutorService workers = Executors.newFixedThreadPool(2);
    try {
      for (int run = 0; run < 16; run++) {
        try (FrontierFixture fixture = new FrontierFixture(Mockito.mock(SinkListener.class))) {
          fixture.source.isBlocked();
          CountDownLatch start = new CountDownLatch(1);
          Future<?> send =
              workers.submit(
                  () -> {
                    awaitFrontierStart(start);
                    fixture.sink.send(fixture.block);
                  });
          Future<?> close =
              workers.submit(
                  () -> {
                    awaitFrontierStart(start);
                    fixture.sink.close();
                  });
          start.countDown();
          send.get(10, TimeUnit.SECONDS);
          close.get(10, TimeUnit.SECONDS);

          Assert.assertTrue(fixture.queue.isClosed());
          Assert.assertTrue(fixture.queue.isEmpty());
          Assert.assertEquals(0L, fixture.sink.getBufferRetainedSizeInBytes());
          Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
          Assert.assertEquals(fixture.laneBytes, fixture.pool.getReservedBytes());
          Assert.assertTrue(fixture.reservation.requestClose().isDone());
          Assert.assertEquals(0L, fixture.pool.getReservedBytes());
        }
      }
    } finally {
      workers.shutdownNow();
      Assert.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testFrontierClaimedEofFinishExcludesSubsequentAbortNotification() {
    final FrontierFixture[] holder = new FrontierFixture[1];
    List<String> events = new ArrayList<>();
    SinkListener listener = Mockito.mock(SinkListener.class);
    Mockito.doAnswer(
            invocation -> {
              assertFrontierLocksReleased(holder[0]);
              events.add("end");
              return null;
            })
        .when(listener)
        .onEndOfBlocks(Mockito.any(LocalSinkChannel.class));
    Mockito.doAnswer(
            invocation -> {
              assertFrontierLocksReleased(holder[0]);
              events.add("finish");
              return null;
            })
        .when(listener)
        .onFinish(Mockito.any(LocalSinkChannel.class));

    try (FrontierFixture fixture = new FrontierFixture(listener)) {
      holder[0] = fixture;
      synchronized (fixture.queue) {
        fixture.sink.setNoMoreTsBlocks();
        Assert.assertTrue(events.isEmpty());
        Assert.assertTrue(fixture.sink.abort());
        Assert.assertTrue(events.isEmpty());
      }
      fixture.queue.flushNotifications();

      Assert.assertEquals(Arrays.asList("end", "finish"), events);
      Assert.assertTrue(fixture.queue.isClosed());
      Assert.assertFalse(fixture.sink.abort());
      Mockito.verify(listener, Mockito.times(1)).onFinish(fixture.sink);
      Mockito.verify(listener, Mockito.never()).onAborted(fixture.sink);
    }
  }

  @Test
  public void testFrontierQueueAbortSuppressesAlreadyQueuedEofCompletionCheck() {
    final FrontierFixture[] holder = new FrontierFixture[1];
    AtomicInteger ends = new AtomicInteger();
    SinkListener listener = Mockito.mock(SinkListener.class);
    Mockito.doAnswer(
            invocation -> {
              assertFrontierLocksReleased(holder[0]);
              ends.incrementAndGet();
              return null;
            })
        .when(listener)
        .onEndOfBlocks(Mockito.any(LocalSinkChannel.class));

    try (FrontierFixture fixture = new FrontierFixture(listener)) {
      holder[0] = fixture;
      fixture.source.isBlocked();
      fixture.sink.send(fixture.block);
      synchronized (fixture.queue) {
        fixture.sink.setNoMoreTsBlocks();
        fixture.queue.deferNotification(fixture.sink::checkAndInvokeOnFinished);
        fixture.queue.abort();
        Assert.assertEquals(0, ends.get());
      }
      fixture.queue.flushNotifications();

      Assert.assertEquals(1, ends.get());
      Assert.assertTrue(fixture.queue.isClosed());
      Assert.assertTrue(fixture.queue.isEmpty());
      Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
      fixture.sink.checkAndInvokeOnFinished();
      Mockito.verify(listener, Mockito.never()).onFinish(fixture.sink);
      Mockito.verify(listener, Mockito.never()).onAborted(fixture.sink);
    }
  }

  @Test
  public void testFrontierQueueAbortThenSinkCloseNotifiesAbortAndPreservesFirstCause() {
    for (boolean withCause : new boolean[] {false, true}) {
      final FrontierFixture[] holder = new FrontierFixture[1];
      RuntimeException firstCause = new RuntimeException("first-queue-abort");
      RuntimeException ignoredCause = new RuntimeException("later-sink-listener-cause");
      SinkListener listener = Mockito.mock(SinkListener.class);
      Mockito.doAnswer(
              invocation -> {
                assertFrontierLocksReleased(holder[0]);
                return Optional.of(ignoredCause);
              })
          .when(listener)
          .onAborted(Mockito.any(LocalSinkChannel.class));

      try (FrontierFixture fixture = new FrontierFixture(listener)) {
        holder[0] = fixture;
        fixture.source.isBlocked();
        fixture.sink.send(fixture.block);
        if (withCause) {
          fixture.queue.abort(firstCause);
        } else {
          fixture.queue.abort();
        }
        Assert.assertTrue(fixture.sink.close());
        Assert.assertTrue(fixture.sink.isAborted());
        Assert.assertTrue(fixture.queue.wasFrontierAborted());
        Assert.assertEquals(
            withCause ? Optional.of(firstCause) : Optional.empty(),
            fixture.queue.getAbortedCause());
        Assert.assertFalse(fixture.sink.close());
        Assert.assertFalse(fixture.sink.abort());
        Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
        Mockito.verify(listener, Mockito.times(1)).onAborted(fixture.sink);
        Mockito.verify(listener, Mockito.never()).onFinish(fixture.sink);
      }
    }
  }

  @Test
  public void testFrontierQueueAbortThenSinkAbortStillNotifiesEndpointOnce() {
    final FrontierFixture[] holder = new FrontierFixture[1];
    RuntimeException firstCause = new RuntimeException("first-queue-abort");
    SinkListener listener = Mockito.mock(SinkListener.class);
    Mockito.doAnswer(
            invocation -> {
              assertFrontierLocksReleased(holder[0]);
              return Optional.of(new RuntimeException("ignored-later-cause"));
            })
        .when(listener)
        .onAborted(Mockito.any(LocalSinkChannel.class));

    try (FrontierFixture fixture = new FrontierFixture(listener)) {
      holder[0] = fixture;
      fixture.queue.abort(firstCause);
      Assert.assertTrue(fixture.sink.abort());
      Assert.assertTrue(fixture.sink.isAborted());
      Assert.assertSame(firstCause, fixture.queue.getAbortedCause().get());
      Assert.assertFalse(fixture.sink.abort());
      Assert.assertFalse(fixture.sink.close());
      Mockito.verify(listener, Mockito.times(1)).onAborted(fixture.sink);
      Mockito.verify(listener, Mockito.never()).onFinish(fixture.sink);
    }
  }

  @Test
  public void testFrontierBufferedEofQueueAbortThenSinkCloseKeepsAbortTerminal() {
    final FrontierFixture[] holder = new FrontierFixture[1];
    List<String> events = new ArrayList<>();
    SinkListener listener = Mockito.mock(SinkListener.class);
    Mockito.doAnswer(
            invocation -> {
              assertFrontierLocksReleased(holder[0]);
              events.add("end");
              return null;
            })
        .when(listener)
        .onEndOfBlocks(Mockito.any(LocalSinkChannel.class));
    Mockito.doAnswer(
            invocation -> {
              assertFrontierLocksReleased(holder[0]);
              events.add("abort");
              return Optional.empty();
            })
        .when(listener)
        .onAborted(Mockito.any(LocalSinkChannel.class));

    try (FrontierFixture fixture = new FrontierFixture(listener)) {
      holder[0] = fixture;
      fixture.source.isBlocked();
      fixture.sink.send(fixture.block);
      synchronized (fixture.queue) {
        fixture.sink.setNoMoreTsBlocks();
        fixture.queue.deferNotification(fixture.sink::checkAndInvokeOnFinished);
        fixture.queue.abort();
        Assert.assertTrue(fixture.sink.close());
        Assert.assertTrue(events.isEmpty());
      }
      fixture.queue.flushNotifications();

      Assert.assertEquals(Arrays.asList("end", "abort"), events);
      Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
      fixture.sink.checkAndInvokeOnFinished();
      Assert.assertFalse(fixture.sink.abort());
      Mockito.verify(listener, Mockito.times(1)).onAborted(fixture.sink);
      Mockito.verify(listener, Mockito.never()).onFinish(fixture.sink);
    }
  }

  @Test
  public void testFrontierNormalQueueCloseWinsAgainstPendingSinkAbort() {
    final FrontierFixture[] holder = new FrontierFixture[1];
    SinkListener listener = Mockito.mock(SinkListener.class);
    Mockito.doAnswer(
            invocation -> {
              assertFrontierLocksReleased(holder[0]);
              return null;
            })
        .when(listener)
        .onFinish(Mockito.any(LocalSinkChannel.class));

    try (FrontierFixture fixture = new FrontierFixture(listener)) {
      holder[0] = fixture;
      synchronized (fixture.queue) {
        fixture.queue.close();
        Assert.assertTrue(fixture.sink.abort());
        Mockito.verify(listener, Mockito.never()).onFinish(fixture.sink);
      }
      fixture.queue.flushNotifications();

      Assert.assertFalse(fixture.queue.wasFrontierAborted());
      Assert.assertFalse(fixture.sink.isAborted());
      Assert.assertFalse(fixture.sink.close());
      Assert.assertFalse(fixture.sink.abort());
      Mockito.verify(listener, Mockito.times(1)).onFinish(fixture.sink);
      Mockito.verify(listener, Mockito.never()).onAborted(fixture.sink);
    }
  }

  @Test
  public void testFrontierEofCloseRaceHasOneOrderedTerminalNotification() throws Exception {
    ExecutorService workers = Executors.newFixedThreadPool(2);
    try {
      for (int run = 0; run < 16; run++) {
        final FrontierFixture[] holder = new FrontierFixture[1];
        List<String> events = new ArrayList<>();
        SinkListener listener = Mockito.mock(SinkListener.class);
        Mockito.doAnswer(
                invocation -> {
                  assertFrontierLocksReleased(holder[0]);
                  events.add("end");
                  return null;
                })
            .when(listener)
            .onEndOfBlocks(Mockito.any(LocalSinkChannel.class));
        Mockito.doAnswer(
                invocation -> {
                  assertFrontierLocksReleased(holder[0]);
                  events.add("finish");
                  return null;
                })
            .when(listener)
            .onFinish(Mockito.any(LocalSinkChannel.class));

        try (FrontierFixture fixture = new FrontierFixture(listener)) {
          holder[0] = fixture;
          CountDownLatch start = new CountDownLatch(1);
          Future<?> eof =
              workers.submit(
                  () -> {
                    awaitFrontierStart(start);
                    fixture.sink.setNoMoreTsBlocks();
                  });
          Future<?> close =
              workers.submit(
                  () -> {
                    awaitFrontierStart(start);
                    fixture.sink.close();
                  });
          start.countDown();
          eof.get(10, TimeUnit.SECONDS);
          close.get(10, TimeUnit.SECONDS);

          Assert.assertTrue(
              events.equals(Collections.singletonList("finish"))
                  || events.equals(Arrays.asList("end", "finish")));
          Mockito.verify(listener, Mockito.times(1)).onFinish(fixture.sink);
          Mockito.verify(listener, Mockito.never()).onAborted(fixture.sink);
          Assert.assertTrue(fixture.queue.isClosed());
          Assert.assertFalse(fixture.queue.wasFrontierAborted());
          Assert.assertEquals(0, fixture.reservation.getLeasedCreditCount());
        }
      }
    } finally {
      workers.shutdownNow();
      Assert.assertTrue(workers.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void testFrontierLateEofDoesNotOverrideEarlierQueueAbort() {
    final FrontierFixture[] holder = new FrontierFixture[1];
    SinkListener listener = Mockito.mock(SinkListener.class);
    Mockito.doAnswer(
            invocation -> {
              assertFrontierLocksReleased(holder[0]);
              return Optional.empty();
            })
        .when(listener)
        .onAborted(Mockito.any(LocalSinkChannel.class));

    try (FrontierFixture fixture = new FrontierFixture(listener)) {
      holder[0] = fixture;
      fixture.queue.abort();
      fixture.sink.setNoMoreTsBlocks();
      fixture.sink.checkAndInvokeOnFinished();
      Mockito.verify(listener, Mockito.never()).onEndOfBlocks(fixture.sink);
      Mockito.verify(listener, Mockito.never()).onFinish(fixture.sink);
      Assert.assertTrue(fixture.sink.close());
      Mockito.verify(listener, Mockito.times(1)).onAborted(fixture.sink);
      Assert.assertTrue(fixture.sink.isAborted());
    }
  }

  @Test
  public void testFrontierBothConstructorsPublishInitializedBlockedFuture() {
    for (boolean withFragmentInstanceId : new boolean[] {false, true}) {
      try (FrontierFixture fixture = new FrontierFixture(Mockito.mock(SinkListener.class))) {
        // The spy retains the real reservation's original queue owner, so this test only constructs
        // the channel and observes its future. It deliberately does not add a block through the
        // spy.
        SharedTsBlockQueue publishingQueue = Mockito.spy(fixture.queue);
        AtomicInteger publications = new AtomicInteger();
        Mockito.doAnswer(
                invocation -> {
                  invocation.callRealMethod();
                  LocalSinkChannel published = invocation.getArgument(0);
                  Assert.assertNotNull(published.isFull());
                  publications.incrementAndGet();
                  return null;
                })
            .when(publishingQueue)
            .setSinkChannel(Mockito.any(LocalSinkChannel.class));

        SinkListener listener = Mockito.mock(SinkListener.class);
        LocalSinkChannel channel =
            withFragmentInstanceId
                ? new LocalSinkChannel(
                    new TFragmentInstanceId("frontier_sink", 0, "0"), publishingQueue, listener)
                : new LocalSinkChannel(publishingQueue, listener);
        Assert.assertEquals(1, publications.get());
        Assert.assertNotNull(channel.isFull());
        Mockito.verify(publishingQueue, Mockito.times(1)).setSinkChannel(channel);
      }
    }
  }

  private static void awaitFrontierStart(CountDownLatch start) {
    try {
      if (!start.await(10, TimeUnit.SECONDS)) {
        throw new AssertionError("Concurrent frontier test did not start");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static void assertFrontierLocksReleased(FrontierFixture fixture) {
    Assert.assertFalse("queue monitor held by callback", Thread.holdsLock(fixture.queue));
    Assert.assertFalse("sink monitor held by callback", Thread.holdsLock(fixture.sink));
    Assert.assertFalse("source monitor held by callback", Thread.holdsLock(fixture.source));
  }

  private static final class FrontierFixture implements AutoCloseable {
    private final TsBlock block;
    private final long laneBytes;
    private final MemoryPool pool;
    private final RuntimeFrontierReservation reservation;
    private final SharedTsBlockQueue queue;
    private final LocalSinkChannel sink;
    private final LocalSourceHandle source;

    private FrontierFixture(SinkListener listener) {
      TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT64));
      builder.getTimeColumnBuilder().writeLong(1L);
      builder.getColumnBuilder(0).writeLong(11L);
      builder.declarePosition();
      block = builder.build();
      laneBytes = block.getRetainedSizeInBytes();
      MemoryManager memory = new MemoryManager(4 * laneBytes);
      pool = new MemoryPool("frontier-sink-test", memory, 4 * laneBytes);
      LocalMemoryManager localMemory = Mockito.mock(LocalMemoryManager.class);
      Mockito.when(localMemory.getQueryPool()).thenReturn(pool);
      TFragmentInstanceId id = new TFragmentInstanceId("frontier_sink", 0, "0");
      SharedTsBlockQueue.MemoryReservationKey groupKey =
          new SharedTsBlockQueue.MemoryReservationKey(
              id.getQueryId(),
              FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(id),
              "frontier-group");
      reservation =
          RuntimeFrontierReservation.tryCreate(pool, groupKey, 1, laneBytes, 4 * laneBytes).get();
      queue =
          new SharedTsBlockQueue(
              id, "frontier-lane", localMemory, newDirectExecutorService(), false, reservation, 0);
      sink = new LocalSinkChannel(id, queue, listener);
      source =
          new LocalSourceHandle(
              id,
              "frontier-lane",
              queue,
              Mockito.mock(MPPDataExchangeManager.SourceHandleListener.class));
      Assert.assertEquals(laneBytes, pool.getReservedBytes());
    }

    @Override
    public void close() {
      queue.close();
      queue.flushNotifications();
      Assert.assertTrue(reservation.requestClose().isDone());
      Assert.assertEquals(0, reservation.getLeasedCreditCount());
      Assert.assertEquals(0L, pool.getReservedBytes());
    }
  }
}
