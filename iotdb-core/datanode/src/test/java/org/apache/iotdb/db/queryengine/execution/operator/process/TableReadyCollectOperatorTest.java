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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.execution.operator.process.CollectOperator;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TableReadyCollectOperatorTest {
  public static class StubOperator implements Operator {
    @Override
    public com.google.common.util.concurrent.ListenableFuture<?> isBlocked() {
      return NOT_BLOCKED;
    }

    private final CommonOperatorContext context = mock(CommonOperatorContext.class);

    @Override
    public CommonOperatorContext getOperatorContext() {
      return context;
    }

    @Override
    public TsBlock next() throws Exception {
      return null;
    }

    @Override
    public boolean hasNext() throws Exception {
      return false;
    }

    @Override
    public void close() throws Exception {}

    @Override
    public boolean isFinished() throws Exception {
      return !hasNext();
    }

    @Override
    public long calculateMaxPeekMemory() {
      return 0;
    }

    @Override
    public long calculateMaxReturnSize() {
      return 0;
    }

    @Override
    public long calculateRetainedSizeAfterCallingNext() {
      return 0;
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }

    @Override
    public TsBlock nextWithTimer() throws Exception {
      return Operator.super.nextWithTimer();
    }

    @Override
    public boolean hasNextWithTimer() throws Exception {
      return Operator.super.hasNextWithTimer();
    }

    @Override
    public long calculateMaxPeekMemoryWithCounter() {
      return Operator.super.calculateMaxPeekMemoryWithCounter();
    }
  }

  private static CommonOperatorContext context() {
    return mock(CommonOperatorContext.class);
  }

  private static Operator child(ListenableFuture<?> blocked, TsBlock... input) throws Exception {
    Operator child = spy(new StubOperator());
    Queue<TsBlock> blocks = new LinkedList<>(Arrays.asList(input));
    when(child.getOperatorContext()).thenReturn(context());
    when(child.isBlocked()).thenReturn((ListenableFuture) blocked);
    when(child.hasNext()).thenAnswer(invocation -> !blocks.isEmpty());
    when(child.next()).thenAnswer(invocation -> blocks.remove());
    return child;
  }

  private static TsBlock block(Long value) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT64));
    builder.getTimeColumnBuilder().writeLong(0);
    if (value == null) builder.getColumnBuilder(0).appendNull();
    else builder.getColumnBuilder(0).writeLong(value);
    builder.declarePosition();
    return builder.build();
  }

  @Test
  public void readyChildrenAreRoundRobinAndPreserveNullsAndDuplicates() throws Exception {
    Operator left = child(Operator.NOT_BLOCKED, block(1L), block(null), block(1L));
    Operator right = child(Operator.NOT_BLOCKED, block(2L), block(2L), block(3L));
    TableReadyCollectOperator collect =
        new TableReadyCollectOperator(context(), Arrays.asList(left, right));
    List<Long> actual = new ArrayList<>();
    for (int bounded = 0; collect.hasNext() && bounded < 12; bounded++) {
      assertTrue(collect.isBlocked().isDone());
      TsBlock output = collect.next();
      if (output != null) {
        actual.add(output.getColumn(0).isNull(0) ? null : output.getColumn(0).getLong(0));
      }
    }
    assertEquals(Arrays.asList(1L, 2L, null, 2L, 1L, 3L), actual);
    assertTrue(collect.isFinished());
    collect.close();
    verify(left, times(1)).close();
    verify(right, times(1)).close();
  }

  @Test
  public void anyChildCompletionWakesDriverWithoutCallingOperatorsFromListener() throws Exception {
    SettableFuture<Void> first = SettableFuture.create();
    SettableFuture<Void> second = SettableFuture.create();
    Operator left = child(first);
    Operator right = child(second);
    TableReadyCollectOperator collect =
        new TableReadyCollectOperator(context(), Arrays.asList(left, right));
    ListenableFuture<?> wait = collect.isBlocked();
    assertFalse(wait.isDone());
    second.set(null);
    assertTrue(wait.isDone());
    assertFalse(first.isDone());
    verify(left, times(1)).isBlocked();
    verify(right, times(1)).isBlocked();
    verify(left, never()).hasNext();
    verify(right, never()).hasNext();
    collect.close();
  }

  @Test
  public void cancellingWaitViewDoesNotCancelChildrenOrPoisonNextWait() throws Exception {
    SettableFuture<Void> future = SettableFuture.create();
    Operator input = child(future);
    TableReadyCollectOperator collect =
        new TableReadyCollectOperator(context(), Collections.singletonList(input));
    ListenableFuture<?> first = collect.isBlocked();
    assertTrue(first.cancel(true));
    assertFalse(future.isCancelled());
    ListenableFuture<?> second = collect.isBlocked();
    assertNotSame(first, second);
    assertFalse(second.isDone());
    future.set(null);
    assertTrue(second.isDone());
    assertFalse(second.isCancelled());
    collect.close();
  }

  private static final class CountingFuture
      extends ForwardingListenableFuture.SimpleForwardingListenableFuture<Void> {
    private int listeners;

    CountingFuture() {
      super(SettableFuture.create());
    }

    @Override
    public void addListener(Runnable listener, java.util.concurrent.Executor executor) {
      listeners++;
      super.addListener(listener, executor);
    }
  }

  @Test
  public void longIdleInputKeepsOneListenerAcrossManyWakeupGenerations() throws Exception {
    CountingFuture idle = new CountingFuture();
    Operator left = child(idle);
    Operator right = child(Operator.NOT_BLOCKED);
    SettableFuture<Void> busy = SettableFuture.create();
    doReturn(busy).when(right).isBlocked();
    TableReadyCollectOperator collect =
        new TableReadyCollectOperator(context(), Arrays.asList(left, right));
    for (int generation = 0; generation < 200; generation++) {
      ListenableFuture<?> wait = collect.isBlocked();
      assertFalse(wait.isDone());
      busy.set(null);
      assertTrue(wait.isDone());
      busy = SettableFuture.create();
      doReturn(busy).when(right).isBlocked();
    }
    assertEquals(1, idle.listeners);
    collect.close();
  }

  private static final class CompletesAfterObservationFuture
      extends ForwardingListenableFuture.SimpleForwardingListenableFuture<Void> {
    private final SettableFuture<Void> delegate;
    private int completeAfterObservations = -1;

    CompletesAfterObservationFuture() {
      this(SettableFuture.create());
    }

    private CompletesAfterObservationFuture(SettableFuture<Void> delegate) {
      super(delegate);
      this.delegate = delegate;
    }

    @Override
    public boolean isDone() {
      boolean observed = super.isDone();
      if (!observed && completeAfterObservations > 0 && --completeAfterObservations == 0) {
        // Completion occurs immediately after the caller observes false.
        delegate.set(null);
      }
      return observed;
    }
  }

  @Test
  public void completionBetweenProbeAndWakeupPublicationCannotBeLost() throws Exception {
    CompletesAfterObservationFuture idle = new CompletesAfterObservationFuture();
    Operator left = child(idle);
    SettableFuture<Void> firstBusy = SettableFuture.create();
    Operator right = child(firstBusy);
    TableReadyCollectOperator collect =
        new TableReadyCollectOperator(context(), Arrays.asList(left, right));
    ListenableFuture<?> initialWait = collect.isBlocked();
    firstBusy.set(null);
    assertTrue(initialWait.isDone());
    SettableFuture<Void> nextBusy = SettableFuture.create();
    doReturn(nextBusy).when(right).isBlocked();
    // The remembered pending check and final readiness check are two observations.
    idle.completeAfterObservations = 2;
    ListenableFuture<?> nextWait = collect.isBlocked();
    assertTrue(
        "A completion sent to the previous wakeup must also wake this generation",
        nextWait.isDone());
    assertFalse(nextBusy.isDone());
    collect.close();
  }

  @Test
  public void successfulWakeupCanBeFollowedByANewBlockedFuture() throws Exception {
    SettableFuture<Void> first = SettableFuture.create();
    SettableFuture<Void> next = SettableFuture.create();
    Operator input = child(first);
    doReturn(first, next).when(input).isBlocked();
    TableReadyCollectOperator collect =
        new TableReadyCollectOperator(context(), Collections.singletonList(input));
    ListenableFuture<?> initialWait = collect.isBlocked();
    first.set(null);
    assertTrue(initialWait.isDone());
    ListenableFuture<?> nextWait = collect.isBlocked();
    assertFalse(nextWait.isDone());
    next.set(null);
    assertTrue(nextWait.isDone());
    collect.close();
  }

  @Test
  public void closingPendingCollectWakesWaiterWithoutCancellingChild() throws Exception {
    SettableFuture<Void> pending = SettableFuture.create();
    Operator input = child(pending);
    TableReadyCollectOperator collect =
        new TableReadyCollectOperator(context(), Collections.singletonList(input));
    ListenableFuture<?> wait = collect.isBlocked();
    collect.close();
    assertTrue(wait.isDone());
    assertFalse(pending.isCancelled());
    assertTrue(collect.isFinished());
    collect.close();
    verify(input, times(1)).close();
  }

  @Test
  public void failedRememberedFutureIsThrownOnDriverEvenWithAnotherReadyChild() throws Exception {
    SettableFuture<Void> failed = SettableFuture.create();
    Operator input = child(failed);
    TableReadyCollectOperator collect =
        new TableReadyCollectOperator(context(), Arrays.asList(input, child(Operator.NOT_BLOCKED)));
    collect.isBlocked();
    IOException cause = new IOException("real child failure");
    failed.setException(cause);
    try {
      collect.isBlocked();
      fail("Failed future must not become an EOF or ordinary ready result");
    } catch (IllegalStateException expected) {
      assertSame(cause, expected.getCause());
    } finally {
      collect.close();
    }
    verify(input, never()).hasNext();
  }

  @Test
  public void nextAlsoPropagatesCancelledFutureBeforeAnyConsumption() throws Exception {
    SettableFuture<Void> cancelled = SettableFuture.create();
    Operator input = child(cancelled);
    TableReadyCollectOperator collect =
        new TableReadyCollectOperator(context(), Arrays.asList(child(Operator.NOT_BLOCKED), input));
    collect.isBlocked();
    cancelled.cancel(false);
    try {
      collect.next();
      fail("Cancellation must reach the driver");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getCause() instanceof java.util.concurrent.CancellationException);
    } finally {
      collect.close();
    }
    verify(input, never()).hasNext();
  }

  @Test
  public void closeAggregatesFailuresAndIsIdempotent() throws Exception {
    Operator first = child(Operator.NOT_BLOCKED);
    Operator second = child(Operator.NOT_BLOCKED);
    Operator third = child(Operator.NOT_BLOCKED);
    IOException one = new IOException("one");
    IOException two = new IOException("two");
    doThrow(one).when(first).close();
    doThrow(two).when(second).close();
    TableReadyCollectOperator collect =
        new TableReadyCollectOperator(context(), Arrays.asList(first, second, third));
    try {
      collect.close();
      fail("Expected close failure");
    } catch (IOException expected) {
      assertSame(one, expected);
      assertArrayEquals(new Throwable[] {two}, expected.getSuppressed());
    }
    collect.close();
    assertTrue(collect.isFinished());
    verify(first, times(1)).close();
    verify(second, times(1)).close();
    verify(third, times(1)).close();
  }

  @Test
  public void emptyCollectFinishesAndEofCloseFailureDoesNotLeaveLiveChild() throws Exception {
    TableReadyCollectOperator empty =
        new TableReadyCollectOperator(context(), Collections.emptyList());
    assertTrue(empty.isFinished());
    assertFalse(empty.hasNext());
    assertTrue(empty.isBlocked().isDone());
    assertNull(empty.next());
    empty.close();
    Operator input = child(Operator.NOT_BLOCKED);
    doThrow(new IOException("eof close")).when(input).close();
    TableReadyCollectOperator collect =
        new TableReadyCollectOperator(context(), Collections.singletonList(input));
    try {
      collect.next();
      fail("Expected EOF close failure");
    } catch (IOException expected) {
      assertEquals("eof close", expected.getMessage());
    }
    assertTrue(collect.isFinished());
    collect.close();
    verify(input, times(1)).close();
  }

  @Test
  public void memoryIncludesAllRetainedChildrenAndRejectsOverflow() throws Exception {
    Operator left = child(Operator.NOT_BLOCKED);
    Operator right = child(Operator.NOT_BLOCKED);
    when(left.calculateRetainedSizeAfterCallingNext()).thenReturn(10L);
    when(right.calculateRetainedSizeAfterCallingNext()).thenReturn(20L);
    when(left.calculateMaxPeekMemory()).thenReturn(100L);
    when(right.calculateMaxPeekMemory()).thenReturn(70L);
    when(left.calculateMaxReturnSize()).thenReturn(8L);
    when(right.calculateMaxReturnSize()).thenReturn(12L);
    TableReadyCollectOperator collect =
        new TableReadyCollectOperator(context(), Arrays.asList(left, right));
    assertEquals(30, collect.calculateRetainedSizeAfterCallingNext());
    assertEquals(12, collect.calculateMaxReturnSize());
    assertEquals(120, collect.calculateMaxPeekMemory());
    when(left.calculateRetainedSizeAfterCallingNext()).thenReturn(Long.MAX_VALUE);
    try {
      collect.calculateMaxPeekMemory();
      fail("Overflow must reject planning");
    } catch (ArithmeticException expected) {
      // Fail closed before admission.
    }
    when(left.calculateRetainedSizeAfterCallingNext()).thenReturn(-1L);
    try {
      collect.calculateRetainedSizeAfterCallingNext();
      fail("Negative estimate must reject planning");
    } catch (IllegalArgumentException expected) {
      // Fail closed before admission.
    }
    collect.close();
  }

  private static ExchangeOperator exchange(SharedTsBlockQueue queue, String id) {
    LocalSourceHandle source =
        new LocalSourceHandle(
            queue, mock(MPPDataExchangeManager.SourceHandleListener.class), "table-ready-test");
    OperatorContext context = mock(OperatorContext.class);
    when(context.getSpecifiedInfo()).thenReturn(new HashMap<>());
    return new ExchangeOperator(context, source, new PlanNodeId(id));
  }

  @Test
  public void realBoundedQueuePoolCycleIsBlockedSequentiallyAndReleasedByReadyFanIn()
      throws Exception {
    TsBlock payload = block(7L);
    long bytes = payload.getSizeInBytes();
    String query = "table_ready_real_pool";
    TFragmentInstanceId id = new TFragmentInstanceId(query, 0, "0");
    MemoryPool pool = new MemoryPool(query, new MemoryManager(bytes * 2), bytes);
    LocalMemoryManager memory = mock(LocalMemoryManager.class);
    when(memory.getQueryPool()).thenReturn(pool);
    ExecutorService executor = MoreExecutors.newDirectExecutorService();
    SharedTsBlockQueue first = new SharedTsBlockQueue(id, "same_source", memory, executor);
    SharedTsBlockQueue second = new SharedTsBlockQueue(id, "same_source", memory, executor);
    TableReadyCollectOperator fair = null;
    try {
      first.setMaxBytesCanReserve(bytes);
      second.setMaxBytesCanReserve(bytes);
      first.allowAddingTsBlock();
      second.allowAddingTsBlock();
      assertTrue(second.add(payload).isDone());
      second.setNoMoreTsBlocks(true);
      ListenableFuture<?> pendingReservation = first.add(payload);
      assertFalse(pendingReservation.isDone());
      assertEquals(1, pool.getMemoryReservationSize());
      Operator left = exchange(first, "first");
      Operator right = exchange(second, "second");
      // A sequential consumer waits only for its first input. The upstream CollectOperator
      // now implements ready fan-in too, so it must not serve as the sequential control.
      ListenableFuture<?> blocked = left.isBlocked();
      try {
        blocked.get(20, TimeUnit.MILLISECONDS);
        fail("Sequential fan-in must remain blocked in this bounded real-pool cycle");
      } catch (TimeoutException expected) {
        // The ready second child owns the shared account; the first cannot reserve until it drains.
      }
      assertEquals(bytes, pool.snapshotFragmentReservations(query, "0.0").fragmentReservedBytes());
      CollectOperator nativeCollect =
          new CollectOperator(context(), new ArrayList<>(Arrays.asList(left, right)));
      assertTrue(
          "Upstream Collect must also observe the ready second input",
          nativeCollect.isBlocked().isDone());
      fair = new TableReadyCollectOperator(context(), Arrays.asList(left, right));
      assertTrue(fair.isBlocked().isDone());
      assertSame(payload, fair.next());
      assertTrue(pendingReservation.isDone());
      first.setNoMoreTsBlocks(true);
      assertSame(payload, fair.next());
      for (int i = 0; fair.hasNext() && i < 4; i++) {
        assertTrue(fair.isBlocked().isDone());
        assertNull(fair.next());
      }
      assertTrue(fair.isFinished());
      assertEquals(0, pool.getMemoryReservationSize());
      assertEquals(0L, pool.snapshotFragmentReservations(query, "0.0").fragmentReservedBytes());
    } finally {
      if (fair != null) fair.close();
      first.close();
      second.close();
      pool.deRegisterFragmentInstanceFromQueryMemoryMap(query, "0.0", false);
      assertEquals(0, pool.getQueryMemoryReservationSize());
      executor.shutdownNow();
    }
  }
}
