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

package org.apache.iotdb.db.queryengine.execution.runtime;

import org.apache.iotdb.db.queryengine.execution.driver.IDriver;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class RuntimeScanGroupTest {
  @Test
  public void rejectsInvalidDependenciesAndRepeatedDriverIdentity() {
    IDriver driver = mock(IDriver.class);
    expect(NullPointerException.class, () -> new RuntimeScanGroup(null));
    expect(NullPointerException.class, () -> new RuntimeScanGroup(List.of(driver), null));
    expect(
        IllegalArgumentException.class,
        () -> new RuntimeScanGroup(List.of(driver), Collections.emptyList()));
    expect(NullPointerException.class, () -> new RuntimeScanGroup(Arrays.asList((IDriver) null)));
    expect(
        NullPointerException.class,
        () -> new RuntimeScanGroup(List.of(driver), Arrays.asList((ListenableFuture<?>) null)));
    expect(IllegalArgumentException.class, () -> new RuntimeScanGroup(List.of(driver, driver)));
  }

  @Test
  public void emptyGroupNaturallyFinishesButQuiescenceRequiresStop() {
    RuntimeScanGroup group = new RuntimeScanGroup(Collections.emptyList());
    assertTrue(group.isFinished());
    assertFalse(group.isStopped());
    assertEquals(0, group.size());
    assertFalse(group.poll(1).getLease().isPresent());
    assertTrue(group.poll(1).getReadyFuture().isDone());
    assertFalse(group.getQuiescenceFuture().isDone());
    group.requestStop();
    assertTrue(group.getQuiescenceFuture().isDone());
  }

  @Test
  public void consumerCloseDependencyControlsEligibility() {
    IDriver driver = mock(IDriver.class);
    SettableFuture<Void> consumerClosed = SettableFuture.create();
    RuntimeScanGroup group = new RuntimeScanGroup(List.of(driver), List.of(consumerClosed));
    RuntimeScanGroup.PollResult waiting = group.poll(7);
    assertFalse(waiting.getLease().isPresent());
    assertFalse(waiting.getReadyFuture().isDone());
    assertEquals(0, group.getReadyCount());
    verifyZeroInteractions(driver);
    consumerClosed.set(null);
    assertTrue(waiting.getReadyFuture().isDone());
    RuntimeLaneLease lease = lease(group, 7);
    assertSame(driver, lease.getDriver());
    assertEquals(0, lease.getLaneIndex());
    assertEquals(7, lease.getWorkerId());
    assertEquals(1, lease.getGeneration());
    group.release(lease, null, true);
    assertTrue(group.isFinished());
    verifyZeroInteractions(driver);
  }

  @Test
  public void oneWorkerContinuesOtherLanesWhileFirstIsBackpressured() {
    RuntimeScanGroup group = group(3);
    SettableFuture<Void> blocked = SettableFuture.create();
    RuntimeLaneLease first = lease(group, 1);
    assertEquals(0, first.getLaneIndex());
    group.release(first, blocked, false);
    assertEquals(0, group.getLeasedCount());
    RuntimeLaneLease second = lease(group, 1);
    assertEquals(1, second.getLaneIndex());
    group.release(second, null, true);
    RuntimeLaneLease third = lease(group, 1);
    assertEquals(2, third.getLaneIndex());
    group.release(third, null, true);
    RuntimeScanGroup.PollResult waiting = group.poll(1);
    assertFalse(waiting.getLease().isPresent());
    assertFalse(waiting.getReadyFuture().isDone());
    blocked.set(null);
    assertTrue(waiting.getReadyFuture().isDone());
    RuntimeLaneLease resumed = lease(group, 1);
    assertEquals(0, resumed.getLaneIndex());
    assertTrue(resumed.getGeneration() > first.getGeneration());
    group.release(resumed, null, true);
    assertTrue(group.isFinished());
  }

  @Test
  public void oneWorkerCannotHoldTwoExecutionLeases() {
    RuntimeScanGroup group = group(2);
    RuntimeLaneLease first = lease(group, 1);
    expect(IllegalStateException.class, () -> group.poll(1));
    RuntimeLaneLease second = lease(group, 2);
    assertTrue(first.getLaneIndex() != second.getLaneIndex());
    group.release(first, null, false);
    group.release(second, null, false);
    assertEquals(2, group.getReadyCount());
    expect(IllegalArgumentException.class, () -> group.poll(-1));
  }

  @Test
  public void concurrentPollersNeverExceedLogicalWidthOrShareALane() throws Exception {
    RuntimeScanGroup group = group(3);
    ExecutorService executor = Executors.newFixedThreadPool(8);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch polled = new CountDownLatch(8);
    CountDownLatch returnLeases = new CountDownLatch(1);
    List<Future<Integer>> results = new ArrayList<>();
    try {
      for (int i = 0; i < 8; i++) {
        final long workerId = i;
        results.add(
            executor.submit(
                () -> {
                  assertTrue(start.await(5, TimeUnit.SECONDS));
                  Optional<RuntimeLaneLease> acquired = group.poll(workerId).getLease();
                  polled.countDown();
                  if (!acquired.isPresent()) {
                    return -1;
                  }
                  assertTrue(returnLeases.await(5, TimeUnit.SECONDS));
                  group.release(acquired.get(), null, false);
                  return acquired.get().getLaneIndex();
                }));
      }
      start.countDown();
      assertTrue(polled.await(5, TimeUnit.SECONDS));
      assertEquals(3, group.getLeasedCount());
      assertEquals(0, group.getReadyCount());
      returnLeases.countDown();
      Set<Integer> indices = new HashSet<>();
      int acquired = 0;
      for (Future<Integer> result : results) {
        int index = result.get(5, TimeUnit.SECONDS);
        if (index >= 0) {
          acquired++;
          assertTrue(indices.add(index));
        }
      }
      assertEquals(3, acquired);
      assertEquals(Set.of(0, 1, 2), indices);
      assertEquals(0, group.getLeasedCount());
      assertEquals(3, group.getReadyCount());
    } finally {
      returnLeases.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void stopWaitsForActiveLeasesAndNeverClosesLogicalDrivers() {
    IDriver driver = mock(IDriver.class);
    RuntimeScanGroup group = new RuntimeScanGroup(List.of(driver));
    RuntimeLaneLease active = lease(group, 1);
    ListenableFuture<?> drained = group.getQuiescenceFuture();
    group.requestStop();
    assertTrue(group.isStopped());
    assertFalse(drained.isDone());
    assertFalse(group.isFinished());
    assertFalse(group.poll(2).getLease().isPresent());
    assertTrue(group.poll(2).getReadyFuture().isDone());
    group.release(active, null, false);
    assertTrue(drained.isDone());
    assertEquals(0, group.getLeasedCount());
    assertEquals(0, group.getReadyCount());
    assertFalse(group.isFinished());
    group.requestStop();
    group.release(active, null, true);
    assertFalse(group.isFinished());
    verify(driver, never()).close();
    verifyZeroInteractions(driver);
  }

  @Test
  public void duplicateReleaseCannotModifyNewLease() {
    RuntimeScanGroup group = group(1);
    RuntimeLaneLease first = lease(group, 1);
    group.release(first, null, false);
    RuntimeLaneLease next = lease(group, 2);
    assertTrue(next.getGeneration() > first.getGeneration());
    group.release(first, null, true);
    assertEquals(1, group.getLeasedCount());
    assertEquals(0, group.getReadyCount());
    assertFalse(group.isFinished());
    group.release(next, null, true);
    assertTrue(group.isFinished());
  }

  @Test
  public void leaseFromAnotherGroupCannotReleaseSameIndexedLane() {
    RuntimeScanGroup first = group(1);
    RuntimeScanGroup second = group(1);
    RuntimeLaneLease a = lease(first, 5);
    RuntimeLaneLease b = lease(second, 5);
    assertEquals(a.getLaneIndex(), b.getLaneIndex());
    assertEquals(a.getGeneration(), b.getGeneration());
    expect(IllegalArgumentException.class, () -> first.release(b, null, true));
    assertEquals(1, first.getLeasedCount());
    assertEquals(1, second.getLeasedCount());
    first.release(a, null, true);
    second.release(b, null, true);
  }

  @Test
  public void staleBlockedListenerCannotMakeANewerGenerationReady() {
    RuntimeScanGroup group = group(1);
    AtomicReference<Runnable> oldCallback = new AtomicReference<>();
    ListenableFuture<?> oldBlocked = callbackFuture(oldCallback);
    RuntimeLaneLease first = lease(group, 1);
    group.release(first, oldBlocked, false);
    oldCallback.get().run();
    RuntimeLaneLease newer = lease(group, 2);
    SettableFuture<Void> newBlocked = SettableFuture.create();
    group.release(newer, newBlocked, false);
    oldCallback.get().run();
    assertEquals(0, group.getReadyCount());
    assertFalse(group.poll(3).getReadyFuture().isDone());
    newBlocked.set(null);
    assertEquals(1, group.getReadyCount());
    assertTrue(group.poll(3).getLease().isPresent());
  }

  @Test
  public void resizeWakeDoesNotMakeBlockedLaneReadyOrPoisonOtherWaiters() {
    RuntimeScanGroup group = group(1);
    RuntimeLaneLease first = lease(group, 1);
    SettableFuture<Void> blocked = SettableFuture.create();
    group.release(first, blocked, false);
    ListenableFuture<?> cancelledView = group.poll(2).getReadyFuture();
    ListenableFuture<?> otherView = group.poll(3).getReadyFuture();
    assertNotSame(cancelledView, otherView);
    cancelledView.cancel(false);
    assertTrue(cancelledView.isCancelled());
    assertFalse(otherView.isDone());
    group.wakeUp();
    assertTrue(otherView.isDone());
    RuntimeScanGroup.PollResult stillBlocked = group.poll(2);
    assertFalse(stillBlocked.getLease().isPresent());
    assertFalse(stillBlocked.getReadyFuture().isDone());
    blocked.set(null);
    assertTrue(stillBlocked.getReadyFuture().isDone());
    assertTrue(group.poll(3).getLease().isPresent());
  }

  @Test
  public void alreadyCompletedBlockedFutureReturnsLaneImmediately() {
    RuntimeScanGroup group = group(1);
    RuntimeLaneLease first = lease(group, 1);
    group.release(first, Futures.immediateVoidFuture(), false);
    assertEquals(0, group.getLeasedCount());
    assertEquals(1, group.getReadyCount());
    assertTrue(group.poll(2).getLease().isPresent());
  }

  @Test
  public void cancelledAndFailedBlockedFuturesPreserveTheirFailure() throws Exception {
    RuntimeScanGroup cancelledGroup = group(1);
    SettableFuture<Void> cancelled = SettableFuture.create();
    cancelledGroup.release(lease(cancelledGroup, 1), cancelled, false);
    cancelled.cancel(false);
    assertEquals(0, cancelledGroup.getReadyCount());
    assertTrue(cancelled.isCancelled());
    try {
      cancelledGroup.getFailureFuture().get(5, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException expected) {
      assertTrue(expected.getCause() instanceof CancellationException);
    }
    assertFalse(cancelledGroup.poll(2).getLease().isPresent());

    RuntimeScanGroup failedGroup = group(1);
    RuntimeException expected = new RuntimeException();
    SettableFuture<Void> failed = SettableFuture.create();
    failedGroup.release(lease(failedGroup, 1), failed, false);
    failed.setException(expected);
    assertEquals(0, failedGroup.getReadyCount());
    assertFailure(failedGroup.getFailureFuture(), expected);
    assertFailure(failed, expected);
    assertFalse(failedGroup.poll(2).getLease().isPresent());
  }

  @Test
  public void failedConsumerDependencyIsReportedInsteadOfSilentlyGrantingALease() throws Exception {
    IDriver driver = mock(IDriver.class);
    SettableFuture<Void> dependency = SettableFuture.create();
    RuntimeScanGroup group = new RuntimeScanGroup(List.of(driver), List.of(dependency));
    ListenableFuture<?> waiting = group.poll(1).getReadyFuture();
    RuntimeException expected = new RuntimeException();
    dependency.setException(expected);
    assertTrue(waiting.isDone());
    assertFailure(group.getFailureFuture(), expected);
    RuntimeScanGroup.PollResult failed = group.poll(1);
    assertFalse(failed.getLease().isPresent());
    assertFailure(failed.getReadyFuture(), expected);
    assertFalse(group.isStopped());
    verifyZeroInteractions(driver);
  }

  @Test
  public void dependencyCancellationSignalsFailureWithoutCancellingSharedFailureFuture()
      throws Exception {
    SettableFuture<Void> dependency = SettableFuture.create();
    RuntimeScanGroup group =
        new RuntimeScanGroup(List.of(mock(IDriver.class)), List.of(dependency));
    ListenableFuture<?> disposableView = group.getFailureFuture();
    disposableView.cancel(false);
    dependency.cancel(false);
    ListenableFuture<?> failure = group.getFailureFuture();
    assertFalse(failure.isCancelled());
    try {
      failure.get();
      fail();
    } catch (ExecutionException expected) {
      assertTrue(expected.getCause() instanceof CancellationException);
    }
  }

  @Test
  public void blockedListenerRegistrationOccursAfterLeaseReturnAndOutsideMemberLock()
      throws Exception {
    RuntimeScanGroup group = group(1);
    ExecutorService reader = Executors.newSingleThreadExecutor();
    ListenableFuture<?> blocked = mock(ListenableFuture.class);
    try {
      doAnswer(
              call -> {
                assertEquals(
                    0, (int) reader.submit(group::getLeasedCount).get(5, TimeUnit.SECONDS));
                return null;
              })
          .when(blocked)
          .addListener(any(Runnable.class), any(Executor.class));
      group.release(lease(group, 1), blocked, false);
      assertEquals(0, group.getLeasedCount());
    } finally {
      reader.shutdownNow();
    }
  }

  @Test
  public void readyAndQuiescenceNotificationsDoNotRunUnderMemberLock() throws Exception {
    RuntimeScanGroup group = group(1);
    ExecutorService reader = Executors.newSingleThreadExecutor();
    AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
    RuntimeLaneLease first = lease(group, 1);
    try {
      Runnable inspect =
          () -> {
            try {
              reader.submit(group::snapshot).get(5, TimeUnit.SECONDS);
            } catch (Throwable failure) {
              callbackFailure.set(failure);
            }
          };
      group.poll(2).getReadyFuture().addListener(inspect, MoreExecutors.directExecutor());
      group.release(first, null, false);
      assertNull(callbackFailure.get());
      RuntimeLaneLease second = lease(group, 1);
      group.getQuiescenceFuture().addListener(inspect, MoreExecutors.directExecutor());
      group.requestStop();
      group.release(second, null, false);
      assertTrue(group.getQuiescenceFuture().isDone());
      assertNull(callbackFailure.get());
    } finally {
      reader.shutdownNow();
    }
  }

  @Test
  public void stopAndLateBlockedCompletionDoNotReviveALane() {
    RuntimeScanGroup group = group(1);
    SettableFuture<Void> blocked = SettableFuture.create();
    group.release(lease(group, 1), blocked, false);
    group.requestStop();
    assertTrue(group.getQuiescenceFuture().isDone());
    blocked.set(null);
    assertEquals(0, group.getReadyCount());
    assertFalse(group.poll(2).getLease().isPresent());
    assertTrue(group.poll(2).getReadyFuture().isDone());
  }

  @Test
  public void generationOverflowDoesNotConsumeReadyLane() throws Exception {
    RuntimeScanGroup group = group(1);
    Field lanes = RuntimeScanGroup.class.getDeclaredField("lanes");
    lanes.setAccessible(true);
    Object lane = ((List<?>) lanes.get(group)).get(0);
    Field generation = lane.getClass().getDeclaredField("generation");
    generation.setAccessible(true);
    generation.setLong(lane, Long.MAX_VALUE);
    expect(ArithmeticException.class, () -> group.poll(1));
    assertEquals(1, group.getReadyCount());
    assertEquals(0, group.getLeasedCount());
  }

  private static RuntimeScanGroup group(int size) {
    List<IDriver> drivers = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      drivers.add(mock(IDriver.class));
    }
    return new RuntimeScanGroup(drivers);
  }

  private static RuntimeLaneLease lease(RuntimeScanGroup group, long worker) {
    RuntimeScanGroup.PollResult result = group.poll(worker);
    assertTrue(result.getReadyFuture().isDone());
    assertTrue(result.getLease().isPresent());
    return result.getLease().get();
  }

  private static ListenableFuture<?> callbackFuture(AtomicReference<Runnable> callback) {
    ListenableFuture<?> future = mock(ListenableFuture.class);
    when(future.isDone()).thenReturn(true);
    doAnswer(
            call -> {
              callback.set(call.getArgument(0));
              return null;
            })
        .when(future)
        .addListener(any(Runnable.class), any(Executor.class));
    return future;
  }

  private static void assertFailure(ListenableFuture<?> future, Throwable expected)
      throws Exception {
    try {
      future.get(5, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException failure) {
      assertSame(expected, failure.getCause());
    }
  }

  private static void expect(Class<? extends Throwable> expected, Runnable action) {
    try {
      action.run();
      fail();
    } catch (Throwable failure) {
      assertTrue(failure.toString(), expected.isInstance(failure));
    }
  }
}
