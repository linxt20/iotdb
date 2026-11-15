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

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class RuntimeScanWorkerTest {
  private static final Duration SLICE = new Duration(0, TimeUnit.NANOSECONDS);

  @Test
  public void aBlockedLaneDoesNotKeepWorkerExecutionOwnership() {
    IDriver first = lane();
    IDriver second = lane();
    SettableFuture<Void> blocked = SettableFuture.create();
    doReturn(blocked).when(first).processFor(any(Duration.class));
    RuntimeScanGroup group = new RuntimeScanGroup(Arrays.asList(first, second));
    RuntimeScanWorker worker = worker(group, 0, new AtomicInteger());
    worker.processFor(SLICE);
    assertEquals(0, group.getLeasedCount());
    worker.processFor(SLICE);
    verify(first).processFor(any(Duration.class));
    verify(second).processFor(any(Duration.class));
    assertFalse(blocked.isDone());
    worker.close();
    verify(first, never()).close();
    verify(second, never()).close();
  }

  @Test
  public void retiringBlockedWorkerPreservesLogicalStreamAndResourcesSettleOnce() throws Exception {
    IDriver lane = lane();
    SettableFuture<Void> blocked = SettableFuture.create();
    doReturn(blocked).when(lane).processFor(any(Duration.class));
    RuntimeScanGroup group = new RuntimeScanGroup(Collections.singletonList(lane));
    AtomicInteger released = new AtomicInteger();
    RuntimeScanWorker worker = worker(group, 0, released);
    worker.processFor(SLICE);
    ListenableFuture<?> anyReady = worker.processFor(SLICE);
    assertFalse(anyReady.isDone());
    worker.requestRetirement().get(5, TimeUnit.SECONDS);
    assertTrue(anyReady.isDone());
    assertTrue(worker.isFinished());
    worker.close();
    assertEquals(1, released.get());
    verify(lane, never()).close();
    assertFalse(blocked.isDone());
    assertFalse(group.isStopped());
    verify(worker.getDriverContext(), never()).finished();
  }

  @Test
  public void replacementUsesSameLogicalCursor() throws Exception {
    IDriver lane = lane();
    RuntimeScanGroup group = new RuntimeScanGroup(Collections.singletonList(lane));
    RuntimeScanWorker old = worker(group, 0, new AtomicInteger());
    old.processFor(SLICE);
    old.requestRetirement().get(5, TimeUnit.SECONDS);
    RuntimeScanWorker replacement = worker(group, 1, new AtomicInteger());
    replacement.processFor(SLICE);
    verify(lane, times(2)).processFor(any(Duration.class));
    assertEquals(0, group.getLeasedCount());
    verify(lane, never()).close();
    replacement.close();
  }

  @Test
  public void activeRetirementWaitsForQuantumWithoutClosingLane() throws Exception {
    IDriver lane = lane();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    doAnswer(
            call -> {
              entered.countDown();
              await(release);
              return Operator.NOT_BLOCKED;
            })
        .when(lane)
        .processFor(any(Duration.class));
    RuntimeScanGroup group = new RuntimeScanGroup(Collections.singletonList(lane));
    AtomicInteger released = new AtomicInteger();
    RuntimeScanWorker worker = worker(group, 0, released);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> quantum = executor.submit(() -> worker.processFor(SLICE));
    try {
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      ListenableFuture<Void> retired = worker.requestRetirement();
      assertFalse(retired.isDone());
      assertEquals(0, released.get());
      assertEquals(1, group.getLeasedCount());
      release.countDown();
      quantum.get(5, TimeUnit.SECONDS);
      retired.get(5, TimeUnit.SECONDS);
      assertEquals(0, group.getLeasedCount());
      assertEquals(1, released.get());
      verify(lane, never()).close();
    } finally {
      release.countDown();
      executor.shutdownNow();
      worker.close();
    }
  }

  @Test
  public void schedulerRemovalIsDistinctFromRetirement() throws Exception {
    RuntimeScanWorker worker =
        worker(new RuntimeScanGroup(Collections.singletonList(lane())), 0, new AtomicInteger());
    worker.requestRetirement().get(5, TimeUnit.SECONDS);
    assertFalse(worker.getSchedulerRemovalFuture().isDone());
    worker.acknowledgeSchedulerRemoval();
    worker.getSchedulerRemovalFuture().get(5, TimeUnit.SECONDS);
    worker.acknowledgeSchedulerRemoval();
  }

  @Test
  public void naturalEofReturnsLaneWithoutClosingFromWorker() {
    IDriver lane = lane();
    when(lane.isFinished()).thenReturn(false, true);
    RuntimeScanGroup group = new RuntimeScanGroup(Collections.singletonList(lane));
    AtomicInteger released = new AtomicInteger();
    RuntimeScanWorker worker = worker(group, 0, released);
    worker.processFor(SLICE);
    assertTrue(group.isFinished());
    assertTrue(worker.isFinished());
    assertEquals(1, released.get());
    verify(lane, never()).close();
  }

  @Test
  public void lateBlockedFailureStillFailsActiveReplacement() throws Exception {
    IDriver lane = lane();
    SettableFuture<Void> blocked = SettableFuture.create();
    doReturn(blocked).when(lane).processFor(any(Duration.class));
    RuntimeScanGroup group = new RuntimeScanGroup(Collections.singletonList(lane));
    RuntimeScanWorker old = worker(group, 0, new AtomicInteger());
    old.processFor(SLICE);
    old.requestRetirement().get(5, TimeUnit.SECONDS);
    RuntimeScanWorker replacement = worker(group, 1, new AtomicInteger());
    RuntimeException expected = new RuntimeException("late scan failure");
    blocked.setException(expected);
    verify(replacement.getDriverContext()).failed(expected);
    verify(old.getDriverContext(), never()).failed(any(Throwable.class));
    assertTrue(group.isStopped());
    assertTrue(replacement.isFinished());
    verify(lane, never()).close();
  }

  @Test
  public void dependencyFailureBeforeWorkerCreationIsNotLost() {
    SettableFuture<Void> dependency = SettableFuture.create();
    RuntimeException expected = new RuntimeException("consumer failed");
    dependency.setException(expected);
    RuntimeScanGroup group =
        new RuntimeScanGroup(
            Collections.singletonList(lane()), Collections.singletonList(dependency));
    RuntimeScanWorker worker = worker(group, 0, new AtomicInteger());
    verify(worker.getDriverContext()).failed(expected);
    assertTrue(worker.isFinished());
  }

  @Test
  public void resourceReleaseFailureRemainsObservable() throws Exception {
    RuntimeException expected = new RuntimeException("release failed");
    DriverContext context = mock(DriverContext.class);
    RuntimeScanWorker worker =
        new RuntimeScanWorker(
            new RuntimeScanGroup(Collections.singletonList(lane())),
            context,
            0,
            32,
            () -> {
              throw expected;
            });
    ListenableFuture<Void> retired = worker.requestRetirement();
    assertTrue(retired.isDone());
    try {
      retired.get(5, TimeUnit.SECONDS);
      fail("release failure must be reported");
    } catch (java.util.concurrent.ExecutionException e) {
      assertSame(expected, e.getCause());
    }
    verify(context).failed(expected);
  }

  @Test
  public void oneObserverCannotCancelOtherWorkersWakeup() {
    SettableFuture<Void> dependency = SettableFuture.create();
    RuntimeScanGroup group =
        new RuntimeScanGroup(
            Collections.singletonList(lane()), Collections.singletonList(dependency));
    RuntimeScanWorker first = worker(group, 0, new AtomicInteger());
    RuntimeScanWorker second = worker(group, 1, new AtomicInteger());
    ListenableFuture<?> firstWake = first.processFor(SLICE);
    ListenableFuture<?> secondWake = second.processFor(SLICE);
    assertTrue(firstWake.cancel(false));
    assertFalse(secondWake.isDone());
    dependency.set(null);
    assertTrue(secondWake.isDone());
    first.close();
    second.close();
  }

  @Test
  public void fixedGroupExecutesWithOneTwoFourTwoOneWorkers() throws Exception {
    java.util.List<IDriver> lanes = new java.util.ArrayList<>();
    AtomicInteger active = new AtomicInteger();
    AtomicInteger maximum = new AtomicInteger();
    AtomicInteger processed = new AtomicInteger();
    java.util.concurrent.atomic.AtomicBoolean hold =
        new java.util.concurrent.atomic.AtomicBoolean();
    CountDownLatch fourEntered = new CountDownLatch(4);
    CountDownLatch release = new CountDownLatch(1);
    for (int i = 0; i < 8; i++) {
      IDriver lane = lane();
      doAnswer(
              call -> {
                int concurrent = active.incrementAndGet();
                maximum.accumulateAndGet(concurrent, Math::max);
                try {
                  processed.incrementAndGet();
                  if (hold.get()) {
                    fourEntered.countDown();
                    await(release);
                  }
                  return Operator.NOT_BLOCKED;
                } finally {
                  active.decrementAndGet();
                }
              })
          .when(lane)
          .processFor(any(Duration.class));
      lanes.add(lane);
    }
    RuntimeScanGroup group = new RuntimeScanGroup(lanes);
    AtomicInteger released = new AtomicInteger();
    RuntimeScanWorker first = worker(group, 0, released);
    first.processFor(SLICE);
    RuntimeScanWorker second = worker(group, 1, released);
    first.processFor(SLICE);
    second.processFor(SLICE);
    RuntimeScanWorker third = worker(group, 2, released);
    RuntimeScanWorker fourth = worker(group, 3, released);
    RuntimeScanWorker[] members = {first, second, third, fourth};
    ExecutorService executor = Executors.newFixedThreadPool(4);
    java.util.List<Future<?>> quanta = new java.util.ArrayList<>();
    try {
      hold.set(true);
      for (RuntimeScanWorker member : members) {
        quanta.add(executor.submit(() -> member.processFor(SLICE)));
      }
      assertTrue(fourEntered.await(5, TimeUnit.SECONDS));
      assertEquals(4, active.get());
      assertEquals(4, group.getLeasedCount());
      release.countDown();
      for (Future<?> quantum : quanta) {
        quantum.get(5, TimeUnit.SECONDS);
      }
      hold.set(false);
      third.requestRetirement().get(5, TimeUnit.SECONDS);
      fourth.requestRetirement().get(5, TimeUnit.SECONDS);
      first.processFor(SLICE);
      second.processFor(SLICE);
      assertEquals(2, released.get());
      second.requestRetirement().get(5, TimeUnit.SECONDS);
      first.processFor(SLICE);
      assertEquals(3, released.get());
      assertEquals(10, processed.get());
      assertEquals(4, maximum.get());
      assertEquals(0, group.getLeasedCount());
      assertFalse(group.isStopped());
      for (IDriver lane : lanes) {
        verify(lane, never()).close();
      }
    } finally {
      release.countDown();
      executor.shutdownNow();
      for (RuntimeScanWorker member : members) {
        member.close();
      }
    }
    assertEquals(4, released.get());
  }

  @Test
  public void failureStopsGroupBeforeRepublishingBadLane() {
    IDriver lane = lane();
    RuntimeScanGroup group = new RuntimeScanGroup(Collections.singletonList(lane));
    java.util.concurrent.atomic.AtomicBoolean reacquired =
        new java.util.concurrent.atomic.AtomicBoolean();
    RuntimeException expected = new RuntimeException("bad lane");
    doAnswer(
            call -> {
              ListenableFuture<?> waiting = group.poll(99).getReadyFuture();
              assertFalse(waiting.isDone());
              waiting.addListener(
                  () -> {
                    java.util.Optional<RuntimeLaneLease> bad = group.poll(99).getLease();
                    reacquired.set(bad.isPresent());
                    if (bad.isPresent()) {
                      group.release(bad.get(), Operator.NOT_BLOCKED, false);
                    }
                  },
                  com.google.common.util.concurrent.MoreExecutors.directExecutor());
              throw expected;
            })
        .when(lane)
        .processFor(any(Duration.class));
    RuntimeScanWorker worker = worker(group, 0, new AtomicInteger());
    try {
      worker.processFor(SLICE);
      fail("the scan error must remain observable");
    } catch (RuntimeException actual) {
      assertSame(expected, actual);
    }
    assertFalse(reacquired.get());
    assertTrue(group.isStopped());
    assertEquals(0, group.getLeasedCount());
    verify(worker.getDriverContext()).failed(expected);
  }

  private static RuntimeScanWorker worker(RuntimeScanGroup group, long id, AtomicInteger releases) {
    return new RuntimeScanWorker(
        group, mock(DriverContext.class), id, 64, releases::incrementAndGet);
  }

  private static IDriver lane() {
    IDriver lane = mock(IDriver.class);
    doReturn(Operator.NOT_BLOCKED).when(lane).processFor(any(Duration.class));
    return lane;
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }
}
