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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.queryengine.execution.runtime.RuntimeCleanupCoordinator.Phase.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class RuntimeCleanupCoordinatorTest {
  @Test
  public void sealingDoesNotTerminateALiveFragment() {
    RuntimeCleanupCoordinator c = coordinator();
    Owner owner = new Owner();
    c.registerDriver(owner.driver);
    c.sealConstruction();
    assertFalse(c.isStopRequested());
    assertFalse(c.getCompletedFuture().isDone());
    verify(owner.driver, never()).requestQuiescence();
    verify(owner.driver, never()).requestClose();
  }

  @Test
  public void cancellationDuringConstructionWaitsForLateOwnersAndSeal() throws Exception {
    RuntimeCleanupCoordinator c = coordinator();
    List<String> events = new ArrayList<>();
    c.addCleanup(RELEASE_RESOURCES, () -> mark(events, "release"));
    c.requestStop();
    assertFalse(c.getCompletedFuture().isDone());
    Owner late = new Owner();
    c.registerDriver(late.driver);
    verify(late.driver).requestQuiescence();
    late.quiesced.set(null);
    late.closed.set(null);
    verify(late.driver, never()).requestClose();
    assertTrue(events.isEmpty());
    c.sealConstruction();
    c.getCompletedFuture().get(5, TimeUnit.SECONDS);
    assertEquals(Collections.singletonList("release"), events);
    verify(late.driver).requestClose();
  }

  @Test
  public void leasedLanePreventsCleanupEvenWhenAllDriversAcknowledge() throws Exception {
    IDriver lane = mock(IDriver.class);
    RuntimeScanGroup group = new RuntimeScanGroup(Collections.singletonList(lane));
    RuntimeLaneLease lease = group.poll(0).getLease().get();
    RuntimeCleanupCoordinator c = coordinator();
    Owner owner = new Owner();
    owner.quiesced.set(null);
    owner.closed.set(null);
    c.registerDriver(owner.driver);
    c.registerGroup(group);
    c.sealConstruction();
    c.requestStop();
    assertTrue(group.isStopped());
    assertFalse(c.getQuiescenceFuture().isDone());
    verify(owner.driver, never()).requestClose();
    group.release(lease, Futures.immediateVoidFuture(), false);
    c.getCompletedFuture().get(5, TimeUnit.SECONDS);
  }

  @Test
  public void statisticsWaitForEveryActualCloseAndResourcesWaitForEveryPhase() throws Exception {
    RuntimeCleanupCoordinator c = coordinator();
    Owner a = new Owner();
    Owner b = new Owner();
    List<String> events = new ArrayList<>();
    SettableFuture<Void> endpoint = SettableFuture.create();
    SettableFuture<Void> frontier = SettableFuture.create();
    c.registerDriver(a.driver);
    c.registerDriver(b.driver);
    c.addCleanup(BEFORE_DRIVER_CLOSE, () -> mark(events, "abort-sink"));
    c.addCleanup(FREEZE_STATISTICS, () -> mark(events, "freeze"));
    c.addCleanup(
        CLOSE_ENDPOINTS,
        () -> {
          events.add("close-endpoints");
          return endpoint;
        });
    c.addCleanup(
        RELEASE_FRONTIERS,
        () -> {
          events.add("frontier");
          return frontier;
        });
    c.addCleanup(RELEASE_RESOURCES, () -> mark(events, "base-and-data"));
    c.addCleanup(DEREGISTER, () -> mark(events, "deregister"));
    c.sealConstruction();
    c.requestStop();
    a.quiesced.set(null);
    assertTrue(events.isEmpty());
    b.quiesced.set(null);
    assertTrue(c.getQuiescenceFuture().isDone());
    assertEquals(Collections.singletonList("abort-sink"), events);
    // isFinished is deliberately already true, while actual close is still in flight.
    assertTrue(a.driver.isFinished());
    a.closed.set(null);
    assertFalse(c.getDriversClosedFuture().isDone());
    assertEquals(Collections.singletonList("abort-sink"), events);
    b.closed.set(null);
    assertEquals(Arrays.asList("abort-sink", "freeze", "close-endpoints"), events);
    assertTrue(c.getDriversClosedFuture().isDone());
    endpoint.set(null);
    assertEquals("frontier", events.get(events.size() - 1));
    assertFalse(c.getCompletedFuture().isDone());
    frontier.set(null);
    c.getCompletedFuture().get(5, TimeUnit.SECONDS);
    assertEquals(
        Arrays.asList(
            "abort-sink", "freeze", "close-endpoints", "frontier", "base-and-data", "deregister"),
        events);
  }

  @Test
  public void pendingSinkAbortCompletesBeforeDriverClose() throws Exception {
    RuntimeCleanupCoordinator c = coordinator();
    Owner owner = new Owner();
    owner.quiesced.set(null);
    owner.closed.set(null);
    SettableFuture<Void> abort = SettableFuture.create();
    c.registerDriver(owner.driver);
    c.addCleanup(BEFORE_DRIVER_CLOSE, () -> abort);
    c.sealConstruction();
    c.requestStop();
    verify(owner.driver, never()).requestClose();
    abort.set(null);
    c.getCompletedFuture().get(5, TimeUnit.SECONDS);
    verify(owner.driver).requestClose();
  }

  @Test
  public void asynchronousCleanupFailureDoesNotSkipOtherOwners() throws Exception {
    RuntimeCleanupCoordinator c = coordinator();
    List<String> events = new ArrayList<>();
    RuntimeException first = new RuntimeException("sink error");
    RuntimeException second = new RuntimeException("publication error");
    c.addCleanup(BEFORE_DRIVER_CLOSE, () -> Futures.immediateFailedFuture(first));
    c.addCleanup(
        FREEZE_STATISTICS,
        () -> {
          throw second;
        });
    c.addCleanup(RELEASE_RESOURCES, () -> mark(events, "release"));
    c.addCleanup(DEREGISTER, () -> mark(events, "deregister"));
    c.sealConstruction();
    assertSame(first, failure(c.requestStop()));
    assertEquals(Arrays.asList("release", "deregister"), events);
    assertArrayEquals(new Throwable[] {second}, first.getSuppressed());
  }

  @Test
  public void allCleanupOwnersWithinAPhaseMustSettle() throws Exception {
    RuntimeCleanupCoordinator c = coordinator();
    SettableFuture<Void> one = SettableFuture.create();
    SettableFuture<Void> two = SettableFuture.create();
    AtomicInteger releases = new AtomicInteger();
    c.addCleanup(CLOSE_ENDPOINTS, () -> one);
    c.addCleanup(CLOSE_ENDPOINTS, () -> two);
    c.addCleanup(
        RELEASE_FRONTIERS,
        () -> {
          releases.incrementAndGet();
          return Futures.immediateVoidFuture();
        });
    c.sealConstruction();
    c.requestStop();
    one.set(null);
    assertEquals(0, releases.get());
    two.set(null);
    c.getCompletedFuture().get(5, TimeUnit.SECONDS);
    assertEquals(1, releases.get());
  }

  @Test
  public void exceptionalDriverAcknowledgementsStillProveWriterExit() throws Exception {
    RuntimeCleanupCoordinator c = coordinator();
    Owner owner = new Owner();
    RuntimeException q = new RuntimeException("quiescent cleanup failure");
    RuntimeException closed = new RuntimeException("closed cleanup failure");
    AtomicInteger released = new AtomicInteger();
    c.registerDriver(owner.driver);
    c.addCleanup(
        RELEASE_RESOURCES,
        () -> {
          released.incrementAndGet();
          return Futures.immediateVoidFuture();
        });
    c.sealConstruction();
    c.requestStop();
    owner.quiesced.setException(q);
    assertTrue(c.getQuiescenceFuture().isDone());
    assertFalse(c.getCompletedFuture().isDone());
    owner.closed.setException(closed);
    assertSame(q, failure(c.getCompletedFuture()));
    assertEquals(1, released.get());
    assertArrayEquals(new Throwable[] {closed}, q.getSuppressed());
  }

  @Test
  public void observerCancellationDoesNotCancelCleanup() throws Exception {
    RuntimeCleanupCoordinator c = coordinator();
    Owner owner = new Owner();
    c.registerDriver(owner.driver);
    c.sealConstruction();
    ListenableFuture<Void> first = c.requestStop();
    assertTrue(first.cancel(false));
    assertTrue(c.getQuiescenceFuture().cancel(false));
    assertTrue(c.getDriversClosedFuture().cancel(false));
    owner.quiesced.set(null);
    owner.closed.set(null);
    c.getCompletedFuture().get(5, TimeUnit.SECONDS);
    assertFalse(c.getQuiescenceFuture().isCancelled());
    assertFalse(c.getDriversClosedFuture().isCancelled());
    verify(owner.driver).requestClose();
  }

  @Test
  public void rejectsUncoordinatedAndDuplicateOwnersAndRegistrationAfterSeal() {
    RuntimeCleanupCoordinator c = coordinator();
    expectIllegalArgument(() -> c.registerDriver(mock(IDriver.class)));
    Owner owner = new Owner();
    c.registerDriver(owner.driver);
    expectIllegalArgument(() -> c.registerDriver(owner.driver));
    RuntimeScanGroup group = new RuntimeScanGroup(Collections.emptyList());
    c.registerGroup(group);
    expectIllegalArgument(() -> c.registerGroup(group));
    c.sealConstruction();
    for (Runnable action :
        Arrays.<Runnable>asList(
            () -> c.registerDriver(new Owner().driver),
            () -> c.registerGroup(new RuntimeScanGroup(Collections.emptyList())),
            () -> c.addCleanup(DEREGISTER, () -> Futures.immediateVoidFuture()))) {
      try {
        action.run();
        fail("registration after seal");
      } catch (IllegalStateException expected) {
      }
    }
  }

  @Test
  public void concurrentTerminalRequestsRunCleanupOnce() throws Exception {
    QueueExecutor queue = new QueueExecutor();
    RuntimeCleanupCoordinator c = new RuntimeCleanupCoordinator(queue);
    Owner owner = new Owner();
    owner.quiesced.set(null);
    owner.closed.set(null);
    AtomicInteger releases = new AtomicInteger();
    c.registerDriver(owner.driver);
    c.addCleanup(
        RELEASE_RESOURCES,
        () -> {
          releases.incrementAndGet();
          return Futures.immediateVoidFuture();
        });
    c.sealConstruction();
    ExecutorService workers = Executors.newFixedThreadPool(8);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<?>> tasks = new ArrayList<>();
    try {
      for (int i = 0; i < 8; i++) {
        tasks.add(
            workers.submit(
                () -> {
                  await(start);
                  c.requestStop();
                }));
      }
      start.countDown();
      for (Future<?> task : tasks) {
        task.get(5, TimeUnit.SECONDS);
      }
      assertFalse(c.getCompletedFuture().isDone());
      queue.drain();
      c.getCompletedFuture().get(5, TimeUnit.SECONDS);
      c.requestStop().get(5, TimeUnit.SECONDS);
      c.sealConstruction();
      assertEquals(1, releases.get());
      verify(owner.driver).requestClose();
    } finally {
      start.countDown();
      workers.shutdownNow();
    }
  }

  @Test
  public void rejectedExecutorDoesNotRunInlineOrReleaseResources() throws Exception {
    RejectedExecutionException rejected = new RejectedExecutionException();
    RuntimeCleanupCoordinator c =
        new RuntimeCleanupCoordinator(
            task -> {
              throw rejected;
            });
    AtomicInteger releases = new AtomicInteger();
    c.addCleanup(
        RELEASE_RESOURCES,
        () -> {
          releases.incrementAndGet();
          return Futures.immediateVoidFuture();
        });
    c.sealConstruction();
    assertSame(rejected, failure(c.requestStop()));
    assertEquals(0, releases.get());
    assertSame(rejected, failure(c.getQuiescenceFuture()));
  }

  @Test
  public void thrownStopInvocationIsNotProofOfQuiescence() throws Exception {
    RuntimeCleanupCoordinator c = coordinator();
    Owner owner = new Owner();
    IllegalStateException expected = new IllegalStateException();
    when(owner.driver.requestQuiescence()).thenThrow(expected);
    AtomicInteger releases = new AtomicInteger();
    c.registerDriver(owner.driver);
    c.addCleanup(
        RELEASE_RESOURCES,
        () -> {
          releases.incrementAndGet();
          return Futures.immediateVoidFuture();
        });
    c.sealConstruction();
    assertSame(expected, failure(c.requestStop()));
    verify(owner.driver, never()).requestClose();
    assertEquals(0, releases.get());
  }

  @Test
  public void thrownCloseInvocationWithholdsDependentRelease() throws Exception {
    RuntimeCleanupCoordinator c = coordinator();
    Owner owner = new Owner();
    owner.quiesced.set(null);
    IllegalStateException expected = new IllegalStateException();
    when(owner.driver.requestClose()).thenThrow(expected);
    AtomicInteger releases = new AtomicInteger();
    c.registerDriver(owner.driver);
    c.addCleanup(
        RELEASE_RESOURCES,
        () -> {
          releases.incrementAndGet();
          return Futures.immediateVoidFuture();
        });
    c.sealConstruction();
    assertSame(expected, failure(c.requestStop()));
    assertEquals(0, releases.get());
    assertSame(expected, failure(c.getDriversClosedFuture()));
  }

  @Test
  public void failedObserverMustNotSkipActualCloseOrResourceSettlement() throws Exception {
    RuntimeCleanupCoordinator c = coordinator();
    Owner owner = new Owner();
    AssertionError expected = new AssertionError("observer failed");
    AtomicInteger released = new AtomicInteger();
    c.registerDriver(owner.driver);
    c.addCleanup(
        RELEASE_RESOURCES,
        () -> {
          released.incrementAndGet();
          return Futures.immediateVoidFuture();
        });
    c.getQuiescenceFuture()
        .addListener(
            () -> {
              throw expected;
            },
            MoreExecutors.directExecutor());
    c.getDriversClosedFuture()
        .addListener(
            () -> {
              throw expected;
            },
            MoreExecutors.directExecutor());
    c.sealConstruction();
    c.requestStop();
    owner.quiesced.set(null);
    owner.closed.set(null);
    assertSame(expected, failure(c.getCompletedFuture()));
    assertEquals(1, released.get());
    verify(owner.driver).requestClose();
  }

  @Test
  public void groupWakeObserverCannotStrandStopBeforeCleanupDispatch() throws Exception {
    SettableFuture<Void> dependency = SettableFuture.create();
    RuntimeScanGroup group =
        new RuntimeScanGroup(
            Collections.singletonList(mock(IDriver.class)), Collections.singletonList(dependency));
    AssertionError expected = new AssertionError("wakeup observer");
    group
        .poll(0)
        .getReadyFuture()
        .addListener(
            () -> {
              throw expected;
            },
            MoreExecutors.directExecutor());
    RuntimeCleanupCoordinator c = coordinator();
    AtomicInteger released = new AtomicInteger();
    c.registerGroup(group);
    c.addCleanup(
        RELEASE_RESOURCES,
        () -> {
          released.incrementAndGet();
          return Futures.immediateVoidFuture();
        });
    c.sealConstruction();
    assertSame(expected, failure(c.requestStop()));
    assertTrue(group.getQuiescenceFuture().isDone());
    assertEquals(1, released.get());
  }

  @Test
  public void unsafeFailureObserversCannotSkipFinalCompletion() throws Exception {
    RejectedExecutionException rejected = new RejectedExecutionException();
    AssertionError observer = new AssertionError("failure observer");
    RuntimeCleanupCoordinator c =
        new RuntimeCleanupCoordinator(
            task -> {
              throw rejected;
            });
    c.getQuiescenceFuture()
        .addListener(
            () -> {
              throw observer;
            },
            MoreExecutors.directExecutor());
    c.getDriversClosedFuture()
        .addListener(
            () -> {
              throw observer;
            },
            MoreExecutors.directExecutor());
    c.sealConstruction();
    assertSame(rejected, failure(c.requestStop()));
    assertTrue(c.getDriversClosedFuture().isDone());
    assertArrayEquals(new Throwable[] {observer}, rejected.getSuppressed());
  }

  @Test
  public void throwingCloseStillAttemptsOtherOwnersWithoutReleasingSharedResources()
      throws Exception {
    RuntimeCleanupCoordinator c = coordinator();
    Owner broken = new Owner();
    Owner other = new Owner();
    broken.quiesced.set(null);
    other.quiesced.set(null);
    other.closed.set(null);
    IllegalStateException expected = new IllegalStateException();
    when(broken.driver.requestClose()).thenThrow(expected);
    AtomicInteger released = new AtomicInteger();
    c.registerDriver(broken.driver);
    c.registerDriver(other.driver);
    c.addCleanup(
        RELEASE_RESOURCES,
        () -> {
          released.incrementAndGet();
          return Futures.immediateVoidFuture();
        });
    c.sealConstruction();
    assertSame(expected, failure(c.requestStop()));
    verify(other.driver).requestClose();
    assertEquals(0, released.get());
  }

  private static RuntimeCleanupCoordinator coordinator() {
    return new RuntimeCleanupCoordinator(MoreExecutors.directExecutor());
  }

  private static ListenableFuture<Void> mark(List<String> events, String event) {
    events.add(event);
    return Futures.immediateVoidFuture();
  }

  private static Throwable failure(ListenableFuture<?> future) throws Exception {
    try {
      future.get(5, TimeUnit.SECONDS);
      fail("expected failure");
      return null;
    } catch (ExecutionException e) {
      return e.getCause();
    }
  }

  private static void expectIllegalArgument(Runnable action) {
    try {
      action.run();
      fail("expected invalid owner rejection");
    } catch (IllegalArgumentException expected) {
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static class Owner {
    final IDriver driver = mock(IDriver.class);
    final SettableFuture<Void> quiesced = SettableFuture.create();
    final SettableFuture<Void> closed = SettableFuture.create();

    Owner() {
      when(driver.supportsQuiescence()).thenReturn(true);
      when(driver.supportsCoordinatedClose()).thenReturn(true);
      when(driver.isFinished()).thenReturn(true);
      when(driver.requestQuiescence())
          .thenAnswer(call -> Futures.nonCancellationPropagating(quiesced));
      when(driver.requestClose()).thenAnswer(call -> Futures.nonCancellationPropagating(closed));
    }
  }

  private static class QueueExecutor implements Executor {
    final ArrayDeque<Runnable> tasks = new ArrayDeque<>();

    public synchronized void execute(Runnable task) {
      tasks.add(task);
    }

    void drain() {
      while (true) {
        Runnable task;
        synchronized (this) {
          task = tasks.poll();
        }
        if (task == null) {
          return;
        }
        task.run();
      }
    }
  }
}
