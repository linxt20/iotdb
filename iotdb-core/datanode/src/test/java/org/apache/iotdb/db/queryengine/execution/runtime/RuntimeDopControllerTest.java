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

import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeDopPolicy.Action;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeDopPolicy.Config;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeDopPolicy.Input;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeDopPolicy.MetricValue;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeDopPolicy.Reason;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RuntimeDopControllerTest {
  @Test
  public void realPolicyGrowsOnlyAfterConsecutiveReadyBacklogWindows() throws Exception {
    TestTarget owner = new TestTarget();
    AtomicLong clock = new AtomicLong(1000);
    owner.inputs = (now, sequence) -> Map.of("scan", input(now, sequence, 2, 2, 2, 0));
    try (RuntimeDopController controller = controller(owner, clock, 2)) {
      sample(controller, clock);
      sample(controller, clock);
      assertTrue(owner.resizes.isEmpty());
      assertEquals(Reason.HYSTERESIS, controller.snapshotEvents().get(1).reason());
      sample(controller, clock);
      assertEquals(List.of(new Resize("scan", 3)), owner.resizes);
      RuntimeDopController.Event event = controller.snapshotEvents().get(2);
      assertEquals(Action.GROW, event.action());
      assertEquals(Reason.READY_BACKLOG, event.reason());
      assertEquals(2, event.observedWorkers());
      assertEquals(3, event.requestedWorkers());
      assertEquals(300L, event.sampleNanos());
      assertEquals(3L, event.sampleSequence());
      assertEquals(MetricValue.valid(30), event.completedWork());
      assertTrue(owner.failures.isEmpty());
    }
  }

  @Test
  public void realPolicyShrinksAfterSustainedBackpressureWindows() throws Exception {
    TestTarget owner = new TestTarget();
    AtomicLong clock = new AtomicLong(7000);
    owner.inputs = (now, sequence) -> Map.of("scan", input(now, sequence, 3, 0, 0, 3));
    try (RuntimeDopController controller = controller(owner, clock, 2)) {
      sample(controller, clock);
      sample(controller, clock);
      assertTrue(owner.resizes.isEmpty());
      sample(controller, clock);
      assertEquals(List.of(new Resize("scan", 2)), owner.resizes);
      RuntimeDopController.Event event = controller.snapshotEvents().get(2);
      assertEquals(Action.SHRINK, event.action());
      assertEquals(Reason.SUSTAINED_BACKPRESSURE, event.reason());
      assertEquals(3, event.observedWorkers());
      assertEquals(2, event.requestedWorkers());
      assertTrue(owner.failures.isEmpty());
    }
  }

  @Test
  public void closedControllerRejectsManualSamplingAndSubsequentStart() throws Exception {
    TestTarget owner = new TestTarget();
    AtomicLong clock = new AtomicLong(1000);
    RuntimeDopController controller = controller(owner, clock, 1);
    controller.close();
    controller.getStoppedFuture().get(5, TimeUnit.SECONDS);
    controller.start();
    sample(controller, clock);
    controller.close();
    assertEquals(0, owner.samples.get());
    assertTrue(owner.resizes.isEmpty());
    assertTrue(controller.snapshotEvents().isEmpty());
    assertTrue(owner.failures.isEmpty());
  }

  @Test
  public void terminalStopWaitsForActualSamplerExitAndRejectsConcurrentSamples() throws Exception {
    TestTarget owner = new TestTarget();
    AtomicLong clock = new AtomicLong(1000);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    owner.inputs =
        (now, sequence) -> {
          entered.countDown();
          await(release);
          return Map.of("scan", input(now, sequence, 2, 2, 2, 0));
        };
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (RuntimeDopController controller = controller(owner, clock, 1)) {
      clock.addAndGet(100);
      Future<?> running = executor.submit(controller::sampleOnce);
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      controller.sampleOnce();
      assertEquals(1, owner.samples.get());
      ListenableFuture<Void> cancelledObserver = controller.getStoppedFuture();
      controller.close();
      assertFalse(cancelledObserver.isDone());
      assertTrue(cancelledObserver.cancel(true));
      ListenableFuture<Void> stopped = controller.getStoppedFuture();
      assertFalse(stopped.isDone());
      controller.sampleOnce();
      assertEquals(1, owner.samples.get());
      release.countDown();
      running.get(5, TimeUnit.SECONDS);
      stopped.get(5, TimeUnit.SECONDS);
      assertFalse(stopped.isCancelled());
      assertTrue(owner.resizes.isEmpty());
      assertTrue(controller.snapshotEvents().isEmpty());
      assertTrue(owner.failures.isEmpty());
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void terminalStopWaitsForInProgressResizeInvocationToActuallyReturn() throws Exception {
    TestTarget owner = new TestTarget();
    AtomicLong clock = new AtomicLong(1000);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    owner.inputs = (now, sequence) -> Map.of("scan", input(now, sequence, 2, 2, 2, 0));
    owner.resizer =
        (group, workers) -> {
          entered.countDown();
          await(release);
          return Futures.immediateVoidFuture();
        };
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (RuntimeDopController controller = controller(owner, clock, 1)) {
      sample(controller, clock);
      clock.addAndGet(100);
      Future<?> running = executor.submit(controller::sampleOnce);
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      controller.close();
      ListenableFuture<Void> stopped = controller.getStoppedFuture();
      assertFalse(stopped.isDone());
      assertFalse(running.isDone());
      release.countDown();
      running.get(5, TimeUnit.SECONDS);
      stopped.get(5, TimeUnit.SECONDS);
      assertEquals(List.of(new Resize("scan", 3)), owner.resizes);
      assertTrue(owner.failures.isEmpty());
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void controlEvidenceKeepsOnlyNewest128ImmutableEvents() throws Exception {
    TestTarget owner = new TestTarget();
    AtomicLong clock = new AtomicLong(1000);
    owner.inputs = (now, sequence) -> Map.of("scan", input(now, sequence, 2, 1, 0, 0));
    try (RuntimeDopController controller = controller(owner, clock, 2)) {
      for (int i = 0; i < 130; i++) {
        sample(controller, clock);
      }
      List<RuntimeDopController.Event> snapshot = controller.snapshotEvents();
      assertEquals(128, snapshot.size());
      assertEquals(3L, snapshot.get(0).sampleSequence());
      assertEquals(130L, snapshot.get(127).sampleSequence());
      assertEquals(300L, snapshot.get(0).sampleNanos());
      assertEquals(13000L, snapshot.get(127).sampleNanos());
      assertThrows(UnsupportedOperationException.class, snapshot::clear);
      sample(controller, clock);
      assertEquals(3L, snapshot.get(0).sampleSequence());
      assertEquals(130L, snapshot.get(127).sampleSequence());
      assertEquals(4L, controller.snapshotEvents().get(0).sampleSequence());
      assertEquals(131L, controller.snapshotEvents().get(127).sampleSequence());
      assertTrue(owner.resizes.isEmpty());
      assertTrue(owner.failures.isEmpty());
    }
  }

  @Test
  public void asynchronousResizeFailureIsReportedToOwner() throws Exception {
    TestTarget owner = new TestTarget();
    AtomicLong clock = new AtomicLong(1000);
    SettableFuture<Void> resizeReceipt = SettableFuture.create();
    RuntimeException expected = new RuntimeException("resize failed");
    owner.inputs = (now, sequence) -> Map.of("scan", input(now, sequence, 2, 2, 2, 0));
    owner.resizer = (group, workers) -> resizeReceipt;
    try (RuntimeDopController controller = controller(owner, clock, 1)) {
      sample(controller, clock);
      sample(controller, clock);
      assertEquals(List.of(new Resize("scan", 3)), owner.resizes);
      assertTrue(owner.failures.isEmpty());
      resizeReceipt.setException(expected);
      assertEquals(1, owner.failures.size());
      assertSame(expected, owner.failures.get(0).getCause());
      controller.close();
      controller.getStoppedFuture().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void synchronousResizeFailureIsReportedWithoutLeakingSamplerOwnership() throws Exception {
    TestTarget owner = new TestTarget();
    AtomicLong clock = new AtomicLong(1000);
    RuntimeException expected = new RuntimeException("resize threw");
    owner.inputs = (now, sequence) -> Map.of("scan", input(now, sequence, 2, 2, 2, 0));
    owner.resizer =
        (group, workers) -> {
          throw expected;
        };
    try (RuntimeDopController controller = controller(owner, clock, 1)) {
      sample(controller, clock);
      sample(controller, clock);
      assertEquals(1, owner.failures.size());
      assertSame(expected, owner.failures.get(0));
      assertEquals(List.of(new Resize("scan", 3)), owner.resizes);
      controller.close();
      controller.getStoppedFuture().get(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void pendingResizeMakesLaterGroupsObserveResizeInProgressWithinSameSample()
      throws Exception {
    TestTarget owner = new TestTarget();
    AtomicLong clock = new AtomicLong(1000);
    SettableFuture<Void> pendingResize = SettableFuture.create();
    owner.inputs =
        (now, sequence) -> {
          Map<String, Input> inputs = new java.util.LinkedHashMap<>();
          inputs.put("first", groupInput("first", now, sequence, 3, 0, 0, 3));
          inputs.put("second", groupInput("second", now, sequence, 3, 0, 0, 3));
          return inputs;
        };
    owner.resizer = (group, workers) -> pendingResize;
    try (RuntimeDopController controller = controller(owner, clock, 1)) {
      sample(controller, clock);
      assertTrue(owner.resizes.isEmpty());
      sample(controller, clock);
      assertFalse(pendingResize.isDone());
      assertEquals(List.of(new Resize("first", 2)), owner.resizes);
      assertTrue(owner.failures.isEmpty());
      List<RuntimeDopController.Event> evidence = controller.snapshotEvents();
      assertEquals(4, evidence.size());
      assertEquals("first", evidence.get(2).groupId());
      assertEquals(Action.SHRINK, evidence.get(2).action());
      assertEquals("second", evidence.get(3).groupId());
      assertEquals(Action.KEEP, evidence.get(3).action());
      assertEquals(Reason.RESIZE_IN_PROGRESS, evidence.get(3).reason());
      pendingResize.set(null);
    }
  }

  @Test
  public void terminalResizeCancellationIsSuppressedButRealFailuresStillReachOwner()
      throws Exception {
    for (boolean asynchronous : new boolean[] {false, true}) {
      for (boolean cancellation : new boolean[] {true, false}) {
        TestTarget owner = new TestTarget();
        AtomicLong clock = new AtomicLong(1000);
        SettableFuture<Void> pendingResize = SettableFuture.create();
        java.util.concurrent.atomic.AtomicReference<RuntimeDopController> selected =
            new java.util.concurrent.atomic.AtomicReference<>();
        RuntimeException expected =
            cancellation
                ? new java.util.concurrent.CancellationException("terminal won resize race")
                : new IllegalStateException("real resize failure after terminal");
        owner.inputs = (now, sequence) -> Map.of("scan", input(now, sequence, 2, 2, 2, 0));
        owner.resizer =
            (group, workers) -> {
              if (asynchronous) {
                return pendingResize;
              }
              selected.get().close();
              throw expected;
            };
        try (RuntimeDopController controller = controller(owner, clock, 1)) {
          selected.set(controller);
          sample(controller, clock);
          sample(controller, clock);
          assertEquals(List.of(new Resize("scan", 3)), owner.resizes);
          if (asynchronous) {
            controller.close();
            if (cancellation) {
              assertTrue(pendingResize.cancel(false));
            } else {
              pendingResize.setException(expected);
            }
          }
          controller.getStoppedFuture().get(5, TimeUnit.SECONDS);
          if (cancellation) {
            assertTrue(owner.failures.isEmpty());
          } else {
            assertEquals(1, owner.failures.size());
            Throwable failure = owner.failures.get(0);
            assertSame(expected, asynchronous ? failure.getCause() : failure);
          }
        }
      }
    }
  }

  private static RuntimeDopController controller(
      TestTarget owner, AtomicLong clock, int requiredWindows) {
    return new RuntimeDopController(
        owner, new RuntimeDopPolicy(new Config(1, requiredWindows, 100, 500, 1000, 4)), clock::get);
  }

  private static void sample(RuntimeDopController controller, AtomicLong clock) {
    clock.addAndGet(100);
    controller.sampleOnce();
  }

  private static Input input(
      long now, long sequence, int workers, int active, int ready, int backpressured) {
    return groupInput("scan", now, sequence, workers, active, ready, backpressured);
  }

  private static Input groupInput(
      String group,
      long now,
      long sequence,
      int workers,
      int active,
      int ready,
      int backpressured) {
    return new Input(
        group,
        now,
        now,
        sequence,
        8,
        8,
        workers,
        active,
        ready,
        backpressured,
        8,
        MetricValue.valid(sequence * 10),
        MetricValue.valid(10000),
        false,
        false);
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new AssertionError(failure);
    }
  }

  private record Resize(String group, int workers) {}

  private static final class TestTarget implements RuntimeDopController.Target {
    final AtomicInteger samples = new AtomicInteger();
    final List<Resize> resizes = new CopyOnWriteArrayList<>();
    final List<Throwable> failures = new CopyOnWriteArrayList<>();
    BiFunction<Long, Long, Map<String, Input>> inputs =
        (now, sequence) -> Map.of("scan", input(now, sequence, 2, 1, 0, 0));
    BiFunction<String, Integer, ListenableFuture<?>> resizer =
        (group, workers) -> Futures.immediateVoidFuture();

    @Override
    public Map<String, Input> sampleControlInputs(long now, long sequence) {
      samples.incrementAndGet();
      return inputs.apply(now, sequence);
    }

    @Override
    public ListenableFuture<?> resize(String group, int workers) {
      resizes.add(new Resize(group, workers));
      return resizer.apply(group, workers);
    }

    @Override
    public void fail(Throwable failure) {
      failures.add(failure);
    }
  }
}
