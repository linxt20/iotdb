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
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeDopPolicy.Coverage;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeDopPolicy.Input;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeDopPolicy.MetricValue;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeDopPolicy.Reason;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeDopPolicy.Result;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeDopPolicy.State;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

public class RuntimeDopPolicyTest {
  private final RuntimeDopPolicy policy = new RuntimeDopPolicy(new Config(1, 2, 100, 500, 1000, 4));

  @Test
  public void testFirstSampleEstablishesImmutableBaseline() {
    State empty = State.empty();
    Result result = policy.evaluate(sample(1, 10).build(), empty);
    assertDecision(result, Action.KEEP, Reason.FIRST_SAMPLE, 2);
    assertNull(result.decision().completedWorkDelta());
    assertEquals("query-a/source-a", result.nextState().executionGroupId());
    assertEquals(1, result.nextState().sampleSequence());
    assertEquals(-1, empty.sampleSequence());
    assertNotSame(empty, result.nextState());
  }

  @Test
  public void testSustainedReadyBacklogGrowsBySingleStep() {
    State first = baseline(sample(1, 10));
    Result second = policy.evaluate(sample(2, 20).build(), first);
    assertDecision(second, Action.KEEP, Reason.HYSTERESIS, 2);
    assertEquals(1, second.nextState().consecutiveWindows());
    Result third = policy.evaluate(sample(3, 30).build(), second.nextState());
    assertDecision(third, Action.GROW, Reason.READY_BACKLOG, 3);
    assertEquals(Long.valueOf(10), third.decision().completedWorkDelta());
    assertEquals(0, third.nextState().consecutiveWindows());
    assertEquals(300, third.nextState().lastActionNanos());
    assertEquals(1, second.nextState().consecutiveWindows());
  }

  @Test
  public void testDuplicateSampleDoesNotAdvanceHysteresis() {
    Result observed = policy.evaluate(sample(2, 20).build(), baseline(sample(1, 10)));
    Sample duplicate = sample(2, 999);
    duplicate.now = 500;
    Result repeated = policy.evaluate(duplicate.build(), observed.nextState());
    assertDecision(repeated, Action.KEEP, Reason.DUPLICATE_SAMPLE, 2);
    assertSame(observed.nextState(), repeated.nextState());
    assertEquals(1, repeated.nextState().consecutiveWindows());
    Result next = policy.evaluate(sample(3, 30).build(), repeated.nextState());
    assertDecision(next, Action.GROW, Reason.READY_BACKLOG, 3);
    assertEquals(Long.valueOf(10), next.decision().completedWorkDelta());
  }

  @Test
  public void testExecutionIdentityResetDiscardsOldDeltaAndCooldown() {
    RuntimeDopPolicy immediate = new RuntimeDopPolicy(new Config(1, 1, 100, 500, 1000, 4));
    State first = immediate.evaluate(sample(1, 10).build(), State.empty()).nextState();
    Result action = immediate.evaluate(sample(2, 20).build(), first);
    assertEquals(Action.GROW, action.decision().action());
    Sample other = sample(3, 1);
    other.group = "query-b/source-a";
    Result reset = immediate.evaluate(other.build(), action.nextState());
    assertDecision(reset, Action.KEEP, Reason.RESET_IDENTITY, 2);
    assertEquals(-1, reset.nextState().lastActionNanos());
    assertNull(reset.decision().completedWorkDelta());
    Sample next = sample(4, 2);
    next.group = other.group;
    assertDecision(
        immediate.evaluate(next.build(), reset.nextState()), Action.GROW, Reason.READY_BACKLOG, 3);
  }

  @Test
  public void testMissingCompletedMeasurementsHaveNoNumericZero() {
    for (MetricValue missing :
        new MetricValue[] {MetricValue.unknown(), MetricValue.unsupported()}) {
      Sample next = sample(2, 20);
      next.completed = missing;
      Result result = policy.evaluate(next.build(), baseline(sample(1, 10)));
      assertEquals(Action.KEEP, result.decision().action());
      assertEquals(missing.coverage(), result.decision().completedCoverage());
      assertNull(result.decision().completedWorkDelta());
      assertNull(missing.value());
      assertEquals(
          missing.coverage() == Coverage.UNKNOWN
              ? Reason.COMPLETED_WORK_UNKNOWN
              : Reason.COMPLETED_WORK_UNSUPPORTED,
          result.decision().reason());
    }
  }

  @Test
  public void testCounterResetAndRecoveredCoverageOnlyEstablishBaselines() {
    Result reset = policy.evaluate(sample(2, 1).build(), baseline(sample(1, 100)));
    assertDecision(reset, Action.KEEP, Reason.COMPLETED_COUNTER_RESET, 2);
    assertNull(reset.decision().completedWorkDelta());
    Result afterReset = policy.evaluate(sample(3, 2).build(), reset.nextState());
    assertEquals(Long.valueOf(1), afterReset.decision().completedWorkDelta());
    assertEquals(1, afterReset.nextState().consecutiveWindows());
    Sample missing = sample(1, 0);
    missing.completed = MetricValue.unknown();
    Result recovered = policy.evaluate(sample(2, 1).build(), baseline(missing));
    assertDecision(recovered, Action.KEEP, Reason.RESET_PROGRESS_BASELINE, 2);
    assertNull(recovered.decision().completedWorkDelta());
    assertEquals(0, recovered.nextState().consecutiveWindows());
  }

  @Test
  public void testStaleAndFutureSamplesDoNotChangeTheProgressBaseline() {
    State first = baseline(sample(1, 10));
    Sample stale = sample(2, 20);
    stale.now = 1201;
    Result staleResult = policy.evaluate(stale.build(), first);
    assertDecision(staleResult, Action.KEEP, Reason.STALE_SAMPLE, 2);
    assertEquals(1, staleResult.nextState().sampleSequence());
    Sample future = sample(2, 20);
    future.now = 199;
    Result futureResult = policy.evaluate(future.build(), first);
    assertDecision(futureResult, Action.KEEP, Reason.FUTURE_SAMPLE, 2);
    assertNull(futureResult.decision().completedWorkDelta());
    assertEquals(1, futureResult.nextState().sampleSequence());
  }

  @Test
  public void testSequenceClockAndSampleWindowGuards() {
    State first = baseline(sample(1, 10));
    Sample shortWindow = sample(2, 20);
    shortWindow.sampleTime = 150;
    shortWindow.now = 150;
    Result tooShort = policy.evaluate(shortWindow.build(), first);
    assertDecision(tooShort, Action.KEEP, Reason.WINDOW_TOO_SHORT, 2);
    assertSame(first, tooShort.nextState());
    Sample sameTime = sample(2, 20);
    sameTime.sampleTime = 100;
    assertDecision(
        policy.evaluate(sameTime.build(), first), Action.KEEP, Reason.NON_MONOTONIC_SAMPLE_TIME, 2);
    Sample laterClock = sample(1, 10);
    laterClock.now = 300;
    assertDecision(
        policy.evaluate(sample(2, 20).build(), baseline(laterClock)),
        Action.KEEP,
        Reason.NON_MONOTONIC_CLOCK,
        2);
    Result reset = policy.evaluate(sample(1, 30).at(1100).build(), baseline(sample(10, 20)));
    assertDecision(reset, Action.KEEP, Reason.RESET_SAMPLE_SEQUENCE, 2);
    assertEquals(1, reset.nextState().sampleSequence());
    assertNull(reset.decision().completedWorkDelta());
  }

  @Test
  public void testUnknownUnsupportedAndNearEofTotalsPreventGrowth() {
    for (MetricValue missing :
        new MetricValue[] {MetricValue.unknown(), MetricValue.unsupported()}) {
      Sample first = sample(1, 10);
      first.total = missing;
      Sample next = sample(2, 20);
      next.total = missing;
      Result result = policy.evaluate(next.build(), baseline(first));
      assertEquals(Action.KEEP, result.decision().action());
      assertEquals(missing.coverage(), result.decision().totalCoverage());
      assertEquals(
          missing.coverage() == Coverage.UNKNOWN
              ? Reason.TOTAL_WORK_UNKNOWN
              : Reason.TOTAL_WORK_UNSUPPORTED,
          result.decision().reason());
    }
    Result nearEof = policy.evaluate(sample(2, 996).build(), baseline(sample(1, 990)));
    assertDecision(nearEof, Action.KEEP, Reason.NEAR_EOF, 2);
    assertEquals(Long.valueOf(6), nearEof.decision().completedWorkDelta());
  }

  @Test
  public void testResourcePermitBoundsGrowthAndRequestsOnlyBoundedRetirement() {
    Sample first = sample(1, 10);
    first.cap = 2;
    Sample second = sample(2, 20);
    second.cap = 2;
    assertDecision(
        policy.evaluate(second.build(), baseline(first)),
        Action.KEEP,
        Reason.RESOURCE_GROWTH_LIMIT,
        2);
    Sample four = sample(1, 10);
    four.workers = 4;
    four.active = 4;
    four.ready = 4;
    Sample reduced = sample(2, 20);
    reduced.workers = 4;
    reduced.active = 4;
    reduced.ready = 4;
    reduced.cap = 0;
    Result contraction = policy.evaluate(reduced.build(), baseline(four));
    assertDecision(contraction, Action.SHRINK, Reason.RESOURCE_CAP_CONTRACTION, 3);
    assertEquals(4, reduced.active);
    assertEquals(4, reduced.workers);
    assertNull(contraction.decision().completedWorkDelta());
  }

  @Test
  public void testFourWorkersWithOneUnfinishedLaneRetireOnlyOneAtATime() {
    Sample first = sample(1, 10);
    first.workers = 4;
    first.active = 1;
    first.ready = 0;
    first.unfinished = 1;
    Sample next = sample(2, 20);
    next.workers = 4;
    next.active = 1;
    next.ready = 0;
    next.unfinished = 1;
    assertDecision(
        policy.evaluate(next.build(), baseline(first)),
        Action.SHRINK,
        Reason.UNFINISHED_LANE_CONTRACTION,
        3);
  }

  @Test
  public void testSustainedBackpressureShrinksButPreservesOneWorker() {
    Sample first = blocked(1, 10, 3);
    Result second = policy.evaluate(blocked(2, 20, 3).build(), baseline(first));
    assertDecision(second, Action.KEEP, Reason.HYSTERESIS, 3);
    assertDecision(
        policy.evaluate(blocked(3, 30, 3).build(), second.nextState()),
        Action.SHRINK,
        Reason.SUSTAINED_BACKPRESSURE,
        2);
    assertDecision(
        policy.evaluate(blocked(2, 20, 1).build(), baseline(blocked(1, 10, 1))),
        Action.KEEP,
        Reason.MINIMUM_WORKERS,
        1);
  }

  @Test
  public void testZeroActiveLeasesAloneDoesNotInferCpuIdle() {
    Sample first = sample(1, 10);
    first.active = 0;
    first.ready = 0;
    Sample next = sample(2, 20);
    next.active = 0;
    next.ready = 0;
    assertDecision(
        policy.evaluate(next.build(), baseline(first)), Action.KEEP, Reason.NO_PRESSURE, 2);
    Sample noWork = sample(2, 20);
    noWork.unfinished = 0;
    noWork.active = 0;
    noWork.ready = 0;
    assertDecision(
        policy.evaluate(noWork.build(), baseline(first)),
        Action.KEEP,
        Reason.NO_UNFINISHED_LANES,
        2);
  }

  @Test
  public void testCooldownRequiresFreshHysteresisAfterAnAction() {
    Result second = policy.evaluate(sample(2, 20).build(), baseline(sample(1, 10)));
    Result action = policy.evaluate(sample(3, 30).build(), second.nextState());
    Result cooled = policy.evaluate(sample(4, 40).build(), action.nextState());
    assertDecision(cooled, Action.KEEP, Reason.COOLDOWN, 2);
    assertEquals(0, cooled.nextState().consecutiveWindows());
    Result afterCooldown = policy.evaluate(sample(8, 80).build(), cooled.nextState());
    assertDecision(afterCooldown, Action.KEEP, Reason.HYSTERESIS, 2);
    assertDecision(
        policy.evaluate(sample(9, 90).build(), afterCooldown.nextState()),
        Action.GROW,
        Reason.READY_BACKLOG,
        3);
  }

  @Test
  public void testPendingResizeSuppressesGrowthAndForcedRetirement() {
    Sample pending = sample(2, 20);
    pending.pending = true;
    pending.cap = 0;
    Result result = policy.evaluate(pending.build(), baseline(sample(1, 10)));
    assertDecision(result, Action.KEEP, Reason.RESIZE_IN_PROGRESS, 2);
    assertEquals(0, result.nextState().consecutiveWindows());
    assertEquals(2, result.nextState().sampleSequence());
  }

  @Test
  public void testNoProgressAndMixedPressureDoNotAccumulateGrowth() {
    assertDecision(
        policy.evaluate(sample(2, 10).build(), baseline(sample(1, 10))),
        Action.KEEP,
        Reason.NO_PROGRESS,
        2);
    Sample mixed = sample(2, 20);
    mixed.backpressured = 1;
    assertDecision(
        policy.evaluate(mixed.build(), baseline(sample(1, 10))),
        Action.KEEP,
        Reason.MIXED_PRESSURE,
        2);
  }

  @Test
  public void testInvalidZeroWorkersAndIncoherentSnapshotsAreRejected() {
    Sample zeroWorkers = sample(1, 10);
    zeroWorkers.workers = 0;
    zeroWorkers.active = 0;
    assertDecision(
        policy.evaluate(zeroWorkers.build(), State.empty()), Action.KEEP, Reason.INVALID_INPUT, 0);
    Sample overlaps = sample(1, 10);
    overlaps.ready = 7;
    assertDecision(
        policy.evaluate(overlaps.build(), State.empty()), Action.KEEP, Reason.INVALID_INPUT, 2);
    Sample excessiveCompleted = sample(1, 1001);
    assertDecision(
        policy.evaluate(excessiveCompleted.build(), State.empty()),
        Action.KEEP,
        Reason.INVALID_INPUT,
        2);
    assertDecision(policy.evaluate(null, null), Action.KEEP, Reason.INVALID_INPUT, 0);
  }

  @Test
  public void testPressureReversalStartsNewHysteresis() {
    Result growthWindow = policy.evaluate(sample(2, 20).build(), baseline(sample(1, 10)));
    Result shrinkWindow = policy.evaluate(blocked(3, 30, 2).build(), growthWindow.nextState());
    assertDecision(shrinkWindow, Action.KEEP, Reason.HYSTERESIS, 2);
    assertEquals(1, shrinkWindow.nextState().consecutiveWindows());
    assertDecision(
        policy.evaluate(blocked(4, 40, 2).build(), shrinkWindow.nextState()),
        Action.SHRINK,
        Reason.SUSTAINED_BACKPRESSURE,
        1);
  }

  @Test
  public void testChangedTotalOrActualWorkerCountResetsFeedbackBaseline() {
    Sample changedTotal = sample(2, 20);
    changedTotal.total = MetricValue.valid(2000);
    Result totalResult = policy.evaluate(changedTotal.build(), baseline(sample(1, 10)));
    assertDecision(totalResult, Action.KEEP, Reason.TOTAL_WORK_CHANGED, 2);
    assertNull(totalResult.decision().completedWorkDelta());
    Sample changedWorkers = sample(2, 20);
    changedWorkers.workers = 3;
    changedWorkers.active = 3;
    Result workerResult = policy.evaluate(changedWorkers.build(), baseline(sample(1, 10)));
    assertDecision(workerResult, Action.KEEP, Reason.WORKER_COUNT_CHANGED, 3);
    assertNull(workerResult.decision().completedWorkDelta());
  }

  @Test
  public void testLongBoundariesTerminalAndMetricConfigurationValidation() {
    Sample first = sample(1, Long.MAX_VALUE - 20).at(Long.MAX_VALUE - 200);
    first.total = MetricValue.valid(Long.MAX_VALUE);
    Sample next = sample(2, Long.MAX_VALUE - 10).at(Long.MAX_VALUE - 100);
    next.total = MetricValue.valid(Long.MAX_VALUE);
    Result result = policy.evaluate(next.build(), baseline(first));
    assertDecision(result, Action.KEEP, Reason.HYSTERESIS, 2);
    assertEquals(Long.valueOf(10), result.decision().completedWorkDelta());
    Sample terminal = sample(1, 0);
    terminal.logical = 0;
    terminal.unfinished = 0;
    terminal.workers = 0;
    terminal.active = 0;
    terminal.ready = 0;
    terminal.terminal = true;
    assertDecision(
        policy.evaluate(terminal.build(), State.empty()), Action.KEEP, Reason.TERMINAL, 0);
    expectInvalid(() -> new Config(0, 2, 100, 500, 1000, 4));
    expectInvalid(() -> new Config(1, 0, 100, 500, 1000, 4));
    expectInvalid(() -> new Config(1, 2, 0, 500, 1000, 4));
    expectInvalid(() -> new Config(1, 2, 100, -1, 1000, 4));
    expectInvalid(() -> new Config(1, 2, 100, 500, -1, 4));
    expectInvalid(() -> new Config(1, 2, 100, 500, 1000, -1));
    expectInvalid(() -> new RuntimeDopPolicy(null));
    expectInvalid(() -> new MetricValue(Coverage.UNKNOWN, 0L));
    expectInvalid(() -> new MetricValue(Coverage.UNSUPPORTED, 0L));
    expectInvalid(() -> new MetricValue(Coverage.VALID, null));
    expectInvalid(() -> MetricValue.valid(-1));
    assertEquals(Long.valueOf(0), MetricValue.valid(0).value());
  }

  private State baseline(Sample sample) {
    return policy.evaluate(sample.build(), State.empty()).nextState();
  }

  private static Sample sample(long sequence, long completed) {
    return new Sample(sequence, completed);
  }

  private static Sample blocked(long sequence, long completed, int workers) {
    Sample sample = sample(sequence, completed);
    sample.workers = workers;
    sample.active = 0;
    sample.ready = 0;
    sample.backpressured = workers;
    return sample;
  }

  private static void assertDecision(Result result, Action action, Reason reason, int target) {
    assertEquals(action, result.decision().action());
    assertEquals(reason, result.decision().reason());
    assertEquals(target, result.decision().targetWorkers());
  }

  private static void expectInvalid(Runnable operation) {
    try {
      operation.run();
      fail();
    } catch (IllegalArgumentException expected) {
      // Expected constructor validation.
    }
  }

  private static final class Sample {
    private String group = "query-a/source-a";
    private long now;
    private long sampleTime;
    private final long sequence;
    private int logical = 8;
    private int unfinished = 8;
    private int workers = 2;
    private int active = 2;
    private int ready = 4;
    private int backpressured;
    private int cap = 8;
    private MetricValue completed;
    private MetricValue total = MetricValue.valid(1000);
    private boolean pending;
    private boolean terminal;

    private Sample(long sequence, long completed) {
      this.sequence = sequence;
      this.sampleTime = sequence * 100;
      this.now = sampleTime;
      this.completed = MetricValue.valid(completed);
    }

    private Sample at(long time) {
      now = time;
      sampleTime = time;
      return this;
    }

    private Input build() {
      return new Input(
          group,
          now,
          sampleTime,
          sequence,
          logical,
          unfinished,
          workers,
          active,
          ready,
          backpressured,
          cap,
          completed,
          total,
          pending,
          terminal);
    }
  }
}
