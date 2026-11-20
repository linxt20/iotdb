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

import java.util.concurrent.TimeUnit;

/** Pure scan-worker policy. The caller owns sampling, admission, actuation, and resource leases. */
public final class RuntimeDopPolicy {
  public enum Action {
    KEEP,
    GROW,
    SHRINK
  }

  public enum Coverage {
    VALID,
    UNKNOWN,
    UNSUPPORTED
  }

  public enum Reason {
    SHARED_PRESSURE_CONTRACTION,
    SHARED_PRESSURE_HOLD,
    SHARED_PRESSURE_RECOVERY,
    INVALID_INPUT,
    TERMINAL,
    FIRST_SAMPLE,
    RESET_IDENTITY,
    FUTURE_SAMPLE,
    STALE_SAMPLE,
    DUPLICATE_SAMPLE,
    RESET_SAMPLE_SEQUENCE,
    NON_MONOTONIC_SAMPLE_TIME,
    NON_MONOTONIC_CLOCK,
    WINDOW_TOO_SHORT,
    RESIZE_IN_PROGRESS,
    NO_UNFINISHED_LANES,
    RESOURCE_CAP_CONTRACTION,
    UNFINISHED_LANE_CONTRACTION,
    COMPLETED_WORK_UNKNOWN,
    COMPLETED_WORK_UNSUPPORTED,
    RESET_PROGRESS_BASELINE,
    COMPLETED_COUNTER_RESET,
    TOTAL_WORK_CHANGED,
    WORKER_COUNT_CHANGED,
    COOLDOWN,
    NO_PROGRESS,
    MIXED_PRESSURE,
    NO_PRESSURE,
    TOTAL_WORK_UNKNOWN,
    TOTAL_WORK_UNSUPPORTED,
    NEAR_EOF,
    RESOURCE_GROWTH_LIMIT,
    UNFINISHED_LANE_GROWTH_LIMIT,
    MINIMUM_WORKERS,
    HYSTERESIS,
    READY_BACKLOG,
    SUSTAINED_BACKPRESSURE
  }

  /** Missing measurements have no numeric value, so they cannot silently become zero. */
  public record MetricValue(Coverage coverage, Long value) {
    public MetricValue {
      if (coverage == null
          || (coverage == Coverage.VALID && (value == null || value < 0))
          || (coverage != Coverage.VALID && value != null)) {
        throw new IllegalArgumentException();
      }
    }

    public static MetricValue valid(long value) {
      return new MetricValue(Coverage.VALID, value);
    }

    public static MetricValue unknown() {
      return new MetricValue(Coverage.UNKNOWN, null);
    }

    public static MetricValue unsupported() {
      return new MetricValue(Coverage.UNSUPPORTED, null);
    }
  }

  public record Config(
      int maxStep,
      int requiredConsecutiveWindows,
      long minimumSampleWindowNanos,
      long cooldownNanos,
      long maxSampleAgeNanos,
      long nearEofWorkUnits) {
    public Config {
      if (maxStep < 1
          || requiredConsecutiveWindows < 1
          || minimumSampleWindowNanos < 1
          || cooldownNanos < 0
          || maxSampleAgeNanos < 0
          || nearEofWorkUnits < 0) {
        throw new IllegalArgumentException();
      }
    }

    public static Config conservativeDefaults() {
      return new Config(
          1,
          2,
          TimeUnit.MILLISECONDS.toNanos(100),
          TimeUnit.MILLISECONDS.toNanos(500),
          TimeUnit.SECONDS.toNanos(2),
          4);
    }
  }

  /**
   * All timestamps are monotonic elapsed nanos from one execution origin. maxResourceWorkers is a
   * growth permit, not revocation of grants already held by current workers. Lane counters describe
   * disjoint ready, leased, and backpressured states in one coherent source-group snapshot.
   */
  public record Input(
      String executionGroupId,
      long nowNanos,
      long sampleNanos,
      long sampleSequence,
      int logicalLaneCount,
      int unfinishedLaneCount,
      int currentWorkers,
      int activeLeases,
      int readyLaneCount,
      int backpressuredLaneCount,
      int maxResourceWorkers,
      MetricValue completedWork,
      MetricValue totalWork,
      boolean resizeInProgress,
      boolean terminal) {}

  public record Decision(
      Action action,
      int targetWorkers,
      Reason reason,
      Coverage completedCoverage,
      Coverage totalCoverage,
      Long completedWorkDelta) {}

  public record Result(Decision decision, State nextState) {}

  public static final class State {
    private final String executionGroupId;
    private final long sampleSequence;
    private final long sampleNanos;
    private final long lastObservedNowNanos;
    private final MetricValue completedWork;
    private final MetricValue totalWork;
    private final int currentWorkers;
    private final Action pressure;
    private final int consecutiveWindows;
    private final long lastActionNanos;

    private State(
        String executionGroupId,
        long sampleSequence,
        long sampleNanos,
        long lastObservedNowNanos,
        MetricValue completedWork,
        MetricValue totalWork,
        int currentWorkers,
        Action pressure,
        int consecutiveWindows,
        long lastActionNanos) {
      this.executionGroupId = executionGroupId;
      this.sampleSequence = sampleSequence;
      this.sampleNanos = sampleNanos;
      this.lastObservedNowNanos = lastObservedNowNanos;
      this.completedWork = completedWork;
      this.totalWork = totalWork;
      this.currentWorkers = currentWorkers;
      this.pressure = pressure;
      this.consecutiveWindows = consecutiveWindows;
      this.lastActionNanos = lastActionNanos;
    }

    public static State empty() {
      return new State(
          null, -1, -1, -1, MetricValue.unknown(), MetricValue.unknown(), 0, Action.KEEP, 0, -1);
    }

    public String executionGroupId() {
      return executionGroupId;
    }

    public long sampleSequence() {
      return sampleSequence;
    }

    public int consecutiveWindows() {
      return consecutiveWindows;
    }

    public long lastActionNanos() {
      return lastActionNanos;
    }
  }

  private final Config config;

  public RuntimeDopPolicy(Config config) {
    if (config == null) {
      throw new IllegalArgumentException();
    }
    this.config = config;
  }

  public Result evaluate(Input input, State previous) {
    return evaluate(input, previous, null);
  }

  Result evaluate(Input input, State previous, RuntimeSharedPressureFeedback.Advice advice) {
    State state = previous == null ? State.empty() : previous;
    if (!validInput(input)) {
      return keep(input, Reason.INVALID_INPUT, clearPressure(state), null);
    }
    if (input.terminal()) {
      return keep(input, Reason.TERMINAL, clearPressure(state), null);
    }
    if (input.sampleNanos() > input.nowNanos()) {
      return keep(input, Reason.FUTURE_SAMPLE, clearPressure(state), null);
    }
    if (input.nowNanos() - input.sampleNanos() > config.maxSampleAgeNanos()) {
      return keep(input, Reason.STALE_SAMPLE, clearPressure(state), null);
    }
    if (state.executionGroupId == null) {
      return keep(input, Reason.FIRST_SAMPLE, baseline(input, -1), null);
    }
    if (!state.executionGroupId.equals(input.executionGroupId())) {
      return keep(input, Reason.RESET_IDENTITY, baseline(input, -1), null);
    }
    if (input.nowNanos() < state.lastObservedNowNanos) {
      return keep(input, Reason.NON_MONOTONIC_CLOCK, clearPressure(state), null);
    }
    if (input.sampleSequence() == state.sampleSequence) {
      return keep(input, Reason.DUPLICATE_SAMPLE, state, null);
    }
    if (input.sampleSequence() < state.sampleSequence) {
      return keep(
          input, Reason.RESET_SAMPLE_SEQUENCE, baseline(input, state.lastActionNanos), null);
    }
    if (input.sampleNanos() <= state.sampleNanos) {
      return keep(input, Reason.NON_MONOTONIC_SAMPLE_TIME, clearPressure(state), null);
    }
    if (input.sampleNanos() - state.sampleNanos < config.minimumSampleWindowNanos()) {
      return keep(input, Reason.WINDOW_TOO_SHORT, state, null);
    }
    State idle = baseline(input, state.lastActionNanos);
    if (input.resizeInProgress()) {
      return keep(input, Reason.RESIZE_IN_PROGRESS, idle, null);
    }
    if (input.unfinishedLaneCount() == 0) {
      return keep(input, Reason.NO_UNFINISHED_LANES, idle, null);
    }

    // These are bounded retirement requests. Existing grants remain owned until actuation finishes.
    int resourceFloor = Math.max(1, input.maxResourceWorkers());
    if (input.currentWorkers() > resourceFloor) {
      return shrinkTo(input, resourceFloor, Reason.RESOURCE_CAP_CONTRACTION);
    }
    if (input.currentWorkers() > input.unfinishedLaneCount()) {
      return shrinkTo(input, input.unfinishedLaneCount(), Reason.UNFINISHED_LANE_CONTRACTION);
    }
    if (input.completedWork().coverage() != Coverage.VALID) {
      return keep(
          input,
          input.completedWork().coverage() == Coverage.UNKNOWN
              ? Reason.COMPLETED_WORK_UNKNOWN
              : Reason.COMPLETED_WORK_UNSUPPORTED,
          idle,
          null);
    }
    if (state.completedWork.coverage() != Coverage.VALID) {
      return keep(input, Reason.RESET_PROGRESS_BASELINE, idle, null);
    }
    if (input.completedWork().value() < state.completedWork.value()) {
      return keep(input, Reason.COMPLETED_COUNTER_RESET, idle, null);
    }
    if (input.totalWork().coverage() == Coverage.VALID
        && state.totalWork.coverage() == Coverage.VALID
        && !input.totalWork().value().equals(state.totalWork.value())) {
      return keep(input, Reason.TOTAL_WORK_CHANGED, idle, null);
    }
    if (input.currentWorkers() != state.currentWorkers) {
      return keep(input, Reason.WORKER_COUNT_CHANGED, idle, null);
    }
    long delta = input.completedWork().value() - state.completedWork.value();
    if (state.lastActionNanos >= 0
        && input.nowNanos() - state.lastActionNanos < config.cooldownNanos()) {
      return keep(input, Reason.COOLDOWN, idle, delta);
    }

    boolean contended =
        advice != null && advice.phase() == RuntimeSharedPressureFeedback.Phase.CONTENDED;
    if (contended && input.currentWorkers() > advice.width()) {
      return shrinkTo(input, advice.width(), Reason.SHARED_PRESSURE_CONTRACTION);
    }
    if (advice != null
        && advice.phase() == RuntimeSharedPressureFeedback.Phase.RECOVERING
        && delta > 0
        && input.backpressuredLaneCount() == 0
        && input.readyLaneCount() > 0
        && input.totalWork().coverage() == Coverage.VALID
        && input.totalWork().value() - input.completedWork().value() > config.nearEofWorkUnits()) {
      long goal =
          Math.min(
              Math.min(advice.width(), input.maxResourceWorkers()), input.unfinishedLaneCount());
      if (goal > input.currentWorkers()) {
        int target = (int) Math.min(goal, (long) input.currentWorkers() + config.maxStep());
        return result(
            input,
            Action.GROW,
            target,
            Reason.SHARED_PRESSURE_RECOVERY,
            baseline(input, input.nowNanos()),
            delta);
      }
    }

    Action pressure = Action.KEEP;
    Reason actionReason = Reason.NO_PRESSURE;
    if (input.readyLaneCount() > 0 && input.backpressuredLaneCount() > 0) {
      return keep(input, Reason.MIXED_PRESSURE, idle, delta);
    }
    if (input.readyLaneCount() >= input.currentWorkers()) {
      if (contended) {
        return keep(input, Reason.SHARED_PRESSURE_HOLD, idle, delta);
      }
      if (delta == 0) {
        return keep(input, Reason.NO_PROGRESS, idle, delta);
      }
      if (input.totalWork().coverage() != Coverage.VALID) {
        return keep(
            input,
            input.totalWork().coverage() == Coverage.UNKNOWN
                ? Reason.TOTAL_WORK_UNKNOWN
                : Reason.TOTAL_WORK_UNSUPPORTED,
            idle,
            delta);
      }
      if (input.totalWork().value() - input.completedWork().value() <= config.nearEofWorkUnits()) {
        return keep(input, Reason.NEAR_EOF, idle, delta);
      }
      if (input.currentWorkers() >= input.maxResourceWorkers()) {
        return keep(input, Reason.RESOURCE_GROWTH_LIMIT, idle, delta);
      }
      if (input.currentWorkers() >= input.unfinishedLaneCount()) {
        return keep(input, Reason.UNFINISHED_LANE_GROWTH_LIMIT, idle, delta);
      }
      pressure = Action.GROW;
      actionReason = Reason.READY_BACKLOG;
    } else if (input.readyLaneCount() == 0
        && input.backpressuredLaneCount() >= input.currentWorkers()) {
      if (input.currentWorkers() == 1) {
        return keep(input, Reason.MINIMUM_WORKERS, idle, delta);
      }
      pressure = Action.SHRINK;
      actionReason = Reason.SUSTAINED_BACKPRESSURE;
    }
    if (pressure == Action.KEEP) {
      return keep(input, Reason.NO_PRESSURE, idle, delta);
    }
    int windows =
        state.pressure == pressure
            ? (int)
                Math.min(config.requiredConsecutiveWindows(), (long) state.consecutiveWindows + 1)
            : 1;
    State observed = snapshot(input, pressure, windows, state.lastActionNanos);
    if (windows < config.requiredConsecutiveWindows()) {
      return keep(input, Reason.HYSTERESIS, observed, delta);
    }
    int target =
        pressure == Action.GROW
            ? (int)
                Math.min(
                    Math.min(input.maxResourceWorkers(), input.unfinishedLaneCount()),
                    (long) input.currentWorkers() + config.maxStep())
            : (int) Math.max(1L, (long) input.currentWorkers() - config.maxStep());
    return result(input, pressure, target, actionReason, baseline(input, input.nowNanos()), delta);
  }

  private Result shrinkTo(Input input, int floor, Reason reason) {
    int target = (int) Math.max(floor, (long) input.currentWorkers() - config.maxStep());
    return result(input, Action.SHRINK, target, reason, baseline(input, input.nowNanos()), null);
  }

  private static boolean validInput(Input input) {
    if (input == null
        || input.executionGroupId() == null
        || input.executionGroupId().isEmpty()
        || input.nowNanos() < 0
        || input.sampleNanos() < 0
        || input.sampleSequence() < 0
        || input.logicalLaneCount() < 0
        || input.unfinishedLaneCount() < 0
        || input.unfinishedLaneCount() > input.logicalLaneCount()
        || input.currentWorkers() < 0
        || input.currentWorkers() > input.logicalLaneCount()
        || (!input.terminal() && input.currentWorkers() == 0)
        || input.activeLeases() < 0
        || input.activeLeases() > input.currentWorkers()
        || input.readyLaneCount() < 0
        || input.backpressuredLaneCount() < 0
        || (long) input.activeLeases() + input.readyLaneCount() + input.backpressuredLaneCount()
            > input.unfinishedLaneCount()
        || input.maxResourceWorkers() < 0
        || input.completedWork() == null
        || input.totalWork() == null) {
      return false;
    }
    return input.completedWork().coverage() != Coverage.VALID
        || input.totalWork().coverage() != Coverage.VALID
        || input.completedWork().value() <= input.totalWork().value();
  }

  private static State baseline(Input input, long lastActionNanos) {
    return snapshot(input, Action.KEEP, 0, lastActionNanos);
  }

  private static State snapshot(Input input, Action pressure, int windows, long lastActionNanos) {
    return new State(
        input.executionGroupId(),
        input.sampleSequence(),
        input.sampleNanos(),
        input.nowNanos(),
        input.completedWork(),
        input.totalWork(),
        input.currentWorkers(),
        pressure,
        windows,
        lastActionNanos);
  }

  private static State clearPressure(State state) {
    return new State(
        state.executionGroupId,
        state.sampleSequence,
        state.sampleNanos,
        state.lastObservedNowNanos,
        state.completedWork,
        state.totalWork,
        state.currentWorkers,
        Action.KEEP,
        0,
        state.lastActionNanos);
  }

  private static Result keep(Input input, Reason reason, State state, Long delta) {
    return result(
        input,
        Action.KEEP,
        input == null ? 0 : Math.max(0, input.currentWorkers()),
        reason,
        state,
        delta);
  }

  private static Result result(
      Input input, Action action, int target, Reason reason, State state, Long delta) {
    return new Result(
        new Decision(
            action,
            target,
            reason,
            input == null || input.completedWork() == null
                ? Coverage.UNKNOWN
                : input.completedWork().coverage(),
            input == null || input.totalWork() == null
                ? Coverage.UNKNOWN
                : input.totalWork().coverage(),
            delta),
        state);
  }
}
