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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.db.queryengine.plan.planner.DynamicParallelScheduleDecision.Action;

import java.math.BigInteger;
import java.util.Map;

/** Pure deterministic policy. Callers serialize state replacement for one bounded history key. */
public final class DynamicParallelScheduler {
  public static final class Result {
    private final DynamicParallelScheduleDecision decision;
    private final DynamicParallelPolicy.State state;

    Result(DynamicParallelScheduleDecision decision, DynamicParallelPolicy.State state) {
      this.decision = decision;
      this.state = state;
    }

    public DynamicParallelScheduleDecision getDecision() {
      return decision;
    }

    public DynamicParallelPolicy.State getState() {
      return state;
    }
  }

  public Result decide(DynamicParallelScheduleInput input, DynamicParallelPolicy.State previous) {
    long check = Math.addExact(previous.checkSequence, 1), last = previous.lastObservation;
    String a = previous.keyA, b = previous.keyB;
    int sign = previous.sign, streak = previous.streak;
    int cooldown = Math.max(0, previous.cooldown - 1);
    boolean cooling = previous.cooldown > 0, fresh = false;
    DynamicParallelDecisionReason reason =
        cooling
            ? DynamicParallelDecisionReason.COOLDOWN
            : DynamicParallelDecisionReason.NO_NEW_FEEDBACK;
    int cap = Math.max(1, Math.min(input.getInputDop(), Math.max(1, input.getWorkUnitCount())));
    long idleLast = previous.lastObservation;
    if (cooling) {
      for (DynamicParallelScheduleInput.Observation observation : input.getObservations()) {
        idleLast = Math.max(idleLast, observation.getSequence());
      }
    }
    DynamicParallelPolicy.State idleState =
        new DynamicParallelPolicy.State(check, idleLast, a, b, sign, streak, cooldown);
    ScanParallelPlan.FallbackReason guard = input.getQualificationReason();
    if (input.getInputDop() <= 1
        || input.getWorkUnitCount() == 0
        || guard != null && guard != ScanParallelPlan.FallbackReason.NONE) {
      return result(
          Action.SERIAL_FALLBACK,
          1,
          DynamicParallelDecisionReason.INPUT_QUALIFICATION_FAILED,
          idleState);
    }
    if (input.isAdmissionRejected()) {
      if (input.getPreviousDop() <= 1) {
        return result(
            Action.SERIAL_FALLBACK,
            1,
            DynamicParallelDecisionReason.MINIMUM_DOP_REJECTED,
            idleState);
      }
      return result(
          Action.REDUCE_DOP,
          Math.min(cap, input.getPreviousDop() - 1),
          DynamicParallelDecisionReason.ADMISSION_REJECTED,
          idleState);
    }

    // Process every newly received complete observation once. Arrival sequence is local and
    // monotonic.
    for (DynamicParallelScheduleInput.Observation observation : input.getObservations()) {
      if (observation.getSequence() <= last) {
        continue;
      }
      last = observation.getSequence();
      fresh = true;
      if (cooling) {
        continue;
      }
      long min = Long.MAX_VALUE, max = 0;
      int positive = 0;
      for (long cost : observation.getBucketCosts()) {
        if (cost > 0) {
          positive++;
          min = Math.min(min, cost);
          max = Math.max(max, cost);
        }
      }
      if (positive < 2) {
        streak = 0;
        a = null;
        b = null;
        sign = 0;
        reason = DynamicParallelDecisionReason.INSUFFICIENT_POSITIVE_COSTS;
        continue;
      }
      if (BigInteger.valueOf(max)
              .shiftLeft(1)
              .compareTo(BigInteger.valueOf(min).multiply(BigInteger.valueOf(3)))
          <= 0) {
        streak = 0;
        a = null;
        b = null;
        sign = 0;
        reason = DynamicParallelDecisionReason.RATIO_NOT_EXCEEDED;
        continue;
      }
      String heavy = null, light = null;
      long highest = Long.MIN_VALUE, lowest = Long.MAX_VALUE;
      for (Map.Entry<String, Long> entry : observation.getProjectedCosts().entrySet()) {
        long cost = entry.getValue();
        if (cost <= 0) {
          continue;
        }
        if (cost > highest) {
          highest = cost;
          heavy = entry.getKey();
        }
        if (cost < lowest) {
          lowest = cost;
          light = entry.getKey();
        }
      }
      if (heavy == null || light == null || highest == lowest) {
        streak = 0;
        a = null;
        b = null;
        sign = 0;
        reason = DynamicParallelDecisionReason.NO_DIRECTION;
        continue;
      }
      String nextA = heavy.compareTo(light) < 0 ? heavy : light,
          nextB = heavy.compareTo(light) < 0 ? light : heavy;
      int nextSign = heavy.equals(nextA) ? 1 : -1;
      if (nextA.equals(a) && nextB.equals(b) && sign != 0 && nextSign != sign) {
        cooling = true;
        cooldown = DynamicParallelPolicy.COOLDOWN_CHECKS - 1;
        streak = 0;
        a = null;
        b = null;
        sign = 0;
        reason = DynamicParallelDecisionReason.COOLDOWN;
      } else {
        boolean same = nextA.equals(a) && nextB.equals(b) && nextSign == sign;
        streak = same ? Math.min(DynamicParallelPolicy.REQUIRED_OBSERVATIONS, streak + 1) : 1;
        a = nextA;
        b = nextB;
        sign = nextSign;
        reason =
            streak >= DynamicParallelPolicy.REQUIRED_OBSERVATIONS
                ? DynamicParallelDecisionReason.COST_SKEW
                : DynamicParallelDecisionReason.INSUFFICIENT_FEEDBACK;
      }
    }
    DynamicParallelPolicy.State state =
        new DynamicParallelPolicy.State(check, last, a, b, sign, streak, cooldown);
    if (!cooling
        && fresh
        && streak >= DynamicParallelPolicy.REQUIRED_OBSERVATIONS
        && reason == DynamicParallelDecisionReason.COST_SKEW) {
      return result(
          Action.REBALANCE_NEXT_FRAGMENT, Math.min(cap, input.getPreviousDop()), reason, state);
    }
    return result(Action.KEEP_STATIC, cap, reason, state);
  }

  private static Result result(
      Action action,
      int target,
      DynamicParallelDecisionReason reason,
      DynamicParallelPolicy.State state) {
    return new Result(new DynamicParallelScheduleDecision(action, target, reason), state);
  }
}
