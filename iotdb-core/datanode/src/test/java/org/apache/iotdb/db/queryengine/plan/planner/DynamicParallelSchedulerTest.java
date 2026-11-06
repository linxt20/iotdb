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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class DynamicParallelSchedulerTest {
  static DynamicParallelScheduleInput.Observation observation(long sequence, long a, long b) {
    return new DynamicParallelScheduleInput.Observation(
        sequence, Map.of("a", a, "b", b), List.of(a, b));
  }

  static DynamicParallelScheduleInput input(
      boolean rejection,
      ScanParallelPlan.FallbackReason guard,
      DynamicParallelScheduleInput.Observation... observations) {
    return new DynamicParallelScheduleInput(4, 4, 12, guard, rejection, List.of(observations));
  }

  @Test
  public void twoDifferentSameDirectionFeedbacksRebalanceThenDuplicatesKeep() {
    DynamicParallelScheduler s = new DynamicParallelScheduler();
    DynamicParallelScheduler.Result first =
        s.decide(
            input(false, null, observation(1, 9000, 1500)), DynamicParallelPolicy.State.empty());
    assertEquals(Action.KEEP_STATIC, first.getDecision().getAction());
    DynamicParallelScheduler.Result second =
        s.decide(
            input(false, null, observation(1, 9000, 1500), observation(2, 9000, 1500)),
            first.getState());
    assertEquals(Action.REBALANCE_NEXT_FRAGMENT, second.getDecision().getAction());
    assertEquals(4, second.getDecision().getTargetDop());
    assertEquals(
        Action.KEEP_STATIC,
        s.decide(input(false, null, observation(2, 9000, 1500)), second.getState())
            .getDecision()
            .getAction());
  }

  @Test
  public void priorityIsSafetyThenAdmissionThenCosts() {
    DynamicParallelScheduler s = new DynamicParallelScheduler();
    DynamicParallelScheduleInput in =
        input(true, null, observation(1, 9000, 1500), observation(2, 9000, 1500));
    assertEquals(
        Action.REDUCE_DOP,
        s.decide(in, new DynamicParallelPolicy.State(0, 0, null, null, 0, 0, 2))
            .getDecision()
            .getAction());
    assertEquals(3, s.decide(in, DynamicParallelPolicy.State.empty()).getDecision().getTargetDop());
    assertEquals(
        Action.SERIAL_FALLBACK,
        s.decide(
                input(
                    true,
                    ScanParallelPlan.FallbackReason.SPILLED_DEVICE_ENTRIES,
                    observation(1, 9000, 1500),
                    observation(2, 9000, 1500)),
                DynamicParallelPolicy.State.empty())
            .getDecision()
            .getAction());
  }

  @Test
  public void estimateCapAndMinimumAdmissionAreDeterministic() {
    DynamicParallelScheduler s = new DynamicParallelScheduler();
    assertEquals(
        2,
        s.decide(
                new DynamicParallelScheduleInput(2, 4, 12, null, true, List.of()),
                DynamicParallelPolicy.State.empty())
            .getDecision()
            .getTargetDop());
    assertEquals(
        Action.SERIAL_FALLBACK,
        s.decide(
                new DynamicParallelScheduleInput(4, 1, 12, null, true, List.of()),
                DynamicParallelPolicy.State.empty())
            .getDecision()
            .getAction());
  }

  @Test
  public void exactThresholdAndZeroBuckets() {
    DynamicParallelScheduler s = new DynamicParallelScheduler();
    assertEquals(
        Action.KEEP_STATIC,
        s.decide(
                input(false, null, observation(1, 9000, 6000), observation(2, 9000, 6000)),
                DynamicParallelPolicy.State.empty())
            .getDecision()
            .getAction());
    DynamicParallelScheduleInput.Observation a =
        new DynamicParallelScheduleInput.Observation(
            1, Map.of("a", 9000L, "b", 1500L, "c", 0L), List.of(9000L, 1500L, 0L));
    DynamicParallelScheduleInput.Observation b =
        new DynamicParallelScheduleInput.Observation(2, a.getProjectedCosts(), a.getBucketCosts());
    assertEquals(
        Action.REBALANCE_NEXT_FRAGMENT,
        s.decide(input(false, null, a, b), DynamicParallelPolicy.State.empty())
            .getDecision()
            .getAction());
    assertEquals(
        Action.KEEP_STATIC,
        s.decide(
                input(false, null, observation(1, 9000, 0), observation(2, 9000, 0)),
                DynamicParallelPolicy.State.empty())
            .getDecision()
            .getAction());
  }

  @Test
  public void ratioDoesNotOverflowOrRoundAtLongBoundary() {
    DynamicParallelScheduler s = new DynamicParallelScheduler();
    assertEquals(
        Action.REBALANCE_NEXT_FRAGMENT,
        s.decide(
                input(
                    false,
                    null,
                    observation(1, Long.MAX_VALUE, 1),
                    observation(2, Long.MAX_VALUE, 1)),
                DynamicParallelPolicy.State.empty())
            .getDecision()
            .getAction());
  }

  @Test
  public void inputOwnsCollectionsAndSortsLocalArrivalSequence() {
    Map<String, Long> values = new java.util.HashMap<>(Map.of("a", 9L, "b", 1L));
    List<Long> costs = new ArrayList<>(List.of(9L, 1L));
    DynamicParallelScheduleInput.Observation first =
        new DynamicParallelScheduleInput.Observation(1, values, costs);
    values.clear();
    costs.clear();
    DynamicParallelScheduleInput in = input(false, null, observation(2, 9, 1), first);
    assertEquals(1, in.getObservations().get(0).getSequence());
    assertEquals(2, first.getProjectedCosts().size());
    assertEquals(2, first.getBucketCosts().size());
  }
}
