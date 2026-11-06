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

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class DynamicParallelHysteresisTest {
  @Test
  public void reversalCoolsForExactlyThreeChecksAndNeedsTwoNewFeedbacks() {
    DynamicParallelScheduler s = new DynamicParallelScheduler();
    DynamicParallelPolicy.State state = DynamicParallelPolicy.State.empty();
    for (int i = 1; i <= 2; i++) {
      DynamicParallelScheduler.Result r =
          s.decide(
              DynamicParallelSchedulerTest.input(
                  false, null, DynamicParallelSchedulerTest.observation(i, 9000, 1500)),
              state);
      state = r.getState();
      assertEquals(
          i == 2 ? Action.REBALANCE_NEXT_FRAGMENT : Action.KEEP_STATIC,
          r.getDecision().getAction());
    }
    for (int i = 3; i <= 7; i++) {
      DynamicParallelScheduler.Result r =
          s.decide(
              DynamicParallelSchedulerTest.input(
                  false, null, DynamicParallelSchedulerTest.observation(i, 1500, 9000)),
              state);
      state = r.getState();
      assertEquals(
          i == 7 ? Action.REBALANCE_NEXT_FRAGMENT : Action.KEEP_STATIC,
          r.getDecision().getAction());
      assertEquals(i, state.getCheckSequence());
      if (i <= 5) {
        assertEquals(5 - i, state.getCooldownRemaining());
      }
    }
  }

  @Test
  public void aDifferentExtremeKeyPairResetsWithoutClaimingReversal() {
    DynamicParallelScheduler s = new DynamicParallelScheduler();
    DynamicParallelPolicy.State state =
        s.decide(
                DynamicParallelSchedulerTest.input(
                    false, null, DynamicParallelSchedulerTest.observation(1, 9000, 1500)),
                DynamicParallelPolicy.State.empty())
            .getState();
    DynamicParallelScheduleInput.Observation next =
        new DynamicParallelScheduleInput.Observation(
            2, Map.of("a", 1500L, "c", 9000L), List.of(1500L, 9000L));
    DynamicParallelScheduler.Result r =
        s.decide(DynamicParallelSchedulerTest.input(false, null, next), state);
    assertEquals(0, r.getState().getCooldownRemaining());
    assertEquals(1, r.getState().getConsecutiveObservations());
    assertEquals(Action.KEEP_STATIC, r.getDecision().getAction());
  }

  @Test
  public void memoryRejectionDoesNotEvaluateLowerPriorityCosts() {
    DynamicParallelScheduler s = new DynamicParallelScheduler();
    DynamicParallelPolicy.State state =
        s.decide(
                DynamicParallelSchedulerTest.input(
                    false,
                    null,
                    DynamicParallelSchedulerTest.observation(1, 9000, 1500),
                    DynamicParallelSchedulerTest.observation(2, 9000, 1500)),
                DynamicParallelPolicy.State.empty())
            .getState();
    DynamicParallelScheduler.Result r =
        s.decide(
            DynamicParallelSchedulerTest.input(
                true, null, DynamicParallelSchedulerTest.observation(3, 1500, 9000)),
            state);
    assertEquals(Action.REDUCE_DOP, r.getDecision().getAction());
    assertEquals(0, r.getState().getCooldownRemaining());
    assertEquals(state.getLastObservation(), r.getState().getLastObservation());
  }
}
