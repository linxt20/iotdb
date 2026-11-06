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

import org.junit.Test;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class StaticFallbackReasonContractTest {
  private final ScanParallelPlanner planner = new ScanParallelPlanner();

  @Test
  public void declaredRawReasonsKeepTheirSpecifiedCanonicalCategory() {
    Map<ScanParallelPlan.FallbackReason, String> expected =
        new EnumMap<>(ScanParallelPlan.FallbackReason.class);
    expected.put(ScanParallelPlan.FallbackReason.NONE, "NONE");
    expected.put(
        ScanParallelPlan.FallbackReason.DOP_NOT_GREATER_THAN_ONE, "PARALLEL_SCAN_NOT_SELECTED");
    expected.put(ScanParallelPlan.FallbackReason.SINGLE_DEVICE, "PARALLEL_SCAN_NOT_SELECTED");
    expected.put(
        ScanParallelPlan.FallbackReason.GLOBAL_LIMIT_OR_OFFSET, "PARALLEL_SCAN_NOT_SELECTED");
    expected.put(
        ScanParallelPlan.FallbackReason.PARALLEL_SCAN_NOT_ALLOWED, "PARALLEL_SCAN_NOT_SELECTED");
    expected.put(
        ScanParallelPlan.FallbackReason.PARALLEL_SCAN_NOT_SELECTED, "PARALLEL_SCAN_NOT_SELECTED");
    expected.put(ScanParallelPlan.FallbackReason.SPILLED_DEVICE_ENTRIES, "SPILLED_DEVICE_ENTRIES");
    expected.put(ScanParallelPlan.FallbackReason.ORDERING_NOT_PROVEN, "ORDERING_NOT_PROVEN");
    expected.put(ScanParallelPlan.FallbackReason.EMPTY_INPUT, "EMPTY_INPUT");
    expected.put(ScanParallelPlan.FallbackReason.INVALID_METADATA, "INVALID_COST");
    expected.put(ScanParallelPlan.FallbackReason.OVERFLOW, "INVALID_COST");
    expected.put(ScanParallelPlan.FallbackReason.ESTIMATE_UNAVAILABLE, "ESTIMATE_UNAVAILABLE");
    expected.put(ScanParallelPlan.FallbackReason.INVALID_COST, "INVALID_COST");
    expected.put(ScanParallelPlan.FallbackReason.PLAN_ERROR, "PLAN_ERROR");
    expected.put(
        ScanParallelPlan.FallbackReason.UNSUPPORTED_TIME_PARTITION_RANGE,
        "UNSUPPORTED_TIME_PARTITION_RANGE");
    expected.put(ScanParallelPlan.FallbackReason.DYNAMIC_GROUPING_BUDGET_EXCEEDED, "PLAN_ERROR");
    expected.put(ScanParallelPlan.FallbackReason.DYNAMIC_GROUPING_COVERAGE_MISMATCH, "PLAN_ERROR");
    assertEquals(EnumSet.allOf(ScanParallelPlan.FallbackReason.class), expected.keySet());
    for (ScanParallelPlan.FallbackReason reason : expected.keySet()) {
      ScanParallelPlan plan = withReason(reason);
      assertEquals(reason.name(), plan.getRawSplitReason());
      assertEquals(reason.name(), expected.get(reason), plan.getCanonicalFallbackReason());
    }
  }

  @Test
  public void realQualificationFallbacksKeepWorkAndDoNotClaimPlanningFailure() {
    List<ScanParallelPlanner.Input> inputs =
        List.of(
            input(List.of("d0", "d1"), 1, true, false),
            input(List.of("d0"), 4, true, false),
            input(List.of("d0", "d1"), 4, true, true),
            input(List.of("d0", "d1"), 4, false, false));
    List<ScanParallelPlan.FallbackReason> reasons =
        List.of(
            ScanParallelPlan.FallbackReason.DOP_NOT_GREATER_THAN_ONE,
            ScanParallelPlan.FallbackReason.SINGLE_DEVICE,
            ScanParallelPlan.FallbackReason.GLOBAL_LIMIT_OR_OFFSET,
            ScanParallelPlan.FallbackReason.PARALLEL_SCAN_NOT_ALLOWED);
    for (int i = 0; i < inputs.size(); i++) {
      ScanParallelPlan plan = planner.plan(inputs.get(i));
      assertEquals(ScanParallelPlan.Mode.SERIAL, plan.getMode());
      assertEquals(reasons.get(i), plan.getFallbackReason());
      assertEquals(reasons.get(i).name(), plan.getRawSplitReason());
      assertEquals("PARALLEL_SCAN_NOT_SELECTED", plan.getCanonicalFallbackReason());
      assertEquals(1, plan.getPipelineCount());
      assertEquals(
          i == 1 ? List.of(List.of("d0")) : List.of(List.of("d0", "d1")), plan.getDeviceGroups());
      assertEquals(List.of(List.of(0L, 1L)), plan.getTimePartitionGroups());
    }
  }

  @Test
  public void rawReasonsRemainDistinctInPlanIdentityWithinOneCanonicalCategory() {
    ScanParallelPlan dop = withReason(ScanParallelPlan.FallbackReason.DOP_NOT_GREATER_THAN_ONE);
    ScanParallelPlan device = withReason(ScanParallelPlan.FallbackReason.SINGLE_DEVICE);
    assertEquals(dop.getCanonicalFallbackReason(), device.getCanonicalFallbackReason());
    assertNotEquals(dop.getRawSplitReason(), device.getRawSplitReason());
    assertNotEquals(dop.getPlanIdentityChecksum(), device.getPlanIdentityChecksum());
    assertEquals(
        dop.getPlanIdentityChecksum(),
        withReason(ScanParallelPlan.FallbackReason.DOP_NOT_GREATER_THAN_ONE)
            .getPlanIdentityChecksum());
  }

  private ScanParallelPlan withReason(ScanParallelPlan.FallbackReason reason) {
    ScanParallelPlan baseline = planner.plan(input(List.of("d0", "d1"), 1, true, false));
    return new ScanParallelPlan(
        baseline.getMode(),
        reason,
        baseline.getInputDop(),
        baseline.getTargetParallelism(),
        baseline.getDeviceGroups(),
        baseline.getTimePartitionGroups(),
        baseline.getWorkUnits(),
        baseline.getBuckets());
  }

  private ScanParallelPlanner.Input input(
      List<String> devices, int dop, boolean allowed, boolean limit) {
    return new ScanParallelPlanner.Input(
        devices, Arrays.asList(0L, 1L), dop, allowed, false, limit, false, false, true);
  }
}
