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
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class StaticParallelPlanAccountingTest {

  @Test
  public void everySourceBelongsToOneNonEmptyBucket() {
    ScanParallelPlanner.Input input =
        new ScanParallelPlanner.Input(
            Arrays.asList("d0", "d1"),
            Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L),
            null,
            8,
            true,
            false,
            false,
            false,
            false,
            true,
            "q",
            "f",
            "p");
    ScanParallelPlan plan = new ScanParallelPlanner().plan(input);
    assertEquals(8, plan.getTargetParallelism());
    assertEquals(8, plan.getWorkUnits().size());
    assertEquals(8, plan.getBuckets().size());
    Set<String> workUnitIds = new HashSet<>();
    for (ScanParallelPlan.WorkUnit workUnit : plan.getWorkUnits()) {
      assertFalse("duplicate work unit", !workUnitIds.add(workUnit.getSourceId()));
    }
    Set<String> bucketIds = new HashSet<>();
    for (ScanParallelPlan.Bucket bucket : plan.getBuckets()) {
      assertFalse("empty bucket", bucket.getWorkUnits().isEmpty());
      for (ScanParallelPlan.WorkUnit workUnit : bucket.getWorkUnits()) {
        assertFalse("source assigned twice", !bucketIds.add(workUnit.getSourceId()));
      }
    }
    assertEquals(workUnitIds, bucketIds);
  }

  @Test
  public void overflowingCostFallsBackToSerial() {
    ScanParallelPlanner.Input input =
        new ScanParallelPlanner.Input(
            Arrays.asList("d0", "d1"),
            Arrays.asList(0L),
            Arrays.asList(Long.MAX_VALUE, Long.MAX_VALUE),
            2,
            true,
            false,
            false,
            false,
            false,
            false);
    ScanParallelPlan plan = new ScanParallelPlanner().plan(input);
    assertEquals(ScanParallelPlan.Mode.SERIAL, plan.getMode());
    assertEquals(ScanParallelPlan.FallbackReason.OVERFLOW, plan.getFallbackReason());
  }

  @Test
  public void invalidCostFallsBackWithoutPartialPlan() {
    ScanParallelPlanner.Input input =
        new ScanParallelPlanner.Input(
            Arrays.asList("d0", "d1"),
            Arrays.asList(0L),
            Arrays.asList(10L, -1L),
            2,
            true,
            false,
            false,
            false,
            false,
            false);
    ScanParallelPlan plan = new ScanParallelPlanner().plan(input);
    assertEquals(ScanParallelPlan.Mode.SERIAL, plan.getMode());
    assertEquals(ScanParallelPlan.FallbackReason.INVALID_COST, plan.getFallbackReason());
    assertEquals(1, plan.getBuckets().size());
    assertEquals(2, plan.getWorkUnits().get(0).getDevices().size());
  }
}
