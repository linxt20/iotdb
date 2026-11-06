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

import static org.junit.Assert.assertEquals;

public class StaticParallelPlanDeterminismTest {

  @Test
  public void repeatedPlanningProducesSameIdentityAndBuckets() {
    ScanParallelPlanner.Input input =
        new ScanParallelPlanner.Input(
            Arrays.asList("d0", "d1", "d2", "d3"),
            Arrays.asList(0L, 1L, 2L),
            Arrays.asList(8L, 3L, 5L, 1L),
            3,
            true,
            false,
            false,
            false,
            false,
            true,
            "q0",
            "f0",
            "p0");
    ScanParallelPlanner planner = new ScanParallelPlanner();
    ScanParallelPlan first = planner.plan(input);
    ScanParallelPlan second = planner.plan(input);
    assertEquals(first.getPlanIdentityChecksum(), second.getPlanIdentityChecksum());
    assertEquals(first.getBuckets().size(), second.getBuckets().size());
    for (int i = 0; i < first.getBuckets().size(); i++) {
      assertEquals(
          first.getBuckets().get(i).getWorkUnits().size(),
          second.getBuckets().get(i).getWorkUnits().size());
      for (int j = 0; j < first.getBuckets().get(i).getWorkUnits().size(); j++) {
        assertEquals(
            first.getBuckets().get(i).getWorkUnits().get(j).getSourceId(),
            second.getBuckets().get(i).getWorkUnits().get(j).getSourceId());
      }
    }
  }
}
