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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.db.queryengine.statistics.snapshot.RuntimeSnapshotValidity;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StaticScheduleInputValidityTest {
  @Test
  public void testMissingCostIsNotUsable() {
    StaticParallelScheduleInput input =
        new StaticParallelScheduleInput(
            4, Collections.emptyList(), "ESTIMATE_UNAVAILABLE", "ESTIMATE_UNAVAILABLE", null);
    assertFalse(input.hasCompleteCostEstimate());
    assertFalse(input.isUsableForScheduling());
    assertTrue(
        input
            .withValidity(RuntimeSnapshotValidity.NOT_FOUND)
            .getValidity()
            .contains(RuntimeSnapshotValidity.NOT_FOUND));
  }

  @Test
  public void testCompleteCostIsUsable() {
    StaticParallelScheduleInput input =
        new StaticParallelScheduleInput(4, Arrays.asList(10L, 20L), "NONE", "NONE", null);
    assertTrue(input.hasCompleteCostEstimate());
    assertTrue(input.isUsableForScheduling());
  }
}
