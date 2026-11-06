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

package org.apache.iotdb.db.queryengine.statistics.snapshot;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RuntimeSnapshotDeltaTest {

  @Test
  public void testNonNegativeOutputRowsDelta() {
    QueryRuntimeSnapshot previous = snapshot(1, 10, 4);
    QueryRuntimeSnapshot current = snapshot(2, 20, 9);
    RuntimeSnapshotDelta delta = RuntimeSnapshotDelta.between(previous, current);
    assertEquals(Long.valueOf(5), delta.getOutputRowsByDriver().get("d"));
    assertTrue(delta.getValidity().isEmpty());
  }

  @Test
  public void testCounterRegressionIsInvalid() {
    RuntimeSnapshotDelta delta =
        RuntimeSnapshotDelta.between(snapshot(2, 20, 9), snapshot(3, 30, 4));
    assertTrue(delta.getValidity().contains(RuntimeSnapshotValidity.INVALID_VALUE));
  }

  @Test
  public void wallUsesSameDriverIntervalAndDuplicateGenerationIsRejected() {
    RuntimeSnapshotDelta delta =
        RuntimeSnapshotDelta.between(snapshot(1, 10, 4), snapshot(2, 20, 9));
    assertEquals(Long.valueOf(50), delta.getWallNanosByDriverKey().values().iterator().next());
    RuntimeSnapshotDelta repeated =
        RuntimeSnapshotDelta.between(snapshot(1, 10, 4), snapshot(1, 20, 9));
    assertTrue(repeated.getValidity().contains(RuntimeSnapshotValidity.STALE));
    assertTrue(repeated.getWallNanosByDriverKey().isEmpty());
  }

  @Test
  public void anotherQueryCannotProduceCounterDifferences() {
    QueryRuntimeSnapshot original = snapshot(1, 10, 4), other = snapshot(2, 20, 9);
    QueryRuntimeSnapshot renamed =
        new QueryRuntimeSnapshot("other", 2, 20, other.getFragments(), null, "checksum");
    RuntimeSnapshotDelta delta = RuntimeSnapshotDelta.between(original, renamed);
    assertTrue(delta.getValidity().contains(RuntimeSnapshotValidity.STALE));
    assertTrue(delta.getOutputRowsByDriverKey().isEmpty());
  }

  @Test
  public void equalTaskLabelsInDifferentFragmentsBothSurviveTheInterval() {
    QueryRuntimeSnapshot before =
        new QueryRuntimeSnapshot(
            "q",
            1,
            10,
            java.util.Arrays.asList(
                fragment("f1", 1, 4, 40L, "e", 1), fragment("f2", 1, 10, 100L, "e", 1)),
            null,
            "checksum");
    QueryRuntimeSnapshot after =
        new QueryRuntimeSnapshot(
            "q",
            2,
            20,
            java.util.Arrays.asList(
                fragment("f1", 2, 9, 90L, "e", 1), fragment("f2", 2, 12, 120L, "e", 1)),
            null,
            "checksum");
    RuntimeSnapshotDelta delta = RuntimeSnapshotDelta.between(before, after);
    assertTrue(delta.getValidity().isEmpty());
    assertEquals(2, delta.getOutputRowsByDriverKey().size());
    assertEquals(
        Long.valueOf(5),
        delta.getOutputRowsByDriverKey().get(new RuntimeSnapshotDriverKey("q", "f1", "p", "d")));
    assertEquals(
        Long.valueOf(2),
        delta.getOutputRowsByDriverKey().get(new RuntimeSnapshotDriverKey("q", "f2", "p", "d")));
    assertTrue(delta.getOutputRowsByDriver().isEmpty());
  }

  @Test
  public void missingWallPlanDriftEndpointChangeAndCounterRegressionRejectWholeInterval() {
    QueryRuntimeSnapshot before =
        new QueryRuntimeSnapshot(
            "q",
            1,
            10,
            Collections.singletonList(fragment("f", 1, 4, 40L, "e", 1)),
            null,
            "checksum");
    for (FragmentRuntimeSnapshot invalid :
        java.util.Arrays.asList(
            fragment("f", 2, 9, null, "e", 1),
            fragment("f", 2, 9, 90L, "e", 2),
            fragment("f", 2, 9, 90L, "other", 1),
            fragment("f", 2, 9, 30L, "e", 1))) {
      RuntimeSnapshotDelta delta =
          RuntimeSnapshotDelta.between(
              before,
              new QueryRuntimeSnapshot(
                  "q", 2, 20, Collections.singletonList(invalid), null, "checksum"));
      assertTrue(!delta.getValidity().isEmpty());
      assertTrue(delta.getWallNanosByDriverKey().isEmpty());
      assertTrue(delta.getOutputRowsByDriverKey().isEmpty());
    }
  }

  private static FragmentRuntimeSnapshot fragment(
      String id, long generation, long rows, Long wall, String endpoint, long planGeneration) {
    DriverRuntimeSnapshot driver =
        DriverRuntimeSnapshot.builder(id, "p", "d")
            .outputRows(rows)
            .wallNanos(wall)
            .sampleGeneration(generation)
            .build();
    return RuntimeSnapshotBuilder.buildFragment(
        "q",
        id,
        generation,
        generation * 10,
        "n",
        endpoint,
        "RUNNING",
        false,
        Collections.singletonList(driver),
        null,
        null,
        new StaticParallelPlanIdentity(
            "plan",
            "q",
            id,
            planGeneration,
            1,
            1,
            Collections.singletonList("work"),
            Collections.singletonList(0),
            Collections.emptyList(),
            "NONE"));
  }

  @Test
  public void tsBlockDifferencesUseTheSameStructuredIdentityAndRejectRegression() {
    RuntimeSnapshotDelta delta =
        RuntimeSnapshotDelta.between(snapshot(1, 10, 4, 2L), snapshot(2, 20, 9, 7L));
    assertTrue(delta.getValidity().isEmpty());
    assertEquals(
        Long.valueOf(5),
        delta.getTsBlocksByDriverKey().get(new RuntimeSnapshotDriverKey("q", "f", "p", "d")));
    for (Long after : new Long[] {1L, null}) {
      RuntimeSnapshotDelta invalid =
          RuntimeSnapshotDelta.between(snapshot(1, 10, 4, 2L), snapshot(2, 20, 9, after));
      assertTrue(!invalid.getValidity().isEmpty());
      assertTrue(invalid.getTsBlocksByDriverKey().isEmpty());
      assertTrue(invalid.getOutputRowsByDriverKey().isEmpty());
      assertTrue(invalid.getWallNanosByDriverKey().isEmpty());
    }
  }

  @Test
  public void absentBlockCountersStayAbsentWhileSupportedCountersRemainUsable() {
    RuntimeSnapshotDelta delta =
        RuntimeSnapshotDelta.between(snapshot(1, 10, 4), snapshot(2, 20, 9));
    assertTrue(delta.getValidity().isEmpty());
    assertTrue(delta.getTsBlocksByDriverKey().isEmpty());
    assertEquals(1, delta.getWallNanosByDriverKey().size());
  }

  private static QueryRuntimeSnapshot snapshot(long generation, long captured, long rows) {
    return snapshot(generation, captured, rows, null);
  }

  private static QueryRuntimeSnapshot snapshot(
      long generation, long captured, long rows, Long blocks) {
    DriverRuntimeSnapshot driver =
        DriverRuntimeSnapshot.builder("f", "p", "d")
            .outputRows(rows)
            .wallNanos(rows * 10)
            .tsBlocks(blocks)
            .sampleGeneration(generation)
            .build();
    FragmentRuntimeSnapshot fragment =
        RuntimeSnapshotBuilder.buildFragment(
            "q",
            "f",
            generation,
            captured,
            "n",
            "e",
            "RUNNING",
            false,
            Collections.singletonList(driver),
            null,
            null,
            new StaticParallelPlanIdentity(
                "plan",
                "q",
                "f",
                1,
                1,
                1,
                Collections.singletonList("work"),
                Collections.singletonList(0),
                Collections.emptyList(),
                "NONE"));
    return new QueryRuntimeSnapshot(
        "q", generation, captured, Collections.singletonList(fragment), null, "checksum");
  }
}
