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

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RuntimeSnapshotPlanIdentityTest {
  @Test
  public void testIdentityIsExposedByReadOnlyView() {
    StaticParallelPlanIdentity identity =
        new StaticParallelPlanIdentity(
            "plan-sha",
            "q",
            "f",
            1L,
            2,
            3,
            Arrays.asList("w0", "w1"),
            Arrays.asList(0, 1),
            Collections.singletonList("edge0"),
            "NONE");
    FragmentRuntimeSnapshot fragment =
        RuntimeSnapshotBuilder.buildFragment(
            "q",
            "f",
            1,
            2,
            "n",
            "e",
            "FINISHED",
            true,
            Collections.singletonList(DriverRuntimeSnapshot.builder("f", "p", "d").build()),
            null,
            null,
            identity);
    RuntimeSnapshotView view =
        new RuntimeSnapshotView(
            RuntimeSnapshotMerger.merge(
                "q",
                1,
                2,
                Collections.singletonList(fragment),
                Collections.singleton("f"),
                Collections.emptySet()));
    assertEquals(identity, view.getPlanIdentity("f"));
    assertEquals(1, view.getActualDriverCount("f"));
    assertEquals(Arrays.asList("w0", "w1"), view.getWorkUnitIds("f"));
    assertEquals(Arrays.asList(0, 1), view.getBucketIds("f"));
  }

  @Test
  public void testMergerRejectsEndpointAndPayloadChecksumMismatch() {
    FragmentRuntimeSnapshot fragment =
        RuntimeSnapshotBuilder.buildFragment(
                "q", "f", 1, 2, "n", "e1", "FINISHED", true, null, null, null)
            .withChecksum("tampered");
    QueryRuntimeSnapshot merged =
        RuntimeSnapshotMerger.merge(
            "q",
            1,
            2,
            Collections.singletonList(fragment),
            Collections.singleton("f"),
            Collections.emptySet(),
            "e2");
    assertTrue(merged.getValidity().contains(RuntimeSnapshotValidity.ENDPOINT_MISMATCH));
    assertTrue(merged.getValidity().contains(RuntimeSnapshotValidity.CHECKSUM_MISMATCH));
  }
}
