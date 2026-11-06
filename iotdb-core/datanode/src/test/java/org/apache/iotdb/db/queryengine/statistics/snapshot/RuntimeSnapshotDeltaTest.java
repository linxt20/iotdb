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

  private static QueryRuntimeSnapshot snapshot(long generation, long captured, long rows) {
    DriverRuntimeSnapshot driver =
        DriverRuntimeSnapshot.builder("f", "p", "d").outputRows(rows).build();
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
            null);
    return new QueryRuntimeSnapshot(
        "q", generation, captured, Collections.singletonList(fragment), null, "checksum");
  }
}
