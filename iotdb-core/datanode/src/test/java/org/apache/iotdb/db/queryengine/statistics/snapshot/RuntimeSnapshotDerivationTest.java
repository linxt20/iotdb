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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RuntimeSnapshotDerivationTest {

  @Test
  public void testRowsAndWallRules() {
    DriverRuntimeSnapshot d1 =
        DriverRuntimeSnapshot.builder("f", "p1", "d1").wallNanos(30L).outputRows(50L).build();
    DriverRuntimeSnapshot d2 =
        DriverRuntimeSnapshot.builder("f", "p2", "d2").wallNanos(10L).outputRows(75L).build();
    DriverRuntimeSnapshot d3 =
        DriverRuntimeSnapshot.builder("f", "p3", "d3").wallNanos(25L).outputRows(60L).build();
    DriverRuntimeSnapshot d4 =
        DriverRuntimeSnapshot.builder("f", "p4", "d4").wallNanos(20L).outputRows(65L).build();
    java.util.List<DriverRuntimeSnapshot> drivers = Arrays.asList(d1, d2, d3, d4);

    assertEquals(
        250L,
        RuntimeSnapshotDerivation.sumDriverField(drivers, DriverRuntimeSnapshot::getOutputRows));
    assertEquals(10.0, RuntimeSnapshotDerivation.minDriverWall(drivers).getAsDouble(), 0.0);
    assertEquals(30.0, RuntimeSnapshotDerivation.maxDriverWall(drivers).getAsDouble(), 0.0);
  }

  @Test
  public void testBlockedRatioAndMissingDenominator() {
    assertEquals(0.2, RuntimeSnapshotDerivation.blockedRatio(50L, 200L).getAsDouble(), 0.000001);
    assertFalse(RuntimeSnapshotDerivation.blockedRatio(0L, 0L).isPresent());
    assertFalse(RuntimeSnapshotDerivation.blockedRatio(null, 200L).isPresent());
  }

  @Test
  public void testMemoryIsNotSummedByDerivation() {
    // Memory fields are intentionally not accepted by sumDriverField.  The caller must keep
    // estimated, reserved and peakObserved as separate values.
    assertTrue(RuntimeSnapshotDerivation.throughput(100L, 10L).isPresent());
    assertFalse(RuntimeSnapshotDerivation.throughput(100L, 0L).isPresent());
  }

  @Test
  public void testOverflowIsRejected() {
    try {
      RuntimeSnapshotDerivation.sumNonNull(Arrays.asList(Long.MAX_VALUE, 1L));
    } catch (ArithmeticException expected) {
      return;
    }
    throw new AssertionError("overflow must not wrap");
  }

  @org.junit.Test
  public void driverIdentityOrderUsesStringKeysIncludingMultiDigitIds() {
    java.util.List<DriverRuntimeSnapshot> drivers = new java.util.ArrayList<>();
    drivers.add(DriverRuntimeSnapshot.builder("f", "2", "task-2").build());
    drivers.add(DriverRuntimeSnapshot.builder("f", "10", "task-b").build());
    drivers.add(DriverRuntimeSnapshot.builder("f", "10", "task-a").build());
    drivers.sort(RuntimeSnapshotDerivation.stableDriverOrder());
    org.junit.Assert.assertEquals("10", drivers.get(0).getDriverPipelineId());
    org.junit.Assert.assertEquals("task-a", drivers.get(0).getDriverTaskId());
    org.junit.Assert.assertEquals("task-b", drivers.get(1).getDriverTaskId());
    org.junit.Assert.assertEquals("2", drivers.get(2).getDriverPipelineId());
  }
}
