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

public class RuntimeSnapshotValueTest {

  @Test
  public void testDriverValuesAreImmutableAndValidateBounds() {
    DriverRuntimeSnapshot snapshot =
        DriverRuntimeSnapshot.builder("f0", "p1", "d2")
            .role("input")
            .sampleGeneration(3)
            .outputRows(10L)
            .outputBytes(20L)
            .build();

    assertEquals("f0", snapshot.getFragmentInstanceId());
    assertEquals(Long.valueOf(10), snapshot.getOutputRows());
    assertTrue(snapshot.getValidity().isEmpty());

    try {
      DriverRuntimeSnapshot.builder("f0", "p1", "d2").outputRows(-1L).build();
    } catch (IllegalArgumentException expected) {
      return;
    }
    throw new AssertionError("negative output rows must be rejected");
  }

  @Test
  public void testFragmentDefensivelyCopiesLists() {
    java.util.List<DriverRuntimeSnapshot> drivers =
        new java.util.ArrayList<>(
            Collections.singletonList(DriverRuntimeSnapshot.builder("f0", "p1", "d1").build()));
    FragmentRuntimeSnapshot snapshot =
        new FragmentRuntimeSnapshot(
            "q0",
            "f0",
            1,
            2,
            "n0",
            "127.0.0.1:9000",
            "FINISHED",
            true,
            drivers,
            Collections.emptyList(),
            RuntimeSnapshotValiditySet.empty(),
            null);
    drivers.clear();
    assertEquals(1, snapshot.getDrivers().size());
    try {
      snapshot.getDrivers().add(DriverRuntimeSnapshot.builder("f0", "p1", "d2").build());
    } catch (UnsupportedOperationException expected) {
      return;
    }
    throw new AssertionError("snapshot driver list must be immutable");
  }

  @Test
  public void testChecksumUsesExplicitFieldOrder() {
    String first = RuntimeSnapshotChecksum.sha256(Arrays.asList("a", "b"));
    String second = RuntimeSnapshotChecksum.sha256(Arrays.asList("b", "a"));
    assertEquals(64, first.length());
    org.junit.Assert.assertNotEquals(first, second);
  }
}
