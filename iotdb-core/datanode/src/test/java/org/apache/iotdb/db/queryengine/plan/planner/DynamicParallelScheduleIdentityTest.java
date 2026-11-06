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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class DynamicParallelScheduleIdentityTest {
  private static DynamicQueryShape shape(
      String source, List<String> devices, String filter, String order, Map<String, String> types) {
    return new DynamicQueryShape(
        "db",
        "table",
        "region0",
        source,
        devices,
        "time>=0",
        filter,
        types,
        order,
        ScanParallelPlan.Mode.DEVICE);
  }

  @Test
  public void sameDeviceCountCannotHideDifferentCoverage() {
    assertNotEquals(
        shape("0.0", List.of("d0"), "s>0", "ASC", Map.of("s", "INT64")),
        shape("0.0", List.of("d1"), "s>0", "ASC", Map.of("s", "INT64")));
    assertNotEquals(
        new DynamicWorkUnitKey("r", "0.0", List.of("d0"), List.of(1L)),
        new DynamicWorkUnitKey("r", "0.0", List.of("d0"), List.of(2L)));
  }

  @Test
  public void aliasesFiltersTypesAndOrderingMustSeparateHistory() {
    DynamicQueryShape baseline = shape("0.0", List.of("d0"), "s>0", "ASC", Map.of("s", "INT64"));
    assertNotEquals(baseline, shape("0.1", List.of("d0"), "s>0", "ASC", Map.of("s", "INT64")));
    assertNotEquals(baseline, shape("0.0", List.of("d0"), "s>1", "ASC", Map.of("s", "INT64")));
    assertNotEquals(baseline, shape("0.0", List.of("d0"), "s>0", "DESC", Map.of("s", "INT64")));
    assertNotEquals(baseline, shape("0.0", List.of("d0"), "s>0", "ASC", Map.of("s", "DOUBLE")));
  }

  @Test
  public void framingPreventsConcatenationAndMissingValueCollisions() {
    assertNotEquals(
        new DynamicWorkUnitKey("ab", "c", List.of("d"), List.of()),
        new DynamicWorkUnitKey("a", "bc", List.of("d"), List.of()));
    assertNotEquals(
        shape("0", List.of("d0"), null, "ASC", Map.of()),
        shape("0", List.of("d0"), "null", "ASC", Map.of()));
  }

  @Test
  public void callersCannotMutateFrozenIdentity() {
    List<String> devices = new ArrayList<>(List.of("d0"));
    Map<String, String> types = new java.util.HashMap<>(Map.of("s", "INT64"));
    DynamicQueryShape shape = shape("0", devices, "s>0", "ASC", types);
    String checksum = shape.getChecksum();
    devices.set(0, "d1");
    types.clear();
    assertEquals(checksum, shape.getChecksum());
    assertEquals(List.of("d0"), shape.getDevices());
  }
}
