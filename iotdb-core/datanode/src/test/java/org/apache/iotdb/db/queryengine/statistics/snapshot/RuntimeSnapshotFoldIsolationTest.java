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

public class RuntimeSnapshotFoldIsolationTest {

  @Test
  public void testDifferentBaseIdsRemainSeparateAfterStableOrdering() {
    DriverRuntimeSnapshot left =
        DriverRuntimeSnapshot.builder("f", "base-10", "d1").outputRows(3L).build();
    DriverRuntimeSnapshot right =
        DriverRuntimeSnapshot.builder("f", "base-20", "d1").outputRows(5L).build();
    java.util.List<DriverRuntimeSnapshot> ordered = Arrays.asList(right, left);
    ordered.sort(RuntimeSnapshotDerivation.stableDriverOrder());
    assertEquals("base-10", ordered.get(0).getDriverPipelineId());
    assertEquals("base-20", ordered.get(1).getDriverPipelineId());
    assertEquals(Long.valueOf(3), ordered.get(0).getOutputRows());
    assertEquals(Long.valueOf(5), ordered.get(1).getOutputRows());
  }
}
