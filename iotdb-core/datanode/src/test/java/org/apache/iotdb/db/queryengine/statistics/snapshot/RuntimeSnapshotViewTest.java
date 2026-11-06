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

public class RuntimeSnapshotViewTest {

  @Test
  public void testViewExposesSortedDrivers() {
    DriverRuntimeSnapshot driver =
        DriverRuntimeSnapshot.builder("f", "p", "d").outputRows(4L).build();
    FragmentRuntimeSnapshot fragment =
        RuntimeSnapshotBuilder.buildFragment(
            "q",
            "f",
            1,
            10,
            "n",
            "e",
            "RUNNING",
            false,
            Collections.singletonList(driver),
            null,
            null);
    RuntimeSnapshotView view =
        new RuntimeSnapshotView(
            new QueryRuntimeSnapshot(
                "q", 1, 10, Collections.singletonList(fragment), null, "checksum"));
    assertEquals(1, view.getDrivers().size());
    assertEquals("d", view.getDrivers().get(0).getDriverTaskId());
  }
}
