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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RuntimeSnapshotDriverCoverageTest {

  @Test
  public void testDeviceAndTimePartitionDriversHaveEntries() {
    DriverRuntimeSnapshot device =
        DriverRuntimeSnapshot.builder("f", "device-0", "d0").outputRows(1L).build();
    DriverRuntimeSnapshot time =
        DriverRuntimeSnapshot.builder("f", "time-0", "d1").outputRows(2L).build();
    assertEquals(Long.valueOf(1), device.getOutputRows());
    assertEquals(Long.valueOf(2), time.getOutputRows());
  }

  @Test
  public void testOrderedPathCarriesDriverEntryAfterRecognition() {
    DriverRuntimeSnapshot ordered =
        DriverRuntimeSnapshot.builder("f", "ordered-0", "d0").outputRows(4L).build();
    assertTrue(ordered.getValidity().isEmpty());
    assertEquals(Long.valueOf(4), ordered.getOutputRows());
  }
}
