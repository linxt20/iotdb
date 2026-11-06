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

public class RuntimeSnapshotSerializationTest {

  @Test
  public void testFragmentChecksumIsStableForSameIdentity() {
    FragmentRuntimeSnapshot first =
        RuntimeSnapshotBuilder.buildFragment(
            "q", "f", 1, 2, "n", "127.0.0.1:9000", "FINISHED", true, null, null, null);
    FragmentRuntimeSnapshot second =
        RuntimeSnapshotBuilder.buildFragment(
            "q", "f", 1, 2, "n", "127.0.0.1:9000", "FINISHED", true, null, null, null);
    assertEquals(first.getChecksum(), second.getChecksum());
    assertEquals(64, first.getChecksum().length());
  }

  @Test
  public void testValidityIsExplicit() {
    RuntimeSnapshotValiditySet validity =
        RuntimeSnapshotValiditySet.of(RuntimeSnapshotValidity.UNSUPPORTED_FIELD)
            .plus(RuntimeSnapshotValidity.PARTIAL);
    assertTrue(validity.contains(RuntimeSnapshotValidity.UNSUPPORTED_FIELD));
    assertTrue(validity.contains(RuntimeSnapshotValidity.PARTIAL));
  }
}
