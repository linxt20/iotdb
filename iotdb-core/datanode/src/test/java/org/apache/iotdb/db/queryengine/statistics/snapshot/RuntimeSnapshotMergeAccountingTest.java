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

import static org.junit.Assert.assertTrue;

public class RuntimeSnapshotMergeAccountingTest {

  @Test
  public void testMissingReturnableFragmentIsPartial() {
    FragmentRuntimeSnapshot present =
        RuntimeSnapshotBuilder.buildFragment(
            "q", "f0", 1, 2, "n", "e", "FINISHED", true, null, null, null);
    QueryRuntimeSnapshot merged =
        RuntimeSnapshotMerger.merge(
            "q",
            1,
            2,
            Collections.singletonList(present),
            new java.util.HashSet<>(Arrays.asList("f0", "f1")),
            Collections.emptySet());
    assertTrue(merged.getValidity().contains(RuntimeSnapshotValidity.MISSING_FRAGMENT));
    assertTrue(merged.getValidity().contains(RuntimeSnapshotValidity.PARTIAL));
  }

  @Test
  public void testIgnoredFragmentDoesNotMakeCompleteSetPartial() {
    FragmentRuntimeSnapshot present =
        RuntimeSnapshotBuilder.buildFragment(
            "q", "ignored", 1, 2, "n", "e", "FINISHED", true, null, null, null);
    QueryRuntimeSnapshot merged =
        RuntimeSnapshotMerger.merge(
            "q",
            1,
            2,
            Collections.singletonList(present),
            Collections.emptySet(),
            Collections.singleton("ignored"));
    assertTrue(merged.getValidity().isEmpty());
  }
}
