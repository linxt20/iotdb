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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** Merges immutable fragment snapshots without treating missing sources as zero. */
public final class RuntimeSnapshotMerger {

  private RuntimeSnapshotMerger() {
    // utility class
  }

  public static QueryRuntimeSnapshot merge(
      String queryId,
      long generation,
      long capturedAtNanos,
      Collection<FragmentRuntimeSnapshot> fragments,
      Set<String> returnableFragments,
      Set<String> intentionallyIgnoredFragments) {
    return merge(
        queryId,
        generation,
        capturedAtNanos,
        fragments,
        returnableFragments,
        intentionallyIgnoredFragments,
        null);
  }

  /** Merge with an optional endpoint expectation supplied by the coordinator. */
  public static QueryRuntimeSnapshot merge(
      String queryId,
      long generation,
      long capturedAtNanos,
      Collection<FragmentRuntimeSnapshot> fragments,
      Set<String> returnableFragments,
      Set<String> intentionallyIgnoredFragments,
      String expectedEndpoint) {
    Set<String> returnable =
        returnableFragments == null ? java.util.Collections.emptySet() : returnableFragments;
    Set<String> ignored =
        intentionallyIgnoredFragments == null
            ? java.util.Collections.emptySet()
            : intentionallyIgnoredFragments;
    TreeSet<String> seen = new TreeSet<>();
    List<FragmentRuntimeSnapshot> unique = new ArrayList<>();
    RuntimeSnapshotValiditySet validity = RuntimeSnapshotValiditySet.empty();
    if (fragments != null) {
      for (FragmentRuntimeSnapshot fragment : fragments) {
        if (!seen.add(fragment.getFragmentInstanceId())) {
          validity = validity.plus(RuntimeSnapshotValidity.CHECKSUM_MISMATCH);
          continue;
        }
        if (!queryId.equals(fragment.getQueryId())) {
          validity = validity.plus(RuntimeSnapshotValidity.INVALID_VALUE);
        }
        if (expectedEndpoint != null && !expectedEndpoint.equals(fragment.getSourceEndpoint())) {
          validity = validity.plus(RuntimeSnapshotValidity.ENDPOINT_MISMATCH);
        }
        if (fragment.getChecksum() == null
            || !fragment.getChecksum().equals(RuntimeSnapshotChecksum.forFragment(fragment))) {
          validity = validity.plus(RuntimeSnapshotValidity.CHECKSUM_MISMATCH);
        }
        java.util.Set<String> driverKeys = new java.util.HashSet<>();
        for (DriverRuntimeSnapshot driver : fragment.getDrivers()) {
          String key = driver.getDriverPipelineId() + "|" + driver.getDriverTaskId();
          if (!driverKeys.add(key)
              || (!fragment.getFragmentInstanceId().equals(driver.getFragmentInstanceId())
                  && !(queryId + "." + fragment.getFragmentInstanceId())
                      .equals(driver.getFragmentInstanceId()))) {
            validity = validity.plus(RuntimeSnapshotValidity.INVALID_VALUE);
          }
        }
        StaticParallelPlanIdentity identity = fragment.getPlanIdentity();
        if (identity != null) {
          if (!queryId.equals(identity.getQueryId())
              || !fragment.getFragmentInstanceId().equals(identity.getFragmentInstanceId())
              || new java.util.HashSet<>(identity.getBucketIds()).size()
                  != identity.getBucketIds().size()
              || new java.util.HashSet<>(identity.getWorkUnitIds()).size()
                  != identity.getWorkUnitIds().size()
              || (identity.getPlannedDop() == 0
                  && (!"NO_MANAGED_SCAN".equals(identity.getSplitReason())
                      || !identity.getWorkUnitIds().isEmpty()
                      || !identity.getBucketIds().isEmpty()))
              || identity.getActualPipelineCount() <= 0) {
            validity = validity.plus(RuntimeSnapshotValidity.INVALID_VALUE);
          }
        }
        for (RuntimeSnapshotValidity fragmentValidity : fragment.getValidity().asSet()) {
          validity = validity.plus(fragmentValidity);
        }
        unique.add(fragment);
      }
    }
    Set<String> actual = new HashSet<>(seen);
    if (!actual.containsAll(returnable)) {
      validity = validity.plus(RuntimeSnapshotValidity.MISSING_FRAGMENT);
    }
    Set<String> allowed = new HashSet<>(returnable);
    allowed.addAll(ignored);
    if (!allowed.containsAll(actual)) {
      validity = validity.plus(RuntimeSnapshotValidity.UNEXPECTED_FRAGMENT);
    }
    if (validity.contains(RuntimeSnapshotValidity.MISSING_FRAGMENT)
        || validity.contains(RuntimeSnapshotValidity.UNEXPECTED_FRAGMENT)) {
      validity = validity.plus(RuntimeSnapshotValidity.PARTIAL);
    }
    unique.sort(Comparator.comparing(FragmentRuntimeSnapshot::getFragmentInstanceId));
    List<String> checksumFields =
        unique.stream()
            .map(
                fragment ->
                    fragment.getFragmentInstanceId() + ":" + String.valueOf(fragment.getChecksum()))
            .collect(Collectors.toList());
    checksumFields.add(0, queryId);
    checksumFields.add(1, Long.toString(generation));
    return new QueryRuntimeSnapshot(
        queryId,
        generation,
        capturedAtNanos,
        unique,
        validity,
        RuntimeSnapshotChecksum.sha256(checksumFields));
  }
}
