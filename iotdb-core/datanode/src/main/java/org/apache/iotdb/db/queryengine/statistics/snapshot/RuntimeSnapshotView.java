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

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Read-only view consumed by static and dynamic schedulers. */
public final class RuntimeSnapshotView {

  private final QueryRuntimeSnapshot snapshot;

  public RuntimeSnapshotView(QueryRuntimeSnapshot snapshot) {
    this.snapshot =
        Objects.requireNonNull(
            snapshot,
            () ->
                String.format(
                    DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918,
                    "snapshot"));
  }

  public QueryRuntimeSnapshot getSnapshot() {
    return snapshot;
  }

  public List<DriverRuntimeSnapshot> getDrivers() {
    List<DriverRuntimeSnapshot> result = new ArrayList<>();
    for (FragmentRuntimeSnapshot fragment : snapshot.getFragments()) {
      result.addAll(fragment.getDrivers());
    }
    result.sort(RuntimeSnapshotDerivation.stableDriverOrder());
    return Collections.unmodifiableList(result);
  }

  public List<StaticParallelPlanIdentity> getPlanIdentities() {
    List<StaticParallelPlanIdentity> result = new ArrayList<>();
    for (FragmentRuntimeSnapshot fragment : snapshot.getFragments()) {
      if (fragment.getPlanIdentity() != null) {
        result.add(fragment.getPlanIdentity());
      }
    }
    return Collections.unmodifiableList(result);
  }

  public StaticParallelPlanIdentity getPlanIdentity(String fragmentInstanceId) {
    for (FragmentRuntimeSnapshot fragment : snapshot.getFragments()) {
      if (fragment.getFragmentInstanceId().equals(fragmentInstanceId)) {
        return fragment.getPlanIdentity();
      }
    }
    return null;
  }

  public int getActualDriverCount(String fragmentInstanceId) {
    for (FragmentRuntimeSnapshot fragment : snapshot.getFragments()) {
      if (fragment.getFragmentInstanceId().equals(fragmentInstanceId)) {
        return fragment.getDrivers().size();
      }
    }
    return 0;
  }

  public List<String> getWorkUnitIds(String fragmentInstanceId) {
    StaticParallelPlanIdentity identity = getPlanIdentity(fragmentInstanceId);
    return identity == null ? Collections.emptyList() : identity.getWorkUnitIds();
  }

  public List<Integer> getBucketIds(String fragmentInstanceId) {
    StaticParallelPlanIdentity identity = getPlanIdentity(fragmentInstanceId);
    return identity == null ? Collections.emptyList() : identity.getBucketIds();
  }

  public List<String> getExchangeEdgeIds(String fragmentInstanceId) {
    StaticParallelPlanIdentity identity = getPlanIdentity(fragmentInstanceId);
    return identity == null ? Collections.emptyList() : identity.getExchangeEdgeIds();
  }

  public boolean isFieldSupported(RuntimeSnapshotValidity validity) {
    return !snapshot.getValidity().contains(validity);
  }
}
