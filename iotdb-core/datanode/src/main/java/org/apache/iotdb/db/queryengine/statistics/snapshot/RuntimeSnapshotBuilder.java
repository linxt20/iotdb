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
import java.util.Collections;
import java.util.List;

/** Builds immutable fragment snapshots from already-copied runtime values. */
public final class RuntimeSnapshotBuilder {

  private RuntimeSnapshotBuilder() {
    // utility class
  }

  public static FragmentRuntimeSnapshot buildFragment(
      String queryId,
      String fragmentInstanceId,
      long generation,
      long capturedAtNanos,
      String sourceNodeId,
      String sourceEndpoint,
      String state,
      boolean terminal,
      List<DriverRuntimeSnapshot> drivers,
      List<OperatorRuntimeSnapshot> operators,
      RuntimeSnapshotValiditySet validity) {
    return buildFragment(
        queryId,
        fragmentInstanceId,
        generation,
        capturedAtNanos,
        sourceNodeId,
        sourceEndpoint,
        state,
        terminal,
        drivers,
        operators,
        validity,
        null);
  }

  public static FragmentRuntimeSnapshot buildFragment(
      String queryId,
      String fragmentInstanceId,
      long generation,
      long capturedAtNanos,
      String sourceNodeId,
      String sourceEndpoint,
      String state,
      boolean terminal,
      List<DriverRuntimeSnapshot> drivers,
      List<OperatorRuntimeSnapshot> operators,
      RuntimeSnapshotValiditySet validity,
      StaticParallelPlanIdentity planIdentity) {
    List<DriverRuntimeSnapshot> sortedDrivers =
        drivers == null ? Collections.emptyList() : new ArrayList<>(drivers);
    sortedDrivers.sort(RuntimeSnapshotDerivation.stableDriverOrder());
    FragmentRuntimeSnapshot snapshot =
        new FragmentRuntimeSnapshot(
            queryId,
            fragmentInstanceId,
            generation,
            capturedAtNanos,
            sourceNodeId,
            sourceEndpoint,
            state,
            terminal,
            sortedDrivers,
            operators,
            validity,
            null,
            planIdentity);
    return snapshot.withChecksum(RuntimeSnapshotChecksum.forFragment(snapshot));
  }
}
