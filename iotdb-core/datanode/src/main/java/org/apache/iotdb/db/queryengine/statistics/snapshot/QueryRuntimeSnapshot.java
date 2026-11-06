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

/** Immutable query-level snapshot containing fragment snapshots. */
public final class QueryRuntimeSnapshot {

  private final String queryId;
  private final long generation;
  private final long capturedAtNanos;
  private final List<FragmentRuntimeSnapshot> fragments;
  private final RuntimeSnapshotValiditySet validity;
  private final String checksum;

  public QueryRuntimeSnapshot(
      String queryId,
      long generation,
      long capturedAtNanos,
      List<FragmentRuntimeSnapshot> fragments,
      RuntimeSnapshotValiditySet validity,
      String checksum) {
    if (queryId == null || queryId.isEmpty()) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_QUERYID_MUST_NOT_BE_EMPTY_C9669429);
    }
    if (generation < 0 || capturedAtNanos < 0) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages
              .EXCEPTION_GENERATION_AND_CAPTUREDATNANOS_MUST_BE_NON_NEGATIVE_B9AD4F5E);
    }
    this.queryId = queryId;
    this.generation = generation;
    this.capturedAtNanos = capturedAtNanos;
    this.fragments =
        fragments == null || fragments.isEmpty()
            ? Collections.emptyList()
            : Collections.unmodifiableList(new ArrayList<>(fragments));
    this.validity = validity == null ? RuntimeSnapshotValiditySet.empty() : validity;
    this.checksum = checksum;
  }

  public String getQueryId() {
    return queryId;
  }

  public long getGeneration() {
    return generation;
  }

  public long getCapturedAtNanos() {
    return capturedAtNanos;
  }

  public List<FragmentRuntimeSnapshot> getFragments() {
    return fragments;
  }

  public RuntimeSnapshotValiditySet getValidity() {
    return validity;
  }

  public String getChecksum() {
    return checksum;
  }
}
