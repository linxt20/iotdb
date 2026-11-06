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

import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Per-instance best-effort fetch result. Missing sources remain errors instead of zero values. */
public final class RuntimeSnapshotFetchResult {

  private final Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> responses;
  private final Map<FragmentInstanceId, FragmentRuntimeSnapshot> snapshots;
  private final Map<FragmentInstanceId, String> errors;

  public RuntimeSnapshotFetchResult(
      Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> responses,
      Map<FragmentInstanceId, String> errors) {
    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> responseCopy =
        new LinkedHashMap<>(responses == null ? Collections.emptyMap() : responses);
    Map<FragmentInstanceId, String> errorCopy =
        new LinkedHashMap<>(errors == null ? Collections.emptyMap() : errors);
    Map<FragmentInstanceId, FragmentRuntimeSnapshot> snapshotCopy = new LinkedHashMap<>();
    for (Map.Entry<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> entry :
        new LinkedHashMap<>(responseCopy).entrySet()) {
      try {
        if (entry.getValue() != null
            && entry.getValue().isSetFragmentInstanceId()
            && !entry.getKey().toThrift().equals(entry.getValue().getFragmentInstanceId())) {
          errorCopy.put(entry.getKey(), "FRAGMENT_INSTANCE_MISMATCH");
          responseCopy.remove(entry.getKey());
          continue;
        }
        snapshotCopy.put(entry.getKey(), RuntimeSnapshotAdapters.fromResponse(entry.getValue()));
      } catch (RuntimeException e) {
        errorCopy.put(entry.getKey(), "MALFORMED_SNAPSHOT:" + e.getMessage());
      }
    }
    this.responses = Collections.unmodifiableMap(responseCopy);
    this.snapshots = Collections.unmodifiableMap(snapshotCopy);
    this.errors = Collections.unmodifiableMap(errorCopy);
  }

  public Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> getResponses() {
    return responses;
  }

  public Map<FragmentInstanceId, String> getErrors() {
    return errors;
  }

  public Map<FragmentInstanceId, FragmentRuntimeSnapshot> getSnapshots() {
    return snapshots;
  }

  public QueryRuntimeSnapshot merge(
      String queryId,
      long generation,
      long capturedAtNanos,
      Set<String> returnableFragments,
      Set<String> intentionallyIgnoredFragments,
      String expectedEndpoint) {
    return RuntimeSnapshotMerger.merge(
        queryId,
        generation,
        capturedAtNanos,
        snapshots.values(),
        returnableFragments,
        intentionallyIgnoredFragments,
        expectedEndpoint);
  }

  public boolean isPartial() {
    return !errors.isEmpty();
  }
}
