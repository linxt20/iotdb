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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.db.queryengine.statistics.snapshot.DynamicSnapshotHistoryStore;
import org.apache.iotdb.db.queryengine.statistics.snapshot.FragmentRuntimeSnapshot;

/** Compact provenance of an actually projected feedback, independent of retained responses. */
public final class DynamicFeedbackIdentity {
  private final String queryId,
      fragmentId,
      endpoint,
      sourceId,
      snapshotChecksum,
      fragmentPlanChecksum,
      sourcePlanChecksum,
      groupingChecksum,
      shapeChecksum;
  private final long sequence, snapshotGeneration, planGeneration;

  DynamicFeedbackIdentity(DynamicSnapshotHistoryStore.Entry entry, DynamicSourcePlan source) {
    FragmentRuntimeSnapshot snapshot = entry.getRecord().getSnapshot();
    queryId = snapshot.getQueryId();
    fragmentId = snapshot.getFragmentInstanceId();
    endpoint = snapshot.getSourceEndpoint();
    sourceId = source.getSourcePlanNodeId();
    snapshotChecksum = snapshot.getChecksum();
    fragmentPlanChecksum = snapshot.getPlanIdentity().getPlanIdentityChecksum();
    sourcePlanChecksum = source.getPlanIdentityChecksum();
    groupingChecksum = source.getGroupingChecksum();
    shapeChecksum = source.getShape().getChecksum();
    sequence = entry.getSequence();
    snapshotGeneration = snapshot.getGeneration();
    planGeneration = snapshot.getPlanIdentity().getPlanGeneration();
  }

  public String getQueryId() {
    return queryId;
  }

  public String getFragmentId() {
    return fragmentId;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public String getSourceId() {
    return sourceId;
  }

  public String getSnapshotChecksum() {
    return snapshotChecksum;
  }

  public String getFragmentPlanChecksum() {
    return fragmentPlanChecksum;
  }

  public String getSourcePlanChecksum() {
    return sourcePlanChecksum;
  }

  public String getGroupingChecksum() {
    return groupingChecksum;
  }

  public String getShapeChecksum() {
    return shapeChecksum;
  }

  public long estimatedRetainedBytes() {
    long bytes = 768;
    for (String value :
        new String[] {
          queryId,
          fragmentId,
          endpoint,
          sourceId,
          snapshotChecksum,
          fragmentPlanChecksum,
          sourcePlanChecksum,
          groupingChecksum,
          shapeChecksum
        }) bytes = Math.addExact(bytes, Math.multiplyExact(2L, value.length()));
    return bytes;
  }

  public long getSequence() {
    return sequence;
  }

  public long getSnapshotGeneration() {
    return snapshotGeneration;
  }

  public long getPlanGeneration() {
    return planGeneration;
  }
}
