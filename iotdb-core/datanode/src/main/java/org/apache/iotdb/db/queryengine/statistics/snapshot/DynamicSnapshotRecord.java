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

package org.apache.iotdb.db.queryengine.statistics.snapshot;

import org.apache.iotdb.db.queryengine.plan.planner.DynamicSourcePlan;
import org.apache.iotdb.db.queryengine.plan.planner.StaticParallelPlanChecksum;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/** A frozen C response with independent, immutable source coverage and registered scope. */
public final class DynamicSnapshotRecord {
  public static final long MAX_RECORD_BYTES = 4L * 1024 * 1024;
  public static final int MAX_ASSIGNMENTS = 4096, MAX_WORK_REFERENCES = 16384;

  /**
   * Captured under the statistics write lock; validation and history insertion happen after unlock.
   */
  public static final class Payload {
    private final String queryId, fragmentId, endpoint;
    private final TFetchFragmentInstanceStatisticsResp response;
    private final Map<String, DynamicSourcePlan> plans;
    private final Map<String, org.apache.iotdb.db.queryengine.plan.planner.DynamicDecisionReceipt>
        receipts;
    private final long chargedBytes;

    private Payload(
        String queryId,
        String fragmentId,
        String endpoint,
        TFetchFragmentInstanceStatisticsResp response,
        Map<String, DynamicSourcePlan> plans,
        Map<String, org.apache.iotdb.db.queryengine.plan.planner.DynamicDecisionReceipt> receipts,
        long bytes) {
      this.queryId = queryId;
      this.fragmentId = fragmentId;
      this.endpoint = endpoint;
      this.response = response;
      this.plans = plans;
      this.receipts = receipts;
      chargedBytes = bytes;
    }
  }

  private final Payload payload;
  private final FragmentRuntimeSnapshot snapshot;
  private final RuntimeSnapshotView runtimeView;
  private final String runtimeIdentity, contentChecksum;

  private DynamicSnapshotRecord(Payload payload, FragmentRuntimeSnapshot snapshot) {
    this.payload = payload;
    this.snapshot = snapshot;
    // This is an explicitly scoped, complete one-fragment C view, not a merge of different
    // query runs. Reuse the validated immutable carrier without another RPC or Thrift copy.
    runtimeView =
        new RuntimeSnapshotView(
            RuntimeSnapshotMerger.merge(
                snapshot.getQueryId(),
                snapshot.getGeneration(),
                snapshot.getCapturedAtNanos(),
                java.util.Collections.singletonList(snapshot),
                java.util.Collections.singleton(snapshot.getFragmentInstanceId()),
                java.util.Collections.emptySet(),
                snapshot.getSourceEndpoint()));
    if (!runtimeView.getSnapshot().getValidity().isEmpty())
      throw new IllegalArgumentException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    runtimeIdentity =
        frame(payload.queryId)
            + frame(payload.fragmentId)
            + frame(payload.endpoint)
            + snapshot.getGeneration();
    StringBuilder canonical = new StringBuilder(frame(snapshot.getChecksum()));
    for (Map.Entry<String, DynamicSourcePlan> e : payload.plans.entrySet()) {
      canonical
          .append(frame(e.getKey()))
          .append(frame(e.getValue().getShape().getChecksum()))
          .append(frame(e.getValue().getPlanIdentityChecksum()))
          .append(frame(e.getValue().getGroupingChecksum()));
    }
    for (Map.Entry<String, org.apache.iotdb.db.queryengine.plan.planner.DynamicDecisionReceipt>
        entry : payload.receipts.entrySet())
      canonical.append(frame(entry.getKey())).append(frame(entry.getValue().getContentChecksum()));
    contentChecksum = StaticParallelPlanChecksum.sha256(canonical.toString());
  }

  public static Payload capture(
      String queryId,
      String fragmentId,
      String endpoint,
      TFetchFragmentInstanceStatisticsResp response,
      Collection<DynamicSourcePlan> sourcePlans) {
    return capture(queryId, fragmentId, endpoint, response, sourcePlans, Collections.emptyList());
  }

  public static Payload capture(
      String queryId,
      String fragmentId,
      String endpoint,
      TFetchFragmentInstanceStatisticsResp response,
      Collection<DynamicSourcePlan> sourcePlans,
      Collection<org.apache.iotdb.db.queryengine.plan.planner.DynamicDecisionReceipt>
          decisionReceipts) {
    if (queryId == null
        || fragmentId == null
        || endpoint == null
        || response == null
        || sourcePlans == null
        || decisionReceipts == null) {
      throw new IllegalArgumentException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    }
    TreeMap<String, DynamicSourcePlan> plans = new TreeMap<>();
    TreeMap<String, org.apache.iotdb.db.queryengine.plan.planner.DynamicDecisionReceipt> receipts =
        new TreeMap<>();
    long bytes = 256;
    try {
      for (DynamicSourcePlan source : sourcePlans) {
        if (source == null || plans.put(source.getSourcePlanNodeId(), source) != null) {
          throw new IllegalArgumentException(
              org.apache.iotdb.db.i18n.DataNodeQueryMessages
                  .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
        }
        bytes =
            Math.addExact(
                bytes, Math.addExact(64L, Math.multiplyExact(2L, source.estimatedRetainedBytes())));
        if (bytes > MAX_RECORD_BYTES) {
          return null;
        }
      }
      for (org.apache.iotdb.db.queryengine.plan.planner.DynamicDecisionReceipt receipt :
          decisionReceipts) {
        if (receipt == null
            || receipts.put(receipt.getSource().getSourcePlanNodeId(), receipt) != null)
          throw new IllegalArgumentException(
              org.apache.iotdb.db.i18n.DataNodeQueryMessages
                  .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
        bytes = Math.addExact(bytes, Math.multiplyExact(2L, receipt.estimatedRetainedBytes()));
        if (bytes > MAX_RECORD_BYTES) return null;
      }
      long responseBytes = DynamicSnapshotBudget.estimate(response, (MAX_RECORD_BYTES - bytes) / 3);
      if (responseBytes == Long.MAX_VALUE) {
        return null;
      }
      bytes = Math.addExact(bytes, Math.multiplyExact(responseBytes, 3L));
    } catch (ArithmeticException e) {
      return null;
    }
    int assignments = 0, workReferences = 0;
    if (response.isSetDriverSnapshots()) {
      for (org.apache.iotdb.mpp.rpc.thrift.TDriverRuntimeSnapshot driver :
          response.getDriverSnapshots()) {
        if (driver.isSetScanAssignments()) {
          assignments = Math.addExact(assignments, driver.getScanAssignmentsSize());
          if (assignments > MAX_ASSIGNMENTS) {
            return null;
          }
          for (org.apache.iotdb.mpp.rpc.thrift.TDriverBucketAssignment assignment :
              driver.getScanAssignments()) {
            workReferences = Math.addExact(workReferences, assignment.getWorkUnitIdsSize());
            if (workReferences > MAX_WORK_REFERENCES) {
              return null;
            }
          }
        }
      }
    }
    return new Payload(
        queryId,
        fragmentId,
        endpoint,
        response.deepCopy(),
        Collections.unmodifiableMap(plans),
        Collections.unmodifiableMap(receipts),
        bytes);
  }

  public static DynamicSnapshotRecord validate(Payload payload) {
    if (payload == null) {
      throw new IllegalArgumentException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    }
    TFetchFragmentInstanceStatisticsResp response = payload.response;
    if (response.isSetDynamicFeedbackDropReason()
        || !response.isSetStatus()
        || response.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
        || !response.isSetSnapshotGeneration()
        || !response.isSetSnapshotChecksum()
        || !response.isSetIsTerminal()
        || !response.isIsTerminal()) {
      throw new IllegalArgumentException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    }
    FragmentRuntimeSnapshot snapshot = RuntimeSnapshotAdapters.fromResponse(response);
    if (!snapshot.getValidity().isEmpty()
        || !payload.queryId.equals(snapshot.getQueryId())
        || !payload.fragmentId.equals(snapshot.getFragmentInstanceId())
        || !payload.endpoint.equals(snapshot.getSourceEndpoint())
        || !response.getSnapshotChecksum().equals(snapshot.getChecksum())) {
      throw new IllegalArgumentException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    }
    for (DynamicSourcePlan source : payload.plans.values()) {
      org.apache.iotdb.db.queryengine.plan.planner.ScanParallelPlan.SourceKey key =
          source.getPlan().getWorkUnits().get(0).getSourceKey();
      if (!payload.queryId.equals(key.getQueryId())
          || !payload.fragmentId.equals(key.getFragmentId())
          || !source.verifies()) {
        throw new IllegalArgumentException(
            org.apache.iotdb.db.i18n.DataNodeQueryMessages
                .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
      }
    }
    if (!payload.receipts.isEmpty()) {
      if (!payload.receipts.keySet().equals(payload.plans.keySet()))
        throw new IllegalArgumentException(
            org.apache.iotdb.db.i18n.DataNodeQueryMessages
                .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
      for (Map.Entry<String, org.apache.iotdb.db.queryengine.plan.planner.DynamicDecisionReceipt>
          entry : payload.receipts.entrySet()) {
        org.apache.iotdb.db.queryengine.plan.planner.DynamicDecisionReceipt receipt =
            entry.getValue();
        DynamicSourcePlan source = payload.plans.get(entry.getKey());
        if (!payload.queryId.equals(receipt.getQueryId())
            || !payload.fragmentId.equals(receipt.getFragmentId())
            || !payload.endpoint.equals(receipt.getEndpoint())
            || !"ADMISSION_ACCEPTED".equals(receipt.getAdmissionStatus())
            || receipt.getDecision().getTargetDop() != source.getPlan().getTargetParallelism()
            || !source
                .getPlanIdentityChecksum()
                .equals(receipt.getSource().getPlanIdentityChecksum())
            || !source.getGroupingChecksum().equals(receipt.getSource().getGroupingChecksum()))
          throw new IllegalArgumentException(
              org.apache.iotdb.db.i18n.DataNodeQueryMessages
                  .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
        for (org.apache.iotdb.db.queryengine.plan.planner.DynamicFeedbackIdentity input :
            receipt.getInputs())
          if (!payload.endpoint.equals(input.getEndpoint())
              || !source.getShape().getChecksum().equals(input.getShapeChecksum()))
            throw new IllegalArgumentException(
                org.apache.iotdb.db.i18n.DataNodeQueryMessages
                    .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
      }
    }
    if (response.isSetDynamicDecisions()) {
      java.util.Set<String> traced = new java.util.HashSet<>();
      for (org.apache.iotdb.mpp.rpc.thrift.TDynamicDecisionTrace trace :
          response.getDynamicDecisions()) {
        DynamicSourcePlan source = payload.plans.get(trace.getSourcePlanNodeId());
        org.apache.iotdb.db.queryengine.plan.planner.DynamicDecisionReceipt receipt =
            payload.receipts.get(trace.getSourcePlanNodeId());
        if (!org.apache.iotdb.db.queryengine.plan.planner.DynamicDecisionTrace.verifies(trace)
            || source == null
            || !traced.add(trace.getSourcePlanNodeId())
            || !payload.queryId.equals(trace.getQueryId())
            || !payload.fragmentId.equals(trace.getFragmentId())
            || !payload.endpoint.equals(trace.getEndpoint())
            || trace.getTargetDop() != source.getPlan().getTargetParallelism()
            || trace.getActualBucketCount() != source.getPlan().getBuckets().size()
            || trace.getWorkUnitCount() != source.getPlan().getWorkUnits().size()
            || !source.getPlanIdentityChecksum().equals(trace.getSourcePlanChecksum())
            || !source.getGroupingChecksum().equals(trace.getGroupingChecksum())
            || !source.getShape().getChecksum().equals(trace.getShapeChecksum())
            || receipt != null
                && !org.apache.iotdb.db.queryengine.plan.planner.DynamicDecisionTrace.fromReceipt(
                        receipt)
                    .equals(trace))
          throw new IllegalArgumentException(
              org.apache.iotdb.db.i18n.DataNodeQueryMessages
                  .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
      }
      if (!traced.equals(payload.plans.keySet()))
        throw new IllegalArgumentException(
            org.apache.iotdb.db.i18n.DataNodeQueryMessages
                .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    }
    if (!payload.plans.isEmpty()) {
      StaticParallelPlanIdentity identity = snapshot.getPlanIdentity();
      if (identity == null || identity.getActualPipelineCount() != snapshot.getDrivers().size()) {
        throw new IllegalArgumentException(
            org.apache.iotdb.db.i18n.DataNodeQueryMessages
                .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
      }
      StringBuilder canonical = new StringBuilder();
      String single = null;
      int planned = 0;
      java.util.List<String> work = new java.util.ArrayList<>();
      java.util.List<Integer> buckets = new java.util.ArrayList<>();
      for (Map.Entry<String, DynamicSourcePlan> entry : payload.plans.entrySet()) {
        DynamicSourcePlan source = entry.getValue();
        String checksum =
            StaticParallelPlanChecksum.withGeneration(
                source.getPlanIdentityChecksum(), identity.getPlanGeneration());
        canonical
            .append(entry.getKey().length())
            .append(':')
            .append(entry.getKey())
            .append('|')
            .append(checksum)
            .append('|');
        single = checksum;
        planned = Math.max(planned, source.getPlan().getTargetParallelism());
        for (org.apache.iotdb.db.queryengine.plan.planner.ScanParallelPlan.WorkUnit unit :
            source.getPlan().getWorkUnits()) {
          work.add(unit.getSourceId());
        }
        for (org.apache.iotdb.db.queryengine.plan.planner.ScanParallelPlan.Bucket ignored :
            source.getPlan().getBuckets()) {
          buckets.add(buckets.size());
        }
      }
      String checksum =
          payload.plans.size() == 1
              ? single
              : StaticParallelPlanChecksum.sha256(canonical.toString());
      if (!checksum.equals(identity.getPlanIdentityChecksum())
          || planned != identity.getPlannedDop()
          || !work.equals(identity.getWorkUnitIds())
          || !buckets.equals(identity.getBucketIds())) {
        throw new IllegalArgumentException(
            org.apache.iotdb.db.i18n.DataNodeQueryMessages
                .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
      }
    }
    return new DynamicSnapshotRecord(payload, snapshot);
  }

  public RuntimeSnapshotView getRuntimeView() {
    return runtimeView;
  }

  public Map<String, org.apache.iotdb.db.queryengine.plan.planner.DynamicDecisionReceipt>
      getDecisionReceipts() {
    return payload.receipts;
  }

  public FragmentRuntimeSnapshot getSnapshot() {
    return snapshot;
  }

  public Map<String, DynamicSourcePlan> getSourcePlans() {
    return payload.plans;
  }

  public TFetchFragmentInstanceStatisticsResp copyResponse() {
    return payload.response.deepCopy();
  }

  public long getChargedBytes() {
    return payload.chargedBytes;
  }

  public String getRuntimeIdentity() {
    return runtimeIdentity;
  }

  public String getContentChecksum() {
    return contentChecksum;
  }

  public String getEndpoint() {
    return payload.endpoint;
  }

  public boolean isFinishedV2() {
    return Integer.valueOf(2).equals(snapshot.getSchemaVersion())
        && snapshot.isTerminal()
        && "FINISHED".equals(snapshot.getState());
  }

  private static String frame(String value) {
    return value == null ? "N|" : value.length() + ":" + value + "|";
  }
}
