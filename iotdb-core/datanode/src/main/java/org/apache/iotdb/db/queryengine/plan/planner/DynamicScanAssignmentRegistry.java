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

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import java.util.*;

/** Shared by every subcontext; immutable source-local ownership freezes before admission. */
public final class DynamicScanAssignmentRegistry {
  private final boolean enabled;
  private final long metadataLimit;
  private final int assignmentLimit, workReferenceLimit;
  private long metadataBytes, receiptBytes;
  private int assignmentCount, workReferenceCount;
  private volatile String feedbackDropReason;
  private final Map<String, DynamicSourcePlan> sources = new TreeMap<>();
  private final Map<Integer, List<DynamicDriverBucketAssignment>> assignments = new TreeMap<>();
  private volatile boolean frozen;
  private Set<Integer> frozenPipelines;
  private final Map<String, DynamicDecisionReceipt> receipts = new TreeMap<>();

  public void registerReceipt(DynamicDecisionReceipt receipt) {
    requireMutable();
    if (feedbackDropReason != null) return;
    if (receipts.containsKey(receipt.getSource().getSourcePlanNodeId()))
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    long charge = Math.addExact(256L, receipt.estimatedRetainedBytes());
    if (!retain(charge)) return;
    receipts.put(receipt.getSource().getSourcePlanNodeId(), receipt);
    receiptBytes = Math.addExact(receiptBytes, charge);
  }

  public synchronized Collection<DynamicDecisionReceipt> finishAdmission(
      String status, String reason) {
    return finishAdmission(status, reason, null);
  }

  public synchronized Collection<DynamicDecisionReceipt> finishAdmission(
      String status, String reason, AdmissionMemoryAccount.Snapshot memory) {
    if (!frozen)
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    if (feedbackDropReason != null) return receipts();
    Map<String, DynamicDecisionReceipt> finished = new TreeMap<>();
    long charge = 0;
    for (Map.Entry<String, DynamicDecisionReceipt> entry : receipts.entrySet()) {
      DynamicDecisionReceipt finalReceipt = entry.getValue().finish(status, reason, memory);
      finished.put(entry.getKey(), finalReceipt);
      charge = Math.addExact(charge, Math.addExact(256L, finalReceipt.estimatedRetainedBytes()));
    }
    long total = Math.addExact(metadataBytes - receiptBytes, charge);
    if (total > metadataLimit) {
      dropFeedback();
      return receipts();
    }
    receipts.clear();
    receipts.putAll(finished);
    metadataBytes = total;
    receiptBytes = charge;
    return receipts();
  }

  public synchronized Collection<DynamicDecisionReceipt> receipts() {
    return Collections.unmodifiableList(new ArrayList<>(receipts.values()));
  }

  public DynamicScanAssignmentRegistry(boolean enabled) {
    this(
        enabled,
        org.apache.iotdb.db.queryengine.statistics.snapshot.DynamicSnapshotRecord.MAX_RECORD_BYTES,
        org.apache.iotdb.db.queryengine.statistics.snapshot.DynamicSnapshotRecord.MAX_ASSIGNMENTS,
        org.apache.iotdb.db.queryengine.statistics.snapshot.DynamicSnapshotRecord
            .MAX_WORK_REFERENCES);
  }

  DynamicScanAssignmentRegistry(
      boolean enabled, long metadataLimit, int assignmentLimit, int workReferenceLimit) {
    if (metadataLimit < 1 || assignmentLimit < 1 || workReferenceLimit < 1)
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    this.enabled = enabled;
    this.metadataLimit = metadataLimit;
    this.assignmentLimit = assignmentLimit;
    this.workReferenceLimit = workReferenceLimit;
  }

  public String getFeedbackDropReason() {
    return feedbackDropReason;
  }

  public long getRetainedMetadataBytes() {
    return metadataBytes;
  }

  private boolean retain(long charge) {
    if (feedbackDropReason != null) return false;
    if (charge < 0 || charge > metadataLimit - metadataBytes) {
      dropFeedback();
      return false;
    }
    metadataBytes += charge;
    return true;
  }

  private void dropFeedback() {
    sources.clear();
    receipts.clear();
    assignments.clear();
    metadataBytes = receiptBytes = 0;
    assignmentCount = workReferenceCount = 0;
    feedbackDropReason = "DYNAMIC_FEEDBACK_METADATA_BUDGET_EXCEEDED";
  }

  public boolean wantsTerminalFeedback() {
    return enabled && frozen && !sources.isEmpty();
  }

  public boolean isEnabled() {
    return enabled;
  }

  public boolean isFrozen() {
    return frozen;
  }

  private void requireMutable() {
    if (frozen)
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
  }

  public void registerSource(DynamicSourcePlan source) {
    requireMutable();
    if (feedbackDropReason != null) return;
    if (sources.containsKey(source.getSourcePlanNodeId()))
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    if (!retain(Math.addExact(256L, source.estimatedRetainedBytes()))) return;
    sources.put(source.getSourcePlanNodeId(), source);
  }

  public void registerBucket(int pipelineId, ScanParallelPlan plan, int bucketIndex) {
    requireMutable();
    if (feedbackDropReason != null || plan.getWorkUnits().isEmpty()) return;
    ScanParallelPlan.Bucket bucket = plan.getBuckets().get(bucketIndex);
    String source = plan.getWorkUnits().get(0).getSourceKey().getPlanNodeId();
    if (assignmentCount >= assignmentLimit
        || bucket.getWorkUnits().size() > workReferenceLimit - workReferenceCount) {
      dropFeedback();
      return;
    }
    List<String> work = new ArrayList<>();
    long charge = 512L + 2L * source.length();
    for (ScanParallelPlan.WorkUnit unit : bucket.getWorkUnits()) {
      String id = unit.getSourceId();
      charge = Math.addExact(charge, 64L + 2L * id.length());
      if (charge > metadataLimit - metadataBytes) {
        dropFeedback();
        return;
      }
      work.add(id);
    }
    for (List<DynamicDriverBucketAssignment> existing : assignments.values())
      for (DynamicDriverBucketAssignment a : existing)
        if (a.getSourcePlanNodeId().equals(source) && a.getBucketId() == bucket.getBucketId())
          throw new IllegalStateException(
              DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    if (!retain(charge)) return;
    assignmentCount++;
    workReferenceCount += work.size();
    assignments
        .computeIfAbsent(pipelineId, k -> new ArrayList<>())
        .add(new DynamicDriverBucketAssignment(source, bucket.getBucketId(), work));
  }

  public void freeze(Set<Integer> actualPipelines) {
    requireMutable();
    if (!actualPipelines.containsAll(assignments.keySet()))
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    for (DynamicSourcePlan source : sources.values()) {
      Map<Integer, List<String>> owned = new TreeMap<>();
      for (List<DynamicDriverBucketAssignment> list : assignments.values())
        for (DynamicDriverBucketAssignment a : list)
          if (source.getSourcePlanNodeId().equals(a.getSourcePlanNodeId()))
            owned.put(a.getBucketId(), a.getWorkUnitIds());
      if (owned.size() != source.getPlan().getBuckets().size())
        throw new IllegalStateException(
            DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
      for (ScanParallelPlan.Bucket b : source.getPlan().getBuckets()) {
        List<String> expected = new ArrayList<>();
        for (ScanParallelPlan.WorkUnit u : b.getWorkUnits()) expected.add(u.getSourceId());
        if (!expected.equals(owned.get(b.getBucketId())))
          throw new IllegalStateException(
              DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
      }
    }
    frozenPipelines = Collections.unmodifiableSet(new HashSet<>(actualPipelines));
    frozen = true;
  }

  /** Null means this physical driver owns no scan; empty is never fabricated for a root. */
  public void verifyFinalPipelines(Set<Integer> actualPipelines) {
    if (!frozen || !frozenPipelines.equals(actualPipelines))
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
  }

  public List<DynamicDriverBucketAssignment> assignmentsFor(int pipelineId) {
    List<DynamicDriverBucketAssignment> list = assignments.get(pipelineId);
    return list == null ? null : Collections.unmodifiableList(new ArrayList<>(list));
  }

  public Collection<DynamicSourcePlan> sourcePlans() {
    return Collections.unmodifiableList(new ArrayList<>(sources.values()));
  }
}
