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

import java.util.Objects;

/** One immutable source decision. Admission replaces PENDING exactly once with a final value. */
public final class DynamicDecisionReceipt {
  private final DynamicSourcePlan source;
  private final DynamicParallelScheduleDecision decision;
  private final String queryId, fragmentId, endpoint, admissionStatus, admissionReason;
  private final long checkSequence;
  private final int contextDop, inputDop, previousDop;
  private final boolean dopEstimationEnabled;
  private final String estimateStatus;
  private final Integer estimatedDop;
  private final java.util.List<DynamicFeedbackIdentity> inputs;
  private final AdmissionMemoryAccount.Snapshot admissionMemory;

  public DynamicDecisionReceipt(
      DynamicSourcePlan source,
      DynamicParallelScheduleDecision decision,
      String endpoint,
      long checkSequence,
      int contextDop,
      int inputDop,
      int previousDop,
      boolean dopEstimationEnabled) {
    this(
        source,
        decision,
        endpoint,
        checkSequence,
        contextDop,
        inputDop,
        previousDop,
        dopEstimationEnabled,
        dopEstimationEnabled ? "CALLER_SUPPLIED_CAP" : "DISABLED",
        null,
        "ADMISSION_PENDING",
        null);
  }

  public DynamicDecisionReceipt(
      DynamicSourcePlan source,
      DynamicParallelScheduleDecision decision,
      String endpoint,
      long checkSequence,
      int contextDop,
      int inputDop,
      int previousDop,
      boolean dopEstimationEnabled,
      String estimateStatus,
      Integer estimatedDop) {
    this(
        source,
        decision,
        endpoint,
        checkSequence,
        contextDop,
        inputDop,
        previousDop,
        dopEstimationEnabled,
        estimateStatus,
        estimatedDop,
        "ADMISSION_PENDING",
        null);
  }

  private DynamicDecisionReceipt(
      DynamicSourcePlan source,
      DynamicParallelScheduleDecision decision,
      String endpoint,
      long checkSequence,
      int contextDop,
      int inputDop,
      int previousDop,
      boolean dopEstimationEnabled,
      String estimateStatus,
      Integer estimatedDop,
      String status,
      String reason) {
    this(
        source,
        decision,
        endpoint,
        checkSequence,
        contextDop,
        inputDop,
        previousDop,
        dopEstimationEnabled,
        estimateStatus,
        estimatedDop,
        status,
        reason,
        java.util.Collections.emptyList());
  }

  private DynamicDecisionReceipt(
      DynamicSourcePlan source,
      DynamicParallelScheduleDecision decision,
      String endpoint,
      long checkSequence,
      int contextDop,
      int inputDop,
      int previousDop,
      boolean dopEstimationEnabled,
      String estimateStatus,
      Integer estimatedDop,
      String status,
      String reason,
      java.util.List<DynamicFeedbackIdentity> inputs) {
    this(
        source,
        decision,
        endpoint,
        checkSequence,
        contextDop,
        inputDop,
        previousDop,
        dopEstimationEnabled,
        estimateStatus,
        estimatedDop,
        status,
        reason,
        inputs,
        null);
  }

  private DynamicDecisionReceipt(
      DynamicSourcePlan source,
      DynamicParallelScheduleDecision decision,
      String endpoint,
      long checkSequence,
      int contextDop,
      int inputDop,
      int previousDop,
      boolean dopEstimationEnabled,
      String estimateStatus,
      Integer estimatedDop,
      String status,
      String reason,
      java.util.List<DynamicFeedbackIdentity> inputs,
      AdmissionMemoryAccount.Snapshot admissionMemory) {
    this.source = Objects.requireNonNull(source);
    this.decision = Objects.requireNonNull(decision);
    ScanParallelPlan.SourceKey key = source.getPlan().getWorkUnits().get(0).getSourceKey();
    queryId = key.getQueryId();
    fragmentId = key.getFragmentId();
    this.endpoint = Objects.requireNonNull(endpoint);
    this.checkSequence = checkSequence;
    this.contextDop = contextDop;
    this.inputDop = inputDop;
    this.previousDop = previousDop;
    this.dopEstimationEnabled = dopEstimationEnabled;
    this.estimateStatus = Objects.requireNonNull(estimateStatus);
    this.estimatedDop = estimatedDop;
    this.inputs = java.util.Collections.unmodifiableList(new java.util.ArrayList<>(inputs));
    this.admissionMemory = admissionMemory;
    admissionStatus = status;
    admissionReason = reason;
  }

  public DynamicDecisionReceipt finish(String status, String reason) {
    return finish(status, reason, admissionMemory);
  }

  public DynamicDecisionReceipt finish(
      String status, String reason, AdmissionMemoryAccount.Snapshot memory) {
    if (!"ADMISSION_ACCEPTED".equals(status) && !"ADMISSION_REJECTED".equals(status))
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    if (!"ADMISSION_PENDING".equals(admissionStatus)) {
      if (admissionStatus.equals(status) && Objects.equals(admissionReason, reason)) return this;
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    }
    return new DynamicDecisionReceipt(
        source,
        decision,
        endpoint,
        checkSequence,
        contextDop,
        inputDop,
        previousDop,
        dopEstimationEnabled,
        estimateStatus,
        estimatedDop,
        status,
        reason,
        inputs,
        memory);
  }

  public DynamicDecisionReceipt withInputs(java.util.List<DynamicFeedbackIdentity> values) {
    if (!"ADMISSION_PENDING".equals(admissionStatus))
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    return new DynamicDecisionReceipt(
        source,
        decision,
        endpoint,
        checkSequence,
        contextDop,
        inputDop,
        previousDop,
        dopEstimationEnabled,
        estimateStatus,
        estimatedDop,
        admissionStatus,
        admissionReason,
        values);
  }

  public java.util.List<DynamicFeedbackIdentity> getInputs() {
    return inputs;
  }

  public String getInputSnapshotChecksum() {
    return inputs.isEmpty() ? null : inputs.get(inputs.size() - 1).getSnapshotChecksum();
  }

  public String getInputPlanIdentityChecksum() {
    return inputs.isEmpty() ? null : inputs.get(inputs.size() - 1).getFragmentPlanChecksum();
  }

  public String getRawReason() {
    return decision.getReason() == DynamicParallelDecisionReason.INPUT_QUALIFICATION_FAILED
        ? source.getPlan().getRawSplitReason()
        : decision.getReason().name();
  }

  public String getCanonicalReason() {
    if (decision.getReason() == DynamicParallelDecisionReason.INPUT_QUALIFICATION_FAILED)
      return source.getPlan().getCanonicalFallbackReason();
    if (decision.getReason() == DynamicParallelDecisionReason.MINIMUM_DOP_REJECTED)
      return "ADMISSION_REJECTED";
    return decision.getReason().name();
  }

  public AdmissionMemoryAccount.Snapshot getAdmissionMemory() {
    return admissionMemory;
  }

  public String getContentChecksum() {
    StringBuilder b = new StringBuilder();
    DynamicIdentityEncoding.field(b, "IOTDB_DYNAMIC_DECISION_RECEIPT_V1");
    for (String value :
        new String[] {
          queryId,
          fragmentId,
          endpoint,
          admissionStatus,
          admissionReason,
          estimateStatus,
          String.valueOf(estimatedDop),
          Boolean.toString(dopEstimationEnabled),
          Long.toString(checkSequence),
          Integer.toString(contextDop),
          Integer.toString(inputDop),
          Integer.toString(previousDop),
          Integer.toString(getTargetDop()),
          decision.getAction().name(),
          Integer.toString(decision.getTargetDop()),
          getRawReason(),
          getCanonicalReason(),
          source.getShape().getChecksum(),
          source.getPlanIdentityChecksum(),
          source.getGroupingChecksum()
        }) DynamicIdentityEncoding.field(b, value);
    b.append(inputs.size()).append("|");
    for (DynamicFeedbackIdentity input : inputs)
      for (String value :
          new String[] {
            input.getQueryId(),
            input.getFragmentId(),
            input.getEndpoint(),
            input.getSourceId(),
            input.getShapeChecksum(),
            input.getSnapshotChecksum(),
            input.getFragmentPlanChecksum(),
            input.getSourcePlanChecksum(),
            input.getGroupingChecksum(),
            Long.toString(input.getSequence()),
            Long.toString(input.getSnapshotGeneration()),
            Long.toString(input.getPlanGeneration())
          }) DynamicIdentityEncoding.field(b, value);
    if (admissionMemory == null) DynamicIdentityEncoding.field(b, null);
    else
      for (long value :
          new long[] {
            admissionMemory.getChecks(),
            admissionMemory.getAllocationAttempts(),
            admissionMemory.getReservations(),
            admissionMemory.getReservedBytes(),
            admissionMemory.getReleases(),
            admissionMemory.getReleasedBytes()
          }) DynamicIdentityEncoding.field(b, Long.toString(value));
    return StaticParallelPlanChecksum.sha256(b.toString());
  }

  public long estimatedRetainedBytes() {
    long bytes = 1024;
    for (String value :
        new String[] {
          queryId, fragmentId, endpoint, admissionStatus, admissionReason, estimateStatus
        }) if (value != null) bytes = Math.addExact(bytes, Math.multiplyExact(2L, value.length()));
    for (DynamicFeedbackIdentity input : inputs)
      bytes = Math.addExact(bytes, input.estimatedRetainedBytes());
    return bytes;
  }

  public DynamicSourcePlan getSource() {
    return source;
  }

  public DynamicParallelScheduleDecision getDecision() {
    return decision;
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

  public long getCheckSequence() {
    return checkSequence;
  }

  public int getContextDop() {
    return contextDop;
  }

  public int getInputDop() {
    return inputDop;
  }

  public int getPreviousDop() {
    return previousDop;
  }

  public int getTargetDop() {
    return source.getPlan().getTargetParallelism();
  }

  public boolean isDopEstimationEnabled() {
    return dopEstimationEnabled;
  }

  public String getEstimateStatus() {
    return estimateStatus;
  }

  public Integer getEstimatedDop() {
    return estimatedDop;
  }

  public String getAdmissionStatus() {
    return admissionStatus;
  }

  public String getAdmissionReason() {
    return admissionReason;
  }
}
