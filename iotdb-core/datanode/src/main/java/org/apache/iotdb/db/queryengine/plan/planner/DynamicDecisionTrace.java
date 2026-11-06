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

import org.apache.iotdb.db.queryengine.statistics.snapshot.RuntimeSnapshotChecksum;
import org.apache.iotdb.mpp.rpc.thrift.*;

import org.apache.thrift.*;
import org.apache.thrift.protocol.TCompactProtocol;

import java.util.*;

/**
 * A checksum-bound diagnostic projection of the frozen decision; never a second scheduler input.
 */
public final class DynamicDecisionTrace {
  private DynamicDecisionTrace() {}

  public static TDynamicDecisionTrace fromReceipt(DynamicDecisionReceipt receipt) {
    DynamicSourcePlan source = receipt.getSource();
    TDynamicDecisionTrace trace = new TDynamicDecisionTrace();
    trace.setQueryId(receipt.getQueryId());
    trace.setFragmentId(receipt.getFragmentId());
    trace.setEndpoint(receipt.getEndpoint());
    trace.setSourcePlanNodeId(source.getSourcePlanNodeId());
    trace.setCheckSequence(receipt.getCheckSequence());
    trace.setContextDop(receipt.getContextDop());
    trace.setInputDop(receipt.getInputDop());
    trace.setPreviousDop(receipt.getPreviousDop());
    trace.setTargetDop(receipt.getTargetDop());
    trace.setDopEstimationEnabled(receipt.isDopEstimationEnabled());
    trace.setEstimateStatus(receipt.getEstimateStatus());
    if (receipt.getEstimatedDop() != null) trace.setEstimatedDop(receipt.getEstimatedDop());
    trace.setAction(receipt.getDecision().getAction().name());
    trace.setRawReason(receipt.getRawReason());
    trace.setCanonicalReason(receipt.getCanonicalReason());
    trace.setDynamicGrouping(
        receipt.getDecision().getAction() != DynamicParallelScheduleDecision.Action.KEEP_STATIC
            && receipt.getDecision().getAction()
                != DynamicParallelScheduleDecision.Action.SERIAL_FALLBACK);
    trace.setWorkUnitCount(source.getPlan().getWorkUnits().size());
    trace.setActualBucketCount(source.getPlan().getBuckets().size());
    trace.setShapeChecksum(source.getShape().getChecksum());
    trace.setSourcePlanChecksum(source.getPlanIdentityChecksum());
    trace.setGroupingChecksum(source.getGroupingChecksum());
    trace.setAdmissionStatus(receipt.getAdmissionStatus());
    if (receipt.getAdmissionReason() != null)
      trace.setAdmissionReason(receipt.getAdmissionReason());
    List<TDynamicFeedbackIdentity> inputs = new ArrayList<>();
    for (DynamicFeedbackIdentity input : receipt.getInputs()) {
      TDynamicFeedbackIdentity value = new TDynamicFeedbackIdentity();
      value.setQueryId(input.getQueryId());
      value.setFragmentId(input.getFragmentId());
      value.setEndpoint(input.getEndpoint());
      value.setSourceId(input.getSourceId());
      value.setSnapshotChecksum(input.getSnapshotChecksum());
      value.setFragmentPlanChecksum(input.getFragmentPlanChecksum());
      value.setSourcePlanChecksum(input.getSourcePlanChecksum());
      value.setGroupingChecksum(input.getGroupingChecksum());
      value.setShapeChecksum(input.getShapeChecksum());
      value.setSequence(input.getSequence());
      value.setSnapshotGeneration(input.getSnapshotGeneration());
      value.setPlanGeneration(input.getPlanGeneration());
      inputs.add(value);
    }
    trace.setInputs(inputs);
    AdmissionMemoryAccount.Snapshot memory = receipt.getAdmissionMemory();
    if (memory != null) {
      TAdmissionMemoryAccount value = new TAdmissionMemoryAccount();
      value.setChecks(memory.getChecks());
      value.setAllocationAttempts(memory.getAllocationAttempts());
      value.setReservations(memory.getReservations());
      value.setReservedBytes(memory.getReservedBytes());
      value.setReleases(memory.getReleases());
      value.setReleasedBytes(memory.getReleasedBytes());
      trace.setAdmissionMemory(value);
    }
    trace.setReceiptChecksum(receipt.getContentChecksum());
    trace.setTraceChecksum(checksum(trace));
    return trace;
  }

  public static String checksum(TDynamicDecisionTrace trace) {
    TDynamicDecisionTrace copy = trace.deepCopy();
    copy.unsetTraceChecksum();
    try {
      return RuntimeSnapshotChecksum.sha256(
          Arrays.asList(
              "IOTDB_DYNAMIC_DECISION_TRACE_V1",
              Base64.getEncoder()
                  .encodeToString(
                      new TSerializer(new TCompactProtocol.Factory()).serialize(copy))));
    } catch (TException e) {
      throw new IllegalArgumentException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753,
          e);
    }
  }

  public static boolean verifies(TDynamicDecisionTrace trace) {
    if (trace == null) return false;
    for (int i = 1; i <= 27; i++) {
      if (i == 12 || i == 23 || i == 25) continue;
      org.apache.iotdb.mpp.rpc.thrift.TDynamicDecisionTrace._Fields field = trace.fieldForId(i);
      if (!trace.isSet(field)) return false;
    }
    return trace.getTargetDop() > 0
        && trace.getActualBucketCount() > 0
        && trace.getWorkUnitCount() > 0
        && trace.getTraceChecksum().equals(checksum(trace));
  }

  public static Map<String, Object> display(TDynamicDecisionTrace trace) {
    if (!verifies(trace)) return Collections.singletonMap("validity", "CHECKSUM_MISMATCH");
    return fields(trace);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Map<String, Object> fields(TBase value) {
    Map<String, Object> fields = new LinkedHashMap<>();
    for (int i = 1; i <= 64; i++) {
      TFieldIdEnum field = value.fieldForId(i);
      if (field == null || !value.isSet(field)) continue;
      Object item = value.getFieldValue(field);
      if (item instanceof TBase) item = fields((TBase) item);
      else if (item instanceof List) {
        List<Object> list = new ArrayList<>();
        for (Object element : (List) item)
          list.add(element instanceof TBase ? fields((TBase) element) : element);
        item = list;
      }
      fields.put(field.getFieldName(), item);
    }
    return Collections.unmodifiableMap(fields);
  }
}
