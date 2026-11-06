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

import org.apache.iotdb.mpp.rpc.thrift.*;

import org.apache.thrift.*;
import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class DynamicDecisionTraceTest {
  public static TDynamicDecisionTrace fixture() {
    DynamicSourcePlan source =
        DynamicSnapshotHistoryStoreTest.source("trace_query", "trace_source", "all");
    DynamicDecisionReceipt receipt =
        new DynamicDecisionReceipt(
            source,
            new DynamicParallelScheduleDecision(
                DynamicParallelScheduleDecision.Action.KEEP_STATIC,
                4,
                DynamicParallelDecisionReason.NO_NEW_FEEDBACK),
            "127.0.0.1:10730",
            7,
            8,
            4,
            4,
            true,
            "ESTIMATED",
            4);
    return DynamicDecisionTrace.fromReceipt(receipt);
  }

  @Test
  public void typedTraceRoundTripsAndKeepsPendingReceiptAndOptionalEstimate() throws Exception {
    TDynamicDecisionTrace trace = fixture();
    TFetchFragmentInstanceStatisticsResp response = new TFetchFragmentInstanceStatisticsResp();
    response.setDynamicDecisions(Collections.singletonList(trace));
    response.setStatus(org.apache.iotdb.rpc.RpcUtils.SUCCESS_STATUS);
    byte[] wire = new TSerializer(new TCompactProtocol.Factory()).serialize(response);
    TFetchFragmentInstanceStatisticsResp decoded = new TFetchFragmentInstanceStatisticsResp();
    new TDeserializer(new TCompactProtocol.Factory()).deserialize(decoded, wire);
    TDynamicDecisionTrace result = decoded.getDynamicDecisions().get(0);
    assertTrue(DynamicDecisionTrace.verifies(result));
    assertEquals("ADMISSION_PENDING", result.getAdmissionStatus());
    assertEquals(8, result.getContextDop());
    assertEquals(4, result.getInputDop());
    assertEquals(4, result.getEstimatedDop());
    assertFalse(result.isDynamicGrouping());
    assertEquals(4, result.getWorkUnitCount());
    assertEquals(4, result.getActualBucketCount());
    trace.setTargetDop(1);
    assertEquals(4, result.getTargetDop());
  }

  @Test
  public void changingAnyTransportFieldInvalidatesTheDiagnosticProjection() {
    TDynamicDecisionTrace trace = fixture();
    trace.setInputDop(99);
    assertFalse(DynamicDecisionTrace.verifies(trace));
    assertEquals(
        Collections.singletonMap("validity", "CHECKSUM_MISMATCH"),
        DynamicDecisionTrace.display(trace));
    trace = fixture();
    trace.setAdmissionStatus("ADMISSION_REJECTED");
    assertFalse(DynamicDecisionTrace.verifies(trace));
    trace = fixture();
    trace.setInputs(Collections.singletonList(new TDynamicFeedbackIdentity().setQueryId("forged")));
    assertFalse(DynamicDecisionTrace.verifies(trace));
    trace = fixture();
    trace.unsetWorkUnitCount();
    trace.setTraceChecksum(DynamicDecisionTrace.checksum(trace));
    assertFalse(DynamicDecisionTrace.verifies(trace));
    trace = fixture();
    trace.setAdmissionMemory(new TAdmissionMemoryAccount().setReservedBytes(999));
    assertFalse(DynamicDecisionTrace.verifies(trace));
  }

  @Test
  public void registrationBudgetDropsOptionalMetadataAndPreservesThePipelineGate() {
    DynamicSourcePlan source = DynamicSnapshotHistoryStoreTest.source("budget", "s", "all");
    DynamicScanAssignmentRegistry registry =
        new DynamicScanAssignmentRegistry(true, 1, 4096, 16384);
    registry.registerSource(source);
    assertEquals("DYNAMIC_FEEDBACK_METADATA_BUDGET_EXCEEDED", registry.getFeedbackDropReason());
    assertEquals(0, registry.getRetainedMetadataBytes());
    assertTrue(registry.sourcePlans().isEmpty());
    registry.registerBucket(1, source.getPlan(), 0);
    registry.freeze(Set.of(0, 1));
    registry.verifyFinalPipelines(Set.of(0, 1));
    assertThrows(IllegalStateException.class, () -> registry.verifyFinalPipelines(Set.of(0)));
    assertFalse(registry.wantsTerminalFeedback());
    assertNull(registry.assignmentsFor(1));
    var id =
        new org.apache.iotdb.db.queryengine.common.FragmentInstanceId(
            new org.apache.iotdb.db.queryengine.common.PlanFragmentId(
                new org.apache.iotdb.db.queryengine.common.QueryId("budget"), 0),
            "i");
    var context =
        org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext
            .createFragmentInstanceContext(
                id,
                new org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine(
                    id, Runnable::run),
                null,
                false,
                false);
    context.setDynamicScanAssignments(registry);
    context.setDynamicScanAssignments(null);
    assertEquals(registry.getFeedbackDropReason(), context.getDynamicFeedbackDropReason());
  }

  @Test
  public void assignmentAndWorkReferenceLimitsAreCheckedBeforeRetainingLists() {
    DynamicSourcePlan source = DynamicSnapshotHistoryStoreTest.source("budget2", "s", "all");
    DynamicScanAssignmentRegistry assignments =
        new DynamicScanAssignmentRegistry(true, 1000000, 1, 16384);
    assignments.registerSource(source);
    assignments.registerBucket(1, source.getPlan(), 0);
    assertNull(assignments.getFeedbackDropReason());
    assignments.registerBucket(2, source.getPlan(), 1);
    assertNotNull(assignments.getFeedbackDropReason());
    assertEquals(0, assignments.getRetainedMetadataBytes());
    ScanParallelPlanner.Input input =
        new ScanParallelPlanner.Input(
            List.of("d0", "d1", "d2", "d3"),
            List.of(),
            null,
            4,
            true,
            false,
            false,
            false,
            false,
            false,
            "budget2",
            "0.i",
            "s");
    ScanParallelPlan plan =
        new ScanParallelPlanner().planWithGrouping(input, source.getGrouping(), 1);
    DynamicSourcePlan one = new DynamicSourcePlan(source.getShape(), plan, source.getGrouping());
    DynamicScanAssignmentRegistry references =
        new DynamicScanAssignmentRegistry(true, 1000000, 4096, 3);
    references.registerSource(one);
    references.registerBucket(1, plan, 0);
    assertNotNull(references.getFeedbackDropReason());
    assertTrue(references.sourcePlans().isEmpty());
    assertNull(references.assignmentsFor(1));
  }

  @Test
  public void v1CannotClaimEitherNewDiagnosticField() {
    for (boolean drop : new boolean[] {false, true}) {
      TFetchFragmentInstanceStatisticsResp response =
          DynamicParallelTransportCompatibilityTest.response(1);
      if (drop) response.setDynamicFeedbackDropReason("DYNAMIC_FEEDBACK_METADATA_BUDGET_EXCEEDED");
      else response.setDynamicDecisions(List.of(fixture()));
      assertTrue(
          org.apache.iotdb.db.queryengine.statistics.snapshot.RuntimeSnapshotAdapters.fromResponse(
                  response)
              .getValidity()
              .contains(
                  org.apache.iotdb.db.queryengine.statistics.snapshot.RuntimeSnapshotValidity
                      .INCONSISTENT_SCHEMA_VERSION));
    }
  }
}
