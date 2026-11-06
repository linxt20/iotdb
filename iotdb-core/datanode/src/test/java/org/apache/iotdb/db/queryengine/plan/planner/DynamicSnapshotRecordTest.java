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

import org.apache.iotdb.db.queryengine.statistics.snapshot.*;
import org.apache.iotdb.mpp.rpc.thrift.*;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class DynamicSnapshotRecordTest {
  @Test
  public void independentCopySurvivesCallerAndExportMutation() {
    TFetchFragmentInstanceStatisticsResp response = DynamicSnapshotHistoryStoreTest.response("q0");
    DynamicSourcePlan source = DynamicSnapshotHistoryStoreTest.source("q0", "s", "all");
    DynamicSnapshotRecord.Payload payload =
        DynamicSnapshotRecord.capture(
            "q0", "0.i", DynamicSnapshotHistoryStoreTest.ENDPOINT, response, List.of(source));
    response.setSourceEndpoint("tampered");
    DynamicSnapshotRecord record = DynamicSnapshotRecord.validate(payload);
    record.copyResponse().setSourceEndpoint("again");
    assertEquals(
        DynamicSnapshotHistoryStoreTest.ENDPOINT, record.getSnapshot().getSourceEndpoint());
    assertEquals(1, record.getSourcePlans().size());
    assertTrue(record.isFinishedV2());
    assertTrue(record.getRuntimeView().getSnapshot().getValidity().isEmpty());
    assertEquals(1, record.getRuntimeView().getSnapshot().getFragments().size());
    assertEquals(
        record.getSnapshot().getDrivers().size(), record.getRuntimeView().getDrivers().size());
    assertEquals(
        record.getSnapshot().getPlanIdentity(),
        record.getRuntimeView().getPlanIdentity(record.getSnapshot().getFragmentInstanceId()));
  }

  @Test
  public void wrongScopeEndpointAndChecksumAreRejected() {
    TFetchFragmentInstanceStatisticsResp response = DynamicSnapshotHistoryStoreTest.response("q0");
    DynamicSourcePlan source = DynamicSnapshotHistoryStoreTest.source("q0", "s", "all");
    for (String query : List.of("q1", "q0")) {
      String endpoint = query.equals("q0") ? "other:1" : DynamicSnapshotHistoryStoreTest.ENDPOINT;
      DynamicSnapshotRecord.Payload payload =
          DynamicSnapshotRecord.capture(query, "0.i", endpoint, response, List.of(source));
      assertThrows(IllegalArgumentException.class, () -> DynamicSnapshotRecord.validate(payload));
    }
    response.setSnapshotChecksum("bad");
    DynamicSnapshotRecord.Payload payload =
        DynamicSnapshotRecord.capture(
            "q0", "0.i", DynamicSnapshotHistoryStoreTest.ENDPOINT, response, List.of(source));
    assertThrows(IllegalArgumentException.class, () -> DynamicSnapshotRecord.validate(payload));
  }

  @Test
  public void incompleteOrNonterminalPayloadCannotBecomeARecord() {
    TFetchFragmentInstanceStatisticsResp response = DynamicSnapshotHistoryStoreTest.response("q0");
    response.setIsTerminal(false);
    DynamicParallelTransportCompatibilityTest.signed(response);
    DynamicSnapshotRecord.Payload payload =
        DynamicSnapshotRecord.capture(
            "q0",
            "0.i",
            DynamicSnapshotHistoryStoreTest.ENDPOINT,
            response,
            List.of(DynamicSnapshotHistoryStoreTest.source("q0", "s", "all")));
    assertThrows(IllegalArgumentException.class, () -> DynamicSnapshotRecord.validate(payload));
  }

  @Test
  public void oversizePayloadIsRejectedBeforeCopyAndWithoutSerialization() {
    TFetchFragmentInstanceStatisticsResp response = DynamicSnapshotHistoryStoreTest.response("q0");
    response.setSourceNodeId("x".repeat(1000000));
    assertNull(
        DynamicSnapshotRecord.capture(
            "q0", "0.i", DynamicSnapshotHistoryStoreTest.ENDPOINT, response, List.of()));
    assertEquals(Long.MAX_VALUE, DynamicSnapshotBudget.estimate(new Object(), 1000));
  }

  @Test
  public void cyclicOrDeepContainersCannotDefeatBudget() {
    java.util.List<Object> cycle = new java.util.ArrayList<>();
    cycle.add(cycle);
    assertEquals(Long.MAX_VALUE, DynamicSnapshotBudget.estimate(cycle, 1000000));
  }

  @Test
  public void shapeCoverageCannotClaimAnotherDeviceSet() {
    DynamicSourcePlan original = DynamicSnapshotHistoryStoreTest.source("q0", "s", "all");
    DynamicQueryShape wrong =
        new DynamicQueryShape(
            "db",
            "table",
            "region0",
            "s",
            List.of("other"),
            "all",
            "all",
            Map.of(),
            "none",
            ScanParallelPlan.Mode.DEVICE);
    assertThrows(
        IllegalArgumentException.class,
        () -> new DynamicSourcePlan(wrong, original.getPlan(), original.getGrouping()));
  }

  @Test
  public void alteredCoverageWithUnchangedWorkIdsCannotPassFrameBinding() {
    String query = "q0";
    TFetchFragmentInstanceStatisticsResp response = DynamicSnapshotHistoryStoreTest.response(query);
    List<String> devices = List.of("changed0", "changed1", "changed2", "changed3");
    ScanParallelPlanner.Input input =
        new ScanParallelPlanner.Input(
            devices, List.of(), null, 4, true, false, false, false, false, false, query, "0.i",
            "s");
    DynamicScanGrouping grouping = new DynamicScanGroupingBuilder().build(input);
    ScanParallelPlan plan = new ScanParallelPlanner().planWithGrouping(input, grouping, 4);
    assertEquals(
        response.getWorkUnitIds(),
        plan.getWorkUnits().stream()
            .map(ScanParallelPlan.WorkUnit::getSourceId)
            .collect(java.util.stream.Collectors.toList()));
    DynamicQueryShape shape =
        new DynamicQueryShape(
            "db",
            "table",
            "region0",
            "s",
            devices,
            "all",
            "all",
            Map.of("s", "INT64"),
            "none",
            ScanParallelPlan.Mode.DEVICE);
    DynamicSourcePlan changed = new DynamicSourcePlan(shape, plan, grouping);
    DynamicSnapshotRecord.Payload payload =
        DynamicSnapshotRecord.capture(
            query, "0.i", DynamicSnapshotHistoryStoreTest.ENDPOINT, response, List.of(changed));
    assertThrows(IllegalArgumentException.class, () -> DynamicSnapshotRecord.validate(payload));
  }

  @Test
  public void sourceLexicalDisplayOrderBindsMultipleSourcePlans() {
    String query = "q0";
    DynamicSourcePlan two = DynamicSnapshotHistoryStoreTest.source(query, "2", "all"),
        ten = DynamicSnapshotHistoryStoreTest.source(query, "10", "all");
    TFetchFragmentInstanceStatisticsResp response =
        DynamicSnapshotHistoryStoreTest.bind(
            query, DynamicSnapshotHistoryStoreTest.response(query), List.of(two, ten));
    DynamicSnapshotRecord record =
        DynamicSnapshotRecord.validate(
            DynamicSnapshotRecord.capture(
                query,
                "0.i",
                DynamicSnapshotHistoryStoreTest.ENDPOINT,
                response,
                List.of(two, ten)));
    assertEquals(List.of("10", "2"), new java.util.ArrayList<>(record.getSourcePlans().keySet()));
    assertEquals(8, response.getBucketIdsSize());
  }

  @Test
  public void diagnosticTraceMustMatchCoverageAndTheIndependentlyRetainedReceipt() {
    String query = "trace_binding";
    DynamicSourcePlan source = DynamicSnapshotHistoryStoreTest.source(query, "s", "all");
    DynamicDecisionReceipt receipt =
        new DynamicDecisionReceipt(
                source,
                new DynamicParallelScheduleDecision(
                    DynamicParallelScheduleDecision.Action.KEEP_STATIC,
                    4,
                    DynamicParallelDecisionReason.NO_NEW_FEEDBACK),
                DynamicSnapshotHistoryStoreTest.ENDPOINT,
                1,
                8,
                4,
                4,
                false)
            .finish("ADMISSION_ACCEPTED", "CHECK_DISABLED");
    TFetchFragmentInstanceStatisticsResp response = DynamicSnapshotHistoryStoreTest.response(query);
    response.setDynamicDecisions(List.of(DynamicDecisionTrace.fromReceipt(receipt)));
    assertNotNull(
        DynamicSnapshotRecord.validate(
            DynamicSnapshotRecord.capture(
                query,
                "0.i",
                DynamicSnapshotHistoryStoreTest.ENDPOINT,
                response,
                List.of(source),
                List.of(receipt))));
    for (int kind = 0; kind < 3; kind++) {
      TDynamicDecisionTrace trace = DynamicDecisionTrace.fromReceipt(receipt);
      if (kind == 0) trace.setWorkUnitCount(99);
      if (kind == 1) trace.setReceiptChecksum("another_receipt");
      if (kind == 2) trace.setContextDop(99);
      trace.setTraceChecksum(DynamicDecisionTrace.checksum(trace));
      assertTrue(DynamicDecisionTrace.verifies(trace));
      response.setDynamicDecisions(List.of(trace));
      DynamicSnapshotRecord.Payload payload =
          DynamicSnapshotRecord.capture(
              query,
              "0.i",
              DynamicSnapshotHistoryStoreTest.ENDPOINT,
              response,
              List.of(source),
              List.of(receipt));
      assertThrows(IllegalArgumentException.class, () -> DynamicSnapshotRecord.validate(payload));
    }
  }

  @Test
  public void feedbackDropKeepsCMetricsReadableButCannotBecomeECostHistory() {
    TFetchFragmentInstanceStatisticsResp response =
        DynamicSnapshotHistoryStoreTest.response("drop");
    response.setDynamicFeedbackDropReason("DYNAMIC_FEEDBACK_METADATA_BUDGET_EXCEEDED");
    assertTrue(RuntimeSnapshotAdapters.fromResponse(response).getValidity().isEmpty());
    DynamicSnapshotRecord.Payload payload =
        DynamicSnapshotRecord.capture(
            "drop",
            "0.i",
            DynamicSnapshotHistoryStoreTest.ENDPOINT,
            response,
            List.of(DynamicSnapshotHistoryStoreTest.source("drop", "s", "all")));
    assertThrows(IllegalArgumentException.class, () -> DynamicSnapshotRecord.validate(payload));
  }
}
