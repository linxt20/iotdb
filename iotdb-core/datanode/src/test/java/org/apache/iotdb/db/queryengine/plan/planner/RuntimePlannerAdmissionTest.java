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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.exchange.RuntimeExchangeBlockCodec;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeEndpointFrontier;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.spill.DeviceEntryDataSetHandle;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.HashPartitioningDescriptor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableHashPartitioningShuffleSinkNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Real planner preflight over real plan nodes, without building drivers or starting a service.
 *
 * <p>The package-private budget entry is the function called by native planning before operator
 * construction. These tests cover its source/approval decisions and budget contracts. They do not
 * claim end-to-end query execution, worker-counter hot-path behavior, or scheduler admission.
 */
public class RuntimePlannerAdmissionTest {
  private static final Symbol GROUP_KEY = new Symbol("s1");

  @Test
  public void hashPartialApprovesPlainAndFilterProjectWrappedRawScan() {
    try (AdmissionFixture fixture = new AdmissionFixture("admission_wrapped_hash")) {
      for (int wrappers = 0; wrappers <= 2; wrappers++) {
        DeviceTableScanNode scan = scan("raw_" + wrappers, 4);
        PlanNode source = scan;
        if (wrappers >= 1) {
          source = new FilterNode(id("filter_" + wrappers), source, BooleanLiteral.TRUE_LITERAL);
        }
        if (wrappers >= 2) {
          source =
              new ProjectNode(
                  id("project_" + wrappers),
                  source,
                  Assignments.identity(source.getOutputSymbols()));
        }
        TableHashPartitioningShuffleSinkNode sink =
            hashSource("wrapped_" + wrappers, source, AggregationNode.Step.PARTIAL, fixture);

        LocalExecutionPlanContext.RuntimeExchangeBudget budget =
            LocalExecutionPlanner.runtimeExchangeBudget(sink, fixture.context);

        assertEquals(
            Collections.singleton(scan.getPlanNodeId().getId()),
            budget.groupPrepaidBudgets().keySet());
        assertEquals(
            Collections.singleton(scan.getPlanNodeId().getId()),
            budget.approvedRawScanPlanNodeIds());
        assertFalse(scan.isAllowParallelScan());
        assertBudgetConservation(budget, fixture.context);
      }
      assertNoRuntimeStateCreated(fixture.context);
    }
  }

  @Test
  public void hashPartialDoesNotOverrideScanAdmissionAcrossDiffProject() {
    try (AdmissionFixture fixture = new AdmissionFixture("admission_diff_project")) {
      DeviceTableScanNode scan = scan("diff_project_raw", 4);
      Symbol delta = new Symbol("delta");
      fixture
          .context
          .getTypeProvider()
          .putTableModelType(delta, TypeFactory.getType(TSDataType.DOUBLE));
      ProjectNode project =
          new ProjectNode(
              id("diff_project"),
              scan,
              Assignments.of(
                  delta,
                  new FunctionCall(
                      QualifiedName.of("diff"),
                      Collections.singletonList(GROUP_KEY.toSymbolReference()))));
      FilterNode transparentFilter =
          new FilterNode(id("transparent_filter"), project, BooleanLiteral.TRUE_LITERAL);
      TableHashPartitioningShuffleSinkNode sink =
          hashSource(
              "diff_project_hash", transparentFilter, AggregationNode.Step.PARTIAL, fixture, delta);

      LocalExecutionPlanContext.RuntimeExchangeBudget budget =
          LocalExecutionPlanner.runtimeExchangeBudget(sink, fixture.context);

      assertTrue(budget.groupPrepaidBudgets().containsKey(scan.getPlanNodeId().getId()));
      assertTrue(budget.approvedRawScanPlanNodeIds().isEmpty());
      assertFalse(scan.isAllowParallelScan());
      assertBudgetConservation(budget, fixture.context);
      assertNoRuntimeStateCreated(fixture.context);
    }
  }

  @Test
  public void hashPartialDoesNotOverrideScanAdmissionAcrossDiffFilter() {
    try (AdmissionFixture fixture = new AdmissionFixture("admission_diff_filter")) {
      DeviceTableScanNode scan = scan("diff_filter_raw", 4);
      FilterNode filter =
          new FilterNode(
              id("diff_filter"),
              scan,
              new ComparisonExpression(
                  ComparisonExpression.Operator.GREATER_THAN,
                  new FunctionCall(
                      QualifiedName.of("DIFF"),
                      Collections.singletonList(GROUP_KEY.toSymbolReference())),
                  new LongLiteral("0")));
      ProjectNode transparentProject =
          new ProjectNode(
              id("transparent_project"), filter, Assignments.identity(filter.getOutputSymbols()));
      TableHashPartitioningShuffleSinkNode sink =
          hashSource("diff_filter_hash", transparentProject, AggregationNode.Step.PARTIAL, fixture);

      LocalExecutionPlanContext.RuntimeExchangeBudget budget =
          LocalExecutionPlanner.runtimeExchangeBudget(sink, fixture.context);

      assertTrue(budget.groupPrepaidBudgets().containsKey(scan.getPlanNodeId().getId()));
      assertTrue(budget.approvedRawScanPlanNodeIds().isEmpty());
      assertFalse(scan.isAllowParallelScan());
      assertBudgetConservation(budget, fixture.context);
      assertNoRuntimeStateCreated(fixture.context);
    }
  }

  @Test
  public void hashApprovalsAreIntersectedWithActuallyPayableSourceSubset() {
    try (AdmissionFixture fixture = new AdmissionFixture("admission_paid_subset")) {
      DeviceTableScanNode eligible = scan("hash_paid", 4);
      DeviceTableScanNode single = scan("hash_single", 1);
      DeviceTableScanNode spilled = spilledScan("hash_spilled");
      DeviceTableScanNode limited = scan("hash_global_limit", 4);
      limited.setPushDownLimit(10);
      limited.setPushLimitToEachDevice(false);
      DeviceTableScanNode ordinary = scan("ordinary_paid", 4);
      ordinary.setAllowParallelScan(true);
      CollectNode root = new CollectNode(id("mixed_sources"), Collections.singletonList(GROUP_KEY));
      for (DeviceTableScanNode source : Arrays.asList(eligible, single, spilled, limited)) {
        PlanNode wrapped =
            new ProjectNode(
                id(source.getPlanNodeId().getId() + "_project"),
                new FilterNode(
                    id(source.getPlanNodeId().getId() + "_filter"),
                    source,
                    BooleanLiteral.TRUE_LITERAL),
                Assignments.identity(GROUP_KEY));
        root.addChild(
            hashSource(
                source.getPlanNodeId().getId() + "_hash",
                wrapped,
                AggregationNode.Step.PARTIAL,
                fixture));
      }
      root.addChild(
          new ProjectNode(id("ordinary_projection"), ordinary, Assignments.identity(GROUP_KEY)));

      LocalExecutionPlanContext.RuntimeExchangeBudget budget =
          LocalExecutionPlanner.runtimeExchangeBudget(root, fixture.context);

      assertEquals(
          new HashSet<>(Arrays.asList("hash_paid", "ordinary_paid")),
          budget.groupPrepaidBudgets().keySet());
      assertEquals(Collections.singleton("hash_paid"), budget.approvedRawScanPlanNodeIds());
      assertTrue(
          budget.groupPrepaidBudgets().keySet().containsAll(budget.approvedRawScanPlanNodeIds()));
      assertFalse(eligible.isAllowParallelScan());
      assertTrue(ordinary.isAllowParallelScan());
      assertFalse(budget.groupPrepaidBudgets().containsKey("hash_single"));
      assertFalse(budget.groupPrepaidBudgets().containsKey("hash_spilled"));
      assertFalse(budget.groupPrepaidBudgets().containsKey("hash_global_limit"));
      assertBudgetConservation(budget, fixture.context);
      assertNoRuntimeStateCreated(fixture.context);
    }
  }

  @Test
  public void unsafeRawSourcesStayUnpaidAndHaveSafeSerialCandidate() {
    try (AdmissionFixture fixture = new AdmissionFixture("admission_serial_safety")) {
      DeviceTableScanNode single = scan("single", 1);
      DeviceTableScanNode spilled = spilledScan("spilled");
      DeviceTableScanNode limited = scan("limited", 4);
      limited.setPushDownLimit(10);
      limited.setPushLimitToEachDevice(false);
      for (DeviceTableScanNode scan : Arrays.asList(single, spilled, limited)) {
        scan.setAllowParallelScan(true);
        TableHashPartitioningShuffleSinkNode root =
            hashSource(
                scan.getPlanNodeId().getId() + "_hash",
                scan,
                AggregationNode.Step.PARTIAL,
                fixture);

        // Returning an empty paid set must be a valid preflight result, not a constructor error
        // caused by retaining approval for a scan that cannot safely be split.
        LocalExecutionPlanContext.RuntimeExchangeBudget budget =
            LocalExecutionPlanner.runtimeExchangeBudget(root, fixture.context);
        assertTrue(budget.groupPrepaidBudgets().isEmpty());
        assertTrue(budget.approvedRawScanPlanNodeIds().isEmpty());
        assertEquals(budget.outputCapBytes(), budget.nativeOutputHeadroomBytes());
        assertBudgetConservation(budget, fixture.context);

        List<String> devices = new ArrayList<>();
        for (DeviceEntry entry : scan.getDeviceEntries()) {
          devices.add(entry.getDeviceID().toString());
        }
        ScanParallelPlan serial =
            new ScanParallelPlanner()
                .plan(
                    new ScanParallelPlanner.Input(
                        devices,
                        Collections.emptyList(),
                        fixture.context.getDegreeOfParallelism(),
                        true,
                        scan.getDeviceEntryDataSetHandle().isPresent(),
                        scan.getPushDownLimit() > 0 && !scan.isPushLimitToEachDevice(),
                        false,
                        false,
                        false));
        assertEquals(ScanParallelPlan.Mode.SERIAL, serial.getMode());
        assertEquals(1, serial.getPipelineCount());
      }
      assertNoRuntimeStateCreated(fixture.context);
    }
  }

  @Test
  public void ordinaryPaidScanDoesNotNeedHashApproval() {
    try (AdmissionFixture fixture = new AdmissionFixture("admission_ordinary")) {
      DeviceTableScanNode scan = scan("ordinary", 4);
      scan.setAllowParallelScan(true);

      LocalExecutionPlanContext.RuntimeExchangeBudget budget =
          LocalExecutionPlanner.runtimeExchangeBudget(scan, fixture.context);

      assertEquals(Collections.singleton("ordinary"), budget.groupPrepaidBudgets().keySet());
      assertTrue(budget.approvedRawScanPlanNodeIds().isEmpty());
      assertTrue(scan.isAllowParallelScan());
      assertEquals(0L, budget.inputCapBytes());
      assertBudgetConservation(budget, fixture.context);
      assertNoRuntimeStateCreated(fixture.context);
    }
  }

  @Test
  public void hashSourcesWithNonPartialAggregationDoNotReceiveOverrideApproval() {
    try (AdmissionFixture fixture = new AdmissionFixture("admission_wrong_step")) {
      for (AggregationNode.Step step :
          Arrays.asList(
              AggregationNode.Step.SINGLE,
              AggregationNode.Step.FINAL,
              AggregationNode.Step.INTERMEDIATE)) {
        DeviceTableScanNode scan = scan("step_" + step.name(), 4);
        TableHashPartitioningShuffleSinkNode root =
            hashSource("sink_" + step.name(), scan, step, fixture);

        LocalExecutionPlanContext.RuntimeExchangeBudget budget =
            LocalExecutionPlanner.runtimeExchangeBudget(root, fixture.context);

        assertEquals(
            Collections.singleton(scan.getPlanNodeId().getId()),
            budget.groupPrepaidBudgets().keySet());
        assertTrue(budget.approvedRawScanPlanNodeIds().isEmpty());
        assertFalse(scan.isAllowParallelScan());
        assertBudgetConservation(budget, fixture.context);
      }
    }
  }

  @Test
  public void exchangeBoundaryDoesNotChargeOrApproveRemoteScanSubtree() {
    try (AdmissionFixture fixture = new AdmissionFixture("admission_exchange_boundary")) {
      DeviceTableScanNode local = scan("local", 4);
      local.setAllowParallelScan(true);
      DeviceTableScanNode remote = scan("remote", 4);
      TableHashPartitioningShuffleSinkNode remoteSink =
          hashSource("remote_hash", remote, AggregationNode.Step.PARTIAL, fixture);
      ExchangeNode exchange = new ExchangeNode(id("remote_exchange"));
      exchange.setOutputSymbols(remoteSink.getOutputSymbols());
      exchange.setChild(remoteSink);
      exchange.setUpstream(
          new TEndPoint("192.0.2.2", 10740),
          fixture.peer(1, "remote_source"),
          remoteSink.getPlanNodeId());
      CollectNode root =
          new CollectNode(id("exchange_collect"), Collections.singletonList(GROUP_KEY));
      root.addChild(new ProjectNode(id("local_project"), local, Assignments.identity(GROUP_KEY)));
      root.addChild(exchange);

      LocalExecutionPlanContext.RuntimeExchangeBudget budget =
          LocalExecutionPlanner.runtimeExchangeBudget(root, fixture.context);

      assertEquals(Collections.singleton("local"), budget.groupPrepaidBudgets().keySet());
      assertTrue(budget.approvedRawScanPlanNodeIds().isEmpty());
      assertTrue(budget.inputCapBytes() > 0);
      assertTrue(budget.outputCapBytes() >= budget.inputCapBytes());
      assertTrue(budget.outputCapBytes() - budget.inputCapBytes() <= 1);
      assertFalse(remote.isAllowParallelScan());
      assertBudgetConservation(budget, fixture.context);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void budgetContractRejectsApprovalWithoutMatchingPrepayment() {
    new LocalExecutionPlanContext.RuntimeExchangeBudget(
        0,
        1024L * 1024,
        512L * 1024,
        4096,
        Collections.singletonMap("paid", 4096L),
        Collections.singleton("unpaid_approval"),
        16L * 1024 * 1024);
  }

  @Test
  public void staticPlanningGuardDoesNotCreateRuntimeStateOrCounterOwner() throws Exception {
    try (AdmissionFixture fixture = new AdmissionFixture("admission_static_guard")) {
      DeviceTableScanNode scan = scan("static", 4);
      scan.setAllowParallelScan(true);
      assertNull(fixture.context.getInstanceContext().getRuntimeLifecycle());
      Method initialize =
          LocalExecutionPlanner.class.getDeclaredMethod(
              "initializeRuntimeExchangeBudget", PlanNode.class, LocalExecutionPlanContext.class);
      initialize.setAccessible(true);

      // Exercise the native planner's actual lifecycle guard, without entering services, operator
      // construction, or a live-worker path. No lifecycle means no runtime counter owner to attach.
      initialize.invoke(LocalExecutionPlanner.getInstance(), scan, fixture.context);

      assertNull(fixture.context.getInstanceContext().getRuntimeLifecycle());
      assertNoRuntimeStateCreated(fixture.context);
      assertEquals(0, fixture.context.getPipelineNumber());
      assertTrue(
          ((DataDriverContext) fixture.context.getDriverContext()).getSourceOperators().isEmpty());
      assertTrue(scan.isAllowParallelScan());
    }
  }

  @Test
  public void physicalEndpointPaymentsUseOneAccountPerChannelAndCorrectSideCaps() throws Exception {
    try (AdmissionFixture fixture = new AdmissionFixture("admission_endpoint_payments")) {
      RuntimeExchangeBlockCodec.Bounds bounds = smallEndpointBounds();
      long outputBytes = Math.multiplyExact(2, bounds.outputEnvelopeBytes());
      long inputBytes = bounds.inputEnvelopeBytes();
      LocalExecutionPlanContext.RuntimeExchangeBudget budget =
          new LocalExecutionPlanContext.RuntimeExchangeBudget(
              inputBytes,
              outputBytes,
              outputBytes,
              4096,
              Map.of(),
              Collections.emptySet(),
              16L * 1024 * 1024,
              Map.of("sink", List.of(bounds, bounds)),
              Map.of("exchange", bounds));
      MemoryPool pool = pool("endpoint-payments", Math.addExact(inputBytes, outputBytes));
      fixture.context.initializeRuntimeExchangeBudget(pool, budget);
      FragmentInstanceId id = fixture.context.getFragmentInstanceId();
      assertEquals(3, fixture.context.getOwnedRuntimeFrontiers().size());
      assertEquals(
          outputBytes,
          pool.getFragmentMemoryReservedBytes(
              id.getQueryId().getId(), id.getFragmentInstanceId(), MemorySide.OUTPUT));
      assertEquals(
          inputBytes,
          pool.getFragmentMemoryReservedBytes(
              id.getQueryId().getId(), id.getFragmentInstanceId(), MemorySide.INPUT));
      assertEquals(Math.addExact(inputBytes, outputBytes), pool.getReservedBytes());
      assertEquals(inputBytes, fixture.context.getInstanceContext().getInputMaxBytes());
      assertEquals(outputBytes, fixture.context.getInstanceContext().getOutputMaxBytes());
      RuntimeEndpointFrontier first = fixture.context.getRuntimeOutputFrontier("sink", 0);
      RuntimeEndpointFrontier second = fixture.context.getRuntimeOutputFrontier("sink", 1);
      RuntimeEndpointFrontier input = fixture.context.getRuntimeInputFrontier("exchange");
      assertEquals(MemorySide.OUTPUT, first.reservation().getMemorySide());
      assertEquals(MemorySide.INPUT, input.reservation().getMemorySide());
      assertTrue(first.reservation() != second.reservation());
      assertSame(bounds, first.bounds());
      assertEquals(bounds.outputEnvelopeBytes(), first.reservation().getBytesPerLane());
      assertEquals(bounds.inputEnvelopeBytes(), input.reservation().getBytesPerLane());
      for (RuntimeFrontierReservation owner : fixture.context.getOwnedRuntimeFrontiers()) {
        owner.requestClose().get(5, TimeUnit.SECONDS);
      }
      assertEquals(0, pool.getReservedBytes());
      pool.deRegisterFragmentInstanceFromQueryMemoryMap(
          id.getQueryId().getId(), id.getFragmentInstanceId(), true);
    }
  }

  @Test
  public void loopbackExchangeStillReceivesRpcInputEnvelopeAndSinkChildSchema() {
    try (AdmissionFixture fixture = new AdmissionFixture("admission_loopback_rpc")) {
      ExchangeNode exchange = new ExchangeNode(id("loopback_exchange"));
      exchange.setOutputSymbols(List.of(GROUP_KEY));
      exchange.setUpstream(
          new TEndPoint(
              IoTDBDescriptor.getInstance().getConfig().getInternalAddress(),
              IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort()),
          fixture.peer(1, "loopback_sender"),
          id("loopback_sender_sink"));
      IdentitySinkNode sink = new IdentitySinkNode(id("loopback_receiver_sink"));
      sink.addChild(exchange);
      sink.addDownStreamChannelLocation(
          new DownStreamChannelLocation(
              new TEndPoint("192.0.2.7", 10740),
              fixture.peer(2, "next_receiver").toThrift(),
              "next_exchange"));
      LocalExecutionPlanContext.RuntimeExchangeBudget budget =
          LocalExecutionPlanner.runtimeExchangeBudget(sink, fixture.context);
      assertEquals(
          Collections.singleton("loopback_exchange"), budget.inputEndpointBounds().keySet());
      assertEquals(
          Collections.singleton("loopback_receiver_sink"), budget.outputEndpointBounds().keySet());
      assertEquals(
          List.of(TSDataType.INT32),
          budget.inputEndpointBounds().get("loopback_exchange").schema());
      assertEquals(
          List.of(TSDataType.INT32),
          budget.outputEndpointBounds().get("loopback_receiver_sink").get(0).schema());
      assertTrue(
          budget.inputCapBytes()
              >= budget.inputEndpointBounds().get("loopback_exchange").inputEnvelopeBytes());
      assertTrue(budget.groupPrepaidBudgets().isEmpty());
      assertNoRuntimeStateCreated(fixture.context);
    }
  }

  @Test
  public void missingEndpointSchemaFailsBeforeFirstPhysicalReservation() {
    try (AdmissionFixture fixture = new AdmissionFixture("admission_missing_schema")) {
      fixture.context.getTypeProvider().setTableModelTypes(Collections.emptyMap());
      IdentitySinkNode sink = new IdentitySinkNode(id("missing_schema_sink"));
      sink.addChild(scan("missing_schema_scan", 4));
      sink.addDownStreamChannelLocation(
          new DownStreamChannelLocation(
              new TEndPoint("192.0.2.8", 10740),
              fixture.peer(1, "schema_peer").toThrift(),
              "schema_exchange"));
      MemoryPool pool = pool("missing-schema", 1024 * 1024);
      assertThrows(
          IllegalArgumentException.class,
          () -> LocalExecutionPlanner.runtimeExchangeBudget(sink, fixture.context));
      assertEquals(0, pool.getReservedBytes());
      assertNoRuntimeStateCreated(fixture.context);
    }
  }

  @Test
  public void partialEndpointPaymentFailureRetainsEveryPaidOwnerForRollback() throws Exception {
    try (AdmissionFixture fixture = new AdmissionFixture("admission_partial_payment")) {
      RuntimeExchangeBlockCodec.Bounds bounds = smallEndpointBounds();
      long one = bounds.outputEnvelopeBytes();
      long outputCap = Math.multiplyExact(2, one);
      LocalExecutionPlanContext.RuntimeExchangeBudget budget =
          new LocalExecutionPlanContext.RuntimeExchangeBudget(
              0,
              outputCap,
              outputCap,
              4096,
              Map.of(),
              Collections.emptySet(),
              16L * 1024 * 1024,
              Map.of("sink", List.of(bounds, bounds)),
              Map.of());
      // The first payment fits; the second fails the real shared physical pool.
      MemoryPool pool = pool("partial-payment", Math.addExact(one, one / 2));
      assertThrows(
          IllegalStateException.class,
          () -> fixture.context.initializeRuntimeExchangeBudget(pool, budget));
      assertTrue(fixture.context.isRuntimeParallelismEnabled());
      assertEquals(1, fixture.context.getOwnedRuntimeFrontiers().size());
      RuntimeFrontierReservation owner = fixture.context.getOwnedRuntimeFrontiers().get(0);
      assertSame(owner, fixture.context.getRuntimeOutputFrontier("sink", 0).reservation());
      assertEquals(one, pool.getReservedBytes());
      assertFalse(owner.getClosedFuture().isDone());
      owner.requestClose().get(5, TimeUnit.SECONDS);
      assertEquals(0, pool.getReservedBytes());
      assertTrue(owner.getClosedFuture().isDone());
      // The owner remains available as evidence until the overall successful cleanup clears it.
      assertSame(owner, fixture.context.getOwnedRuntimeFrontiers().get(0));
    }
  }

  @Test
  public void endpointEnvelopeSumAndUnboundSchemaAreRejectedBeforePayment() {
    RuntimeExchangeBlockCodec.Bounds bounds = smallEndpointBounds();
    long output = bounds.outputEnvelopeBytes();
    long input = bounds.inputEnvelopeBytes();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LocalExecutionPlanContext.RuntimeExchangeBudget(
                0,
                output,
                output - 1,
                4096,
                Map.of(),
                Collections.emptySet(),
                16L * 1024 * 1024,
                Map.of("sink", List.of(bounds)),
                Map.of()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LocalExecutionPlanContext.RuntimeExchangeBudget(
                input - 1,
                output,
                output,
                4096,
                Map.of(),
                Collections.emptySet(),
                16L * 1024 * 1024,
                Map.of(),
                Map.of("exchange", bounds)));
    RuntimeExchangeBlockCodec.Bounds unbound =
        new RuntimeExchangeBlockCodec.Bounds(16, 1, 128, 256, 512, 1024, 8192);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new LocalExecutionPlanContext.RuntimeExchangeBudget(
                0,
                output,
                output,
                4096,
                Map.of(),
                Collections.emptySet(),
                16L * 1024 * 1024,
                Map.of("sink", List.of(unbound)),
                Map.of()));
  }

  private static RuntimeExchangeBlockCodec.Bounds smallEndpointBounds() {
    return new RuntimeExchangeBlockCodec.Bounds(16, 1, 128, 256, 512, 1024, 8192)
        .withSchema(List.of(TSDataType.INT32));
  }

  private static MemoryPool pool(String name, long bytes) {
    return new MemoryPool(name, new MemoryManager(bytes), bytes);
  }

  private static void assertNoRuntimeStateCreated(LocalExecutionPlanContext context) {
    assertFalse(context.isRuntimeParallelismEnabled());
    assertTrue(context.getOwnedRuntimeFrontiers().isEmpty());
    assertTrue(context.getRuntimeGroupDescriptors().isEmpty());
  }

  private static void assertBudgetConservation(
      LocalExecutionPlanContext.RuntimeExchangeBudget budget, LocalExecutionPlanContext context) {
    long total = IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance();
    assertEquals(total, Math.addExact(budget.inputCapBytes(), budget.outputCapBytes()));
    long prepaid = 0;
    for (Long bytes : budget.groupPrepaidBudgets().values()) {
      assertTrue(bytes > 0);
      assertEquals(
          (long) context.getDegreeOfParallelism() * budget.maxCompactRetainedBlockBytes(),
          bytes.longValue());
      prepaid = Math.addExact(prepaid, bytes);
    }
    assertTrue(prepaid <= budget.outputCapBytes() - budget.nativeOutputHeadroomBytes());
    assertTrue(
        budget.groupPrepaidBudgets().keySet().containsAll(budget.approvedRawScanPlanNodeIds()));
    if (!budget.groupPrepaidBudgets().isEmpty()) {
      assertTrue(budget.maxCompactRetainedBlockBytes() >= 4096);
    }
  }

  private static TableHashPartitioningShuffleSinkNode hashSource(
      String name, PlanNode source, AggregationNode.Step step, AdmissionFixture fixture) {
    return hashSource(name, source, step, fixture, GROUP_KEY);
  }

  private static TableHashPartitioningShuffleSinkNode hashSource(
      String name,
      PlanNode source,
      AggregationNode.Step step,
      AdmissionFixture fixture,
      Symbol groupKey) {
    AggregationNode aggregation =
        new AggregationNode(
            id(name + "_aggregation"),
            source,
            Collections.emptyMap(),
            AggregationNode.singleGroupingSet(Collections.singletonList(groupKey)),
            Collections.emptyList(),
            step,
            Optional.empty(),
            Optional.empty());
    TableHashPartitioningShuffleSinkNode sink =
        new TableHashPartitioningShuffleSinkNode(
            id(name),
            new HashPartitioningDescriptor(
                Collections.singletonList(groupKey),
                HashPartitioningDescriptor.HashVersion.TABLE_HASH_V1,
                HashPartitioningDescriptor.NullRouting.HASH_NULL,
                2));
    sink.addChild(aggregation);
    for (int channel = 0; channel < 2; channel++) {
      sink.addDownStreamChannelLocation(
          new DownStreamChannelLocation(
              new TEndPoint("192.0.2." + (channel + 3), 10740),
              fixture.peer(2, name + "_receiver_" + channel).toThrift(),
              name + "_exchange_" + channel));
    }
    return sink;
  }

  private static DeviceTableScanNode spilledScan(String name) {
    DeviceTableScanNode scan = scan(name, 4);
    scan.setDeviceEntryDataSetHandle(
        new DeviceEntryDataSetHandle(
            "admission_spill", scan.getPlanNodeId(), new TEndPoint("127.0.0.1", 1), 1, 4, false));
    return scan;
  }

  private static DeviceTableScanNode scan(String name, int deviceCount) {
    Symbol time = new Symbol("time");
    Symbol tag = new Symbol("tag1");
    Map<Symbol, ColumnSchema> assignments = new HashMap<>();
    assignments.put(
        time,
        new ColumnSchema(
            "time", TypeFactory.getType(TSDataType.TIMESTAMP), false, TsTableColumnCategory.TIME));
    assignments.put(
        tag,
        new ColumnSchema(
            "tag1", TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.TAG));
    assignments.put(
        GROUP_KEY,
        new ColumnSchema(
            "s1", TypeFactory.getType(TSDataType.INT32), false, TsTableColumnCategory.FIELD));
    List<DeviceEntry> devices = new ArrayList<>();
    for (int index = 0; index < deviceCount; index++) {
      devices.add(
          new AlignedDeviceEntry(
              IDeviceID.Factory.DEFAULT_FACTORY.create("table1.d" + index), new Binary[0]));
    }
    return new DeviceTableScanNode(
        id(name),
        new QualifiedObjectName("testdb", "table1"),
        Arrays.asList(time, tag, GROUP_KEY),
        assignments,
        devices,
        Collections.singletonMap(tag, 0),
        Ordering.ASC,
        null,
        null,
        0,
        0,
        false,
        false);
  }

  private static PlanNodeId id(String value) {
    return new PlanNodeId(value);
  }

  private static final class AdmissionFixture implements AutoCloseable {
    private final ExecutorService notifications = newDirectExecutorService();
    private final LocalExecutionPlanContext context;
    private final long previousFragmentExchangeBytes =
        IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance();

    private AdmissionFixture(String query) {
      // Endpoint schemas use a fixed cross-FI wire contract. Keep enough capacity for the
      // original mixed-source admission cases independently of the Surefire JVM heap size.
      IoTDBDescriptor.getInstance()
          .getMemoryConfig()
          .setMaxBytesPerFragmentInstance(128L * 1024 * 1024);
      FragmentInstanceId instance =
          new FragmentInstanceId(new PlanFragmentId(new QueryId(query), 0), "0");
      FragmentInstanceStateMachine states =
          new FragmentInstanceStateMachine(instance, notifications);
      FragmentInstanceContext fragment = createFragmentInstanceContext(instance, states);
      fragment.setDataRegion(Mockito.mock(DataRegion.class));
      TypeProvider types = new TypeProvider();
      types.putTableModelType(new Symbol("time"), TypeFactory.getType(TSDataType.TIMESTAMP));
      types.putTableModelType(new Symbol("tag1"), TypeFactory.getType(TSDataType.STRING));
      types.putTableModelType(GROUP_KEY, TypeFactory.getType(TSDataType.INT32));
      context = new LocalExecutionPlanContext(types, fragment, new DataNodeQueryContext(1));
      context.setDegreeOfParallelism(4);
    }

    private FragmentInstanceId peer(int fragment, String instance) {
      return new FragmentInstanceId(
          new PlanFragmentId(context.getFragmentInstanceId().getQueryId(), fragment), instance);
    }

    @Override
    public void close() {
      try {
        // These preflight tests publish no handles or drivers; only the directly owned payments
        // need closing. Failed construction must keep them discoverable until this receipt settles.
        for (RuntimeFrontierReservation owner : context.getOwnedRuntimeFrontiers()) {
          owner.requestClose();
        }
      } finally {
        IoTDBDescriptor.getInstance()
            .getMemoryConfig()
            .setMaxBytesPerFragmentInstance(previousFragmentExchangeBytes);
        notifications.shutdownNow();
      }
    }
  }
}
