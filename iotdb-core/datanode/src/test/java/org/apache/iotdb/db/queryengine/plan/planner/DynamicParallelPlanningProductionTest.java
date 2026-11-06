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

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.*;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.statistics.snapshot.*;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.mpp.rpc.thrift.*;

import org.junit.*;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DynamicParallelPlanningProductionTest {
  private final DeviceTableScanParallelPipelineTest fixtures =
      new DeviceTableScanParallelPipelineTest();
  private final DataNodeTableOperatorGenerator generator =
      new DataNodeTableOperatorGenerator(new TableMetadataImpl());
  private boolean oldFlag, oldTime, oldEstimate, oldOrdered;

  @BeforeClass
  public static void setup() {
    DeviceTableScanParallelPipelineTest.setUp();
  }

  @AfterClass
  public static void teardown() {
    DeviceTableScanParallelPipelineTest.tearDown();
  }

  @Before
  public void enable() {
    oldFlag = IoTDBDescriptor.getInstance().getConfig().isEnableDynamicScanParallelism();
    oldTime = IoTDBDescriptor.getInstance().getConfig().isEnableTimePartitionMorsel();
    oldEstimate = IoTDBDescriptor.getInstance().getConfig().isEnableDopEstimation();
    oldOrdered = IoTDBDescriptor.getInstance().getConfig().isEnableOrderedParallelScan();
    IoTDBDescriptor.getInstance().getConfig().setEnableDynamicScanParallelism(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(false);
  }

  @After
  public void restore() {
    IoTDBDescriptor.getInstance().getConfig().setEnableDynamicScanParallelism(oldFlag);
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(oldTime);
    IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(oldEstimate);
    IoTDBDescriptor.getInstance().getConfig().setEnableOrderedParallelScan(oldOrdered);
  }

  private LocalExecutionPlanContext context(String name, int dop) {
    LocalExecutionPlanContext c = fixtures.createLocalExecutionPlanContext(name);
    c.setDegreeOfParallelism(dop);
    when(c.getInstanceContext().getDataRegion().getDataRegionIdString()).thenReturn(name);
    return c;
  }

  private ScanParallelPlanner.Input input(
      DeviceTableScanNode node, String query, int dop, List<Long> times, boolean ordered) {
    return new ScanParallelPlanner.Input(
        node.getDeviceEntries().stream()
            .map(d -> String.valueOf(d.getDeviceID()))
            .collect(Collectors.toList()),
        times,
        null,
        dop,
        true,
        false,
        false,
        ordered,
        ordered,
        !times.isEmpty(),
        query,
        "0.i",
        node.getPlanNodeId().getId());
  }

  private void publish(
      DynamicSnapshotHistoryStore history,
      DynamicQueryShape shape,
      DeviceTableScanNode node,
      String query,
      List<Long> times,
      boolean ordered) {
    ScanParallelPlan plan = new ScanParallelPlanner().plan(input(node, query, 4, times, ordered));
    DynamicSourcePlan source = new DynamicSourcePlan(shape, plan, null);
    TFetchFragmentInstanceStatisticsResp response =
        DynamicSnapshotHistoryStoreTest.bind(
            query, DynamicSnapshotHistoryStoreTest.response(query), List.of(source));
    response.setSourceEndpoint(DynamicParallelPlanningService.localEndpoint());
    long[] costs = {9000, 6000, 1500, 3000};
    for (int i = 1; i < response.getDriverSnapshotsSize(); i++)
      response.getDriverSnapshots().get(i).setDriverWallNanos(costs[i - 1]);
    DynamicParallelTransportCompatibilityTest.signed(response);
    assertTrue(
        history.publish(
            DynamicSnapshotRecord.validate(
                DynamicSnapshotRecord.capture(
                    query,
                    "0.i",
                    DynamicParallelPlanningService.localEndpoint(),
                    response,
                    List.of(source)))));
  }

  @Test
  public void flagIsCapturedOnceAndRegistryIsShared() {
    LocalExecutionPlanContext c = context("capture_flag", 4);
    IoTDBDescriptor.getInstance().getConfig().setEnableDynamicScanParallelism(false);
    assertTrue(c.getDynamicScanAssignments().isEnabled());
    assertSame(c.getDynamicScanAssignments(), c.createSubContext().getDynamicScanAssignments());
    assertSame(
        c.getDynamicParallelPlanningService(),
        c.createSubContext().getDynamicParallelPlanningService());
    LocalExecutionPlanContext disabled = context("capture_disabled", 4);
    assertFalse(disabled.getDynamicScanAssignments().isEnabled());
    DeviceTableScanNode node = fixtures.initDeviceTableScanNode(8);
    node.setAllowParallelScan(true);
    disabled.getDynamicParallelPlanningService().prepare(node, disabled);
    assertNull(
        disabled
            .getDynamicParallelPlanningService()
            .select(input(node, "disabled", 4, List.of(), false), disabled)
            .getShape());
  }

  @Test
  public void queryAndRuntimePlanIdsDoNotEnterSemanticShape() {
    LocalExecutionPlanContext c = context("shape_region", 4);
    DeviceTableScanNode a = fixtures.initDeviceTableScanNode(8),
        b = fixtures.initDeviceTableScanNode(8);
    a.setPlanNodeId(new PlanNodeId("runtime_a"));
    b.setPlanNodeId(new PlanNodeId("runtime_b"));
    DynamicParallelPlanningService left = new DynamicParallelPlanningService(),
        right = new DynamicParallelPlanningService();
    left.prepare(a, c);
    right.prepare(b, c);
    assertEquals(
        left.shapeFor("runtime_a", ScanParallelPlan.Mode.DEVICE, c),
        right.shapeFor("runtime_b", ScanParallelPlan.Mode.DEVICE, c));
    FilterNode filtered =
        new FilterNode(
            new PlanNodeId("filter"),
            b,
            new ComparisonExpression(
                ComparisonExpression.Operator.GREATER_THAN,
                new Symbol("s1").toSymbolReference(),
                new LongLiteral("1")));
    DynamicParallelPlanningService withFilter = new DynamicParallelPlanningService();
    withFilter.prepare(filtered, c);
    assertNotEquals(
        left.shapeFor("runtime_a", ScanParallelPlan.Mode.DEVICE, c),
        withFilter.shapeFor("runtime_b", ScanParallelPlan.Mode.DEVICE, c));
  }

  @Test
  public void arrivalMembershipIsFrozenAndActualInputCapWins() {
    LocalExecutionPlanContext c = context("frozen_feedback_cap", 8);
    DeviceTableScanNode node = fixtures.initDeviceTableScanNode(8);
    node.setAllowParallelScan(true);
    DynamicSnapshotHistoryStore history = new DynamicSnapshotHistoryStore();
    DynamicParallelPlanningService frozen =
        new DynamicParallelPlanningService(history, DynamicParallelPlanningService.localEndpoint());
    frozen.prepare(node, c);
    DynamicQueryShape shape =
        frozen.shapeFor(node.getPlanNodeId().getId(), ScanParallelPlan.Mode.DEVICE, c);
    publish(history, shape, node, "cap_sample_0", List.of(), false);
    publish(history, shape, node, "cap_sample_1", List.of(), false);
    assertEquals(
        DynamicParallelScheduleDecision.Action.KEEP_STATIC,
        frozen.select(input(node, "cap_before", 2, List.of(), false), c).getDecision().getAction());
    DynamicParallelPlanningService next =
        new DynamicParallelPlanningService(history, DynamicParallelPlanningService.localEndpoint());
    next.prepare(node, c);
    DynamicParallelPlanningService.Selection selected =
        next.select(input(node, "cap_after", 2, List.of(), false), c);
    assertEquals(
        DynamicParallelScheduleDecision.Action.REBALANCE_NEXT_FRAGMENT,
        selected.getDecision().getAction());
    assertEquals(2, selected.getPlan().getInputDop());
    assertEquals(2, selected.getPlan().getBuckets().size());
    assertEquals(8, selected.getPlan().getWorkUnits().size());
  }

  @Test
  public void actualParallelDriversOwnSourceLocalBucketsAndFreeze() throws Exception {
    LocalExecutionPlanContext c = context("actual_bucket_owners", 4);
    DeviceTableScanNode node = fixtures.initDeviceTableScanNode(8);
    node.setAllowParallelScan(true);
    c.getDynamicParallelPlanningService().prepare(node, c);
    Operator root = node.accept(generator, c);
    try {
      c.freezeDynamicScanAssignments();
      assertTrue(c.getDynamicScanAssignments().isFrozen());
      assertTrue(c.getDynamicScanAssignments().wantsTerminalFeedback());
      assertThrows(
          IllegalStateException.class,
          () -> c.getDynamicParallelPlanningService().prepare(node, c));
      assertThrows(
          IllegalStateException.class,
          () ->
              c.getDynamicParallelPlanningService()
                  .select(input(node, "after_freeze", 4, List.of(), false), c));
      assertNull(
          c.getDynamicScanAssignments()
              .assignmentsFor(c.getDriverContext().getDriverTaskID().getPipelineId()));
      Set<Integer> buckets = new HashSet<>();
      Set<String> work = new HashSet<>();
      for (PipelineDriverFactory factory : c.getPipelineDriverFactories()) {
        List<DynamicDriverBucketAssignment> owned =
            c.getDynamicScanAssignments()
                .assignmentsFor(factory.getDriverContext().getDriverTaskID().getPipelineId());
        assertEquals(1, owned.size());
        assertTrue(buckets.add(owned.get(0).getBucketId()));
        work.addAll(owned.get(0).getWorkUnitIds());
      }
      assertEquals(4, buckets.size());
      assertEquals(4, work.size());
      assertEquals(1, c.getDynamicScanAssignments().sourcePlans().size());
      assertThrows(
          IllegalStateException.class,
          () ->
              c.getDynamicScanAssignments()
                  .registerBucket(
                      99,
                      c.getDynamicScanAssignments().sourcePlans().iterator().next().getPlan(),
                      0));
    } finally {
      close(root, c);
    }
  }

  @Test
  public void twoSerialSourcesShareOneActualDriverWithoutLosingEitherSource() throws Exception {
    LocalExecutionPlanContext c = context("serial_multi", 1);
    for (String id : List.of("source_a", "source_b")) {
      DeviceTableScanNode node = fixtures.initDeviceTableScanNode(4);
      node.setPlanNodeId(new PlanNodeId(id));
      // Prepare a semantic source independently; assignment registration is shared by the context.
      c.getDynamicParallelPlanningService().prepare(node, c);
      Operator root = node.accept(generator, c);
      root.close();
    }
    c.freezeDynamicScanAssignments();
    assertEquals(
        2,
        c.getDynamicScanAssignments()
            .assignmentsFor(c.getDriverContext().getDriverTaskID().getPipelineId())
            .size());
    assertEquals(2, c.getDynamicScanAssignments().sourcePlans().size());
  }

  @Test
  public void duplicateOwnershipAndUnpublishedPipelineAreRejected() {
    DeviceTableScanNode node = fixtures.initDeviceTableScanNode(8);
    ScanParallelPlan plan =
        new ScanParallelPlanner().plan(input(node, "registry", 4, List.of(), false));
    DynamicScanAssignmentRegistry registry = new DynamicScanAssignmentRegistry(true);
    registry.registerBucket(7, plan, 0);
    assertThrows(IllegalStateException.class, () -> registry.registerBucket(8, plan, 0));
    assertThrows(IllegalStateException.class, () -> registry.freeze(Set.of(8)));
  }

  @Test
  public void dynamicDeviceConsumerReadsEveryWorkUnitExactlyOnce() throws Exception {
    LocalExecutionPlanContext c = context("dynamic_device_consumer", 4);
    DeviceTableScanNode node = fixtures.initDeviceTableScanNode(12);
    node.setAllowParallelScan(true);
    DynamicParallelPlanningService service = c.getDynamicParallelPlanningService();
    service.prepare(node, c);
    DynamicQueryShape shape =
        service.shapeFor(node.getPlanNodeId().getId(), ScanParallelPlan.Mode.DEVICE, c);
    publish(
        DynamicParallelPlanningService.historyStore(),
        shape,
        node,
        "device_seed_0",
        List.of(),
        false);
    publish(
        DynamicParallelPlanningService.historyStore(),
        shape,
        node,
        "device_seed_1",
        List.of(),
        false);
    service.prepare(node, c);
    Operator root = node.accept(generator, c);
    try {
      assertEquals(
          DynamicParallelScheduleDecision.Action.REBALANCE_NEXT_FRAGMENT,
          service.selections().iterator().next().getDecision().getAction());
      c.freezeDynamicScanAssignments();
      DynamicSourcePlan selected = c.getDynamicScanAssignments().sourcePlans().iterator().next();
      assertEquals(12, selected.getPlan().getWorkUnits().size());
      DynamicDecisionReceipt receipt = c.getDynamicScanAssignments().receipts().iterator().next();
      assertEquals(2, receipt.getInputs().size());
      assertEquals("device_seed_0", receipt.getInputs().get(0).getQueryId());
      assertEquals("device_seed_1", receipt.getInputs().get(1).getQueryId());
      assertEquals(
          receipt.getInputs().get(1).getSnapshotChecksum(), receipt.getInputSnapshotChecksum());
      assertEquals(
          receipt.getInputs().get(1).getFragmentPlanChecksum(),
          receipt.getInputPlanIdentityChecksum());
      assertEquals("COST_SKEW", receipt.getRawReason());
      assertEquals("COST_SKEW", receipt.getCanonicalReason());
      DynamicDecisionReceipt finished = receipt.finish("ADMISSION_ACCEPTED", null);
      assertEquals(receipt.getInputs(), finished.getInputs());
      assertThrows(UnsupportedOperationException.class, () -> finished.getInputs().clear());
      int devices = 0;
      for (PipelineDriverFactory factory : c.getPipelineDriverFactories()) {
        Operator scan =
            ((DataDriverContext) factory.getDriverContext()).getSourceOperators().get(0);
        devices +=
            Integer.parseInt(
                scan.getOperatorContext()
                    .getSpecifiedInfo()
                    .get(
                        org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter
                            .DEVICE_NUMBER)
                    .toString());
      }
      assertEquals(12, devices);
    } finally {
      close(root, c);
    }
  }

  @Test
  public void writerKeepsAssignmentsAfterManagerClearsPlanningContext() throws Exception {
    verifyWriterAndPublication(false);
  }

  @Test
  public void ordinaryTerminalQueryPublishesBeforeOriginalCleanup() throws Exception {
    verifyWriterAndPublication(true);
  }

  private void verifyWriterAndPublication(boolean terminalListener) throws Exception {
    String name = terminalListener ? "actual_terminal_publication" : "actual_v2_writer";
    LocalExecutionPlanContext c = context(name, 4);
    DeviceTableScanNode node = fixtures.initDeviceTableScanNode(8);
    node.setAllowParallelScan(true);
    c.getDynamicParallelPlanningService().prepare(node, c);
    Operator root = node.accept(generator, c);
    try {
      c.freezeDynamicScanAssignments();
      c.getInstanceContext().setActualPipelineCount(c.getPipelineNumber() + 1);
      c.getInstanceContext().setAdmissionAccepted("CHECK_DISABLED");
      c.finalizeDynamicAdmission();
      c.getInstanceContext().setPlanningContext(null);
      org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext context =
          spy(c.getInstanceContext());
      doNothing().when(context).releaseResourceWhenAllDriversAreClosed();
      doNothing().when(context).releaseMemoryReservationManager();
      org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine state =
          new org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine(
              context.getId(), Runnable::run);
      List<org.apache.iotdb.db.queryengine.execution.driver.DriverContext> driverContexts =
          new ArrayList<>();
      for (PipelineDriverFactory factory : c.getPipelineDriverFactories())
        driverContexts.add(factory.getDriverContext());
      driverContexts.add(c.getDriverContext());
      List<org.apache.iotdb.db.queryengine.execution.driver.IDriver> drivers = new ArrayList<>();
      for (org.apache.iotdb.db.queryengine.execution.driver.DriverContext dc : driverContexts) {
        org.apache.iotdb.db.queryengine.execution.driver.IDriver driver =
            mock(org.apache.iotdb.db.queryengine.execution.driver.IDriver.class);
        when(driver.getDriverContext()).thenReturn(dc);
        when(driver.getDriverTaskId()).thenReturn(dc.getDriverTaskID());
        when(driver.getFirstProcessNanos()).thenReturn(100L);
        when(driver.getLastProcessNanos()).thenReturn(10100L);
        drivers.add(driver);
      }
      org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink sink =
          mock(org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink.class);
      org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager exchange =
          mock(org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.class);
      java.lang.reflect.Constructor<
              org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceExecution>
          ctor =
              org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceExecution.class
                  .getDeclaredConstructor(
                      org.apache.iotdb.db.queryengine.common.FragmentInstanceId.class,
                      org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext
                          .class,
                      List.class,
                      org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink.class,
                      org.apache.iotdb.db.queryengine.execution.fragment
                          .FragmentInstanceStateMachine.class,
                      long.class,
                      org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager
                          .class);
      ctor.setAccessible(true);
      org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceExecution execution =
          ctor.newInstance(context.getId(), context, drivers, sink, state, 1000L, exchange);
      if (terminalListener) {
        java.lang.reflect.Method initialize =
            execution
                .getClass()
                .getDeclaredMethod(
                    "initialize",
                    org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler.class,
                    boolean.class);
        initialize.setAccessible(true);
        initialize.invoke(
            execution,
            mock(org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler.class),
            false);
        state.finished();
        assertNull(context.getDynamicScanAssignments());
        DynamicQueryShape shape =
            c.getDynamicScanAssignments().sourcePlans().iterator().next().getShape();
        List<DynamicSnapshotHistoryStore.Entry> history =
            DynamicParallelPlanningService.historyStore()
                .history(shape, DynamicParallelPlanningService.localEndpoint());
        assertEquals(1, history.size());
        assertEquals(1, history.get(0).getRecord().getDecisionReceipts().size());
        assertEquals(
            "ADMISSION_ACCEPTED",
            history
                .get(0)
                .getRecord()
                .getDecisionReceipts()
                .values()
                .iterator()
                .next()
                .getAdmissionStatus());
        assertThrows(
            UnsupportedOperationException.class,
            () -> history.get(0).getRecord().getDecisionReceipts().clear());
        assertEquals(
            context.getId().getQueryId().getId(),
            history.get(0).getRecord().getSnapshot().getQueryId());
        assertTrue(new DynamicSnapshotAggregator().aggregate(history.get(0).getRecord()).isValid());
        org.mockito.InOrder cleanup = inOrder(sink, drivers.get(0), context, exchange);
        cleanup.verify(sink).close();
        cleanup.verify(drivers.get(0)).close();
        cleanup.verify(context).releaseResourceWhenAllDriversAreClosed();
        cleanup
            .verify(exchange)
            .deRegisterFragmentInstanceFromMemoryPool(
                context.getId().getQueryId().getId(),
                context.getId().getFragmentInstanceId(),
                true);
        cleanup.verify(context).releaseMemoryReservationManager();
      } else {
        state.finished();
        java.lang.reflect.Field f = execution.getClass().getDeclaredField("statisticsLock");
        f.setAccessible(true);
        java.util.concurrent.locks.ReentrantReadWriteLock lock =
            (java.util.concurrent.locks.ReentrantReadWriteLock) f.get(execution);
        lock.writeLock().lock();
        try {
          TFetchFragmentInstanceStatisticsResp response = execution.buildStatistics();
          assertEquals(2, response.getSchemaVersion());
          assertEquals(1, response.getDynamicDecisionsSize());
          org.apache.iotdb.mpp.rpc.thrift.TDynamicDecisionTrace trace =
              response.getDynamicDecisions().get(0);
          assertTrue(DynamicDecisionTrace.verifies(trace));
          assertEquals("ADMISSION_ACCEPTED", trace.getAdmissionStatus());
          assertEquals("CHECK_DISABLED", trace.getAdmissionReason());
          assertEquals(4, trace.getContextDop());
          assertEquals(4, trace.getInputDop());
          assertEquals(4, trace.getActualBucketCount());
          assertEquals(
              c.getDynamicScanAssignments().receipts().iterator().next().getContentChecksum(),
              trace.getReceiptChecksum());
          assertTrue(response.isIsTerminal());
          assertEquals(5, response.getDriverSnapshotsSize());
          assertFalse(response.getDriverSnapshots().get(4).isSetScanAssignments());
          for (int i = 0; i < 4; i++)
            assertEquals(1, response.getDriverSnapshots().get(i).getScanAssignmentsSize());
          DynamicSnapshotRecord record =
              DynamicSnapshotRecord.validate(
                  DynamicSnapshotRecord.capture(
                      context.getId().getQueryId().getId(),
                      context.getId().getFragmentInstanceId(),
                      DynamicParallelPlanningService.localEndpoint(),
                      response,
                      c.getDynamicScanAssignments().sourcePlans()));
          assertTrue(new DynamicSnapshotAggregator().aggregate(record).isValid());
        } finally {
          lock.writeLock().unlock();
        }
      }
    } finally {
      close(root, c);
    }
  }

  @Test
  public void actualTimeConsumerUsesAll1024DynamicUnitsWithoutWeightedOverwrite() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(true);
    LocalExecutionPlanContext c = context("dynamic_time_1024", 4);
    TsFileManager manager = mock(TsFileManager.class);
    Set<Long> partitions = new LinkedHashSet<>();
    for (long i = 0; i < 1024; i++) partitions.add(i);
    when(manager.getTimePartitions()).thenReturn(partitions);
    when(((DataRegion) c.getInstanceContext().getDataRegion()).getTsFileManager())
        .thenReturn(manager);
    DeviceTableScanNode node = fixtures.initDeviceTableScanNode(2);
    node.setAllowParallelScan(true);
    DynamicParallelPlanningService service = c.getDynamicParallelPlanningService();
    service.prepare(node, c);
    DynamicQueryShape shape =
        service.shapeFor(node.getPlanNodeId().getId(), ScanParallelPlan.Mode.TIME_PARTITION, c);
    List<Long> times = new ArrayList<>(partitions);
    publish(
        DynamicParallelPlanningService.historyStore(),
        shape,
        node,
        "time_1024_seed_0",
        times,
        false);
    publish(
        DynamicParallelPlanningService.historyStore(),
        shape,
        node,
        "time_1024_seed_1",
        times,
        false);
    service.prepare(node, c);
    Operator root = node.accept(generator, c);
    try {
      c.freezeDynamicScanAssignments();
      DynamicSourcePlan source = c.getDynamicScanAssignments().sourcePlans().iterator().next();
      assertEquals(1024, source.getPlan().getWorkUnits().size());
      assertEquals(
          DynamicParallelScheduleDecision.Action.REBALANCE_NEXT_FRAGMENT,
          service.selections().iterator().next().getDecision().getAction());
      int children = 0;
      for (PipelineDriverFactory factory : c.getPipelineDriverFactories()) {
        List<? extends Operator> scans =
            ((DataDriverContext) factory.getDriverContext()).getSourceOperators();
        children += scans.size();
        for (Operator scan : scans)
          assertEquals(
              "DYNAMIC_WORK_UNIT_LPT",
              scan.getOperatorContext().getSpecifiedInfo().get("MORSEL_SCHEDULING"));
      }
      assertEquals(1024, children);
    } finally {
      close(root, c);
    }
  }

  @Test
  public void actualOrderedConsumerRetainsAll2048DeviceSubscans() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableOrderedParallelScan(true);
    LocalExecutionPlanContext c = context("dynamic_ordered_2048", 4);
    DeviceTableScanNode node = fixtures.initDeviceTableScanNode(2048);
    org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode sort =
        new org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode(
            new PlanNodeId("sort"),
            node,
            new org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme(
                List.of(new Symbol("time")),
                Map.of(
                    new Symbol("time"),
                    org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder
                        .ASC_NULLS_FIRST)),
            false,
            false);
    DynamicParallelPlanningService service = c.getDynamicParallelPlanningService();
    service.prepare(sort, c);
    DynamicQueryShape shape =
        service.shapeFor(node.getPlanNodeId().getId(), ScanParallelPlan.Mode.ORDERED, c);
    publish(
        DynamicParallelPlanningService.historyStore(),
        shape,
        node,
        "ordered_2048_seed_0",
        List.of(),
        true);
    publish(
        DynamicParallelPlanningService.historyStore(),
        shape,
        node,
        "ordered_2048_seed_1",
        List.of(),
        true);
    service.prepare(sort, c);
    Operator root = sort.accept(generator, c);
    try {
      c.freezeDynamicScanAssignments();
      assertEquals(
          1024,
          c.getDynamicScanAssignments()
              .sourcePlans()
              .iterator()
              .next()
              .getPlan()
              .getWorkUnits()
              .size());
      int children = 0;
      for (PipelineDriverFactory factory : c.getPipelineDriverFactories())
        children += ((DataDriverContext) factory.getDriverContext()).getSourceOperators().size();
      assertEquals(2048, children);
      assertEquals(
          org.apache.iotdb.calc.execution.operator.process.TableMergeSortOperator.class,
          root.getClass());
    } finally {
      close(root, c);
    }
  }

  @Test
  public void trustedRejectedPlansReduceOnlyFutureFragmentsAndReceiptsFinalizeOnce()
      throws Exception {
    String region = "admission_chain";
    int[] targets = {4, 3, 2, 1, 1};
    for (int round = 0; round < targets.length; round++) {
      LocalExecutionPlanContext c = context(region + "_query_" + round, 4);
      when(c.getInstanceContext().getDataRegion().getDataRegionIdString()).thenReturn(region);
      DeviceTableScanNode node = fixtures.initDeviceTableScanNode(8);
      node.setAllowParallelScan(true);
      DynamicParallelPlanningService service = c.getDynamicParallelPlanningService();
      service.prepare(node, c);
      if (round == 1) {
        DynamicQueryShape shape =
            service.shapeFor(node.getPlanNodeId().getId(), ScanParallelPlan.Mode.DEVICE, c);
        publish(
            DynamicParallelPlanningService.historyStore(),
            shape,
            node,
            "admission_priority_seed_0",
            List.of(),
            false);
        publish(
            DynamicParallelPlanningService.historyStore(),
            shape,
            node,
            "admission_priority_seed_1",
            List.of(),
            false);
        service.prepare(node, c);
      }
      Operator root = node.accept(generator, c);
      try {
        c.freezeDynamicScanAssignments();
        DynamicDecisionReceipt pending = c.getDynamicScanAssignments().receipts().iterator().next();
        assertEquals("ADMISSION_PENDING", pending.getAdmissionStatus());
        assertEquals(targets[round], pending.getTargetDop());
        assertEquals(4, pending.getInputDop());
        assertEquals(4, pending.getContextDop());
        DynamicParallelScheduleDecision.Action action =
            round == 0
                ? DynamicParallelScheduleDecision.Action.KEEP_STATIC
                : round == 4
                    ? DynamicParallelScheduleDecision.Action.SERIAL_FALLBACK
                    : DynamicParallelScheduleDecision.Action.REDUCE_DOP;
        assertEquals(action, pending.getDecision().getAction());
        if (round == 1) assertTrue(pending.getInputs().isEmpty());
        if (round == 4) {
          assertEquals(0, c.getPipelineNumber());
          assertEquals(ScanParallelPlan.Mode.SERIAL, pending.getSource().getPlan().getMode());
        }
        c.getInstanceContext().setAdmissionRejected("INSUFFICIENT_OPERATOR_MEMORY");
        c.finalizeDynamicAdmission();
        DynamicDecisionReceipt rejected =
            c.getDynamicScanAssignments().receipts().iterator().next();
        assertEquals("ADMISSION_REJECTED", rejected.getAdmissionStatus());
        assertSame(
            rejected,
            c.getDynamicScanAssignments()
                .finishAdmission("ADMISSION_REJECTED", "INSUFFICIENT_OPERATOR_MEMORY")
                .iterator()
                .next());
        assertThrows(
            IllegalStateException.class,
            () -> c.getDynamicScanAssignments().finishAdmission("ADMISSION_ACCEPTED", null));
        assertEquals(
            targets[round],
            DynamicParallelPlanningService.historyStore()
                .admissionRejection(rejected.getSource().getShape(), rejected.getEndpoint())
                .getPreviousDop());
      } finally {
        close(root, c);
      }
    }
  }

  @Test
  public void actual192MiBEstimateCapsContextEightAndRequestedFourAtTwo() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(true);
    LocalExecutionPlanContext c = context("actual_estimate_192m", 8);
    TsFileManager manager = mock(TsFileManager.class);
    org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource file =
        mock(org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource.class);
    when(file.getTsFileSize()).thenReturn(64L * 1024 * 1024);
    when(manager.getAllTsFileListForQuery(null, null))
        .thenReturn(
            new org.apache.tsfile.utils.Pair<>(
                java.util.Arrays.asList(file, file, file), List.of()));
    when(manager.getTimePartitions()).thenReturn(Set.of(0L));
    when(((DataRegion) c.getInstanceContext().getDataRegion()).getTsFileManager())
        .thenReturn(manager);
    DeviceTableScanNode node = fixtures.initDeviceTableScanNode(8);
    node.setAllowParallelScan(true);
    DynamicParallelPlanningService service = c.getDynamicParallelPlanningService();
    service.prepare(node, c);
    DynamicQueryShape shape =
        service.shapeFor(node.getPlanNodeId().getId(), ScanParallelPlan.Mode.DEVICE, c);
    publish(
        DynamicParallelPlanningService.historyStore(),
        shape,
        node,
        "estimate_cap_prior_0",
        List.of(),
        false);
    publish(
        DynamicParallelPlanningService.historyStore(),
        shape,
        node,
        "estimate_cap_prior_1",
        List.of(),
        false);
    service.prepare(node, c);
    Operator root = node.accept(generator, c);
    try {
      c.freezeDynamicScanAssignments();
      DynamicDecisionReceipt receipt = c.getDynamicScanAssignments().receipts().iterator().next();
      assertEquals(8, receipt.getContextDop());
      assertEquals(4, receipt.getPreviousDop());
      assertEquals(
          DynamicParallelScheduleDecision.Action.REBALANCE_NEXT_FRAGMENT,
          receipt.getDecision().getAction());
      assertEquals(2, receipt.getInputDop());
      assertEquals(2, receipt.getTargetDop());
      assertEquals(Integer.valueOf(2), receipt.getEstimatedDop());
      assertEquals("ESTIMATED_FROM_TSFILE_BYTES", receipt.getEstimateStatus());
      ScanParallelPlanner.Input actual =
          input(node, "requested_four", receipt.getInputDop(), List.of(), false);
      ScanParallelPlan proposed =
          new ScanParallelPlanner()
              .planWithGrouping(actual, new DynamicScanGroupingBuilder().build(actual), 4);
      assertEquals(2, proposed.getBuckets().size());
    } finally {
      close(root, c);
    }
  }

  @Test
  public void oneDynamicTimeBucketStillBuilds1024ChildrenWithWeightedMetadataAvailable()
      throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(true);
    boolean oldWeighted =
        IoTDBDescriptor.getInstance().getConfig().isEnableTimePartitionMorselSizeWeighting();
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorselSizeWeighting(true);
    try {
      TsFileManager manager = mock(TsFileManager.class);
      Set<Long> partitions = new LinkedHashSet<>();
      for (long i = 0; i < 1024; i++) partitions.add(i);
      when(manager.getTimePartitions()).thenReturn(partitions);
      org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource file =
          mock(org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource.class);
      when(file.getTsFileSize()).thenReturn(100L);
      when(manager.getTsFileListSnapshot(org.mockito.ArgumentMatchers.anyLong()))
          .thenReturn(new org.apache.tsfile.utils.Pair<>(List.of(file), List.of()));
      for (int round = 0; round < 2; round++) {
        LocalExecutionPlanContext c = context("one_time_bucket_query_" + round, round == 0 ? 2 : 4);
        when(c.getInstanceContext().getDataRegion().getDataRegionIdString())
            .thenReturn("one_time_bucket_region");
        when(((DataRegion) c.getInstanceContext().getDataRegion()).getTsFileManager())
            .thenReturn(manager);
        DeviceTableScanNode node = fixtures.initDeviceTableScanNode(2);
        node.setAllowParallelScan(true);
        c.getDynamicParallelPlanningService().prepare(node, c);
        Operator root = node.accept(generator, c);
        try {
          c.freezeDynamicScanAssignments();
          DynamicDecisionReceipt receipt =
              c.getDynamicScanAssignments().receipts().iterator().next();
          if (round == 0) {
            c.getInstanceContext().setAdmissionRejected("INSUFFICIENT_OPERATOR_MEMORY");
            c.finalizeDynamicAdmission();
          } else {
            assertEquals(
                DynamicParallelScheduleDecision.Action.REDUCE_DOP,
                receipt.getDecision().getAction());
            assertEquals(1, receipt.getTargetDop());
            assertEquals(1, c.getPipelineNumber());
            assertEquals(1024, receipt.getSource().getPlan().getWorkUnits().size());
            List<? extends Operator> scans =
                ((DataDriverContext) c.getPipelineDriverFactories().get(0).getDriverContext())
                    .getSourceOperators();
            assertEquals(1024, scans.size());
            for (Operator scan : scans)
              assertEquals(
                  "DYNAMIC_WORK_UNIT_LPT",
                  scan.getOperatorContext().getSpecifiedInfo().get("MORSEL_SCHEDULING"));
          }
        } finally {
          close(root, c);
        }
      }
    } finally {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableTimePartitionMorselSizeWeighting(oldWeighted);
    }
  }

  @Test
  public void emptyInputKeepsItsSafetyReasonBeforeGroupingAndCostWork() {
    LocalExecutionPlanContext c = context("empty_safety_priority", 4);
    DeviceTableScanNode node = fixtures.initDeviceTableScanNode(0);
    DynamicParallelPlanningService service = c.getDynamicParallelPlanningService();
    service.prepare(node, c);
    DynamicParallelPlanningService.Selection selection =
        service.select(input(node, "empty_safety", 4, List.of(), false), c);
    assertEquals(
        DynamicParallelScheduleDecision.Action.SERIAL_FALLBACK,
        selection.getDecision().getAction());
    assertEquals(
        ScanParallelPlan.FallbackReason.EMPTY_INPUT, selection.getPlan().getFallbackReason());
    assertEquals(1, selection.getPlan().getWorkUnits().size());
    assertEquals(1, selection.getPlan().getBuckets().size());
  }

  @Test
  public void runtimeSymbolAliasesNormalizeButPhysicalTypesAndSourcePositionsRemainDistinct() {
    LocalExecutionPlanContext c = context("canonical_symbol_aliases", 4);
    DeviceTableScanNode a = fixtures.initDeviceTableScanNode(8),
        b = fixtures.initDeviceTableScanNode(8);
    a.setPlanNodeId(new PlanNodeId("runtime_a"));
    b.setPlanNodeId(new PlanNodeId("runtime_b"));
    Map<Symbol, org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema>
        renamed = new LinkedHashMap<>();
    for (Map.Entry<
            Symbol, org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema>
        entry : a.getAssignments().entrySet())
      renamed.put(new Symbol(entry.getKey().getName() + "_generated_42"), entry.getValue());
    b.setAssignments(renamed);
    b.setOutputSymbols(
        a.getOutputSymbols().stream()
            .map(symbol -> new Symbol(symbol.getName() + "_generated_42"))
            .collect(Collectors.toList()));
    FilterNode filterA =
        new FilterNode(
            new PlanNodeId("fa"),
            a,
            new ComparisonExpression(
                ComparisonExpression.Operator.GREATER_THAN,
                new Symbol("s1").toSymbolReference(),
                new LongLiteral("1")));
    FilterNode filterB =
        new FilterNode(
            new PlanNodeId("fb"),
            b,
            new ComparisonExpression(
                ComparisonExpression.Operator.GREATER_THAN,
                new Symbol("s1_generated_42").toSymbolReference(),
                new LongLiteral("1")));
    DynamicParallelPlanningService first = new DynamicParallelPlanningService(),
        second = new DynamicParallelPlanningService();
    first.prepare(filterA, c);
    second.prepare(filterB, c);
    assertEquals(
        first.shapeFor("runtime_a", ScanParallelPlan.Mode.DEVICE, c),
        second.shapeFor("runtime_b", ScanParallelPlan.Mode.DEVICE, c));
    org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode aliases =
        new org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode(
            new PlanNodeId("collect"), java.util.Arrays.asList(a, b), a.getOutputSymbols());
    DynamicParallelPlanningService multi = new DynamicParallelPlanningService();
    multi.prepare(aliases, c);
    assertNotEquals(
        multi.shapeFor("runtime_a", ScanParallelPlan.Mode.DEVICE, c),
        multi.shapeFor("runtime_b", ScanParallelPlan.Mode.DEVICE, c));
    renamed.put(
        new Symbol("s1_generated_42"),
        new org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema(
            "s1",
            org.apache.tsfile.read.common.type.TypeFactory.getType(
                org.apache.tsfile.enums.TSDataType.INT64),
            false,
            org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.FIELD));
    b.setAssignments(renamed);
    DynamicParallelPlanningService changed = new DynamicParallelPlanningService();
    changed.prepare(filterB, c);
    assertNotEquals(
        first.shapeFor("runtime_a", ScanParallelPlan.Mode.DEVICE, c),
        changed.shapeFor("runtime_b", ScanParallelPlan.Mode.DEVICE, c));
  }

  @Test
  public void unsupportedTimeRouteCannotOverwriteTheEstimatedSerialSafetyReason() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(true);
    LocalExecutionPlanContext c = context("unsupported_time_serial_guard", 4);
    TsFileManager manager = mock(TsFileManager.class);
    when(manager.getTimePartitions()).thenReturn(Set.of(Long.MIN_VALUE, Long.MAX_VALUE));
    org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource file =
        mock(org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource.class);
    when(file.getTsFileSize()).thenReturn(64L * 1024 * 1024);
    when(manager.getAllTsFileListForQuery(null, null))
        .thenReturn(new org.apache.tsfile.utils.Pair<>(List.of(file), List.of()));
    when(((DataRegion) c.getInstanceContext().getDataRegion()).getTsFileManager())
        .thenReturn(manager);
    DeviceTableScanNode node = fixtures.initDeviceTableScanNode(8);
    node.setAllowParallelScan(true);
    c.getDynamicParallelPlanningService().prepare(node, c);
    Operator root = node.accept(generator, c);
    try {
      c.freezeDynamicScanAssignments();
      DynamicDecisionReceipt receipt = c.getDynamicScanAssignments().receipts().iterator().next();
      assertEquals(1, receipt.getInputDop());
      assertEquals(1, receipt.getTargetDop());
      assertEquals(0, c.getPipelineNumber());
      assertEquals("DOP_NOT_GREATER_THAN_ONE", receipt.getRawReason());
      assertEquals("DOP_NOT_GREATER_THAN_ONE", receipt.getSource().getPlan().getRawSplitReason());
    } finally {
      close(root, c);
    }
  }

  @Test
  public void keepStaticReceiptUsesTheSelectedCartesianPlanTargetRatherThanTheInputCap()
      throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(true);
    LocalExecutionPlanContext c = context("keep_static_cartesian_target", 8);
    TsFileManager manager = mock(TsFileManager.class);
    when(manager.getTimePartitions()).thenReturn(Set.of(0L, 1L, 2L));
    when(((DataRegion) c.getInstanceContext().getDataRegion()).getTsFileManager())
        .thenReturn(manager);
    DeviceTableScanNode node = fixtures.initDeviceTableScanNode(4);
    node.setAllowParallelScan(true);
    c.getDynamicParallelPlanningService().prepare(node, c);
    Operator root = node.accept(generator, c);
    try {
      c.freezeDynamicScanAssignments();
      DynamicDecisionReceipt receipt = c.getDynamicScanAssignments().receipts().iterator().next();
      assertEquals(8, receipt.getInputDop());
      assertEquals(6, receipt.getTargetDop());
      assertEquals(6, receipt.getDecision().getTargetDop());
      assertEquals(6, c.getPipelineNumber());
      assertEquals(
          DynamicParallelScheduleDecision.Action.KEEP_STATIC, receipt.getDecision().getAction());
      assertEquals(64, receipt.getContentChecksum().length());
      assertNotEquals(
          receipt.getContentChecksum(),
          receipt.finish("ADMISSION_ACCEPTED", null).getContentChecksum());
    } finally {
      close(root, c);
    }
  }

  @Test
  public void timeConsumerKeepsExplicitEmptyBucketsAfterMeasuredZeroCostRebalance()
      throws Exception {
    zeroCostLeavesAnOwnedEmptyPipeline(false);
  }

  @Test
  public void orderedConsumerKeepsExplicitEmptyBucketsAfterMeasuredZeroCostRebalance()
      throws Exception {
    zeroCostLeavesAnOwnedEmptyPipeline(true);
  }

  private void zeroCostLeavesAnOwnedEmptyPipeline(boolean ordered) throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableOrderedParallelScan(ordered);
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(!ordered);
    String name = ordered ? "owned_empty_ordered" : "owned_empty_time";
    LocalExecutionPlanContext c = context(name, 4);
    TsFileManager manager = mock(TsFileManager.class);
    when(manager.getTimePartitions()).thenReturn(Set.of(0L, 1L));
    when(((DataRegion) c.getInstanceContext().getDataRegion()).getTsFileManager())
        .thenReturn(manager);
    DeviceTableScanNode node = fixtures.initDeviceTableScanNode(ordered ? 4 : 2);
    node.setAllowParallelScan(true);
    org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode plan = node;
    if (ordered)
      plan =
          new org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode(
              new PlanNodeId("sort"),
              node,
              new org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme(
                  List.of(new Symbol("time")),
                  Map.of(
                      new Symbol("time"),
                      org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder
                          .ASC_NULLS_FIRST)),
              false,
              false);
    DynamicParallelPlanningService service = c.getDynamicParallelPlanningService();
    service.prepare(plan, c);
    DynamicQueryShape shape =
        service.shapeFor(
            node.getPlanNodeId().getId(),
            ordered ? ScanParallelPlan.Mode.ORDERED : ScanParallelPlan.Mode.TIME_PARTITION,
            c);
    for (int round = 0; round < 2; round++) {
      String query = name + "_seed_" + round;
      ScanParallelPlan old =
          new ScanParallelPlanner()
              .plan(input(node, query, 4, ordered ? List.of() : List.of(0L, 1L), ordered));
      DynamicSourcePlan source = new DynamicSourcePlan(shape, old, null);
      TFetchFragmentInstanceStatisticsResp response =
          DynamicSnapshotHistoryStoreTest.bind(
              query, DynamicSnapshotHistoryStoreTest.response(query), List.of(source));
      response.setSourceEndpoint(DynamicParallelPlanningService.localEndpoint());
      long[] costs = {10000, 1, 0, 0};
      for (int i = 1; i < response.getDriverSnapshotsSize(); i++)
        response.getDriverSnapshots().get(i).setDriverWallNanos(costs[i - 1]);
      DynamicParallelTransportCompatibilityTest.signed(response);
      assertTrue(
          DynamicParallelPlanningService.historyStore()
              .publish(
                  DynamicSnapshotRecord.validate(
                      DynamicSnapshotRecord.capture(
                          query,
                          "0.i",
                          DynamicParallelPlanningService.localEndpoint(),
                          response,
                          List.of(source)))));
    }
    service.prepare(plan, c);
    Operator root = plan.accept(generator, c);
    try {
      c.freezeDynamicScanAssignments();
      DynamicDecisionReceipt receipt = c.getDynamicScanAssignments().receipts().iterator().next();
      assertEquals(
          DynamicParallelScheduleDecision.Action.REBALANCE_NEXT_FRAGMENT,
          receipt.getDecision().getAction());
      assertEquals(4, c.getPipelineNumber());
      assertTrue(
          receipt.getSource().getPlan().getBuckets().stream()
              .anyMatch(b -> b.getWorkUnits().isEmpty()));
      int empty = 0, scans = 0;
      for (PipelineDriverFactory factory : c.getPipelineDriverFactories()) {
        List<? extends Operator> sources =
            ((DataDriverContext) factory.getDriverContext()).getSourceOperators();
        scans += sources.size();
        if (sources.isEmpty()) {
          empty++;
          assertEquals(
              org.apache.iotdb.db.queryengine.execution.operator.EmptyDataOperator.class,
              factory.getOperation().getClass());
          List<DynamicDriverBucketAssignment> owned =
              c.getDynamicScanAssignments()
                  .assignmentsFor(factory.getDriverContext().getDriverTaskID().getPipelineId());
          assertEquals(1, owned.size());
          assertTrue(owned.get(0).getWorkUnitIds().isEmpty());
          // isInputDriver is the legacy data-driver flag. Empty scan ownership is proved by
          // the explicit source/bucket/work assignment rather than inferred from this label.
          assertTrue(
              receipt
                  .getSource()
                  .getPlan()
                  .getBuckets()
                  .get(owned.get(0).getBucketId())
                  .getWorkUnits()
                  .isEmpty());
        }
      }
      assertEquals(1, empty);
      assertEquals(4, scans);
    } finally {
      close(root, c);
    }
  }

  @Test
  public void coldStaticCandidateIsBudgetCheckedBeforeRepeatedCoverageAllocation() {
    LocalExecutionPlanContext c = context("cold_static_materialization_budget", 128);
    DeviceTableScanNode node = fixtures.initDeviceTableScanNode(10000);
    node.setAllowParallelScan(true);
    List<Long> partitions = new ArrayList<>();
    for (long i = 0; i < 1000; i++) partitions.add(i);
    DynamicParallelPlanningService service = c.getDynamicParallelPlanningService();
    service.prepare(node, c);
    DynamicParallelPlanningService.Selection selected =
        service.select(input(node, "cold_budget", 128, partitions, false), c);
    assertEquals(
        DynamicParallelScheduleDecision.Action.SERIAL_FALLBACK, selected.getDecision().getAction());
    assertEquals(
        ScanParallelPlan.FallbackReason.DYNAMIC_GROUPING_BUDGET_EXCEEDED,
        selected.getPlan().getFallbackReason());
    assertEquals(1, selected.getPlan().getBuckets().size());
    assertEquals(1, selected.getPlan().getWorkUnits().size());
    assertEquals(10000, selected.getPlan().getWorkUnits().get(0).getDevices().size());
    assertEquals(1000, selected.getPlan().getWorkUnits().get(0).getTimePartitions().size());
    assertEquals(0, c.getResourceRegistry().getObjectCount());
    assertEquals(0, c.getPipelineNumber());
  }

  @Test
  public void specializedScansDoNotEnterPlainScanSemanticExtraction() {
    LocalExecutionPlanContext c = context("specialized_scan_scope", 4);
    DeviceTableScanNode specialized =
        mock(
            org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode
                .class);
    when(specialized.getPlanNodeId()).thenReturn(new PlanNodeId("aggregate_scan"));
    when(specialized.getChildren()).thenReturn(List.of());
    when(specialized.getQualifiedObjectName())
        .thenThrow(
            new AssertionError("specialized metadata must remain with its original visitor"));
    DynamicParallelPlanningService service = c.getDynamicParallelPlanningService();
    service.prepare(specialized, c);
    assertNull(service.shapeFor("aggregate_scan", ScanParallelPlan.Mode.DEVICE, c));
    assertTrue(c.getDynamicScanAssignments().sourcePlans().isEmpty());
  }

  @Test
  public void unavailableSemanticMetadataSkipsFeedbackWithoutFailingPreparation() {
    LocalExecutionPlanContext c = context("missing_semantic_metadata", 4);
    DeviceTableScanNode node = fixtures.initDeviceTableScanNode(8);
    node.setOutputSymbols(List.of(new Symbol("unresolved_output")));
    DynamicParallelPlanningService service = c.getDynamicParallelPlanningService();
    service.prepare(node, c);
    assertNull(service.shapeFor(node.getPlanNodeId().getId(), ScanParallelPlan.Mode.DEVICE, c));
  }

  @Test
  public void unmanagedFragmentsDoNotActivateTheOrdinaryFeedbackCollector() {
    DynamicScanAssignmentRegistry registry = new DynamicScanAssignmentRegistry(true);
    registry.freeze(Set.of(0));
    assertTrue(registry.isEnabled());
    assertFalse(registry.wantsTerminalFeedback());
  }

  @Test
  public void runningDeltaPairsQualifiedDriverIdsFromTheActualCAdapterPath() {
    String query = "adapter_running_delta";
    org.apache.iotdb.db.queryengine.common.FragmentInstanceId id =
        new org.apache.iotdb.db.queryengine.common.FragmentInstanceId(
            new org.apache.iotdb.db.queryengine.common.PlanFragmentId(
                new org.apache.iotdb.db.queryengine.common.QueryId(query), 0),
            "i");
    QueryRuntimeSnapshot[] snapshots = new QueryRuntimeSnapshot[2];
    for (int index = 0; index < 2; index++) {
      long generation = index + 1;
      TFetchFragmentInstanceStatisticsResp response =
          DynamicSnapshotHistoryStoreTest.response(query);
      response.setSnapshotGeneration(generation);
      response.setCapturedAtNanos(generation * 10000);
      response.setIsTerminal(false);
      response.setState("RUNNING");
      for (TDriverRuntimeSnapshot driver : response.getDriverSnapshots()) {
        driver.setDriverSampleGeneration(generation);
        driver.setDriverFirstProcessNanos(100);
        driver.setDriverLastProcessNanos(100 + generation * 1000);
        driver.setDriverWallNanos(generation * 1000);
        driver.setDriverOutputRows(index == 0 ? 2 : 5);
        driver.setDriverTsBlockCount(generation * 2);
        driver.setValidityReasons(List.of());
      }
      DynamicParallelTransportCompatibilityTest.signed(response);
      RuntimeSnapshotFetchResult fetched =
          new RuntimeSnapshotFetchResult(Map.of(id, response), Map.of());
      assertFalse(fetched.isPartial());
      snapshots[index] =
          fetched.merge(
              query,
              generation,
              generation * 10000,
              Set.of(id.getFragmentInstanceId()),
              Set.of(),
              DynamicSnapshotHistoryStoreTest.ENDPOINT);
      assertTrue(snapshots[index].getValidity().isEmpty());
    }
    assertEquals("0.i", snapshots[0].getFragments().get(0).getFragmentInstanceId());
    assertEquals(
        query + ".0.i",
        snapshots[0].getFragments().get(0).getDrivers().get(0).getFragmentInstanceId());
    RuntimeSnapshotDelta delta = RuntimeSnapshotDelta.between(snapshots[0], snapshots[1]);
    assertTrue(delta.getValidity().isEmpty());
    assertEquals(Long.valueOf(2), delta.getTsBlocksByDriverKey().values().iterator().next());
    assertEquals(5, delta.getOutputRowsByDriverKey().size());
    RuntimeSnapshotDriverKey key =
        new RuntimeSnapshotDriverKey(query, "0.i", "1", query + ".0.i.1");
    assertEquals(Long.valueOf(3), delta.getOutputRowsByDriverKey().get(key));
    assertEquals(Long.valueOf(1000), delta.getWallNanosByDriverKey().get(key));
  }

  @Test
  public void actualLocalPlannerEntryPreparesFreezesAndAdmitsWithTheFlagOnAndOff()
      throws Exception {
    int originalDop = IoTDBDescriptor.getInstance().getConfig().getDegreeOfParallelism();
    int originalNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    if (originalNodeId < 0) IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    boolean originalMemory =
        IoTDBDescriptor.getInstance().getMemoryConfig().isEnableQueryMemoryEstimation();
    IoTDBDescriptor.getInstance().getConfig().setDegreeOfParallelism(4);
    IoTDBDescriptor.getInstance().getMemoryConfig().setEnableQueryMemoryEstimation(true);
    LocalExecutionPlanner planner = LocalExecutionPlanner.getInstance();
    try {
      for (boolean flag : new boolean[] {true, false}) {
        IoTDBDescriptor.getInstance().getConfig().setEnableDynamicScanParallelism(flag);
        LocalExecutionPlanContext fixture = context("actual_local_entry_" + flag, 4);
        org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext instance =
            spy(fixture.getInstanceContext());
        when(instance.getSessionInfo())
            .thenReturn(
                new org.apache.iotdb.commons.queryengine.common.SessionInfo(
                    0,
                    "root",
                    java.time.ZoneId.of("UTC"),
                    "db",
                    org.apache.iotdb.commons.queryengine.common.SqlDialect.TABLE));
        DeviceTableScanNode node = fixtures.initDeviceTableScanNode(8);
        node.setAllowParallelScan(true);
        long freeBefore = planner.getFreeMemoryForOperators();
        boolean verified = false;
        try {
          List<PipelineDriverFactory> factories =
              planner.plan(
                  node,
                  fixture.getTypeProvider(),
                  instance,
                  new org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext(1));
          assertEquals(5, factories.size());
          DynamicScanAssignmentRegistry registry = instance.getDynamicScanAssignments();
          assertTrue(registry.isFrozen());
          assertEquals(flag, registry.isEnabled());
          if (flag) {
            // Removing prepare() from LocalExecutionPlanner loses this source and receipt.
            assertEquals(1, registry.sourcePlans().size());
            assertEquals(1, registry.receipts().size());
            DynamicDecisionReceipt receipt = registry.receipts().iterator().next();
            assertEquals("ADMISSION_ACCEPTED", receipt.getAdmissionStatus());
            assertEquals(4, receipt.getInputDop());
            assertEquals(1, instance.getAdmissionMemoryAccount().snapshot().getChecks());
            assertEquals(1, instance.getAdmissionMemoryAccount().snapshot().getReservations());
          } else {
            assertTrue(registry.sourcePlans().isEmpty());
            assertTrue(registry.receipts().isEmpty());
            assertNull(instance.getAdmissionMemoryAccount());
          }
          java.util.Set<Integer> pipelines = new java.util.HashSet<>();
          for (PipelineDriverFactory factory : factories)
            pipelines.add(factory.getDriverContext().getDriverTaskID().getPipelineId());
          registry.verifyFinalPipelines(pipelines);
          verified = true;
        } finally {
          LocalExecutionPlanContext actual = instance.getPlanningContext();
          if (actual != null)
            for (PipelineDriverFactory factory : actual.getPipelineDriverFactories())
              factory.closeUncreated();
          instance.getStateMachine().finished();
          if (verified) {
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
            while ((planner.getFreeMemoryForOperators() != freeBefore
                    || flag && instance.getAdmissionMemoryAccount().snapshot().getReleases() != 1)
                && System.nanoTime() < deadline) Thread.sleep(1);
            assertEquals(freeBefore, planner.getFreeMemoryForOperators());
            if (flag)
              assertEquals(1, instance.getAdmissionMemoryAccount().snapshot().getReleases());
          }
        }
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setDegreeOfParallelism(originalDop);
      IoTDBDescriptor.getInstance().getConfig().setDataNodeId(originalNodeId);
      IoTDBDescriptor.getInstance()
          .getMemoryConfig()
          .setEnableQueryMemoryEstimation(originalMemory);
    }
  }

  private void close(Operator root, LocalExecutionPlanContext context) throws Exception {
    try {
      root.close();
    } finally {
      for (PipelineDriverFactory factory : context.getPipelineDriverFactories())
        factory.closeUncreated();
    }
  }
}
