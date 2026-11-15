/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.iotdb.calc.execution.operator.process.TableMergeSortOperator;
import org.apache.iotdb.calc.execution.operator.process.TableSortOperator;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.operator.process.RuntimeCompactOutputOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableReadyCollectOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.spill.DeviceEntryDataSetHandle;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.utils.Binary;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.DEVICE_NUMBER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Tests for splitting one {@link DeviceTableScanNode} into multiple parallel scan pipelines
 * (grouped by deviceEntries) in {@link DataNodeTableOperatorGenerator#visitDeviceTableScan}.
 */
public class DeviceTableScanParallelPipelineTest {

  static ExecutorService instanceNotificationExecutor;

  private final DataNodeTableOperatorGenerator generator =
      new DataNodeTableOperatorGenerator(new TableMetadataImpl());

  @BeforeClass
  public static void setUp() {
    instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-parallel-scan-notification");
  }

  @AfterClass
  public static void tearDown() {
    instanceNotificationExecutor.shutdown();
  }

  /**
   * dop = 4, 8 devices, no ordering requirement (allowParallelScan == true). Expected result is 4
   * sub scan pipelines, each of which scans 2 devices, and the root operator of current pipeline is
   * a TableReadyCollectOperator merging 4 ExchangeOperators.
   */
  @Test
  public void testParallelScanWithDop4And8Devices() throws Exception {
    DeviceTableScanNode node = initDeviceTableScanNode(8);
    node.setAllowParallelScan(true);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext("parallel_scan_test_1");
    context.setDegreeOfParallelism(4);

    Operator root = node.accept(generator, context);
    try {
      assertEquals(TableReadyCollectOperator.class, root.getClass());
      List<Operator> children = ((TableReadyCollectOperator) root).getChildren();
      assertEquals(4, children.size());
      for (Operator child : children) {
        assertEquals(ExchangeOperator.class, child.getClass());
      }

      // 4 sub pipelines are created, each of them holds a TableScanOperator scanning 2 devices
      assertEquals(4, context.getPipelineNumber());
      int totalDeviceNum = 0;
      for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
        assertEquals(TableScanOperator.class, driverFactory.getOperation().getClass());
        DataDriverContext dataDriverContext = (DataDriverContext) driverFactory.getDriverContext();
        assertTrue(dataDriverContext.isInputDriver());
        assertEquals(1, dataDriverContext.getSourceOperators().size());
        assertEquals(2, deviceNumberOf(driverFactory.getOperation()));
        totalDeviceNum += deviceNumberOf(driverFactory.getOperation());
      }
      assertEquals(8, totalDeviceNum);

      // the source operators are moved into sub driver contexts, the root driver context only holds
      // the TableReadyCollectOperator
      DataDriverContext rootDriverContext = (DataDriverContext) context.getDriverContext();
      assertEquals(0, rootDriverContext.getSourceOperators().size());

      // Source and sink aliases share one physical queue for each sub pipeline.
      assertEquals(4, context.getExchangeSumNum());
      assertEquals(4, context.getResourceRegistry().getObjectCount());
      assertTrue(
          context.getResourceRegistry().snapshotEntries().stream()
              .allMatch(entry -> "LOCAL_QUEUE".equals(entry.kind())));
    } finally {
      closeQuietly(root);
      for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
        closeQuietly(driverFactory.getOperation());
      }
    }
  }

  /**
   * dop = 8, but only 3 devices. Expected result is that the number of sub pipelines is clamped to
   * min(dop, deviceCount) = 3, each of which scans exactly 1 device.
   */
  @Test
  public void testParallelScanPipelineNumClampedByDeviceCount() throws Exception {
    DeviceTableScanNode node = initDeviceTableScanNode(3);
    node.setAllowParallelScan(true);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext("parallel_scan_test_2");
    context.setDegreeOfParallelism(8);

    Operator root = node.accept(generator, context);
    try {
      assertEquals(TableReadyCollectOperator.class, root.getClass());
      assertEquals(3, ((TableReadyCollectOperator) root).getChildren().size());
      assertEquals(3, context.getPipelineNumber());
      for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
        assertEquals(TableScanOperator.class, driverFactory.getOperation().getClass());
        assertEquals(1, deviceNumberOf(driverFactory.getOperation()));
      }
      assertEquals(3, context.getExchangeSumNum());
    } finally {
      closeQuietly(root);
      for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
        closeQuietly(driverFactory.getOperation());
      }
    }
  }

  /**
   * The scan is not marked with allowParallelScan (e.g. the parent requires ordering and merges
   * children via MergeSort). Expected result is that no split happens even if dop > 1, and the scan
   * keeps running in a single driver.
   */
  @Test
  public void testNoParallelScanWhenOrderingRequired() throws Exception {
    DeviceTableScanNode node = initDeviceTableScanNode(8);
    // allowParallelScan is false by default
    LocalExecutionPlanContext context = createLocalExecutionPlanContext("parallel_scan_test_3");
    context.setDegreeOfParallelism(4);

    Operator root = node.accept(generator, context);
    try {
      assertEquals(TableScanOperator.class, root.getClass());
      // no extra pipeline is created
      assertEquals(0, context.getPipelineNumber());
      assertEquals(0, context.getExchangeSumNum());
      DataDriverContext rootDriverContext = (DataDriverContext) context.getDriverContext();
      assertTrue(rootDriverContext.isInputDriver());
      assertEquals(1, rootDriverContext.getSourceOperators().size());
      assertEquals(8, deviceNumberOf(root));
    } finally {
      closeQuietly(root);
    }
  }

  /**
   * A global limit (shared by all devices, pushLimitToEachDevice == false) is pushed down into the
   * scan. Expected result is that no split happens, otherwise each split scan would apply the
   * global limit independently and produce wrong result.
   */
  @Test
  public void testNoParallelScanWhenGlobalLimitPushDown() throws Exception {
    DeviceTableScanNode node = initDeviceTableScanNode(8);
    node.setAllowParallelScan(true);
    node.setPushDownLimit(10);
    node.setPushLimitToEachDevice(false);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext("parallel_scan_test_4");
    context.setDegreeOfParallelism(4);

    Operator root = node.accept(generator, context);
    try {
      assertEquals(TableScanOperator.class, root.getClass());
      assertEquals(0, context.getPipelineNumber());
      assertEquals(0, context.getExchangeSumNum());
      assertEquals(8, deviceNumberOf(root));
    } finally {
      closeQuietly(root);
    }
  }

  /** dop = 1. Expected result is that no split happens. */
  @Test
  public void testNoParallelScanWhenDopIsOne() throws Exception {
    DeviceTableScanNode node = initDeviceTableScanNode(8);
    node.setAllowParallelScan(true);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext("parallel_scan_test_5");
    context.setDegreeOfParallelism(1);

    Operator root = node.accept(generator, context);
    try {
      assertEquals(TableScanOperator.class, root.getClass());
      assertEquals(0, context.getPipelineNumber());
    } finally {
      closeQuietly(root);
    }
  }

  /**
   * The device entries have been spilled to disk (represented by a shared DeviceEntryDataSetHandle
   * with an empty in-memory deviceEntries list). Expected result is that no split happens even
   * though dop > 1 and allowParallelScan == true, otherwise the parallel sub scans would share one
   * stateful segment source and read/delete each other's segments, producing wrong results.
   */
  @Test
  public void testNoParallelScanWhenDeviceEntriesSpilled() throws Exception {
    DeviceTableScanNode node = initDeviceTableScanNode(8);
    node.setAllowParallelScan(true);
    // switch the node to the spilled representation: an on-disk handle instead of inline entries
    node.setDeviceEntryDataSetHandle(
        new DeviceEntryDataSetHandle(
            "parallel_scan_test_6",
            node.getPlanNodeId(),
            org.apache.iotdb.db.queryengine.common.DataNodeEndPoints.LOCAL_HOST_DATA_BLOCK_ENDPOINT,
            /* segmentCount= */ 4,
            /* entryCount= */ 8,
            /* ordered= */ false));
    LocalExecutionPlanContext context = createLocalExecutionPlanContext("parallel_scan_test_6");
    context.setDegreeOfParallelism(4);

    Operator root = node.accept(generator, context);
    try {
      assertEquals(TableScanOperator.class, root.getClass());
      assertEquals(0, context.getPipelineNumber());
      assertEquals(0, context.getExchangeSumNum());
    } finally {
      closeQuietly(root);
    }
  }

  /**
   * ORDER BY time is not safe with a TableReadyCollectOperator: every table scan emits one device
   * at a time. With the experiment enabled, each sub driver therefore merges its single-device
   * scans first, and the root merges those already sorted streams. This asserts the operator-tree
   * shape that protects that invariant rather than merely counting drivers.
   */
  /** Unordered time-partition morsels use the same local pipeline builder as device splits. */
  @Test
  public void testTimePartitionParallelScanUsesCommonBuilder() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorselSizeWeighting(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(false);
    try {
      TsFileManager tsFileManager = Mockito.mock(TsFileManager.class);
      Mockito.when(tsFileManager.getTimePartitions())
          .thenReturn(new java.util.HashSet<>(Arrays.asList(0L, 1L)));
      DataRegion dataRegion = Mockito.mock(DataRegion.class);
      Mockito.when(dataRegion.getTsFileManager()).thenReturn(tsFileManager);

      DeviceTableScanNode node = initDeviceTableScanNode(4);
      node.setAllowParallelScan(true);
      LocalExecutionPlanContext context = createLocalExecutionPlanContext("time_morsel_builder");
      context.getDriverContext().getFragmentInstanceContext().setDataRegion(dataRegion);
      context.setDegreeOfParallelism(4);

      Operator root = node.accept(generator, context);
      try {
        assertEquals(TableReadyCollectOperator.class, root.getClass());
        assertEquals(4, ((TableReadyCollectOperator) root).getChildren().size());
        assertEquals(4, context.getPipelineNumber());
        assertEquals(4, context.getExchangeSumNum());
        assertEquals(4, context.getResourceRegistry().getObjectCount());
        assertTrue(
            context.getResourceRegistry().snapshotEntries().stream()
                .allMatch(entry -> "LOCAL_QUEUE".equals(entry.kind())));
        for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
          assertEquals(TableScanOperator.class, driverFactory.getOperation().getClass());
        }
      } finally {
        closeQuietly(root);
        for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
          closeQuietly(driverFactory.getOperation());
        }
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(false);
      IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorselSizeWeighting(true);
      IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(false);
    }
  }

  @Test
  public void testOrderedParallelScanBuildsTwoLevelMergeTree() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableOrderedParallelScan(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(false);
    DeviceTableScanNode scanNode = initDeviceTableScanNode(8);
    Symbol time = new Symbol("time");
    SortNode sortNode =
        new SortNode(
            new PlanNodeId("ordered-sort"),
            scanNode,
            new OrderingScheme(
                Collections.singletonList(time),
                Collections.singletonMap(time, SortOrder.ASC_NULLS_FIRST)),
            false,
            false);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext("ordered_parallel_scan");
    context.setDegreeOfParallelism(4);

    Operator root = sortNode.accept(generator, context);
    try {
      assertEquals(TableMergeSortOperator.class, root.getClass());
      assertEquals("4", root.getOperatorContext().getSpecifiedInfo().get("Merge sort branches"));
      assertEquals(4, context.getPipelineNumber());
      assertEquals(4, context.getExchangeSumNum());
      for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
        assertEquals(TableMergeSortOperator.class, driverFactory.getOperation().getClass());
        DataDriverContext dataDriverContext = (DataDriverContext) driverFactory.getDriverContext();
        assertEquals(2, dataDriverContext.getSourceOperators().size());
        assertEquals(
            "2",
            driverFactory
                .getOperation()
                .getOperatorContext()
                .getSpecifiedInfo()
                .get("Merge sort branches"));
      }
    } finally {
      closeQuietly(root);
      for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
        closeQuietly(driverFactory.getOperation());
      }
      IoTDBDescriptor.getInstance().getConfig().setEnableOrderedParallelScan(false);
    }
  }

  /** Device (TAG) then time has the same per-device source invariant as ORDER BY time. */
  @Test
  public void testOrderedParallelScanSupportsDeviceThenTime() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableOrderedParallelScan(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(false);
    DeviceTableScanNode scanNode = initDeviceTableScanNode(8);
    Symbol tag = new Symbol("tag1");
    Symbol time = new Symbol("time");
    Map<Symbol, SortOrder> ordering = new HashMap<>();
    ordering.put(tag, SortOrder.ASC_NULLS_FIRST);
    ordering.put(time, SortOrder.ASC_NULLS_FIRST);
    StreamSortNode sortNode =
        new StreamSortNode(
            new PlanNodeId("device-time-sort"),
            scanNode,
            new OrderingScheme(Arrays.asList(tag, time), ordering),
            false,
            true,
            1);
    LocalExecutionPlanContext context =
        createLocalExecutionPlanContext("device_time_ordered_parallel_scan");
    context.setDegreeOfParallelism(4);

    Operator root = sortNode.accept(generator, context);
    try {
      assertEquals(TableMergeSortOperator.class, root.getClass());
      assertEquals("4", root.getOperatorContext().getSpecifiedInfo().get("Merge sort branches"));
      assertEquals(4, context.getPipelineNumber());
      assertEquals(4, context.getExchangeSumNum());
    } finally {
      closeQuietly(root);
      for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
        closeQuietly(driverFactory.getOperation());
      }
      IoTDBDescriptor.getInstance().getConfig().setEnableOrderedParallelScan(false);
    }
  }

  /** A field key has no per-device ordering guarantee and must retain the ordinary sort path. */
  @Test
  public void testOrderedParallelScanRejectsFieldOrdering() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableOrderedParallelScan(true);
    DeviceTableScanNode scanNode = initDeviceTableScanNode(8);
    Symbol field = new Symbol("s1");
    SortNode sortNode =
        new SortNode(
            new PlanNodeId("field-sort"),
            scanNode,
            new OrderingScheme(
                Collections.singletonList(field),
                Collections.singletonMap(field, SortOrder.ASC_NULLS_FIRST)),
            false,
            false);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext("field_ordered_scan");
    context.setDegreeOfParallelism(4);

    Operator root = sortNode.accept(generator, context);
    try {
      assertEquals(TableSortOperator.class, root.getClass());
      assertEquals(0, context.getPipelineNumber());
      assertEquals(0, context.getExchangeSumNum());
    } finally {
      closeQuietly(root);
      IoTDBDescriptor.getInstance().getConfig().setEnableOrderedParallelScan(false);
    }
  }

  LocalExecutionPlanContext createLocalExecutionPlanContext(String queryId) {
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(new QueryId(queryId), 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    DataRegion dataRegion = Mockito.mock(DataRegion.class);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    fragmentInstanceContext.setDataRegion(dataRegion);

    TypeProvider typeProvider = new TypeProvider();
    typeProvider.putTableModelType(new Symbol("time"), TypeFactory.getType(TSDataType.TIMESTAMP));
    typeProvider.putTableModelType(new Symbol("tag1"), TypeFactory.getType(TSDataType.STRING));
    typeProvider.putTableModelType(new Symbol("s1"), TypeFactory.getType(TSDataType.INT32));
    return new LocalExecutionPlanContext(
        typeProvider, fragmentInstanceContext, new DataNodeQueryContext(1));
  }

  DeviceTableScanNode initDeviceTableScanNode(int deviceNum) {
    Symbol time = new Symbol("time");
    Symbol tag1 = new Symbol("tag1");
    Symbol s1 = new Symbol("s1");

    Map<Symbol, ColumnSchema> assignments = new HashMap<>();
    assignments.put(
        time,
        new ColumnSchema(
            "time", TypeFactory.getType(TSDataType.TIMESTAMP), false, TsTableColumnCategory.TIME));
    assignments.put(
        tag1,
        new ColumnSchema(
            "tag1", TypeFactory.getType(TSDataType.STRING), false, TsTableColumnCategory.TAG));
    assignments.put(
        s1,
        new ColumnSchema(
            "s1", TypeFactory.getType(TSDataType.INT32), false, TsTableColumnCategory.FIELD));

    List<DeviceEntry> deviceEntries = new ArrayList<>();
    for (int i = 0; i < deviceNum; i++) {
      deviceEntries.add(
          new AlignedDeviceEntry(
              IDeviceID.Factory.DEFAULT_FACTORY.create("table1.d" + i), new Binary[0]));
    }

    Map<Symbol, Integer> tagAndAttributeIndexMap = new HashMap<>();
    tagAndAttributeIndexMap.put(tag1, 0);

    return new DeviceTableScanNode(
        new PlanNodeId("DeviceTableScanNode"),
        new QualifiedObjectName("testdb", "table1"),
        Arrays.asList(time, tag1, s1),
        assignments,
        deviceEntries,
        tagAndAttributeIndexMap,
        Ordering.ASC,
        null,
        null,
        0,
        0,
        false,
        false);
  }

  /**
   * enable_dop_estimation = false (default). The split count must still be min(dop, deviceCount)
   * regardless of what the data region says, so the flag acts as a gate and nothing else changes.
   */
  @Test
  public void testDopEstimationDisabledFallsBackToMinDopDeviceCount() throws Exception {
    // Arrange: flag off (default), 8 devices, dop 4.
    IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(false);
    DeviceTableScanNode node = initDeviceTableScanNode(8);
    node.setAllowParallelScan(true);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext("dop_est_disabled");
    context.setDegreeOfParallelism(4);

    Operator root = node.accept(generator, context);
    try {
      // With the flag off the split is exactly min(4, 8) = 4 no matter what the region holds.
      assertEquals(TableReadyCollectOperator.class, root.getClass());
      assertEquals(4, ((TableReadyCollectOperator) root).getChildren().size());
      assertEquals(4, context.getPipelineNumber());
    } finally {
      closeQuietly(root);
      for (PipelineDriverFactory df : context.getPipelineDriverFactories()) {
        closeQuietly(df.getOperation());
      }
      IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(false);
    }
  }

  /**
   * enable_dop_estimation = true. When the region holds enough data to justify more drivers than
   * the estimation suggests, the result is clamped down to the estimated value.
   *
   * <p>Setup: 8 devices, dop 8. Two time partitions, four TsFiles each 64 MiB. Total = 256 MiB.
   * TARGET_BYTES_PER_SCAN_DRIVER = 128 MiB → estimated = ceil(256 / 128) = 2. The final pipeline
   * count is min(min(dop=8, deviceCount=8), estimated=2) = 2.
   */
  @Test
  public void testDopEstimationClampsToDataSizeEstimate() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(true);
    try {
      // Build a mock TsFileManager that reports 2 time partitions and 4 × 64-MiB seq files.
      TsFileManager tsFileManager = Mockito.mock(TsFileManager.class);
      Set<Long> twoPartitions = new java.util.HashSet<>(Arrays.asList(0L, 1L));
      Mockito.when(tsFileManager.getTimePartitions()).thenReturn(twoPartitions);

      final long FILE_SIZE = 64L * 1024 * 1024; // 64 MiB each
      List<TsFileResource> seqFiles = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        TsFileResource r = Mockito.mock(TsFileResource.class);
        Mockito.when(r.getTsFileSize()).thenReturn(FILE_SIZE);
        seqFiles.add(r);
      }
      Mockito.when(tsFileManager.getAllTsFileListForQuery(Mockito.isNull(), Mockito.isNull()))
          .thenReturn(new org.apache.tsfile.utils.Pair<>(seqFiles, new ArrayList<>()));

      DataRegion dataRegion = Mockito.mock(DataRegion.class);
      Mockito.when(dataRegion.getTsFileManager()).thenReturn(tsFileManager);

      // Build context, injecting the mock DataRegion into the FI context.
      FragmentInstanceId instanceId =
          new FragmentInstanceId(
              new PlanFragmentId(new QueryId("dop_est_enabled"), 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      fragmentInstanceContext.setDataRegion(dataRegion);
      LocalExecutionPlanContext context =
          new LocalExecutionPlanContext(
              new TypeProvider(), fragmentInstanceContext, new DataNodeQueryContext(1));
      context.setDegreeOfParallelism(8);

      DeviceTableScanNode node = initDeviceTableScanNode(8);
      node.setAllowParallelScan(true);

      // 256 MiB total / 128 MiB target = 2 drivers; min(min(8,8), 2) = 2
      Operator root = node.accept(generator, context);
      try {
        assertEquals(TableReadyCollectOperator.class, root.getClass());
        assertEquals(2, ((TableReadyCollectOperator) root).getChildren().size());
        assertEquals(2, context.getPipelineNumber());
      } finally {
        closeQuietly(root);
        for (PipelineDriverFactory df : context.getPipelineDriverFactories()) {
          closeQuietly(df.getOperation());
        }
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(false);
    }
  }

  /**
   * The number of devices actually assigned to a scan operator, read from the operator statistics
   * ({@code DEVICE_NUMBER}). Since upstream introduced the spillable batch device entry source, the
   * device paths are no longer eagerly materialized into the driver context, so we rely on this
   * recorded statistic instead of {@code DataDriverContext#getPaths}.
   */
  private int deviceNumberOf(Operator operator) {
    return Integer.parseInt(
        (String) operator.getOperatorContext().getSpecifiedInfo().get(DEVICE_NUMBER));
  }

  private void closeQuietly(Operator operator) {
    try {
      operator.close();
    } catch (Exception ignored) {
      // ignore the exception during close in test
    }
  }

  @Test
  public void testRuntimePaidDeviceScanDoesNotRequireGroupByApproval() throws Exception {
    try (RuntimeGeneratorOptions ignored = new RuntimeGeneratorOptions();
        RuntimeGeneratorFixture fixture =
            new RuntimeGeneratorFixture("runtime_native_device", "DeviceTableScanNode", false)) {
      DeviceTableScanNode node = initDeviceTableScanNode(8);
      node.setAllowParallelScan(true);
      assertFalse(fixture.context.isApprovedRuntimeScan(node.getPlanNodeId().getId()));

      Operator root = node.accept(generator, fixture.context);
      fixture.freeze(root);
      assertEquals(TableReadyCollectOperator.class, root.getClass());
      RuntimeScanGroupDescriptor group = assertRuntimeDescriptor(fixture, node, 4);
      assertEquals(ScanParallelPlan.Mode.DEVICE, group.getLayout().getLanePlan().getMode());
      assertEquals(4, group.getLayout().getInitialWorkers());
      Set<String> actualDevices = new HashSet<>();
      int deviceOccurrences = 0;
      for (RuntimeScanGroupDescriptor.Lane lane : group.getLanes()) {
        assertEquals(
            RuntimeCompactOutputOperator.class, lane.getFactory().getOperation().getClass());
        assertEquals(
            TableScanOperator.class, runtimeDelegate(lane.getFactory().getOperation()).getClass());
        for (Operator scan : runtimeLaneSources(lane)) {
          for (DeviceEntry entry : runtimeScanDevices(scan)) {
            assertTrue(
                "Duplicate device in actual scans",
                actualDevices.add(entry.getDeviceID().toString()));
            deviceOccurrences++;
          }
        }
      }
      assertEquals(runtimeDeviceIds(node), actualDevices);
      assertEquals(8, deviceOccurrences);
      assertTrue(node.isAllowParallelScan());
    }
  }

  @Test
  public void testRuntimeApprovedGroupByRawScanKeepsOriginalAdmissionFlag() throws Exception {
    try (RuntimeGeneratorOptions ignored = new RuntimeGeneratorOptions();
        RuntimeGeneratorFixture fixture =
            new RuntimeGeneratorFixture("runtime_group_by_raw", "DeviceTableScanNode", true)) {
      DeviceTableScanNode node = initDeviceTableScanNode(8);
      assertFalse(node.isAllowParallelScan());
      assertTrue(fixture.context.isApprovedRuntimeScan(node.getPlanNodeId().getId()));

      Operator root = node.accept(generator, fixture.context);
      fixture.freeze(root);
      assertEquals(TableReadyCollectOperator.class, root.getClass());
      RuntimeScanGroupDescriptor group = assertRuntimeDescriptor(fixture, node, 4);
      assertEquals(ScanParallelPlan.Mode.DEVICE, group.getLayout().getLanePlan().getMode());
      assertFalse(
          "Scoped runtime approval must not mutate the serialized scan flag",
          node.isAllowParallelScan());
      assertEquals(8, group.getLayout().getLanePlan().getWorkUnits().size());
    }
  }

  @Test
  public void testRuntimeEstimatedDopOneKeepsFourLogicalLanes() throws Exception {
    try (RuntimeGeneratorOptions ignored = new RuntimeGeneratorOptions();
        RuntimeGeneratorFixture fixture =
            new RuntimeGeneratorFixture("runtime_estimate_one", "DeviceTableScanNode", false)) {
      IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(true);
      TsFileManager files = Mockito.mock(TsFileManager.class);
      Mockito.when(files.getTimePartitions()).thenReturn(Collections.singleton(0L));
      TsFileResource smallFile = Mockito.mock(TsFileResource.class);
      Mockito.when(smallFile.getTsFileSize()).thenReturn(1024L * 1024);
      Mockito.when(files.getAllTsFileListForQuery(Mockito.isNull(), Mockito.isNull()))
          .thenReturn(
              new org.apache.tsfile.utils.Pair<>(
                  Collections.singletonList(smallFile), Collections.emptyList()));
      DataRegion region = Mockito.mock(DataRegion.class);
      Mockito.when(region.getTsFileManager()).thenReturn(files);
      fixture.context.getInstanceContext().setDataRegion(region);
      DeviceTableScanNode node = initDeviceTableScanNode(8);
      node.setAllowParallelScan(true);

      Operator root = node.accept(generator, fixture.context);
      fixture.freeze(root);
      RuntimeScanGroupDescriptor group = assertRuntimeDescriptor(fixture, node, 4);
      assertEquals(8, group.getLayout().getLanePlan().getInputDop());
      assertEquals(1, group.getLayout().getInitialWorkers());
      assertEquals(5, fixture.context.getPipelineNumber());
      assertEquals(4, ((TableReadyCollectOperator) root).getChildren().size());
    }
  }

  @Test
  public void testRuntimeWeightedTimePartitionBucketsExecuteEveryActualWorkUnit() throws Exception {
    try (RuntimeGeneratorOptions ignored = new RuntimeGeneratorOptions();
        RuntimeGeneratorFixture fixture =
            new RuntimeGeneratorFixture("runtime_weighted_tp", "DeviceTableScanNode", false)) {
      IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(true);
      IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorselSizeWeighting(true);
      List<Long> partitions = Arrays.asList(0L, 1L, 2L, 3L);
      TsFileManager files = Mockito.mock(TsFileManager.class);
      Mockito.when(files.getTimePartitions()).thenReturn(new HashSet<>(partitions));
      List<TsFileResource> allFiles = new ArrayList<>();
      long[] sizes = {900L, 100L, 10L, 1L};
      for (int index = 0; index < partitions.size(); index++) {
        TsFileResource file = Mockito.mock(TsFileResource.class);
        Mockito.when(file.getTsFileSize()).thenReturn(sizes[index]);
        allFiles.add(file);
        Mockito.when(files.getTsFileListSnapshot(partitions.get(index)))
            .thenReturn(
                new org.apache.tsfile.utils.Pair<>(
                    Collections.singletonList(file), Collections.emptyList()));
      }
      Mockito.when(files.getAllTsFileListForQuery(Mockito.isNull(), Mockito.isNull()))
          .thenReturn(new org.apache.tsfile.utils.Pair<>(allFiles, Collections.emptyList()));
      DataRegion region = Mockito.mock(DataRegion.class);
      Mockito.when(region.getTsFileManager()).thenReturn(files);
      fixture.context.getInstanceContext().setDataRegion(region);
      DeviceTableScanNode node = initDeviceTableScanNode(8);
      node.setAllowParallelScan(true);

      Operator root = node.accept(generator, fixture.context);
      fixture.freeze(root);
      RuntimeScanGroupDescriptor group = assertRuntimeDescriptor(fixture, node, 4);
      assertEquals(ScanParallelPlan.Mode.TIME_PARTITION, group.getLayout().getLanePlan().getMode());
      assertEquals(8, group.getLayout().getLanePlan().getWorkUnits().size());
      assertTrue(
          group.getLayout().getLanePlan().getBuckets().stream()
              .allMatch(bucket -> bucket.getWorkUnits().size() > 1));

      Map<String, Integer> actualCoverage = new HashMap<>();
      int actualSources = 0;
      long interval = TimePartitionUtils.getTimePartitionInterval();
      for (RuntimeScanGroupDescriptor.Lane lane : group.getLanes()) {
        Operator laneRoot = runtimeDelegate(lane.getFactory().getOperation());
        assertEquals(TableReadyCollectOperator.class, laneRoot.getClass());
        assertEquals(
            group
                .getLayout()
                .getLanePlan()
                .getBuckets()
                .get(lane.getLaneIndex())
                .getWorkUnits()
                .size(),
            runtimeLaneSources(lane).size());
        for (Operator scan : runtimeLaneSources(lane)) {
          actualSources++;
          assertEquals(
              "LPT_TSFILE_BYTES",
              scan.getOperatorContext().getSpecifiedInfo().get("MORSEL_SCHEDULING"));
          SeriesScanOptions options = (SeriesScanOptions) runtimeField(scan, "seriesScanOptions");
          Set<Long> actualPartitions = new HashSet<>();
          for (Long partition : partitions) {
            if (options.getGlobalTimeFilter().satisfy(partition * interval + interval / 2, null)) {
              actualPartitions.add(partition);
            }
          }
          assertFalse(actualPartitions.isEmpty());
          assertFalse(options.getGlobalTimeFilter().satisfy(-interval / 2, null));
          assertFalse(options.getGlobalTimeFilter().satisfy(4 * interval + interval / 2, null));
          for (DeviceEntry entry : runtimeScanDevices(scan)) {
            for (Long partition : actualPartitions) {
              actualCoverage.merge(entry.getDeviceID() + "@" + partition, 1, Integer::sum);
            }
          }
        }
      }
      Map<String, Integer> expectedCoverage = new HashMap<>();
      for (String device : runtimeDeviceIds(node)) {
        for (Long partition : partitions) {
          expectedCoverage.put(device + "@" + partition, 1);
        }
      }
      assertEquals(expectedCoverage, actualCoverage);
      assertEquals(8, actualSources);
    }
  }

  @Test
  public void testRuntimeOrderedScanCompactsRootLanesAndEveryDeviceHead() throws Exception {
    try (RuntimeGeneratorOptions ignored = new RuntimeGeneratorOptions();
        RuntimeGeneratorFixture fixture =
            new RuntimeGeneratorFixture("runtime_ordered_heads", "DeviceTableScanNode", false)) {
      IoTDBDescriptor.getInstance().getConfig().setEnableOrderedParallelScan(true);
      DeviceTableScanNode node = initDeviceTableScanNode(8);
      Symbol time = new Symbol("time");
      SortNode sort =
          new SortNode(
              new PlanNodeId("runtime-order"),
              node,
              new OrderingScheme(
                  Collections.singletonList(time),
                  Collections.singletonMap(time, SortOrder.ASC_NULLS_FIRST)),
              false,
              false);

      Operator root = sort.accept(generator, fixture.context);
      fixture.freeze(root);
      assertEquals(RuntimeCompactOutputOperator.class, root.getClass());
      assertEquals(4096L, root.calculateMaxReturnSize());
      Operator rootMerge = runtimeDelegate(root);
      assertEquals(TableMergeSortOperator.class, rootMerge.getClass());
      RuntimeScanGroupDescriptor group = assertRuntimeDescriptor(fixture, node, 4);
      assertEquals(ScanParallelPlan.Mode.ORDERED, group.getLayout().getLanePlan().getMode());
      List<Operator> rootInputs = runtimeChildren(rootMerge);
      assertEquals(4, rootInputs.size());
      Set<String> actualDevices = new HashSet<>();
      int compactDeviceHeads = 0;
      for (RuntimeScanGroupDescriptor.Lane lane : group.getLanes()) {
        assertSame(lane.getOwnExchange(), rootInputs.get(lane.getLaneIndex()));
        Operator compactLane = lane.getFactory().getOperation();
        assertEquals(RuntimeCompactOutputOperator.class, compactLane.getClass());
        assertEquals(4096L, compactLane.calculateMaxReturnSize());
        Operator laneMerge = runtimeDelegate(compactLane);
        assertEquals(TableMergeSortOperator.class, laneMerge.getClass());
        List<Operator> deviceHeads = runtimeChildren(laneMerge);
        assertEquals(2, deviceHeads.size());
        for (Operator head : deviceHeads) {
          assertEquals(RuntimeCompactOutputOperator.class, head.getClass());
          assertEquals(4096L, head.calculateMaxReturnSize());
          Operator scan = runtimeDelegate(head);
          assertEquals(TableScanOperator.class, scan.getClass());
          assertTrue(runtimeLaneSources(lane).contains(scan));
          List<DeviceEntry> devices = runtimeScanDevices(scan);
          assertEquals(1, devices.size());
          assertTrue(actualDevices.add(devices.get(0).getDeviceID().toString()));
          compactDeviceHeads++;
        }
      }
      assertEquals(runtimeDeviceIds(node), actualDevices);
      assertEquals(8, compactDeviceHeads);
      assertFalse(node.isAllowParallelScan());
    }
  }

  @Test
  public void testRuntimePaidAndApprovedScanStillRejectsGlobalLimit() throws Exception {
    try (RuntimeGeneratorOptions ignored = new RuntimeGeneratorOptions();
        RuntimeGeneratorFixture fixture =
            new RuntimeGeneratorFixture("runtime_global_limit", "DeviceTableScanNode", true)) {
      DeviceTableScanNode node = initDeviceTableScanNode(8);
      node.setAllowParallelScan(true);
      node.setPushDownLimit(10);
      node.setPushLimitToEachDevice(false);

      Operator root = node.accept(generator, fixture.context);
      fixture.freeze(root);
      assertEquals(TableScanOperator.class, root.getClass());
      assertEquals(1, fixture.context.getPipelineNumber());
      assertTrue(fixture.context.getRuntimeGroupDescriptors().isEmpty());
      assertTrue(fixture.context.getOwnedRuntimeFrontiers().isEmpty());
      assertEquals(10L, node.getPushDownLimit());
      assertFalse(node.isPushLimitToEachDevice());
    }
  }

  @Test
  public void testRuntimeBudgetDoesNotConvertUnpaidSourceFromNativeStaticPath() throws Exception {
    try (RuntimeGeneratorOptions ignored = new RuntimeGeneratorOptions();
        RuntimeGeneratorFixture fixture =
            new RuntimeGeneratorFixture("runtime_unpaid_source", "some-other-source", false)) {
      DeviceTableScanNode node = initDeviceTableScanNode(8);
      node.setAllowParallelScan(true);

      Operator root = node.accept(generator, fixture.context);
      fixture.freeze(root);
      assertEquals(TableReadyCollectOperator.class, root.getClass());
      assertEquals(8, ((TableReadyCollectOperator) root).getChildren().size());
      assertEquals(9, fixture.context.getPipelineNumber());
      assertTrue(fixture.context.getRuntimeGroupDescriptors().isEmpty());
      assertTrue(fixture.context.getOwnedRuntimeFrontiers().isEmpty());
      for (Operator child : ((TableReadyCollectOperator) root).getChildren()) {
        ExchangeOperator exchange = (ExchangeOperator) child;
        SharedTsBlockQueue queue =
            ((LocalSourceHandle) exchange.getSourceHandle()).getSharedTsBlockQueue();
        assertFalse(queue.isFrontierMode());
      }
      for (int index = 0; index < fixture.context.getPipelineNumber() - 1; index++) {
        assertEquals(
            TableScanOperator.class,
            fixture.context.getPipelineDriverFactories().get(index).getOperation().getClass());
      }
    }
  }

  private RuntimeScanGroupDescriptor assertRuntimeDescriptor(
      RuntimeGeneratorFixture fixture, DeviceTableScanNode node, int expectedLanes) {
    assertEquals(1, fixture.context.getRuntimeGroupDescriptors().size());
    RuntimeScanGroupDescriptor group = fixture.context.getRuntimeGroupDescriptors().get(0);
    assertEquals(node.getPlanNodeId().getId(), group.getSourcePlanNodeId());
    assertEquals(expectedLanes, group.getLayout().getLaneCount());
    assertEquals(expectedLanes, group.getLanes().size());
    assertEquals(expectedLanes * 4096L, group.getReservation().getTotalReservedBytes());
    assertEquals(0, group.getReservation().getLeasedCreditCount());
    Set<SharedTsBlockQueue.MemoryReservationKey> identities = new HashSet<>();
    for (RuntimeScanGroupDescriptor.Lane lane : group.getLanes()) {
      SharedTsBlockQueue queue = lane.getSink().getSharedTsBlockQueue();
      assertTrue(queue.isFrontierMode());
      assertEquals(lane.getLaneIndex(), queue.getFrontierLaneIndex());
      assertEquals(group.getReservation().getMemoryReservationKey(), queue.getActualAccountKey());
      assertTrue(identities.add(queue.getQueueIdentityKey()));
      assertFalse(queue.getQueueIdentityKey().equals(queue.getActualAccountKey()));
      assertSame(queue, ((LocalSourceHandle) lane.getSource()).getSharedTsBlockQueue());
      assertSame(lane.getSource(), lane.getOwnExchange().getSourceHandle());
      assertSame(
          lane.getFactory(),
          fixture.context.getPipelineDriverFactories().get(lane.getOriginalFactoryIndex()));
      assertEquals(
          lane.getFactory().getDriverContext().getPipelineId(), lane.getOriginalPipelineId());
      assertTrue(lane.getStartDependency().isDone());
      assertFalse(lane.getOwnConsumerClose().isDone());
    }
    SharedTsBlockQueue.MemoryReservationKey key = group.getReservation().getMemoryReservationKey();
    assertEquals(
        expectedLanes * 4096L,
        fixture.pool.getPlanNodeMemoryReservedBytes(
            key.queryId(), key.fragmentInstanceId(), key.planNodeId()));
    return group;
  }

  private static List<Operator> runtimeLaneSources(RuntimeScanGroupDescriptor.Lane lane) {
    return new ArrayList<>(
        ((DataDriverContext) lane.getFactory().getDriverContext()).getSourceOperators());
  }

  private static Operator runtimeDelegate(Operator compact) throws Exception {
    assertEquals(RuntimeCompactOutputOperator.class, compact.getClass());
    return (Operator) runtimeField(compact, "delegate");
  }

  @SuppressWarnings("unchecked")
  private static List<Operator> runtimeChildren(Operator parent) throws Exception {
    return (List<Operator>) runtimeField(parent, "children");
  }

  @SuppressWarnings("unchecked")
  private static List<DeviceEntry> runtimeScanDevices(Operator scan) throws Exception {
    Object source = runtimeField(scan, "deviceEntrySource");
    assertEquals(
        org.apache.iotdb.db.queryengine.plan.relational.metadata.spill.InMemoryDeviceEntrySource
            .class,
        source.getClass());
    return new ArrayList<>((List<DeviceEntry>) runtimeField(source, "entries"));
  }

  private static Object runtimeField(Object instance, String name) throws Exception {
    for (Class<?> type = instance.getClass(); type != null; type = type.getSuperclass()) {
      try {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(instance);
      } catch (NoSuchFieldException ignored) {
        // Operator state is declared on the corresponding source or consume-all superclass.
      }
    }
    throw new NoSuchFieldException(name);
  }

  private static Set<String> runtimeDeviceIds(DeviceTableScanNode node) {
    Set<String> devices = new HashSet<>();
    for (DeviceEntry entry : node.getDeviceEntries()) {
      devices.add(entry.getDeviceID().toString());
    }
    return devices;
  }

  private final class RuntimeGeneratorFixture implements AutoCloseable {
    private final LocalExecutionPlanContext context;
    private final MemoryPool pool;
    private final long originalReservedBytes;

    private RuntimeGeneratorFixture(String queryId, String paidSource, boolean approve) {
      context = createLocalExecutionPlanContext(queryId);
      context.setDegreeOfParallelism(8);
      pool =
          MPPDataExchangeService.getInstance()
              .getMPPDataExchangeManager()
              .getLocalMemoryManager()
              .getQueryPool();
      originalReservedBytes = pool.getReservedBytes();
      long output = context.getMaxBytesOneHandleCanReserve();
      long prepaid = 4 * 4096L;
      assertTrue(output >= prepaid);
      context.initializeRuntimeExchangeBudget(
          pool,
          new LocalExecutionPlanContext.RuntimeExchangeBudget(
              0,
              output,
              output - prepaid,
              4096,
              Collections.singletonMap(paidSource, prepaid),
              approve ? Collections.singleton(paidSource) : Collections.emptySet(),
              1024L * 1024));
    }

    private void freeze(Operator root) {
      // Parent dependencies are final only after the original root factory has also been appended.
      context.addPipelineDriverFactory(root, context.getDriverContext(), 0);
      context.freezeRuntimeGroups(new ArrayList<>(context.getPipelineDriverFactories()));
    }

    @Override
    public void close() {
      Throwable cleanup = new IllegalStateException("Runtime generator test cleanup");
      for (PipelineDriverFactory factory : context.getPipelineDriverFactories()) {
        try {
          factory.closeUncreated();
        } catch (Throwable failure) {
          cleanup.addSuppressed(failure);
        }
      }
      try {
        context.getResourceRegistry().abortUntransferred(cleanup);
      } catch (Throwable failure) {
        cleanup.addSuppressed(failure);
      }
      for (RuntimeFrontierReservation reservation : context.getOwnedRuntimeFrontiers()) {
        try {
          assertTrue(reservation.requestClose().isDone());
          assertEquals(0, reservation.getLeasedCreditCount());
          assertEquals(0L, reservation.getInternalUsedBytes());
        } catch (Throwable failure) {
          cleanup.addSuppressed(failure);
        }
      }
      assertEquals(originalReservedBytes, pool.getReservedBytes());
      if (cleanup.getSuppressed().length != 0) {
        throw new AssertionError("Runtime generator cleanup failed", cleanup);
      }
    }
  }

  private static final class RuntimeGeneratorOptions implements AutoCloseable {
    private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    private final boolean timeMorsel = config.isEnableTimePartitionMorsel();
    private final boolean timeWeighting = config.isEnableTimePartitionMorselSizeWeighting();
    private final boolean estimation = config.isEnableDopEstimation();
    private final boolean ordered = config.isEnableOrderedParallelScan();
    private final boolean dynamic = config.isEnableDynamicScanParallelism();

    private RuntimeGeneratorOptions() {
      config.setEnableTimePartitionMorsel(false);
      config.setEnableTimePartitionMorselSizeWeighting(false);
      config.setEnableDopEstimation(false);
      config.setEnableOrderedParallelScan(false);
      config.setEnableDynamicScanParallelism(false);
    }

    @Override
    public void close() {
      config.setEnableTimePartitionMorsel(timeMorsel);
      config.setEnableTimePartitionMorselSizeWeighting(timeWeighting);
      config.setEnableDopEstimation(estimation);
      config.setEnableOrderedParallelScan(ordered);
      config.setEnableDynamicScanParallelism(dynamic);
    }
  }
}
