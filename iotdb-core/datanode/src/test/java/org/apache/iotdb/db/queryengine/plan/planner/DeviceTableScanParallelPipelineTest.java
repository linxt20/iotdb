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
import org.apache.iotdb.calc.execution.operator.process.CollectOperator;
import org.apache.iotdb.calc.execution.operator.process.TableMergeSortOperator;
import org.apache.iotdb.calc.execution.operator.process.TableSortOperator;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.DEVICE_NUMBER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for splitting one {@link DeviceTableScanNode} into multiple parallel scan pipelines
 * (grouped by deviceEntries) in {@link DataNodeTableOperatorGenerator#visitDeviceTableScan}.
 */
public class DeviceTableScanParallelPipelineTest {

  private static ExecutorService instanceNotificationExecutor;

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
   * a CollectOperator merging 4 ExchangeOperators.
   */
  @Test
  public void testParallelScanWithDop4And8Devices() throws Exception {
    DeviceTableScanNode node = initDeviceTableScanNode(8);
    node.setAllowParallelScan(true);
    LocalExecutionPlanContext context = createLocalExecutionPlanContext("parallel_scan_test_1");
    context.setDegreeOfParallelism(4);

    Operator root = node.accept(generator, context);
    try {
      assertEquals(CollectOperator.class, root.getClass());
      List<Operator> children = ((CollectOperator) root).getChildren();
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
      // the CollectOperator
      DataDriverContext rootDriverContext = (DataDriverContext) context.getDriverContext();
      assertEquals(0, rootDriverContext.getSourceOperators().size());

      // one local exchange pair for each sub pipeline
      assertEquals(4, context.getExchangeSumNum());
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
      assertEquals(CollectOperator.class, root.getClass());
      assertEquals(3, ((CollectOperator) root).getChildren().size());
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
            new TEndPoint("1.2.3.4", 9999),
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
   * ORDER BY time is not safe with a CollectOperator: every table scan emits one device at a time.
   * With the experiment enabled, each sub driver therefore merges its single-device scans first,
   * and the root merges those already sorted streams. This asserts the operator-tree shape that
   * protects that invariant rather than merely counting drivers.
   */
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

  private LocalExecutionPlanContext createLocalExecutionPlanContext(String queryId) {
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

  private DeviceTableScanNode initDeviceTableScanNode(int deviceNum) {
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
      assertEquals(CollectOperator.class, root.getClass());
      assertEquals(4, ((CollectOperator) root).getChildren().size());
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
        assertEquals(CollectOperator.class, root.getClass());
        assertEquals(2, ((CollectOperator) root).getChildren().size());
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
}
