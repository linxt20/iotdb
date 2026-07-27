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
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
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
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
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
      int totalPathNum = 0;
      for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
        assertEquals(TableScanOperator.class, driverFactory.getOperation().getClass());
        DataDriverContext dataDriverContext = (DataDriverContext) driverFactory.getDriverContext();
        assertTrue(dataDriverContext.isInputDriver());
        assertEquals(1, dataDriverContext.getSourceOperators().size());
        assertEquals(2, dataDriverContext.getPaths().size());
        totalPathNum += dataDriverContext.getPaths().size();
      }
      assertEquals(8, totalPathNum);

      // the source operators and paths are moved into sub driver contexts, the root driver
      // context only holds the CollectOperator
      DataDriverContext rootDriverContext = (DataDriverContext) context.getDriverContext();
      assertEquals(0, rootDriverContext.getSourceOperators().size());
      assertEquals(0, rootDriverContext.getPaths().size());

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
        assertEquals(1, ((DataDriverContext) driverFactory.getDriverContext()).getPaths().size());
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
      assertEquals(8, rootDriverContext.getPaths().size());
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
      assertEquals(8, ((DataDriverContext) context.getDriverContext()).getPaths().size());
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

  private LocalExecutionPlanContext createLocalExecutionPlanContext(String queryId) {
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(new QueryId(queryId), 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    DataRegion dataRegion = Mockito.mock(DataRegion.class);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    fragmentInstanceContext.setDataRegion(dataRegion);

    return new LocalExecutionPlanContext(
        new TypeProvider(), fragmentInstanceContext, new DataNodeQueryContext(1));
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

  private void closeQuietly(Operator operator) {
    try {
      operator.close();
    } catch (Exception ignored) {
      // ignore the exception during close in test
    }
  }
}
