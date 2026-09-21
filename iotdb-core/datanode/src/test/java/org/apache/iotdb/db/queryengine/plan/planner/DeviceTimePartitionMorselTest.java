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
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the device×TimePartition morsel split of a {@link DeviceTableScanNode} — the safe
 * version, gated by {@code enable_timepartition_morsel}.
 *
 * <p>Two things are asserted, and the second matters more than the first:
 *
 * <ol>
 *   <li>the <em>shape</em> of the split — how many pipelines are created and how devices and time
 *       partitions are cut between them;
 *   <li>that the split really <em>narrows what each pipeline scans</em>. A split that produces the
 *       expected pipeline count while every pipeline still scans the whole table would pass a
 *       counting-only test and yet deliver no parallelism at all. So we also read back each sub
 *       scan's own time filter and assert those filters are pairwise disjoint and together cover
 *       every partition exactly once.
 * </ol>
 */
public class DeviceTimePartitionMorselTest {

  private static final long TP_INTERVAL = TimePartitionUtils.getTimePartitionInterval();

  /** Matches TARGET_BYTES_PER_SCAN_DRIVER, so one file means exactly one estimated driver. */
  private static final long TEST_FILE_SIZE = 128L * 1024 * 1024;

  private static ExecutorService instanceNotificationExecutor;

  private final DataNodeTableOperatorGenerator generator =
      new DataNodeTableOperatorGenerator(new TableMetadataImpl());

  @BeforeClass
  public static void setUp() {
    instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-tp-morsel-notification");
  }

  @AfterClass
  public static void tearDown() {
    instanceNotificationExecutor.shutdown();
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(false);
  }

  /**
   * Flag off. Even with several time partitions present the split stays purely device-based: the
   * pipeline count is min(dop, deviceCount) and no sub scan carries a time filter.
   */
  @Test
  public void testTpMorselFlagOffKeepsDeviceOnlySplit() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(false);
    try {
      DeviceTableScanNode node = initDeviceTableScanNode(8);
      node.setAllowParallelScan(true);
      LocalExecutionPlanContext context =
          createContext("tp_morsel_off", dataRegionWithPartitions(0L, 1L, 2L));
      context.setDegreeOfParallelism(4);

      Operator root = node.accept(generator, context);
      try {
        assertEquals(CollectOperator.class, root.getClass());
        assertEquals(4, ((CollectOperator) root).getChildren().size());
        assertEquals(4, context.getPipelineNumber());
        for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
          assertNull(
              "with the flag off no sub scan may be narrowed to a time partition",
              timeFilterOf(driverFactory.getOperation()));
        }
      } finally {
        closeQuietly(root);
        closePipelineOperations(context);
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(false);
    }
  }

  /**
   * Core shape case: 6 devices × 3 time partitions at dop = 4.
   *
   * <p>Parallelism is decomposed as K = Kt × Kd. With a target of 4 the partitions form Kt = 3
   * groups and the devices Kd = max(1, 4 / 3) = 1 group, giving 3 pipelines. Each pipeline covers
   * all 6 devices but only its own partition, so every (device, partition) pair is scanned exactly
   * once across the three pipelines, and 3 × 6 = 18 device-scans equals 6 × 3.
   */
  @Test
  public void testSixDevicesThreePartitionsDop4() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(true);
    try {
      DeviceTableScanNode node = initDeviceTableScanNode(6);
      node.setAllowParallelScan(true);
      LocalExecutionPlanContext context =
          createContext("tp_morsel_6x3_dop4", dataRegionWithPartitions(0L, 1L, 2L));
      context.setDegreeOfParallelism(4);

      Operator root = node.accept(generator, context);
      try {
        assertEquals(CollectOperator.class, root.getClass());
        assertEquals(3, ((CollectOperator) root).getChildren().size());
        assertEquals(3, context.getPipelineNumber());

        int totalDeviceScans = 0;
        for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
          // Kd == 1, so every pipeline still scans the whole device list ...
          assertEquals(6, deviceNumberOf(driverFactory.getOperation()));
          totalDeviceScans += 6;
        }
        // ... and 3 pipelines × 6 devices = 18 = 6 devices × 3 partitions, i.e. no pair is missed
        // or duplicated.
        assertEquals(18, totalDeviceScans);

        // The point of the whole feature: each sub scan is narrowed to its own partition.
        List<long[]> ranges = subScanTimeRanges(context);
        assertEquals(3, ranges.size());
        assertPairwiseDisjoint(ranges);
        assertCoversPartitionsExactlyOnce(ranges, Arrays.asList(0L, 1L, 2L));
      } finally {
        closeQuietly(root);
        closePipelineOperations(context);
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(false);
    }
  }

  /**
   * Both axes get cut when there is enough parallelism to spread over devices and partitions: 8
   * devices × 2 partitions at dop = 4 gives Kt = 2 and Kd = 2, i.e. 4 pipelines each scanning 4
   * devices × 1 partition = 4 device-scans, totalling 16 = 8 × 2.
   */
  @Test
  public void testBothAxesCutWhenParallelismAllows() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(true);
    try {
      DeviceTableScanNode node = initDeviceTableScanNode(8);
      node.setAllowParallelScan(true);
      LocalExecutionPlanContext context =
          createContext("tp_morsel_8x2_dop4", dataRegionWithPartitions(0L, 1L));
      context.setDegreeOfParallelism(4);

      Operator root = node.accept(generator, context);
      try {
        assertEquals(CollectOperator.class, root.getClass());
        assertEquals(4, ((CollectOperator) root).getChildren().size());
        assertEquals(4, context.getPipelineNumber());

        int totalDeviceScans = 0;
        for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
          assertEquals(4, deviceNumberOf(driverFactory.getOperation()));
          totalDeviceScans += 4;
        }
        assertEquals(16, totalDeviceScans);

        // Both axes are cut here, so a partition is legitimately covered by several pipelines (one
        // per device group) and the partition ranges alone are not disjoint. Correctness is a
        // property of the (device, partition) product instead, which is what this checks.
        assertProductSplitIsComplete(context, 8, Arrays.asList(0L, 1L));
      } finally {
        closeQuietly(root);
        closePipelineOperations(context);
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(false);
    }
  }

  /**
   * A single time partition leaves nothing to split along that axis, so the split must fall back to
   * the plain device-only behaviour even with the flag on.
   */
  @Test
  public void testSinglePartitionFallsBackToDeviceOnlySplit() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(true);
    try {
      DeviceTableScanNode node = initDeviceTableScanNode(8);
      node.setAllowParallelScan(true);
      LocalExecutionPlanContext context =
          createContext("tp_morsel_single_tp", dataRegionWithPartitions(0L));
      context.setDegreeOfParallelism(4);

      Operator root = node.accept(generator, context);
      try {
        assertEquals(CollectOperator.class, root.getClass());
        assertEquals(4, ((CollectOperator) root).getChildren().size());
        assertEquals(4, context.getPipelineNumber());
        for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
          assertEquals(2, deviceNumberOf(driverFactory.getOperation()));
        }
      } finally {
        closeQuietly(root);
        closePipelineOperations(context);
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(false);
    }
  }

  /**
   * Both flags on at once, which is the configuration the end-to-end comparison runs: the morsel
   * path must honour the DOP estimate rather than always using the full dop. With 2 TsFiles of 128
   * MiB the estimate is 2 drivers, so 8 devices × 2 partitions at dop = 4 collapses to Kt = 2,
   * Kd = 1, i.e. 2 pipelines instead of the 4 the same setup yields with estimation off.
   */
  @Test
  public void testDopEstimationConstrainsTpMorselSplit() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(true);
    try {
      // Estimation off: dop = 4 spreads over both axes -> Kt = 2, Kd = 2 -> 4 pipelines.
      IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(false);
      DeviceTableScanNode node = initDeviceTableScanNode(8);
      node.setAllowParallelScan(true);
      LocalExecutionPlanContext withoutEstimation =
          createContext(
              "tp_morsel_estimation_off",
              dataRegionWithPartitionsAndFiles(2, 0L, 1L));
      withoutEstimation.setDegreeOfParallelism(4);
      Operator rootWithout = node.accept(generator, withoutEstimation);
      try {
        assertEquals(4, withoutEstimation.getPipelineNumber());
      } finally {
        closeQuietly(rootWithout);
        closePipelineOperations(withoutEstimation);
      }

      // Estimation on: the same region estimates 2 drivers, capping the split at 2 pipelines.
      IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(true);
      DeviceTableScanNode node2 = initDeviceTableScanNode(8);
      node2.setAllowParallelScan(true);
      LocalExecutionPlanContext withEstimation =
          createContext("tp_morsel_estimation_on", dataRegionWithPartitionsAndFiles(2, 0L, 1L));
      withEstimation.setDegreeOfParallelism(4);
      Operator rootWith = node2.accept(generator, withEstimation);
      try {
        assertEquals(
            "the DOP estimate must cap the morsel split, not just the device-only split",
            2,
            withEstimation.getPipelineNumber());

        // Still a complete partition of the (device, partition) product, just with fewer drivers.
        assertProductSplitIsComplete(withEstimation, 8, Arrays.asList(0L, 1L));
      } finally {
        closeQuietly(rootWith);
        closePipelineOperations(withEstimation);
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(false);
      IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(false);
    }
  }

  /**
   * A pre-existing user time predicate must be preserved and AND-ed with the partition range, never
   * replaced: the partition filter narrows the scan further, it does not drop the user's own bound.
   */
  @Test
  public void testExistingTimePredicateIsPreserved() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(true);
    try {
      DeviceTableScanNode node = initDeviceTableScanNode(4);
      node.setAllowParallelScan(true);
      node.setTimePredicate(
          new ComparisonExpression(
              ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
              new SymbolReference("time"),
              new LongLiteral("5")));

      LocalExecutionPlanContext context =
          createContext("tp_morsel_user_pred", dataRegionWithPartitions(0L, 1L));
      context.setDegreeOfParallelism(2);

      Operator root = node.accept(generator, context);
      try {
        assertEquals(CollectOperator.class, root.getClass());
        long tightestLowerBound = Long.MAX_VALUE;
        for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
          Filter filter = timeFilterOf(driverFactory.getOperation());
          assertNotNull("each sub scan must keep a time predicate", filter);
          // The partition range only ever narrows the scan further, so no pipeline may reach below
          // the user's own bound of 5.
          long lower = lowerBound(filter);
          assertTrue(
              "the user's time bound must not be widened by the split, got " + filter, lower >= 5L);
          tightestLowerBound = Math.min(tightestLowerBound, lower);
        }
        // And the user's bound is still the effective floor somewhere, i.e. it was not discarded
        // in favour of the partition range.
        assertEquals("the user's own time bound must survive the split", 5L, tightestLowerBound);
      } finally {
        closeQuietly(root);
        closePipelineOperations(context);
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(false);
    }
  }

  /**
   * The partition predicate must be a half-open range [partitionStart, partitionStart + interval)
   * whose column side is the time symbol, not a literal and not another column.
   */
  @Test
  public void testPartitionPredicateShape() {
    Expression predicate =
        DataNodeTableOperatorGenerator.buildTimePartitionRangePredicate(0L, TP_INTERVAL, "time");

    assertEquals(LogicalExpression.class, predicate.getClass());
    LogicalExpression and = (LogicalExpression) predicate;
    assertEquals(LogicalExpression.Operator.AND, and.getOperator());
    assertEquals(2, and.getTerms().size());

    long expectedLower = TimePartitionUtils.getStartTimeByPartitionId(0L);
    assertEquals(
        expectedLower,
        boundValue(and.getTerms().get(0), ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL));
    assertEquals(
        expectedLower + TP_INTERVAL,
        boundValue(and.getTerms().get(1), ComparisonExpression.Operator.LESS_THAN));

    ComparisonExpression first = (ComparisonExpression) and.getTerms().get(0);
    assertEquals(SymbolReference.class, first.getLeft().getClass());
    assertEquals("time", ((SymbolReference) first.getLeft()).getName());
  }

  /**
   * The last representable partition must not have its upper bound overflow into a negative value,
   * which would turn the range inside out and silently scan everything.
   */
  @Test
  public void testLastPartitionUpperBoundDoesNotOverflow() {
    final long lastPartitionId = TimePartitionUtils.getTimePartitionId(Long.MAX_VALUE);
    Expression predicate =
        DataNodeTableOperatorGenerator.buildTimePartitionRangePredicate(
            lastPartitionId, TP_INTERVAL, "time");
    LogicalExpression and = (LogicalExpression) predicate;
    long lower =
        boundValue(and.getTerms().get(0), ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL);
    long upper = boundValue(and.getTerms().get(1), ComparisonExpression.Operator.LESS_THAN);
    assertTrue(
        "upper bound must stay above the lower bound, got [" + lower + ", " + upper + ")",
        upper > lower);
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  /**
   * Check the split as a partition of the (device × partition) product.
   *
   * <p>When both axes are cut, several pipelines cover the same partition (one per device group),
   * so the raw list of sub scan ranges is expected to repeat. What must hold is that the
   * <em>distinct</em> ranges are disjoint and together cover every partition exactly once, and that
   * the total number of device-scans equals {@code deviceCount × partitionCount} — no pair read
   * twice, none missed.
   */
  private void assertProductSplitIsComplete(
      LocalExecutionPlanContext context, int deviceCount, List<Long> tpIds) {
    List<long[]> allRanges = subScanTimeRanges(context);

    // Deduplicate by value: pipelines in different device groups share the same partition range.
    List<long[]> distinct = new ArrayList<>();
    for (long[] range : allRanges) {
      boolean seen = false;
      for (long[] existing : distinct) {
        if (existing[0] == range[0] && existing[1] == range[1]) {
          seen = true;
          break;
        }
      }
      if (!seen) {
        distinct.add(range);
      }
    }

    assertPairwiseDisjoint(distinct);
    assertCoversPartitionsExactlyOnce(distinct, tpIds);

    int totalDeviceScans = 0;
    for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
      totalDeviceScans += deviceNumberOf(driverFactory.getOperation());
    }
    assertEquals(
        "every (device, partition) pair must be scanned exactly once",
        deviceCount * tpIds.size(),
        totalDeviceScans);
  }

  /**
   * The {min, max} pair of every time range each sub scan is narrowed to.
   *
   * <p>{@code seriesScanOptions} is protected on the operator, so it is read reflectively rather
   * than by widening the visibility of a shared execution-layer class from a planner test.
   */
  private List<long[]> subScanTimeRanges(LocalExecutionPlanContext context) {
    List<long[]> ranges = new ArrayList<>();
    for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
      Filter filter = timeFilterOf(driverFactory.getOperation());
      assertNotNull("sub scan must carry a narrowed time filter", filter);
      for (TimeRange range : filter.getTimeRanges()) {
        ranges.add(new long[] {range.getMin(), range.getMax()});
      }
    }
    return ranges;
  }

  private Filter timeFilterOf(Operator operator) {
    try {
      Field field = AbstractTableScanOperator.class.getDeclaredField("seriesScanOptions");
      field.setAccessible(true);
      SeriesScanOptions options = (SeriesScanOptions) field.get(operator);
      return options == null ? null : options.getGlobalTimeFilter();
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("cannot read seriesScanOptions for " + operator, e);
    }
  }

  /** The smallest time the scan can emit, i.e. the tightest lower bound of its filter. */
  private long lowerBound(Filter filter) {
    long min = Long.MAX_VALUE;
    for (TimeRange range : filter.getTimeRanges()) {
      min = Math.min(min, range.getMin());
    }
    return min;
  }

  private void assertPairwiseDisjoint(List<long[]> ranges) {
    for (int i = 0; i < ranges.size(); i++) {
      for (int j = i + 1; j < ranges.size(); j++) {
        long[] a = ranges.get(i);
        long[] b = ranges.get(j);
        assertTrue(
            "sub scan time ranges must not overlap, otherwise rows are read twice: ["
                + a[0]
                + ","
                + a[1]
                + "] vs ["
                + b[0]
                + ","
                + b[1]
                + "]",
            a[1] < b[0] || b[1] < a[0]);
      }
    }
  }

  /** Every partition must be covered by exactly one sub scan range, and none may be missing. */
  private void assertCoversPartitionsExactlyOnce(List<long[]> ranges, List<Long> tpIds) {
    for (Long tpId : tpIds) {
      long lower = TimePartitionUtils.getStartTimeByPartitionId(tpId);
      long upperInclusive = lower + TP_INTERVAL - 1;
      int covering = 0;
      for (long[] range : ranges) {
        if (range[0] <= lower && range[1] >= upperInclusive) {
          covering++;
        }
      }
      assertEquals(
          "partition " + tpId + " must be covered exactly once, ranges=" + describe(ranges),
          1,
          covering);
    }
  }

  private String describe(List<long[]> ranges) {
    StringBuilder sb = new StringBuilder("[");
    for (long[] r : ranges) {
      sb.append("[").append(r[0]).append(",").append(r[1]).append("]");
    }
    return sb.append("]").toString();
  }

  private long boundValue(Expression comparison, ComparisonExpression.Operator expected) {
    assertEquals(ComparisonExpression.class, comparison.getClass());
    ComparisonExpression cmp = (ComparisonExpression) comparison;
    assertEquals(expected, cmp.getOperator());
    assertEquals(LongLiteral.class, cmp.getRight().getClass());
    return ((LongLiteral) cmp.getRight()).getParsedValue();
  }

  private int deviceNumberOf(Operator operator) {
    return Integer.parseInt(
        (String) operator.getOperatorContext().getSpecifiedInfo().get(DEVICE_NUMBER));
  }

  private void closePipelineOperations(LocalExecutionPlanContext context) {
    for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
      closeQuietly(driverFactory.getOperation());
    }
  }

  private void closeQuietly(Operator operator) {
    try {
      operator.close();
    } catch (Exception ignored) {
      // ignore the exception during close in test
    }
  }

  /**
   * A context whose region reports the given time partitions, so the morsel split sees a realistic
   * partition count without needing real storage behind it.
   */
  private LocalExecutionPlanContext createContext(String queryId, DataRegion dataRegion) {
    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(new QueryId(queryId), 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    fragmentInstanceContext.setDataRegion(dataRegion);
    return new LocalExecutionPlanContext(
        new TypeProvider(), fragmentInstanceContext, new DataNodeQueryContext(1));
  }

  private DataRegion dataRegionWithPartitions(Long... tpIds) {
    return dataRegionWithPartitionsAndFiles(0, tpIds);
  }

  /**
   * A region reporting the given partitions and {@code fileCount} TsFiles of {@code fileSize} bytes
   * each, so that DOP estimation has something to estimate from.
   */
  private DataRegion dataRegionWithPartitionsAndFiles(int fileCount, Long... tpIds) {
    Set<Long> partitions = new HashSet<>(Arrays.asList(tpIds));
    List<TsFileResource> files = new ArrayList<>(fileCount);
    for (int i = 0; i < fileCount; i++) {
      TsFileResource resource = Mockito.mock(TsFileResource.class);
      Mockito.when(resource.getTsFileSize())
          .thenReturn(DeviceTimePartitionMorselTest.TEST_FILE_SIZE);
      files.add(resource);
    }
    TsFileManager tsFileManager = Mockito.mock(TsFileManager.class);
    Mockito.when(tsFileManager.getTimePartitions()).thenReturn(partitions);
    Mockito.when(tsFileManager.getAllTsFileListForQuery(Mockito.isNull(), Mockito.isNull()))
        .thenReturn(new Pair<>(files, Collections.emptyList()));
    DataRegion dataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(dataRegion.getTsFileManager()).thenReturn(tsFileManager);
    return dataRegion;
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
}
