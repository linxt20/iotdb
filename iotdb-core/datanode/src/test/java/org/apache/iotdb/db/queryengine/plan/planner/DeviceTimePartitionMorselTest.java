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
import org.apache.iotdb.calc.execution.operator.process.FilterAndProjectOperator;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.FilterNode;
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
import org.apache.iotdb.db.queryengine.execution.operator.process.TableReadyCollectOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.spill.DeviceEntryDataSet;
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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.LongPredicate;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanGraphPrinter.DEVICE_NUMBER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
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
        assertEquals(TableReadyCollectOperator.class, root.getClass());
        assertEquals(4, ((TableReadyCollectOperator) root).getChildren().size());
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
        assertEquals(TableReadyCollectOperator.class, root.getClass());
        assertEquals(3, ((TableReadyCollectOperator) root).getChildren().size());
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
        assertEquals(TableReadyCollectOperator.class, root.getClass());
        assertEquals(4, ((TableReadyCollectOperator) root).getChildren().size());
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

  /** Missing per-partition snapshots preserve the original contiguous equal-count split. */
  @Test
  public void testMissingSizeMetadataKeepsEqualCountMorsels() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(false);
    try {
      DeviceTableScanNode node = initDeviceTableScanNode(6);
      node.setAllowParallelScan(true);
      LocalExecutionPlanContext context =
          createContext("tp_morsel_missing_sizes", dataRegionWithPartitions(0L, 2L, 4L, 6L));
      context.setDegreeOfParallelism(2);

      Operator root = node.accept(generator, context);
      try {
        assertEquals(
            Arrays.asList(Arrays.asList(0L, 2L), Arrays.asList(4L, 6L)),
            partitionGroupsByDriver(context));
        assertProductSplitIsComplete(context, 6, Arrays.asList(0L, 2L, 4L, 6L));
      } finally {
        closeQuietly(root);
        closePipelineOperations(context);
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(false);
      IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(false);
    }
  }

  /**
   * File-size-aware partitioning uses the minimum-loaded group for every next-largest partition.
   * Four equally-counted partitions can therefore have a very unequal workload: 70, 60, 10, and 10
   * bytes are grouped as (70 + 10) and (60 + 10), instead of the contiguous (70 + 60) and (10 + 10)
   * split. The product coverage assertion keeps this scheduling optimization honest.
   */
  @Test
  public void testWeightedTimePartitionMorselsBalanceSkewAndPreserveCoverage() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorselSizeWeighting(true);
    try {
      Map<Long, Long> partitionSizes = new LinkedHashMap<>();
      partitionSizes.put(0L, 70L);
      partitionSizes.put(2L, 60L);
      partitionSizes.put(4L, 10L);
      partitionSizes.put(6L, 10L);

      DeviceTableScanNode node = initDeviceTableScanNode(6);
      node.setAllowParallelScan(true);
      LocalExecutionPlanContext context =
          createContext(
              "tp_morsel_weighted_skew", dataRegionWithPartitionFileSizes(partitionSizes));
      context.setDegreeOfParallelism(2);

      Operator root = node.accept(generator, context);
      try {
        assertEquals(2, context.getPipelineNumber());
        assertEquals(
            Arrays.asList(Arrays.asList(0L, 6L), Arrays.asList(2L, 4L)),
            partitionGroupsByDriver(context));
        // The maximum group weight is 80 rather than 130 for a contiguous equal-count split.
        assertEquals(80L, maxGroupWeight(partitionGroupsByDriver(context), partitionSizes));
        assertMorselEvidence(
            context,
            Arrays.asList(80L, 70L),
            "LPT_TSFILE_BYTES",
            Arrays.asList("[0, 6]", "[2, 4]"));
        assertProductSplitIsComplete(context, 6, new ArrayList<>(partitionSizes.keySet()));
      } finally {
        closeQuietly(root);
        closePipelineOperations(context);
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(false);
      IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(false);
      IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorselSizeWeighting(true);
    }
  }

  /**
   * The control arm keeps exactly the same safe morsel decomposition but forms time groups by
   * count. Its per-driver EXPLAIN ANALYZE annotations retain the observed metadata estimates, so an
   * equal-count versus LPT comparison is reproducible without pretending that unavailable metadata
   * is a zero-byte estimate.
   */
  @Test
  public void testEqualCountMorselsExposeControlEvidence() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorselSizeWeighting(false);
    try {
      Map<Long, Long> partitionSizes = new LinkedHashMap<>();
      partitionSizes.put(0L, 70L);
      partitionSizes.put(2L, 60L);
      partitionSizes.put(4L, 10L);
      partitionSizes.put(6L, 10L);

      DeviceTableScanNode node = initDeviceTableScanNode(6);
      node.setAllowParallelScan(true);
      LocalExecutionPlanContext context =
          createContext(
              "tp_morsel_equal_count_evidence", dataRegionWithPartitionFileSizes(partitionSizes));
      context.setDegreeOfParallelism(2);

      Operator root = node.accept(generator, context);
      try {
        assertEquals(
            Arrays.asList(Arrays.asList(0L, 2L), Arrays.asList(4L, 6L)),
            partitionGroupsByDriver(context));
        // The control arm is intentionally less balanced on this skew: 130 versus 20 bytes. The
        // important evidence is exported on the same individual driver contexts as the LPT arm,
        // rather than inferred by a test-only helper after the query has completed.
        assertEquals(130L, maxGroupWeight(partitionGroupsByDriver(context), partitionSizes));
        assertMorselEvidence(
            context,
            Arrays.asList(130L, 20L),
            "EQUAL_PARTITION_COUNT",
            Arrays.asList("[0, 2]", "[4, 6]"));
        assertProductSplitIsComplete(context, 6, new ArrayList<>(partitionSizes.keySet()));
      } finally {
        closeQuietly(root);
        closePipelineOperations(context);
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(false);
      IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(false);
      IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorselSizeWeighting(true);
    }
  }

  /** The same metadata must always create the same driver-to-partition assignment. */
  @Test
  public void testWeightedTimePartitionMorselsAreDeterministic() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(false);
    try {
      Map<Long, Long> partitionSizes = new LinkedHashMap<>();
      partitionSizes.put(0L, 70L);
      partitionSizes.put(2L, 60L);
      partitionSizes.put(4L, 10L);
      partitionSizes.put(6L, 10L);

      List<List<Long>> first =
          partitionGroupsForContext("tp_morsel_weighted_first", partitionSizes);
      List<List<Long>> second =
          partitionGroupsForContext("tp_morsel_weighted_second", partitionSizes);

      assertEquals(first, second);
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(false);
      IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(false);
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
        assertEquals(TableReadyCollectOperator.class, root.getClass());
        assertEquals(4, ((TableReadyCollectOperator) root).getChildren().size());
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
   * MiB the estimate is 2 drivers, so 8 devices × 2 partitions at dop = 4 collapses to Kt = 2, Kd =
   * 1, i.e. 2 pipelines instead of the 4 the same setup yields with estimation off.
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
          createContext("tp_morsel_estimation_off", dataRegionWithPartitionsAndFiles(2, 0L, 1L));
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
        assertEquals(TableReadyCollectOperator.class, root.getClass());
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

  /** A last partition containing MAX_VALUE has no representable exclusive upper bound. */
  @Test
  public void testLastPartitionWithUnrepresentableUpperBoundIsRejected() {
    final long lastPartitionId = TimePartitionUtils.getTimePartitionId(Long.MAX_VALUE);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DataNodeTableOperatorGenerator.buildTimePartitionRangePredicate(
                lastPartitionId, TP_INTERVAL, "time"));
  }

  @Test
  public void testNegativePartitionUsesIndependentHalfOpenBounds() throws Exception {
    withTimePartitionConfiguration(
        0L,
        10L,
        () -> {
          Expression predicate =
              DataNodeTableOperatorGenerator.buildTimePartitionRangePredicate(-2L, 10L, "time");
          assertPointwisePredicate(
              predicate,
              "time",
              new long[] {Long.MIN_VALUE, -21L, -20L, -19L, -11L, -10L, -1L, 0L},
              time -> time >= -20L && time < -10L);
        });
  }

  @Test
  public void testNonZeroOriginUsesIndependentExpectedBounds() throws Exception {
    withTimePartitionConfiguration(
        7L,
        10L,
        () -> {
          Expression negative =
              DataNodeTableOperatorGenerator.buildTimePartitionRangePredicate(-1L, 10L, "event_ts");
          Expression positive =
              DataNodeTableOperatorGenerator.buildTimePartitionRangePredicate(2L, 10L, "event_ts");
          long[] points = {-4L, -3L, -2L, 6L, 7L, 26L, 27L, 36L, 37L};
          assertPointwisePredicate(negative, "event_ts", points, time -> time >= -3L && time < 7L);
          assertPointwisePredicate(positive, "event_ts", points, time -> time >= 27L && time < 37L);
        });
  }

  @Test
  public void testTimeAliasAndOriginalPredicateIntersectPointByPoint() throws Exception {
    withTimePartitionConfiguration(
        0L,
        10L,
        () -> {
          DeviceTableScanNode node = initDeviceTableScanNode(4);
          node.setAllowParallelScan(true);
          Symbol originalTime = new Symbol("time");
          Symbol alias = new Symbol("event_ts");
          Map<Symbol, ColumnSchema> assignments = new HashMap<>(node.getAssignments());
          ColumnSchema timeColumn = assignments.remove(originalTime);
          assignments.put(alias, timeColumn);
          node.setAssignments(assignments);
          node.setOutputSymbols(Arrays.asList(alias, new Symbol("tag1"), new Symbol("s1")));
          Expression userPredicate =
              LogicalExpression.and(
                  comparison("event_ts", ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, -7L),
                  comparison("event_ts", ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, 13L));
          node.setTimePredicate(userPredicate);
          List<DeviceEntry> originalDevices = new ArrayList<>(node.getDeviceEntries());
          LocalExecutionPlanContext context =
              createContext("alias_intersection", dataRegionWithPartitions(-1L, 0L, 1L));
          context.setDegreeOfParallelism(2);
          context
              .getTypeProvider()
              .putTableModelType(alias, TypeFactory.getType(TSDataType.TIMESTAMP));
          Operator root = node.accept(generator, context);
          try {
            assertEquals(TableReadyCollectOperator.class, root.getClass());
            assertEquals("TIME_PARTITION", context.getInstanceContext().getSplitMode());
            assertPointwiseScanCoverage(
                context,
                4,
                new long[] {
                  Long.MIN_VALUE,
                  -11L,
                  -10L,
                  -8L,
                  -7L,
                  -1L,
                  0L,
                  9L,
                  10L,
                  13L,
                  14L,
                  19L,
                  20L,
                  Long.MAX_VALUE
                },
                time -> time >= -7L && time <= 13L);
            assertSame(userPredicate, node.getTimePredicate().get());
            assertEquals(originalDevices, node.getDeviceEntries());
            assertEquals(alias, node.getTimeColumn().get());
          } finally {
            closeQuietly(root);
            closePipelineOperations(context);
          }
        });
  }

  @Test
  public void testMultiplePartitionOrPreservesGapsAndAllDevices() throws Exception {
    withTimePartitionConfiguration(
        0L,
        10L,
        () -> {
          DeviceTableScanNode node = initDeviceTableScanNode(4);
          node.setAllowParallelScan(true);
          LocalExecutionPlanContext context =
              createContext("partition_or_gaps", dataRegionWithPartitions(0L, 2L, 4L));
          context.setDegreeOfParallelism(2);
          Operator root = node.accept(generator, context);
          try {
            assertEquals("TIME_PARTITION", context.getInstanceContext().getSplitMode());
            boolean foundDisjointRanges = false;
            for (Operator scan : actualScanRoots(context)) {
              foundDisjointRanges |= timeFilterOf(scan).getTimeRanges().size() > 1;
            }
            assertTrue("at least one morsel must carry the disjoint OR", foundDisjointRanges);
            assertPointwiseScanCoverage(
                context,
                4,
                new long[] {-1L, 0L, 9L, 10L, 19L, 20L, 29L, 30L, 39L, 40L, 49L, 50L},
                time ->
                    (time >= 0L && time < 10L)
                        || (time >= 20L && time < 30L)
                        || (time >= 40L && time < 50L));
          } finally {
            closeQuietly(root);
            closePipelineOperations(context);
          }
        });
  }

  @Test
  public void testNegativeIntervalFallsBackBeforeTimePartitionResources() throws Exception {
    withTimePartitionConfiguration(
        0L,
        -10L,
        () ->
            assertSafeDeviceFallback(
                "negative_interval",
                null,
                new Long[] {0L, 1L},
                new long[] {Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE},
                time -> true));
  }

  @Test
  public void testUnboundPartitionIdentityFallsBackWithoutNarrowing() throws Exception {
    withTimePartitionConfiguration(
        0L,
        10L,
        () ->
            assertSafeDeviceFallback(
                "unbound_partition",
                comparison("time", ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, -5L),
                new Long[] {Long.MIN_VALUE, 0L},
                new long[] {Long.MIN_VALUE, -6L, -5L, 0L, Long.MAX_VALUE},
                time -> time >= -5L));
  }

  @Test
  public void testLowerClampedPartitionFallsBackWithoutWideningPredicate() throws Exception {
    withTimePartitionConfiguration(
        0L,
        10L,
        () -> {
          long first = Math.floorDiv(Long.MIN_VALUE, 10L);
          assertSafeDeviceFallback(
              "lower_clamped_partition",
              comparison(
                  "time", ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, Long.MIN_VALUE + 9L),
              new Long[] {first, first + 1L},
              new long[] {
                Long.MIN_VALUE,
                Long.MIN_VALUE + 1L,
                Long.MIN_VALUE + 7L,
                Long.MIN_VALUE + 8L,
                Long.MIN_VALUE + 9L,
                Long.MIN_VALUE + 10L,
                0L,
                Long.MAX_VALUE
              },
              time -> time <= Long.MIN_VALUE + 9L);
        });
  }

  @Test
  public void testUpperClampedPartitionFallsBackWithoutDroppingMaxTimestamp() throws Exception {
    withTimePartitionConfiguration(
        0L,
        10L,
        () ->
            assertSafeDeviceFallback(
                "upper_clamped_partition",
                comparison(
                    "time",
                    ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
                    Long.MAX_VALUE - 1L),
                new Long[] {0L, Math.floorDiv(Long.MAX_VALUE, 10L)},
                new long[] {
                  Long.MIN_VALUE, 0L, Long.MAX_VALUE - 2L, Long.MAX_VALUE - 1L, Long.MAX_VALUE
                },
                time -> time >= Long.MAX_VALUE - 1L));
  }

  @Test
  public void testPartitionPredicateRejectsUnboundOrNonPositiveIntervals() throws Exception {
    withTimePartitionConfiguration(
        0L,
        10L,
        () -> {
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  DataNodeTableOperatorGenerator.buildTimePartitionRangePredicate(0L, 11L, "time"));
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  DataNodeTableOperatorGenerator.buildTimePartitionRangePredicate(0L, 0L, "time"));
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  DataNodeTableOperatorGenerator.buildTimePartitionRangePredicate(
                      Long.MIN_VALUE, 10L, "time"));
        });
  }

  @Test
  public void testTransparentFilterSurvivesUnsafePartitionFallback() throws Exception {
    withTimePartitionConfiguration(
        0L,
        10L,
        () -> {
          DeviceTableScanNode node = initDeviceTableScanNode(4);
          node.setAllowParallelScan(true);
          FilterNode filter = new FilterNode(new PlanNodeId("unsafe_filter"), node, s1IsPositive());
          LocalExecutionPlanContext context =
              createTypedContext(
                  "unsafe_partition_filter",
                  dataRegionWithPartitions(0L, Math.floorDiv(Long.MAX_VALUE, 10L)));
          context.setDegreeOfParallelism(2);
          Operator root = filter.accept(generator, context);
          try {
            assertEquals(FilterAndProjectOperator.class, root.getClass());
            assertEquals(
                "UNSUPPORTED_TIME_PARTITION_RANGE", context.getInstanceContext().getSplitReason());
            assertPointwiseScanCoverage(
                context, 4, new long[] {Long.MIN_VALUE, 0L, Long.MAX_VALUE}, time -> true);
          } finally {
            closeQuietly(root);
            closePipelineOperations(context);
          }
        });
  }

  @Test
  public void testClonePreservesExecutionFieldsAndTopKWithoutChangingOriginal() throws Exception {
    withTimePartitionConfiguration(
        0L,
        10L,
        () -> {
          DeviceTableScanNode origin = initDeviceTableScanNode(4);
          origin.setAllowParallelScan(true);
          origin.setScanOrder(Ordering.DESC);
          origin.setPushLimitToEachDevice(true);
          origin.setPushDownLimit(7L);
          origin.setPushDownOffset(2L);
          origin.setContainsNonAlignedDevice();
          origin.setTopKRuntimeFilterSourceId("topk_contract_source");
          Expression originalTime =
              comparison("time", ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, -7L);
          Expression originalField = s1IsPositive();
          origin.setTimePredicate(originalTime);
          origin.setPushDownPredicate(originalField);
          Filter originalDerivedTime = Mockito.mock(Filter.class);
          origin.setTimeFilter(originalDerivedTime);
          TRegionReplicaSet routing = new TRegionReplicaSet();
          origin.setRegionReplicaSet(routing);
          List<DeviceEntry> originalDevices = new ArrayList<>(origin.getDeviceEntries());
          DeviceEntryDataSet dataSet = Mockito.mock(DeviceEntryDataSet.class);
          Mockito.when(dataSet.getInlineEntries()).thenReturn(originalDevices);
          Mockito.when(dataSet.getEntryCount()).thenReturn(4);
          origin.setCoordinatorDeviceEntryDataSet(dataSet);
          List<DeviceEntry> subset = new ArrayList<>(originalDevices.subList(0, 2));
          PlanNodeId newId = new PlanNodeId("cloned_time_scan");
          Expression range =
              DataNodeTableOperatorGenerator.buildTimePartitionRangePredicate(-1L, 10L, "time");
          Method clone =
              DataNodeTableOperatorGenerator.class.getDeclaredMethod(
                  "cloneSubScanNode",
                  DeviceTableScanNode.class,
                  PlanNodeId.class,
                  List.class,
                  Expression.class);
          clone.setAccessible(true);
          DeviceTableScanNode sub =
              (DeviceTableScanNode) clone.invoke(null, origin, newId, subset, range);
          assertNotSame(origin, sub);
          assertEquals(newId, sub.getPlanNodeId());
          assertEquals(origin.getQualifiedObjectName(), sub.getQualifiedObjectName());
          assertEquals(origin.getOutputSymbols(), sub.getOutputSymbols());
          assertEquals(origin.getAssignments(), sub.getAssignments());
          assertEquals(origin.getTagAndAttributeIndexMap(), sub.getTagAndAttributeIndexMap());
          assertEquals(Ordering.DESC, sub.getScanOrder());
          assertSame(originalField, sub.getPushDownPredicate());
          assertEquals(7L, sub.getPushDownLimit());
          assertEquals(2L, sub.getPushDownOffset());
          assertTrue(sub.isPushLimitToEachDevice());
          assertTrue(sub.containsNonAlignedDevice());
          assertTrue(sub.isAllowParallelScan());
          assertEquals("topk_contract_source", sub.getTopKRuntimeFilterSourceId());
          assertSame(dataSet, sub.getCoordinatorDeviceEntryDataSet());
          assertEquals(subset, sub.getDeviceEntries());
          assertFalse(sub.getDeviceEntryDataSetHandle().isPresent());
          assertNotSame(originalTime, sub.getTimePredicate().get());
          assertPointwisePredicate(
              sub.getTimePredicate().get(),
              "time",
              new long[] {-11L, -10L, -8L, -7L, -1L, 0L, 1L},
              time -> time >= -7L && time < 0L);
          // The derived filter is rebuilt for the narrowed predicate. Region routing is rebound by
          // distributed planning; local execution takes its DataRegion from the fragment context.
          assertNull(sub.getTimeFilter());
          assertNull(sub.getRegionReplicaSet());
          assertSame(originalDerivedTime, origin.getTimeFilter());
          assertSame(routing, origin.getRegionReplicaSet());
          assertEquals(new PlanNodeId("DeviceTableScanNode"), origin.getPlanNodeId());
          assertSame(originalTime, origin.getTimePredicate().get());
          assertSame(originalField, origin.getPushDownPredicate());
          assertEquals(originalDevices, origin.getDeviceEntries());
          assertEquals(4, origin.getDeviceEntries().size());
          assertEquals("topk_contract_source", origin.getTopKRuntimeFilterSourceId());
          subset.clear();
          assertEquals(4, origin.getDeviceEntries().size());
        });
  }

  private void assertSafeDeviceFallback(
      String queryId,
      Expression userPredicate,
      Long[] partitionIds,
      long[] points,
      LongPredicate expected)
      throws Exception {
    DeviceTableScanNode node = initDeviceTableScanNode(4);
    node.setAllowParallelScan(true);
    node.setTimePredicate(userPredicate);
    List<DeviceEntry> devices = new ArrayList<>(node.getDeviceEntries());
    DataRegion region = dataRegionWithPartitions(partitionIds);
    LocalExecutionPlanContext context = createContext(queryId, region);
    context.setDegreeOfParallelism(2);
    Operator root = node.accept(generator, context);
    try {
      assertEquals(TableReadyCollectOperator.class, root.getClass());
      assertEquals("DEVICE", context.getInstanceContext().getSplitMode());
      assertEquals(
          "UNSUPPORTED_TIME_PARTITION_RANGE", context.getInstanceContext().getSplitReason());
      assertEquals(
          "UNSUPPORTED_TIME_PARTITION_RANGE",
          context.getInstanceContext().getCanonicalFallbackReason());
      assertEquals(2, context.getPipelineDriverFactories().size());
      assertPointwiseScanCoverage(context, 4, points, expected);
      for (Operator scan : actualScanRoots(context)) {
        assertFalse(
            scan.getOperatorContext().getSpecifiedInfo().containsKey("MORSEL_TIME_PARTITIONS"));
      }
      Mockito.verify(region.getTsFileManager(), Mockito.never())
          .getTsFileListSnapshot(Mockito.anyLong());
      assertEquals(devices, node.getDeviceEntries());
      if (userPredicate == null) {
        assertFalse(node.getTimePredicate().isPresent());
      } else {
        assertSame(userPredicate, node.getTimePredicate().get());
      }
    } finally {
      closeQuietly(root);
      closePipelineOperations(context);
    }
  }

  private void assertPointwiseScanCoverage(
      LocalExecutionPlanContext context, int deviceCount, long[] points, LongPredicate expected) {
    List<Operator> scans = actualScanRoots(context);
    for (long time : points) {
      int devicesReadingTime = 0;
      for (Operator scan : scans) {
        Filter filter = timeFilterOf(scan);
        boolean selected = filter == null;
        if (filter != null) {
          for (TimeRange range : filter.getTimeRanges()) {
            selected |= time >= range.getMin() && time <= range.getMax();
          }
        }
        if (selected) {
          devicesReadingTime += deviceNumberOf(scan);
        }
      }
      assertEquals(
          "device coverage at time " + time,
          expected.test(time) ? deviceCount : 0,
          devicesReadingTime);
    }
  }

  private List<Operator> actualScanRoots(LocalExecutionPlanContext context) {
    List<Operator> scans = new ArrayList<>();
    for (PipelineDriverFactory factory : context.getPipelineDriverFactories()) {
      collectActualScans(factory.getOperation(), scans);
    }
    return scans;
  }

  private void collectActualScans(Operator operator, List<Operator> scans) {
    if (operator instanceof AbstractTableScanOperator) {
      scans.add(operator);
    } else if (operator instanceof TableReadyCollectOperator) {
      for (Operator child : ((TableReadyCollectOperator) operator).getChildren()) {
        collectActualScans(child, scans);
      }
    } else {
      throw new AssertionError("unexpected scan pipeline root: " + operator.getClass());
    }
  }

  private static Expression comparison(
      String symbol, ComparisonExpression.Operator operator, long value) {
    return new ComparisonExpression(
        operator, new SymbolReference(symbol), new LongLiteral(Long.toString(value)));
  }

  private void assertPointwisePredicate(
      Expression predicate, String symbol, long[] points, LongPredicate expected) {
    for (long time : points) {
      assertEquals(
          "predicate at time " + time,
          expected.test(time),
          evaluateTimePredicate(predicate, symbol, time));
    }
  }

  private boolean evaluateTimePredicate(Expression predicate, String symbol, long time) {
    if (predicate instanceof LogicalExpression) {
      LogicalExpression logical = (LogicalExpression) predicate;
      boolean result = logical.getOperator() == LogicalExpression.Operator.AND;
      for (Expression term : logical.getTerms()) {
        if (logical.getOperator() == LogicalExpression.Operator.AND) {
          result &= evaluateTimePredicate(term, symbol, time);
        } else {
          result |= evaluateTimePredicate(term, symbol, time);
        }
      }
      return result;
    }
    assertTrue(predicate instanceof ComparisonExpression);
    ComparisonExpression comparison = (ComparisonExpression) predicate;
    assertTrue(comparison.getLeft() instanceof SymbolReference);
    assertEquals(symbol, ((SymbolReference) comparison.getLeft()).getName());
    assertTrue(comparison.getRight() instanceof LongLiteral);
    long value = ((LongLiteral) comparison.getRight()).getParsedValue();
    switch (comparison.getOperator()) {
      case GREATER_THAN_OR_EQUAL:
        return time >= value;
      case GREATER_THAN:
        return time > value;
      case LESS_THAN_OR_EQUAL:
        return time <= value;
      case LESS_THAN:
        return time < value;
      case EQUAL:
        return time == value;
      default:
        throw new AssertionError("unexpected time comparison: " + comparison.getOperator());
    }
  }

  @FunctionalInterface
  private interface CheckedTimeAssertion {
    void run() throws Exception;
  }

  private void withTimePartitionConfiguration(
      long origin, long interval, CheckedTimeAssertion assertion) throws Exception {
    Field originField = TimePartitionUtils.class.getDeclaredField("timePartitionOrigin");
    originField.setAccessible(true);
    long previousOrigin = originField.getLong(null);
    long previousInterval = TimePartitionUtils.getTimePartitionInterval();
    org.apache.iotdb.db.conf.IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    boolean previousMorsel = config.isEnableTimePartitionMorsel();
    boolean previousWeighting = config.isEnableTimePartitionMorselSizeWeighting();
    boolean previousEstimation = config.isEnableDopEstimation();
    try {
      TimePartitionUtils.setTimePartitionOrigin(origin);
      TimePartitionUtils.setTimePartitionInterval(interval);
      config.setEnableTimePartitionMorsel(true);
      config.setEnableTimePartitionMorselSizeWeighting(false);
      config.setEnableDopEstimation(false);
      assertion.run();
    } finally {
      TimePartitionUtils.setTimePartitionInterval(previousInterval);
      TimePartitionUtils.setTimePartitionOrigin(previousOrigin);
      config.setEnableTimePartitionMorsel(previousMorsel);
      config.setEnableTimePartitionMorselSizeWeighting(previousWeighting);
      config.setEnableDopEstimation(previousEstimation);
    }
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

    int totalDevicePartitionPairs = 0;
    for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
      // A table-scan operator may hold several disjoint partition ranges. Its device number alone
      // is therefore only the number of device iterators, not the number of (device, partition)
      // work items. This distinction matters when time partitions are grouped into a morsel.
      totalDevicePartitionPairs +=
          deviceNumberOf(driverFactory.getOperation())
              * timeFilterOf(driverFactory.getOperation()).getTimeRanges().size();
    }
    assertEquals(
        "every (device, partition) pair must be scanned exactly once",
        deviceCount * tpIds.size(),
        totalDevicePartitionPairs);
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

  private List<List<Long>> partitionGroupsByDriver(LocalExecutionPlanContext context) {
    List<List<Long>> groups = new ArrayList<>();
    for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
      List<Long> partitions = new ArrayList<>();
      Filter filter = timeFilterOf(driverFactory.getOperation());
      assertNotNull("sub scan must carry a narrowed time filter", filter);
      for (TimeRange range : filter.getTimeRanges()) {
        partitions.add(TimePartitionUtils.getTimePartitionId(range.getMin()));
      }
      Collections.sort(partitions);
      groups.add(partitions);
    }
    return groups;
  }

  private long maxGroupWeight(List<List<Long>> groups, Map<Long, Long> partitionSizes) {
    long max = 0L;
    for (List<Long> group : groups) {
      long total = 0L;
      for (Long partition : group) {
        total += partitionSizes.get(partition);
      }
      max = Math.max(max, total);
    }
    return max;
  }

  private void assertMorselEvidence(
      LocalExecutionPlanContext context,
      List<Long> expectedBytes,
      String expectedScheduling,
      List<String> expectedPartitions) {
    for (int i = 0; i < context.getPipelineDriverFactories().size(); i++) {
      Map<String, Object> specifiedInfo =
          context
              .getPipelineDriverFactories()
              .get(i)
              .getOperation()
              .getOperatorContext()
              .getSpecifiedInfo();
      assertEquals(expectedScheduling, specifiedInfo.get("MORSEL_SCHEDULING"));
      assertEquals(expectedPartitions.get(i), specifiedInfo.get("MORSEL_TIME_PARTITIONS"));
      if (expectedBytes.get(i) < 0) {
        assertFalse(specifiedInfo.containsKey("MORSEL_ESTIMATED_TSFILE_BYTES"));
      } else {
        assertEquals(
            expectedBytes.get(i).toString(), specifiedInfo.get("MORSEL_ESTIMATED_TSFILE_BYTES"));
      }
    }
  }

  private List<List<Long>> partitionGroupsForContext(String queryId, Map<Long, Long> partitionSizes)
      throws Exception {
    DeviceTableScanNode node = initDeviceTableScanNode(6);
    node.setAllowParallelScan(true);
    LocalExecutionPlanContext context =
        createContext(queryId, dataRegionWithPartitionFileSizes(partitionSizes));
    context.setDegreeOfParallelism(2);
    Operator root = node.accept(generator, context);
    try {
      return partitionGroupsByDriver(context);
    } finally {
      closeQuietly(root);
      closePipelineOperations(context);
    }
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
   * Fusing a filter into parallel morsels is only sound when nothing above the scan needs the rows
   * in the scan's own order. The distributed planner records that in {@code allowParallelScan}: it
   * sets the bit only where the required properties are unordered, and clears it both when a sort
   * has to stay above and when it proved the scan's own ordering already satisfies the parent and
   * dropped that sort. A filter above such a scan must therefore not split it.
   */
  @Test
  public void testFilterFusionRefusesWhenOrderingIsRequired() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(true);
    try {
      DeviceTableScanNode scanNode = initDeviceTableScanNode(8);
      scanNode.setAllowParallelScan(false);
      FilterNode filterNode =
          new FilterNode(new PlanNodeId("FilterNode"), scanNode, s1IsPositive());

      LocalExecutionPlanContext context =
          createTypedContext("filter_fusion_ordered", dataRegionWithPartitions(0L, 1L, 2L));
      context.setDegreeOfParallelism(4);

      Operator root = filterNode.accept(generator, context);
      try {
        assertFalse(
            "the TableReadyCollectOperator of the morsel split concatenates streams in arrival order,"
                + " so it may not be used when the parent relies on the scan ordering",
            root instanceof TableReadyCollectOperator);
        assertEquals(
            "the filter stays at the root, above the single unsplit scan stream",
            FilterAndProjectOperator.class,
            root.getClass());
      } finally {
        closeQuietly(root);
        closePipelineOperations(context);
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(false);
    }
  }

  /**
   * The control for {@link #testFilterFusionRefusesWhenOrderingIsRequired()}. Same node, same
   * region, same degree of parallelism, only the bit differs: with the bit set the fusion path does
   * run and the three time partitions become three pipelines.
   */
  @Test
  public void testFilterFusionRunsWhenOrderingIsNotRequired() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(true);
    try {
      DeviceTableScanNode scanNode = initDeviceTableScanNode(8);
      scanNode.setAllowParallelScan(true);
      FilterNode filterNode =
          new FilterNode(new PlanNodeId("FilterNode"), scanNode, s1IsPositive());

      LocalExecutionPlanContext context =
          createTypedContext("filter_fusion_unordered", dataRegionWithPartitions(0L, 1L, 2L));
      context.setDegreeOfParallelism(4);

      Operator root = filterNode.accept(generator, context);
      try {
        assertEquals(TableReadyCollectOperator.class, root.getClass());
        assertEquals(3, ((TableReadyCollectOperator) root).getChildren().size());
        assertEquals(3, context.getPipelineNumber());
        for (PipelineDriverFactory driverFactory : context.getPipelineDriverFactories()) {
          assertEquals(
              "each pipeline root is the fused filter, not the bare scan",
              FilterAndProjectOperator.class,
              driverFactory.getOperation().getClass());
        }
      } finally {
        closeQuietly(root);
        closePipelineOperations(context);
      }
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(false);
    }
  }

  private static Expression s1IsPositive() {
    return new ComparisonExpression(
        ComparisonExpression.Operator.GREATER_THAN,
        new SymbolReference("s1"),
        new LongLiteral("0"));
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

  /**
   * The same context as {@link #createContext(String, DataRegion)} with the scan's output symbols
   * typed. Building a filter above the scan resolves the child's output column types through this
   * provider, and the stub context starts with an empty one.
   */
  private LocalExecutionPlanContext createTypedContext(String queryId, DataRegion dataRegion) {
    LocalExecutionPlanContext context = createContext(queryId, dataRegion);
    TypeProvider typeProvider = context.getTypeProvider();
    typeProvider.putTableModelType(new Symbol("time"), TypeFactory.getType(TSDataType.TIMESTAMP));
    typeProvider.putTableModelType(new Symbol("tag1"), TypeFactory.getType(TSDataType.STRING));
    typeProvider.putTableModelType(new Symbol("s1"), TypeFactory.getType(TSDataType.INT32));
    return context;
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

  /** A region whose per-time-partition snapshots expose a deliberately skewed physical size. */
  private DataRegion dataRegionWithPartitionFileSizes(Map<Long, Long> partitionSizes) {
    TsFileManager tsFileManager = Mockito.mock(TsFileManager.class);
    Mockito.when(tsFileManager.getTimePartitions())
        .thenReturn(new HashSet<>(partitionSizes.keySet()));
    Mockito.when(tsFileManager.getTsFileListSnapshot(Mockito.anyLong()))
        .thenAnswer(
            invocation -> {
              long partition = invocation.getArgument(0);
              Long size = partitionSizes.get(partition);
              if (size == null) {
                return new Pair<>(Collections.emptyList(), Collections.emptyList());
              }
              TsFileResource resource = Mockito.mock(TsFileResource.class);
              Mockito.when(resource.getTsFileSize()).thenReturn(size);
              return new Pair<>(Collections.singletonList(resource), Collections.emptyList());
            });
    DataRegion dataRegion = Mockito.mock(DataRegion.class);
    Mockito.when(dataRegion.getTsFileManager()).thenReturn(tsFileManager);
    return dataRegion;
  }

  @Test
  public void testDeviceConsumerPreservesPredicatePaginationAndSharedTopK() throws Exception {
    assertConsumerScanOptions("DEVICE");
  }

  @Test
  public void testTimePartitionConsumerPreservesPushedPredicatesAndSharedTopK() throws Exception {
    assertConsumerScanOptions("TIME_PARTITION");
  }

  @Test
  public void testOrderedConsumerPreservesPredicatePaginationAndSharedTopK() throws Exception {
    assertConsumerScanOptions("ORDERED");
  }

  private void assertConsumerScanOptions(String mode) throws Exception {
    var config = IoTDBDescriptor.getInstance().getConfig();
    boolean oldDynamic = config.isEnableDynamicScanParallelism();
    boolean oldOrdered = config.isEnableOrderedParallelScan();
    try {
      for (boolean dynamic : new boolean[] {false, true}) {
        config.setEnableDynamicScanParallelism(dynamic);
        config.setEnableOrderedParallelScan("ORDERED".equals(mode));
        withTimePartitionConfiguration(
            0L,
            10L,
            () -> {
              config.setEnableTimePartitionMorsel("TIME_PARTITION".equals(mode));
              DeviceTableScanNode node = initDeviceTableScanNode(4);
              node.setAllowParallelScan(true);
              node.setTimePredicate(
                  LogicalExpression.and(
                      comparison("time", ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, 3L),
                      comparison("time", ComparisonExpression.Operator.LESS_THAN, 17L)));
              node.setPushDownPredicate(s1IsPositive());
              node.setPushLimitToEachDevice(true);
              // Time-partition splitting retains the parent pagination safety assumptions.
              // Exercise per-device pagination where each device belongs to exactly one scan.
              long limit = "TIME_PARTITION".equals(mode) ? 0L : 7L;
              long offset = "TIME_PARTITION".equals(mode) ? 0L : 2L;
              node.setPushDownLimit(limit);
              node.setPushDownOffset(offset);
              node.setTopKRuntimeFilterSourceId("compatibility_topk");
              DataRegion region = dataRegionWithPartitions(0L, 1L);
              Mockito.when(region.getDataRegionIdString()).thenReturn("compatibility_region");
              LocalExecutionPlanContext context =
                  createTypedContext(
                      "scan_options_" + mode.toLowerCase(java.util.Locale.ROOT) + "_" + dynamic,
                      region);
              context.setDegreeOfParallelism(2);
              org.apache.iotdb.calc.execution.filter.TopKRuntimeFilter runtimeFilter =
                  new org.apache.iotdb.calc.execution.filter.TopKRuntimeFilter(true);
              context.dataNodeQueryContext.registerTopKRuntimeFilter(
                  "compatibility_topk", runtimeFilter);
              org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode input = node;
              if ("ORDERED".equals(mode)) {
                Symbol time = new Symbol("time");
                input =
                    new org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode(
                        new PlanNodeId("compatibility_sort"),
                        node,
                        new org.apache.iotdb.commons.queryengine.plan.relational.planner
                            .OrderingScheme(
                            Collections.singletonList(time),
                            Collections.singletonMap(
                                time,
                                org.apache.iotdb.commons.queryengine.plan.relational.planner
                                    .SortOrder.ASC_NULLS_LAST)),
                        false,
                        false);
              }
              context.getDynamicParallelPlanningService().prepare(input, context);
              Operator root = input.accept(generator, context);
              try {
                assertEquals(2, context.getPipelineDriverFactories().size());
                int scans = 0;
                for (PipelineDriverFactory factory : context.getPipelineDriverFactories()) {
                  org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext driver =
                      (org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext)
                          factory.getDriverContext();
                  for (Operator scan : driver.getSourceOperators()) {
                    Field field =
                        AbstractTableScanOperator.class.getDeclaredField("seriesScanOptions");
                    field.setAccessible(true);
                    SeriesScanOptions options = (SeriesScanOptions) field.get(scan);
                    assertNotNull(options.getPushDownFilter());
                    assertFalse(options.getPushDownFilter().satisfyRow(5L, new Object[] {-1}));
                    assertFalse(options.getPushDownFilter().satisfyRow(5L, new Object[] {0}));
                    assertFalse(options.getPushDownFilter().satisfyRow(5L, new Object[] {null}));
                    assertTrue(options.getPushDownFilter().satisfyRow(5L, new Object[] {1}));
                    assertNotNull(options.getGlobalTimeFilter());
                    for (TimeRange range : options.getGlobalTimeFilter().getTimeRanges()) {
                      assertTrue(range.getMin() >= 3L);
                      assertTrue(range.getMax() < 17L);
                    }
                    assertEquals(limit, options.getPushDownLimit());
                    assertEquals(offset, options.getPushDownOffset());
                    assertTrue(options.getPushLimitToEachDevice());
                    assertEquals(Collections.singleton("s1"), options.getAllSensors());
                    assertSame(runtimeFilter, options.getTopKRuntimeFilter());
                    scans++;
                  }
                }
                assertEquals("ORDERED".equals(mode) ? 4 : 2, scans);
              } finally {
                closeQuietly(root);
                closePipelineOperations(context);
              }
            });
      }
    } finally {
      config.setEnableDynamicScanParallelism(oldDynamic);
      config.setEnableOrderedParallelScan(oldOrdered);
    }
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
