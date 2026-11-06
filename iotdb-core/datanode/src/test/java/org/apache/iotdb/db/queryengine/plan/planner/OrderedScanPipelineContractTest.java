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
import org.apache.iotdb.calc.plan.planner.memory.PipelineMemoryEstimator;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.AbstractTableScanOperatorParameter;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Replaces storage leaves only. Production constructs and executes the comparator, two merge
 * levels, exchanges, local sinks and queues; Java's separate row comparator supplies the oracle.
 */
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest(DataNodeTableOperatorGenerator.class)
public class OrderedScanPipelineContractTest {
  private static final Symbol TIME = new Symbol("time");
  private static final Symbol TAG = new Symbol("tag1");
  private static final Symbol FIELD = new Symbol("s1");
  private static final List<TSDataType> TYPES =
      Arrays.asList(TSDataType.TIMESTAMP, TSDataType.STRING, TSDataType.INT32);
  private final DeviceTableScanParallelPipelineTest fixture =
      new DeviceTableScanParallelPipelineTest();
  private final DataNodeTableOperatorGenerator generator =
      new DataNodeTableOperatorGenerator(new TableMetadataImpl());
  private final List<Leaf> leaves = new ArrayList<>();
  private boolean originalOrdered, originalEstimate, originalMorsel;

  private record Row(long time, String tag, int payload) {}

  private record Leaf(
      TableScanOperator operator,
      AbstractTableScanOperatorParameter parameter,
      AtomicInteger closeCalls) {}

  @BeforeClass
  public static void startFixtureExecutor() {
    DeviceTableScanParallelPipelineTest.setUp();
  }

  @AfterClass
  public static void stopFixtureExecutor() {
    DeviceTableScanParallelPipelineTest.tearDown();
  }

  @Before
  public void configureOrderedScanAndStorageLeaves() throws Exception {
    originalOrdered = IoTDBDescriptor.getInstance().getConfig().isEnableOrderedParallelScan();
    originalEstimate = IoTDBDescriptor.getInstance().getConfig().isEnableDopEstimation();
    originalMorsel = IoTDBDescriptor.getInstance().getConfig().isEnableTimePartitionMorsel();
    IoTDBDescriptor.getInstance().getConfig().setEnableOrderedParallelScan(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(false);
    PowerMockito.whenNew(TableScanOperator.class)
        .withAnyArguments()
        .thenAnswer(invocation -> storageLeaf(invocation.getArgument(0)));
  }

  @After
  public void restoreConfiguration() {
    IoTDBDescriptor.getInstance().getConfig().setEnableOrderedParallelScan(originalOrdered);
    IoTDBDescriptor.getInstance().getConfig().setEnableDopEstimation(originalEstimate);
    IoTDBDescriptor.getInstance().getConfig().setEnableTimePartitionMorsel(originalMorsel);
  }

  @Test(timeout = 20000)
  public void orderedTimeAscendingMatchesCompleteRows() throws Exception {
    assertCompleteMerge(false, false, true, false);
  }

  @Test(timeout = 20000)
  public void orderedTimeDescendingMatchesCompleteRows() throws Exception {
    assertCompleteMerge(false, true, true, false);
  }

  @Test(timeout = 20000)
  public void orderedTagAscendingTimeDescendingNullsFirst() throws Exception {
    assertCompleteMerge(true, true, true, false);
  }

  @Test(timeout = 20000)
  public void orderedTagAscendingTimeDescendingNullsLast() throws Exception {
    assertCompleteMerge(true, true, false, false);
  }

  @Test(timeout = 20000)
  public void orderedTagDescendingTimeAscendingNullsFirst() throws Exception {
    assertCompleteMerge(true, false, true, true);
  }

  @Test(timeout = 20000)
  public void orderedTagDescendingTimeAscendingNullsLast() throws Exception {
    assertCompleteMerge(true, false, false, true);
  }

  @Test
  public void timeKeyBeforeTagRetainsSingleStreamSort() throws Exception {
    DeviceTableScanNode scan = scan(false, false);
    assertSingleStreamSort(
        scan,
        Arrays.asList(TIME, TAG),
        Arrays.asList(SortOrder.ASC_NULLS_FIRST, SortOrder.ASC_NULLS_FIRST),
        "time_not_last");
  }

  @Test
  public void timeDirectionMismatchRetainsSingleStreamSort() throws Exception {
    assertSingleStreamSort(
        scan(false, false),
        Collections.singletonList(TIME),
        Collections.singletonList(SortOrder.DESC_NULLS_LAST),
        "time_direction_mismatch");
  }

  @Test
  public void missingAssignmentRejectedBeforeParallelResources() throws Exception {
    DeviceTableScanNode scan = scan(false, false);
    scan.setOutputSymbols(Arrays.asList(TAG, FIELD));
    Map<Symbol, ColumnSchema> assignments = new HashMap<>(scan.getAssignments());
    assignments.remove(TIME);
    scan.setAssignments(assignments);
    assertRejectedBeforeParallelResources(scan, "missing_assignment");
  }

  @Test
  public void missingOutputMappingRejectedBeforeParallelResources() throws Exception {
    DeviceTableScanNode scan = scan(false, false);
    // The assignment exists, but the scan does not output the ordering symbol. An assignment
    // alone cannot prove that the local/root merge comparators address the intended value column.
    scan.setOutputSymbols(Arrays.asList(TAG, FIELD));
    assertTrue(scan.getAssignments().containsKey(TIME));
    assertFalse(scan.getOutputSymbols().contains(TIME));
    assertRejectedBeforeParallelResources(scan, "missing_output_mapping");
  }

  private DeviceTableScanNode scan(boolean withTags, boolean descending) {
    DeviceTableScanNode scan = fixture.initDeviceTableScanNode(6);
    String[] tags =
        withTags
            ? new String[] {null, "b", "a", null, "a", "b"}
            : new String[] {null, null, null, null, null, null};
    List<DeviceEntry> devices = new ArrayList<>();
    for (int i = 0; i < tags.length; i++) {
      devices.add(
          new AlignedDeviceEntry(
              IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"table1", tags[i], "d" + i}),
              new Binary[0]));
    }
    scan.setDeviceEntries(devices);
    scan.setScanOrder(descending ? Ordering.DESC : Ordering.ASC);
    scan.setAllowParallelScan(false);
    return scan;
  }

  private void assertCompleteMerge(
      boolean withTags, boolean descendingTime, boolean nullsFirst, boolean descendingTag)
      throws Exception {
    String scenario =
        (withTags ? "tag" : "time") + "_" + descendingTime + "_" + nullsFirst + "_" + descendingTag;
    DeviceTableScanNode scan = scan(withTags, descendingTime);
    List<DeviceEntry> originalDevices = new ArrayList<>(scan.getDeviceEntries());
    List<Symbol> originalOutputs = new ArrayList<>(scan.getOutputSymbols());
    Map<Symbol, ColumnSchema> originalAssignments = new HashMap<>(scan.getAssignments());
    String originalId = scan.getPlanNodeId().getId();
    SortOrder timeOrder = descendingTime ? SortOrder.DESC_NULLS_LAST : SortOrder.ASC_NULLS_FIRST;
    SortOrder tagOrder =
        descendingTag
            ? (nullsFirst ? SortOrder.DESC_NULLS_FIRST : SortOrder.DESC_NULLS_LAST)
            : (nullsFirst ? SortOrder.ASC_NULLS_FIRST : SortOrder.ASC_NULLS_LAST);
    List<Symbol> keys = withTags ? Arrays.asList(TAG, TIME) : Collections.singletonList(TIME);
    List<SortOrder> orders =
        withTags ? Arrays.asList(tagOrder, timeOrder) : Collections.singletonList(timeOrder);
    LocalExecutionPlanContext context =
        fixture.createLocalExecutionPlanContext("ordered_rows_" + scenario);
    context.setDegreeOfParallelism(3);
    SortNode sort = sort(scan, keys, orders, scenario);
    MemoryPool pool =
        MPPDataExchangeService.getInstance()
            .getMPPDataExchangeManager()
            .getLocalMemoryManager()
            .getQueryPool();
    long baselineExchangeBytes = pool.getRemainingBytes();
    Operator root = null;
    try {
      root = sort.accept(generator, context);
      context.registerPipelineRoot(root);
      assertEquals(TableMergeSortOperator.class, root.getClass());
      assertEquals(3, context.getPipelineNumber());
      assertEquals(3, context.getExchangeSumNum());
      assertEquals(3, context.getResourceRegistry().getObjectCount());
      assertTrue(
          context.getResourceRegistry().snapshotEntries().stream()
              .allMatch(entry -> "LOCAL_QUEUE".equals(entry.kind())));
      assertEquals(6, leaves.size());
      assertEquals(3, context.getInstanceContext().getExchangeEdgeIds().size());
      assertEquals(3, new HashSet<>(context.getInstanceContext().getExchangeEdgeIds()).size());
      assertTrue(((DataDriverContext) context.getDriverContext()).getSourceOperators().isEmpty());
      Set<SharedTsBlockQueue.MemoryReservationKey> memoryKeys = new HashSet<>();
      Set<String> sourceIds = new HashSet<>();
      PipelineMemoryEstimator rootEstimate =
          context.constructPipelineMemoryEstimator(root, null, sort, -1);
      assertEquals(3, rootEstimate.getChildren().size());

      for (int i = 0; i < 3; i++) {
        PipelineDriverFactory factory = context.getPipelineDriverFactories().get(i);
        assertEquals(TableMergeSortOperator.class, factory.getOperation().getClass());
        assertEquals(i + 1, factory.getDriverContext().getPipelineId());
        DataDriverContext driver = (DataDriverContext) factory.getDriverContext();
        assertTrue(driver.isInputDriver());
        assertEquals(2, driver.getSourceOperators().size());
        assertSame(factory.getOperation(), rootEstimate.getChildren().get(i).getRoot());
        assertTrue(driver.getSink() instanceof LocalSinkChannel);
        SharedTsBlockQueue queue = ((LocalSinkChannel) driver.getSink()).getSharedTsBlockQueue();
        assertTrue(memoryKeys.add(queue.getMemoryReservationKey()));
        for (int j = 0; j < 2; j++) {
          Leaf leaf = leaves.get(i * 2 + j);
          assertSame(leaf.operator(), driver.getSourceOperators().get(j));
          assertEquals(1, leaf.parameter().deviceEntries.size());
          assertEquals(originalId + "-ordered-" + i + "-" + j, leaf.parameter().sourceId.getId());
          assertTrue(sourceIds.add(leaf.parameter().sourceId.getId()));
          assertEquals(scan.getScanOrder(), leaf.parameter().scanOrder);
          assertEquals(originalDevices.get(i * 2 + j), leaf.parameter().deviceEntries.get(0));
        }
        drainIntoSink(factory.getOperation(), driver.getSink());
        driver.getSink().setNoMoreTsBlocks();
      }
      assertEquals(3, memoryKeys.size());
      assertEquals(6, sourceIds.size());
      assertTrue(
          pool.snapshotFragmentReservations(
                      context.getFragmentInstanceId().getQueryId().getId(),
                      context.getFragmentInstanceId().getFragmentInstanceId())
                  .fragmentReservedBytes()
              > 0);
      List<Row> actual = drainRows(root);
      assertEquals(
          0L,
          pool.getQueryMemoryReservedBytes(context.getFragmentInstanceId().getQueryId().getId()));
      assertEquals(baselineExchangeBytes, pool.getRemainingBytes());
      List<Row> expected = independentExpectedRows(withTags);
      expected.sort(independentComparator(withTags, descendingTime, nullsFirst, descendingTag));
      assertEquals(24, actual.size());
      assertEquals(multiset(independentExpectedRows(withTags)), multiset(actual));
      assertEquals(expected, actual);
      assertEquals(originalId, scan.getPlanNodeId().getId());
      assertEquals(originalDevices, scan.getDeviceEntries());
      assertEquals(originalOutputs, scan.getOutputSymbols());
      assertEquals(originalAssignments, scan.getAssignments());
      assertEquals(descendingTime ? Ordering.DESC : Ordering.ASC, scan.getScanOrder());
    } finally {
      cleanup(context, root);
    }
    for (Leaf leaf : leaves) assertEquals(1, leaf.closeCalls().get());
  }

  private void assertSingleStreamSort(
      DeviceTableScanNode scan, List<Symbol> keys, List<SortOrder> orders, String scenario)
      throws Exception {
    LocalExecutionPlanContext context =
        fixture.createLocalExecutionPlanContext("ordered_fallback_" + scenario);
    context.setDegreeOfParallelism(3);
    Operator root = null;
    try {
      root = sort(scan, keys, orders, scenario).accept(generator, context);
      context.registerPipelineRoot(root);
      assertEquals(TableSortOperator.class, root.getClass());
      assertEquals(0, context.getPipelineNumber());
      assertEquals(0, context.getExchangeSumNum());
      assertEquals(0, context.getResourceRegistry().getObjectCount());
    } finally {
      cleanup(context, root);
    }
  }

  private void assertRejectedBeforeParallelResources(DeviceTableScanNode scan, String scenario)
      throws Exception {
    LocalExecutionPlanContext context =
        fixture.createLocalExecutionPlanContext("ordered_rejected_" + scenario);
    context.setDegreeOfParallelism(3);
    try {
      // These Sort inputs are invalid in the baseline too. Require the ordinary planner's
      // original rejection, before the ordered optimization enters its resource construction.
      IllegalStateException failure =
          assertThrows(
              IllegalStateException.class,
              () ->
                  sort(
                          scan,
                          Collections.singletonList(TIME),
                          Collections.singletonList(SortOrder.ASC_NULLS_FIRST),
                          scenario)
                      .accept(generator, context));
      assertTrue(failure.getMessage().contains("Sort Item time is not included"));
      boolean ordinarySort = false;
      for (StackTraceElement frame : failure.getStackTrace()) {
        if (frame.getClassName().endsWith(".TableOperatorGenerator")
            && frame.getMethodName().equals("visitSort")) ordinarySort = true;
        assertFalse(
            "invalid ordering entered ordered parallel construction",
            frame.getMethodName().equals("constructParallelOrderedDeviceTableScan"));
      }
      assertTrue("baseline ordinary Sort must own the rejection", ordinarySort);
      assertEquals(0, context.getPipelineNumber());
      assertEquals(0, context.getExchangeSumNum());
      assertEquals(0, context.getResourceRegistry().getObjectCount());
      assertTrue(leaves.isEmpty());
    } finally {
      cleanup(context, null);
    }
  }

  private SortNode sort(
      DeviceTableScanNode scan, List<Symbol> keys, List<SortOrder> orders, String scenario) {
    Map<Symbol, SortOrder> orderings = new LinkedHashMap<>();
    for (int i = 0; i < keys.size(); i++) orderings.put(keys.get(i), orders.get(i));
    return new SortNode(
        new PlanNodeId("sort-" + scenario),
        scan,
        new OrderingScheme(keys, orderings),
        false,
        false);
  }

  private TableScanOperator storageLeaf(AbstractTableScanOperatorParameter parameter)
      throws Exception {
    List<Row> rows = new ArrayList<>();
    long[] times =
        parameter.scanOrder == Ordering.DESC ? new long[] {4, 2, 2, 1} : new long[] {1, 2, 2, 4};
    for (DeviceEntry device : parameter.deviceEntries) {
      Object tag = device.getNthSegment(1);
      for (long time : times) {
        // Nonmonotonic payloads expose an accidental TIME-to-FIELD comparator mapping.
        int payload;
        if (time == 1) payload = 70;
        else if (time == 2) payload = -5;
        else if (time == 4) payload = 30;
        else throw new AssertionError("unexpected fixture time " + time);
        rows.add(new Row(time, tag == null ? null : tag.toString(), payload));
      }
    }
    Deque<TsBlock> blocks = new ArrayDeque<>();
    if (!rows.isEmpty()) {
      int middle = rows.size() / 2;
      blocks.add(tsBlock(rows.subList(0, middle)));
      blocks.add(tsBlock(rows.subList(middle, rows.size())));
    }
    TableScanOperator leaf = mock(TableScanOperator.class);
    AtomicInteger closeCalls = new AtomicInteger();
    when(leaf.getOperatorContext()).thenReturn(parameter.context);
    when(leaf.getSourceId()).thenReturn(parameter.sourceId);
    when(leaf.isBatchQueryDataSource()).thenReturn(parameter.batchQueryDataSource);
    doReturn(Operator.NOT_BLOCKED).when(leaf).isBlocked();
    when(leaf.hasNext()).thenAnswer(invocation -> !blocks.isEmpty());
    when(leaf.hasNextWithTimer()).thenAnswer(invocation -> !blocks.isEmpty());
    when(leaf.next()).thenAnswer(invocation -> blocks.pollFirst());
    when(leaf.nextWithTimer()).thenAnswer(invocation -> blocks.pollFirst());
    when(leaf.isFinished()).thenAnswer(invocation -> blocks.isEmpty());
    when(leaf.calculateMaxReturnSize()).thenReturn(4096L);
    when(leaf.calculateMaxPeekMemory()).thenReturn(4096L);
    when(leaf.calculateMaxPeekMemoryWithCounter()).thenReturn(4096L);
    when(leaf.ramBytesUsed()).thenReturn(256L);
    doAnswer(
            invocation -> {
              closeCalls.incrementAndGet();
              parameter.deviceEntrySource.close();
              return null;
            })
        .when(leaf)
        .close();
    leaves.add(new Leaf(leaf, parameter, closeCalls));
    return leaf;
  }

  private static TsBlock tsBlock(List<Row> rows) {
    TsBlockBuilder block = new TsBlockBuilder(TYPES);
    for (Row row : rows) {
      block.getTimeColumnBuilder().writeLong(0);
      block.getColumnBuilder(0).writeLong(row.time());
      if (row.tag() == null) block.getColumnBuilder(1).appendNull();
      else block.getColumnBuilder(1).writeBinary(new Binary(row.tag(), StandardCharsets.UTF_8));
      block.getColumnBuilder(2).writeInt(row.payload());
      block.declarePosition();
    }
    return block.build();
  }

  private static void drainIntoSink(Operator operator, ISink sink) throws Exception {
    for (int steps = 0; steps < 1000; steps++) {
      operator.isBlocked().get(2, TimeUnit.SECONDS);
      if (!operator.hasNextWithTimer()) return;
      TsBlock block = operator.nextWithTimer();
      if (block != null && !block.isEmpty()) {
        sink.isFull().get(2, TimeUnit.SECONDS);
        sink.send(block);
      }
    }
    fail("local merge exceeded bounded drain iterations");
  }

  private static List<Row> drainRows(Operator operator) throws Exception {
    List<Row> rows = new ArrayList<>();
    for (int steps = 0; steps < 1000; steps++) {
      operator.isBlocked().get(2, TimeUnit.SECONDS);
      if (!operator.hasNextWithTimer()) return rows;
      TsBlock block = operator.nextWithTimer();
      if (block == null) continue;
      for (int i = 0; i < block.getPositionCount(); i++) {
        rows.add(
            new Row(
                block.getColumn(0).getLong(i),
                block.getColumn(1).isNull(i) ? null : block.getColumn(1).getBinary(i).toString(),
                block.getColumn(2).getInt(i)));
      }
    }
    throw new AssertionError("root merge exceeded bounded drain iterations");
  }

  private static List<Row> independentExpectedRows(boolean withTags) {
    // Every tied sort key has the same payload, so equality never assumes an unspecified stable
    // tie.
    List<Row> expected = new ArrayList<>();
    String[] tags =
        withTags
            ? new String[] {null, "b", "a", null, "a", "b"}
            : new String[] {null, null, null, null, null, null};
    for (String tag : tags) {
      // Independent fixed oracle rows; do not derive values with the leaf's mapping logic.
      expected.add(new Row(1, tag, 70));
      expected.add(new Row(2, tag, -5));
      expected.add(new Row(2, tag, -5));
      expected.add(new Row(4, tag, 30));
    }
    return expected;
  }

  private static Comparator<Row> independentComparator(
      boolean withTags, boolean descendingTime, boolean nullsFirst, boolean descendingTag) {
    return (left, right) -> {
      if (withTags) {
        int comparison;
        if (left.tag() == null || right.tag() == null) {
          comparison =
              left.tag() == right.tag()
                  ? 0
                  : left.tag() == null ? (nullsFirst ? -1 : 1) : (nullsFirst ? 1 : -1);
        } else {
          comparison = left.tag().compareTo(right.tag());
          if (descendingTag) comparison = -comparison;
        }
        if (comparison != 0) return comparison;
      }
      return descendingTime
          ? Long.compare(right.time(), left.time())
          : Long.compare(left.time(), right.time());
    };
  }

  private static Map<Row, Integer> multiset(List<Row> rows) {
    Map<Row, Integer> counts = new HashMap<>();
    for (Row row : rows) counts.merge(row, 1, Integer::sum);
    return counts;
  }

  private static void cleanup(LocalExecutionPlanContext context, Operator root) throws Exception {
    try {
      if (root != null) {
        try {
          root.close();
        } finally {
          context.getResourceRegistry().releaseOperatorRoot(root);
        }
      }
    } finally {
      try {
        for (PipelineDriverFactory factory : context.getPipelineDriverFactories())
          factory.closeUncreated();
      } finally {
        context.getResourceRegistry().abortUntransferred(new RuntimeException("test cleanup"));
        FragmentInstanceId id = context.getFragmentInstanceId();
        MPPDataExchangeManager exchange =
            MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
        exchange.forceDeregisterFragmentInstance(id.toThrift());
        exchange.deRegisterFragmentInstanceFromMemoryPool(
            id.getQueryId().getId(), id.getFragmentInstanceId(), false);
        MemoryPool.FragmentReservationSnapshot memory =
            exchange
                .getLocalMemoryManager()
                .getQueryPool()
                .snapshotFragmentReservations(id.getQueryId().getId(), id.getFragmentInstanceId());
        assertFalse(memory.present());
        assertEquals(0L, memory.fragmentReservedBytes());
      }
    }
  }
}
