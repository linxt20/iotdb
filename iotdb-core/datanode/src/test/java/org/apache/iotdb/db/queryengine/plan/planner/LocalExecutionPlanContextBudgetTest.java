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
import org.apache.iotdb.calc.execution.operator.process.CollectOperator;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SinkListener;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SourceHandleListener;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.PipelineSourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.db.queryengine.execution.operator.process.RuntimeCompactOutputOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.statistics.snapshot.DynamicSnapshotRecord;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LocalExecutionPlanContextBudgetTest {
  private long previousBudget;
  private int previousBlockSize;
  private MemoryPool pool;
  private LocalExecutionPlanContext context;
  private final List<ExecutorService> runtimeExecutors = new ArrayList<>();

  @Before
  public void setUp() {
    previousBudget =
        IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance();
    previousBlockSize = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(1024);
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(64);
    pool = new MemoryPool("budget-test", new MemoryManager(4096), 1024);
    context = newContext();
  }

  @After
  public void tearDown() {
    if (context != null && context.isRuntimeParallelismEnabled()) {
      context.getResourceRegistry().abortUntransferred(new IllegalStateException("test cleanup"));
      for (RuntimeFrontierReservation reservation : context.getOwnedRuntimeFrontiers()) {
        reservation.requestClose();
      }
    }
    for (ExecutorService executor : runtimeExecutors) {
      executor.shutdownNow();
    }
    IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(previousBudget);
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(previousBlockSize);
  }

  private LocalExecutionPlanContext newContext() {
    FragmentInstanceContext instance =
        FragmentInstanceContext.createFragmentInstanceContext(
            new FragmentInstanceId(new PlanFragmentId(new QueryId("budget"), 0), "0"),
            mock(FragmentInstanceStateMachine.class),
            mock(MemoryReservationManager.class));
    LocalExecutionPlanContext result =
        new LocalExecutionPlanContext(new TypeProvider(), instance, (DataNodeQueryContext) null);
    result.setExchangeSumNum(4);
    return result;
  }

  private static PlanNode node(PlanNodeType type, PlanNode... children) {
    Class<? extends PlanNode> nodeClass;
    switch (type) {
      case IDENTITY_SINK:
        nodeClass =
            org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode.class;
        break;
      case TABLE_EXCHANGE_NODE:
        nodeClass = org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode.class;
        break;
      case EXCHANGE:
        nodeClass =
            org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ExchangeNode.class;
        break;
      case TABLE_COLLECT_NODE:
        nodeClass = CollectNode.class;
        break;
      case TABLE_TOPK_NODE:
        nodeClass = TopKNode.class;
        break;
      case TREE_COLLECT:
        nodeClass =
            org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.CollectNode.class;
        break;
      default:
        nodeClass = PlanNode.class;
        break;
    }
    PlanNode node = mock(nodeClass);
    when(node.getType()).thenReturn(type);
    when(node.getChildren()).thenReturn(Arrays.asList(children));
    return node;
  }

  private static PlanNode collect() {
    return new CollectNode(
        new PlanNodeId("collect"),
        Arrays.asList(
            node(PlanNodeType.TABLE_EXCHANGE_NODE), node(PlanNodeType.TABLE_EXCHANGE_NODE)),
        Collections.emptyList());
  }

  private CollectOperator generateTableCollect(PlanNode plan) {
    Operator first = mock(Operator.class);
    Operator second = mock(Operator.class);
    when(first.isBlocked()).thenReturn(SettableFuture.create());
    doReturn(Operator.NOT_BLOCKED).when(second).isBlocked();
    when(first.calculateMaxPeekMemoryWithCounter()).thenReturn(100L);
    when(first.calculateRetainedSizeAfterCallingNext()).thenReturn(30L);
    when(second.calculateMaxPeekMemoryWithCounter()).thenReturn(80L);
    when(second.calculateRetainedSizeAfterCallingNext()).thenReturn(50L);
    doReturn(first).when(plan.getChildren().get(0)).accept(any(), same(context));
    doReturn(second).when(plan.getChildren().get(1)).accept(any(), same(context));
    return (CollectOperator)
        plan.accept(new DataNodeTableOperatorGenerator(mock(Metadata.class)), context);
  }

  private static ISinkHandle sinkHandle() {
    ISinkHandle sink = mock(ISinkHandle.class);
    when(sink.getChannel(0))
        .thenReturn(
            mock(org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkChannel.class));
    return sink;
  }

  private static ExchangeOperator exchange(
      LocalExecutionPlanContext context, ISourceHandle source) {
    int operatorId = context.getNextOperatorId();
    PlanNodeId id = new PlanNodeId("budget_exchange_" + operatorId);
    return new ExchangeOperator(
        context.getDriverContext().addOperatorContext(operatorId, id, "ExchangeOperator"),
        source,
        id);
  }

  private ISourceHandle addSource(LocalExecutionPlanContext target) {
    SourceHandle handle = mock(SourceHandle.class);
    when(handle.getLocalFragmentInstanceId()).thenReturn(target.getFragmentInstanceId().toThrift());
    when(handle.getLocalPlanNodeId()).thenReturn("budget_source");
    ExchangeOperator operator = exchange(target, handle);
    target.addExchangeOperator(operator);
    return handle;
  }

  private void assertAdmissionIsShared(boolean shared) {
    assertAdmissionIsShared(shared, MemorySide.INPUT);
  }

  private void assertAdmissionIsShared(boolean shared, MemorySide side) {
    assertAdmissionIsShared(shared, side, shared ? 1024 : 256);
  }

  private void assertAdmissionIsShared(boolean shared, MemorySide side, long limit) {
    FragmentInstanceContext instance = context.getDriverContext().getFragmentInstanceContext();
    if (shared) {
      assertEquals(
          limit,
          side == MemorySide.INPUT ? instance.getInputMaxBytes() : instance.getOutputMaxBytes());
    } else {
      assertEquals(0, instance.getInputMaxBytes());
      assertEquals(0, instance.getOutputMaxBytes());
    }
    String fi = context.getFragmentInstanceId().getFragmentInstanceId();
    pool.registerPlanNodeIdToQueryMemoryMap("budget", fi, "a" + side, side);
    pool.registerPlanNodeIdToQueryMemoryMap("budget", fi, "b" + side, side);
    assertTrue(
        pool.reserveWithPriority("budget", fi, "a" + side, limit, limit, false).isReserveSuccess());
    MemoryPool.MemoryReservationResult second =
        pool.reserveWithPriority("budget", fi, "b" + side, limit, limit, false);
    if (shared) {
      assertFalse(second.getFuture().isDone());
      pool.free("budget", fi, "a" + side, limit);
      assertTrue(second.getFuture().isDone());
    } else {
      assertTrue(second.isReserveSuccess());
      pool.free("budget", fi, "a" + side, limit);
    }
    pool.free("budget", fi, "b" + side, limit);
  }

  @Test
  public void testRealSourceHandlesShareCanonicalFragmentAccountAndCleanup() throws Exception {
    FragmentInstanceId id = context.getFragmentInstanceId();
    String fi = FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(id.toThrift());
    LocalMemoryManager manager = mock(LocalMemoryManager.class);
    when(manager.getQueryPool()).thenReturn(pool);
    SourceHandle first = realSourceHandle(id, "first", manager);
    SourceHandle second = realSourceHandle(id, "second", manager);
    for (SourceHandle source : Arrays.asList(first, second)) {
      ExchangeOperator exchange = exchange(context, source);
      context.addExchangeOperator(exchange);
    }
    try {
      context.setMaxBytesOneHandleCanReserve();
      context.setExchangeMemoryLimits(collect(), pool);
      context.freezeResourceQuota();
      assertEquals(1024, context.getInstanceContext().getInputMaxBytes());
      first.isBlocked();
      second.isBlocked();
      first.updatePendingDataBlockInfo(0, List.of(1024L));
      second.updatePendingDataBlockInfo(0, List.of(1024L));
      assertEquals(1024, pool.getFragmentMemoryReservedBytes("budget", fi, MemorySide.INPUT));
      assertEquals(0, pool.getPlanNodeMemoryReservedBytes("budget", fi, "second"));
      assertEquals(1, pool.getMemoryReservationSize());
      first.abort();
      assertEquals(1024, pool.getPlanNodeMemoryReservedBytes("budget", fi, "second"));
    } finally {
      first.abort();
      second.abort();
      pool.deRegisterFragmentInstanceFromQueryMemoryMap("budget", fi, true);
    }
    assertEquals(0, pool.getMemoryReservationSize());
    assertEquals(0, pool.getReservedBytes());
    assertEquals(0, pool.getQueryMemoryReservationSize());
  }

  private static SourceHandle realSourceHandle(
      FragmentInstanceId id, String planNode, LocalMemoryManager manager) {
    return new SourceHandle(
        new TEndPoint("remote", 12345),
        new TFragmentInstanceId(id.getQueryId().getId(), 1, "0"),
        id.toThrift(),
        planNode,
        0,
        manager,
        mock(ExecutorService.class),
        new TsBlockSerde(),
        mock(SourceHandleListener.class),
        mock(IClientManager.class));
  }

  @Test
  public void testPureCollectUsesSharedSideBudgets() {
    ISourceHandle source = addSource(context);
    ISourceHandle secondSource = addSource(context);
    ISinkHandle sink = sinkHandle();
    context.setISink(sink);
    context.setExchangeMemoryLimits(node(PlanNodeType.IDENTITY_SINK, collect()), pool);
    verify(source).setMaxBytesCanReserve(512);
    verify(secondSource).setMaxBytesCanReserve(512);
    verify(sink).setMaxBytesCanReserve(512);
    assertAdmissionIsShared(true, MemorySide.INPUT, 512);
    assertAdmissionIsShared(true, MemorySide.OUTPUT, 512);
  }

  @Test
  public void testOddBudgetIsInitializedAfterBothDirectionsAreKnown() {
    IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(1025);
    pool = new MemoryPool("odd-budget", new MemoryManager(4096), 1025);
    context = newContext();
    ISourceHandle source = addSource(context);
    ISinkHandle sink = sinkHandle();
    context.setISink(sink);
    context.setExchangeMemoryLimits(node(PlanNodeType.IDENTITY_SINK, collect()), pool);
    verify(source).setMaxBytesCanReserve(512);
    verify(sink).setMaxBytesCanReserve(513);
    assertAdmissionIsShared(true, MemorySide.INPUT, 512);
    assertAdmissionIsShared(true, MemorySide.OUTPUT, 513);
  }

  @Test
  public void testInputOnlyCollectUsesFullBudgetEvenForOneFullBlock() {
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(1024);
    context.setExchangeSumNum(1);
    ISourceHandle source = addSource(context);
    context.setExchangeMemoryLimits(
        node(PlanNodeType.TABLE_COLLECT_NODE, node(PlanNodeType.TABLE_EXCHANGE_NODE)), pool);
    verify(source).setMaxBytesCanReserve(1024);
    assertEquals(0, context.getDriverContext().getFragmentInstanceContext().getOutputMaxBytes());
    assertAdmissionIsShared(true);
  }

  @Test
  public void testOutputOnlyCollectUsesFullBudgetEvenForOneFullBlock() {
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(1024);
    context.setExchangeSumNum(1);
    ISinkHandle sink = sinkHandle();
    context.setISink(sink);
    context.setExchangeMemoryLimits(
        node(
            PlanNodeType.IDENTITY_SINK,
            node(
                PlanNodeType.TABLE_COLLECT_NODE,
                node(PlanNodeType.SERIES_SCAN),
                node(PlanNodeType.SERIES_SCAN))),
        pool);
    verify(sink).setMaxBytesCanReserve(1024);
    assertEquals(0, context.getDriverContext().getFragmentInstanceContext().getInputMaxBytes());
    assertAdmissionIsShared(true, MemorySide.OUTPUT);
  }

  private void addLocalInput() {
    ExchangeOperator exchange = exchange(context, mock(LocalSourceHandle.class));
    context.addExchangeOperator(exchange);
  }

  @Test
  public void testLocalInputsDoNotSplitOutputBudget() {
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(1024);
    addLocalInput();
    ISinkHandle sink = sinkHandle();
    context.setISink(sink);
    context.setExchangeMemoryLimits(node(PlanNodeType.IDENTITY_SINK, collect()), pool);
    verify(sink).setMaxBytesCanReserve(1024);
    assertEquals(0, context.getDriverContext().getFragmentInstanceContext().getInputMaxBytes());
    assertAdmissionIsShared(true, MemorySide.OUTPUT);
  }

  @Test
  public void testMixedLocalAndRemoteInputsStillSplitBidirectionalBudget() {
    addLocalInput();
    ISourceHandle source = addSource(context);
    ISinkHandle sink = sinkHandle();
    context.setISink(sink);
    context.setExchangeMemoryLimits(node(PlanNodeType.IDENTITY_SINK, collect()), pool);
    verify(source).setMaxBytesCanReserve(512);
    verify(sink).setMaxBytesCanReserve(512);
    assertAdmissionIsShared(true, MemorySide.INPUT, 512);
    assertAdmissionIsShared(true, MemorySide.OUTPUT, 512);
  }

  @Test
  public void testLocalInputsWithoutSinkDoNotRegisterUnusedSharedBudget() {
    addLocalInput();
    context.setExchangeMemoryLimits(collect(), pool);
    assertAdmissionIsShared(false);
  }

  @Test
  public void testMappingCollectUsesSharedBudget() {
    addSource(context);
    PlanNode union =
        new UnionNode(
            new PlanNodeId("union"),
            Arrays.asList(
                node(PlanNodeType.TABLE_EXCHANGE_NODE), node(PlanNodeType.TABLE_EXCHANGE_NODE)),
            ArrayListMultimap.create(),
            Collections.emptyList());
    context.setExchangeMemoryLimits(union, pool);
    assertAdmissionIsShared(true);
  }

  @Test
  public void testTreePlanningRetainsLegacySourceQuota() {
    ISourceHandle source = addSource(context);
    context.setMaxBytesOneHandleCanReserve();
    context.freezeResourceQuota();
    verify(source).setMaxBytesCanReserve(256);
    assertFalse(
        SharedExchangeBudgetEligibility.isEligible(
            node(
                PlanNodeType.TREE_COLLECT,
                node(PlanNodeType.EXCHANGE),
                node(PlanNodeType.EXCHANGE))));
    assertAdmissionIsShared(false);
  }

  @Test
  public void testEligibleTableCollectEnablesReadyConsumptionAndSharedBudget() {
    PlanNode plan = collect();
    CollectOperator collect = generateTableCollect(plan);
    addSource(context);
    context.setExchangeMemoryLimits(plan, pool);
    assertTrue(collect.isBlocked().isDone());
    assertEquals(80, collect.calculateRetainedSizeAfterCallingNext());
    assertEquals(150, collect.calculateMaxPeekMemory());
    assertAdmissionIsShared(true);
  }

  @Test
  public void testEligibleTableUnionEnablesReadyConsumptionAndSharedBudget() {
    PlanNode union =
        new UnionNode(
            new PlanNodeId("union"),
            Arrays.asList(
                node(PlanNodeType.TABLE_EXCHANGE_NODE), node(PlanNodeType.TABLE_EXCHANGE_NODE)),
            ArrayListMultimap.create(),
            Collections.emptyList());
    CollectOperator collect = generateTableCollect(union);
    addSource(context);
    context.setExchangeMemoryLimits(union, pool);
    assertTrue(collect.isBlocked().isDone());
    assertEquals(80, collect.calculateRetainedSizeAfterCallingNext());
    assertEquals(150, collect.calculateMaxPeekMemory());
    assertAdmissionIsShared(true);
  }

  @Test
  public void testSingleChildTopKRetainsLegacyBudgetAndConsumption() {
    PlanNode child = collect();
    CollectOperator collect = generateTableCollect(child);
    ISourceHandle source = addSource(context);
    PlanNode topK = node(PlanNodeType.TABLE_TOPK_NODE, child);
    context.setExchangeMemoryLimits(topK, pool);
    verify(source).setMaxBytesCanReserve(256);
    assertTrue(collect.isBlocked().isDone());
    assertAdmissionIsShared(false);
    assertFalse(
        SharedExchangeBudgetEligibility.isEligible(
            node(PlanNodeType.TABLE_COLLECT_NODE, topK, collect())));
    assertTrue(
        SharedExchangeBudgetEligibility.isEligible(
            node(
                PlanNodeType.TABLE_COLLECT_NODE,
                node(PlanNodeType.TABLE_EXCHANGE_NODE, topK),
                node(PlanNodeType.TABLE_EXCHANGE_NODE))));
  }

  @Test
  public void testMultiplePipelinesKeepLegacyBudgetWithReadyCollectConsumption() {
    PlanNode plan = collect();
    CollectOperator collect = generateTableCollect(plan);
    LocalExecutionPlanContext child = context.createSubContext();
    child.addPipelineDriverFactory(mock(Operator.class), child.getDriverContext(), 0);
    addSource(context);
    context.setExchangeMemoryLimits(plan, pool);
    assertTrue(collect.isBlocked().isDone());
    assertAdmissionIsShared(false);
  }

  @Test
  public void testNestedDependencyDisablesSharingForEntireFragment() {
    for (PlanNodeType dependency :
        new PlanNodeType[] {
          PlanNodeType.TABLE_MERGESORT_NODE, PlanNodeType.MERGE_SORT, PlanNodeType.AGG_MERGE_SORT,
          PlanNodeType.TABLE_JOIN_NODE, PlanNodeType.TABLE_SEMI_JOIN_NODE,
              PlanNodeType.TABLE_TOPK_NODE,
          PlanNodeType.COUNT_MERGE, PlanNodeType.DEVICE_VIEW
        }) {
      PlanNode dependent =
          node(
              dependency,
              node(PlanNodeType.TABLE_EXCHANGE_NODE),
              node(PlanNodeType.TABLE_EXCHANGE_NODE));
      assertFalse(
          SharedExchangeBudgetEligibility.isEligible(
              node(PlanNodeType.TABLE_COLLECT_NODE, dependent, collect())));
      assertFalse(
          SharedExchangeBudgetEligibility.isEligible(node(dependency, collect(), collect())));
    }
    ISourceHandle source = addSource(context);
    ISinkHandle sink = sinkHandle();
    context.setExchangeSumNum(2);
    context.setISink(sink);
    context.setExchangeSumNum(4);
    context.setExchangeMemoryLimits(
        node(
            PlanNodeType.TABLE_COLLECT_NODE,
            collect(),
            node(
                PlanNodeType.TABLE_MERGESORT_NODE,
                node(PlanNodeType.TABLE_EXCHANGE_NODE),
                node(PlanNodeType.TABLE_EXCHANGE_NODE))),
        pool);
    verify(source).setMaxBytesCanReserve(256);
    // The legacy sink quota is captured at its original point in operator generation.
    verify(sink).setMaxBytesCanReserve(512);
    assertAdmissionIsShared(false);
  }

  @Test
  public void testSequentialAndUnknownMultiInputOperatorsDisableSharing() {
    assertFalse(
        SharedExchangeBudgetEligibility.isEligible(
            node(PlanNodeType.TABLE_COLLECT_NODE, collect(), collect())));
    assertFalse(
        SharedExchangeBudgetEligibility.isEligible(
            node(PlanNodeType.IDENTITY_SINK, collect(), collect())));
    assertFalse(
        SharedExchangeBudgetEligibility.isEligible(
            node(PlanNodeType.SCHEMA_QUERY_MERGE, collect(), collect())));
    for (PlanNodeType multiChildSetOperation :
        new PlanNodeType[] {PlanNodeType.TABLE_INTERSECT_NODE, PlanNodeType.TABLE_EXCEPT_NODE}) {
      assertFalse(
          SharedExchangeBudgetEligibility.isEligible(
              node(
                  multiChildSetOperation,
                  node(PlanNodeType.TABLE_EXCHANGE_NODE),
                  node(PlanNodeType.TABLE_EXCHANGE_NODE))));
    }
  }

  @Test
  public void testExchangeBoundaryDoesNotInspectUpstreamFragment() {
    PlanNode remote = node(PlanNodeType.TABLE_MERGESORT_NODE, collect(), collect());
    assertTrue(
        SharedExchangeBudgetEligibility.isEligible(
            node(
                PlanNodeType.TABLE_COLLECT_NODE,
                node(PlanNodeType.TABLE_EXCHANGE_NODE, remote),
                node(PlanNodeType.TABLE_EXCHANGE_NODE))));
  }

  @Test
  public void testSourceFragmentRootedAtExchangeKeepsLegacyBudget() {
    // The source fragment's root is an ExchangeNode; its upstream child belongs to another
    // fragment and must not make this fragment eligible for a shared budget.
    assertFalse(
        SharedExchangeBudgetEligibility.isEligible(
            node(
                PlanNodeType.TABLE_EXCHANGE_NODE,
                node(PlanNodeType.TABLE_COLLECT_NODE, collect(), collect()))));
    context.setExchangeMemoryLimits(
        node(
            PlanNodeType.TABLE_EXCHANGE_NODE,
            node(PlanNodeType.TABLE_COLLECT_NODE, collect(), collect())),
        pool);
    assertAdmissionIsShared(false);
  }

  @Test
  public void testNoCollectKeepsLegacyBudget() {
    context.setExchangeMemoryLimits(
        node(
            PlanNodeType.TABLE_MERGESORT_NODE,
            node(PlanNodeType.TABLE_EXCHANGE_NODE),
            node(PlanNodeType.TABLE_EXCHANGE_NODE)),
        pool);
    assertAdmissionIsShared(false);
  }

  @Test
  public void testInternalPipelineDisablesSharingForEntireFragment() {
    LocalExecutionPlanContext child = context.createSubContext();
    child.addPipelineDriverFactory(mock(Operator.class), child.getDriverContext(), 0);
    ISourceHandle source = addSource(child);
    PipelineSourceHandle pipelineSource = mock(PipelineSourceHandle.class);
    ExchangeOperator exchange = exchange(context, pipelineSource);
    context.addExchangeOperator(exchange);
    context.setExchangeMemoryLimits(collect(), pool);
    verify(source).setMaxBytesCanReserve(256);
    verify(pipelineSource).setMaxBytesCanReserve(256);
    assertAdmissionIsShared(false);
  }

  @Test
  public void testPipelineSchedulingDependencyDisablesSharing() {
    context.getDriverContext().setDependencyDriverIndex(0);
    context.setExchangeMemoryLimits(collect(), pool);
    assertAdmissionIsShared(false);
  }

  @Test
  public void testSideSmallerThanOneBlockKeepsLegacyBudget() {
    addSource(context);
    context.setISink(sinkHandle());
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(1024);
    context.setExchangeMemoryLimits(collect(), pool);
    assertAdmissionIsShared(false);
  }

  @Test
  public void testSinkOnlyFragmentCanStillReserveOneFullBlock() {
    IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(64);
    context = newContext();
    context.setExchangeSumNum(1);
    ISinkHandle sink = sinkHandle();
    context.setISink(sink);
    context.setExchangeMemoryLimits(
        node(PlanNodeType.IDENTITY_SINK, node(PlanNodeType.SERIES_SCAN)), pool);
    verify(sink).setMaxBytesCanReserve(64);
  }

  @Test
  public void testRuntimeBudgetIsSharedAndImmutableAcrossNestedContexts() {
    Map<String, Long> paid = new HashMap<>();
    paid.put("normal-scan", 512L);
    Set<String> approved = new HashSet<>();
    LocalExecutionPlanContext.RuntimeExchangeBudget budget =
        new LocalExecutionPlanContext.RuntimeExchangeBudget(
            256, 768, 128, 256, paid, approved, 4096);
    context.setDegreeOfParallelism(8);
    context.initializeRuntimeExchangeBudget(pool, budget);
    LocalExecutionPlanContext child = context.createSubContext();
    LocalExecutionPlanContext nested = child.createSubContext();
    paid.put("late-scan", 256L);
    approved.add("late-scan");
    IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(64);

    assertSame(budget, child.getRuntimeExchangeBudget());
    assertSame(budget, nested.getRuntimeExchangeBudget());
    assertEquals(Map.of("normal-scan", 512L), budget.groupPrepaidBudgets());
    assertTrue(budget.approvedRawScanPlanNodeIds().isEmpty());
    assertTrue(nested.isRuntimeScan("normal-scan"));
    assertFalse(nested.isApprovedRuntimeScan("normal-scan"));
    assertFalse(nested.isRuntimeScan("late-scan"));
    assertEquals(2, nested.getRuntimeMaxLogicalLanes("normal-scan"));
    expectRuntimeFailure(
        UnsupportedOperationException.class,
        () -> budget.groupPrepaidBudgets().put("mutation", 1L));
    expectRuntimeFailure(
        IllegalStateException.class, () -> child.initializeRuntimeExchangeBudget(pool, budget));
    assertSame(budget, context.getRuntimeExchangeBudget());
  }

  @Test
  public void testRuntimeInputAndOutputUseOneFrozenBudgetDespiteConfigAndExchangeCountChanges() {
    context.initializeRuntimeExchangeBudget(pool, runtimeBudget("normal-scan"));
    LocalExecutionPlanContext child = context.createSubContext();
    ISourceHandle remote = addSource(child);
    ISinkHandle sink = sinkHandle();
    child.setISink(sink);
    context.setExchangeSumNum(99);
    child.setExchangeSumNum(7);
    IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(64);

    child.setExchangeMemoryLimits(node(PlanNodeType.IDENTITY_SINK, collect()), pool);
    verify(remote).setMaxBytesCanReserve(256);
    verify(sink).setMaxBytesCanReserve(768);
    assertEquals(256, context.getInstanceContext().getInputMaxBytes());
    assertEquals(768, context.getInstanceContext().getOutputMaxBytes());
    assertAdmissionIsShared(true, MemorySide.INPUT, 256);
    assertAdmissionIsShared(true, MemorySide.OUTPUT, 768);
    expectRuntimeFailure(
        IllegalStateException.class,
        () ->
            context.setExchangeMemoryLimits(
                collect(), new MemoryPool("different-pool", new MemoryManager(4096), 1024)));
  }

  @Test
  public void testRuntimeBudgetRejectsOvercommitAndInvalidScopedApproval() {
    expectRuntimeFailure(
        IllegalArgumentException.class,
        () ->
            new LocalExecutionPlanContext.RuntimeExchangeBudget(
                -1, 768, 128, 256, Map.of("scan", 512L), Set.of(), 4096));
    expectRuntimeFailure(
        IllegalArgumentException.class,
        () ->
            new LocalExecutionPlanContext.RuntimeExchangeBudget(
                256, 768, 128, 256, Map.of("scan", 641L), Set.of(), 4096));
    expectRuntimeFailure(
        IllegalArgumentException.class,
        () ->
            new LocalExecutionPlanContext.RuntimeExchangeBudget(
                256, 768, 128, 256, Map.of("scan", 512L), Set.of("unpaid"), 4096));
    expectRuntimeFailure(
        IllegalArgumentException.class,
        () ->
            new LocalExecutionPlanContext.RuntimeExchangeBudget(
                256, 768, 128, 0, Map.of("scan", 512L), Set.of(), 4096));
    expectRuntimeFailure(
        ArithmeticException.class,
        () ->
            new LocalExecutionPlanContext.RuntimeExchangeBudget(
                0, Long.MAX_VALUE, 0, 256, Map.of("a", Long.MAX_VALUE, "b", 1L), Set.of(), 4096));

    LocalExecutionPlanContext.RuntimeExchangeBudget overcommitted =
        new LocalExecutionPlanContext.RuntimeExchangeBudget(
            512, 768, 128, 256, Map.of("scan", 512L), Set.of(), 4096);
    expectRuntimeFailure(
        IllegalStateException.class,
        () -> context.initializeRuntimeExchangeBudget(pool, overcommitted));
    assertFalse(context.isRuntimeParallelismEnabled());
    assertEquals(0, pool.getReservedBytes());
    assertEquals(0, context.getInstanceContext().getInputMaxBytes());
    assertEquals(0, context.getInstanceContext().getOutputMaxBytes());
  }

  @Test
  public void testRuntimeBudgetCannotInitializeAfterNativeFactoryCreation() {
    LocalExecutionPlanContext child = context.createSubContext();
    child.addPipelineDriverFactory(mock(Operator.class), child.getDriverContext(), 0);
    expectRuntimeFailure(
        IllegalStateException.class,
        () -> context.initializeRuntimeExchangeBudget(pool, runtimeBudget("scan")));
    assertFalse(context.isRuntimeParallelismEnabled());
    assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void testPaidNormalRuntimeScanDoesNotNeedScopedPropertyOverride() {
    String source = "paid-normal-scan";
    context.initializeRuntimeExchangeBudget(pool, runtimeBudget(source));
    context.setDegreeOfParallelism(8);
    RuntimeScanLayout layout = runtimeLayout(source, 2);
    context.registerRuntimeLayout(source, layout);
    assertTrue(context.isRuntimeScan(source));
    assertFalse(context.isApprovedRuntimeScan(source));
    assertSame(layout, context.getRuntimeLayout(layout.getLanePlan()));
    RuntimeScanGroupDescriptor.Builder builder = context.beginRuntimeScanGroup(source, layout);
    assertSame(builder.getReservation(), context.getOwnedRuntimeFrontiers().get(0));
    assertEquals(512, pool.getReservedBytes());
    assertEquals(2, context.getRuntimeMaxLogicalLanes(source));
    expectRuntimeFailure(
        IllegalStateException.class, () -> context.beginRuntimeScanGroup(source, layout));
    expectRuntimeFailure(
        IllegalStateException.class, () -> context.registerRuntimeLayout(source, layout));
    builder.abortBuild(new IllegalStateException("incomplete construction"));
    assertTrue(builder.getReservation().getClosedFuture().isDone());
    assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void testFrozenExecutionBindingsSurviveFeedbackMetadataEviction() {
    String source = "metadata-independent-scan";
    context.initializeRuntimeExchangeBudget(pool, runtimeBudget(source));
    RuntimeScanGroupDescriptor.Builder builder = beginRuntimeTestGroup(source, 2);
    RuntimeTestLane first = addRuntimeLane(builder, 0);
    RuntimeTestLane second = addRuntimeLane(builder, 1);
    builder.completeConstruction();
    LocalExecutionPlanContext.RuntimePlanBindings bindings =
        context.freezeRuntimeGroups(new ArrayList<>(context.getPipelineDriverFactories()));
    RuntimeScanGroupDescriptor descriptor = bindings.getGroups().get(0);
    assertNotNull(
        context
            .getDynamicScanAssignments()
            .assignmentsFor(first.factory.getDriverContext().getPipelineId()));

    // Exercise the real default metadata budget; dropping diagnostics must not own execution.
    String oversizedSource = "x".repeat((int) (DynamicSnapshotRecord.MAX_RECORD_BYTES / 2 + 1));
    ScanParallelPlan tooLarge = runtimeLayout(oversizedSource, 1).getLanePlan();
    context.getDynamicScanAssignments().registerBucket(100000, tooLarge, 0);
    assertEquals(
        "DYNAMIC_FEEDBACK_METADATA_BUDGET_EXCEEDED",
        context.getDynamicScanAssignments().getFeedbackDropReason());
    assertEquals(0, context.getDynamicScanAssignments().getRetainedMetadataBytes());
    assertNull(
        context
            .getDynamicScanAssignments()
            .assignmentsFor(first.factory.getDriverContext().getPipelineId()));
    assertSame(bindings, context.getRuntimePlanBindings());
    assertSame(descriptor, context.getRuntimeGroupDescriptors().get(0));
    assertSame(descriptor, bindings.groupForFactory(first.factory));
    assertSame(descriptor, bindings.groupForFactory(second.factory));
    assertSame(builder.getReservation(), descriptor.getReservation());
    assertSame(first.source, descriptor.getLanes().get(0).getSource());
    assertFalse(builder.getReservation().isCloseRequested());
    assertEquals(512, pool.getReservedBytes());
    expectRuntimeFailure(UnsupportedOperationException.class, () -> descriptor.getLanes().clear());
    expectRuntimeFailure(
        IllegalStateException.class,
        () -> context.freezeRuntimeGroups(new ArrayList<>(context.getPipelineDriverFactories())));
  }

  @Test
  public void testOriginalNativeFactoryIndicesRemainSeparateFromOwnConsumerClose()
      throws Exception {
    String source = "dependency-scan";
    context.initializeRuntimeExchangeBudget(pool, runtimeBudget(source));
    LocalExecutionPlanContext nested = context.createSubContext();
    PipelineDriverFactory nativeNested =
        nested.addPipelineDriverFactoryAndReturn(
            mock(Operator.class), nested.getDriverContext(), 0);
    SettableFuture<Void> nestedStartGate = SettableFuture.create();
    ExchangeOperator nestedConsumer = mock(ExchangeOperator.class);
    when(nestedConsumer.getBlockedDependencyDriver()).thenReturn(nestedStartGate);
    nativeNested.setDownstreamOperator(nestedConsumer);

    RuntimeScanGroupDescriptor.Builder builder = beginRuntimeTestGroup(source, 2);
    RuntimeTestLane first = addRuntimeLane(builder, 0);
    RuntimeTestLane second = addRuntimeLane(builder, 1);
    builder.completeConstruction();

    PipelineDriverFactory nativeRoot =
        context.addPipelineDriverFactoryAndReturn(
            mock(Operator.class), context.getDriverContext(), 0);
    SettableFuture<Void> rootStartGate = SettableFuture.create();
    ExchangeOperator rootConsumer = mock(ExchangeOperator.class);
    when(rootConsumer.getBlockedDependencyDriver()).thenReturn(rootStartGate);
    nativeRoot.setDownstreamOperator(rootConsumer);
    first.factory.setDependencyPipeline(0);
    second.factory.setDependencyPipeline(3);

    LocalExecutionPlanContext.RuntimePlanBindings bindings =
        context.freezeRuntimeGroups(new ArrayList<>(context.getPipelineDriverFactories()));
    RuntimeScanGroupDescriptor descriptor = bindings.getGroups().get(0);
    RuntimeScanGroupDescriptor.Lane firstLane = descriptor.getLanes().get(0);
    RuntimeScanGroupDescriptor.Lane secondLane = descriptor.getLanes().get(1);
    assertEquals(1, firstLane.getOriginalFactoryIndex());
    assertEquals(2, secondLane.getOriginalFactoryIndex());
    assertEquals(
        first.factory.getDriverContext().getPipelineId(), firstLane.getOriginalPipelineId());
    assertTrue(firstLane.getOriginalPipelineId() != firstLane.getOriginalFactoryIndex());
    assertEquals(0, nativeRoot.getDriverContext().getPipelineId());
    assertNull(bindings.groupForFactory(nativeRoot));
    assertNull(bindings.groupForFactory(nativeNested));
    assertTrue(bindings.startDependencyFor(nativeRoot).isDone());
    assertFalse(firstLane.getStartDependency().isDone());
    assertFalse(firstLane.getOwnConsumerClose().isDone());
    assertFalse(secondLane.getStartDependency().isDone());

    firstLane.getStartDependency().cancel(false);
    firstLane.getOwnConsumerClose().cancel(false);
    assertFalse(nestedStartGate.isCancelled());
    assertFalse(first.exchange.getBlockedDependencyDriver().isCancelled());
    assertFalse(bindings.startDependencyFor(first.factory).isDone());
    nestedStartGate.set(null);
    assertTrue(firstLane.getStartDependency().isDone());
    assertFalse(firstLane.getOwnConsumerClose().isDone());
    assertFalse(secondLane.getStartDependency().isDone());
    first.exchange.close();
    assertTrue(firstLane.getOwnConsumerClose().isDone());
    assertFalse(rootStartGate.isDone());
    assertFalse(secondLane.getOwnConsumerClose().isDone());
    rootStartGate.set(null);
    assertTrue(secondLane.getStartDependency().isDone());
  }

  @Test
  public void testRuntimeBindingRejectsReorderedNativeFactoryList() {
    context.initializeRuntimeExchangeBudget(pool, runtimeBudget("scan"));
    LocalExecutionPlanContext first = context.createSubContext();
    LocalExecutionPlanContext second = context.createSubContext();
    first.addPipelineDriverFactory(mock(Operator.class), first.getDriverContext(), 0);
    second.addPipelineDriverFactory(mock(Operator.class), second.getDriverContext(), 0);
    List<PipelineDriverFactory> reordered = new ArrayList<>(context.getPipelineDriverFactories());
    Collections.reverse(reordered);
    expectRuntimeFailure(IllegalStateException.class, () -> context.freezeRuntimeGroups(reordered));
  }

  @Test
  public void testRuntimeBindingRejectsInvalidNativeDependencyTargets() {
    for (int dependency : new int[] {-2, 0, 2}) {
      LocalExecutionPlanContext candidate = newContext();
      candidate.initializeRuntimeExchangeBudget(pool, runtimeBudget("scan"));
      LocalExecutionPlanContext child = candidate.createSubContext();
      PipelineDriverFactory factory =
          child.addPipelineDriverFactoryAndReturn(
              mock(Operator.class), child.getDriverContext(), 0);
      factory.setDependencyPipeline(dependency);
      expectRuntimeFailure(
          IllegalStateException.class,
          () ->
              candidate.freezeRuntimeGroups(
                  new ArrayList<>(candidate.getPipelineDriverFactories())));
      candidate.getResourceRegistry().abortUntransferred(new IllegalStateException("cleanup"));
    }

    LocalExecutionPlanContext candidate = newContext();
    candidate.initializeRuntimeExchangeBudget(pool, runtimeBudget("scan"));
    LocalExecutionPlanContext first = candidate.createSubContext();
    LocalExecutionPlanContext second = candidate.createSubContext();
    PipelineDriverFactory factory =
        first.addPipelineDriverFactoryAndReturn(mock(Operator.class), first.getDriverContext(), 0);
    second.addPipelineDriverFactory(mock(Operator.class), second.getDriverContext(), 0);
    factory.setDependencyPipeline(1);
    expectRuntimeFailure(
        IllegalStateException.class,
        () ->
            candidate.freezeRuntimeGroups(new ArrayList<>(candidate.getPipelineDriverFactories())));
    candidate.getResourceRegistry().abortUntransferred(new IllegalStateException("cleanup"));
  }

  @Test
  public void testRuntimeBindingRejectsNativeDependencyCycle() {
    context.initializeRuntimeExchangeBudget(pool, runtimeBudget("scan"));
    LocalExecutionPlanContext first = context.createSubContext();
    LocalExecutionPlanContext second = context.createSubContext();
    PipelineDriverFactory a =
        first.addPipelineDriverFactoryAndReturn(mock(Operator.class), first.getDriverContext(), 0);
    PipelineDriverFactory b =
        second.addPipelineDriverFactoryAndReturn(
            mock(Operator.class), second.getDriverContext(), 0);
    ExchangeOperator aConsumer = mock(ExchangeOperator.class);
    ExchangeOperator bConsumer = mock(ExchangeOperator.class);
    when(aConsumer.getBlockedDependencyDriver()).thenReturn(SettableFuture.create());
    when(bConsumer.getBlockedDependencyDriver()).thenReturn(SettableFuture.create());
    a.setDownstreamOperator(aConsumer);
    b.setDownstreamOperator(bConsumer);
    a.setDependencyPipeline(1);
    b.setDependencyPipeline(0);
    expectRuntimeFailure(
        IllegalStateException.class,
        () -> context.freezeRuntimeGroups(new ArrayList<>(context.getPipelineDriverFactories())));
  }

  @Test
  public void testPartialRuntimeBuildAbortReleasesPaidCreditAndRegistryClosesRootOnce()
      throws Exception {
    context.initializeRuntimeExchangeBudget(pool, runtimeBudget("failed-scan"));
    RuntimeScanGroupDescriptor.Builder builder = beginRuntimeTestGroup("failed-scan", 2);
    RuntimeTestLane first = addRuntimeLane(builder, 0);
    first.source.isBlocked();
    TsBlockBuilder blockBuilder =
        new TsBlockBuilder(1, Collections.singletonList(TSDataType.INT64));
    blockBuilder.getTimeColumnBuilder().writeLong(1);
    blockBuilder.getColumnBuilder(0).writeLong(2);
    blockBuilder.declarePosition();
    TsBlock block = blockBuilder.build();
    assertTrue(block.getRetainedSizeInBytes() <= 256);
    ListenableFuture<Void> writable = first.queue.add(block);
    assertEquals(1, builder.getReservation().getLeasedCreditCount());
    IllegalStateException failure = new IllegalStateException("second lane construction failure");

    builder.abortBuild(failure);
    builder.abortBuild(failure);
    assertTrue(writable.isDone());
    assertEquals(0, builder.getReservation().getLeasedCreditCount());
    assertTrue(builder.getReservation().getClosedFuture().isDone());
    assertEquals(0, pool.getReservedBytes());
    verify(first.rawRoot, never()).close();
    expectRuntimeFailure(
        IllegalStateException.class,
        () -> context.freezeRuntimeGroups(new ArrayList<>(context.getPipelineDriverFactories())));

    context.getResourceRegistry().abortUntransferred(failure);
    context.getResourceRegistry().abortUntransferred(failure);
    first.factory.closeUncreated();
    first.factory.closeUncreated();
    verify(first.rawRoot, times(1)).close();
    assertEquals(0, pool.getReservedBytes());
  }

  private LocalExecutionPlanContext.RuntimeExchangeBudget runtimeBudget(String source) {
    return new LocalExecutionPlanContext.RuntimeExchangeBudget(
        256, 768, 128, 256, Map.of(source, 512L), Set.of(), 4096);
  }

  private RuntimeScanGroupDescriptor.Builder beginRuntimeTestGroup(String source, int lanes) {
    context.setDegreeOfParallelism(Math.max(context.getDegreeOfParallelism(), lanes));
    RuntimeScanLayout layout = runtimeLayout(source, lanes);
    context.registerRuntimeLayout(source, layout);
    return context.beginRuntimeScanGroup(source, layout);
  }

  private RuntimeScanLayout runtimeLayout(String source, int laneCount) {
    List<ScanParallelPlan.WorkUnit> units = new ArrayList<>();
    List<ScanParallelPlan.Bucket> buckets = new ArrayList<>();
    List<List<String>> devices = new ArrayList<>();
    for (int lane = 0; lane < laneCount; lane++) {
      List<String> group = Collections.singletonList("device-" + lane);
      ScanParallelPlan.WorkUnit unit =
          new ScanParallelPlan.WorkUnit(
              new ScanParallelPlan.SourceKey(
                  "budget", "budget.0", source, "DEVICE", "all", Integer.toString(lane)),
              group,
              Collections.emptyList(),
              1,
              true);
      units.add(unit);
      devices.add(group);
      buckets.add(new ScanParallelPlan.Bucket(lane, Collections.singletonList(unit), 1));
    }
    return new RuntimeScanLayout(
        new ScanParallelPlan(
            ScanParallelPlan.Mode.DEVICE,
            ScanParallelPlan.FallbackReason.NONE,
            Math.max(2, laneCount),
            laneCount,
            devices,
            Collections.emptyList(),
            units,
            buckets),
        1);
  }

  private RuntimeTestLane addRuntimeLane(RuntimeScanGroupDescriptor.Builder builder, int lane) {
    LocalExecutionPlanContext child = context.createSubContext();
    Operator raw = mock(Operator.class);
    Operator root = new RuntimeCompactOutputOperator(raw, 256, 4096);
    PipelineDriverFactory factory =
        child.addPipelineDriverFactoryAndReturn(root, child.getDriverContext(), 0);
    child
        .getDynamicScanAssignments()
        .registerBucket(
            child.getDriverContext().getPipelineId(), builder.getLayout().getLanePlan(), lane);
    LocalMemoryManager manager = mock(LocalMemoryManager.class);
    when(manager.getQueryPool()).thenReturn(pool);
    ExecutorService notifications = newDirectExecutorService();
    runtimeExecutors.add(notifications);
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            context.getFragmentInstanceId().toThrift(),
            "runtime-lane-" + lane,
            manager,
            notifications,
            false,
            builder.getReservation(),
            lane);
    builder.captureQueue(lane, queue);
    LocalSinkChannel sink =
        new LocalSinkChannel(
            context.getFragmentInstanceId().toThrift(), queue, mock(SinkListener.class));
    LocalSourceHandle source =
        new LocalSourceHandle(
            context.getFragmentInstanceId().toThrift(),
            "runtime-parent-" + lane,
            queue,
            mock(SourceHandleListener.class));
    ExchangeOperator exchange = exchange(context, source);
    factory.setDownstreamOperator(exchange);
    context.getResourceRegistry().registerQueue(queue);
    context.getResourceRegistry().registerSink(sink);
    context.getResourceRegistry().registerSourceHandle(source, queue.getMemoryReservationKey());
    context.registerRuntimeLane(builder, lane, factory, exchange, sink, source);
    return new RuntimeTestLane(raw, factory, queue, source, exchange);
  }

  private static void expectRuntimeFailure(
      Class<? extends Throwable> expected, Runnable operation) {
    try {
      operation.run();
      fail("Expected " + expected.getSimpleName());
    } catch (Throwable failure) {
      if (!expected.isInstance(failure)) {
        throw new AssertionError(
            "Unexpected failure instead of " + expected.getSimpleName(), failure);
      }
    }
  }

  private static final class RuntimeTestLane {
    private final Operator rawRoot;
    private final PipelineDriverFactory factory;
    private final SharedTsBlockQueue queue;
    private final LocalSourceHandle source;
    private final ExchangeOperator exchange;

    private RuntimeTestLane(
        Operator rawRoot,
        PipelineDriverFactory factory,
        SharedTsBlockQueue queue,
        LocalSourceHandle source,
        ExchangeOperator exchange) {
      this.rawRoot = rawRoot;
      this.factory = factory;
      this.queue = queue;
      this.source = source;
      this.exchange = exchange;
    }
  }
}
