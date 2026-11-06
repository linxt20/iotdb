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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.execution.operator.process.CollectOperator;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.ResourceAuditManager;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.source.PipelineSourceHandle;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.operator.source.DataSourceOperator;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanContext;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionResourceRegistry;
import org.apache.iotdb.db.queryengine.plan.planner.LocalScanPipelineBuilder;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.storageengine.dataregion.IDataRegionForQuery;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Only the failing constructor boundary is replaced. The public manager, planner, common builder,
 * queues, surviving endpoints, factories and fragment accounting all execute their production code.
 */
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest(MPPDataExchangeManager.class)
public class LocalScanPipelineFailureIntegrationTest {
  private enum FailurePoint {
    BEFORE_QUEUE,
    SINK_CONSTRUCTOR,
    SOURCE_CONSTRUCTOR
  }

  @BeforeClass
  public static void setUpNodeId() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
  }

  @Test(timeout = 15000)
  public void queueBeforeFailureCleansEarlierPipelines() throws Exception {
    assertProductionBuilderFailure(FailurePoint.BEFORE_QUEUE);
  }

  @Test(timeout = 15000)
  public void sinkConstructionFailureAbortsPublishedQueue() throws Exception {
    assertProductionBuilderFailure(FailurePoint.SINK_CONSTRUCTOR);
  }

  @Test(timeout = 15000)
  public void sourceConstructionFailureClosesSinkAndFactory() throws Exception {
    assertProductionBuilderFailure(FailurePoint.SOURCE_CONSTRUCTOR);
  }

  @SuppressWarnings("unchecked")
  private void assertProductionBuilderFailure(FailurePoint point) throws Exception {
    RuntimeException primary = new IllegalArgumentException("injected-" + point);
    if (point == FailurePoint.SINK_CONSTRUCTOR) {
      PowerMockito.whenNew(LocalSinkChannel.class).withAnyArguments().thenThrow(primary);
    } else if (point == FailurePoint.SOURCE_CONSTRUCTOR) {
      PowerMockito.whenNew(PipelineSourceHandle.class).withAnyArguments().thenThrow(primary);
    }
    FragmentInstanceId id =
        new FragmentInstanceId(
            new PlanFragmentId(
                new QueryId("builder_failure_" + point.name().toLowerCase(java.util.Locale.ROOT)),
                0),
            "0");
    FragmentInstanceManager manager = FragmentInstanceManager.getInstance();
    MPPDataExchangeManager exchange =
        MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
    // Verify the singleton graph did not split across the PowerMock and ordinary application
    // loaders.
    Field builderExchange =
        LocalScanPipelineBuilder.class.getDeclaredField("MPP_DATA_EXCHANGE_MANAGER");
    builderExchange.setAccessible(true);
    assertSame(exchange, builderExchange.get(null));
    Field managerExchange = FragmentInstanceManager.class.getDeclaredField("exchangeManager");
    managerExchange.setAccessible(true);
    assertSame(exchange, managerExchange.get(manager));

    MemoryPool pool = exchange.getLocalMemoryManager().getQueryPool();
    ResourceAuditManager audit = ResourceAuditManager.getInstance();
    String window = audit.openWindow(30_000, pool.getRemainingBytes(), 2048);
    AtomicReference<LocalExecutionPlanContext> planning = new AtomicReference<>();
    List<Operator> roots = new ArrayList<>();
    PlanNode plan = mock(PlanNode.class);
    when(plan.getPlanNodeId()).thenReturn(new PlanNodeId("builder-parent"));
    when(plan.getType()).thenReturn(PlanNodeType.SERIES_SCAN);
    doAnswer(
            invocation -> {
              LocalExecutionPlanContext context = invocation.getArgument(1);
              planning.set(context);
              LocalScanPipelineBuilder.build(
                  context,
                  point == FailurePoint.BEFORE_QUEUE ? 2 : 1,
                  (index, subContext) -> {
                    if (point == FailurePoint.BEFORE_QUEUE && index == 1) throw primary;
                    DataSourceOperator root = mock(DataSourceOperator.class);
                    roots.add(root);
                    ((DataDriverContext) subContext.getDriverContext()).addSourceOperator(root);
                    PlanNodeId scanId = new PlanNodeId("scan-" + index);
                    PlanNode scan = mock(PlanNode.class);
                    when(scan.getPlanNodeId()).thenReturn(scanId);
                    when(scan.getType()).thenReturn(PlanNodeType.SERIES_SCAN);
                    return new LocalScanPipelineBuilder.PipelineSpec(
                        root, scan, plan.getPlanNodeId(), scanId);
                  });
              throw new AssertionError("construction should have failed at " + point);
            })
        .when(plan)
        .accept(any(), any());
    PlanFragment fragment = mock(PlanFragment.class);
    when(fragment.getPlanNodeTree()).thenReturn(plan);
    when(fragment.getTypeProvider()).thenReturn(new TypeProvider());
    FragmentInstance instance = mock(FragmentInstance.class);
    when(instance.getId()).thenReturn(id);
    when(instance.getFragment()).thenReturn(fragment);
    when(instance.getDataNodeFINum()).thenReturn(1);
    when(instance.getTimeOut()).thenReturn(10_000L);
    try {
      FragmentInstanceInfo info =
          manager.execDataQueryFragmentInstance(instance, mock(IDataRegionForQuery.class));
      assertNotNull(info);
      assertTrue(info.getState().isFailed());
      assertEquals(primary.getMessage(), info.getMessage());
      assertNotNull(planning.get());
      assertNull(planning.get().getInstanceContext().getPlanningContext());
      assertEquals(1, roots.size());
      verify(roots.get(0), times(1)).close();
      assertEquals(
          point == FailurePoint.SINK_CONSTRUCTOR ? 0 : 1,
          planning.get().getPipelineDriverFactories().size());

      Field entriesField = LocalExecutionResourceRegistry.class.getDeclaredField("entries");
      entriesField.setAccessible(true);
      Map<Object, ?> entries =
          (Map<Object, ?>) entriesField.get(planning.get().getResourceRegistry());
      assertEquals(1, entries.size());
      for (Object object : entries.keySet()) {
        assertTrue(object instanceof SharedTsBlockQueue);
        SharedTsBlockQueue queue = (SharedTsBlockQueue) object;
        assertSame(SharedTsBlockQueue.class.getClassLoader(), queue.getClass().getClassLoader());
        assertTrue(queue.isClosed());
        assertEquals(0L, queue.getBufferRetainedSizeInBytes());
        assertTrue(queue.getCanAddTsBlock().isDone());
        assertEquals(id.getQueryId().getId(), queue.getMemoryReservationKey().queryId());
        assertEquals(
            id.getFragmentInstanceId(), queue.getMemoryReservationKey().fragmentInstanceId());
      }
      assertEquals(
          "LOCAL_QUEUE", planning.get().getResourceRegistry().snapshotEntries().get(0).kind());
      MemoryPool.FragmentReservationSnapshot memory =
          pool.snapshotFragmentReservations(id.getQueryId().getId(), id.getFragmentInstanceId());
      assertFalse(memory.present());
      assertEquals(0L, memory.fragmentReservedBytes());
      Map<String, Object> observation =
          audit.snapshot(window, id.getQueryId().getId(), id.getFullId(), pool, 2048);
      List<Map<String, Object>> objects = (List<Map<String, Object>>) observation.get("objects");
      assertEquals(
          point == FailurePoint.SINK_CONSTRUCTOR ? 0L : 1L,
          objects.stream().filter(object -> "SINK".equals(object.get("kind"))).count());
      assertEquals(
          point == FailurePoint.BEFORE_QUEUE ? 1L : 0L,
          objects.stream().filter(object -> "SOURCE_HANDLE".equals(object.get("kind"))).count());
      assertEquals(
          objects.size(), objects.stream().map(object -> object.get("token")).distinct().count());
      for (Map<String, Object> object : objects) assertEquals(true, object.get("terminal"));
      assertEquals(true, observation.get("complete"));
      assertEquals(0, observation.get("createdDrivers"));
      assertEquals(0, observation.get("liveDrivers"));
      assertEquals(0, observation.get("unconsumedFactories"));
      assertEquals(0, observation.get("liveEndpoints"));
      assertEquals(0, observation.get("liveQueues"));
      assertEquals(0, observation.get("pendingFutures"));
      assertEquals(true, observation.get("released"));
      org.awaitility.Awaitility.await()
          .atMost(10, java.util.concurrent.TimeUnit.SECONDS)
          .until(() -> !FragmentInstanceManager.hasDataNodeQueryContext(id.getQueryId()));
    } finally {
      audit.closeWindow(window);
      manager.cancelQuery(
          id.getQueryId(),
          java.util.Collections.singletonList(id),
          true,
          FragmentInstanceManager.CancellationOrigin.LOCAL_CANCEL_TASK);
    }
  }

  @Test(timeout = 15000)
  public void compositeRootIsClosedWhenQueueConstructionFails() throws Exception {
    RuntimeException primary = new IllegalArgumentException("injected-before-composite-queue");
    // The call site is MPPDataExchangeManager, already isolated by PrepareForTest above.
    PowerMockito.whenNew(SharedTsBlockQueue.class).withAnyArguments().thenThrow(primary);
    FragmentInstanceId id =
        new FragmentInstanceId(
            new PlanFragmentId(new QueryId("builder_composite_before_queue"), 0), "0");
    FragmentInstanceManager manager = FragmentInstanceManager.getInstance();
    MPPDataExchangeManager exchange =
        MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
    Field builderExchange =
        LocalScanPipelineBuilder.class.getDeclaredField("MPP_DATA_EXCHANGE_MANAGER");
    builderExchange.setAccessible(true);
    assertSame(exchange, builderExchange.get(null));
    MemoryPool pool = exchange.getLocalMemoryManager().getQueryPool();
    ResourceAuditManager audit = ResourceAuditManager.getInstance();
    String window = audit.openWindow(30_000, pool.getRemainingBytes(), 2048);
    AtomicReference<LocalExecutionPlanContext> planning = new AtomicReference<>();
    AtomicReference<CollectOperator> composite = new AtomicReference<>();
    AtomicInteger compositeCloseCalls = new AtomicInteger();
    DataSourceOperator child = mock(DataSourceOperator.class);
    PlanNode plan = mock(PlanNode.class);
    when(plan.getPlanNodeId()).thenReturn(new PlanNodeId("composite-parent"));
    when(plan.getType()).thenReturn(PlanNodeType.SERIES_SCAN);
    doAnswer(
            invocation -> {
              LocalExecutionPlanContext context = invocation.getArgument(1);
              planning.set(context);
              LocalScanPipelineBuilder.build(
                  context,
                  1,
                  (index, subContext) -> {
                    ((DataDriverContext) subContext.getDriverContext()).addSourceOperator(child);
                    PlanNodeId rootId = new PlanNodeId("composite-scan-root");
                    CollectOperator root =
                        spy(
                            new CollectOperator(
                                subContext
                                    .getDriverContext()
                                    .addOperatorContext(
                                        subContext.getNextOperatorId(),
                                        rootId,
                                        CollectOperator.class.getSimpleName()),
                                new java.util.ArrayList<>(
                                    java.util.Collections.singletonList(child))));
                    try {
                      doAnswer(
                              close -> {
                                compositeCloseCalls.incrementAndGet();
                                return close.callRealMethod();
                              })
                          .when(root)
                          .close();
                    } catch (Exception setupFailure) {
                      throw new AssertionError(
                          "failed to attach the real composite close observer", setupFailure);
                    }
                    composite.set(root);
                    return new LocalScanPipelineBuilder.PipelineSpec(
                        root, plan, plan.getPlanNodeId(), rootId);
                  });
              throw new AssertionError("queue construction must throw the original failure");
            })
        .when(plan)
        .accept(any(), any());
    PlanFragment fragment = mock(PlanFragment.class);
    when(fragment.getPlanNodeTree()).thenReturn(plan);
    when(fragment.getTypeProvider()).thenReturn(new TypeProvider());
    FragmentInstance instance = mock(FragmentInstance.class);
    when(instance.getId()).thenReturn(id);
    when(instance.getFragment()).thenReturn(fragment);
    when(instance.getDataNodeFINum()).thenReturn(1);
    when(instance.getTimeOut()).thenReturn(10_000L);
    try {
      FragmentInstanceInfo info =
          manager.execDataQueryFragmentInstance(instance, mock(IDataRegionForQuery.class));
      assertNotNull(info);
      assertTrue(info.getState().isFailed());
      assertEquals(primary.getMessage(), info.getMessage());
      assertNotNull(planning.get());
      assertNotNull(composite.get());
      assertNull(planning.get().getInstanceContext().getPlanningContext());
      assertTrue(planning.get().getPipelineDriverFactories().isEmpty());
      assertEquals(0, planning.get().getResourceRegistry().getObjectCount());
      assertThrows(
          IllegalStateException.class,
          () -> planning.get().getResourceRegistry().registerOperatorRoot(mock(Operator.class)));
      MemoryPool.FragmentReservationSnapshot memory =
          pool.snapshotFragmentReservations(id.getQueryId().getId(), id.getFragmentInstanceId());
      assertFalse(memory.present());
      assertEquals(0L, memory.fragmentReservedBytes());
      Map<String, Object> observation =
          audit.snapshot(window, id.getQueryId().getId(), id.getFullId(), pool, 2048);
      assertEquals(true, observation.get("complete"));
      assertEquals(0, observation.get("createdDrivers"));
      assertEquals(0, observation.get("unconsumedFactories"));
      assertEquals(0, observation.get("liveEndpoints"));
      assertEquals(0, observation.get("liveQueues"));
      assertEquals(0, observation.get("pendingFutures"));
      assertEquals(true, observation.get("released"));
      org.awaitility.Awaitility.await()
          .atMost(10, java.util.concurrent.TimeUnit.SECONDS)
          .until(() -> !FragmentInstanceManager.hasDataNodeQueryContext(id.getQueryId()));
      verify(child, times(1)).close();
      // Without immediate ownership adoption after PipelineSpec, the real manager closes the
      // registered child but loses this newly built composite root before any queue/factory exists.
      assertEquals(
          "manager must close the returned composite before queue construction",
          1,
          compositeCloseCalls.get());
      verify(composite.get(), times(1)).close();
    } finally {
      try {
        // If the regression is present, close the orphan only after its missing manager cleanup
        // has failed the assertions. This teardown must not turn the production assertion green.
        if (composite.get() != null && compositeCloseCalls.get() == 0) composite.get().close();
      } finally {
        audit.closeWindow(window);
        manager.cancelQuery(
            id.getQueryId(),
            java.util.Collections.singletonList(id),
            true,
            FragmentInstanceManager.CancellationOrigin.LOCAL_CANCEL_TASK);
      }
    }
  }
}
