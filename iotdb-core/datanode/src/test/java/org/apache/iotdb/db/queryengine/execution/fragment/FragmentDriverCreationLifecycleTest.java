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
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanContext;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionResourceRegistry;
import org.apache.iotdb.db.queryengine.plan.planner.PipelineDriverFactory;

import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class FragmentDriverCreationLifecycleTest {
  @Test
  public void laterFactoryFailureClosesCreatedDriversAndUnconsumedSlots() throws Exception {
    FragmentInstanceContext context = context();
    LocalExecutionPlanContext planning = mock(LocalExecutionPlanContext.class);
    LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
    Operator orphan = mock(Operator.class);
    registry.registerOperatorRoot(orphan);
    when(planning.getResourceRegistry()).thenReturn(registry);
    when(context.getPlanningContext()).thenReturn(planning);
    PipelineDriverFactory a = mock(PipelineDriverFactory.class),
        b = mock(PipelineDriverFactory.class),
        c = mock(PipelineDriverFactory.class);
    org.apache.iotdb.db.queryengine.execution.driver.Driver driver =
        mock(org.apache.iotdb.db.queryengine.execution.driver.Driver.class);
    when(a.createDriver()).thenReturn(driver);
    IllegalArgumentException primary = new IllegalArgumentException("construction failure");
    when(b.createDriver()).thenThrow(primary);
    Exception cleanup = new Exception("cleanup failure");
    when(c.closeUncreated()).thenThrow(cleanup);
    List<PipelineDriverFactory> factories = Arrays.asList(a, b, c);
    List<IDriver> drivers = new ArrayList<>();
    int[] count = {0};
    try {
      create(factories, drivers, count, context);
      fail("expected construction failure");
    } catch (InvocationTargetException e) {
      assertSame(primary, e.getCause());
    }
    cleanup(factories, drivers, count[0], context, primary);
    assertEquals(1, count[0]);
    verify(driver).close();
    verify(b).closeUncreated();
    verify(c).closeUncreated();
    verify(context, times(2)).decrementNumOfUnClosedDriver();
    verify(orphan).close();
    verify(context).setPlanningContext(null);
    assertArrayEquals(new Throwable[] {cleanup}, primary.getSuppressed());
  }

  @Test
  public void cancellationAfterConstructionStopsTransferAndClosesCreatedDriver() throws Exception {
    FragmentInstanceContext context = context();
    AtomicBoolean cancelled = new AtomicBoolean();
    when(context.isCancellationRequested()).thenAnswer(x -> cancelled.get());
    CountDownLatch constructed = new CountDownLatch(1), release = new CountDownLatch(1);
    org.apache.iotdb.db.queryengine.execution.driver.Driver driver =
        mock(org.apache.iotdb.db.queryengine.execution.driver.Driver.class);
    PipelineDriverFactory a = mock(PipelineDriverFactory.class),
        b = mock(PipelineDriverFactory.class);
    when(a.createDriver())
        .thenAnswer(
            x -> {
              constructed.countDown();
              if (!release.await(10, TimeUnit.SECONDS))
                throw new AssertionError("construction timeout");
              return driver;
            });
    List<PipelineDriverFactory> factories = Arrays.asList(a, b);
    List<IDriver> drivers = new ArrayList<>();
    int[] count = {0};
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Throwable primary = null;
    try {
      Future<Throwable> result =
          executor.submit(
              () -> {
                try {
                  create(factories, drivers, count, context);
                  return null;
                } catch (InvocationTargetException e) {
                  return e.getCause();
                }
              });
      assertTrue(constructed.await(10, TimeUnit.SECONDS));
      cancelled.set(true);
      release.countDown();
      primary = result.get(10, TimeUnit.SECONDS);
      assertTrue(primary instanceof CancellationException);
      cleanup(factories, drivers, count[0], context, primary);
      verify(driver).close();
      verify(b, never()).createDriver();
      verify(b).closeUncreated();
      verify(context).decrementNumOfUnClosedDriver();
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  private static FragmentInstanceContext context() {
    FragmentInstanceContext context = mock(FragmentInstanceContext.class);
    FragmentInstanceStateMachine state = mock(FragmentInstanceStateMachine.class);
    when(state.getState()).thenReturn(FragmentInstanceState.RUNNING);
    when(context.getStateMachine()).thenReturn(state);
    return context;
  }

  private static void create(
      List<PipelineDriverFactory> factories,
      List<IDriver> drivers,
      int[] count,
      FragmentInstanceContext context)
      throws Exception {
    Method method =
        FragmentInstanceManager.class.getDeclaredMethod(
            "createDrivers", List.class, List.class, int[].class, FragmentInstanceContext.class);
    method.setAccessible(true);
    method.invoke(FragmentInstanceManager.getInstance(), factories, drivers, count, context);
  }

  private static void cleanup(
      List<PipelineDriverFactory> factories,
      List<IDriver> drivers,
      int count,
      FragmentInstanceContext context,
      Throwable primary)
      throws Exception {
    Method method =
        FragmentInstanceManager.class.getDeclaredMethod(
            "cleanupDriverCreationFailure",
            List.class,
            List.class,
            int.class,
            FragmentInstanceContext.class,
            Throwable.class);
    method.setAccessible(true);
    method.invoke(
        FragmentInstanceManager.getInstance(), factories, drivers, count, context, primary);
  }

  @Test
  public void cancellationBeforeSchedulerHandoffPreventsSubmission() throws Exception {
    FragmentInstanceContext context = context();
    when(context.isCancellationRequested()).thenReturn(true);
    IDriver driver = mock(IDriver.class);
    when(driver.getDriverContext())
        .thenReturn(new org.apache.iotdb.db.queryengine.execution.driver.DriverContext());
    org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler scheduler =
        mock(org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler.class);
    org.apache.iotdb.db.queryengine.common.FragmentInstanceId id =
        new org.apache.iotdb.db.queryengine.common.FragmentInstanceId(
            new org.apache.iotdb.db.queryengine.common.PlanFragmentId(
                new org.apache.iotdb.db.queryengine.common.QueryId("cancel_submit"), 1),
            "0");
    try {
      FragmentInstanceExecution.createFragmentInstanceExecution(
          scheduler,
          id,
          context,
          Collections.singletonList(driver),
          mock(org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink.class),
          context.getStateMachine(),
          1000,
          false,
          mock(org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.class));
      fail("cancelled fragment was submitted");
    } catch (CancellationException expected) {
      verify(scheduler, never()).submitDrivers(any(), anyList(), anyLong(), any());
    }
  }

  @Test(timeout = 20000)
  public void dataManagerMiddleFactoryFailureSettlesThreeRealSlots() throws Exception {
    assertManagerMiddleFactoryFailureSettlesThreeRealSlots(true);
  }

  @Test(timeout = 20000)
  public void schemaManagerMiddleFactoryFailureSettlesThreeRealSlots() throws Exception {
    assertManagerMiddleFactoryFailureSettlesThreeRealSlots(false);
  }

  @SuppressWarnings("unchecked")
  private void assertManagerMiddleFactoryFailureSettlesThreeRealSlots(boolean data)
      throws Exception {
    org.apache.iotdb.db.conf.IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
    org.apache.iotdb.db.queryengine.common.FragmentInstanceId id =
        new org.apache.iotdb.db.queryengine.common.FragmentInstanceId(
            new org.apache.iotdb.db.queryengine.common.PlanFragmentId(
                new org.apache.iotdb.db.queryengine.common.QueryId(
                    "middle_factory_real_" + (data ? "data" : "schema")),
                0),
            "0");
    FragmentInstanceManager manager = FragmentInstanceManager.getInstance();
    org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager exchange =
        org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService.getInstance()
            .getMPPDataExchangeManager();
    org.apache.iotdb.db.queryengine.execution.memory.MemoryPool memory =
        exchange.getLocalMemoryManager().getQueryPool();
    org.apache.iotdb.db.queryengine.execution.exchange.ResourceAuditManager audit =
        org.apache.iotdb.db.queryengine.execution.exchange.ResourceAuditManager.getInstance();
    String window = audit.openWindow(30_000, memory.getRemainingBytes(), 2048);
    ExecutorService waiterExecutor = Executors.newSingleThreadExecutor();
    java.util.concurrent.atomic.AtomicReference<LocalExecutionPlanContext> planned =
        new java.util.concurrent.atomic.AtomicReference<>();
    java.util.concurrent.atomic.AtomicReference<Future<?>> waiter =
        new java.util.concurrent.atomic.AtomicReference<>();
    List<Operator> operations =
        Arrays.asList(mock(Operator.class), mock(Operator.class), mock(Operator.class));
    org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode node =
        mock(org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode.class);
    when(node.getPlanNodeId())
        .thenReturn(
            new org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId(
                "middle-factory"));
    when(node.getType())
        .thenReturn(
            org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType.SERIES_SCAN);
    doAnswer(
            invocation -> {
              LocalExecutionPlanContext context = invocation.getArgument(1);
              planned.set(context);
              LocalExecutionPlanContext first = context.createSubContext();
              first.setISink(
                  exchange.createLocalSinkChannelForPipeline(
                      first.getDriverContext(),
                      "first-sink",
                      context.getResourceRegistry()::registerQueue));
              first.addPipelineDriverFactory(operations.get(0), first.getDriverContext(), 0);
              LocalExecutionPlanContext second = context.createSubContext();
              // Real Driver construction rejects the absent sink in the middle factory. No factory,
              // driver, FragmentInstanceContext or countDown operation is mocked.
              second.addPipelineDriverFactory(operations.get(1), second.getDriverContext(), 0);
              context.setISink(
                  exchange.createLocalSinkChannelForPipeline(
                      context.getDriverContext(),
                      "root-sink",
                      context.getResourceRegistry()::registerQueue));
              return operations.get(2);
            })
        .when(node)
        .accept(any(), any());
    doAnswer(
            invocation -> {
              FragmentInstanceContext context = planned.get().getInstanceContext();
              java.lang.reflect.Field field =
                  FragmentInstanceContext.class.getDeclaredField("allDriversClosed");
              field.setAccessible(true);
              CountDownLatch actualSlots = (CountDownLatch) field.get(context);
              assertEquals(3L, actualSlots.getCount());
              CountDownLatch waiting = new CountDownLatch(1);
              waiter.set(
                  waiterExecutor.submit(
                      () -> {
                        waiting.countDown();
                        context.releaseResourceWhenAllDriversAreClosed();
                      }));
              assertTrue(waiting.await(5, TimeUnit.SECONDS));
              try {
                waiter.get().get(100, TimeUnit.MILLISECONDS);
                fail("resource waiter passed before the real driver/factory slots were closed");
              } catch (TimeoutException expected) {
                // The actual context latch still owns all three slots while first.close is running.
              }
              return null;
            })
        .when(operations.get(0))
        .close();

    org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment fragment =
        mock(org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment.class);
    when(fragment.getPlanNodeTree()).thenReturn(node);
    when(fragment.getTypeProvider())
        .thenReturn(new org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider());
    org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance instance =
        mock(org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance.class);
    when(instance.getId()).thenReturn(id);
    when(instance.getFragment()).thenReturn(fragment);
    when(instance.getDataNodeFINum()).thenReturn(1);
    when(instance.getTimeOut()).thenReturn(10_000L);
    try {
      FragmentInstanceInfo info =
          data
              ? manager.execDataQueryFragmentInstance(
                  instance,
                  mock(org.apache.iotdb.db.storageengine.dataregion.IDataRegionForQuery.class))
              : manager.execSchemaQueryFragmentInstance(
                  instance,
                  mock(org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion.class));
      assertNotNull(info);
      assertTrue(info.getState().isFailed());
      assertTrue(info.getMessage().toLowerCase(java.util.Locale.ROOT).contains("sink"));
      assertNotNull(planned.get());
      assertEquals(3, planned.get().getPipelineDriverFactories().size());
      for (int i = 0; i < 3; i++) {
        assertSame(
            operations.get(i), planned.get().getPipelineDriverFactories().get(i).getOperation());
        verify(operations.get(i), times(1)).close();
      }
      assertNotNull(waiter.get());
      waiter.get().get(5, TimeUnit.SECONDS);
      java.lang.reflect.Field field =
          FragmentInstanceContext.class.getDeclaredField("allDriversClosed");
      field.setAccessible(true);
      assertEquals(0L, ((CountDownLatch) field.get(planned.get().getInstanceContext())).getCount());
      assertNull(planned.get().getInstanceContext().getPlanningContext());
      Map<String, Object> observation =
          audit.snapshot(window, id.getQueryId().getId(), id.getFullId(), memory, 2048);
      assertEquals(1, observation.get("createdDrivers"));
      assertEquals(1, observation.get("closedDrivers"));
      assertEquals(0, observation.get("liveDrivers"));
      assertEquals(0, observation.get("unconsumedFactories"));
      assertEquals(0, observation.get("pendingFutures"));
      assertEquals(false, observation.get("fragmentMapPresent"));
      assertEquals(true, observation.get("released"));
    } finally {
      waiterExecutor.shutdownNow();
      assertTrue(waiterExecutor.awaitTermination(5, TimeUnit.SECONDS));
      audit.closeWindow(window);
      manager.cancelQuery(
          id.getQueryId(),
          Collections.singletonList(id),
          true,
          FragmentInstanceManager.CancellationOrigin.LOCAL_CANCEL_TASK);
    }
  }
}
