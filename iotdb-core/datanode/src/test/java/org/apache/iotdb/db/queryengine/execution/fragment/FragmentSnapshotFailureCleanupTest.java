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

import org.apache.iotdb.db.queryengine.common.*;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler;

import org.junit.Test;
import org.mockito.InOrder;

import java.lang.reflect.*;
import java.util.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class FragmentSnapshotFailureCleanupTest {
  @Test
  public void failedSnapshotStillClosesDriversAndReleasesBothMemoryOwners() throws Exception {
    FragmentInstanceId id =
        new FragmentInstanceId(new PlanFragmentId(new QueryId("snapshot_failure"), 1), "0");
    FragmentInstanceContext context = mock(FragmentInstanceContext.class);
    FragmentInstanceStateMachine state = new FragmentInstanceStateMachine(id, Runnable::run);
    IDriver driver = mock(IDriver.class);
    when(driver.getDriverContext()).thenReturn(new DriverContext());
    doThrow(new IllegalArgumentException("injected statistics failure"))
        .when(driver)
        .recordMorselCompletionStatistics();
    ISink sink = mock(ISink.class);
    MPPDataExchangeManager exchange = mock(MPPDataExchangeManager.class);
    Constructor<FragmentInstanceExecution> ctor =
        FragmentInstanceExecution.class.getDeclaredConstructor(
            FragmentInstanceId.class,
            FragmentInstanceContext.class,
            List.class,
            ISink.class,
            FragmentInstanceStateMachine.class,
            long.class,
            MPPDataExchangeManager.class);
    ctor.setAccessible(true);
    FragmentInstanceExecution execution =
        ctor.newInstance(
            id, context, Collections.singletonList(driver), sink, state, 1000L, exchange);
    Method initialize =
        FragmentInstanceExecution.class.getDeclaredMethod(
            "initialize", IDriverScheduler.class, boolean.class);
    initialize.setAccessible(true);
    initialize.invoke(execution, mock(IDriverScheduler.class), true);
    state.finished();
    assertTrue(execution.isStaticsRemoved());
    InOrder order = inOrder(sink, driver, context, exchange);
    order.verify(sink).close();
    order.verify(driver).close();
    order.verify(context).releaseResourceWhenAllDriversAreClosed();
    order
        .verify(exchange)
        .deRegisterFragmentInstanceFromMemoryPool("snapshot_failure", "1.0", true);
    order.verify(context).releaseMemoryReservationManager();
    // Lock must be available to a later statistics reader after the exception.
    java.util.concurrent.ExecutorService reader =
        java.util.concurrent.Executors.newSingleThreadExecutor();
    try {
      assertTrue(
          reader
              .submit(
                  () -> {
                    execution.lockStatistics();
                    try {
                      return true;
                    } finally {
                      execution.unlockStatistics();
                    }
                  })
              .get(5, java.util.concurrent.TimeUnit.SECONDS));
    } finally {
      reader.shutdownNow();
    }
  }

  @Test
  public void ordinaryQueryFeedbackFailureDoesNotEnterOuterFallbackCleanup() throws Exception {
    FragmentInstanceId id =
        new FragmentInstanceId(
            new PlanFragmentId(new QueryId("optional_feedback_failure"), 1), "0");
    FragmentInstanceContext context = mock(FragmentInstanceContext.class);
    org.apache.iotdb.db.queryengine.plan.planner.DynamicScanAssignmentRegistry registry =
        new org.apache.iotdb.db.queryengine.plan.planner.DynamicScanAssignmentRegistry(true);
    java.util.List<String> devices = java.util.Arrays.asList("d0", "d1");
    org.apache.iotdb.db.queryengine.plan.planner.ScanParallelPlanner.Input input =
        new org.apache.iotdb.db.queryengine.plan.planner.ScanParallelPlanner.Input(
            devices,
            Collections.emptyList(),
            null,
            2,
            true,
            false,
            false,
            false,
            false,
            false,
            "optional_feedback_failure",
            "1.0",
            "scan");
    org.apache.iotdb.db.queryengine.plan.planner.DynamicScanGrouping grouping =
        new org.apache.iotdb.db.queryengine.plan.planner.DynamicScanGroupingBuilder().build(input);
    org.apache.iotdb.db.queryengine.plan.planner.ScanParallelPlan plan =
        new org.apache.iotdb.db.queryengine.plan.planner.ScanParallelPlanner()
            .planWithGrouping(input, grouping, 1);
    org.apache.iotdb.db.queryengine.plan.planner.DynamicQueryShape shape =
        new org.apache.iotdb.db.queryengine.plan.planner.DynamicQueryShape(
            "db",
            "table",
            "region",
            "source",
            devices,
            null,
            null,
            Collections.emptyMap(),
            "ASC",
            org.apache.iotdb.db.queryengine.plan.planner.ScanParallelPlan.Mode.DEVICE);
    registry.registerSource(
        new org.apache.iotdb.db.queryengine.plan.planner.DynamicSourcePlan(shape, plan, grouping));
    registry.registerBucket(0, plan, 0);
    registry.freeze(Collections.singleton(0));
    when(context.getDynamicScanAssignments()).thenReturn(registry);
    when(context.getId()).thenReturn(id);
    FragmentInstanceStateMachine state = new FragmentInstanceStateMachine(id, Runnable::run);
    IDriver driver = mock(IDriver.class);
    when(driver.getDriverContext()).thenReturn(new DriverContext());
    // buildStatistics fails at the absent task identity. This failure is optional feedback only.
    ISink sink = mock(ISink.class);
    MPPDataExchangeManager exchange = mock(MPPDataExchangeManager.class);
    Constructor<FragmentInstanceExecution> ctor =
        FragmentInstanceExecution.class.getDeclaredConstructor(
            FragmentInstanceId.class,
            FragmentInstanceContext.class,
            List.class,
            ISink.class,
            FragmentInstanceStateMachine.class,
            long.class,
            MPPDataExchangeManager.class);
    ctor.setAccessible(true);
    FragmentInstanceExecution execution =
        ctor.newInstance(
            id, context, Collections.singletonList(driver), sink, state, 1000L, exchange);
    Method initialize =
        FragmentInstanceExecution.class.getDeclaredMethod(
            "initialize", IDriverScheduler.class, boolean.class);
    initialize.setAccessible(true);
    initialize.invoke(execution, mock(IDriverScheduler.class), false);
    state.finished();
    InOrder order = inOrder(sink, driver, context, exchange);
    order.verify(sink).close();
    order.verify(driver).close();
    order.verify(context).releaseResourceWhenAllDriversAreClosed();
    order
        .verify(exchange)
        .deRegisterFragmentInstanceFromMemoryPool("optional_feedback_failure", "1.0", true);
    order.verify(context).releaseMemoryReservationManager();
    verify(sink, times(1)).close();
    verify(driver, times(1)).close();
    verify(context, times(1)).releaseResourceWhenAllDriversAreClosed();
    verify(context).setDynamicScanAssignments(null);
    assertTrue(execution.isStaticsRemoved());
  }
}
