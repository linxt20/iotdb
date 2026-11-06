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

import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskId;
import org.apache.iotdb.db.queryengine.statistics.snapshot.FragmentRuntimeSnapshot;
import org.apache.iotdb.db.queryengine.statistics.snapshot.RuntimeSnapshotAdapters;
import org.apache.iotdb.db.queryengine.statistics.snapshot.RuntimeSnapshotValidity;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FragmentRuntimeSnapshotProductionTest {
  @Test
  public void testProductionResponseAndAdapterShareChecksumAndStatus() throws Exception {
    verifyResponse(false);
  }

  @Test
  public void testIgnoredExplainFragmentHasValidTransportResponse() throws Exception {
    verifyResponse(true);
  }

  private void verifyResponse(boolean ignored) throws Exception {
    FragmentInstanceId id =
        new FragmentInstanceId(new PlanFragmentId(new QueryId("production"), 2), "0");
    FragmentInstanceContext context = mock(FragmentInstanceContext.class);
    when(context.getId()).thenReturn(id);
    when(context.getQueryStatistics()).thenReturn(new QueryStatistics());
    when(context.getPlanIdentityChecksum()).thenReturn("deterministic-plan");
    when(context.getWorkUnitIds()).thenReturn(Collections.singletonList("source:device:0"));
    when(context.getBucketIds()).thenReturn(Collections.singletonList(0));
    when(context.getExchangeEdgeIds()).thenReturn(Collections.emptyList());
    when(context.getSplitReason()).thenReturn("NONE");
    FragmentInstanceStateMachine state = mock(FragmentInstanceStateMachine.class);
    when(state.getState()).thenReturn(FragmentInstanceState.FINISHED);
    IDriver driver = mock(IDriver.class);
    when(driver.getDriverTaskId()).thenReturn(new DriverTaskId(id, 1));
    DriverContext driverContext = new DriverContext();
    driverContext
        .addOperatorContext(
            0,
            new org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId(
                "scan-parallel-0"),
            "TableScanOperator")
        .addOutputRows(7);
    when(driver.getDriverContext()).thenReturn(driverContext);
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
            id,
            context,
            Collections.singletonList(driver),
            mock(ISink.class),
            state,
            1000L,
            mock(MPPDataExchangeManager.class));
    // Ignored fragments have no data region. Regular fragments use a mocked region.
    execution.shouldIgnoreForStatistics = ignored;
    if (!ignored) {
      org.apache.iotdb.db.storageengine.dataregion.IDataRegionForQuery region =
          mock(org.apache.iotdb.db.storageengine.dataregion.IDataRegionForQuery.class);
      when(region.getDataRegionIdString()).thenReturn("7");
      when(context.getDataRegion()).thenReturn(region);
    }
    TFetchFragmentInstanceStatisticsResp response = execution.buildStatistics();
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), response.getStatus().getCode());
    assertEquals(id.toThrift(), response.getFragmentInstanceId());
    FragmentRuntimeSnapshot adapted = RuntimeSnapshotAdapters.fromResponse(response);
    assertNotNull(response.getSnapshotChecksum());
    if (!ignored) {
      assertEquals(
          1,
          response.getOperatorStatisticsMap().get("__pipeline_scan_parallel_0").getTsBlockCount());
      org.junit.Assert.assertTrue(
          response
              .getOperatorStatisticsMap()
              .get("__pipeline_scan_parallel_0")
              .isSetTsBlockCount());
    }
    assertEquals(execution.getRuntimeSnapshot().getChecksum(), adapted.getChecksum());
    assertFalse(adapted.getValidity().contains(RuntimeSnapshotValidity.CHECKSUM_MISMATCH));
    assertEquals("production.2.0", adapted.getDrivers().get(0).getFragmentInstanceId());
    // FINISHED state alone does not imply that the terminal cache was published.
    assertFalse(response.isIsTerminal());
    java.lang.reflect.Field lockField =
        FragmentInstanceExecution.class.getDeclaredField("statisticsLock");
    lockField.setAccessible(true);
    java.util.concurrent.locks.ReentrantReadWriteLock lock =
        (java.util.concurrent.locks.ReentrantReadWriteLock) lockField.get(execution);
    lock.writeLock().lock();
    try {
      TFetchFragmentInstanceStatisticsResp frozen = execution.buildStatistics();
      org.junit.Assert.assertTrue(frozen.isIsTerminal());
      assertFalse(
          RuntimeSnapshotAdapters.fromResponse(frozen)
              .getValidity()
              .contains(RuntimeSnapshotValidity.CHECKSUM_MISMATCH));
    } finally {
      lock.writeLock().unlock();
    }
  }
}
