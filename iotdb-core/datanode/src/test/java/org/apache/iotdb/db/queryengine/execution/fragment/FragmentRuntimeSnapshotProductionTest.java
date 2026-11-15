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

  @Test
  public void runtimeOnlyOrdinarySqlCachesTerminalControlWithoutExplainOrHistoricalFeedback()
      throws Exception {
    RuntimeFixture f = new RuntimeFixture();
    org.junit.Assert.assertNull(f.context.getDynamicScanAssignments());
    f.freezeTerminal();
    TFetchFragmentInstanceStatisticsResp cached = f.cache.get();
    assertNotNull(cached);
    org.junit.Assert.assertTrue(cached.isIsTerminal());
    assertFalse(cached.isSetDynamicDecisions());
    assertEquals(f.evidence.get().toJson(), controlJson(cached));
    assertEquals(f.evidence.get().toJson(), f.root.getSpecifiedInfo().get("__runtime_dop_control"));
    assertFalse(
        RuntimeSnapshotAdapters.fromResponse(cached)
            .getValidity()
            .contains(RuntimeSnapshotValidity.CHECKSUM_MISMATCH));
  }

  @Test
  public void mergedRootControlTraceSurvivesThriftBinaryTransportAndAdapterChecksum()
      throws Exception {
    RuntimeFixture f = new RuntimeFixture();
    f.root.getSpecifiedInfo().put("scan_specific_marker", "before-merge");
    f.freezeTerminal();
    TFetchFragmentInstanceStatisticsResp cached = f.cache.get();
    assertNotNull(cached.getOperatorStatisticsMap().get("scan"));
    assertEquals(
        f.evidence.get().toJson(),
        cached
            .getOperatorStatisticsMap()
            .get("scan")
            .getSpecifiedInfo()
            .get("__runtime_dop_control"));
    TFetchFragmentInstanceStatisticsResp transported = roundTrip(cached);
    assertEquals(controlJson(cached), controlJson(transported));
    assertEquals(cached.getSnapshotChecksum(), transported.getSnapshotChecksum());
    FragmentRuntimeSnapshot adapted = RuntimeSnapshotAdapters.fromResponse(transported);
    assertEquals(cached.getSnapshotChecksum(), adapted.getChecksum());
    assertFalse(adapted.getValidity().contains(RuntimeSnapshotValidity.CHECKSUM_MISMATCH));
  }

  @Test
  public void tamperedRuntimeControlJsonInvalidatesSnapshotChecksum() throws Exception {
    RuntimeFixture f = new RuntimeFixture();
    f.freezeTerminal();
    TFetchFragmentInstanceStatisticsResp tampered = f.cache.get().deepCopy();
    String original = controlJson(tampered);
    String changed = original.replace("FINISHED", "FAILED");
    org.junit.Assert.assertNotEquals(original, changed);
    for (org.apache.iotdb.mpp.rpc.thrift.TOperatorStatistics operator :
        tampered.getOperatorStatisticsMap().values()) {
      if (operator.isSetSpecifiedInfo()
          && operator.getSpecifiedInfo().containsKey("__runtime_dop_control")) {
        operator.getSpecifiedInfo().put("__runtime_dop_control", changed);
      }
    }
    org.junit.Assert.assertTrue(
        RuntimeSnapshotAdapters.fromResponse(tampered)
            .getValidity()
            .contains(RuntimeSnapshotValidity.CHECKSUM_MISMATCH));
    assertEquals(original, controlJson(f.cache.get()));
  }

  @Test
  public void fullStatisticsFailureStillCachesPartialControlEvidence() throws Exception {
    RuntimeFixture f = new RuntimeFixture();
    when(f.context.getQueryStatistics()).thenThrow(new IllegalStateException());
    f.freezeTerminal();
    TFetchFragmentInstanceStatisticsResp partial = f.cache.get();
    assertNotNull(partial);
    org.junit.Assert.assertTrue(partial.isIsTerminal());
    org.junit.Assert.assertTrue(
        partial.getValidityReasons().contains("PARTIAL_RUNTIME_CONTROL_ONLY"));
    assertEquals(f.evidence.get().toJson(), controlJson(partial));
    TFetchFragmentInstanceStatisticsResp transported = roundTrip(partial);
    assertEquals(controlJson(partial), controlJson(transported));
    assertFalse(
        RuntimeSnapshotAdapters.fromResponse(transported)
            .getValidity()
            .contains(RuntimeSnapshotValidity.CHECKSUM_MISMATCH));
  }

  @Test
  public void partialFallbackDoesNotRevisitUnavailableLiveDriverContext() throws Exception {
    RuntimeFixture f = new RuntimeFixture();
    when(f.driver.getDriverContext()).thenThrow(new IllegalStateException());
    f.freezeTerminal();
    TFetchFragmentInstanceStatisticsResp partial = f.cache.get();
    assertNotNull(partial);
    org.junit.Assert.assertTrue(
        partial.getValidityReasons().contains("PARTIAL_RUNTIME_CONTROL_ONLY"));
    assertEquals(f.evidence.get().toJson(), controlJson(partial));
    assertFalse(
        RuntimeSnapshotAdapters.fromResponse(roundTrip(partial))
            .getValidity()
            .contains(RuntimeSnapshotValidity.CHECKSUM_MISMATCH));
  }

  @Test
  public void managerReadsTerminalContextCacheAfterSuccessfulOwnerDetachAndExecutionRemoval()
      throws Exception {
    RuntimeFixture f = new RuntimeFixture();
    f.initializeRuntime();
    f.owner.requestStop();
    f.owner.sealConstruction();
    f.owner.getCompletedFuture().get(5, java.util.concurrent.TimeUnit.SECONDS);
    org.junit.Assert.assertNull(f.execution.getDrivers());
    org.mockito.Mockito.verify(f.context).clearRuntimePlanBindings();
    org.mockito.Mockito.verify(f.context).releaseRuntimeQueryResources();
    TFetchFragmentInstanceStatisticsResp cached = f.cache.get();
    assertNotNull(cached);
    String json = controlJson(cached);
    when(f.driver.getDriverContext()).thenThrow(new AssertionError("released driver accessed"));
    FragmentInstanceManager manager = mock(FragmentInstanceManager.class);
    java.util.Map<FragmentInstanceId, FragmentInstanceContext> contexts =
        new java.util.concurrent.ConcurrentHashMap<>();
    java.util.Map<FragmentInstanceId, FragmentInstanceExecution> executions =
        new java.util.concurrent.ConcurrentHashMap<>();
    contexts.put(f.id, f.context);
    executions.put(f.id, f.execution);
    setPrivateField(FragmentInstanceManager.class, manager, "instanceContext", contexts);
    setPrivateField(FragmentInstanceManager.class, manager, "instanceExecution", executions);
    org.mockito.Mockito.doCallRealMethod().when(manager).getFragmentInstanceStatistics(f.id);
    executions.remove(f.id);
    TFetchFragmentInstanceStatisticsResp fetched = manager.getFragmentInstanceStatistics(f.id);
    org.junit.Assert.assertSame(cached, fetched);
    assertEquals(json, controlJson(fetched));
    assertFalse(
        RuntimeSnapshotAdapters.fromResponse(roundTrip(fetched))
            .getValidity()
            .contains(RuntimeSnapshotValidity.CHECKSUM_MISMATCH));
    f.owner.requestStop().get(5, java.util.concurrent.TimeUnit.SECONDS);
    org.mockito.Mockito.verify(f.context, org.mockito.Mockito.times(1))
        .releaseRuntimeQueryResources();
  }

  @Test
  public void failedPhysicalCloseKeepsOwnersAndStillPublishesTerminalControlTrace()
      throws Exception {
    RuntimeFixture f = new RuntimeFixture();
    when(f.driver.requestClose())
        .thenReturn(
            com.google.common.util.concurrent.Futures.immediateFailedFuture(
                new IllegalStateException()));
    f.initializeRuntime();
    f.owner.requestStop();
    f.owner.sealConstruction();
    org.junit.Assert.assertThrows(
        java.util.concurrent.ExecutionException.class,
        () -> f.owner.getCompletedFuture().get(5, java.util.concurrent.TimeUnit.SECONDS));
    assertNotNull(f.execution.getDrivers());
    assertNotNull(f.cache.get());
    assertEquals(f.evidence.get().toJson(), controlJson(f.cache.get()));
    org.mockito.Mockito.verify(f.context, org.mockito.Mockito.never()).clearRuntimePlanBindings();
    org.mockito.Mockito.verify(f.context, org.mockito.Mockito.never())
        .releaseResourceWhenAllDriversAreClosed();
    assertFalse(
        RuntimeSnapshotAdapters.fromResponse(roundTrip(f.cache.get()))
            .getValidity()
            .contains(RuntimeSnapshotValidity.CHECKSUM_MISMATCH));
  }

  @Test
  public void ignoredRuntimeFragmentStillFreezesTerminalTransportControlEvidence()
      throws Exception {
    RuntimeFixture f = new RuntimeFixture();
    f.execution.shouldIgnoreForStatistics = true;
    when(f.context.getDataRegion()).thenReturn(null);
    f.freezeTerminal();
    TFetchFragmentInstanceStatisticsResp cached = f.cache.get();
    assertNotNull(cached);
    org.junit.Assert.assertTrue(cached.isIsTerminal());
    assertEquals(f.evidence.get().toJson(), controlJson(cached));
    assertFalse(
        RuntimeSnapshotAdapters.fromResponse(roundTrip(cached))
            .getValidity()
            .contains(RuntimeSnapshotValidity.CHECKSUM_MISMATCH));
  }

  private static String controlJson(TFetchFragmentInstanceStatisticsResp response) {
    if (response.isSetOperatorStatisticsMap()) {
      for (org.apache.iotdb.mpp.rpc.thrift.TOperatorStatistics operator :
          response.getOperatorStatisticsMap().values()) {
        if (operator.isSetSpecifiedInfo()
            && operator.getSpecifiedInfo().containsKey("__runtime_dop_control")) {
          return operator.getSpecifiedInfo().get("__runtime_dop_control");
        }
      }
    }
    throw new AssertionError("missing terminal runtime control evidence");
  }

  private static TFetchFragmentInstanceStatisticsResp roundTrip(
      TFetchFragmentInstanceStatisticsResp original) throws Exception {
    byte[] bytes =
        new org.apache.thrift.TSerializer(new org.apache.thrift.protocol.TBinaryProtocol.Factory())
            .serialize(original);
    TFetchFragmentInstanceStatisticsResp transported = new TFetchFragmentInstanceStatisticsResp();
    new org.apache.thrift.TDeserializer(new org.apache.thrift.protocol.TBinaryProtocol.Factory())
        .deserialize(transported, bytes);
    return transported;
  }

  private static void setPrivateField(
      Class<?> declaring, Object owner, String fieldName, Object value) throws Exception {
    java.lang.reflect.Field field = declaring.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(owner, value);
  }

  private static final class RuntimeFixture {
    private final FragmentInstanceId id =
        new FragmentInstanceId(new PlanFragmentId(new QueryId("runtime_terminal"), 0), "instance");
    private final FragmentInstanceContext context = mock(FragmentInstanceContext.class);
    private final FragmentInstanceStateMachine state = mock(FragmentInstanceStateMachine.class);
    private final IDriver driver = mock(IDriver.class);
    private final org.apache.iotdb.db.queryengine.execution.operator.OperatorContext root;
    private final java.util.concurrent.atomic.AtomicReference<
            org.apache.iotdb.db.queryengine.execution.runtime.RuntimeDopExecutionSnapshot>
        evidence = new java.util.concurrent.atomic.AtomicReference<>();
    private final java.util.concurrent.atomic.AtomicReference<TFetchFragmentInstanceStatisticsResp>
        cache = new java.util.concurrent.atomic.AtomicReference<>();
    private final org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFragmentLifecycle owner;
    private final FragmentInstanceExecution execution;

    private RuntimeFixture() throws Exception {
      when(context.getId()).thenReturn(id);
      when(context.getStateMachine()).thenReturn(state);
      when(state.getState()).thenReturn(FragmentInstanceState.FINISHED);
      when(context.getFailureCause()).thenReturn(java.util.Optional.empty());
      when(context.getQueryStatistics()).thenReturn(new QueryStatistics());
      when(context.getPlanIdentityChecksum()).thenReturn("runtime-control-plan");
      when(context.getWorkUnitIds()).thenReturn(List.of("source:device:0"));
      when(context.getBucketIds()).thenReturn(List.of(0));
      when(context.getExchangeEdgeIds()).thenReturn(Collections.emptyList());
      when(context.getSplitReason()).thenReturn("NONE");
      when(context.getOuterQueryDeadlineMs()).thenReturn(123456L);
      org.apache.iotdb.db.storageengine.dataregion.IDataRegionForQuery region =
          mock(org.apache.iotdb.db.storageengine.dataregion.IDataRegionForQuery.class);
      when(region.getDataRegionIdString()).thenReturn("7");
      when(context.getDataRegion()).thenReturn(region);
      org.mockito.Mockito.doAnswer(
              call -> {
                evidence.set(call.getArgument(0));
                return null;
              })
          .when(context)
          .setRuntimeControlEvidence(org.mockito.ArgumentMatchers.any());
      when(context.getRuntimeControlEvidence()).thenAnswer(call -> evidence.get());
      org.mockito.Mockito.doAnswer(
              call -> {
                cache.set(call.getArgument(0));
                return null;
              })
          .when(context)
          .setFragmentInstanceStatistics(org.mockito.ArgumentMatchers.any());
      when(context.getFragmentInstanceStatistics()).thenAnswer(call -> cache.get());
      DriverContext driverContext = new DriverContext(context, 0);
      root =
          driverContext.addOperatorContext(
              0,
              new org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId(
                  "scan-parallel-0"),
              "TableScanOperator");
      root.addOutputRows(7);
      when(driver.getDriverTaskId()).thenReturn(new DriverTaskId(id, 0));
      when(driver.getDriverContext()).thenReturn(driverContext);
      when(driver.supportsQuiescence()).thenReturn(true);
      when(driver.supportsCoordinatedClose()).thenReturn(true);
      when(driver.requestQuiescence())
          .thenReturn(com.google.common.util.concurrent.Futures.immediateVoidFuture());
      when(driver.requestClose())
          .thenReturn(com.google.common.util.concurrent.Futures.immediateVoidFuture());
      owner =
          new org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFragmentLifecycle(
              context, com.google.common.util.concurrent.MoreExecutors.directExecutor());
      when(context.getRuntimeLifecycle()).thenReturn(owner);
      owner.registerOriginalDriver(
          mock(org.apache.iotdb.db.queryengine.plan.planner.PipelineDriverFactory.class), driver);
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
      execution =
          ctor.newInstance(
              id,
              context,
              List.of(driver),
              mock(ISink.class),
              state,
              1000L,
              mock(MPPDataExchangeManager.class));
      execution.shouldIgnoreForStatistics = false;
    }

    private void freezeTerminal() throws Exception {
      java.lang.reflect.Method freeze =
          FragmentInstanceExecution.class.getDeclaredMethod(
              "freezeRuntimeTerminalStatistics", boolean.class);
      freeze.setAccessible(true);
      freeze.invoke(execution, false);
    }

    private void initializeRuntime() throws Exception {
      java.lang.reflect.Method initialize =
          FragmentInstanceExecution.class.getDeclaredMethod("initializeRuntime", boolean.class);
      initialize.setAccessible(true);
      initialize.invoke(execution, false);
    }
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
    assertEquals(2, response.getSchemaVersion());
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
