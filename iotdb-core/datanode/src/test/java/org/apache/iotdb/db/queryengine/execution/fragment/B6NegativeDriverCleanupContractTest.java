/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */
package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.ResourceAuditManager;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;

import com.google.gson.Gson;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/** Actual Manager/Driver/queue/pool path; only storage-free leaf operators are mocked. */
public class B6NegativeDriverCleanupContractTest {
  @Test(timeout = 20000)
  public void realManagerFailureClosesFirstDriverAndSettlesRealAccount() throws Exception {
    managerFailure(false);
  }

  @Test(timeout = 20000)
  public void realManagerFailureRequiresIndependentDeregistrationReceipt() throws Exception {
    managerFailure(true);
  }

  @SuppressWarnings("unchecked")
  private void managerFailure(boolean accountFirst) throws Exception {
    int originalNode = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
    String query = "b6_negative_driver_" + (accountFirst ? "account" : "lifecycle");
    FragmentInstanceId id = new FragmentInstanceId(new PlanFragmentId(new QueryId(query), 0), "0");
    FragmentInstanceManager manager = FragmentInstanceManager.getInstance();
    MPPDataExchangeManager exchange =
        MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
    MemoryPool pool = exchange.getLocalMemoryManager().getQueryPool();
    ResourceAuditManager audit = ResourceAuditManager.getInstance();
    long originalFree = pool.getRemainingBytes();
    String window = audit.openWindow(30_000, originalFree, 2048);
    AtomicReference<LocalExecutionPlanContext> planned = new AtomicReference<>();
    AtomicLong actualNonzeroBytes = new AtomicLong();
    List<Operator> operations =
        Arrays.asList(mock(Operator.class), mock(Operator.class), mock(Operator.class));
    PlanNode node = mock(PlanNode.class);
    when(node.getPlanNodeId()).thenReturn(new PlanNodeId("b6-middle-factory"));
    when(node.getType()).thenReturn(PlanNodeType.SERIES_SCAN);
    doAnswer(
            invocation -> {
              LocalExecutionPlanContext context = invocation.getArgument(1);
              planned.set(context);
              LocalExecutionPlanContext first = context.createSubContext();
              ISinkChannel createdSink =
                  exchange.createLocalSinkChannelForPipeline(
                      first.getDriverContext(),
                      "first-real-nonzero-account",
                      context.getResourceRegistry()::registerQueue);
              assertTrue(
                  "B6_ACTUAL_PIPELINE_SINK_MUST_BE_LOCAL", createdSink instanceof LocalSinkChannel);
              LocalSinkChannel sink = (LocalSinkChannel) createdSink;
              first.setISink(sink);
              sink.getSharedTsBlockQueue().allowAddingTsBlock();
              TsBlock data = block();
              sink.send(data);
              long measured =
                  pool.snapshotFragmentReservations(query, "0.0").fragmentReservedBytes();
              assertTrue(
                  "B6_REAL_ACCOUNT_MUST_BE_NONZERO_BEFORE_FACTORY_FAILURE",
                  measured >= data.getSizeInBytes());
              actualNonzeroBytes.set(measured);
              first.addPipelineDriverFactory(operations.get(0), first.getDriverContext(), 0);
              LocalExecutionPlanContext second = context.createSubContext();
              // Missing sink causes the real second Driver constructor to fail. The first Driver,
              // every factory and the FragmentInstanceContext remain production objects.
              second.addPipelineDriverFactory(operations.get(1), second.getDriverContext(), 0);
              context.setISink(
                  exchange.createLocalSinkChannelForPipeline(
                      context.getDriverContext(),
                      "root-real-sink",
                      context.getResourceRegistry()::registerQueue));
              return operations.get(2);
            })
        .when(node)
        .accept(any(), any());
    PlanFragment fragment = mock(PlanFragment.class);
    when(fragment.getPlanNodeTree()).thenReturn(node);
    when(fragment.getTypeProvider()).thenReturn(new TypeProvider());
    FragmentInstance instance = mock(FragmentInstance.class);
    when(instance.getId()).thenReturn(id);
    when(instance.getFragment()).thenReturn(fragment);
    when(instance.getDataNodeFINum()).thenReturn(1);
    when(instance.getTimeOut()).thenReturn(10_000L);
    try {
      FragmentInstanceInfo info =
          manager.execDataQueryFragmentInstance(
              instance,
              mock(org.apache.iotdb.db.storageengine.dataregion.IDataRegionForQuery.class));
      assertNotNull("B6_REAL_MANAGER_MUST_RETURN_FAILURE", info);
      assertTrue("B6_REAL_SECOND_DRIVER_CONSTRUCTION_MUST_FAIL", info.getState().isFailed());
      assertTrue(
          "B6_REAL_FAILURE_MUST_BE_THE_MISSING_SECOND_SINK",
          info.getMessage().toLowerCase(java.util.Locale.ROOT).contains("sink"));
      assertTrue("B6_REAL_ACCOUNT_WAS_ACTUALLY_RESERVED", actualNonzeroBytes.get() > 0);
      Map<String, Object> observation = null;
      long deadline = System.nanoTime() + 2_000_000_000L;
      do {
        observation = audit.snapshot(window, query, id.getFullId(), pool, 2048);
        if (Boolean.TRUE.equals(observation.get("released"))
            && !FragmentInstanceManager.hasDataNodeQueryContext(id.getQueryId())) break;
        Thread.sleep(10);
      } while (System.nanoTime() < deadline);
      MemoryPool.FragmentReservationSnapshot account =
          pool.snapshotFragmentReservations(query, "0.0");
      MemoryPool.DeregistrationReceipt receipt = pool.getDeregistrationReceipt(query, "0.0");
      boolean actualSharedQueryContextPresent =
          FragmentInstanceManager.hasDataNodeQueryContext(id.getQueryId());
      Map<String, Object> raw = new LinkedHashMap<>();
      raw.put("queryId", query);
      raw.put("actualNonzeroBytesBeforeFailure", actualNonzeroBytes.get());
      raw.put("actualAfterAccount", account);
      raw.put("actualDeregistrationReceipt", receipt);
      raw.put("actualAfterPoolRemainingBytes", pool.getRemainingBytes());
      raw.put("actualBeforePoolRemainingBytes", originalFree);
      raw.put("originalResourceAuditSnapshot", observation);
      raw.put("actualSharedQueryContextPresent", actualSharedQueryContextPresent);
      System.out.println("B6_REAL_MANAGER_FAILURE_OBSERVATION " + new Gson().toJson(raw));
      // Capture every independent observation before choosing the assertion order. A missed
      // Driver.close can still have a clean account through independent endpoint aborts.
      if (accountFirst) assertAccount(account, receipt, originalFree, pool);
      assertEquals("B6_FIRST_REAL_DRIVER_WAS_CREATED", 1, observation.get("createdDrivers"));
      assertEquals("B6_FIRST_REAL_DRIVER_MUST_CLOSE", 1, observation.get("closedDrivers"));
      assertEquals("B6_FIRST_REAL_DRIVER_MUST_BE_TERMINAL", 0, observation.get("liveDrivers"));
      assertEquals(
          "B6_UNCONSUMED_REAL_FACTORIES_MUST_CLOSE", 0, observation.get("unconsumedFactories"));
      assertEquals("B6_REAL_ENDPOINTS_MUST_CLOSE", 0, observation.get("liveEndpoints"));
      assertEquals("B6_REAL_QUEUES_MUST_CLOSE", 0, observation.get("liveQueues"));
      assertEquals("B6_REAL_FUTURES_MUST_SETTLE", 0, observation.get("pendingFutures"));
      for (Map<String, Object> object : (List<Map<String, Object>>) observation.get("objects"))
        assertEquals("B6_EVERY_REAL_OBJECT_MUST_BE_TERMINAL", true, object.get("terminal"));
      if (!accountFirst) assertAccount(account, receipt, originalFree, pool);
      assertFalse("B6_REAL_SHARED_CONTEXT_MUST_RELEASE", actualSharedQueryContextPresent);
      assertEquals("B6_ORIGINAL_RESOURCE_AUDIT_MUST_RELEASE", true, observation.get("released"));
    } finally {
      // These are actual production Driver references, read from the audit recorder only to
      // clean a deliberately failing mutant AFTER its immutable observations/assertion above.
      // No audit fields, account fields or reported closed flags are rewritten.
      for (IDriver driver : actualDrivers(audit, window, query)) driver.close();
      if (planned.get() != null && planned.get().getInstanceContext().getPlanningContext() != null)
        planned
            .get()
            .getResourceRegistry()
            .abortUntransferred(new RuntimeException("B6 test finally"));
      manager.cancelQuery(
          id.getQueryId(),
          Collections.singletonList(id),
          true,
          FragmentInstanceManager.CancellationOrigin.LOCAL_CANCEL_TASK);
      audit.closeWindow(window);
      IoTDBDescriptor.getInstance().getConfig().setDataNodeId(originalNode);
    }
  }

  private static void assertAccount(
      MemoryPool.FragmentReservationSnapshot account,
      MemoryPool.DeregistrationReceipt receipt,
      long originalFree,
      MemoryPool pool) {
    assertEquals("B6_REAL_FRAGMENT_BYTES_MUST_RETURN_TO_ZERO", 0L, account.fragmentReservedBytes());
    assertFalse("B6_REAL_DEREGISTRATION_ACCOUNT_MUST_BE_ABSENT", account.present());
    assertNotNull("B6_REAL_DEREGISTRATION_RECEIPT_MUST_EXIST", receipt);
    assertTrue("B6_REAL_DEREGISTRATION_RECEIPT_MUST_SUCCEED", receipt.success());
    assertEquals("B6_REAL_DEREGISTRATION_BEFORE_MUST_BE_ZERO", 0L, receipt.bytesBefore());
    assertEquals("B6_REAL_DEREGISTRATION_AFTER_MUST_BE_ZERO", 0L, receipt.bytesAfter());
    assertFalse("B6_REAL_DEREGISTRATION_RECEIPT_MUST_CONFIRM_ABSENCE", receipt.presentAfter());
    assertNull("B6_REAL_DEREGISTRATION_RECEIPT_MUST_NOT_HIDE_ERROR", receipt.exceptionType());
    assertEquals("B6_REAL_QUERY_POOL_MUST_RESTORE", originalFree, pool.getRemainingBytes());
  }

  private static TsBlock block() {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT64));
    for (int index = 0; index < 128; index++) {
      builder.getTimeColumnBuilder().writeLong(index);
      builder.getColumnBuilder(0).writeLong(index * 19L);
      builder.declarePosition();
    }
    return builder.build();
  }

  private static Object field(Object object, String name) throws Exception {
    Field field = object.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(object);
  }

  private static List<IDriver> actualDrivers(
      ResourceAuditManager audit, String windowId, String query) throws Exception {
    Map<?, ?> windows = (Map<?, ?>) field(audit, "windows");
    Object window = windows.get(windowId);
    List<IDriver> result = new ArrayList<>();
    if (window == null) return result;
    synchronized (window) {
      for (Object record : ((Map<?, ?>) field(window, "records")).values()) {
        if (!query.equals(field(record, "query"))) continue;
        for (Object driver : ((Map<?, ?>) field(record, "drivers")).keySet())
          result.add((IDriver) driver);
      }
    }
    return result;
  }
}
