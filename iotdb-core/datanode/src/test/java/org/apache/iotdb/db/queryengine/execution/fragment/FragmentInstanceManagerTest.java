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

import org.apache.iotdb.commons.exception.MetadataLeaseFencedException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.storageengine.dataregion.IDataRegionForQuery;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.commons.exception.MetadataLeaseFencedException.LeaseFencedRetryPolicy.RETRY_UNTIL_SUCCESS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FragmentInstanceManagerTest {

  private static final String FAILURE_MESSAGE = "metadata lease fenced";

  @BeforeClass
  public static void setUpClass() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
  }

  @Test
  public void testDataQueryFailureInfoAfterConcurrentContextRemoval() throws Exception {
    assertFailureInfoAfterConcurrentContextRemoval(true);
  }

  @Test
  public void testSchemaQueryFailureInfoAfterConcurrentContextRemoval() throws Exception {
    assertFailureInfoAfterConcurrentContextRemoval(false);
  }

  private void assertFailureInfoAfterConcurrentContextRemoval(boolean dataQuery) throws Exception {
    String queryType = dataQuery ? "data" : "schema";
    FragmentInstanceId instanceId =
        new FragmentInstanceId(
            new PlanFragmentId(new QueryId(queryType + "_context_removal"), 0), "0");
    FragmentInstance instance = mock(FragmentInstance.class);
    PlanFragment fragment = mock(PlanFragment.class);
    CountDownLatch planningStarted = new CountDownLatch(1);
    CountDownLatch failPlanning = new CountDownLatch(1);

    when(instance.getId()).thenReturn(instanceId);
    when(instance.getFragment()).thenReturn(fragment);
    when(instance.getDataNodeFINum()).thenReturn(1);
    when(instance.getTimeOut()).thenReturn(TimeUnit.SECONDS.toMillis(30));
    when(fragment.getPlanNodeTree())
        .thenAnswer(
            ignored -> {
              planningStarted.countDown();
              if (!failPlanning.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting to fail planning");
              }
              throw new MetadataLeaseFencedException(FAILURE_MESSAGE, RETRY_UNTIL_SUCCESS);
            });

    FragmentInstanceManager manager = FragmentInstanceManager.getInstance();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<FragmentInstanceInfo> executionFuture =
          executor.submit(
              () ->
                  dataQuery
                      ? manager.execDataQueryFragmentInstance(
                          instance, mock(IDataRegionForQuery.class))
                      : manager.execSchemaQueryFragmentInstance(
                          instance, mock(ISchemaRegion.class)));

      assertTrue(planningStarted.await(10, TimeUnit.SECONDS));
      Future<FragmentInstanceInfo> cancellationFuture =
          executor.submit(() -> manager.cancelTask(instanceId, true));

      // cancelTask retains the internal cancellation context to reject late dispatch, while
      // getInstanceInfo preserves cancellation's public null lookup before the in-progress
      // execution-map compute completes. The failure path must still use its captured context.
      await().atMost(10, TimeUnit.SECONDS).until(() -> manager.getInstanceInfo(instanceId) == null);
      failPlanning.countDown();

      FragmentInstanceInfo failureInfo = executionFuture.get(10, TimeUnit.SECONDS);
      assertNotNull(failureInfo);
      assertTrue(failureInfo.getState().isFailed());
      assertEquals(FAILURE_MESSAGE, failureInfo.getMessage());
      assertTrue(failureInfo.getErrorCode().isPresent());
      assertEquals(
          TSStatusCode.METADATA_LEASE_FENCED_RETRY_REQUIRED.getStatusCode(),
          failureInfo.getErrorCode().get().getCode());
      assertNotNull(cancellationFuture.get(10, TimeUnit.SECONDS));
    } finally {
      failPlanning.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
      manager.cancelTask(instanceId, true);
    }
  }

  @Test
  public void cancelledBeforeDispatchRejectsLateDataQuery() throws Exception {
    assertCancelledIdentityRejectsLateDispatch(true);
  }

  @Test
  public void cancelledBeforeDispatchRejectsLateSchemaQuery() throws Exception {
    assertCancelledIdentityRejectsLateDispatch(false);
  }

  private void assertCancelledIdentityRejectsLateDispatch(boolean data) throws Exception {
    QueryId query = new QueryId("cancel_before_late_" + (data ? "data" : "schema"));
    FragmentInstanceId id = new FragmentInstanceId(new PlanFragmentId(query, 0), "0");
    FragmentInstanceManager manager = FragmentInstanceManager.getInstance();
    org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager exchange =
        org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService.getInstance()
            .getMPPDataExchangeManager();
    org.apache.iotdb.db.queryengine.execution.memory.MemoryPool pool =
        exchange.getLocalMemoryManager().getQueryPool();
    org.apache.iotdb.db.queryengine.execution.exchange.ResourceAuditManager audit =
        org.apache.iotdb.db.queryengine.execution.exchange.ResourceAuditManager.getInstance();
    String window = audit.openWindow(30_000, pool.getRemainingBytes(), 2048);
    try {
      assertNotNull(manager.cancelTask(id, true));
      assertTrue(FragmentInstanceManager.isFragmentCancellationRequested(id));
      FragmentInstance instance = mock(FragmentInstance.class);
      when(instance.getId()).thenReturn(id);
      when(instance.getTimeOut()).thenReturn(1000L);
      org.apache.iotdb.commons.exception.IoTDBRuntimeException failure =
          org.junit.Assert.assertThrows(
              org.apache.iotdb.commons.exception.IoTDBRuntimeException.class,
              () -> {
                if (data)
                  manager.execDataQueryFragmentInstance(instance, mock(IDataRegionForQuery.class));
                else manager.execSchemaQueryFragmentInstance(instance, mock(ISchemaRegion.class));
              });
      assertEquals(TSStatusCode.REPEATED_RPC_CALL.getStatusCode(), failure.getErrorCode());
      org.mockito.Mockito.verify(instance, org.mockito.Mockito.never()).getFragment();
      org.junit.Assert.assertFalse(FragmentInstanceManager.hasDataNodeQueryContext(query));
      org.junit.Assert.assertNull(manager.getInstanceInfo(id));
      java.util.Map<String, Object> snapshot =
          audit.snapshot(window, query.getId(), id.getFullId(), pool, 2048);
      assertEquals(true, snapshot.get("complete"));
      assertEquals(0, snapshot.get("createdDrivers"));
      assertEquals(0, snapshot.get("unconsumedFactories"));
      assertEquals(0, snapshot.get("liveQueues"));
      assertEquals(0, snapshot.get("pendingFutures"));
      assertEquals(false, snapshot.get("fragmentMapPresent"));
      assertEquals(true, snapshot.get("released"));
      assertNotNull(manager.cancelTask(id, true));
      assertTrue(FragmentInstanceManager.isFragmentCancellationRequested(id));
    } finally {
      audit.closeWindow(window);
      manager.cancelQuery(
          query,
          java.util.Collections.singletonList(id),
          true,
          FragmentInstanceManager.CancellationOrigin.LOCAL_CANCEL_TASK);
    }
  }
}
