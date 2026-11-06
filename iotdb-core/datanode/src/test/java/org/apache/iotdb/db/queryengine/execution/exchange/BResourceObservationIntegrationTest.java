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

package org.apache.iotdb.db.queryengine.execution.exchange;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.PipelineSourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.*;

public class BResourceObservationIntegrationTest {
  @Test
  public void pipelineWithoutNetworkIdentityCanWaitInsideAuditWindow() {
    ResourceAuditManager audit = ResourceAuditManager.getInstance();
    String window = audit.openWindow(30_000L, 1024L, 2048L);
    MemoryPool pool = new MemoryPool("audit", new MemoryManager(1024L), 512L);
    LocalMemoryManager memory = Mockito.mock(LocalMemoryManager.class);
    Mockito.when(memory.getQueryPool()).thenReturn(pool);
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            new org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId("pipeline_audit", 0, "0"),
            "scan",
            memory,
            MoreExecutors.newDirectExecutorService());
    try {
      audit.beginFragment("pipeline_audit", "pipeline_audit.0.0");
      PipelineSourceHandle source =
          new PipelineSourceHandle(
              queue,
              Mockito.mock(MPPDataExchangeManager.SourceHandleListener.class),
              "audit-pipeline");
      assertFalse(source.isBlocked().isDone());
      assertEquals(
          1,
          audit
              .snapshot(window, "pipeline_audit", "pipeline_audit.0.0", pool, 2048L)
              .get("pendingFutures"));
      source.close();
      assertEquals(
          0,
          audit
              .snapshot(window, "pipeline_audit", "pipeline_audit.0.0", pool, 2048L)
              .get("pendingFutures"));
    } finally {
      queue.close();
      audit.closeWindow(window);
    }
  }

  @Test
  public void queueIsPublishedBeforeSinkConstructionAndAbortedIfPublicationFails() {
    ExecutorService executor = MoreExecutors.newDirectExecutorService();
    MemoryPool pool = new MemoryPool("audit", new MemoryManager(1024L), 512L);
    LocalMemoryManager memory = Mockito.mock(LocalMemoryManager.class);
    Mockito.when(memory.getQueryPool()).thenReturn(pool);
    MPPDataExchangeManager manager =
        new MPPDataExchangeManager(
            memory, new TsBlockSerdeFactory(), executor, Mockito.mock(IClientManager.class));
    FragmentInstanceId id =
        new FragmentInstanceId(new PlanFragmentId(new QueryId("queue_ownership"), 0), "0");
    FragmentInstanceContext fragment =
        FragmentInstanceContext.createFragmentInstanceContext(
            id, new FragmentInstanceStateMachine(id, executor));
    org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext context =
        new org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext(fragment, 0);
    java.util.concurrent.atomic.AtomicReference<SharedTsBlockQueue> observed =
        new java.util.concurrent.atomic.AtomicReference<>();
    RuntimeException failure = new RuntimeException("queue ownership callback failed");
    try {
      assertSame(
          failure,
          assertThrows(
              RuntimeException.class,
              () ->
                  manager.createLocalSinkChannelForPipeline(
                      context,
                      "source",
                      queue -> {
                        observed.set(queue);
                        assertFalse(queue.isClosed());
                        throw failure;
                      })));
      assertNotNull(observed.get());
      assertTrue(observed.get().isClosed());
      assertEquals(1024L, pool.getRemainingBytes());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void cancelledRegistryRejectsAndClosesLatePipelineQueue() {
    ExecutorService executor = MoreExecutors.newDirectExecutorService();
    MemoryPool pool = new MemoryPool("audit", new MemoryManager(1024L), 512L);
    LocalMemoryManager memory = Mockito.mock(LocalMemoryManager.class);
    Mockito.when(memory.getQueryPool()).thenReturn(pool);
    MPPDataExchangeManager manager =
        new MPPDataExchangeManager(
            memory, new TsBlockSerdeFactory(), executor, Mockito.mock(IClientManager.class));
    FragmentInstanceId id =
        new FragmentInstanceId(new PlanFragmentId(new QueryId("late_queue"), 0), "0");
    FragmentInstanceContext fragment =
        FragmentInstanceContext.createFragmentInstanceContext(
            id, new FragmentInstanceStateMachine(id, executor));
    org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext context =
        new org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext(fragment, 0);
    org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionResourceRegistry registry =
        new org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionResourceRegistry();
    registry.abortUntransferred(new RuntimeException("cancel before construction"));
    java.util.concurrent.atomic.AtomicReference<SharedTsBlockQueue> observed =
        new java.util.concurrent.atomic.AtomicReference<>();
    try {
      assertThrows(
          IllegalStateException.class,
          () ->
              manager.createLocalSinkChannelForPipeline(
                  context,
                  "source",
                  queue -> {
                    observed.set(queue);
                    registry.registerQueue(queue);
                  }));
      assertNotNull(observed.get());
      assertTrue(observed.get().isClosed());
      assertEquals(0, registry.getObjectCount());
      assertEquals(1024L, pool.getRemainingBytes());
      assertThrows(IllegalStateException.class, () -> registry.freeze(1024L, 512L));
      assertThrows(
          IllegalStateException.class,
          () ->
              registry.recordLegacySourceQuota(
                  Mockito.mock(
                      org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle
                          .class),
                  512L));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void laterChannelConstructionFailureAbortsEarlierChannels() {
    ResourceAuditManager audit = ResourceAuditManager.getInstance();
    String window = audit.openWindow(30_000L, 1024L, 2048L);
    ExecutorService executor = MoreExecutors.newDirectExecutorService();
    MemoryPool pool = new MemoryPool("audit", new MemoryManager(1024L), 512L);
    LocalMemoryManager memory = Mockito.mock(LocalMemoryManager.class);
    Mockito.when(memory.getQueryPool()).thenReturn(pool);
    FragmentInstanceId id =
        new FragmentInstanceId(new PlanFragmentId(new QueryId("partial_channel_audit"), 0), "0");
    FragmentInstanceContext context =
        FragmentInstanceContext.createFragmentInstanceContext(
            id, new FragmentInstanceStateMachine(id, executor));
    MPPDataExchangeManager manager =
        new MPPDataExchangeManager(
            memory, new TsBlockSerdeFactory(), executor, Mockito.mock(IClientManager.class));
    audit.beginFragment(id.getQueryId().getId(), id.getFullId());
    try {
      DownStreamChannelLocation first =
          new DownStreamChannelLocation(
              new TEndPoint("198.51.100.1", 65000),
              new org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId(
                  "partial_channel_audit", 1, "0"),
              "exchange");
      assertThrows(
          NullPointerException.class,
          () ->
              manager.createShuffleSinkHandle(
                  Arrays.asList(first, null),
                  new DownStreamChannelIndex(0),
                  ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
                  id.toThrift(),
                  "sink",
                  context));
      Map<String, Object> snapshot =
          audit.snapshot(window, id.getQueryId().getId(), id.getFullId(), pool, 2048L);
      assertEquals(0, snapshot.get("liveEndpoints"));
      java.util.List<Map<String, Object>> objects =
          (java.util.List<Map<String, Object>>) snapshot.get("objects");
      assertEquals(1, objects.size());
      assertEquals(true, objects.get(0).get("terminal"));
    } finally {
      audit.closeWindow(window);
      executor.shutdownNow();
    }
  }

  @Test
  public void cancelledProducerRejectsLateFragmentQueue() {
    org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager fragments =
        org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager.getInstance();
    MPPDataExchangeManager exchange =
        MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
    FragmentInstanceId producer = lateId("cancel_late_fragment_queue", 0);
    FragmentInstanceId consumer = lateId("cancel_late_fragment_queue", 1);
    MemoryPool pool = exchange.getLocalMemoryManager().getQueryPool();
    int sourcesBefore = exchange.getSourceHandleSize();
    try {
      assertNotNull(fragments.cancelTask(producer, true));
      assertThrows(
          java.util.concurrent.CancellationException.class,
          () ->
              exchange.createLocalSourceHandleForFragment(
                  consumer.toThrift(),
                  "late-exchange",
                  "late-sink",
                  producer.toThrift(),
                  0,
                  t -> {}));
      assertEquals(sourcesBefore, exchange.getSourceHandleSize());
      assertNoLateReservations(pool, producer);
      assertNoLateReservations(pool, consumer);
      assertFalse(
          org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager
              .hasDataNodeQueryContext(producer.getQueryId()));
    } finally {
      fragments.cancelQuery(
          producer.getQueryId(),
          java.util.Collections.singletonList(producer),
          true,
          org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager
              .CancellationOrigin.LOCAL_CANCEL_TASK);
      exchange.forceDeregisterFragmentInstance(consumer.toThrift());
    }
  }

  @Test
  public void cancelledFragmentRejectsLatePipelineQueue() {
    org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager fragments =
        org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager.getInstance();
    MPPDataExchangeManager exchange =
        MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
    FragmentInstanceId id = lateId("cancel_late_pipeline_queue", 0);
    ExecutorService executor = MoreExecutors.newDirectExecutorService();
    java.util.concurrent.atomic.AtomicBoolean published =
        new java.util.concurrent.atomic.AtomicBoolean();
    try {
      fragments.cancelTask(id, true);
      FragmentInstanceContext fragment =
          FragmentInstanceContext.createFragmentInstanceContext(
              id, new FragmentInstanceStateMachine(id, executor));
      org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext driver =
          new org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext(fragment, 0);
      assertThrows(
          java.util.concurrent.CancellationException.class,
          () ->
              exchange.createLocalSinkChannelForPipeline(
                  driver, "late-source", queue -> published.set(true)));
      assertFalse(published.get());
      assertNoLateReservations(exchange.getLocalMemoryManager().getQueryPool(), id);
      assertTrue(
          org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager
              .isFragmentCancellationRequested(id));
    } finally {
      fragments.cancelQuery(
          id.getQueryId(),
          java.util.Collections.singletonList(id),
          true,
          org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager
              .CancellationOrigin.LOCAL_CANCEL_TASK);
      executor.shutdownNow();
    }
  }

  @Test
  public void existingQueueAbortedWhenProducerCancelled() {
    org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager fragments =
        org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager.getInstance();
    MPPDataExchangeManager exchange =
        MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
    FragmentInstanceId producer = lateId("existing_queue_cancel_producer", 0);
    FragmentInstanceId consumer = lateId("existing_queue_cancel_producer", 1);
    MemoryPool pool = exchange.getLocalMemoryManager().getQueryPool();
    org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle source =
        (org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle)
            exchange.createLocalSourceHandleForFragment(
                consumer.toThrift(), "exchange", "producer-sink", producer.toThrift(), 0, t -> {});
    SharedTsBlockQueue queue = source.getSharedTsBlockQueue();
    com.google.common.util.concurrent.ListenableFuture<Void> waiting = queue.getCanAddTsBlock();
    try {
      assertFalse(waiting.isDone());
      org.apache.tsfile.read.common.block.TsBlockBuilder builder =
          new org.apache.tsfile.read.common.block.TsBlockBuilder(
              java.util.Collections.singletonList(org.apache.tsfile.enums.TSDataType.INT64));
      builder.getTimeColumnBuilder().writeLong(1L);
      builder.getColumnBuilder(0).writeLong(7L);
      builder.declarePosition();
      queue.add(builder.build());
      assertTrue(queue.getBufferRetainedSizeInBytes() > 0);
      assertTrue(
          pool.snapshotFragmentReservations(
                  producer.getQueryId().getId(), producer.getFragmentInstanceId())
              .present());
      assertTrue(pool.getQueryMemoryReservedBytes(producer.getQueryId().getId()) > 0);
      assertNotNull(fragments.cancelTask(producer, true));
      assertTrue(queue.isClosed());
      assertTrue(source.isAborted() || source.isClosed());
      assertTrue(waiting.isDone());
      assertEquals(0L, queue.getBufferRetainedSizeInBytes());
      assertNoLateReservations(pool, producer);
      assertThrows(
          java.util.concurrent.CancellationException.class,
          () ->
              exchange.createLocalSourceHandleForFragment(
                  consumer.toThrift(),
                  "late-exchange",
                  "producer-sink",
                  producer.toThrift(),
                  0,
                  t -> {}));
      assertNotNull(fragments.cancelTask(producer, true));
      assertNoLateReservations(pool, producer);
    } finally {
      source.abort();
      exchange.forceDeregisterFragmentInstance(consumer.toThrift());
      exchange.deRegisterFragmentInstanceFromMemoryPool(
          consumer.getQueryId().getId(), consumer.getFragmentInstanceId(), true);
      fragments.cancelQuery(
          producer.getQueryId(),
          java.util.Collections.singletonList(producer),
          true,
          org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager
              .CancellationOrigin.LOCAL_CANCEL_TASK);
    }
  }

  private static FragmentInstanceId lateId(String query, int fragment) {
    return new FragmentInstanceId(new PlanFragmentId(new QueryId(query), fragment), "0");
  }

  private static void assertNoLateReservations(MemoryPool pool, FragmentInstanceId id) {
    MemoryPool.FragmentReservationSnapshot snapshot =
        pool.snapshotFragmentReservations(id.getQueryId().getId(), id.getFragmentInstanceId());
    assertFalse(snapshot.present());
    assertEquals(0L, snapshot.fragmentReservedBytes());
    assertTrue(snapshot.byPlanNode().isEmpty());
    assertEquals(0L, pool.getQueryMemoryReservedBytes(id.getQueryId().getId()));
  }
}
