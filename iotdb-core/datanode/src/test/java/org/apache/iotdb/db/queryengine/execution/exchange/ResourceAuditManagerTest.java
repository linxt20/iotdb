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

import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionResourceRegistry;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class ResourceAuditManagerTest {
  @Test
  public void liveControlReaderRequiresRecordedIdentityAndUnexpiredWindow() throws Exception {
    AtomicLong clock = new AtomicLong();
    ResourceAuditManager manager = manager(clock, 4, 8, 8);
    String window = manager.openWindow(100L, 1024L, 2048L);
    java.lang.reflect.Field published =
        org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager.class
            .getDeclaredField("publishedInstanceContext");
    published.setAccessible(true);
    Object previous = published.get(null);
    FragmentInstanceId fragment =
        new FragmentInstanceId(
            new org.apache.iotdb.db.queryengine.common.PlanFragmentId(
                new org.apache.iotdb.db.queryengine.common.QueryId("live_observation"), 0),
            "instance");
    org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext context =
        Mockito.mock(
            org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.class);
    Mockito.when(context.getId()).thenReturn(fragment);
    org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFragmentLifecycle lifecycle =
        new org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFragmentLifecycle(
            context, MoreExecutors.directExecutor());
    Mockito.when(context.getRuntimeLifecycle()).thenReturn(lifecycle);
    try {
      published.set(null, Map.of(fragment, context));
      Map<?, ?> missing =
          (Map<?, ?>)
              manager
                  .readRuntimeControl(window, "live_observation", fragment.getFullId())
                  .get("observation");
      assertEquals("FRAGMENT_NOT_RECORDED", missing.get("status"));
      manager.beginFragment("live_observation", fragment.getFullId());
      Map<String, Object> read =
          manager.readRuntimeControl(window, "live_observation", fragment.getFullId());
      assertEquals(true, read.get("auditRecordComplete"));
      Map<?, ?> observation = (Map<?, ?>) read.get("observation");
      assertEquals(fragment.getFullId(), observation.get("fragmentInstanceId"));
      assertEquals(false, observation.get("frozen"));
      assertEquals("NO_RUNTIME_GROUP", observation.get("status"));
      Map<?, ?> wrongQuery =
          (Map<?, ?>)
              manager
                  .readRuntimeControl(window, "another_query", fragment.getFullId())
                  .get("observation");
      assertEquals("FRAGMENT_NOT_RECORDED", wrongQuery.get("status"));
      published.set(null, Map.of());
      Map<?, ?> absent =
          (Map<?, ?>)
              manager
                  .readRuntimeControl(window, "live_observation", fragment.getFullId())
                  .get("observation");
      assertEquals("CONTEXT_NOT_AVAILABLE", absent.get("status"));
      clock.set(100L);
      assertThrows(
          IllegalArgumentException.class,
          () -> manager.readRuntimeControl(window, "live_observation", fragment.getFullId()));
      Mockito.verify(context, Mockito.never()).setRuntimeControlEvidence(Mockito.any());
    } finally {
      published.set(null, previous);
      manager.closeWindow(window);
    }
  }

  private MemoryPool pool() {
    return new MemoryPool("audit", new MemoryManager(1024L), 512L);
  }

  private ResourceAuditManager manager(AtomicLong time, int fragments, int objects, int futures) {
    return new ResourceAuditManager(time::get, 1, fragments, objects, futures);
  }

  private void receipt(ResourceAuditManager manager, boolean success, long before) {
    manager.recordDeregistration(
        "q",
        "q.0.0",
        new MemoryPool.DeregistrationReceipt(
            before, false, 0L, success, success ? null : "MemoryLeakException"));
  }

  @Test
  public void fullObservationIdReadsActualShortMemoryAccount() {
    ResourceAuditManager manager = manager(new AtomicLong(), 4, 8, 8);
    MemoryPool pool = pool();
    String id = manager.openWindow(100L, 1024L, 2048L);
    pool.registerPlanNodeIdToQueryMemoryMap("q", "0.0", "source");
    assertTrue(
        pool.reserveWithPriority("q", "0.0", "source", 256L, 512L, false).isReserveSuccess());
    manager.beginFragment("q", "q.0.0");
    Object driver = new Object();
    manager.driverCreated("q", "q.0.0", driver, "q.0.0.1");
    SettableFuture<Void> future = SettableFuture.create();
    manager.trackFuture("q", "0.0", future);
    Map<String, Object> running = manager.snapshot(id, "q", "q.0.0", pool, 2048L);
    assertEquals(256L, running.get("fragmentReservedBytes"));
    assertEquals(1, running.get("pendingFutures"));
    assertEquals(1, manager.listFragments(id, "q").size());
    assertEquals("q.0.0", manager.listFragments(id, "q").get(0).get("fragmentInstanceId"));
    assertEquals("0.0", running.get("memoryFragmentInstanceId"));
    assertEquals(false, running.get("released"));
    pool.free("q", "0.0", "source", 256L);
    pool.deRegisterFragmentInstanceFromQueryMemoryMap("q", "0.0", true);
    manager.recordDeregistration(
        "q", "0.0", new MemoryPool.DeregistrationReceipt(0L, false, 0L, true, null));
    manager.driverClosed("q", "q.0.0", driver);
    future.cancel(false);
    assertEquals(true, manager.snapshot(id, "q", "q.0.0", pool, 2048L).get("released"));
    manager.closeWindow(id);
  }

  @Test
  public void lateZeroAccountMustBeDeregisteredBeforeReleased() {
    ResourceAuditManager manager = manager(new AtomicLong(), 4, 8, 8);
    MemoryPool pool = pool();
    String id = manager.openWindow(100L, 1024L, 2048L);
    try {
      manager.beginFragment("q", "q.0.0");
      receipt(manager, true, 0L);
      assertEquals(true, manager.snapshot(id, "q", "q.0.0", pool, 2048L).get("released"));
      pool.registerPlanNodeIdToQueryMemoryMap("q", "0.0", "late-source");
      Map<String, Object> retained = manager.snapshot(id, "q", "q.0.0", pool, 2048L);
      assertEquals(0L, retained.get("fragmentReservedBytes"));
      assertEquals(true, retained.get("fragmentMapPresent"));
      assertEquals(false, retained.get("released"));
      pool.deRegisterFragmentInstanceFromQueryMemoryMap("q", "0.0", true);
      receipt(manager, true, 0L);
      assertEquals(true, manager.snapshot(id, "q", "q.0.0", pool, 2048L).get("released"));
    } finally {
      manager.closeWindow(id);
    }
  }

  @Test
  public void missingObservationNeverClaimsReleased() {
    ResourceAuditManager manager = manager(new AtomicLong(), 4, 8, 8);
    String id = manager.openWindow(100L, 1024L, 2048L);
    Map<String, Object> value = manager.snapshot(id, "q", "q.0.0", pool(), 2048L);
    assertEquals(false, value.get("complete"));
    assertEquals(false, value.get("released"));
    assertNull(value.get("deregisterReceipt"));
    manager.closeWindow(id);
  }

  @Test
  public void driverFactoryEndpointAndFutureMustAllFinish() {
    ResourceAuditManager manager = manager(new AtomicLong(), 4, 8, 8);
    String id = manager.openWindow(100L, 1024L, 2048L);
    manager.beginFragment("q", "q.0.0");
    Object driver = new Object();
    manager.driverCreated("q", "q.0.0", driver, "q.0.0.0");
    AtomicBoolean factoryDone = new AtomicBoolean(), endpointDone = new AtomicBoolean();
    manager.registerObject(
        "q", "q.0.0", new Object(), "FACTORY", factoryDone::get, null, null, null);
    manager.registerObject(
        "q", "q.0.0", new Object(), "SOURCE_HANDLE", endpointDone::get, "q", "q.0.0", "p");
    SettableFuture<Void> future = SettableFuture.create();
    manager.trackFuture("q", "q.0.0", future);
    receipt(manager, true, 0L);
    Map<String, Object> value = manager.snapshot(id, "q", "q.0.0", pool(), 2048L);
    assertEquals(1, value.get("createdDrivers"));
    assertEquals(0, value.get("closedDrivers"));
    assertEquals(1, value.get("liveDrivers"));
    assertEquals(1, value.get("unconsumedFactories"));
    assertEquals(1, value.get("liveEndpoints"));
    assertEquals(1, value.get("pendingFutures"));
    assertEquals(false, value.get("released"));
    factoryDone.set(true);
    endpointDone.set(true);
    future.cancel(false);
    manager.driverClosed("q", "q.0.0", driver);
    manager.driverClosed("q", "q.0.0", driver);
    value = manager.snapshot(id, "q", "q.0.0", pool(), 2048L);
    assertEquals(true, value.get("complete"));
    assertEquals(true, value.get("released"));
    assertEquals(1, value.get("closedDrivers"));
    assertEquals(0, value.get("pendingFutures"));
    manager.closeWindow(id);
  }

  @Test
  public void failedDeregistrationSurvivesLaterZeroReadAndSuccess() {
    ResourceAuditManager manager = manager(new AtomicLong(), 4, 8, 8);
    String id = manager.openWindow(100L, 1024L, 2048L);
    manager.beginFragment("q", "q.0.0");
    receipt(manager, false, 256L);
    receipt(manager, true, 0L);
    Map<String, Object> value = manager.snapshot(id, "q", "q.0.0", pool(), 2048L);
    assertEquals(0L, value.get("fragmentReservedBytes"));
    assertEquals(0L, value.get("queryReservedBytes"));
    assertEquals(1, value.get("deregisterFailures"));
    assertEquals(false, value.get("released"));
    manager.closeWindow(id);
  }

  @Test
  public void boundsMarkEvidenceIncompleteInsteadOfTruncatingSuccess() {
    ResourceAuditManager manager = manager(new AtomicLong(), 1, 1, 1);
    String id = manager.openWindow(100L, 1024L, 2048L);
    manager.beginFragment("q", "q.0.0");
    Object identity = new Object();
    manager.registerObject("q", "q.0.0", identity, "LOCAL_QUEUE", () -> true, "q", "q.0.0", "p");
    manager.registerObject("q", "q.0.0", identity, "LOCAL_QUEUE", () -> true, "q", "q.0.0", "p");
    receipt(manager, true, 0L);
    assertEquals(true, manager.snapshot(id, "q", "q.0.0", pool(), 2048L).get("released"));
    manager.registerObject(
        "q", "q.0.0", new Object(), "LOCAL_QUEUE", () -> true, "q", "q.0.0", "p2");
    assertEquals(false, manager.snapshot(id, "q", "q.0.0", pool(), 2048L).get("complete"));
    manager.closeWindow(id);
    id = manager.openWindow(100L, 1024L, 2048L);
    manager.beginFragment("q", "q.0.0");
    manager.beginFragment("q", "q.1.0");
    assertEquals(false, manager.snapshot(id, "q", "q.0.0", pool(), 2048L).get("complete"));
    manager.closeWindow(id);
    id = manager.openWindow(100L, 1024L, 2048L);
    manager.beginFragment("q", "q.0.0");
    manager.trackFuture("q", "q.0.0", SettableFuture.create());
    manager.trackFuture("q", "q.0.0", SettableFuture.create());
    assertEquals(false, manager.snapshot(id, "q", "q.0.0", pool(), 2048L).get("complete"));
    manager.closeWindow(id);
  }

  @Test
  public void expiredAndClosedWindowsReleaseCapacity() {
    AtomicLong clock = new AtomicLong();
    ResourceAuditManager manager = manager(clock, 4, 8, 8);
    String id = manager.openWindow(100L, 1024L, 2048L);
    assertThrows(IllegalStateException.class, () -> manager.openWindow(100L, 1024L, 2048L));
    clock.set(100L);
    assertThrows(IllegalArgumentException.class, () -> manager.listFragments(id, "q"));
    String replacement = manager.openWindow(100L, 1024L, 2048L);
    manager.closeWindow(replacement);
    assertThrows(IllegalArgumentException.class, () -> manager.listFragments(replacement, "q"));
    manager.closeWindow(manager.openWindow(100L, 1024L, 2048L));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void realQueueUsesProducerAccountAndOnePhysicalToken() {
    ResourceAuditManager manager = ResourceAuditManager.getInstance();
    MemoryPool pool = pool();
    String id = manager.openWindow(10_000L, pool.getRemainingBytes(), 2048L);
    TFragmentInstanceId producer = new TFragmentInstanceId("audit_query", 0, "0");
    String full = FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(producer);
    manager.beginFragment("audit_query", full);
    LocalMemoryManager memory = Mockito.mock(LocalMemoryManager.class);
    Mockito.when(memory.getQueryPool()).thenReturn(pool);
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(producer, "scan", memory, MoreExecutors.newDirectExecutorService());
    try {
      LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
      registry.bindResourceAudit(
          new FragmentInstanceId(
              new org.apache.iotdb.db.queryengine.common.PlanFragmentId(
                  new org.apache.iotdb.db.queryengine.common.QueryId("audit_query"), 1),
              "0"));
      registry.registerQueue(queue);
      registry.registerQueue(queue);
      queue.setMaxBytesCanReserve(512L);
      queue.allowAddingTsBlock();
      TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT64));
      builder.getTimeColumnBuilder().writeLong(1L);
      builder.getColumnBuilder(0).writeLong(2L);
      builder.declarePosition();
      queue.add(builder.build());
      Map<String, Object> running = manager.snapshot(id, "audit_query", full, pool, 2048L);
      assertEquals(1, running.get("liveQueues"));
      assertTrue((long) running.get("fragmentReservedBytes") > 0L);
      Map<String, Object> consumer =
          manager.snapshot(id, "audit_query", "audit_query.1.0", pool, 2048L);
      assertEquals(0L, consumer.get("fragmentReservedBytes"));
      List<Map<String, Object>> a = (List<Map<String, Object>>) running.get("objects"),
          b = (List<Map<String, Object>>) consumer.get("objects");
      assertEquals(1, a.size());
      assertEquals(1, b.size());
      assertEquals(a.get(0).get("token"), b.get(0).get("token"));
      assertEquals(full, b.get(0).get("memoryFragmentInstanceId"));
      queue.close();
      MemoryPool.FragmentReservationSnapshot before =
          pool.snapshotFragmentReservations("audit_query", full);
      pool.deRegisterFragmentInstanceFromQueryMemoryMap("audit_query", full, true);
      pool.recordDeregistrationReceipt(
          "audit_query",
          full,
          before.fragmentReservedBytes(),
          pool.snapshotFragmentReservations("audit_query", full),
          true,
          null);
      Map<String, Object> finished = manager.snapshot(id, "audit_query", full, pool, 2048L);
      assertEquals(true, finished.get("released"));
      assertEquals(1024L, finished.get("exchangeRemainingBytes"));
    } finally {
      queue.close();
      manager.closeWindow(id);
    }
  }

  @Test
  public void receiptRetentionIsBounded() {
    MemoryPool pool = pool();
    for (int i = 0; i < 4097; i++)
      pool.recordDeregistrationReceipt(
          "q",
          "q." + i + ".0",
          0L,
          new MemoryPool.FragmentReservationSnapshot(false, Collections.emptyMap(), 0L),
          true,
          null);
    assertNull(pool.getDeregistrationReceipt("q", "q.0.0"));
    assertNotNull(pool.getDeregistrationReceipt("q", "q.4096.0"));
  }

  @Test
  public void retainedNonForceAccountCanFinishWithZeroReceipt() {
    ResourceAuditManager manager = manager(new AtomicLong(), 4, 8, 8);
    String id = manager.openWindow(100L, 1024L, 2048L);
    manager.beginFragment("q", "q.0.0");
    manager.recordDeregistration(
        "q", "q.0.0", new MemoryPool.DeregistrationReceipt(256L, true, 256L, true, null));
    assertEquals(false, manager.snapshot(id, "q", "q.0.0", pool(), 2048L).get("released"));
    receipt(manager, true, 0L);
    assertEquals(true, manager.snapshot(id, "q", "q.0.0", pool(), 2048L).get("released"));
    manager.closeWindow(id);
  }

  @Test
  public void duplicateDriverTaskIdentityMarksObservationIncomplete() {
    ResourceAuditManager manager = manager(new AtomicLong(), 4, 8, 8);
    String id = manager.openWindow(100L, 1024L, 2048L);
    manager.beginFragment("q", "q.0.0");
    manager.driverCreated("q", "q.0.0", new Object(), new String("q.0.0.0"));
    manager.driverCreated("q", "q.0.0", new Object(), new String("q.0.0.0"));
    assertEquals(false, manager.snapshot(id, "q", "q.0.0", pool(), 2048L).get("complete"));
    manager.closeWindow(id);
  }

  @Test
  public void runtimeViewKeepsBothKeysAndDoesNotMergeEndpointTerminals() {
    ResourceAuditManager manager = manager(new AtomicLong(), 4, 8, 8);
    String window = manager.openWindow(100L, 1024L, 2048L);
    MemoryPool pool = pool();
    SharedTsBlockQueue.MemoryReservationKey queueKey =
        new SharedTsBlockQueue.MemoryReservationKey("q", "q.0.0", "lane");
    SharedTsBlockQueue.MemoryReservationKey actualKey =
        new SharedTsBlockQueue.MemoryReservationKey("q", "q.0.0", "group");
    org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation owner =
        org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation.tryCreate(
                pool, actualKey, 2, 32, 512)
            .get();
    Object source = new Object(), sink = new Object();
    AtomicBoolean sourceClosed = new AtomicBoolean();
    try {
      manager.beginFragment("q", "q.0.0");
      manager.registerRuntimeFrontier("q", "q.0.0", "group", owner);
      manager.registerObject(
          "q", "q.0.0", source, "SOURCE_HANDLE", sourceClosed::get, queueKey, actualKey);
      manager.registerObject("q", "q.0.0", sink, "SINK_CHANNEL", () -> false, queueKey, actualKey);
      // A kind alias and freshly allocated terminal supplier are not mapping conflicts.
      manager.registerObject("q", "q.0.0", sink, "SINK", () -> false, queueKey, actualKey);
      org.apache.iotdb.db.queryengine.execution.runtime.RuntimeResourceSnapshot before =
          manager.snapshotRuntime(window, "q", "q.0.0", pool);
      assertEquals(2, before.resources().size());
      assertEquals(1, before.accounts().size());
      assertEquals(Long.valueOf(64), before.actualReservedBytes());
      assertEquals(queueKey, before.resources().get(0).queueIdentityKey());
      assertEquals(actualKey, before.resources().get(0).actualAccountKey());
      assertTrue(before.complete());
      sourceClosed.set(true);
      org.apache.iotdb.db.queryengine.execution.runtime.RuntimeResourceSnapshot after =
          manager.snapshotRuntime(window, "q", "q.0.0", pool);
      assertTrue(after.resources().get(0).terminal());
      assertFalse(after.resources().get(1).terminal());
      assertEquals(1, manager.snapshot(window, "q", "q.0.0", pool, 2048L).get("liveEndpoints"));
      assertEquals(0, manager.snapshot(window, "q", "q.0.0", pool, 2048L).get("liveQueues"));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              manager.registerObject(
                  "q", "q.0.0", source, "SOURCE_HANDLE", sourceClosed::get, queueKey, queueKey));
      assertEquals(
          actualKey,
          manager
              .snapshotRuntime(window, "q", "q.0.0", pool)
              .resources()
              .get(0)
              .actualAccountKey());
      assertThrows(
          IllegalArgumentException.class,
          () -> manager.snapshotRuntime(window, "q", "q.0.0", pool()));
    } finally {
      owner.close();
      manager.closeWindow(window);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void frontierQueueConstructorRegistersActualAccountOnTheFirstAuditEvent() {
    ResourceAuditManager manager = ResourceAuditManager.getInstance();
    String window = manager.openWindow(30_000L, 1024L, 2048L);
    MemoryPool pool = pool();
    TFragmentInstanceId fragment = new TFragmentInstanceId("frontier_first", 0, "0");
    String full = FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(fragment);
    SharedTsBlockQueue.MemoryReservationKey actual =
        new SharedTsBlockQueue.MemoryReservationKey("frontier_first", full, "group");
    org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation owner =
        org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation.tryCreate(
                pool, actual, 1, 64, 512)
            .get();
    LocalMemoryManager memory = Mockito.mock(LocalMemoryManager.class);
    Mockito.when(memory.getQueryPool()).thenReturn(pool);
    java.util.concurrent.ExecutorService executor = MoreExecutors.newDirectExecutorService();
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(fragment, "lane", memory, executor, false, owner, 0);
    try {
      manager.beginFragment("frontier_first", full);
      manager.registerRuntimeFrontier("frontier_first", full, "group", owner);
      org.apache.iotdb.db.queryengine.execution.runtime.RuntimeResourceSnapshot runtime =
          manager.snapshotRuntime(window, "frontier_first", full, pool);
      assertEquals(1, runtime.resources().size());
      assertEquals(queue.getQueueIdentityKey(), runtime.resources().get(0).queueIdentityKey());
      assertEquals(actual, runtime.resources().get(0).actualAccountKey());
      assertEquals(Long.valueOf(64), runtime.actualReservedBytes());
      List<Map<String, Object>> oldObjects =
          (List<Map<String, Object>>)
              manager.snapshot(window, "frontier_first", full, pool, 2048L).get("objects");
      assertEquals("lane", oldObjects.get(0).get("memoryPlanNodeId"));
      manager.registerObject(
          "frontier_first",
          full,
          queue,
          "LOCAL_QUEUE",
          queue::isClosed,
          queue.getQueueIdentityKey(),
          queue.getActualAccountKey());
      assertEquals(
          1, manager.snapshotRuntime(window, "frontier_first", full, pool).resources().size());
    } finally {
      queue.close();
      owner.close();
      manager.closeWindow(window);
      executor.shutdownNow();
    }
  }

  @Test
  public void runtimeGroupOwnershipSurvivesObservationObjectOverflow() {
    ResourceAuditManager manager = manager(new AtomicLong(), 4, 1, 8);
    String window = manager.openWindow(100L, 1024L, 2048L);
    MemoryPool pool = pool();
    SharedTsBlockQueue.MemoryReservationKey account =
        new SharedTsBlockQueue.MemoryReservationKey("q", "q.0.0", "group");
    org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation owner =
        org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation.tryCreate(
                pool, account, 1, 64, 512)
            .get();
    try {
      manager.beginFragment("q", "q.0.0");
      manager.registerRuntimeFrontier("q", "q.0.0", "group", owner);
      manager.registerObject("q", "q.0.0", new Object(), "SOURCE", () -> false, account, account);
      manager.registerObject("q", "q.0.0", new Object(), "SOURCE", () -> false, account, account);
      org.apache.iotdb.db.queryengine.execution.runtime.RuntimeResourceSnapshot runtime =
          manager.snapshotRuntime(window, "q", "q.0.0", pool);
      assertFalse(runtime.complete());
      assertEquals(1, runtime.groups().size());
      assertEquals(Long.valueOf(64), runtime.actualReservedBytes());
      assertThrows(
          IllegalArgumentException.class,
          () -> manager.registerRuntimeFrontier("q", "q.0.0", "other-id", owner));
    } finally {
      owner.close();
      manager.closeWindow(window);
    }
  }
}
