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
}
