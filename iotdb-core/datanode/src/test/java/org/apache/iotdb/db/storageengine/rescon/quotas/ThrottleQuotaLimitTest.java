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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.rescon.quotas;

import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TThrottleQuota;
import org.apache.iotdb.db.storageengine.rescon.quotas.ThrottleQuotaLimit.AdmissionTicket;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ThrottleQuotaLimitTest {

  @Test
  public void testLegacyChecksAndInclusiveReservation() {
    ThrottleQuotaLimit limit = new ThrottleQuotaLimit();
    limit.setQuotas(quota("alice", 2, 100));
    assertTrue(limit.checkCpu("alice", 1));
    assertFalse(limit.checkCpu("alice", 2));
    assertTrue(limit.checkMemory("alice", 99));
    assertFalse(limit.checkMemory("alice", 100));

    AdmissionTicket ticket = limit.tryReserve("alice", 2, 100);
    assertNotNull(ticket);
    assertTotals(limit, "alice", 2, 100);
    assertNull(limit.tryReserve("alice", 1, 0));
    assertNull(limit.tryReserve("alice", 0, 1));
    ticket.close();
    assertTotals(limit, "alice", 0, 0);
  }

  @Test
  public void testAtomicSharedUserResources() {
    ThrottleQuotaLimit limit = new ThrottleQuotaLimit();
    limit.setQuotas(quota("alice", 3, 100));
    AdmissionTicket pending = limit.tryReserve("alice", 2, 50);
    assertNotNull(pending);
    assertNull(limit.tryReserve("alice", 2, 1));
    assertNull(limit.tryReserve("alice", 1, 51));
    assertTotals(limit, "alice", 2, 50);

    AdmissionTicket running = limit.tryReserve("alice", 1, 50);
    AdmissionTicket otherUser = limit.tryReserve("bob", 100, 1000);
    assertNotNull(running);
    assertNotNull(otherUser);
    assertTotals(limit, "alice", 3, 100);
    assertTotals(limit, "bob", 100, 1000);
    pending.close();
    running.close();
    otherUser.close();
    assertTotals(limit, "alice", 0, 0);
    assertTotals(limit, "bob", 0, 0);
  }

  @Test
  public void testInvalidRequestsUnlimitedAndOverflow() {
    ThrottleQuotaLimit limit = new ThrottleQuotaLimit();
    assertNull(limit.tryReserve(null, 1, 1));
    assertNull(limit.tryReserve("", 1, 1));
    assertNull(limit.tryReserve("  ", 1, 1));
    assertNull(limit.tryReserve("alice", -1, 0));
    assertNull(limit.tryReserve("alice", 0, -1));
    assertTotals(limit, "alice", 0, 0);

    limit.setQuotas(quota("alice", 0, 0));
    assertTrue(limit.checkCpu("alice", Integer.MAX_VALUE));
    assertTrue(limit.checkMemory("alice", Long.MAX_VALUE));
    AdmissionTicket cpuMaximum = limit.tryReserve("alice", Long.MAX_VALUE, 7);
    assertNotNull(cpuMaximum);
    assertNull(limit.tryReserve("alice", 1, 2));
    assertTotals(limit, "alice", Long.MAX_VALUE, 7);
    cpuMaximum.close();

    AdmissionTicket memoryMaximum = limit.tryReserve("alice", 3, Long.MAX_VALUE);
    assertNotNull(memoryMaximum);
    assertNull(limit.tryReserve("alice", 1, 1));
    assertTotals(limit, "alice", 3, Long.MAX_VALUE);
    memoryMaximum.close();

    AdmissionTicket absentLimit = limit.tryReserve("bob", Long.MAX_VALUE, Long.MAX_VALUE);
    assertNotNull(absentLimit);
    absentLimit.close();
    assertTotals(limit, "alice", 0, 0);
    assertTotals(limit, "bob", 0, 0);
  }

  @Test
  public void testPartialSplitAndIndependentClose() {
    ThrottleQuotaLimit limit = new ThrottleQuotaLimit();
    AdmissionTicket parent = limit.tryReserve("alice", 3, 90);
    assertNotNull(parent);
    assertNull(parent.split(-1, 0));
    assertNull(parent.split(0, -1));
    assertNull(parent.split(4, 0));
    assertNull(parent.split(0, 91));
    assertEquals(3, parent.getCpuCount());
    assertEquals(90, parent.getMemoryBytes());

    AdmissionTicket worker = parent.split(2, 0);
    AdmissionTicket baseMemory = parent.split(0, 90);
    assertNotNull(worker);
    assertNotNull(baseMemory);
    assertTotals(limit, "alice", 3, 90);
    assertEquals(1, parent.getCpuCount());
    assertEquals(0, parent.getMemoryBytes());
    assertNull(parent.split(2, 0));
    assertNull(parent.split(0, 1));
    worker.close();
    worker.close();
    assertTotals(limit, "alice", 1, 90);
    assertNull(worker.split(0, 0));

    parent.close();
    parent.close();
    assertNull(parent.split(0, 0));
    assertTotals(limit, "alice", 0, 90);
    baseMemory.close();
    baseMemory.close();
    assertTotals(limit, "alice", 0, 0);
  }

  @Test
  public void testLoweringQuotasPreservesOwnership() {
    ThrottleQuotaLimit limit = new ThrottleQuotaLimit();
    limit.setQuotas(quota("alice", 5, 300));
    AdmissionTicket ticket = limit.tryReserve("alice", 4, 200);
    assertNotNull(ticket);
    limit.setQuotas(quota("alice", 1, 100));
    assertTotals(limit, "alice", 4, 200);
    assertNull(limit.tryReserve("alice", 1, 0));
    assertNull(limit.tryReserve("alice", 0, 1));

    AdmissionTicket empty = limit.tryReserve("alice", 0, 0);
    AdmissionTicket worker = ticket.split(1, 0);
    assertNotNull(empty);
    assertNotNull(worker);
    worker.close();
    empty.close();
    assertTotals(limit, "alice", 3, 200);
    ticket.close();
    AdmissionTicket exact = limit.tryReserve("alice", 1, 100);
    assertNotNull(exact);
    exact.close();
    assertTotals(limit, "alice", 0, 0);
  }

  @Test(timeout = 10000)
  public void testConcurrentWholeBatchAdmission() throws Exception {
    ThrottleQuotaLimit limit = new ThrottleQuotaLimit();
    limit.setQuotas(quota("alice", 8, 80));
    ExecutorService executor = Executors.newFixedThreadPool(16);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<AdmissionTicket>> futures = new ArrayList<>();
    List<AdmissionTicket> accepted = new ArrayList<>();
    try {
      for (int i = 0; i < 32; i++) {
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  return limit.tryReserve("alice", 1, 10);
                }));
      }
      start.countDown();
      for (Future<AdmissionTicket> future : futures) {
        AdmissionTicket ticket = future.get(5, TimeUnit.SECONDS);
        if (ticket != null) {
          accepted.add(ticket);
        }
      }
      assertEquals(8, accepted.size());
      assertTotals(limit, "alice", 8, 80);
    } finally {
      start.countDown();
      for (AdmissionTicket ticket : accepted) {
        ticket.close();
      }
      executor.shutdownNow();
    }
    assertTotals(limit, "alice", 0, 0);
  }

  @Test(timeout = 10000)
  public void testSetQuotasAndAdmissionUseOneAtomicSnapshot() throws Exception {
    ThrottleQuotaLimit limit = new ThrottleQuotaLimit();
    limit.setQuotas(quota("alice", 100, 1));
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch start = new CountDownLatch(1);
    try {
      Future<?> setter =
          executor.submit(
              () -> {
                start.await();
                for (int i = 0; i < 3000; i++) {
                  limit.setQuotas(quota("alice", 1, 100));
                  limit.setQuotas(quota("alice", 100, 1));
                }
                return null;
              });
      Future<Integer> unexpectedAdmissions =
          executor.submit(
              () -> {
                start.await();
                int admitted = 0;
                for (int i = 0; i < 3000; i++) {
                  AdmissionTicket ticket = limit.tryReserve("alice", 2, 2);
                  if (ticket != null) {
                    admitted++;
                    ticket.close();
                  }
                }
                return admitted;
              });
      start.countDown();
      setter.get(5, TimeUnit.SECONDS);
      assertEquals(0, unexpectedAdmissions.get(5, TimeUnit.SECONDS).intValue());
    } finally {
      start.countDown();
      executor.shutdownNow();
    }
    assertTotals(limit, "alice", 0, 0);
  }

  @Test(timeout = 10000)
  public void testConcurrentSplitAndCloseAreIdempotent() throws Exception {
    ThrottleQuotaLimit limit = new ThrottleQuotaLimit();
    ExecutorService executor = Executors.newFixedThreadPool(8);
    try {
      for (int round = 0; round < 20; round++) {
        AdmissionTicket parent = limit.tryReserve("alice", 16, 160);
        assertNotNull(parent);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 16; i++) {
          futures.add(
              executor.submit(
                  () -> {
                    start.await();
                    AdmissionTicket child = parent.split(1, 10);
                    if (child != null) {
                      child.close();
                      child.close();
                    }
                    return null;
                  }));
        }
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  parent.close();
                  parent.close();
                  return null;
                }));
        start.countDown();
        for (Future<?> future : futures) {
          future.get(5, TimeUnit.SECONDS);
        }
        assertTotals(limit, "alice", 0, 0);
        assertNull(parent.split(0, 0));
      }
    } finally {
      executor.shutdownNow();
    }
  }

  private static TSetThrottleQuotaReq quota(String userName, int cpuCount, long memoryBytes) {
    TThrottleQuota quota = new TThrottleQuota();
    quota.setThrottleLimit(Collections.emptyMap());
    quota.setCpuLimit(cpuCount);
    quota.setMemLimit(memoryBytes);
    TSetThrottleQuotaReq request = new TSetThrottleQuotaReq();
    request.setUserName(userName);
    request.setThrottleQuota(quota);
    return request;
  }

  private static void assertTotals(
      ThrottleQuotaLimit limit, String userName, long cpuCount, long memoryBytes) {
    assertEquals(cpuCount, limit.getReservedCpu(userName));
    assertEquals(memoryBytes, limit.getReservedMemory(userName));
  }
}
