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
import org.apache.iotdb.db.storageengine.rescon.quotas.ThrottleQuotaLimit;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.queryengine.common.QueryId.MOCK_QUERY_ID;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class FragmentQuotaMemoryLeaseTest {
  @Test
  public void memoryRemainsChargedUntilAllDriversAndPhysicalCleanupFinish() throws Exception {
    ExecutorService executor = executor();
    try {
      FragmentInstanceContext context = context(executor);
      context.initializeNumOfDrivers(1);
      ThrottleQuotaLimit quota = new ThrottleQuotaLimit();
      ThrottleQuotaLimit.AdmissionTicket base = quota.tryReserve("user", 0, 64);
      assertTrue(context.registerQuotaMemoryLease(base));
      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      doAnswer(
              call -> {
                entered.countDown();
                await(release);
                return null;
              })
          .when(context)
          .releaseResource();
      Future<?> cleanup = executor.submit(context::releaseResourceWhenAllDriversAreClosed);
      assertEquals(64, quota.getReservedMemory("user"));
      assertFalse(cleanup.isDone());
      context.decrementNumOfUnClosedDriver();
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      assertEquals(64, quota.getReservedMemory("user"));
      assertFalse(context.registerQuotaMemoryLease(() -> {}));
      release.countDown();
      cleanup.get(5, TimeUnit.SECONDS);
      assertEquals(0, quota.getReservedMemory("user"));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void cleanupOfAnInterruptedCallerTerminatesAndRestoresTheFlag() throws Exception {
    ExecutorService executor = executor();
    try {
      FragmentInstanceContext context = context(executor);
      context.initializeNumOfDrivers(0);
      ThrottleQuotaLimit quota = new ThrottleQuotaLimit();
      assertTrue(context.registerQuotaMemoryLease(quota.tryReserve("user", 0, 32)));
      AtomicBoolean restored = new AtomicBoolean();
      Future<?> task =
          executor.submit(
              () -> {
                Thread.currentThread().interrupt();
                context.releaseResourceWhenAllDriversAreClosed();
                restored.set(Thread.currentThread().isInterrupted());
              });
      task.get(5, TimeUnit.SECONDS);
      assertTrue(restored.get());
      assertEquals(0, quota.getReservedMemory("user"));
      verify(context).releaseResource();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void physicalFailureIsCachedAndPartialCleanupIsNeverReplayed() {
    ExecutorService executor = executor();
    try {
      FragmentInstanceContext context = context(executor);
      ThrottleQuotaLimit quota = new ThrottleQuotaLimit();
      assertTrue(context.registerQuotaMemoryLease(quota.tryReserve("user", 0, 32)));
      IllegalStateException expected = new IllegalStateException("partial physical cleanup");
      doThrow(expected).doNothing().when(context).releaseResource();
      for (int attempt = 0; attempt < 2; attempt++) {
        assertSame(
            expected,
            assertThrows(
                IllegalStateException.class, context::releaseResourceWhenAllDriversAreClosed));
      }
      assertEquals(32, quota.getReservedMemory("user"));
      assertFalse(context.registerQuotaMemoryLease(() -> {}));
      verify(context, times(1)).releaseResource();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void callbacksAreClaimedOnceAndFailuresRemainObservable() {
    ExecutorService executor = executor();
    try {
      FragmentInstanceContext context = context(executor);
      AtomicInteger releases = new AtomicInteger();
      AtomicInteger failedAttempts = new AtomicInteger();
      AssertionError expected = new AssertionError("callback failed");
      assertTrue(
          context.registerQuotaMemoryLease(
              () -> {
                failedAttempts.incrementAndGet();
                throw expected;
              }));
      assertTrue(context.registerQuotaMemoryLease(releases::incrementAndGet));
      for (int attempt = 0; attempt < 2; attempt++) {
        assertSame(
            expected,
            assertThrows(AssertionError.class, context::releaseResourceWhenAllDriversAreClosed));
      }
      assertEquals(1, failedAttempts.get());
      assertEquals(1, releases.get());
      verify(context, times(1)).releaseResource();
      assertFalse(context.registerQuotaMemoryLease(() -> {}));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void rejectedLateLeaseRemainsTheCallersResponsibility() {
    ExecutorService executor = executor();
    try {
      FragmentInstanceContext context = context(executor);
      context.releaseResourceWhenAllDriversAreClosed();
      ThrottleQuotaLimit quota = new ThrottleQuotaLimit();
      ThrottleQuotaLimit.AdmissionTicket late = quota.tryReserve("user", 0, 32);
      assertFalse(context.registerQuotaMemoryLease(late));
      assertEquals(32, quota.getReservedMemory("user"));
      late.close();
      assertEquals(0, quota.getReservedMemory("user"));
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void concurrentCleanupCallersWaitForOneActualPhysicalRelease() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch release = new CountDownLatch(1);
    try {
      FragmentInstanceContext context = context(executor);
      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch callers = new CountDownLatch(2);
      AtomicInteger physicalReleases = new AtomicInteger();
      ThrottleQuotaLimit quota = new ThrottleQuotaLimit();
      assertTrue(context.registerQuotaMemoryLease(quota.tryReserve("user", 0, 64)));
      doAnswer(
              call -> {
                physicalReleases.incrementAndGet();
                entered.countDown();
                await(release);
                return null;
              })
          .when(context)
          .releaseResource();
      Future<?> first =
          executor.submit(
              () -> {
                callers.countDown();
                context.releaseResourceWhenAllDriversAreClosed();
              });
      Future<?> second =
          executor.submit(
              () -> {
                callers.countDown();
                Thread.currentThread().interrupt();
                context.releaseResourceWhenAllDriversAreClosed();
                assertTrue(Thread.currentThread().isInterrupted());
              });
      assertTrue(callers.await(5, TimeUnit.SECONDS));
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      assertFalse(first.isDone());
      assertFalse(second.isDone());
      assertEquals(1, physicalReleases.get());
      assertEquals(64, quota.getReservedMemory("user"));
      release.countDown();
      first.get(5, TimeUnit.SECONDS);
      second.get(5, TimeUnit.SECONDS);
      context.releaseResourceWhenAllDriversAreClosed();
      assertEquals(1, physicalReleases.get());
      assertEquals(0, quota.getReservedMemory("user"));
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void reentrantCleanupCannotReplayOrClaimCompletion() {
    ExecutorService executor = executor();
    try {
      FragmentInstanceContext context = context(executor);
      AtomicInteger calls = new AtomicInteger();
      doAnswer(
              call -> {
                calls.incrementAndGet();
                assertThrows(
                    IllegalStateException.class, context::releaseResourceWhenAllDriversAreClosed);
                return null;
              })
          .when(context)
          .releaseResource();
      context.releaseResourceWhenAllDriversAreClosed();
      context.releaseResourceWhenAllDriversAreClosed();
      assertEquals(1, calls.get());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void quotaHookRetainsOwnershipWhenTheRealReaderCloseReportsIOException() throws Exception {
    ExecutorService executor = executor();
    try {
      FragmentInstanceContext context = context(executor);
      doCallRealMethod().when(context).releaseResource();
      ThrottleQuotaLimit quota = new ThrottleQuotaLimit();
      assertTrue(context.registerQuotaMemoryLease(quota.tryReserve("user", 0, 64)));
      org.apache.tsfile.read.reader.IChunkReader reader =
          mock(org.apache.tsfile.read.reader.IChunkReader.class);
      java.io.IOException expected = new java.io.IOException("reader close failed");
      doThrow(expected).when(reader).close();
      context
          .getQueryChunkReaders()
          .retain(
              reader,
              java.util.Collections.singletonList(
                  mock(org.apache.tsfile.read.reader.IPageReader.class)));
      RuntimeException first =
          assertThrows(RuntimeException.class, context::releaseResourceWhenAllDriversAreClosed);
      assertSame(expected, first.getCause());
      assertSame(
          first,
          assertThrows(RuntimeException.class, context::releaseResourceWhenAllDriversAreClosed));
      assertEquals(64, quota.getReservedMemory("user"));
      verify(reader, times(1)).close();
      verify(context, times(1)).releaseResource();
    } finally {
      executor.shutdownNow();
    }
  }

  private static FragmentInstanceContext context(ExecutorService notifications) {
    FragmentInstanceId id = new FragmentInstanceId(new PlanFragmentId(MOCK_QUERY_ID, 0), "quota");
    FragmentInstanceContext context =
        spy(
            FragmentInstanceContext.createFragmentInstanceContext(
                id, new FragmentInstanceStateMachine(id, notifications)));
    doNothing().when(context).releaseResource();
    return context;
  }

  private static ExecutorService executor() {
    return Executors.newSingleThreadExecutor(
        action -> {
          Thread thread = new Thread(action, "quota-memory-lifecycle-test");
          thread.setDaemon(true);
          return thread;
        });
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }
}
