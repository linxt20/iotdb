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

package org.apache.iotdb.db.queryengine.execution.runtime;

import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue.MemoryReservationKey;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation.BlockCredit;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RuntimeFrontierReservationTest {
  private static final String QUERY = "runtime-frontier-query";
  private static final String FI = "runtime-frontier-fi";

  @Test
  public void reserveWholeGroupOnceAndFreeOnlyOnClose() throws Exception {
    RecordingPool pool = new RecordingPool(1024, 512);
    MemoryReservationKey key = key("group");
    RuntimeFrontierReservation reservation = create(pool, key, 3, 64, 512);
    assertEquals(1, pool.reserveCalls.get());
    assertEquals(192, pool.getReservedBytes());
    assertEquals(832, pool.getRemainingBytes());
    assertEquals(192, pool.getPlanNodeMemoryReservedBytes(QUERY, FI, "group"));
    assertEquals(192, pool.getFragmentMemoryReservedBytes(QUERY, FI, MemorySide.OUTPUT));
    assertEquals(0, pool.getFragmentMemoryReservedBytes(QUERY, FI, MemorySide.INPUT));
    assertEquals(192, reservation.getTotalReservedBytes());
    assertEquals(0, reservation.getInternalUsedBytes());
    BlockCredit first = reservation.acquire(0, 16);
    BlockCredit second = reservation.acquire(1, 64);
    assertEquals(80, reservation.getInternalUsedBytes());
    assertEquals(2, reservation.getLeasedCreditCount());
    reservation.release(first);
    reservation.release(second);
    assertEquals(192, pool.getReservedBytes());
    assertEquals(0, pool.freeCalls.get());
    assertEquals(0, reservation.getInternalUsedBytes());
    reservation.requestClose().get(5, TimeUnit.SECONDS);
    assertEquals(1, pool.freeCalls.get());
    assertEquals(0, pool.getReservedBytes());
    assertEquals(1024, pool.getRemainingBytes());
    assertEquals(0, reservation.getTotalReservedBytes());
    // Empty account cleanup belongs to normal FI teardown, not to this group.
    assertEquals(1, pool.getQueryMemoryReservationSize());
    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY, FI, false);
    assertEquals(0, pool.getQueryMemoryReservationSize());
  }

  @Test
  public void sharedFiOutputCapIncludesIndependentGroups() throws Exception {
    RecordingPool pool = new RecordingPool(1024, 192);
    RuntimeFrontierReservation first = create(pool, key("first"), 2, 64, 192);
    assertFalse(RuntimeFrontierReservation.tryCreate(pool, key("second"), 2, 64, 192).isPresent());
    assertEquals(128, pool.getReservedBytes());
    RuntimeFrontierReservation third = create(pool, key("third"), 1, 64, 192);
    assertEquals(192, pool.getFragmentMemoryReservedBytes(QUERY, FI, MemorySide.OUTPUT));
    first.close();
    first.getClosedFuture().get(5, TimeUnit.SECONDS);
    RuntimeFrontierReservation second = create(pool, key("second"), 2, 64, 192);
    assertEquals(192, pool.getReservedBytes());
    assertEquals(128, pool.getPlanNodeMemoryReservedBytes(QUERY, FI, "second"));
    second.close();
    third.close();
    assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void preservesOtherFiAndInputSideAccounts() throws Exception {
    RecordingPool pool = new RecordingPool(1024, 192);
    pool.registerPlanNodeIdToQueryMemoryMap(QUERY, FI, "input", MemorySide.INPUT);
    assertTrue(pool.tryReserve(QUERY, FI, "input", 160, 192));
    pool.registerPlanNodeIdToQueryMemoryMap(QUERY, "other-fi", "other", MemorySide.OUTPUT);
    assertTrue(pool.tryReserve(QUERY, "other-fi", "other", 80, 192));
    RuntimeFrontierReservation reservation = create(pool, key("group"), 2, 64, 192);
    assertEquals(368, pool.getQueryMemoryReservedBytes(QUERY));
    reservation.requestClose().get(5, TimeUnit.SECONDS);
    assertEquals(240, pool.getReservedBytes());
    assertEquals(160, pool.getFragmentMemoryReservedBytes(QUERY, FI, MemorySide.INPUT));
    assertEquals(80, pool.getPlanNodeMemoryReservedBytes(QUERY, "other-fi", "other"));
    assertEquals(0, pool.getFragmentMemoryReservedBytes(QUERY, FI, MemorySide.OUTPUT));
    pool.free(QUERY, FI, "input", 160);
    pool.free(QUERY, "other-fi", "other", 80);
  }

  @Test
  public void physicalPoolExhaustionHasNoZeroReservationBypass() {
    RecordingPool pool = new RecordingPool(256, 256);
    pool.registerPlanNodeIdToQueryMemoryMap("other-query", "other-fi", "other", MemorySide.INPUT);
    assertTrue(pool.tryReserve("other-query", "other-fi", "other", 256, 256));
    assertFalse(RuntimeFrontierReservation.tryCreate(pool, key("group"), 1, 32, 256).isPresent());
    assertEquals(256, pool.getReservedBytes());
    assertEquals(0, pool.getPlanNodeMemoryReservedBytes(QUERY, FI, "group"));
    assertEquals(0, pool.getMemoryReservationSize());
    assertEquals(2, pool.reserveCalls.get());
    assertEquals(0, pool.priorityCalls.get());
    pool.free("other-query", "other-fi", "other", 256);
  }

  @Test
  public void invalidArgumentsDoNotRegisterOrReserve() {
    RecordingPool pool = new RecordingPool(1024, 512);
    assertThrows(
        NullPointerException.class,
        () -> RuntimeFrontierReservation.tryCreate(null, key("group"), 1, 32, 512));
    assertThrows(
        NullPointerException.class,
        () -> RuntimeFrontierReservation.tryCreate(pool, null, 1, 32, 512));
    assertThrows(
        NullPointerException.class,
        () ->
            RuntimeFrontierReservation.tryCreate(
                pool, new MemoryReservationKey(null, FI, "g"), 1, 32, 512));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RuntimeFrontierReservation.tryCreate(
                pool, new MemoryReservationKey(QUERY, FI, " "), 1, 32, 512));
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeFrontierReservation.tryCreate(pool, key("group"), 0, 32, 512));
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeFrontierReservation.tryCreate(pool, key("group"), -1, 32, 512));
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeFrontierReservation.tryCreate(pool, key("group"), 1, 0, 512));
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeFrontierReservation.tryCreate(pool, key("group"), 1, -32, 512));
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeFrontierReservation.tryCreate(pool, key("group"), 1, 32, 0));
    assertEquals(0, pool.reserveCalls.get());
    assertEquals(0, pool.getQueryMemoryReservationSize());
  }

  @Test
  public void multiplicationOverflowRejectedBeforeRegistration() {
    RecordingPool pool = new RecordingPool(1024, 512);
    assertThrows(
        ArithmeticException.class,
        () -> RuntimeFrontierReservation.tryCreate(pool, key("group"), 2, Long.MAX_VALUE, 512));
    assertEquals(0, pool.reserveCalls.get());
    assertEquals(0, pool.getReservedBytes());
    assertEquals(0, pool.getQueryMemoryReservationSize());
  }

  @Test
  public void refusesOversizedAndNonPositiveBlocks() {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 1, 64, 512);
    assertThrows(IllegalArgumentException.class, () -> reservation.acquire(0, 65));
    assertThrows(IllegalArgumentException.class, () -> reservation.acquire(0, 0));
    assertThrows(IllegalArgumentException.class, () -> reservation.acquire(0, -1));
    assertEquals(0, reservation.getInternalUsedBytes());
    assertEquals(0, reservation.getLeasedCreditCount());
    assertTrue(reservation.whenWritable(0).isDone());
    assertEquals(64, pool.getReservedBytes());
    reservation.close();
  }

  @Test
  public void oneOutstandingBlockPerLane() {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 2, 64, 512);
    BlockCredit first = reservation.acquire(0, 16);
    assertEquals(0, first.getLaneIndex());
    assertEquals(1, first.getGeneration());
    assertEquals(16, first.getActualRetainedBytes());
    assertFalse(first.getWritableFuture().isDone());
    assertFalse(reservation.whenWritable(0).isDone());
    assertTrue(reservation.whenWritable(1).isDone());
    assertThrows(IllegalStateException.class, () -> reservation.acquire(0, 8));
    BlockCredit second = reservation.acquire(1, 32);
    assertEquals(48, reservation.getInternalUsedBytes());
    assertEquals(2, reservation.getLeasedCreditCount());
    reservation.release(first);
    assertTrue(first.getWritableFuture().isDone());
    assertTrue(reservation.whenWritable(0).isDone());
    reservation.release(second);
    reservation.close();
  }

  @Test
  public void invalidLaneIndicesDoNotMutateCredits() {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 2, 64, 512);
    assertThrows(IndexOutOfBoundsException.class, () -> reservation.acquire(-1, 8));
    assertThrows(IndexOutOfBoundsException.class, () -> reservation.acquire(2, 8));
    assertThrows(IndexOutOfBoundsException.class, () -> reservation.whenWritable(-1));
    assertThrows(IndexOutOfBoundsException.class, () -> reservation.whenWritable(2));
    assertEquals(0, reservation.getLeasedCreditCount());
    assertEquals(128, pool.getReservedBytes());
    reservation.close();
  }

  @Test
  public void duplicateAndOldGenerationReturnsCannotFreeNewCredit() {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 1, 64, 512);
    BlockCredit oldCredit = reservation.acquire(0, 16);
    reservation.release(oldCredit);
    reservation.release(oldCredit);
    BlockCredit newCredit = reservation.acquire(0, 32);
    assertEquals(2, newCredit.getGeneration());
    reservation.release(oldCredit);
    assertEquals(32, reservation.getInternalUsedBytes());
    assertEquals(1, reservation.getLeasedCreditCount());
    assertFalse(newCredit.getWritableFuture().isDone());
    assertFalse(reservation.whenWritable(0).isDone());
    assertTrue(oldCredit.getWritableFuture().isDone());
    reservation.release(newCredit);
    assertEquals(0, pool.freeCalls.get());
    reservation.close();
    reservation.release(oldCredit);
    reservation.release(newCredit);
    assertEquals(1, pool.freeCalls.get());
  }

  @Test
  public void foreignGroupTokenIsRejected() {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation first = create(pool, key("first"), 1, 64, 512);
    RuntimeFrontierReservation second = create(pool, key("second"), 1, 64, 512);
    BlockCredit firstCredit = first.acquire(0, 16);
    BlockCredit secondCredit = second.acquire(0, 16);
    assertThrows(IllegalArgumentException.class, () -> first.release(secondCredit));
    assertThrows(IllegalArgumentException.class, () -> second.release(firstCredit));
    assertEquals(16, first.getInternalUsedBytes());
    assertEquals(16, second.getInternalUsedBytes());
    first.release(firstCredit);
    second.release(secondCredit);
    first.close();
    second.close();
    assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void closingWaitsForLastBufferedBlockAndSealsNewCredits() throws Exception {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 3, 64, 512);
    BlockCredit first = reservation.acquire(0, 16);
    BlockCredit second = reservation.acquire(1, 32);
    ListenableFuture<Void> closed = reservation.requestClose();
    assertTrue(reservation.isCloseRequested());
    assertFalse(closed.isDone());
    assertFalse(first.getWritableFuture().isDone());
    assertFalse(reservation.whenWritable(0).isDone());
    assertThrows(IllegalStateException.class, () -> reservation.acquire(2, 8));
    assertTrue(reservation.whenWritable(2).isDone());
    assertThrows(ExecutionException.class, () -> reservation.whenWritable(2).get());
    reservation.release(first);
    assertTrue(first.getWritableFuture().isDone());
    assertFalse(closed.isDone());
    assertEquals(192, pool.getReservedBytes());
    assertEquals(32, reservation.getInternalUsedBytes());
    assertEquals(1, reservation.getLeasedCreditCount());
    reservation.release(second);
    closed.get(5, TimeUnit.SECONDS);
    assertEquals(0, pool.getReservedBytes());
    assertEquals(0, reservation.getInternalUsedBytes());
    assertEquals(0, reservation.getLeasedCreditCount());
    assertEquals(1, pool.freeCalls.get());
  }

  @Test
  public void closeAndRequestCloseAreIdempotent() throws Exception {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 2, 64, 512);
    assertFalse(reservation.getClosedFuture().isDone());
    reservation.close();
    reservation.requestClose().get(5, TimeUnit.SECONDS);
    reservation.close();
    reservation.requestClose().get(5, TimeUnit.SECONDS);
    assertEquals(1, pool.freeCalls.get());
    assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void writableObserversCannotCancelSharedNotification() throws Exception {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 1, 64, 512);
    BlockCredit credit = reservation.acquire(0, 16);
    ListenableFuture<Void> cancelled = credit.getWritableFuture();
    ListenableFuture<Void> laneCancelled = reservation.whenWritable(0);
    assertTrue(cancelled.cancel(false));
    assertTrue(laneCancelled.cancel(false));
    assertFalse(credit.getWritableFuture().isCancelled());
    assertFalse(reservation.whenWritable(0).isDone());
    reservation.release(credit);
    credit.getWritableFuture().get(5, TimeUnit.SECONDS);
    assertTrue(cancelled.isCancelled());
    reservation.close();
  }

  @Test
  public void closedObserversCannotCancelSharedClose() throws Exception {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 1, 64, 512);
    BlockCredit credit = reservation.acquire(0, 16);
    ListenableFuture<Void> cancelled = reservation.requestClose();
    assertTrue(cancelled.cancel(false));
    ListenableFuture<Void> second = reservation.getClosedFuture();
    assertFalse(second.isDone());
    assertFalse(second.isCancelled());
    reservation.release(credit);
    second.get(5, TimeUnit.SECONDS);
    assertTrue(cancelled.isCancelled());
    assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void poolFreeFailureCompletesCloseExceptionallyWithoutRetry() {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 1, 64, 512);
    RuntimeException expected = new RuntimeException("free rejected before accounting");
    pool.freeFailure = expected;
    ListenableFuture<Void> closed = reservation.requestClose();
    ExecutionException actual = assertThrows(ExecutionException.class, closed::get);
    assertSame(expected, actual.getCause());
    assertEquals(64, pool.getReservedBytes());
    assertEquals(64, reservation.getTotalReservedBytes());
    reservation.close();
    assertSame(
        expected,
        assertThrows(ExecutionException.class, () -> reservation.getClosedFuture().get())
            .getCause());
    assertEquals(1, pool.freeCalls.get());
    assertThrows(IllegalStateException.class, () -> reservation.acquire(0, 8));
    pool.freeFailure = null;
    pool.freeWithoutRecording(key("group"), 64);
  }

  @Test
  public void admissionExceptionIsNotReportedAsSuccessfulReservation() {
    RecordingPool pool = new RecordingPool(1024, 512);
    pool.registerPlanNodeIdToQueryMemoryMap(QUERY, "other-fi", "other", MemorySide.INPUT);
    assertTrue(pool.tryReserve(QUERY, "other-fi", "other", 32, 512));
    RuntimeException expected = new RuntimeException("pool admission failure");
    pool.reserveFailure = expected;
    RuntimeException actual =
        assertThrows(
            RuntimeException.class,
            () -> RuntimeFrontierReservation.tryCreate(pool, key("group"), 1, 64, 512));
    assertSame(expected, actual);
    assertEquals(32, pool.getReservedBytes());
    assertEquals(0, pool.getPlanNodeMemoryReservedBytes(QUERY, FI, "group"));
    assertEquals(0, pool.freeCalls.get());
    pool.reserveFailure = null;
    pool.free(QUERY, "other-fi", "other", 32);
  }

  @Test
  public void creditAndCloseNotificationsRunOutsideMemberLock() throws Exception {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 1, 64, 512);
    BlockCredit credit = reservation.acquire(0, 16);
    ExecutorService reader = Executors.newSingleThreadExecutor();
    AtomicInteger notifications = new AtomicInteger();
    try {
      credit
          .getWritableFuture()
          .addListener(
              () -> {
                assertReadableFromOtherThread(reservation, reader);
                notifications.incrementAndGet();
              },
              MoreExecutors.directExecutor());
      reservation
          .getClosedFuture()
          .addListener(
              () -> {
                assertReadableFromOtherThread(reservation, reader);
                notifications.incrementAndGet();
              },
              MoreExecutors.directExecutor());
      reservation.requestClose();
      reservation.release(credit);
      reservation.getClosedFuture().get(5, TimeUnit.SECONDS);
      assertEquals(2, notifications.get());
    } finally {
      reader.shutdownNow();
      reservation.close();
    }
  }

  @Test
  public void poolFreeRunsOutsideMemberLockAndMayReenterClose() throws Exception {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 1, 64, 512);
    ExecutorService reader = Executors.newSingleThreadExecutor();
    try {
      pool.beforeFree =
          () -> {
            assertReadableFromOtherThread(reservation, reader);
            try {
              reader.submit(reservation::close).get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
              throw new AssertionError(e);
            }
          };
      reservation.requestClose().get(5, TimeUnit.SECONDS);
      assertEquals(1, pool.freeCalls.get());
      assertEquals(0, pool.getReservedBytes());
    } finally {
      reader.shutdownNow();
    }
  }

  @Test
  public void simultaneousCloseAndDuplicateReturnsFreeExactlyOnce() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(6);
    try {
      for (int iteration = 0; iteration < 12; iteration++) {
        RecordingPool pool = new RecordingPool(1024, 512);
        RuntimeFrontierReservation reservation = create(pool, key("group"), 4, 32, 512);
        BlockCredit[] credits = new BlockCredit[4];
        for (int lane = 0; lane < credits.length; lane++) {
          credits[lane] = reservation.acquire(lane, lane + 1);
        }
        CountDownLatch entered = new CountDownLatch(6);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> tasks = new ArrayList<>();
        for (BlockCredit credit : credits) {
          tasks.add(
              executor.submit(
                  () -> {
                    entered.countDown();
                    await(start);
                    reservation.release(credit);
                    reservation.release(credit);
                  }));
        }
        for (int closer = 0; closer < 2; closer++) {
          tasks.add(
              executor.submit(
                  () -> {
                    entered.countDown();
                    await(start);
                    reservation.close();
                    reservation.requestClose();
                  }));
        }
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        start.countDown();
        for (Future<?> task : tasks) {
          task.get(5, TimeUnit.SECONDS);
        }
        reservation.getClosedFuture().get(5, TimeUnit.SECONDS);
        assertEquals(1, pool.freeCalls.get());
        assertEquals(0, pool.getReservedBytes());
        assertEquals(0, reservation.getInternalUsedBytes());
        assertEquals(0, reservation.getLeasedCreditCount());
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(timeout = 15000)
  public void acquireRacingCloseEitherOwnsCreditOrSeesSealedGroup() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      for (int iteration = 0; iteration < 32; iteration++) {
        RecordingPool pool = new RecordingPool(1024, 512);
        RuntimeFrontierReservation reservation = create(pool, key("group"), 1, 64, 512);
        CyclicBarrier start = new CyclicBarrier(2);
        Future<BlockCredit> acquisition =
            executor.submit(
                () -> {
                  start.await(5, TimeUnit.SECONDS);
                  try {
                    return reservation.acquire(0, 16);
                  } catch (IllegalStateException closed) {
                    return null;
                  }
                });
        Future<?> closing =
            executor.submit(
                () -> {
                  start.await(5, TimeUnit.SECONDS);
                  reservation.close();
                  return null;
                });
        BlockCredit credit = acquisition.get(5, TimeUnit.SECONDS);
        closing.get(5, TimeUnit.SECONDS);
        try {
          assertTrue(reservation.isCloseRequested());
          assertThrows(IllegalStateException.class, () -> reservation.acquire(0, 16));
          if (credit == null) {
            reservation.getClosedFuture().get(5, TimeUnit.SECONDS);
            assertEquals(0, reservation.getInternalUsedBytes());
            assertEquals(0, reservation.getLeasedCreditCount());
            assertEquals(0, pool.getReservedBytes());
          } else {
            assertFalse(reservation.getClosedFuture().isDone());
            assertEquals(16, reservation.getInternalUsedBytes());
            assertEquals(1, reservation.getLeasedCreditCount());
            assertEquals(64, pool.getReservedBytes());
          }
        } finally {
          if (credit != null) {
            reservation.release(credit);
            reservation.release(credit);
          }
          reservation.close();
        }
        reservation.getClosedFuture().get(5, TimeUnit.SECONDS);
        assertEquals(1, pool.reserveCalls.get());
        assertEquals(1, pool.freeCalls.get());
        assertEquals(0, pool.getReservedBytes());
        assertEquals(0, reservation.getTotalReservedBytes());
        assertEquals(0, reservation.getInternalUsedBytes());
        assertEquals(0, reservation.getLeasedCreditCount());
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test(timeout = 15000)
  public void queueClaimRacingStandaloneAcquireHasOneWinner() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      for (int iteration = 0; iteration < 32; iteration++) {
        RecordingPool pool = new RecordingPool(1024, 512);
        RuntimeFrontierReservation reservation = create(pool, key("group"), 1, 64, 512);
        Object queueOwner = new Object();
        CyclicBarrier start = new CyclicBarrier(2);
        Future<Boolean> claiming =
            executor.submit(
                () -> {
                  start.await(5, TimeUnit.SECONDS);
                  try {
                    reservation.claimQueueLane(0, queueOwner);
                    return true;
                  } catch (IllegalStateException occupied) {
                    return false;
                  }
                });
        Future<BlockCredit> acquisition =
            executor.submit(
                () -> {
                  start.await(5, TimeUnit.SECONDS);
                  try {
                    return reservation.acquire(0, 16);
                  } catch (IllegalStateException bound) {
                    return null;
                  }
                });
        boolean claimed = claiming.get(5, TimeUnit.SECONDS);
        BlockCredit standalone = acquisition.get(5, TimeUnit.SECONDS);
        try {
          assertEquals(64, pool.getReservedBytes());
          assertEquals(0, pool.freeCalls.get());
          if (claimed) {
            assertNull(standalone);
            assertEquals(0, reservation.getInternalUsedBytes());
            assertEquals(0, reservation.getLeasedCreditCount());
          } else {
            assertNotNull(standalone);
            assertEquals(16, reservation.getInternalUsedBytes());
            assertEquals(1, reservation.getLeasedCreditCount());
            assertThrows(
                IllegalStateException.class, () -> reservation.claimQueueLane(0, queueOwner));
            assertThrows(IllegalStateException.class, () -> reservation.acquire(0, queueOwner, 16));
          }
        } finally {
          if (standalone != null) {
            reservation.release(standalone);
            reservation.release(standalone);
          }
        }

        Object boundOwner = claimed ? queueOwner : new Object();
        reservation.claimQueueLane(0, boundOwner);
        if (!claimed) {
          // A failed claim must not bind the lane, even after the standalone credit is returned.
          assertThrows(
              IllegalStateException.class, () -> reservation.claimQueueLane(0, queueOwner));
        }
        assertThrows(
            IllegalStateException.class, () -> reservation.claimQueueLane(0, new Object()));
        assertThrows(IllegalStateException.class, () -> reservation.acquire(0, 16));
        BlockCredit bound = reservation.acquire(0, boundOwner, 16);
        reservation.release(bound);
        reservation.close();
        reservation.getClosedFuture().get(5, TimeUnit.SECONDS);
        assertEquals(1, pool.reserveCalls.get());
        assertEquals(1, pool.freeCalls.get());
        assertEquals(0, pool.getReservedBytes());
        assertEquals(0, reservation.getInternalUsedBytes());
        assertEquals(0, reservation.getLeasedCreditCount());
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void releaseListenerErrorCannotSkipAlreadyClaimedPhysicalFree() throws Exception {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 1, 64, 512);
    BlockCredit credit = reservation.acquire(0, 16);
    AssertionError expected = new AssertionError("listener failure");
    credit
        .getWritableFuture()
        .addListener(
            () -> {
              throw expected;
            },
            MoreExecutors.directExecutor());
    reservation.requestClose();
    assertSame(expected, assertThrows(AssertionError.class, () -> reservation.release(credit)));
    reservation.getClosedFuture().get(5, TimeUnit.SECONDS);
    assertEquals(0, pool.getReservedBytes());
    assertEquals(1, pool.freeCalls.get());
  }

  @Test
  public void generationOverflowLeavesTheLaneEmpty() throws Exception {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 1, 64, 512);
    Field field = RuntimeFrontierReservation.class.getDeclaredField("generations");
    field.setAccessible(true);
    long[] generations = (long[]) field.get(reservation);
    generations[0] = Long.MAX_VALUE;
    assertThrows(ArithmeticException.class, () -> reservation.acquire(0, 16));
    assertTrue(reservation.whenWritable(0).isDone());
    assertEquals(0, reservation.getInternalUsedBytes());
    assertEquals(0, reservation.getLeasedCreditCount());
    reservation.close();
  }

  @Test
  public void rejectedEmptyAccountIsLeftForFiCleanup() {
    RecordingPool pool = new RecordingPool(1024, 32);
    assertFalse(RuntimeFrontierReservation.tryCreate(pool, key("group"), 2, 32, 32).isPresent());
    assertEquals(0, pool.getReservedBytes());
    assertEquals(0, pool.freeCalls.get());
    assertEquals(1, pool.getQueryMemoryReservationSize());
    assertEquals(0, pool.getMemoryReservationSize());
    pool.deRegisterFragmentInstanceFromQueryMemoryMap(QUERY, FI, false);
    assertEquals(0, pool.getQueryMemoryReservationSize());
  }

  @Test
  public void closeSealsLaneDespitePreviouslyCompletedWritableNotification() {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 1, 64, 512);
    ListenableFuture<Void> earlier = reservation.whenWritable(0);
    assertTrue(earlier.isDone());
    reservation.close();
    assertThrows(IllegalStateException.class, () -> reservation.acquire(0, 8));
    assertEquals(0, reservation.getInternalUsedBytes());
    assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void failureAfterPoolAccountingIsObservableAndNeverRetried() {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 1, 64, 512);
    RuntimeException expected = new RuntimeException("callback failed after real free");
    pool.afterFreeFailure = expected;
    ExecutionException actual =
        assertThrows(ExecutionException.class, () -> reservation.requestClose().get());
    assertSame(expected, actual.getCause());
    assertEquals(0, pool.getReservedBytes());
    // A void free does not prove which side of accounting threw. Keep the receipt conservative.
    assertEquals(64, reservation.getTotalReservedBytes());
    reservation.close();
    assertEquals(1, pool.freeCalls.get());
    assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void exposesStableKeyLaneCapacityAndExactPoolIdentity() {
    RecordingPool pool = new RecordingPool(1024, 512);
    RecordingPool differentPool = new RecordingPool(1024, 512);
    MemoryReservationKey groupKey = key("group");
    RuntimeFrontierReservation reservation = create(pool, groupKey, 3, 64, 512);
    assertSame(groupKey, reservation.getMemoryReservationKey());
    assertEquals(3, reservation.getLaneCount());
    assertEquals(64, reservation.getBytesPerLane());
    assertTrue(reservation.isBackedBy(pool));
    assertFalse(reservation.isBackedBy(differentPool));
    assertFalse(reservation.isBackedBy(null));
    reservation.close();
  }

  @Test
  public void queueLaneBindingUsesIdentityAndSurvivesCreditReturn() {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 2, 64, 512);
    Object owner = new String("same-value");
    Object differentOwner = new String("same-value");
    reservation.claimQueueLane(0, owner);
    reservation.claimQueueLane(0, owner);
    assertThrows(IllegalStateException.class, () -> reservation.claimQueueLane(0, differentOwner));
    BlockCredit credit = reservation.acquire(0, owner, 16);
    reservation.release(credit);
    assertEquals(0, reservation.getLeasedCreditCount());
    assertEquals(0, reservation.getInternalUsedBytes());
    reservation.claimQueueLane(0, owner);
    assertThrows(IllegalStateException.class, () -> reservation.claimQueueLane(0, differentOwner));
    reservation.claimQueueLane(1, differentOwner);
    assertEquals(128, pool.getReservedBytes());
    reservation.close();
  }

  @Test
  public void closingRetainsQueueBindingAndRejectsNewBindings() throws Exception {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 2, 64, 512);
    Object owner = new Object();
    Object differentOwner = new Object();
    reservation.claimQueueLane(0, owner);
    BlockCredit credit = reservation.acquire(0, owner, 16);
    reservation.requestClose();
    reservation.claimQueueLane(0, owner);
    assertThrows(IllegalStateException.class, () -> reservation.claimQueueLane(0, differentOwner));
    assertThrows(IllegalStateException.class, () -> reservation.claimQueueLane(1, differentOwner));
    reservation.release(credit);
    reservation.getClosedFuture().get(5, TimeUnit.SECONDS);
    reservation.claimQueueLane(0, owner);
    assertThrows(IllegalStateException.class, () -> reservation.claimQueueLane(0, differentOwner));
    assertThrows(IllegalStateException.class, () -> reservation.claimQueueLane(1, differentOwner));
    assertEquals(0, reservation.getLeasedCreditCount());
    assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void invalidOwnerClaimsDoNotBindLanesOrChangeAccounting() {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 1, 64, 512);
    Object owner = new Object();
    assertThrows(NullPointerException.class, () -> reservation.claimQueueLane(0, null));
    assertThrows(IndexOutOfBoundsException.class, () -> reservation.claimQueueLane(-1, owner));
    assertThrows(IndexOutOfBoundsException.class, () -> reservation.claimQueueLane(1, owner));
    reservation.claimQueueLane(0, owner);
    reservation.claimQueueLane(0, owner);
    assertEquals(0, reservation.getInternalUsedBytes());
    assertEquals(0, reservation.getLeasedCreditCount());
    assertEquals(64, pool.getReservedBytes());
    reservation.close();
    assertThrows(NullPointerException.class, () -> reservation.claimQueueLane(0, null));
  }

  @Test
  public void boundQueueOwnerMustMatchLaneAndCannotBeNull() {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 3, 64, 512);
    Object firstOwner = new Object();
    Object secondOwner = new Object();
    reservation.claimQueueLane(0, firstOwner);
    reservation.claimQueueLane(1, secondOwner);
    assertThrows(NullPointerException.class, () -> reservation.acquire(0, null, 16));
    assertThrows(IllegalStateException.class, () -> reservation.acquire(0, secondOwner, 16));
    assertThrows(IllegalStateException.class, () -> reservation.acquire(1, firstOwner, 16));
    assertThrows(IllegalStateException.class, () -> reservation.acquire(2, firstOwner, 16));
    assertEquals(0, reservation.getLeasedCreditCount());
    BlockCredit first = reservation.acquire(0, firstOwner, 16);
    BlockCredit second = reservation.acquire(1, secondOwner, 32);
    assertEquals(48, reservation.getInternalUsedBytes());
    reservation.release(first);
    reservation.release(second);
    reservation.close();
  }

  @Test
  public void legacyAcquireCannotBypassQueueLaneBinding() {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 2, 64, 512);
    Object owner = new Object();
    reservation.claimQueueLane(0, owner);
    assertThrows(IllegalStateException.class, () -> reservation.acquire(0, 16));
    BlockCredit queueCredit = reservation.acquire(0, owner, 16);
    BlockCredit standaloneCredit = reservation.acquire(1, 32);
    reservation.release(queueCredit);
    assertThrows(IllegalStateException.class, () -> reservation.acquire(0, 16));
    reservation.release(standaloneCredit);
    assertEquals(0, reservation.getInternalUsedBytes());
    reservation.close();
  }

  @Test
  public void firstQueueClaimRequiresNoStandaloneCreditButSameOwnerRemainsIdempotent() {
    RecordingPool pool = new RecordingPool(1024, 512);
    RuntimeFrontierReservation reservation = create(pool, key("group"), 1, 64, 512);
    Object owner = new Object();
    BlockCredit standalone = reservation.acquire(0, 16);
    assertThrows(IllegalStateException.class, () -> reservation.claimQueueLane(0, owner));
    assertEquals(16, reservation.getInternalUsedBytes());
    assertEquals(1, reservation.getLeasedCreditCount());
    reservation.release(standalone);
    reservation.claimQueueLane(0, owner);
    BlockCredit queueCredit = reservation.acquire(0, owner, 32);
    reservation.claimQueueLane(0, owner);
    assertEquals(32, reservation.getInternalUsedBytes());
    assertEquals(1, reservation.getLeasedCreditCount());
    reservation.release(queueCredit);
    reservation.close();
  }

  private static void assertReadableFromOtherThread(
      RuntimeFrontierReservation reservation, ExecutorService executor) {
    try {
      assertEquals(
          0L,
          executor.submit(reservation::getInternalUsedBytes).get(5, TimeUnit.SECONDS).longValue());
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static MemoryReservationKey key(String planNodeId) {
    return new MemoryReservationKey(QUERY, FI, planNodeId);
  }

  private static RuntimeFrontierReservation create(
      MemoryPool pool,
      MemoryReservationKey key,
      int laneCount,
      long bytesPerLane,
      long maxFiBytes) {
    return RuntimeFrontierReservation.tryCreate(pool, key, laneCount, bytesPerLane, maxFiBytes)
        .orElseThrow(AssertionError::new);
  }

  /** Real accounting with counters and explicit fault hooks around the real public pool methods. */
  private static class RecordingPool extends MemoryPool {
    final AtomicInteger reserveCalls = new AtomicInteger();
    final AtomicInteger freeCalls = new AtomicInteger();
    final AtomicInteger priorityCalls = new AtomicInteger();
    volatile RuntimeException reserveFailure;
    volatile RuntimeException freeFailure;
    volatile RuntimeException afterFreeFailure;
    volatile Runnable beforeFree;

    RecordingPool(long capacity, long fiLimit) {
      super("frontier-test", new MemoryManager(capacity), fiLimit);
    }

    @Override
    public boolean tryReserve(
        String queryId, String fragmentInstanceId, String planNodeId, long bytes, long maxBytes) {
      reserveCalls.incrementAndGet();
      if (reserveFailure != null) {
        throw reserveFailure;
      }
      return super.tryReserve(queryId, fragmentInstanceId, planNodeId, bytes, maxBytes);
    }

    @Override
    public MemoryReservationResult reserveWithPriority(
        String queryId,
        String fragmentInstanceId,
        String planNodeId,
        long bytes,
        long maxBytes,
        boolean highestPriority) {
      priorityCalls.incrementAndGet();
      throw new AssertionError("frontier reservation must use real nonblocking pool admission");
    }

    @Override
    public void free(String queryId, String fragmentInstanceId, String planNodeId, long bytes) {
      freeCalls.incrementAndGet();
      if (beforeFree != null) {
        beforeFree.run();
      }
      if (freeFailure != null) {
        throw freeFailure;
      }
      super.free(queryId, fragmentInstanceId, planNodeId, bytes);
      if (afterFreeFailure != null) {
        throw afterFreeFailure;
      }
    }

    void freeWithoutRecording(MemoryReservationKey key, long bytes) {
      super.free(key.queryId(), key.fragmentInstanceId(), key.planNodeId(), bytes);
    }
  }
}
