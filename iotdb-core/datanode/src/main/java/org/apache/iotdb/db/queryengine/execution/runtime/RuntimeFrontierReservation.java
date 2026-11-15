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

import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue.MemoryReservationKey;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.Objects;
import java.util.Optional;

/**
 * A real OUTPUT-side pool reservation for one buffered frontier block per logical lane.
 *
 * <p>Internal credits never reserve or free pool bytes individually. Closing seals new credits and
 * returns the physical reservation only after every existing block has been returned. This does not
 * bypass pool admission, including for highest-priority tasks.
 */
public final class RuntimeFrontierReservation implements AutoCloseable {
  private final Object lock = new Object();
  private final Object owner = new Object();
  private final MemoryPool pool;
  private final MemoryReservationKey key;
  private final MemorySide side;
  private final long bytesPerLane;
  private final long reservationBytes;
  private final BlockCredit[] activeCredits;
  private final Object[] queueOwners;
  private final long[] generations;
  private final SettableFuture<Void> closedFuture = SettableFuture.create();

  // Guarded by lock. Notifications and pool operations happen after releasing lock.
  private long internalUsedBytes;
  private int leasedCreditCount;
  private boolean closeRequested;
  private boolean freeClaimed;
  private boolean physicalReservationReleased;

  private RuntimeFrontierReservation(
      MemoryPool pool,
      MemoryReservationKey key,
      MemorySide side,
      int laneCount,
      long bytesPerLane,
      long totalBytes) {
    this.pool = pool;
    this.key = key;
    this.side = side;
    this.bytesPerLane = bytesPerLane;
    this.reservationBytes = totalBytes;
    this.activeCredits = new BlockCredit[laneCount];
    this.queueOwners = new Object[laneCount];
    this.generations = new long[laneCount];
  }

  /**
   * Try one nonblocking reservation against the real global pool and the FI's shared OUTPUT budget.
   *
   * <p>The caller must provide a stable plan-node identity owned exclusively by this group and use
   * the same authoritative {@code maxFiBytes} for every group and OUTPUT handle in this FI. The
   * pool enforces the cap supplied with each request; it does not establish an immutable FI cap.
   *
   * <p>This call enables the FI's shared budget before attempting admission, including when
   * admission fails. Select this mode before legacy drivers start, and never fall back to legacy
   * independent plan-node accounting for this FI after calling it. An admission failure leaves no
   * pending pool request. The pool has no individual plan-node deregistration API, so an empty
   * registration remains the FI's responsibility on both rejection and normal close.
   */
  public static Optional<RuntimeFrontierReservation> tryCreate(
      MemoryPool pool,
      MemoryReservationKey groupKey,
      int laneCount,
      long bytesPerLane,
      long maxFiBytes) {
    return tryCreate(pool, groupKey, MemorySide.OUTPUT, laneCount, bytesPerLane, maxFiBytes);
  }

  public static Optional<RuntimeFrontierReservation> tryCreate(
      MemoryPool pool,
      MemoryReservationKey groupKey,
      MemorySide side,
      int laneCount,
      long bytesPerLane,
      long maxFiBytes) {
    Objects.requireNonNull(side);
    Objects.requireNonNull(pool);
    Objects.requireNonNull(groupKey);
    requireIdentity(groupKey.queryId());
    requireIdentity(groupKey.fragmentInstanceId());
    requireIdentity(groupKey.planNodeId());
    if (laneCount <= 0 || bytesPerLane <= 0 || maxFiBytes <= 0) {
      throw new IllegalArgumentException();
    }
    long totalBytes = Math.multiplyExact((long) laneCount, bytesPerLane);
    // Allocate the member arrays before obtaining any physical pool bytes.
    RuntimeFrontierReservation reservation =
        new RuntimeFrontierReservation(pool, groupKey, side, laneCount, bytesPerLane, totalBytes);
    pool.registerFragmentInstanceSharedBudget(groupKey.queryId(), groupKey.fragmentInstanceId());
    pool.registerPlanNodeIdToQueryMemoryMap(
        groupKey.queryId(), groupKey.fragmentInstanceId(), groupKey.planNodeId(), side);
    if (!pool.tryReserve(
        groupKey.queryId(),
        groupKey.fragmentInstanceId(),
        groupKey.planNodeId(),
        totalBytes,
        maxFiBytes)) {
      return Optional.empty();
    }
    return Optional.of(reservation);
  }

  private static void requireIdentity(String value) {
    if (Objects.requireNonNull(value).isBlank()) {
      throw new IllegalArgumentException();
    }
  }

  /** Exact pool object backing this owner; exposed for identity-safe observation only. */
  public MemoryPool getMemoryPool() {
    return pool;
  }

  public MemoryReservationKey getMemoryReservationKey() {
    return key;
  }

  public MemorySide getMemorySide() {
    return side;
  }

  public void claimEndpointSlot(int slot, Object endpoint) {
    claimQueueLane(slot, endpoint);
  }

  /** Native/remote credits cover a fixed envelope; they do not measure actual resident bytes. */
  public BlockCredit acquireEnvelope(int slot, Object endpoint) {
    return acquire(slot, endpoint, bytesPerLane);
  }

  public int getLaneCount() {
    return activeCredits.length;
  }

  public long getBytesPerLane() {
    return bytesPerLane;
  }

  /** True only for the exact pool object which holds this group's physical reservation. */
  public boolean isBackedBy(MemoryPool candidate) {
    return pool == candidate;
  }

  /**
   * Bind one lane permanently to one physical queue by object identity. The same owner may repeat
   * the claim, including after close. A different owner and a new binding after close are rejected.
   * Returning blocks or closing a queue never releases this binding, preserving EOS and lane
   * identity.
   */
  public void claimQueueLane(int lane, Object queueOwner) {
    Objects.requireNonNull(queueOwner);
    checkLane(lane);
    synchronized (lock) {
      if (queueOwners[lane] == queueOwner) {
        return;
      }
      if (closeRequested || queueOwners[lane] != null || activeCredits[lane] != null) {
        throw new IllegalStateException();
      }
      queueOwners[lane] = queueOwner;
    }
  }

  /**
   * Acquire a standalone lane's sole block credit. Once a queue has claimed the lane, callers must
   * use the owner-checked overload. A block's retained size must fit the reserved per-lane bound.
   */
  public BlockCredit acquire(int lane, long actualRetainedBytes) {
    checkLane(lane);
    checkBlockSize(actualRetainedBytes);
    synchronized (lock) {
      if (queueOwners[lane] != null) {
        throw new IllegalStateException();
      }
      return acquireLocked(lane, actualRetainedBytes);
    }
  }

  /** Acquire a queue lane's credit only for its already bound owner, using object identity. */
  public BlockCredit acquire(int lane, Object queueOwner, long actualRetainedBytes) {
    Objects.requireNonNull(queueOwner);
    checkLane(lane);
    checkBlockSize(actualRetainedBytes);
    synchronized (lock) {
      if (queueOwners[lane] != queueOwner) {
        throw new IllegalStateException();
      }
      return acquireLocked(lane, actualRetainedBytes);
    }
  }

  private void checkBlockSize(long actualRetainedBytes) {
    if (actualRetainedBytes <= 0 || actualRetainedBytes > bytesPerLane) {
      throw new IllegalArgumentException();
    }
  }

  // Called only while holding lock, after checking standalone or bound-owner eligibility.
  private BlockCredit acquireLocked(int lane, long actualRetainedBytes) {
    if (closeRequested || activeCredits[lane] != null) {
      throw new IllegalStateException();
    }
    long generation = Math.addExact(generations[lane], 1L);
    long usedBytes = Math.addExact(internalUsedBytes, actualRetainedBytes);
    BlockCredit credit = new BlockCredit(owner, lane, generation, actualRetainedBytes);
    generations[lane] = generation;
    activeCredits[lane] = credit;
    internalUsedBytes = usedBytes;
    leasedCreditCount++;
    return credit;
  }

  /**
   * Return only an internal credit. Repeated or stale returns are harmless; a different group's
   * token is rejected. A closing group's final credit also releases its physical reservation.
   */
  public void release(BlockCredit credit) {
    Objects.requireNonNull(credit);
    if (credit.owner != owner) {
      throw new IllegalArgumentException();
    }
    boolean freeReservation;
    synchronized (lock) {
      if (activeCredits[credit.lane] != credit || generations[credit.lane] != credit.generation) {
        return;
      }
      activeCredits[credit.lane] = null;
      internalUsedBytes -= credit.actualRetainedBytes;
      leasedCreditCount--;
      freeReservation = claimFreeIfReady();
    }
    try {
      credit.returnedFuture.set(null);
    } finally {
      // A direct listener may throw. It must not skip the already claimed physical free.
      if (freeReservation) {
        freePhysicalReservation();
      }
    }
  }

  /**
   * Snapshot the lane's current writable notification. An empty open lane is immediately writable.
   * This is a notification rather than a credit: another producer may acquire before the caller.
   */
  public ListenableFuture<Void> whenWritable(int lane) {
    checkLane(lane);
    ListenableFuture<Void> future;
    boolean closed;
    synchronized (lock) {
      BlockCredit credit = activeCredits[lane];
      future = credit == null ? null : credit.returnedFuture;
      closed = closeRequested;
    }
    if (future != null) {
      return Futures.nonCancellationPropagating(future);
    }
    return closed
        ? Futures.immediateFailedFuture(new IllegalStateException())
        : Futures.immediateVoidFuture();
  }

  /**
   * Seal new credits and asynchronously release the pool reservation when all blocks are returned.
   */
  public ListenableFuture<Void> requestClose() {
    boolean freeReservation;
    synchronized (lock) {
      closeRequested = true;
      freeReservation = claimFreeIfReady();
    }
    if (freeReservation) {
      freePhysicalReservation();
    }
    return getClosedFuture();
  }

  @Override
  public void close() {
    requestClose();
  }

  private boolean claimFreeIfReady() {
    if (closeRequested && leasedCreditCount == 0 && !freeClaimed) {
      freeClaimed = true;
      return true;
    }
    return false;
  }

  private void freePhysicalReservation() {
    Throwable failure = null;
    try {
      pool.free(key.queryId(), key.fragmentInstanceId(), key.planNodeId(), reservationBytes);
      synchronized (lock) {
        physicalReservationReleased = true;
      }
    } catch (Throwable t) {
      failure = t;
    }
    if (failure == null) {
      closedFuture.set(null);
    } else {
      // Never retry an ambiguous void free: it may have released bytes before a callback failed.
      closedFuture.setException(failure);
    }
  }

  public ListenableFuture<Void> getClosedFuture() {
    return Futures.nonCancellationPropagating(closedFuture);
  }

  /**
   * Bytes held by the physical reservation. A failed free leaves a conservative positive value,
   * since the pool's void API cannot identify whether an exception occurred before or after free.
   */
  public long getTotalReservedBytes() {
    synchronized (lock) {
      return physicalReservationReleased ? 0L : reservationBytes;
    }
  }

  public long getInternalUsedBytes() {
    synchronized (lock) {
      return internalUsedBytes;
    }
  }

  public int getLeasedCreditCount() {
    synchronized (lock) {
      return leasedCreditCount;
    }
  }

  public boolean isCloseRequested() {
    synchronized (lock) {
      return closeRequested;
    }
  }

  private void checkLane(int lane) {
    if (lane < 0 || lane >= activeCredits.length) {
      throw new IndexOutOfBoundsException();
    }
  }

  /** A private-constructor, generation-bound token for one particular buffered block. */
  public static final class BlockCredit {
    private final Object owner;
    private final int lane;
    private final long generation;
    private final long actualRetainedBytes;
    private final SettableFuture<Void> returnedFuture = SettableFuture.create();

    private BlockCredit(Object owner, int lane, long generation, long actualRetainedBytes) {
      this.owner = owner;
      this.lane = lane;
      this.generation = generation;
      this.actualRetainedBytes = actualRetainedBytes;
    }

    public int getLaneIndex() {
      return lane;
    }

    public long getGeneration() {
      return generation;
    }

    public long getActualRetainedBytes() {
      return actualRetainedBytes;
    }

    /** Pending until this exact block is returned, independent of later credits on the lane. */
    public ListenableFuture<Void> getWritableFuture() {
      return Futures.nonCancellationPropagating(returnedFuture);
    }
  }
}
