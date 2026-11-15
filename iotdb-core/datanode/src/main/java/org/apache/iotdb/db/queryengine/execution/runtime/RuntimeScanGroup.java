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

import org.apache.iotdb.db.queryengine.execution.driver.IDriver;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * Serializes ownership of fixed logical drivers without invoking or closing those drivers.
 *
 * <p>Caller-provided dependencies are the original exchange-consumer-close futures. They must not
 * be replaced by a producer's EOF. All future notifications and listener registration occur outside
 * the membership lock, including callbacks on futures that are already complete.
 */
public final class RuntimeScanGroup {
  private static final ListenableFuture<?> NOT_BLOCKED = Futures.immediateVoidFuture();

  private final Object lock = new Object();
  private final Object leaseOwner = new Object();
  private final List<Lane> lanes;
  private final ArrayDeque<Integer> ready = new ArrayDeque<>();
  private final Map<Long, RuntimeLaneLease> leasedByWorker = new HashMap<>();
  private final SettableFuture<Void> quiescence = SettableFuture.create();
  private final SettableFuture<Void> failureNotification = SettableFuture.create();
  private Throwable failure;
  private SettableFuture<Void> readyNotification = SettableFuture.create();
  private boolean stopped;
  private int finishedCount;

  public RuntimeScanGroup(List<? extends IDriver> lanes) {
    this(lanes, Collections.nCopies(Objects.requireNonNull(lanes).size(), NOT_BLOCKED));
  }

  public RuntimeScanGroup(
      List<? extends IDriver> drivers, List<? extends ListenableFuture<?>> dependencies) {
    Objects.requireNonNull(drivers);
    Objects.requireNonNull(dependencies);
    if (drivers.size() != dependencies.size()) {
      throw new IllegalArgumentException();
    }
    this.lanes = new ArrayList<>(drivers.size());
    Set<IDriver> distinct = Collections.newSetFromMap(new IdentityHashMap<>());
    List<ListenableFuture<?>> copiedDependencies = new ArrayList<>(dependencies.size());
    for (int i = 0; i < drivers.size(); i++) {
      IDriver driver = Objects.requireNonNull(drivers.get(i));
      if (!distinct.add(driver)) {
        throw new IllegalArgumentException();
      }
      this.lanes.add(new Lane(driver));
      copiedDependencies.add(Objects.requireNonNull(dependencies.get(i)));
    }
    for (int i = 0; i < this.lanes.size(); i++) {
      final int laneIndex = i;
      ListenableFuture<?> dependency = copiedDependencies.get(i);
      dependency.addListener(
          () -> dependencyCompleted(laneIndex, dependency), MoreExecutors.directExecutor());
    }
  }

  /** Acquires a lane or atomically binds the caller to the current wakeup generation. */
  public PollResult poll(long workerId) {
    if (workerId < 0) {
      throw new IllegalArgumentException();
    }
    synchronized (lock) {
      if (leasedByWorker.containsKey(workerId)) {
        throw new IllegalStateException();
      }
      if (stopped || finishedCount == lanes.size()) {
        return new PollResult(null, NOT_BLOCKED);
      }
      if (failure != null) {
        return new PollResult(null, failureNotification);
      }
      if (ready.isEmpty()) {
        return new PollResult(null, readyNotification);
      }
      int index = ready.getFirst();
      Lane lane = lanes.get(index);
      // Calculate before removing the ready entry, so overflow cannot strand the lane.
      long nextGeneration = Math.addExact(lane.generation, 1L);
      RuntimeLaneLease lease =
          new RuntimeLaneLease(leaseOwner, lane.driver, index, workerId, nextGeneration);
      ready.removeFirst();
      lane.enqueued = false;
      lane.generation = nextGeneration;
      lane.lease = lease;
      leasedByWorker.put(workerId, lease);
      return new PollResult(lease, NOT_BLOCKED);
    }
  }

  /**
   * Returns execution ownership before registering a blocked callback. Successful completion makes
   * the lane ready; cancellation and failure notify the caller through the shared failure future.
   */
  public void release(RuntimeLaneLease lease, ListenableFuture<?> blocked, boolean finished) {
    Objects.requireNonNull(lease);
    if (!lease.belongsTo(leaseOwner)) {
      throw new IllegalArgumentException();
    }
    SettableFuture<Void> wake = null;
    boolean notifyQuiescence;
    boolean listen = false;
    synchronized (lock) {
      Lane lane = lanes.get(lease.getLaneIndex());
      if (lane.lease != lease) {
        if (lease.getGeneration() <= lane.generation) {
          return;
        }
        throw new IllegalArgumentException();
      }
      lane.lease = null;
      leasedByWorker.remove(lease.getWorkerId());
      if (finished) {
        lane.finished = true;
        finishedCount++;
        lane.blocked = null;
        if (finishedCount == lanes.size()) {
          wake = rotateNotification();
        }
      } else if (!stopped) {
        lane.blocked = blocked;
        if (blocked == null) {
          enqueue(lease.getLaneIndex());
          wake = rotateNotification();
        } else {
          listen = true;
        }
      }
      notifyQuiescence = stopped && leasedByWorker.isEmpty();
    }
    try {
      complete(wake);
    } finally {
      try {
        if (notifyQuiescence) {
          quiescence.set(null);
        }
      } finally {
        if (listen) {
          blocked.addListener(
              () -> blockedCompleted(lease.getLaneIndex(), lease.getGeneration(), blocked),
              MoreExecutors.directExecutor());
        }
      }
    }
  }

  /** Wakes resize/retirement waiters without making a blocked logical lane ready. */
  public void wakeUp() {
    SettableFuture<Void> wake;
    synchronized (lock) {
      wake = rotateNotification();
    }
    complete(wake);
  }

  /** Stops new leases; logical drivers and their output queues remain owned by the caller. */
  public void requestStop() {
    SettableFuture<Void> wake;
    boolean notifyQuiescence;
    synchronized (lock) {
      stopped = true;
      ready.clear();
      for (Lane lane : lanes) {
        lane.enqueued = false;
      }
      wake = rotateNotification();
      notifyQuiescence = leasedByWorker.isEmpty();
    }
    try {
      complete(wake);
    } finally {
      if (notifyQuiescence) {
        quiescence.set(null);
      }
    }
  }

  /** Completes only after stop has been requested and every outstanding lease has returned. */
  public ListenableFuture<?> getQuiescenceFuture() {
    return Futures.nonCancellationPropagating(quiescence);
  }

  /** First failed or cancelled consumer dependency; the caller owns fragment failure handling. */
  public ListenableFuture<?> getFailureFuture() {
    return Futures.nonCancellationPropagating(failureNotification);
  }

  public Snapshot snapshot() {
    return snapshotWithObservedOutputs(Collections.emptySet());
  }

  /** Credit observations are sampled before entering this monitor, never by callbacks under it. */
  public Snapshot snapshotWithObservedOutputs(Set<Integer> observedFullOutputs) {
    synchronized (lock) {
      int backpressured = 0;
      for (int index : observedFullOutputs) {
        if (index < 0 || index >= lanes.size()) throw new IllegalArgumentException();
        Lane lane = lanes.get(index);
        if (!lane.finished && lane.dependencyReady && lane.lease == null && lane.blocked != null) {
          backpressured++;
        }
      }
      return new Snapshot(
          lanes.size(),
          ready.size(),
          leasedByWorker.size(),
          finishedCount,
          backpressured,
          stopped,
          finishedCount == lanes.size());
    }
  }

  public boolean isStopped() {
    return snapshot().isStopped();
  }

  public boolean isFinished() {
    return snapshot().isFinished();
  }

  public int getLeasedCount() {
    return snapshot().getLeasedCount();
  }

  public int getReadyCount() {
    return snapshot().getReadyCount();
  }

  public int size() {
    return lanes.size();
  }

  private void dependencyCompleted(int index, ListenableFuture<?> dependency) {
    // A completed consumer-close dependency can still have failed. A logical driver will not
    // observe this future itself because it is not directly submitted to the native scheduler.
    try {
      Futures.getDone(dependency);
    } catch (ExecutionException e) {
      dependencyFailed(e.getCause() == null ? e : e.getCause());
      return;
    } catch (CancellationException e) {
      dependencyFailed(e);
      return;
    } catch (RuntimeException e) {
      dependencyFailed(e);
      return;
    }
    SettableFuture<Void> wake = null;
    synchronized (lock) {
      Lane lane = lanes.get(index);
      lane.dependencyReady = true;
      if (!stopped && !lane.finished && lane.lease == null && lane.blocked == null) {
        enqueue(index);
        wake = rotateNotification();
      }
    }
    complete(wake);
  }

  private void dependencyFailed(Throwable cause) {
    SettableFuture<Void> wake;
    synchronized (lock) {
      if (stopped || failure != null) {
        return;
      }
      failure = cause;
      wake = rotateNotification();
    }
    try {
      failureNotification.setException(cause);
    } finally {
      complete(wake);
    }
  }

  private void blockedCompleted(int index, long generation, ListenableFuture<?> blocked) {
    Throwable completionFailure = null;
    try {
      Futures.getDone(blocked);
    } catch (ExecutionException e) {
      completionFailure = e.getCause() == null ? e : e.getCause();
    } catch (CancellationException e) {
      completionFailure = e;
    } catch (RuntimeException e) {
      completionFailure = e;
    }
    SettableFuture<Void> wake = null;
    Throwable firstFailure = null;
    synchronized (lock) {
      Lane lane = lanes.get(index);
      if (!stopped
          && !lane.finished
          && lane.lease == null
          && lane.generation == generation
          && lane.blocked == blocked) {
        if (completionFailure != null) {
          if (failure == null) {
            failure = completionFailure;
            firstFailure = completionFailure;
          }
        } else {
          lane.blocked = null;
          enqueue(index);
        }
        wake = rotateNotification();
      }
    }
    try {
      if (firstFailure != null) {
        failureNotification.setException(firstFailure);
      }
    } finally {
      complete(wake);
    }
  }

  // All callers hold lock. Neither helper invokes a driver or completes an external future.
  private void enqueue(int index) {
    Lane lane = lanes.get(index);
    if (lane.dependencyReady && !lane.enqueued) {
      ready.addLast(index);
      lane.enqueued = true;
    }
  }

  private SettableFuture<Void> rotateNotification() {
    SettableFuture<Void> wake = readyNotification;
    readyNotification = SettableFuture.create();
    return wake;
  }

  private static void complete(SettableFuture<Void> future) {
    if (future != null) {
      future.set(null);
    }
  }

  private static final class Lane {
    private final IDriver driver;
    private RuntimeLaneLease lease;
    private ListenableFuture<?> blocked;
    private long generation;
    private boolean dependencyReady;
    private boolean enqueued;
    private boolean finished;

    private Lane(IDriver driver) {
      this.driver = driver;
    }
  }

  public static final class PollResult {
    private final RuntimeLaneLease lease;
    private final ListenableFuture<?> readyFuture;

    private PollResult(RuntimeLaneLease lease, ListenableFuture<?> readyFuture) {
      this.lease = lease;
      this.readyFuture = readyFuture;
    }

    public Optional<RuntimeLaneLease> getLease() {
      return Optional.ofNullable(lease);
    }

    public ListenableFuture<?> getReadyFuture() {
      return Futures.nonCancellationPropagating(readyFuture);
    }
  }

  public static final class Snapshot {
    private final int size;
    private final int readyCount;
    private final int leasedCount;
    private final int finishedCount;
    private final int backpressuredCount;
    private final boolean stopped;
    private final boolean finished;

    private Snapshot(
        int size,
        int readyCount,
        int leasedCount,
        int finishedCount,
        int backpressuredCount,
        boolean stopped,
        boolean finished) {
      this.size = size;
      this.readyCount = readyCount;
      this.leasedCount = leasedCount;
      this.finishedCount = finishedCount;
      this.backpressuredCount = backpressuredCount;
      this.stopped = stopped;
      this.finished = finished;
    }

    public int size() {
      return size;
    }

    public int getReadyCount() {
      return readyCount;
    }

    public int getLeasedCount() {
      return leasedCount;
    }

    public int getUnfinishedCount() {
      return size - finishedCount;
    }

    public int getBackpressuredCount() {
      return backpressuredCount;
    }

    public boolean isStopped() {
      return stopped;
    }

    public boolean isFinished() {
      return finished;
    }
  }
}
