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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

/**
 * Orders terminal work without waiting in a driver quantum or invoking callbacks under its lock.
 *
 * <p>The builder must register every owner and seal construction in a finally block, even when
 * construction fails. A terminal signal stops current owners immediately, but cleanup cannot pass
 * the construction barrier until late-created owners have also been registered. Driver futures
 * acknowledge cessation on both success and failure; an exception obtaining such a future is not
 * proof of cessation, so dependent releases are deliberately withheld in that case.
 *
 * <p>This is an opt-in lifecycle owner. Legacy fragment listeners must be replaced, rather than
 * left active beside it, before a production fragment can use this coordinator.
 */
public final class RuntimeCleanupCoordinator {
  public enum Phase {
    BEFORE_DRIVER_CLOSE,
    FREEZE_STATISTICS,
    CLOSE_ENDPOINTS,
    RELEASE_FRONTIERS,
    RELEASE_RESOURCES,
    DEREGISTER
  }

  @FunctionalInterface
  public interface CleanupStep {
    ListenableFuture<?> run() throws Exception;
  }

  private final Object lock = new Object();
  private final Executor executor;
  private final List<IDriver> drivers = new ArrayList<>();
  private final Set<IDriver> driverIdentities = Collections.newSetFromMap(new IdentityHashMap<>());
  private final List<RuntimeScanGroup> groups = new ArrayList<>();
  private final Set<RuntimeScanGroup> groupIdentities =
      Collections.newSetFromMap(new IdentityHashMap<>());
  private final EnumMap<Phase, List<CleanupStep>> steps = new EnumMap<>(Phase.class);
  private final Set<CleanupStep> requiredSteps = Collections.newSetFromMap(new IdentityHashMap<>());
  private final SettableFuture<Void> quiesced = SettableFuture.create();
  private final SettableFuture<Void> driversClosed = SettableFuture.create();
  private final SettableFuture<Void> completed = SettableFuture.create();
  private final Set<Throwable> failures = Collections.newSetFromMap(new IdentityHashMap<>());
  private Throwable firstFailure;
  private boolean sealed;
  private boolean stopRequested;
  private boolean started;

  public RuntimeCleanupCoordinator(Executor executor) {
    this.executor = Objects.requireNonNull(executor);
    for (Phase phase : Phase.values()) {
      steps.put(phase, new ArrayList<>());
    }
  }

  /** Called by the sole construction owner before sealing, including after cancellation. */
  public void registerDriver(IDriver driver) {
    Objects.requireNonNull(driver);
    if (!driver.supportsQuiescence() || !driver.supportsCoordinatedClose()) {
      throw new IllegalArgumentException();
    }
    boolean stop;
    synchronized (lock) {
      requireOpen();
      if (!driverIdentities.add(driver)) {
        throw new IllegalArgumentException();
      }
      drivers.add(driver);
      stop = stopRequested;
    }
    if (stop) {
      // Obtaining a future here wakes late owners; the sealed snapshot will await it again.
      signalDriver(driver);
    }
  }

  /**
   * Register a newly prepared execution member while the fragment is live. The stop transition and
   * membership publication share this lock, so a successful registration cannot be missed by stop.
   * Rejected members remain the caller's responsibility and must never be submitted or activated.
   */
  public void registerWorker(RuntimeScanWorker worker) {
    Objects.requireNonNull(worker);
    synchronized (lock) {
      if (stopRequested) {
        throw new IllegalStateException();
      }
      if (!driverIdentities.add(worker)) {
        throw new IllegalArgumentException();
      }
      drivers.add(worker);
    }
  }

  /**
   * Drop strong ownership only after normal resource settlement and native mapping removal.
   * Historical contributions must be accumulated by the caller before invoking this method. Once
   * stop has captured membership, that fixed terminal set must remain intact.
   */
  public boolean unregisterRetiredWorker(RuntimeScanWorker worker) {
    Objects.requireNonNull(worker);
    ListenableFuture<?> retired = worker.getRetiredFuture();
    ListenableFuture<?> removed = worker.getSchedulerRemovalFuture();
    if (!retired.isDone() || !removed.isDone()) {
      return false;
    }
    try {
      Futures.getDone(retired);
      Futures.getDone(removed);
    } catch (ExecutionException | CancellationException failure) {
      return false;
    }
    synchronized (lock) {
      if (stopRequested || !driverIdentities.remove(worker)) {
        return false;
      }
      drivers.remove(worker);
      return true;
    }
  }

  public void registerGroup(RuntimeScanGroup group) {
    Objects.requireNonNull(group);
    boolean stop;
    synchronized (lock) {
      requireOpen();
      if (!groupIdentities.add(group)) {
        throw new IllegalArgumentException();
      }
      groups.add(group);
      stop = stopRequested;
    }
    if (stop) {
      group.requestStop();
    }
  }

  public void addCleanup(Phase phase, CleanupStep step) {
    Objects.requireNonNull(phase);
    Objects.requireNonNull(step);
    synchronized (lock) {
      requireOpen();
      steps.get(phase).add(step);
    }
  }

  /**
   * A failed required step withholds all later phases. Independent steps within the same phase are
   * still attempted. Use this for native task removal and physical ownership settlement.
   */
  public void addRequiredCleanup(Phase phase, CleanupStep step) {
    Objects.requireNonNull(phase);
    Objects.requireNonNull(step);
    synchronized (lock) {
      requireOpen();
      steps.get(phase).add(step);
      requiredSteps.add(step);
    }
  }

  public void sealConstruction() {
    boolean launch;
    synchronized (lock) {
      sealed = true;
      launch = claimStart();
    }
    if (launch) {
      dispatch(this::awaitQuiescence);
    }
  }

  /** Idempotent and nonblocking, including when called by an active driver. */
  public ListenableFuture<Void> requestStop() {
    List<IDriver> toStop;
    List<RuntimeScanGroup> groupsToStop;
    boolean launch;
    synchronized (lock) {
      if (stopRequested) {
        return Futures.nonCancellationPropagating(completed);
      }
      stopRequested = true;
      toStop = new ArrayList<>(drivers);
      groupsToStop = new ArrayList<>(groups);
      launch = claimStart();
    }
    for (RuntimeScanGroup group : groupsToStop) {
      try {
        group.requestStop();
      } catch (Throwable failure) {
        recordFailure(failure);
      }
    }
    for (IDriver driver : toStop) {
      signalDriver(driver);
    }
    if (launch) {
      dispatch(this::awaitQuiescence);
    }
    return Futures.nonCancellationPropagating(completed);
  }

  public boolean isStopRequested() {
    synchronized (lock) {
      return stopRequested;
    }
  }

  public ListenableFuture<Void> getQuiescenceFuture() {
    return Futures.nonCancellationPropagating(quiesced);
  }

  public ListenableFuture<Void> getDriversClosedFuture() {
    return Futures.nonCancellationPropagating(driversClosed);
  }

  public ListenableFuture<Void> getCompletedFuture() {
    return Futures.nonCancellationPropagating(completed);
  }

  /** Release strong graph ownership only after every required cleanup phase succeeded. */
  public void releaseSuccessfulOwners() {
    if (!completed.isDone()) throw new IllegalStateException();
    try {
      Futures.getDone(completed);
    } catch (ExecutionException | CancellationException failure) {
      throw new IllegalStateException(failure);
    }
    synchronized (lock) {
      drivers.clear();
      driverIdentities.clear();
      groups.clear();
      groupIdentities.clear();
      for (List<CleanupStep> phase : steps.values()) phase.clear();
      requiredSteps.clear();
    }
  }

  private void requireOpen() {
    if (sealed) {
      throw new IllegalStateException();
    }
  }

  private boolean claimStart() {
    if (sealed && stopRequested && !started) {
      started = true;
      return true;
    }
    return false;
  }

  private void signalDriver(IDriver driver) {
    try {
      Objects.requireNonNull(driver.requestQuiescence());
    } catch (Throwable failure) {
      recordFailure(failure);
    }
  }

  private void awaitQuiescence() {
    List<ListenableFuture<?>> pending = new ArrayList<>();
    try {
      for (RuntimeScanGroup group : groups) {
        group.requestStop();
        pending.add(Objects.requireNonNull(group.getQuiescenceFuture()));
      }
      for (IDriver driver : drivers) {
        pending.add(Objects.requireNonNull(driver.requestQuiescence()));
      }
    } catch (Throwable failure) {
      failUnsafe(failure);
      return;
    }
    awaitAll(
        pending,
        () -> {
          try {
            quiesced.set(null);
          } catch (Throwable failure) {
            recordFailure(failure);
          } finally {
            runPhase(Phase.BEFORE_DRIVER_CLOSE, this::closeDrivers);
          }
        });
  }

  private void closeDrivers() {
    List<ListenableFuture<?>> pending = new ArrayList<>();
    Throwable unsafeFailure = null;
    for (IDriver driver : drivers) {
      try {
        pending.add(Objects.requireNonNull(driver.requestClose()));
      } catch (Throwable failure) {
        recordFailure(failure);
        if (unsafeFailure == null) {
          unsafeFailure = failure;
        }
      }
    }
    if (unsafeFailure != null) {
      // Attempt every independent owner, but a throwing close supplies no completion proof.
      failUnsafe(unsafeFailure);
      return;
    }
    awaitAll(
        pending,
        () -> {
          Throwable physicalFailure = null;
          for (ListenableFuture<?> result : pending) {
            try {
              Futures.getDone(result);
            } catch (ExecutionException failure) {
              physicalFailure = failure.getCause() == null ? failure : failure.getCause();
              break;
            } catch (CancellationException failure) {
              physicalFailure = failure;
              break;
            }
          }
          final Throwable unsettledPhysicalOwner = physicalFailure;
          try {
            if (physicalFailure == null) driversClosed.set(null);
            else driversClosed.setException(physicalFailure);
          } catch (Throwable failure) {
            recordFailure(failure);
          } finally {
            runPhase(
                Phase.FREEZE_STATISTICS,
                () ->
                    runPhase(
                        Phase.CLOSE_ENDPOINTS,
                        () -> {
                          // A failed actual-close receipt certifies execution exit, but cannot
                          // authorize releasing ambiguous operator or resource ownership.
                          if (unsettledPhysicalOwner != null) {
                            failUnsafe(unsettledPhysicalOwner);
                            return;
                          }
                          runPhase(
                              Phase.RELEASE_FRONTIERS,
                              () ->
                                  runPhase(
                                      Phase.RELEASE_RESOURCES,
                                      () -> runPhase(Phase.DEREGISTER, this::finish)));
                        }));
          }
        });
  }

  private void runPhase(Phase phase, Runnable next) {
    List<ListenableFuture<?>> pending = new ArrayList<>();
    List<ListenableFuture<?>> required = new ArrayList<>();
    for (CleanupStep step : steps.get(phase)) {
      ListenableFuture<?> result;
      try {
        result = Objects.requireNonNull(step.run());
      } catch (Throwable failure) {
        recordFailure(failure);
        result = Futures.immediateFailedFuture(failure);
      }
      pending.add(result);
      if (requiredSteps.contains(step)) {
        required.add(result);
      }
    }
    awaitAll(
        pending,
        () -> {
          for (ListenableFuture<?> result : required) {
            try {
              Futures.getDone(result);
            } catch (ExecutionException failure) {
              failUnsafe(failure.getCause() == null ? failure : failure.getCause());
              return;
            } catch (CancellationException failure) {
              failUnsafe(failure);
              return;
            }
          }
          next.run();
        });
  }

  private void awaitAll(List<ListenableFuture<?>> pending, Runnable next) {
    ListenableFuture<?> all = Futures.successfulAsList(pending);
    all.addListener(
        () -> {
          for (ListenableFuture<?> future : pending) {
            try {
              Futures.getDone(future);
            } catch (ExecutionException failure) {
              recordFailure(failure.getCause() == null ? failure : failure.getCause());
            } catch (CancellationException failure) {
              recordFailure(failure);
            }
          }
          dispatch(next);
        },
        MoreExecutors.directExecutor());
  }

  private void dispatch(Runnable action) {
    try {
      executor.execute(
          () -> {
            try {
              action.run();
            } catch (Throwable failure) {
              failUnsafe(failure);
            }
          });
    } catch (Throwable failure) {
      failUnsafe(failure);
    }
  }

  private void recordFailure(Throwable failure) {
    synchronized (lock) {
      if (!failures.add(failure)) {
        return;
      }
      if (firstFailure == null) {
        firstFailure = failure;
      } else {
        firstFailure.addSuppressed(failure);
      }
    }
  }

  private void failUnsafe(Throwable failure) {
    recordFailure(failure);
    // An unsupported/failed stop invocation or rejected executor cannot certify writer cessation.
    // Publish the error and retain dependent resources for diagnosis instead of releasing early.
    try {
      quiesced.setException(failure);
    } catch (Throwable observerFailure) {
      recordFailure(observerFailure);
    } finally {
      try {
        driversClosed.setException(failure);
      } catch (Throwable observerFailure) {
        recordFailure(observerFailure);
      } finally {
        finish();
      }
    }
  }

  private void finish() {
    Throwable failure;
    synchronized (lock) {
      failure = firstFailure;
    }
    if (failure == null) {
      completed.set(null);
    } else {
      completed.setException(failure);
    }
  }
}
