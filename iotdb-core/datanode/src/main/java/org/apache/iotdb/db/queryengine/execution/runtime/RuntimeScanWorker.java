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

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskId;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A schedulable execution member over a fixed set of logical scan drivers. Retiring a member
 * returns execution capacity; only the original logical driver may close its output stream. This
 * class does not register itself with the native scheduler or reserve resources.
 */
public final class RuntimeScanWorker implements IDriver {
  private final RuntimeScanGroup group;
  private final DriverContext context;
  private final long workerId;
  private final Runnable releaseResources;
  private final long incrementalMemoryBytes;
  private final AtomicBoolean processing = new AtomicBoolean();
  private final AtomicBoolean retirementRequested = new AtomicBoolean();
  private final AtomicBoolean settled = new AtomicBoolean();
  private final AtomicBoolean failed = new AtomicBoolean();
  private final AtomicReference<RuntimeScanWorker> failureReceiver = new AtomicReference<>(this);
  private final SettableFuture<Void> retiredFuture = SettableFuture.create();
  private final SettableFuture<Void> schedulerRemovalFuture = SettableFuture.create();
  private volatile boolean highestPriority;

  public RuntimeScanWorker(
      RuntimeScanGroup group,
      DriverContext context,
      long workerId,
      long incrementalMemoryBytes,
      Runnable releaseResources) {
    this.group = Objects.requireNonNull(group);
    this.context = Objects.requireNonNull(context);
    this.releaseResources = Objects.requireNonNull(releaseResources);
    if (workerId < 0 || incrementalMemoryBytes < 0) {
      throw new IllegalArgumentException();
    }
    this.workerId = workerId;
    this.incrementalMemoryBytes = incrementalMemoryBytes;
    watchFailure(group.getFailureFuture(), failureReceiver);
  }

  @Override
  public ListenableFuture<?> processFor(Duration duration) {
    Objects.requireNonNull(duration);
    if (!processing.compareAndSet(false, true)) {
      throw new IllegalStateException();
    }
    try {
      if (shouldFinish()) {
        return Operator.NOT_BLOCKED;
      }
      long budgetNanos = Math.max(0, duration.roundTo(TimeUnit.NANOSECONDS));
      long start = System.nanoTime();
      do {
        RuntimeScanGroup.PollResult poll = group.poll(workerId);
        Optional<RuntimeLaneLease> acquired = poll.getLease();
        if (!acquired.isPresent()) {
          // Retirement can race with poll. The captured future is woken by later requests.
          return shouldFinish() ? Operator.NOT_BLOCKED : poll.getReadyFuture();
        }
        RuntimeLaneLease lease = acquired.get();
        IDriver driver = lease.getDriver();
        ListenableFuture<?> blocked = Operator.NOT_BLOCKED;
        boolean laneFinished = false;
        try {
          if (!retirementRequested.get() && !group.isStopped()) {
            laneFinished = driver.isFinished();
            if (!laneFinished) {
              long elapsed = Math.max(0, System.nanoTime() - start);
              blocked =
                  Objects.requireNonNull(
                      driver.processFor(
                          new Duration(Math.max(0, budgetNanos - elapsed), TimeUnit.NANOSECONDS)));
              laneFinished = driver.isFinished();
            }
          }
        } catch (Throwable t) {
          // Do not republish a failing lane before fragment failure handling stops its peers.
          group.requestStop();
          throw t;
        } finally {
          group.release(lease, blocked, laneFinished);
        }
        if (shouldFinish()) {
          return Operator.NOT_BLOCKED;
        }
      } while (Math.max(0, System.nanoTime() - start) < budgetNanos);
      return Operator.NOT_BLOCKED;
    } catch (Throwable t) {
      failed(t);
      throw t;
    } finally {
      processing.set(false);
      settleIfFinished();
    }
  }

  private static void watchFailure(
      ListenableFuture<?> future, AtomicReference<RuntimeScanWorker> receiver) {
    // The group may outlive a retired worker. Keep only a detachable receiver in its listener.
    future.addListener(
        () -> {
          RuntimeScanWorker worker = receiver.get();
          if (worker == null) {
            return;
          }
          try {
            Futures.getDone(future);
          } catch (ExecutionException e) {
            worker.failed(e.getCause() == null ? e : e.getCause());
          } catch (CancellationException e) {
            worker.failed(e);
          }
        },
        MoreExecutors.directExecutor());
  }

  private boolean shouldFinish() {
    return retirementRequested.get() || group.isStopped() || group.isFinished();
  }

  private void settleIfFinished() {
    if (!processing.get() && shouldFinish() && settled.compareAndSet(false, true)) {
      failureReceiver.set(null);
      try {
        releaseResources.run();
        retiredFuture.set(null);
      } catch (Throwable t) {
        retiredFuture.setException(t);
        failed(t);
      }
    }
  }

  public ListenableFuture<Void> requestRetirement() {
    retirementRequested.set(true);
    group.wakeUp();
    settleIfFinished();
    return Futures.nonCancellationPropagating(retiredFuture);
  }

  /** Native task removal is acknowledged separately from the release of member resources. */
  public void acknowledgeSchedulerRemoval() {
    schedulerRemovalFuture.set(null);
  }

  public ListenableFuture<Void> getSchedulerRemovalFuture() {
    return Futures.nonCancellationPropagating(schedulerRemovalFuture);
  }

  public ListenableFuture<Void> getRetiredFuture() {
    return Futures.nonCancellationPropagating(retiredFuture);
  }

  @Override
  public boolean isFinished() {
    settleIfFinished();
    return settled.get();
  }

  @Override
  public boolean supportsQuiescence() {
    return true;
  }

  @Override
  public ListenableFuture<Void> requestQuiescence() {
    return requestRetirement();
  }

  @Override
  public void close() {
    requestRetirement();
  }

  @Override
  public void failed(Throwable cause) {
    if (failed.compareAndSet(false, true)) {
      group.requestStop();
      retirementRequested.set(true);
      try {
        context.failed(cause);
      } finally {
        settleIfFinished();
      }
    }
  }

  @Override
  public DriverTaskId getDriverTaskId() {
    return context.getDriverTaskID();
  }

  @Override
  public void setDriverTaskId(DriverTaskId taskId) {
    context.setDriverTaskID(taskId);
  }

  @Override
  public long getEstimatedMemorySize() {
    return incrementalMemoryBytes;
  }

  @Override
  public DriverContext getDriverContext() {
    return context;
  }

  @Override
  public ISink getSink() {
    return context.getSink();
  }

  @Override
  public boolean isHighestPriority() {
    return highestPriority;
  }

  @Override
  public void setHighestPriority(boolean highestPriority) {
    this.highestPriority = highestPriority;
  }
}
