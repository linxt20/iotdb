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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/** Samples one live FI, applies the pure policy and records bounded, immutable control evidence. */
public final class RuntimeDopController implements AutoCloseable {
  private static final ScheduledExecutorService SAMPLER =
      Executors.newSingleThreadScheduledExecutor(
          action -> {
            Thread thread = new Thread(action, "runtime-dop-controller");
            thread.setDaemon(true);
            return thread;
          });
  private final Object lock = new Object();

  public interface Target {
    Map<String, RuntimeDopPolicy.Input> sampleControlInputs(long nowNanos, long sequence);

    default org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler.ControlPressure
        sampleSharedPressure() {
      return null;
    }

    ListenableFuture<?> resize(String groupId, int targetWorkers) throws Exception;

    void fail(Throwable failure);
  }

  private final Target owner;
  private final RuntimeDopPolicy policy;
  private final RuntimeSharedPressureFeedback pressureFeedback =
      new RuntimeSharedPressureFeedback();
  private final Map<String, RuntimeDopPolicy.State> states = new LinkedHashMap<>();
  private final ArrayDeque<Event> events = new ArrayDeque<>();
  private final SettableFuture<Void> stopped = SettableFuture.create();
  private final java.util.function.LongSupplier ticker;
  private final long originNanos;
  private ScheduledFuture<?> timer;
  private boolean started;
  private boolean closed;
  private int sampling;
  private long sequence;
  private long recordedEventCount;

  public RuntimeDopController(Target owner) {
    this(
        owner,
        new RuntimeDopPolicy(RuntimeDopPolicy.Config.conservativeDefaults()),
        System::nanoTime);
  }

  RuntimeDopController(
      Target owner, RuntimeDopPolicy policy, java.util.function.LongSupplier ticker) {
    this.owner = java.util.Objects.requireNonNull(owner);
    this.policy = java.util.Objects.requireNonNull(policy);
    this.ticker = java.util.Objects.requireNonNull(ticker);
    this.originNanos = ticker.getAsLong();
  }

  public void start() {
    synchronized (lock) {
      if (started || closed) return;
      started = true;
    }
    ScheduledFuture<?> scheduled =
        SAMPLER.scheduleWithFixedDelay(this::sampleOnce, 100, 100, TimeUnit.MILLISECONDS);
    boolean cancel;
    synchronized (lock) {
      timer = scheduled;
      cancel = closed;
    }
    if (cancel) scheduled.cancel(false);
  }

  public void sampleOnce() {
    synchronized (lock) {
      if (closed || sampling != 0) return;
      sampling++;
    }
    try {
      long nowAbsolute = ticker.getAsLong();
      long now = Math.max(0, nowAbsolute - originNanos);
      boolean actuated = false;
      Map<String, RuntimeDopPolicy.Input> inputs = owner.sampleControlInputs(now, ++sequence);
      var shared = inputs.size() == 1 ? owner.sampleSharedPressure() : null;
      for (Map.Entry<String, RuntimeDopPolicy.Input> entry : inputs.entrySet()) {
        RuntimeDopPolicy.Input input = entry.getValue();
        if (actuated) {
          input =
              new RuntimeDopPolicy.Input(
                  input.executionGroupId(),
                  input.nowNanos(),
                  input.sampleNanos(),
                  input.sampleSequence(),
                  input.logicalLaneCount(),
                  input.unfinishedLaneCount(),
                  input.currentWorkers(),
                  input.activeLeases(),
                  input.readyLaneCount(),
                  input.backpressuredLaneCount(),
                  input.maxResourceWorkers(),
                  input.completedWork(),
                  input.totalWork(),
                  true,
                  input.terminal());
        }
        var advice =
            pressureFeedback.observe(
                shared, shared == null ? nowAbsolute : ticker.getAsLong(), input.currentWorkers());
        RuntimeDopPolicy.Result result =
            policy.evaluate(
                input, states.getOrDefault(entry.getKey(), RuntimeDopPolicy.State.empty()), advice);
        states.put(entry.getKey(), result.nextState());
        RuntimeDopPolicy.Decision decision = result.decision();
        synchronized (lock) {
          if (closed) return;
          if (events.size() == 128) events.removeFirst();
          recordedEventCount++;
          events.addLast(
              new Event(
                  entry.getKey(),
                  now,
                  sequence,
                  entry.getValue().currentWorkers(),
                  decision.targetWorkers(),
                  decision.action(),
                  decision.reason(),
                  entry.getValue().completedWork(),
                  entry.getValue().totalWork(),
                  input,
                  advice));
        }
        if (decision.action() != RuntimeDopPolicy.Action.KEEP) {
          actuated = true;
          ListenableFuture<?> receipt = owner.resize(entry.getKey(), decision.targetWorkers());
          receipt.addListener(
              () -> {
                try {
                  Futures.getDone(receipt);
                } catch (Exception failure) {
                  if (!expectedStoppedCancellation(failure)) owner.fail(failure);
                }
              },
              com.google.common.util.concurrent.MoreExecutors.directExecutor());
        }
      }
    } catch (Throwable failure) {
      if (!expectedStoppedCancellation(failure)) owner.fail(failure);
    } finally {
      boolean acknowledge;
      synchronized (lock) {
        sampling--;
        acknowledge = closed && sampling == 0;
      }
      if (acknowledge) stopped.set(null);
    }
  }

  private boolean expectedStoppedCancellation(Throwable failure) {
    Throwable actual =
        failure instanceof java.util.concurrent.ExecutionException && failure.getCause() != null
            ? failure.getCause()
            : failure;
    synchronized (lock) {
      return closed && actual instanceof java.util.concurrent.CancellationException;
    }
  }

  public List<Event> snapshotEvents() {
    synchronized (lock) {
      return List.copyOf(events);
    }
  }

  /** A read never samples, closes, drains, or resizes the controller. */
  public TraceSnapshot snapshotTrace() {
    synchronized (lock) {
      return new TraceSnapshot(
          originNanos, recordedEventCount, closed && sampling == 0, List.copyOf(events));
    }
  }

  /** Counts events, not sample ticks: several groups can share one sample sequence. */
  public record TraceSnapshot(
      long originNanos, long totalEvents, boolean samplingStopped, List<Event> events) {
    public TraceSnapshot {
      events = List.copyOf(events);
      if (totalEvents < events.size() || events.size() > 128) throw new IllegalArgumentException();
    }

    public long firstRetainedEventOrdinal() {
      return totalEvents - events.size() + 1;
    }
  }

  public ListenableFuture<Void> getStoppedFuture() {
    return Futures.nonCancellationPropagating(stopped);
  }

  @Override
  public void close() {
    ScheduledFuture<?> cancel;
    boolean acknowledge;
    synchronized (lock) {
      closed = true;
      cancel = timer;
      acknowledge = sampling == 0;
    }
    if (cancel != null) cancel.cancel(false);
    if (acknowledge) stopped.set(null);
  }

  public record Event(
      String groupId,
      long sampleNanos,
      long sampleSequence,
      int observedWorkers,
      int requestedWorkers,
      RuntimeDopPolicy.Action action,
      RuntimeDopPolicy.Reason reason,
      RuntimeDopPolicy.MetricValue completedWork,
      RuntimeDopPolicy.MetricValue totalWork,
      RuntimeDopPolicy.Input input,
      RuntimeSharedPressureFeedback.Advice sharedPressureAdvice) {
    public Event(
        String groupId,
        long sampleNanos,
        long sampleSequence,
        int observedWorkers,
        int requestedWorkers,
        RuntimeDopPolicy.Action action,
        RuntimeDopPolicy.Reason reason,
        RuntimeDopPolicy.MetricValue completedWork,
        RuntimeDopPolicy.MetricValue totalWork,
        RuntimeDopPolicy.Input input) {
      this(
          groupId,
          sampleNanos,
          sampleSequence,
          observedWorkers,
          requestedWorkers,
          action,
          reason,
          completedWork,
          totalWork,
          input,
          null);
    }

    public Event(
        String groupId,
        long sampleNanos,
        long sampleSequence,
        int observedWorkers,
        int requestedWorkers,
        RuntimeDopPolicy.Action action,
        RuntimeDopPolicy.Reason reason,
        RuntimeDopPolicy.MetricValue completedWork,
        RuntimeDopPolicy.MetricValue totalWork) {
      this(
          groupId,
          sampleNanos,
          sampleSequence,
          observedWorkers,
          requestedWorkers,
          action,
          reason,
          completedWork,
          totalWork,
          null);
    }
  }
}
