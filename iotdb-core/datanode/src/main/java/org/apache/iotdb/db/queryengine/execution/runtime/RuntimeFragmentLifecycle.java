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

import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.schedule.DriverSubmission;
import org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler;
import org.apache.iotdb.db.queryengine.execution.schedule.PreparedDriverBatch;
import org.apache.iotdb.db.queryengine.execution.schedule.RuntimeSchedulingGroup;
import org.apache.iotdb.db.queryengine.execution.schedule.TaskRegistration;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanContext;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.PipelineDriverFactory;
import org.apache.iotdb.db.queryengine.plan.planner.RuntimeScanGroupDescriptor;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.iotdb.db.queryengine.execution.runtime.RuntimeCleanupCoordinator.Phase.*;

/**
 * Owns the complete lifetime of a runtime-enabled table fragment, including construction failure.
 * Logical drivers remain the owners of streams and statistics; workers only rent execution time.
 */
public final class RuntimeFragmentLifecycle implements RuntimeDopController.Target {
  // Admission covers the worker, context, native task registration and bounded control metadata.
  private static final long WORKER_MEMORY_BYTES = 64L * 1024;
  private final FragmentInstanceContext context;
  private final RuntimeCleanupCoordinator cleanup;
  private final Map<PipelineDriverFactory, IDriver> originals = new IdentityHashMap<>();
  private final Set<PipelineDriverFactory> uncreatedClosed =
      Collections.newSetFromMap(new IdentityHashMap<>());
  private final Map<String, ExecutionGroup> groups = new LinkedHashMap<>();
  private final AtomicBoolean resizeInProgress = new AtomicBoolean();
  private volatile IDriverScheduler scheduler;
  private volatile RuntimeSchedulingGroup schedulingGroup;
  private volatile MPPDataExchangeManager exchangeManager;
  private volatile LocalExecutionPlanContext.RuntimePlanBindings bindings;
  private volatile RuntimeDopController controller;
  private int nextPipelineId;
  private volatile boolean constructionSealed;
  private final Object evidenceLock = new Object();
  private final Map<String, ControlGroupEvidence> controlGroups = new LinkedHashMap<>();
  private volatile RuntimeDopExecutionSnapshot frozenControlEvidence;
  private long controlSequence;
  // Admission and terminal sealing share evidenceLock; a done native future alone does not prove
  // that its evidence listener has executed.
  private final SettableFuture<Void> controlSettlementsDone = SettableFuture.create();
  private int pendingControlSettlements;
  private boolean controlSettlementsSealed;
  private boolean successfulOwnersCleared;

  public RuntimeFragmentLifecycle(FragmentInstanceContext context, Executor executor) {
    this.context = Objects.requireNonNull(context);
    this.cleanup = new RuntimeCleanupCoordinator(executor);
    cleanup.addRequiredCleanup(BEFORE_DRIVER_CLOSE, this::stopNativeTasks);
    cleanup.addRequiredCleanup(
        BEFORE_DRIVER_CLOSE,
        () -> {
          RuntimeDopController current = controller;
          if (current == null) return Futures.immediateVoidFuture();
          current.close();
          return current.getStoppedFuture();
        });

    cleanup.addRequiredCleanup(BEFORE_DRIVER_CLOSE, this::sealControlSettlements);

    cleanup.addCleanup(
        FREEZE_STATISTICS,
        () -> {
          freezeControlEvidence();
          return Futures.immediateVoidFuture();
        });
    cleanup.addRequiredCleanup(CLOSE_ENDPOINTS, this::closePlanningOwners);
    cleanup.addRequiredCleanup(RELEASE_FRONTIERS, this::closeFrontiers);
    cleanup.addRequiredCleanup(
        RELEASE_RESOURCES,
        () -> {
          context.releaseResourceWhenAllDriversAreClosed();
          context.releaseRuntimeQueryResources();
          context.releaseMemoryReservationManager();
          return Futures.immediateVoidFuture();
        });
    cleanup.addRequiredCleanup(
        DEREGISTER,
        () -> {
          MPPDataExchangeManager manager = exchangeManager;
          if (manager != null) {
            manager.forceDeregisterFragmentInstance(context.getId().toThrift());
            manager.deRegisterFragmentInstanceFromMemoryPool(
                context.getId().getQueryId().getId(),
                context.getId().getFragmentInstanceId(),
                true);
          }
          context.setPlanningContext(null);
          return Futures.immediateVoidFuture();
        });
    ListenableFuture<Void> completed = cleanup.getCompletedFuture();
    completed.addListener(
        () -> {
          try {
            Futures.getDone(completed);
          } catch (java.util.concurrent.ExecutionException
              | java.util.concurrent.CancellationException failedCleanup) {
            return;
          }
          clearSuccessfulOwners();
        },
        MoreExecutors.directExecutor());
  }

  public RuntimeCleanupCoordinator getCleanupCoordinator() {
    return cleanup;
  }

  public void bindExchangeManager(MPPDataExchangeManager manager) {
    if (exchangeManager != null && exchangeManager != manager) {
      throw new IllegalStateException();
    }
    exchangeManager = Objects.requireNonNull(manager);
  }

  public void registerOriginalDriver(PipelineDriverFactory factory, IDriver driver) {
    Objects.requireNonNull(factory);
    Objects.requireNonNull(driver);
    if (originals.putIfAbsent(factory, driver) != null) {
      throw new IllegalArgumentException();
    }
    cleanup.registerDriver(driver);
  }

  public void sealConstruction() {
    constructionSealed = true;
    cleanup.sealConstruction();
    if (schedulingGroup != null && !cleanup.isStopRequested() && !groups.isEmpty()) {
      controller = new RuntimeDopController(this);
      controller.start();
      if (cleanup.isStopRequested()) controller.close();
    }
  }

  public ListenableFuture<Void> requestStop() {
    sealControlSettlements();
    RuntimeDopController current = controller;
    if (current != null) current.close();
    return cleanup.requestStop();
  }

  public ListenableFuture<Void> getCompletedFuture() {
    return cleanup.getCompletedFuture();
  }

  /** All dependencies were captured against the original full factory order by the planner. */
  public void prepareAndSubmit(
      IDriverScheduler scheduler,
      LocalExecutionPlanContext.RuntimePlanBindings bindings,
      List<IDriver> logicalDrivers)
      throws org.apache.iotdb.db.queryengine.exception.CpuNotEnoughException,
          org.apache.iotdb.calc.exception.MemoryNotEnoughException {
    if (this.scheduler != null) {
      throw new IllegalStateException();
    }
    this.scheduler = Objects.requireNonNull(scheduler);
    this.bindings = Objects.requireNonNull(bindings);
    List<PipelineDriverFactory> factories = bindings.getOriginalFactories();
    if (factories.size() != logicalDrivers.size()) {
      throw new IllegalArgumentException();
    }
    for (int i = 0; i < factories.size(); i++) {
      if (originals.get(factories.get(i)) != logicalDrivers.get(i)) {
        throw new IllegalArgumentException();
      }
      nextPipelineId =
          Math.max(
              nextPipelineId,
              Math.addExact(factories.get(i).getDriverContext().getPipelineId(), 1));
    }
    List<DriverSubmission> submissions = new ArrayList<>();
    long baseMemoryBytes = 0;
    for (int i = 0; i < factories.size(); i++) {
      IDriver driver = logicalDrivers.get(i);
      baseMemoryBytes = Math.addExact(baseMemoryBytes, driver.getEstimatedMemorySize());
      if (bindings.groupForFactory(factories.get(i)) == null) {
        submissions.add(
            new DriverSubmission(
                driver,
                bindings.startDependencyFor(factories.get(i)),
                DriverSubmission.Role.DIRECT_NATIVE,
                0));
      }
    }
    for (RuntimeScanGroupDescriptor descriptor : bindings.getGroups()) {
      List<IDriver> lanes = new ArrayList<>();
      List<ListenableFuture<?>> dependencies = new ArrayList<>();
      for (RuntimeScanGroupDescriptor.Lane lane : descriptor.getLanes()) {
        lanes.add(logicalDrivers.get(lane.getOriginalFactoryIndex()));
        dependencies.add(lane.getStartDependency());
      }
      RuntimeScanGroup laneGroup = new RuntimeScanGroup(lanes, dependencies);
      ExecutionGroup group = new ExecutionGroup(descriptor, laneGroup);
      groups.put(descriptor.getGroupId(), group);
      registerControlGroup(descriptor);
      cleanup.registerGroup(laneGroup);
      for (int n = 0; n < descriptor.getLayout().getInitialWorkers(); n++) {
        RuntimeScanWorker worker = createWorker(group);
        group.members.add(new WorkerMember(worker));
        submissions.add(workerSubmission(worker));
      }
    }
    schedulingGroup =
        scheduler.openRuntimeGroup(
            context,
            context.getOuterQueryDeadlineMs(),
            context.getSessionInfo(),
            this::requestStop,
            context::failed);
    if (cleanup.isStopRequested()) {
      scheduler.stopRuntimeGroup(schedulingGroup, context.getFailureCause().orElse(null));
      throw new java.util.concurrent.CancellationException();
    }
    PreparedDriverBatch batch =
        scheduler.prepareRuntimeDrivers(schedulingGroup, submissions, baseMemoryBytes);
    bindRegistrations(batch);
    if (cleanup.isStopRequested() || context.getStateMachine().getState().isDone()) {
      scheduler.stopRuntimeGroup(schedulingGroup, context.getFailureCause().orElse(null));
      throw new java.util.concurrent.CancellationException();
    }
    scheduler.activateRuntimeBatch(batch);
    for (ExecutionGroup group : groups.values()) {
      recordControlOutcome(
          group.descriptor.getGroupId(),
          group.descriptor.getLayout().getInitialWorkers(),
          group.descriptor.getLayout().getInitialWorkers(),
          RuntimeDopExecutionSnapshot.Outcome.INITIAL_ACTIVATED);
    }
  }

  /**
   * Change worker membership while retaining every original lane, work ID and exchange object. One
   * actuation is allowed at a time; callers sample again after its receipt completes.
   */
  public ListenableFuture<?> resize(String groupId, int targetWorkers) throws Exception {
    if (!constructionSealed) throw new IllegalStateException();
    ControlSettlement settlement = registerControlSettlement();
    if (settlement == null) {
      int observed = observedControlWorkers(groupId);
      recordControlOutcome(
          groupId,
          Math.max(0, targetWorkers),
          observed,
          RuntimeDopExecutionSnapshot.Outcome.STOPPED);
      return Futures.immediateFuture(
          new ResizeResult(groupId, targetWorkers, observed, ResizeStatus.STOPPED));
    }
    ExecutionGroup group = null;
    boolean claimed = false;
    try {
      if (cleanup.isStopRequested()) {
        int observed = observedControlWorkers(groupId);
        recordControlOutcome(
            groupId,
            Math.max(0, targetWorkers),
            observed,
            RuntimeDopExecutionSnapshot.Outcome.STOPPED);
        settleControl(settlement);
        return Futures.immediateFuture(
            new ResizeResult(groupId, targetWorkers, observed, ResizeStatus.STOPPED));
      }
      group = Objects.requireNonNull(groups.get(groupId));
      if (targetWorkers < 1 || targetWorkers > group.descriptor.getLayout().getLaneCount()) {
        throw new IllegalArgumentException();
      }
      if (!resizeInProgress.compareAndSet(false, true)) {
        int observed = group.liveMembers().size();
        recordControlOutcome(
            groupId, targetWorkers, observed, RuntimeDopExecutionSnapshot.Outcome.BUSY);
        settleControl(settlement);
        return Futures.immediateFuture(
            new ResizeResult(groupId, targetWorkers, observed, ResizeStatus.BUSY));
      }
      claimed = true;
      ListenableFuture<?> result;
      final ExecutionGroup activeGroup = group;
      List<WorkerMember> live = activeGroup.liveMembers();
      if (targetWorkers > live.size()) {
        List<AutoCloseable> prepaid = new ArrayList<>();
        try {
          for (int n = live.size(); n < targetWorkers; n++) {
            prepaid.add(
                LocalExecutionPlanner.getInstance()
                    .reserveRuntimeWorkerMemory(WORKER_MEMORY_BYTES));
          }
        } catch (org.apache.iotdb.calc.exception.MemoryNotEnoughException denied) {
          closeUnsubmittedMemory(prepaid);
          result =
              Futures.immediateFuture(
                  new ResizeResult(
                      groupId, targetWorkers, live.size(), ResizeStatus.MEMORY_DENIED));
          return observeResizeResult(groupId, targetWorkers, result, settlement);
        }
        List<DriverSubmission> submissions = new ArrayList<>();
        int transferred = 0;
        try {
          for (AutoCloseable memory : prepaid) {
            // createWorker owns and closes the current lease even if construction is rejected.
            transferred++;
            RuntimeScanWorker worker = createWorker(activeGroup, memory);
            activeGroup.members.add(new WorkerMember(worker));
            submissions.add(workerSubmission(worker));
          }
        } finally {
          closeUnsubmittedMemory(prepaid.subList(transferred, prepaid.size()));
        }
        org.apache.iotdb.db.queryengine.execution.schedule.RuntimeGrowthPreparation preparation =
            scheduler.prepareRuntimeGrowth(schedulingGroup, submissions);
        bindRegistrations(preparation.getCandidates());
        if (preparation.isQuotaDenied()) {
          result =
              Futures.transform(
                  preparation.getRejectionSettlementFuture(),
                  ignored -> {
                    reapRetiredWorkers();
                    return new ResizeResult(
                        groupId,
                        targetWorkers,
                        activeGroup.liveMembers().size(),
                        ResizeStatus.QUOTA_DENIED);
                  },
                  MoreExecutors.directExecutor());
        } else {
          scheduler.activateRuntimeBatch(preparation.getAdmittedBatch().orElseThrow());
          result =
              Futures.immediateFuture(
                  new ResizeResult(
                      groupId,
                      targetWorkers,
                      activeGroup.liveMembers().size(),
                      ResizeStatus.APPLIED));
        }
      } else {
        List<ListenableFuture<?>> pending = new ArrayList<>();
        for (int n = targetWorkers; n < live.size(); n++) {
          WorkerMember member = live.get(n);
          ListenableFuture<?> removed =
              scheduler.retireRuntimeWorker(schedulingGroup, member.registration);
          pending.add(removed);
          pending.add(member.worker.getRetiredFuture());
          pending.add(member.registration.getQuotaSettledFuture());
        }
        result =
            Futures.transform(
                Futures.allAsList(pending),
                ignored -> {
                  reapRetiredWorkers();
                  return new ResizeResult(
                      groupId,
                      targetWorkers,
                      activeGroup.liveMembers().size(),
                      ResizeStatus.APPLIED);
                },
                MoreExecutors.directExecutor());
      }
      return observeResizeResult(groupId, targetWorkers, result, settlement);
    } catch (Throwable failure) {
      if (claimed) resizeInProgress.set(false);
      try {
        recordControlOutcome(
            groupId,
            targetWorkers,
            group == null ? observedControlWorkers(groupId) : group.liveMembers().size(),
            cleanup.isStopRequested()
                ? RuntimeDopExecutionSnapshot.Outcome.STOPPED
                : RuntimeDopExecutionSnapshot.Outcome.FAILED);
      } finally {
        settleControl(settlement);
      }
      if (!claimed) {
        if (failure instanceof Exception) throw (Exception) failure;
        throw (Error) failure;
      }
      if (cleanup.isStopRequested() && !(failure instanceof Error)) {
        java.util.concurrent.CancellationException stopped =
            new java.util.concurrent.CancellationException();
        stopped.initCause(failure);
        throw stopped;
      }
      // Native members may still be pending. Their normal terminal owner supplies physical proofs;
      // the independent control barrier only acknowledges that this outcome has been recorded.
      context.failed(failure);
      requestStop();
      if (failure instanceof Exception) throw (Exception) failure;
      throw (Error) failure;
    }
  }

  ListenableFuture<?> observeResizeResult(
      String groupId, int requested, ListenableFuture<?> receipt) {
    ControlSettlement settlement = registerControlSettlement();
    if (settlement == null) throw new IllegalStateException();
    return observeResizeResult(groupId, requested, receipt, settlement);
  }

  private ListenableFuture<?> observeResizeResult(
      String groupId, int requested, ListenableFuture<?> receipt, ControlSettlement settlement) {
    SettableFuture<Object> published = SettableFuture.create();
    receipt.addListener(
        () -> {
          Object value = null;
          Throwable receiptFailure = null;
          try {
            value = Futures.getDone(receipt);
            if (!(value instanceof ResizeResult)) throw new IllegalStateException();
            ResizeResult settled = (ResizeResult) value;
            recordControlOutcome(
                groupId,
                requested,
                settled.observedWorkers(),
                RuntimeDopExecutionSnapshot.Outcome.valueOf(settled.status().name()));
          } catch (Throwable failure) {
            receiptFailure = failure;
            recordControlOutcome(
                groupId,
                requested,
                observedControlWorkers(groupId),
                cleanup.isStopRequested()
                    ? RuntimeDopExecutionSnapshot.Outcome.STOPPED
                    : RuntimeDopExecutionSnapshot.Outcome.FAILED);
          } finally {
            resizeInProgress.set(false);
            // This may reenter terminal cleanup. No further physical owner is accessed below.
            // Complete it before notifying caller listeners, which may themselves request stop.
            settleControl(settlement);
          }
          if (receiptFailure == null) {
            published.set(value);
          } else if (receiptFailure instanceof java.util.concurrent.CancellationException) {
            published.cancel(false);
          } else {
            Throwable actual =
                receiptFailure instanceof java.util.concurrent.ExecutionException
                        && receiptFailure.getCause() != null
                    ? receiptFailure.getCause()
                    : receiptFailure;
            published.setException(actual);
          }
        },
        MoreExecutors.directExecutor());
    return Futures.nonCancellationPropagating(published);
  }

  private ControlSettlement registerControlSettlement() {
    synchronized (evidenceLock) {
      if (controlSettlementsSealed || frozenControlEvidence != null) return null;
      pendingControlSettlements++;
      return new ControlSettlement();
    }
  }

  private ListenableFuture<Void> sealControlSettlements() {
    boolean complete;
    synchronized (evidenceLock) {
      controlSettlementsSealed = true;
      complete = pendingControlSettlements == 0;
    }
    // Never notify futures while holding evidenceLock: direct listeners can run terminal phases.
    if (complete) controlSettlementsDone.set(null);
    return Futures.nonCancellationPropagating(controlSettlementsDone);
  }

  private void settleControl(ControlSettlement settlement) {
    boolean complete;
    synchronized (evidenceLock) {
      if (settlement.settled) return;
      settlement.settled = true;
      pendingControlSettlements--;
      complete = controlSettlementsSealed && pendingControlSettlements == 0;
    }
    if (complete) controlSettlementsDone.set(null);
  }

  private static final class ControlSettlement {
    // Guarded by the enclosing lifecycle's evidenceLock.
    private boolean settled;
  }

  private void registerControlGroup(RuntimeScanGroupDescriptor descriptor) {
    org.apache.iotdb.db.queryengine.plan.planner.ScanParallelPlan plan =
        descriptor.getLayout().getLanePlan();
    Map<Integer, org.apache.iotdb.db.queryengine.plan.planner.ScanParallelPlan.Bucket> buckets =
        new LinkedHashMap<>();
    for (org.apache.iotdb.db.queryengine.plan.planner.ScanParallelPlan.Bucket bucket :
        plan.getBuckets()) {
      if (buckets.putIfAbsent(bucket.getBucketId(), bucket) != null) {
        throw new IllegalStateException();
      }
    }
    Map<Integer, RuntimeDopExecutionSnapshot.LaneIdentity> lanes = new LinkedHashMap<>();
    for (RuntimeScanGroupDescriptor.Lane lane : descriptor.getLanes()) {
      org.apache.iotdb.db.queryengine.plan.planner.ScanParallelPlan.Bucket bucket =
          buckets.get(lane.getLaneIndex());
      if (bucket == null) throw new IllegalStateException();
      List<String> workUnits = new ArrayList<>();
      for (org.apache.iotdb.db.queryengine.plan.planner.ScanParallelPlan.WorkUnit unit :
          bucket.getWorkUnits()) workUnits.add(unit.getSourceId());
      if (lanes.putIfAbsent(
              lane.getLaneIndex(),
              new RuntimeDopExecutionSnapshot.LaneIdentity(
                  lane.getLaneIndex(),
                  lane.getOriginalPipelineId(),
                  bucket.getBucketId(),
                  workUnits))
          != null) {
        throw new IllegalStateException();
      }
    }
    if (lanes.size() != descriptor.getLayout().getLaneCount()) throw new IllegalStateException();
    synchronized (evidenceLock) {
      if (frozenControlEvidence != null
          || controlGroups.putIfAbsent(
                  descriptor.getGroupId(),
                  new ControlGroupEvidence(
                      descriptor.getGroupId(),
                      descriptor.getLayout().getLaneCount(),
                      descriptor.getLayout().getInitialWorkers(),
                      descriptor.getSourcePlanNodeId(),
                      plan.getPlanChecksum(),
                      plan.getPlanIdentityChecksum(),
                      lanes))
              != null) {
        throw new IllegalStateException();
      }
    }
  }

  void registerControlGroup(String groupId, int lanes, int initialWorkers) {
    synchronized (evidenceLock) {
      if (frozenControlEvidence != null
          || controlGroups.putIfAbsent(
                  groupId, new ControlGroupEvidence(groupId, lanes, initialWorkers))
              != null) {
        throw new IllegalStateException();
      }
    }
  }

  private int observedControlWorkers(String groupId) {
    synchronized (evidenceLock) {
      ControlGroupEvidence group = controlGroups.get(groupId);
      return group == null ? 0 : group.lastObservedWorkers;
    }
  }

  private void recordControlOutcome(
      String groupId, int requested, int observed, RuntimeDopExecutionSnapshot.Outcome outcome) {
    synchronized (evidenceLock) {
      if (frozenControlEvidence != null) return;
      ControlGroupEvidence group = controlGroups.get(groupId);
      if (group == null) return;
      group.record(++controlSequence, requested, observed, outcome);
    }
  }

  /** Freeze values without retaining any driver, descriptor, future, endpoint or exception. */
  public RuntimeDopExecutionSnapshot freezeControlEvidence() {
    sealControlSettlements();
    RuntimeDopExecutionSnapshot result;
    synchronized (evidenceLock) {
      if (pendingControlSettlements != 0) throw new IllegalStateException();
      if (frozenControlEvidence == null) {
        RuntimeDopController currentController = controller;
        List<RuntimeDopController.Event> controllerEvents = null;
        if (currentController != null) {
          // BEFORE_DRIVER_CLOSE closes the controller and awaits this barrier. Copy
          // its existing bounded events only after sampling has acknowledged stop.
          ListenableFuture<Void> stopped = currentController.getStoppedFuture();
          if (!stopped.isDone())
            throw new IllegalStateException("Controller sampling is not stopped");
          try {
            Futures.getDone(stopped);
          } catch (java.util.concurrent.ExecutionException
              | java.util.concurrent.CancellationException failure) {
            throw new IllegalStateException(
                "Controller sampling did not stop successfully", failure);
          }
          controllerEvents = currentController.snapshotEvents();
        }
        Map<String, RuntimeDopExecutionSnapshot.GroupSnapshot> groups = new LinkedHashMap<>();
        controlGroups.forEach((id, group) -> groups.put(id, group.snapshot()));
        String state =
            context.getStateMachine() == null
                ? "UNKNOWN"
                : Objects.toString(context.getStateMachine().getState(), "UNKNOWN");
        frozenControlEvidence =
            new RuntimeDopExecutionSnapshot(
                context.getId().getQueryId().getId(),
                context.getId().getFullId(),
                context.getOuterQueryDeadlineMs(),
                state,
                groups,
                controllerEvents);
      }
      result = frozenControlEvidence;
    }
    context.setRuntimeControlEvidence(result);
    return result;
  }

  /** Keep the lightweight lifecycle sentinel, but detach every settled physical-owner alias. */
  public synchronized void clearSuccessfulOwners() {
    if (successfulOwnersCleared) return;
    ListenableFuture<Void> completed = cleanup.getCompletedFuture();
    if (!completed.isDone()) throw new IllegalStateException();
    try {
      Futures.getDone(completed);
    } catch (java.util.concurrent.ExecutionException
        | java.util.concurrent.CancellationException failedCleanup) {
      throw new IllegalStateException(failedCleanup);
    }
    freezeControlEvidence();
    context.clearRuntimePlanBindings();
    cleanup.releaseSuccessfulOwners();
    originals.clear();
    uncreatedClosed.clear();
    groups.clear();
    bindings = null;
    scheduler = null;
    schedulingGroup = null;
    exchangeManager = null;
    controller = null;
    synchronized (evidenceLock) {
      controlGroups.clear();
    }
    successfulOwnersCleared = true;
  }

  public Map<String, RuntimeDopPolicy.Input> sampleControlInputs(long nowNanos, long sequence) {
    Map<String, RuntimeDopPolicy.Input> result = new LinkedHashMap<>();
    for (Map.Entry<String, ExecutionGroup> entry : groups.entrySet()) {
      ExecutionGroup group = entry.getValue();
      Set<Integer> fullOutputs = new java.util.HashSet<>();
      for (RuntimeScanGroupDescriptor.Lane lane : group.descriptor.getLanes()) {
        if (!group.descriptor.getReservation().whenWritable(lane.getLaneIndex()).isDone()) {
          fullOutputs.add(lane.getLaneIndex());
        }
      }
      RuntimeScanGroup.Snapshot lanes = group.group.snapshotWithObservedOutputs(fullOutputs);
      long completed = 0;
      long total = 0;
      boolean covered = !group.sources.isEmpty();
      for (org.apache.iotdb.db.queryengine.execution.operator.source.relational
              .AbstractTableScanOperator
          source : group.sources) {
        java.util.Optional<RuntimeScanProgress.Snapshot> progress = source.getRuntimeWorkProgress();
        if (progress.isEmpty()) {
          covered = false;
          break;
        }
        completed = Math.addExact(completed, progress.get().completed());
        total = Math.addExact(total, progress.get().total());
      }
      int workers = group.liveMembers().size();
      RuntimeDopPolicy.MetricValue completedValue =
          covered
              ? RuntimeDopPolicy.MetricValue.valid(completed)
              : RuntimeDopPolicy.MetricValue.unsupported();
      RuntimeDopPolicy.MetricValue totalValue =
          covered
              ? RuntimeDopPolicy.MetricValue.valid(total)
              : RuntimeDopPolicy.MetricValue.unsupported();
      result.put(
          entry.getKey(),
          new RuntimeDopPolicy.Input(
              entry.getKey(),
              nowNanos,
              nowNanos,
              sequence,
              lanes.size(),
              lanes.getUnfinishedCount(),
              workers,
              lanes.getLeasedCount(),
              lanes.getReadyCount(),
              lanes.getBackpressuredCount(),
              lanes.size(),
              completedValue,
              totalValue,
              resizeInProgress.get(),
              cleanup.isStopRequested() || lanes.isStopped() || lanes.isFinished()));
    }
    reapRetiredWorkers();
    return Collections.unmodifiableMap(result);
  }

  public RuntimeDopController getController() {
    return controller;
  }

  private void reapRetiredWorkers() {
    for (ExecutionGroup group : groups.values()) {
      for (WorkerMember member : group.members) {
        TaskRegistration registration = member.registration;
        if (registration == null || !registration.getQuotaSettledFuture().isDone()) continue;
        try {
          Futures.getDone(registration.getQuotaSettledFuture());
          if (cleanup.unregisterRetiredWorker(member.worker)) group.members.remove(member);
        } catch (Exception failure) {
          context.failed(failure);
          requestStop();
        }
      }
    }
  }

  public void fail(Throwable failure) {
    context.failed(failure);
    requestStop();
  }

  public boolean isResizeInProgress() {
    return resizeInProgress.get();
  }

  public Map<String, RuntimeScanGroup> getScanGroups() {
    Map<String, RuntimeScanGroup> result = new LinkedHashMap<>();
    for (Map.Entry<String, ExecutionGroup> entry : groups.entrySet()) {
      result.put(entry.getKey(), entry.getValue().group);
    }
    return Collections.unmodifiableMap(result);
  }

  public int getLiveWorkerCount(String groupId) {
    return Objects.requireNonNull(groups.get(groupId)).liveMembers().size();
  }

  private RuntimeScanWorker createWorker(ExecutionGroup group) {
    return createWorker(
        group, LocalExecutionPlanner.getInstance().reserveRuntimeWorkerMemory(WORKER_MEMORY_BYTES));
  }

  private RuntimeScanWorker createWorker(ExecutionGroup group, AutoCloseable memory) {
    boolean transferred = false;
    RuntimeScanWorker worker = null;
    try {
      int pipeline = nextPipelineId;
      nextPipelineId = Math.addExact(nextPipelineId, 1);
      worker =
          new RuntimeScanWorker(
              group.group,
              new DriverContext(context, pipeline),
              pipeline,
              WORKER_MEMORY_BYTES,
              () -> {
                try {
                  memory.close();
                } catch (Exception failure) {
                  throw new CompletionException(failure);
                }
              });
      worker.setHighestPriority(context.isHighestPriority());
      if (constructionSealed) cleanup.registerWorker(worker);
      else cleanup.registerDriver(worker);
      transferred = true;
      return worker;
    } finally {
      if (!transferred) {
        if (worker != null) worker.requestRetirement();
        else closeUnsubmittedMemory(Collections.singletonList(memory));
      }
    }
  }

  public enum ResizeStatus {
    APPLIED,
    MEMORY_DENIED,
    QUOTA_DENIED,
    STOPPED,
    BUSY
  }

  public record ResizeResult(
      String groupId, int requestedWorkers, int observedWorkers, ResizeStatus status) {}

  private static void closeUnsubmittedMemory(List<AutoCloseable> leases) {
    Throwable first = null;
    for (AutoCloseable lease : leases) {
      try {
        lease.close();
      } catch (Throwable failure) {
        if (first == null) first = failure;
        else if (first != failure) first.addSuppressed(failure);
      }
    }
    if (first != null) throw new CompletionException(first);
  }

  private DriverSubmission workerSubmission(RuntimeScanWorker worker) {
    // Per-lane dependencies remain in RuntimeScanGroup; an idle worker must not wait on one lane.
    return new DriverSubmission(
        worker,
        Futures.immediateVoidFuture(),
        DriverSubmission.Role.SCAN_WORKER,
        WORKER_MEMORY_BYTES);
  }

  private void bindRegistrations(PreparedDriverBatch batch) {
    bindRegistrations(batch.getRegistrations());
  }

  private void bindRegistrations(List<TaskRegistration> registrations) {
    for (TaskRegistration registration : registrations) {
      if (!(registration.getDriver() instanceof RuntimeScanWorker)) continue;
      RuntimeScanWorker worker = (RuntimeScanWorker) registration.getDriver();
      for (ExecutionGroup group : groups.values()) {
        for (WorkerMember member : group.members) {
          if (member.worker == worker) member.registration = registration;
        }
      }
      ListenableFuture<?> removed = registration.getRemovedFuture();
      removed.addListener(
          () -> {
            try {
              Futures.getDone(removed);
              worker.acknowledgeSchedulerRemoval();
            } catch (Exception failure) {
              context.failed(failure);
            }
          },
          MoreExecutors.directExecutor());
    }
  }

  private ListenableFuture<?> stopNativeTasks() {
    RuntimeSchedulingGroup group = schedulingGroup;
    return group == null
        ? Futures.immediateVoidFuture()
        : scheduler.stopRuntimeGroup(group, context.getFailureCause().orElse(null));
  }

  private ListenableFuture<?> closePlanningOwners() throws Exception {
    LocalExecutionPlanContext planning = context.getPlanningContext();
    if (planning == null) return Futures.immediateVoidFuture();
    Throwable first = null;
    for (PipelineDriverFactory factory : planning.getPipelineDriverFactories()) {
      if (originals.containsKey(factory) || !uncreatedClosed.add(factory)) continue;
      try {
        factory.closeUncreated();
      } catch (Throwable failure) {
        if (first == null) first = failure;
        else if (first != failure) first.addSuppressed(failure);
      } finally {
        context.decrementNumOfUnClosedDriver();
      }
    }
    List<ListenableFuture<?>> pending = new ArrayList<>();
    pending.add(
        planning
            .getResourceRegistry()
            .closeRuntimeOwners(
                context.getStateMachine().getState().isFailed(),
                context.getFailureCause().orElse(null)));
    if (first != null) pending.add(Futures.immediateFailedFuture(first));
    return Futures.allAsList(pending);
  }

  private ListenableFuture<?> closeFrontiers() {
    LocalExecutionPlanContext planning = context.getPlanningContext();
    if (planning == null) return Futures.immediateVoidFuture();
    List<ListenableFuture<?>> pending = new ArrayList<>();
    for (RuntimeFrontierReservation reservation : planning.getOwnedRuntimeFrontiers()) {
      reservation.requestClose();
      pending.add(reservation.getClosedFuture());
    }
    return Futures.allAsList(pending);
  }

  private static final class ControlGroupEvidence {
    private final String groupId;
    private final int lanes;
    private final int initialRequestedWorkers;
    private final String sourcePlanNodeId;
    private final String lanePlanChecksum;
    private final String lanePlanIdentityChecksum;
    private final Map<Integer, RuntimeDopExecutionSnapshot.LaneIdentity> laneIdentities;
    private int initialActivatedWorkers;
    private int lastObservedWorkers;
    private int minAppliedWorkers = Integer.MAX_VALUE;
    private int maxAppliedWorkers;
    private final java.util.EnumMap<RuntimeDopExecutionSnapshot.Outcome, Long> counts =
        new java.util.EnumMap<>(RuntimeDopExecutionSnapshot.Outcome.class);
    private final java.util.ArrayDeque<RuntimeDopExecutionSnapshot.ControlEvent> events =
        new java.util.ArrayDeque<>();

    private ControlGroupEvidence(String groupId, int lanes, int initialWorkers) {
      this(groupId, lanes, initialWorkers, "", "", "", Map.of());
    }

    private ControlGroupEvidence(
        String groupId,
        int lanes,
        int initialWorkers,
        String sourcePlanNodeId,
        String lanePlanChecksum,
        String lanePlanIdentityChecksum,
        Map<Integer, RuntimeDopExecutionSnapshot.LaneIdentity> laneIdentities) {
      this.groupId = Objects.requireNonNull(groupId);
      if (lanes < 1 || initialWorkers < 1 || initialWorkers > lanes) {
        throw new IllegalArgumentException();
      }
      this.lanes = lanes;
      this.initialRequestedWorkers = initialWorkers;
      this.sourcePlanNodeId = Objects.requireNonNull(sourcePlanNodeId);
      this.lanePlanChecksum = Objects.requireNonNull(lanePlanChecksum);
      this.lanePlanIdentityChecksum = Objects.requireNonNull(lanePlanIdentityChecksum);
      this.laneIdentities = Collections.unmodifiableMap(new LinkedHashMap<>(laneIdentities));
    }

    private void record(
        long sequence, int requested, int observed, RuntimeDopExecutionSnapshot.Outcome outcome) {
      lastObservedWorkers = observed;
      if (outcome == RuntimeDopExecutionSnapshot.Outcome.INITIAL_ACTIVATED) {
        initialActivatedWorkers = observed;
      }
      if (outcome == RuntimeDopExecutionSnapshot.Outcome.INITIAL_ACTIVATED
          || outcome == RuntimeDopExecutionSnapshot.Outcome.APPLIED) {
        minAppliedWorkers = Math.min(minAppliedWorkers, observed);
        maxAppliedWorkers = Math.max(maxAppliedWorkers, observed);
      }
      counts.put(outcome, Math.addExact(counts.getOrDefault(outcome, 0L), 1));
      if (events.size() == RuntimeDopExecutionSnapshot.EVENT_LIMIT) events.removeFirst();
      events.addLast(
          new RuntimeDopExecutionSnapshot.ControlEvent(sequence, requested, observed, outcome));
    }

    private RuntimeDopExecutionSnapshot.GroupSnapshot snapshot() {
      return new RuntimeDopExecutionSnapshot.GroupSnapshot(
          groupId,
          sourcePlanNodeId,
          lanePlanChecksum,
          lanePlanIdentityChecksum,
          laneIdentities,
          lanes,
          initialRequestedWorkers,
          initialActivatedWorkers,
          lastObservedWorkers,
          minAppliedWorkers == Integer.MAX_VALUE ? 0 : minAppliedWorkers,
          maxAppliedWorkers,
          counts,
          new ArrayList<>(events));
    }
  }

  private static final class ExecutionGroup {
    private final RuntimeScanGroupDescriptor descriptor;
    private final RuntimeScanGroup group;
    private final List<
            org.apache.iotdb.db.queryengine.execution.operator.source.relational
                .AbstractTableScanOperator>
        sources = new ArrayList<>();
    private final List<WorkerMember> members = new java.util.concurrent.CopyOnWriteArrayList<>();

    private ExecutionGroup(RuntimeScanGroupDescriptor descriptor, RuntimeScanGroup group) {
      this.descriptor = descriptor;
      this.group = group;
      Set<Object> identities = Collections.newSetFromMap(new IdentityHashMap<>());
      for (RuntimeScanGroupDescriptor.Lane lane : descriptor.getLanes()) {
        if (lane.getFactory().getDriverContext()
            instanceof org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext) {
          org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext data =
              (org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext)
                  lane.getFactory().getDriverContext();
          for (org.apache.iotdb.db.queryengine.execution.operator.source.DataSourceOperator source :
              data.getSourceOperators()) {
            if (source
                    instanceof
                    org.apache.iotdb.db.queryengine.execution.operator.source.relational
                        .AbstractTableScanOperator
                && identities.add(source)) {
              sources.add(
                  (org.apache.iotdb.db.queryengine.execution.operator.source.relational
                          .AbstractTableScanOperator)
                      source);
            }
          }
        }
      }
    }

    private List<WorkerMember> liveMembers() {
      List<WorkerMember> result = new ArrayList<>();
      for (WorkerMember member : members) {
        if (!member.worker.getRetiredFuture().isDone()) result.add(member);
      }
      return result;
    }
  }

  private static final class WorkerMember {
    private final RuntimeScanWorker worker;
    private volatile TaskRegistration registration;

    private WorkerMember(RuntimeScanWorker worker) {
      this.worker = worker;
    }
  }
}
