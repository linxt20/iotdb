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

import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanContext;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionResourceRegistry;
import org.apache.iotdb.db.queryengine.plan.planner.PipelineDriverFactory;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Test;
import org.mockito.InOrder;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/** Verifies lifetime handoff at the FI boundary rather than only the component stop flags. */
public class RuntimeFragmentLifecycleTest {
  @Test
  public void stopDuringConstructionCannotReleaseLateRegisteredLogicalDrivers() throws Exception {
    Fixture f = new Fixture();
    ListenableFuture<Void> completion = f.owner.requestStop();
    assertFalse(completion.isDone());
    f.owner.registerOriginalDriver(f.factory, f.driver);
    verify(f.driver).requestQuiescence();
    verify(f.context, never()).releaseResourceWhenAllDriversAreClosed();
    f.owner.sealConstruction();
    f.quiesced.set(null);
    verify(f.driver).requestClose();
    verify(f.manager, never())
        .deRegisterFragmentInstanceFromMemoryPool(anyString(), anyString(), anyBoolean());
    f.closed.set(null);
    completion.get(5, TimeUnit.SECONDS);
    InOrder order = inOrder(f.driver, f.context, f.manager);
    order.verify(f.driver).requestClose();
    order.verify(f.context).releaseResourceWhenAllDriversAreClosed();
    order.verify(f.context).releaseRuntimeQueryResources();
    order.verify(f.context).releaseMemoryReservationManager();
    order.verify(f.manager).forceDeregisterFragmentInstance(f.context.getId().toThrift());
    order
        .verify(f.manager)
        .deRegisterFragmentInstanceFromMemoryPool("runtime_lifecycle", "0.instance", true);
  }

  @Test
  public void partialDriverCreationClosesOnlyUncreatedFactorySlots() throws Exception {
    Fixture f = new Fixture();
    LocalExecutionPlanContext planning = mock(LocalExecutionPlanContext.class);
    RegistryFixture endpoints = new RegistryFixture(f.context);
    LocalExecutionResourceRegistry registry = endpoints.registry;
    PipelineDriverFactory uncreated = mock(PipelineDriverFactory.class);
    when(planning.getPipelineDriverFactories()).thenReturn(Arrays.asList(f.factory, uncreated));
    when(planning.getResourceRegistry()).thenReturn(registry);
    when(planning.getOwnedRuntimeFrontiers()).thenReturn(Collections.emptyList());
    when(f.context.getPlanningContext()).thenReturn(planning);
    f.owner.registerOriginalDriver(f.factory, f.driver);
    f.quiesced.set(null);
    f.closed.set(null);
    f.owner.requestStop();
    f.owner.sealConstruction();
    f.owner.getCompletedFuture().get(5, TimeUnit.SECONDS);
    f.owner.requestStop().get(5, TimeUnit.SECONDS);
    verify(f.factory, never()).closeUncreated();
    verify(uncreated, times(1)).closeUncreated();
    verify(f.context, times(1)).decrementNumOfUnClosedDriver();
    registry.closeRuntimeOwners(false, null).get(5, TimeUnit.SECONDS);
    assertTrue(endpoints.source.isClosed());
    assertTrue(endpoints.queue.isClosed());
  }

  @Test
  public void failedUncreatedFactoryAttemptsPeersButWithholdsMemoryAndDeregistration()
      throws Exception {
    Fixture f = new Fixture();
    LocalExecutionPlanContext planning = mock(LocalExecutionPlanContext.class);
    RegistryFixture endpoints = new RegistryFixture(f.context);
    LocalExecutionResourceRegistry registry = endpoints.registry;
    PipelineDriverFactory first = mock(PipelineDriverFactory.class);
    PipelineDriverFactory second = mock(PipelineDriverFactory.class);
    IllegalStateException failure = new IllegalStateException();
    doThrow(failure).when(first).closeUncreated();
    when(planning.getPipelineDriverFactories()).thenReturn(Arrays.asList(first, second));
    when(planning.getResourceRegistry()).thenReturn(registry);
    when(f.context.getPlanningContext()).thenReturn(planning);
    f.owner.requestStop();
    f.owner.sealConstruction();
    assertSame(failure, failure(f.owner.getCompletedFuture()));
    verify(first).closeUncreated();
    verify(second).closeUncreated();
    verify(f.context, times(2)).decrementNumOfUnClosedDriver();
    verify(f.context, never()).releaseResourceWhenAllDriversAreClosed();
    verifyZeroInteractions(f.manager);
  }

  @Test
  public void actualPhysicalFailureIsRetainedInsteadOfDeregisteringThePool() throws Exception {
    Fixture f = new Fixture();
    IllegalStateException expected = new IllegalStateException();
    doThrow(expected).when(f.context).releaseResourceWhenAllDriversAreClosed();
    f.owner.requestStop();
    f.owner.sealConstruction();
    assertSame(expected, failure(f.owner.getCompletedFuture()));
    assertSame(expected, failure(f.owner.requestStop()));
    verify(f.context, times(1)).releaseResourceWhenAllDriversAreClosed();
    verify(f.context, never()).releaseRuntimeQueryResources();
    verify(f.context, never()).releaseMemoryReservationManager();
    verifyZeroInteractions(f.manager);
  }

  @Test
  public void frontierReceiptsMustSettleBeforePhysicalMemoryRelease() throws Exception {
    Fixture f = new Fixture();
    LocalExecutionPlanContext planning = mock(LocalExecutionPlanContext.class);
    RegistryFixture endpoints = new RegistryFixture(f.context);
    LocalExecutionResourceRegistry registry = endpoints.registry;
    RuntimeFrontierReservation frontier =
        RuntimeFrontierReservation.tryCreate(
                new org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager()
                    .getQueryPool(),
                new org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue
                    .MemoryReservationKey("runtime_lifecycle_frontier", "0.instance", "frontier"),
                1,
                128,
                1024)
            .get();
    Object queue = new Object();
    frontier.claimQueueLane(0, queue);
    RuntimeFrontierReservation.BlockCredit credit = frontier.acquire(0, queue, 64);
    when(planning.getPipelineDriverFactories()).thenReturn(Collections.emptyList());
    when(planning.getResourceRegistry()).thenReturn(registry);
    when(planning.getOwnedRuntimeFrontiers()).thenReturn(Collections.singletonList(frontier));
    when(f.context.getPlanningContext()).thenReturn(planning);
    f.owner.requestStop();
    f.owner.sealConstruction();
    assertTrue(frontier.isCloseRequested());
    assertFalse(f.owner.getCompletedFuture().isDone());
    verify(f.context, never()).releaseResourceWhenAllDriversAreClosed();
    frontier.release(credit);
    f.owner.getCompletedFuture().get(5, TimeUnit.SECONDS);
    verify(f.context).releaseResourceWhenAllDriversAreClosed();
    verify(f.manager).deRegisterFragmentInstanceFromMemoryPool(anyString(), anyString(), eq(true));
    frontier
        .getMemoryPool()
        .deRegisterFragmentInstanceFromQueryMemoryMap(
            "runtime_lifecycle_frontier", "0.instance", true);
  }

  @Test
  public void successfulCleanupDropsStrongOwnersButKeepsImmutableEvidenceAndStopSentinel()
      throws Exception {
    Fixture f = new Fixture();
    f.owner.registerOriginalDriver(f.factory, f.driver);
    f.owner.registerControlGroup("g", 4, 2);
    f.owner.observeResizeResult(
        "g",
        3,
        Futures.immediateFuture(
            new RuntimeFragmentLifecycle.ResizeResult(
                "g", 3, 3, RuntimeFragmentLifecycle.ResizeStatus.APPLIED)));
    org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler scheduler =
        mock(org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler.class);
    setField(f.owner, "scheduler", scheduler);
    f.quiesced.set(null);
    f.closed.set(null);
    f.owner.requestStop();
    f.owner.sealConstruction();
    f.owner.getCompletedFuture().get(5, TimeUnit.SECONDS);
    RuntimeDopExecutionSnapshot evidence = f.owner.freezeControlEvidence();
    assertSame(evidence, f.owner.freezeControlEvidence());
    assertEquals(
        1L,
        evidence
            .getGroups()
            .get("g")
            .outcomeCounts()
            .get(RuntimeDopExecutionSnapshot.Outcome.APPLIED)
            .longValue());
    verify(f.context, times(1)).clearRuntimePlanBindings();
    assertTrue(((java.util.Map<?, ?>) field(f.owner, "originals")).isEmpty());
    assertTrue(((java.util.Set<?>) field(f.owner, "uncreatedClosed")).isEmpty());
    assertTrue(((java.util.Map<?, ?>) field(f.owner, "groups")).isEmpty());
    assertTrue(((java.util.Map<?, ?>) field(f.owner, "controlGroups")).isEmpty());
    for (String alias :
        Arrays.asList(
            "bindings", "scheduler", "schedulingGroup", "exchangeManager", "controller")) {
      assertNull(field(f.owner, alias));
    }
    assertTrue(((java.util.List<?>) field(f.owner.getCleanupCoordinator(), "drivers")).isEmpty());
    assertTrue(
        ((java.util.Set<?>) field(f.owner.getCleanupCoordinator(), "driverIdentities")).isEmpty());
    for (Object callbacks :
        ((java.util.Map<?, ?>) field(f.owner.getCleanupCoordinator(), "steps")).values()) {
      assertTrue(((java.util.List<?>) callbacks).isEmpty());
    }
    f.owner.clearSuccessfulOwners();
    f.owner.requestStop().get(5, TimeUnit.SECONDS);
    RuntimeFragmentLifecycle.ResizeResult stopped =
        (RuntimeFragmentLifecycle.ResizeResult) f.owner.resize("g", 2).get(5, TimeUnit.SECONDS);
    assertEquals(RuntimeFragmentLifecycle.ResizeStatus.STOPPED, stopped.status());
    assertSame(evidence, f.owner.freezeControlEvidence());
    verify(f.context, times(1)).releaseRuntimeQueryResources();
    verify(f.context, times(1)).clearRuntimePlanBindings();
  }

  @Test
  public void failedPhysicalCloseRetainsOwnersAndRefusesSuccessOnlyDetach() throws Exception {
    Fixture f = new Fixture();
    f.owner.registerOriginalDriver(f.factory, f.driver);
    f.quiesced.set(null);
    IllegalStateException expected = new IllegalStateException();
    f.closed.setException(expected);
    f.owner.requestStop();
    f.owner.sealConstruction();
    assertSame(expected, failure(f.owner.getCompletedFuture()));
    assertThrows(IllegalStateException.class, f.owner::clearSuccessfulOwners);
    assertTrue(((java.util.Map<?, ?>) field(f.owner, "originals")).containsValue(f.driver));
    assertEquals(
        Collections.singletonList(f.driver), field(f.owner.getCleanupCoordinator(), "drivers"));
    assertFalse(
        ((java.util.Set<?>) field(f.owner.getCleanupCoordinator(), "requiredSteps")).isEmpty());
    assertSame(f.manager, field(f.owner, "exchangeManager"));
    verify(f.context, never()).clearRuntimePlanBindings();
    verify(f.context, never()).releaseResourceWhenAllDriversAreClosed();
    assertSame(f.owner.freezeControlEvidence(), f.owner.freezeControlEvidence());
  }

  @Test
  public void pendingReceiptAndDeniedOutcomesCannotClaimAppliedResize() throws Exception {
    Fixture f = new Fixture();
    f.owner.registerControlGroup("g", 4, 2);
    SettableFuture<RuntimeFragmentLifecycle.ResizeResult> pending = SettableFuture.create();
    f.owner.observeResizeResult("g", 4, pending);
    assertThrows(IllegalStateException.class, f.owner::freezeControlEvidence);
    pending.set(
        new RuntimeFragmentLifecycle.ResizeResult(
            "g", 4, 4, RuntimeFragmentLifecycle.ResizeStatus.APPLIED));
    RuntimeDopExecutionSnapshot frozen = f.owner.freezeControlEvidence();
    RuntimeDopExecutionSnapshot.GroupSnapshot group = frozen.getGroups().get("g");
    assertEquals(
        1L, group.outcomeCounts().get(RuntimeDopExecutionSnapshot.Outcome.APPLIED).longValue());
    assertEquals(0, group.initialActivatedWorkers());
    assertEquals(1, group.events().size());
    assertSame(frozen, f.owner.freezeControlEvidence());

    Fixture denied = new Fixture();
    denied.owner.registerControlGroup("g", 4, 2);
    for (RuntimeFragmentLifecycle.ResizeStatus outcome :
        Arrays.asList(
            RuntimeFragmentLifecycle.ResizeStatus.QUOTA_DENIED,
            RuntimeFragmentLifecycle.ResizeStatus.MEMORY_DENIED,
            RuntimeFragmentLifecycle.ResizeStatus.STOPPED,
            RuntimeFragmentLifecycle.ResizeStatus.BUSY)) {
      denied.owner.observeResizeResult(
          "g",
          4,
          Futures.immediateFuture(new RuntimeFragmentLifecycle.ResizeResult("g", 4, 2, outcome)));
    }
    RuntimeDopExecutionSnapshot.GroupSnapshot rejected =
        denied.owner.freezeControlEvidence().getGroups().get("g");
    assertEquals(
        0L, rejected.outcomeCounts().get(RuntimeDopExecutionSnapshot.Outcome.APPLIED).longValue());
    assertEquals(0, rejected.maxAppliedWorkers());
    for (RuntimeDopExecutionSnapshot.Outcome outcome :
        Arrays.asList(
            RuntimeDopExecutionSnapshot.Outcome.QUOTA_DENIED,
            RuntimeDopExecutionSnapshot.Outcome.MEMORY_DENIED,
            RuntimeDopExecutionSnapshot.Outcome.STOPPED,
            RuntimeDopExecutionSnapshot.Outcome.BUSY)) {
      assertEquals(1L, rejected.outcomeCounts().get(outcome).longValue());
    }
  }

  @Test
  public void reentrantNativeRetirementListenerCannotFreezeBeforeAppliedEvidence()
      throws Exception {
    Fixture f = new Fixture();
    f.owner.registerOriginalDriver(f.factory, f.driver);
    f.owner.registerControlGroup("g", 4, 2);
    f.owner.sealConstruction();
    SettableFuture<Void> retired = SettableFuture.create();
    java.util.concurrent.atomic.AtomicReference<Throwable> firstListenerFailure =
        new java.util.concurrent.atomic.AtomicReference<>();
    // Native scheduler acknowledgment can complete group stop from the first listener of the
    // same retirement future whose later listener still has to derive the resize result.
    retired.addListener(
        () -> {
          try {
            f.owner.requestStop();
            f.quiesced.set(null);
            f.closed.set(null);
            assertFalse(f.owner.getCompletedFuture().isDone());
            verify(f.context, never()).setRuntimeControlEvidence(any());
            verify(f.context, never()).releaseResourceWhenAllDriversAreClosed();
          } catch (Throwable failure) {
            firstListenerFailure.set(failure);
          }
        },
        MoreExecutors.directExecutor());
    ListenableFuture<RuntimeFragmentLifecycle.ResizeResult> nativeReceipt =
        Futures.transform(
            retired,
            ignored ->
                new RuntimeFragmentLifecycle.ResizeResult(
                    "g", 1, 1, RuntimeFragmentLifecycle.ResizeStatus.APPLIED),
            MoreExecutors.directExecutor());
    ListenableFuture<?> published = f.owner.observeResizeResult("g", 1, nativeReceipt);
    retired.set(null);
    assertNull(firstListenerFailure.get());
    published.get(5, TimeUnit.SECONDS);
    f.owner.getCompletedFuture().get(5, TimeUnit.SECONDS);
    RuntimeDopExecutionSnapshot frozen = f.owner.freezeControlEvidence();
    RuntimeDopExecutionSnapshot.GroupSnapshot group = frozen.getGroups().get("g");
    assertEquals(
        1L, group.outcomeCounts().get(RuntimeDopExecutionSnapshot.Outcome.APPLIED).longValue());
    assertEquals(1, group.events().size());
    assertEquals(1, group.events().get(0).observedWorkers());
    RuntimeFragmentLifecycle.ResizeResult stopped =
        (RuntimeFragmentLifecycle.ResizeResult) f.owner.resize("g", 3).get();
    assertEquals(RuntimeFragmentLifecycle.ResizeStatus.STOPPED, stopped.status());
  }

  @Test
  public void stopDuringReceiptSubscriptionAlreadySeesTheRegisteredControlAction()
      throws Exception {
    Fixture f = new Fixture();
    f.owner.registerOriginalDriver(f.factory, f.driver);
    f.owner.registerControlGroup("g", 4, 2);
    f.owner.sealConstruction();
    SettableFuture<RuntimeFragmentLifecycle.ResizeResult> nativeReceipt = SettableFuture.create();
    ListenableFuture<RuntimeFragmentLifecycle.ResizeResult> stopOnSubscription =
        new com.google.common.util.concurrent.ForwardingListenableFuture
                .SimpleForwardingListenableFuture<
            RuntimeFragmentLifecycle.ResizeResult>(nativeReceipt) {
          @Override
          public void addListener(Runnable listener, java.util.concurrent.Executor executor) {
            f.owner.requestStop();
            f.quiesced.set(null);
            f.closed.set(null);
            assertFalse(f.owner.getCompletedFuture().isDone());
            verify(f.context, never()).setRuntimeControlEvidence(any());
            super.addListener(listener, executor);
          }
        };
    ListenableFuture<?> published = f.owner.observeResizeResult("g", 3, stopOnSubscription);
    assertFalse(published.isDone());
    nativeReceipt.set(
        new RuntimeFragmentLifecycle.ResizeResult(
            "g", 3, 3, RuntimeFragmentLifecycle.ResizeStatus.APPLIED));
    published.get(5, TimeUnit.SECONDS);
    f.owner.getCompletedFuture().get(5, TimeUnit.SECONDS);
    assertEquals(
        1L,
        f.owner
            .freezeControlEvidence()
            .getGroups()
            .get("g")
            .outcomeCounts()
            .get(RuntimeDopExecutionSnapshot.Outcome.APPLIED)
            .longValue());
  }

  @Test
  public void failedControlReceiptIsRecordedBeforeTerminalFreezeAndCallerNotification()
      throws Exception {
    Fixture f = new Fixture();
    f.owner.registerControlGroup("g", 4, 2);
    f.owner.sealConstruction();
    SettableFuture<RuntimeFragmentLifecycle.ResizeResult> nativeReceipt = SettableFuture.create();
    ListenableFuture<?> published = f.owner.observeResizeResult("g", 4, nativeReceipt);
    f.owner.requestStop();
    assertFalse(f.owner.getCompletedFuture().isDone());
    IllegalStateException expected = new IllegalStateException();
    nativeReceipt.setException(expected);
    assertSame(expected, failure(published));
    f.owner.getCompletedFuture().get(5, TimeUnit.SECONDS);
    RuntimeDopExecutionSnapshot.GroupSnapshot group =
        f.owner.freezeControlEvidence().getGroups().get("g");
    assertEquals(
        1L, group.outcomeCounts().get(RuntimeDopExecutionSnapshot.Outcome.STOPPED).longValue());
    assertEquals(
        0L, group.outcomeCounts().get(RuntimeDopExecutionSnapshot.Outcome.APPLIED).longValue());
  }

  @Test
  public void publishedReceiptMayReenterFreezeWithoutEvidenceLockOrUnsettledAction()
      throws Exception {
    Fixture f = new Fixture();
    f.owner.registerControlGroup("g", 4, 2);
    SettableFuture<RuntimeFragmentLifecycle.ResizeResult> nativeReceipt = SettableFuture.create();
    ListenableFuture<?> published = f.owner.observeResizeResult("g", 4, nativeReceipt);
    java.util.concurrent.atomic.AtomicReference<Throwable> listenerFailure =
        new java.util.concurrent.atomic.AtomicReference<>();
    published.addListener(
        () -> {
          try {
            assertFalse(Thread.holdsLock(field(f.owner, "evidenceLock")));
            RuntimeDopExecutionSnapshot.GroupSnapshot group =
                f.owner.freezeControlEvidence().getGroups().get("g");
            assertEquals(
                1L,
                group.outcomeCounts().get(RuntimeDopExecutionSnapshot.Outcome.APPLIED).longValue());
          } catch (Throwable failure) {
            listenerFailure.set(failure);
          }
        },
        MoreExecutors.directExecutor());
    nativeReceipt.set(
        new RuntimeFragmentLifecycle.ResizeResult(
            "g", 4, 4, RuntimeFragmentLifecycle.ResizeStatus.APPLIED));
    published.get(5, TimeUnit.SECONDS);
    assertNull(listenerFailure.get());
  }

  @Test
  public void boundedEventsKeepCumulativeActualOutcomeCounts() {
    Fixture f = new Fixture();
    f.owner.registerControlGroup("g", 4, 2);
    for (int action = 0; action < 200; action++) {
      int observed = 1 + action % 3;
      f.owner.observeResizeResult(
          "g",
          observed,
          Futures.immediateFuture(
              new RuntimeFragmentLifecycle.ResizeResult(
                  "g", observed, observed, RuntimeFragmentLifecycle.ResizeStatus.APPLIED)));
    }
    RuntimeDopExecutionSnapshot.GroupSnapshot group =
        f.owner.freezeControlEvidence().getGroups().get("g");
    assertEquals(RuntimeDopExecutionSnapshot.EVENT_LIMIT, group.events().size());
    assertEquals(
        200L, group.outcomeCounts().get(RuntimeDopExecutionSnapshot.Outcome.APPLIED).longValue());
    assertEquals(1, group.minAppliedWorkers());
    assertEquals(3, group.maxAppliedWorkers());
    assertEquals(73L, group.events().get(0).sequence());
    assertEquals(200L, group.events().get(group.events().size() - 1).sequence());
  }

  @Test
  public void realRegistryEndpointFailureWithholdsPhysicalReleaseAndRetainsOwners()
      throws Exception {
    Fixture f = new Fixture();
    RegistryFixture endpoints = new RegistryFixture(f.context);
    IllegalStateException expected = new IllegalStateException();
    doThrow(expected).when(endpoints.listener).onFinished(endpoints.source);
    LocalExecutionPlanContext planning = mock(LocalExecutionPlanContext.class);
    when(planning.getPipelineDriverFactories()).thenReturn(Collections.emptyList());
    when(planning.getResourceRegistry()).thenReturn(endpoints.registry);
    when(planning.getOwnedRuntimeFrontiers()).thenReturn(Collections.emptyList());
    when(f.context.getPlanningContext()).thenReturn(planning);
    f.owner.requestStop();
    f.owner.sealConstruction();
    assertSame(expected, failure(f.owner.getCompletedFuture()));
    assertTrue(endpoints.source.isClosed());
    assertTrue(endpoints.queue.isClosed());
    verify(f.context, never()).releaseResourceWhenAllDriversAreClosed();
    verify(f.context, never()).clearRuntimePlanBindings();
    assertSame(f.manager, field(f.owner, "exchangeManager"));
  }

  @Test
  public void controllerDiagnosticsRequireAcknowledgedSamplingStop() throws Exception {
    Fixture f = new Fixture();
    RuntimeDopController.Target target = mock(RuntimeDopController.Target.class);
    RuntimeDopController controller = new RuntimeDopController(target);
    setField(f.owner, "controller", controller);
    try {
      assertThrows(IllegalStateException.class, f.owner::freezeControlEvidence);
      assertNull(field(f.owner, "frozenControlEvidence"));
      controller.close();
      controller.getStoppedFuture().get(5, TimeUnit.SECONDS);
      RuntimeDopExecutionSnapshot frozen = f.owner.freezeControlEvidence();
      java.util.Map<?, ?> diagnostics =
          (java.util.Map<?, ?>) frozen.toMap().get("controllerDiagnostics");
      assertEquals("CREATED_BUT_NO_SAMPLE", diagnostics.get("status"));
      assertEquals(0, diagnostics.get("retainedCount"));
      verifyZeroInteractions(target);
    } finally {
      controller.close();
    }
  }

  @Test
  public void realControllerKeepReasonsAndCoverageSurvivePhysicalOwnerDetach() throws Exception {
    Fixture f = new Fixture();
    RuntimeDopController.Target target = mock(RuntimeDopController.Target.class);
    when(target.sampleControlInputs(anyLong(), anyLong()))
        .thenAnswer(
            call -> {
              long now = call.getArgument(0);
              long sequence = call.getArgument(1);
              return java.util.Map.of(
                  "g",
                  new RuntimeDopPolicy.Input(
                      "g",
                      now,
                      now,
                      sequence,
                      4,
                      4,
                      1,
                      1,
                      3,
                      0,
                      4,
                      RuntimeDopPolicy.MetricValue.unsupported(),
                      RuntimeDopPolicy.MetricValue.unknown(),
                      false,
                      false));
            });
    java.util.concurrent.atomic.AtomicLong clock = new java.util.concurrent.atomic.AtomicLong(1000);
    RuntimeDopController controller =
        new RuntimeDopController(
            target,
            new RuntimeDopPolicy(RuntimeDopPolicy.Config.conservativeDefaults()),
            clock::get);
    setField(f.owner, "controller", controller);
    try {
      controller.sampleOnce();
      clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(200));
      controller.sampleOnce();
      assertEquals(
          RuntimeDopPolicy.Reason.COMPLETED_WORK_UNSUPPORTED,
          controller.snapshotEvents().get(1).reason());
      f.owner.registerOriginalDriver(f.factory, f.driver);
      f.owner.registerControlGroup("g", 4, 1);
      f.quiesced.set(null);
      f.closed.set(null);
      f.owner.requestStop();
      f.owner.sealConstruction();
      f.owner.getCompletedFuture().get(5, TimeUnit.SECONDS);
      assertNull(field(f.owner, "controller"));
      RuntimeDopExecutionSnapshot frozen = f.owner.freezeControlEvidence();
      java.util.Map<?, ?> diagnostics =
          (java.util.Map<?, ?>) frozen.toMap().get("controllerDiagnostics");
      java.util.List<?> events = (java.util.List<?>) diagnostics.get("events");
      assertEquals(2, events.size());
      java.util.Map<?, ?> event = (java.util.Map<?, ?>) events.get(1);
      assertEquals("KEEP", event.get("action"));
      assertEquals("COMPLETED_WORK_UNSUPPORTED", event.get("reason"));
      java.util.Map<?, ?> completed = (java.util.Map<?, ?>) event.get("completedWork");
      java.util.Map<?, ?> total = (java.util.Map<?, ?>) event.get("totalWork");
      assertEquals("UNSUPPORTED", completed.get("coverage"));
      assertEquals("UNKNOWN", total.get("coverage"));
      assertFalse(completed.containsKey("value"));
      assertFalse(total.containsKey("value"));
      assertEquals(
          0L,
          frozen
              .getGroups()
              .get("g")
              .outcomeCounts()
              .get(RuntimeDopExecutionSnapshot.Outcome.APPLIED)
              .longValue());
      assertSame(frozen, f.owner.freezeControlEvidence());
      java.util.Map<String, Object> live = f.owner.snapshotLiveControlEvidence();
      assertEquals(true, live.get("frozen"));
      RuntimeDopController.TraceSnapshot trace =
          (RuntimeDopController.TraceSnapshot) live.get("controller");
      assertEquals(2, trace.totalEvents());
      assertTrue(trace.samplingStopped());
      assertNotNull(trace.events().get(1).input());
      assertNull(field(f.owner, "controller"));
      verify(target, never()).resize(anyString(), anyInt());
      verify(target, never()).fail(any());
    } finally {
      controller.close();
    }
  }

  @Test
  public void liveSchedulerPressureWorksWithoutAnAdaptiveController() throws Exception {
    Fixture f = new Fixture();
    org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler scheduler =
        mock(org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler.class);
    var own =
        new org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler.RunnableTaskCounts(
            1, 0, 0);
    var other =
        new org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler.RunnableTaskCounts(
            2, 1, 0);
    var sample =
        new org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler.RuntimePressure(
            10, 20, 8, true, true, 4, own, other);
    when(scheduler.observeRuntimePressure(f.context.getId().getQueryId()))
        .thenReturn(java.util.Optional.of(sample));
    setField(f.owner, "scheduler", scheduler);
    assertFalse(f.owner.snapshotLiveControlEvidence().containsKey("schedulerPressure"));
    f.owner.registerControlGroup("fixed", 1, 1);
    java.util.Map<String, Object> live = f.owner.snapshotLiveControlEvidence();
    assertNull(live.get("controller"));
    assertSame(sample, live.get("schedulerPressure"));
    f.owner.freezeControlEvidence();
    assertFalse(f.owner.snapshotLiveControlEvidence().containsKey("schedulerPressure"));
    verify(scheduler).observeRuntimePressure(f.context.getId().getQueryId());
  }

  @Test
  public void cleanupWaitsForInFlightControllerSampleBeforeFreezingDiagnostics() throws Exception {
    Fixture f = new Fixture();
    java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
    RuntimeDopController.Target target = mock(RuntimeDopController.Target.class);
    when(target.sampleControlInputs(anyLong(), anyLong()))
        .thenAnswer(
            call -> {
              entered.countDown();
              assertTrue(release.await(5, TimeUnit.SECONDS));
              return java.util.Map.of();
            });
    RuntimeDopController controller = new RuntimeDopController(target);
    setField(f.owner, "controller", controller);
    java.util.concurrent.ExecutorService executor =
        java.util.concurrent.Executors.newSingleThreadExecutor();
    try {
      java.util.concurrent.Future<?> sample = executor.submit(controller::sampleOnce);
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      java.util.Map<String, Object> live = f.owner.snapshotLiveControlEvidence();
      assertEquals(false, live.get("frozen"));
      assertFalse(((RuntimeDopController.TraceSnapshot) live.get("controller")).samplingStopped());
      assertFalse(controller.getStoppedFuture().isDone());
      f.owner.registerOriginalDriver(f.factory, f.driver);
      f.quiesced.set(null);
      f.closed.set(null);
      f.owner.requestStop();
      f.owner.sealConstruction();
      assertFalse(f.owner.getCompletedFuture().isDone());
      assertNull(field(f.owner, "frozenControlEvidence"));
      release.countDown();
      sample.get(5, TimeUnit.SECONDS);
      f.owner.getCompletedFuture().get(5, TimeUnit.SECONDS);
      RuntimeDopExecutionSnapshot frozen = f.owner.freezeControlEvidence();
      java.util.Map<?, ?> diagnostics =
          (java.util.Map<?, ?>) frozen.toMap().get("controllerDiagnostics");
      assertEquals("CREATED_BUT_NO_SAMPLE", diagnostics.get("status"));
      assertEquals(0, diagnostics.get("retainedCount"));
      assertNull(field(f.owner, "controller"));
      verify(target, never()).resize(anyString(), anyInt());
    } finally {
      release.countDown();
      controller.close();
      executor.shutdownNow();
    }
  }

  @Test
  public void liveReadDoesNotSealPendingOrFutureMembershipSettlements() throws Exception {
    Fixture f = new Fixture();
    f.owner.registerControlGroup("g", 4, 2);
    SettableFuture<RuntimeFragmentLifecycle.ResizeResult> pending = SettableFuture.create();
    f.owner.observeResizeResult("g", 3, pending);
    java.util.Map<String, Object> first = f.owner.snapshotLiveControlEvidence();
    assertEquals(false, first.get("frozen"));
    assertEquals(false, field(f.owner, "controlSettlementsSealed"));
    assertNull(field(f.owner, "frozenControlEvidence"));
    pending.set(
        new RuntimeFragmentLifecycle.ResizeResult(
            "g", 3, 3, RuntimeFragmentLifecycle.ResizeStatus.APPLIED));
    f.owner.observeResizeResult(
        "g",
        2,
        Futures.immediateFuture(
            new RuntimeFragmentLifecycle.ResizeResult(
                "g", 2, 2, RuntimeFragmentLifecycle.ResizeStatus.APPLIED)));
    java.util.Map<?, ?> groups =
        (java.util.Map<?, ?>) f.owner.snapshotLiveControlEvidence().get("groups");
    RuntimeDopExecutionSnapshot.GroupSnapshot group =
        (RuntimeDopExecutionSnapshot.GroupSnapshot) groups.get("g");
    assertEquals(2, group.events().size());
    assertEquals(2, group.lastObservedWorkers());
    assertNotNull(group.events().get(0).observedAtNanos());
    com.google.gson.JsonObject json =
        com.google.gson.JsonParser.parseString(
                new com.google.gson.GsonBuilder()
                    .serializeNulls()
                    .create()
                    .toJson(f.owner.snapshotLiveControlEvidence()))
            .getAsJsonObject();
    assertEquals("NO_CONTROLLER", json.get("status").getAsString());
    assertTrue(json.get("controller").isJsonNull());
    com.google.gson.JsonArray events =
        json.getAsJsonObject("groups").getAsJsonObject("g").getAsJsonArray("events");
    assertEquals(2, events.size());
    assertFalse(events.get(0).getAsJsonObject().get("observedAtNanos").isJsonNull());
    assertEquals("APPLIED", events.get(1).getAsJsonObject().get("outcome").getAsString());
    assertTrue(group.events().get(1).observedAtNanos() >= group.events().get(0).observedAtNanos());
    assertThrows(UnsupportedOperationException.class, () -> first.clear());
    assertEquals(false, field(f.owner, "controlSettlementsSealed"));
  }

  private static Object field(Object owner, String name) throws Exception {
    java.lang.reflect.Field field = owner.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(owner);
  }

  private static void setField(Object owner, String name, Object value) throws Exception {
    java.lang.reflect.Field field = owner.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(owner, value);
  }

  private static final class RegistryFixture {
    private final LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
    private final org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue queue;
    private final org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle
        source;
    private final MPPDataExchangeManager.SourceHandleListener listener =
        mock(MPPDataExchangeManager.SourceHandleListener.class);

    private RegistryFixture(FragmentInstanceContext context) {
      queue =
          new org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue(
              context.getId().toThrift(),
              "registry_endpoint",
              new org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager(),
              MoreExecutors.newDirectExecutorService());
      source =
          new org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle(
              context.getId().toThrift(), "registry_endpoint", queue, listener);
      registry.registerSourceHandle(source);
    }
  }

  private static Throwable failure(ListenableFuture<?> future) throws Exception {
    try {
      future.get(5, TimeUnit.SECONDS);
      throw new AssertionError("expected failed cleanup receipt");
    } catch (ExecutionException expected) {
      return expected.getCause();
    }
  }

  private static final class Fixture {
    private final FragmentInstanceContext context = mock(FragmentInstanceContext.class);
    private final MPPDataExchangeManager manager = mock(MPPDataExchangeManager.class);
    private final PipelineDriverFactory factory = mock(PipelineDriverFactory.class);
    private final IDriver driver = mock(IDriver.class);
    private final SettableFuture<Void> quiesced = SettableFuture.create();
    private final SettableFuture<Void> closed = SettableFuture.create();
    private final RuntimeFragmentLifecycle owner;

    private Fixture() {
      FragmentInstanceId id =
          new FragmentInstanceId(
              new PlanFragmentId(new QueryId("runtime_lifecycle"), 0), "instance");
      when(context.getId()).thenReturn(id);
      org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine machine =
          mock(
              org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine
                  .class);
      when(context.getStateMachine()).thenReturn(machine);
      when(machine.getState())
          .thenReturn(
              org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceState.FINISHED);
      when(context.getFailureCause()).thenReturn(Optional.empty());
      when(driver.supportsQuiescence()).thenReturn(true);
      when(driver.supportsCoordinatedClose()).thenReturn(true);
      when(driver.requestQuiescence()).thenReturn(quiesced);
      when(driver.requestClose()).thenReturn(closed);
      owner = new RuntimeFragmentLifecycle(context, MoreExecutors.directExecutor());
      owner.bindExchangeManager(manager);
    }
  }
}
