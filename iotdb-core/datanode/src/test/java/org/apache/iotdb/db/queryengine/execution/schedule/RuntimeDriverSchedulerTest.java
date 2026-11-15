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

package org.apache.iotdb.db.queryengine.execution.schedule;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;
import org.apache.iotdb.common.rpc.thrift.TThrottleQuota;
import org.apache.iotdb.commons.exception.QueryTimeoutException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.exception.CpuNotEnoughException;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.exchange.IMPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeScanGroup;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeScanWorker;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTask;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskId;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskStatus;
import org.apache.iotdb.db.storageengine.rescon.quotas.ThrottleQuotaLimit;
import org.junit.Test;

public class RuntimeDriverSchedulerTest {
  @Test
  public void firstUserBatchChargesEveryPendingMemberAndOneBaseLease() throws Exception {
    Fixture f = new Fixture(4, 256);
    FragmentInstanceContext context = f.context("first");
    RuntimeSchedulingGroup group = f.group(context);
    PreparedDriverBatch batch =
        f.scheduler.prepareRuntimeDrivers(
            group, List.of(f.sub(f.driver(context, 0)), f.sub(f.driver(context, 1))), 128);
    assertEquals(2, f.quota.getReservedCpu("alice"));
    assertEquals(128, f.quota.getReservedMemory("alice"));
    assertEquals(2, group.getActiveRegistrations().size());
    assertEquals(DriverTaskStatus.PENDING, batch.members.get(0).task.getStatus());
    assertEquals(0, f.scheduler.getReadyQueueTaskCount());
    f.scheduler.stopRuntimeGroup(group, null).get(5, TimeUnit.SECONDS);
    assertEquals(0, f.quota.getReservedCpu("alice"));
    assertEquals(128, f.quota.getReservedMemory("alice"));
    f.releaseMemory();
    assertEquals(0, f.quota.getReservedMemory("alice"));
  }

  @Test
  public void legacyAndRuntimeUseOneInjectedUserLedgerInBothDirections() throws Exception {
    Fixture f = new Fixture(1, 256);
    FragmentInstanceContext legacy = f.context("legacy");
    f.scheduler.submitDriverBatch(
        new DriverBatchRequest(
            f.query, List.of(f.sub(f.driver(legacy, 0))), f.deadline, f.session));
    RuntimeSchedulingGroup runtime = f.group(f.context("runtime"));
    assertThrows(
        CpuNotEnoughException.class,
        () ->
            f.scheduler.prepareRuntimeDrivers(
                runtime, List.of(f.sub(f.driver(runtime.context, 0))), 0));
    assertEquals(1, f.quota.getReservedCpu("alice"));
    f.scheduler.abortQuery(f.query);
    assertEquals(0, f.quota.getReservedCpu("alice"));
    RuntimeSchedulingGroup next = f.group(f.context("next"));
    f.scheduler.prepareRuntimeDrivers(next, List.of(f.sub(f.driver(next.context, 0))), 0);
    assertThrows(
        CpuNotEnoughException.class,
        () ->
            f.scheduler.submitDriverBatch(
                new DriverBatchRequest(
                    f.query,
                    List.of(f.sub(f.driver(f.context("later-legacy"), 0))),
                    f.deadline,
                    f.session)));
    assertEquals(1, f.quota.getReservedCpu("alice"));
    f.scheduler.stopRuntimeGroup(next, null).get(5, TimeUnit.SECONDS);
  }

  @Test
  public void rejectedWholeBatchDoesNotLeakMappingsOrQuota() {
    Fixture f = new Fixture(1, 64);
    RuntimeSchedulingGroup group = f.group(f.context("reject"));
    assertThrows(
        CpuNotEnoughException.class,
        () ->
            f.scheduler.prepareRuntimeDrivers(
                group,
                List.of(f.sub(f.driver(group.context, 0)), f.sub(f.driver(group.context, 1))),
                64));
    assertEquals(0, f.quota.getReservedCpu("alice"));
    assertEquals(0, f.quota.getReservedMemory("alice"));
    assertEquals(0, f.scheduler.getQueryMapSize());
    assertEquals(0, f.scheduler.getReadyQueueTaskCount());
    assertEquals(0, f.scheduler.getReadyQueueReservedTaskCount());
    assertTrue(group.getStoppedAndRemovedFuture().isDone());
  }

  @Test
  public void allDependencyPendingTasksAreRegisteredAndEventuallyWake() throws Exception {
    Fixture f = new Fixture(2, 256);
    RuntimeSchedulingGroup group = f.group(f.context("dependencies"));
    SettableFuture<Void> dependency = SettableFuture.create();
    PreparedDriverBatch batch =
        f.scheduler.prepareRuntimeDrivers(
            group,
            List.of(
                new DriverSubmission(
                    f.driver(group.context, 0),
                    dependency,
                    DriverSubmission.Role.DIRECT_NATIVE,
                    0)),
            0);
    assertEquals(1, group.getActiveRegistrations().size());
    assertEquals(1, f.quota.getReservedCpu("alice"));
    f.scheduler.activateRuntimeBatch(batch);
    DriverTask task = f.scheduler.getReadyQueue().poll();
    assertFalse(f.scheduler.getScheduler().readyToRunning(task));
    assertEquals(DriverTaskStatus.BLOCKED, task.getStatus());
    assertEquals(1, f.scheduler.getReadyQueueReservedTaskCount());
    dependency.set(null);
    assertEquals(1, f.scheduler.getReadyQueueTaskCount());
    assertEquals(0, f.scheduler.getReadyQueueReservedTaskCount());
    f.scheduler.stopRuntimeGroup(group, null).get(5, TimeUnit.SECONDS);
  }

  @Test
  public void immediatelyCompletedOriginalDependencyIsNotLost() throws Exception {
    Fixture f = new Fixture(2, 256);
    RuntimeSchedulingGroup group = f.group(f.context("immediate"));
    PreparedDriverBatch batch =
        f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(f.driver(group.context, 0))), 0);
    f.scheduler.activateRuntimeBatch(batch);
    DriverTask task = f.scheduler.getReadyQueue().poll();
    assertTrue(f.scheduler.getScheduler().readyToRunning(task));
    f.scheduler.stopRuntimeGroup(group, null).get(5, TimeUnit.SECONDS);
  }

  @Test
  public void failedAndCancelledDependenciesNeverActivateWork() throws Exception {
    for (boolean cancel : List.of(false, true)) {
      Fixture f = new Fixture(2, 256);
      RuntimeSchedulingGroup group = f.group(f.context("bad-dependency"));
      SettableFuture<Void> dependency = SettableFuture.create();
      PreparedDriverBatch batch =
          f.scheduler.prepareRuntimeDrivers(
              group,
              List.of(
                  new DriverSubmission(
                      f.driver(group.context, 0),
                      dependency,
                      DriverSubmission.Role.DIRECT_NATIVE,
                      0)),
              0);
      if (cancel) dependency.cancel(false);
      else dependency.setException(new IllegalStateException());
      assertThrows(IllegalStateException.class, () -> f.scheduler.activateRuntimeBatch(batch));
      assertEquals(0, f.scheduler.getReadyQueueTaskCount());
      assertEquals(0, f.quota.getReservedCpu("alice"));
      assertThrows(ExecutionException.class, () -> batch.members.get(0).getFailureFuture().get());
    }
  }

  @Test
  public void incrementalTasksKeepAbsoluteDeadlineAndOriginalPriorityHandle() throws Exception {
    Fixture f = new Fixture(3, 256);
    RuntimeSchedulingGroup group = f.group(f.context("same-handle"));
    PreparedDriverBatch first =
        f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(f.driver(group.context, 0))), 0);
    ExecutionContext execution = new ExecutionContext();
    execution.setScheduledTimeInNanos(1234567);
    first.members.get(0).task.updateSchedulePriority(execution);
    PreparedDriverBatch second =
        f.scheduler
            .prepareRuntimeGrowth(group, List.of(f.workerSub(group.context, 1, 0, () -> {})))
            .getAdmittedBatch()
            .orElseThrow();
    assertEquals(f.deadline, first.members.get(0).task.getDDL());
    assertEquals(f.deadline, second.members.get(0).task.getDDL());
    assertSame(
        first.members.get(0).task.getDriverTaskHandle(),
        second.members.get(0).task.getDriverTaskHandle());
    assertSame(
        first.members.get(0).task.getDriverTaskHandle().getPriority(),
        second.members.get(0).task.getDriverTaskHandle().getPriority());
    assertEquals(1, f.scheduler.getTimeoutQueueTaskCount());
    f.scheduler.stopRuntimeGroup(group, null).get(5, TimeUnit.SECONDS);
  }

  @Test
  public void partialQueuePublicationCannotExecuteFirstMember() throws Exception {
    Fixture f = new Fixture(4, 256, 1);
    RuntimeSchedulingGroup group = f.group(f.context("publication"));
    IDriver first = f.driver(group.context, 0);
    PreparedDriverBatch batch =
        f.scheduler.prepareRuntimeDrivers(
            group, List.of(f.sub(first), f.sub(f.driver(group.context, 1))), 0);
    org.apache.iotdb.calc.execution.schedule.queue.IndexedBlockingReserveQueue<DriverTask> real =
        (org.apache.iotdb.calc.execution.schedule.queue.IndexedBlockingReserveQueue<DriverTask>)
            f.scheduler.getReadyQueue();
    org.apache.iotdb.calc.execution.schedule.queue.IndexedBlockingReserveQueue<DriverTask> queue =
        spy(real);
    AtomicInteger pushes = new AtomicInteger();
    doAnswer(
            call -> {
              call.callRealMethod();
              if (pushes.incrementAndGet() == 1) {
                DriverTask polled = queue.poll();
                assertFalse(f.scheduler.getScheduler().readyToRunning(polled));
              }
              return null;
            })
        .when(queue)
        .push(any());
    Field field = DriverScheduler.class.getDeclaredField("readyQueue");
    field.setAccessible(true);
    field.set(f.scheduler, queue);
    assertThrows(IllegalStateException.class, () -> f.scheduler.activateRuntimeBatch(batch));
    verify(first, never()).processFor(any());
    assertEquals(0, f.scheduler.getReadyQueueTaskCount());
    assertEquals(0, f.scheduler.getReadyQueueReservedTaskCount());
    assertEquals(0, f.quota.getReservedCpu("alice"));
    assertEquals(0, f.scheduler.getQueryMapSize());
    f.scheduler.stopRuntimeGroup(group, null).get(5, TimeUnit.SECONDS);
  }

  @Test
  public void stopBeforeActivationAndLateDependencyCannotResurrectMappings() throws Exception {
    Fixture f = new Fixture(2, 256);
    RuntimeSchedulingGroup group = f.group(f.context("stop-before-activation"));
    SettableFuture<Void> dependency = SettableFuture.create();
    PreparedDriverBatch batch =
        f.scheduler.prepareRuntimeDrivers(
            group,
            List.of(
                new DriverSubmission(
                    f.driver(group.context, 0),
                    dependency,
                    DriverSubmission.Role.DIRECT_NATIVE,
                    0)),
            0);
    f.scheduler.stopRuntimeGroup(group, null).get(5, TimeUnit.SECONDS);
    assertThrows(IllegalStateException.class, () -> f.scheduler.activateRuntimeBatch(batch));
    dependency.set(null);
    assertEquals(0, f.scheduler.getQueryMapSize());
    assertEquals(0, f.scheduler.getReadyQueueTaskCount());
    assertEquals(0, f.quota.getReservedCpu("alice"));
    assertThrows(ExecutionException.class, () -> batch.getActivationFuture().get());
  }

  @Test
  public void normalWorkerRetirementIsFinishedAndNeverDeregistersFragment() throws Exception {
    Fixture f = new Fixture(2, 256);
    RuntimeSchedulingGroup group = f.group(f.context("worker"));
    IDriver logical = f.driver(group.context, 100);
    RuntimeScanWorker worker =
        new RuntimeScanWorker(
            new RuntimeScanGroup(List.of(logical)),
            f.driverContext(group.context, 0),
            0,
            0,
            () -> {});
    PreparedDriverBatch batch =
        f.scheduler.prepareRuntimeDrivers(
            group,
            List.of(
                new DriverSubmission(
                    worker, Futures.immediateVoidFuture(), DriverSubmission.Role.SCAN_WORKER, 0)),
            0);
    f.scheduler.activateRuntimeBatch(batch);
    f.scheduler.retireRuntimeWorker(group, batch.members.get(0)).get(5, TimeUnit.SECONDS);
    assertEquals(DriverTaskStatus.FINISHED, batch.members.get(0).task.getStatus());
    worker.getRetiredFuture().get(5, TimeUnit.SECONDS);
    worker.getSchedulerRemovalFuture().get(5, TimeUnit.SECONDS);
    verify(logical, never()).close();
    verify(logical, never()).failed(any());
    verify(f.exchange, never()).forceDeregisterFragmentInstance(any());
    assertEquals(0, f.quota.getReservedCpu("alice"));
    assertEquals(1, f.scheduler.getTimeoutQueueTaskCount());
    f.scheduler.stopRuntimeGroup(group, null).get(5, TimeUnit.SECONDS);
  }

  @Test
  public void runningAbortKeepsCpuAndRequiredStopUntilCessationSucceeds() throws Exception {
    Fixture f = new Fixture(2, 256);
    RuntimeSchedulingGroup group = f.group(f.context("running"));
    SettableFuture<Void> ceased = SettableFuture.create();
    IDriver driver = f.driver(group.context, 0, ceased);
    PreparedDriverBatch batch = f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(driver)), 0);
    f.scheduler.activateRuntimeBatch(batch);
    assertTrue(f.scheduler.getScheduler().readyToRunning(f.scheduler.getReadyQueue().poll()));
    ListenableFuture<Void> stop = f.scheduler.stopRuntimeGroup(group, new IllegalStateException());
    TaskRegistration member = batch.members.get(0);
    assertTrue(member.getRemovedFuture().isDone());
    assertFalse(stop.isDone());
    assertEquals(1, group.getActiveRegistrations().size());
    assertEquals(1, f.quota.getReservedCpu("alice"));
    assertEquals(0, f.scheduler.getQueryMapSize());
    ceased.set(null);
    stop.get(5, TimeUnit.SECONDS);
    member.getQuotaSettledFuture().get(5, TimeUnit.SECONDS);
    assertEquals(0, f.quota.getReservedCpu("alice"));
    f.scheduler.stopRuntimeGroup(group, null).get(5, TimeUnit.SECONDS);
  }

  @Test
  public void failedCessationWithholdsQuotaAndPreventsGenerationReuse() throws Exception {
    Fixture f = new Fixture(2, 256);
    RuntimeSchedulingGroup group = f.group(f.context("failed-proof"));
    SettableFuture<Void> ceased = SettableFuture.create();
    PreparedDriverBatch batch =
        f.scheduler.prepareRuntimeDrivers(
            group, List.of(f.sub(f.driver(group.context, 0, ceased))), 0);
    ListenableFuture<Void> stop = f.scheduler.stopRuntimeGroup(group, null);
    ceased.setException(new IllegalStateException());
    assertThrows(ExecutionException.class, stop::get);
    assertTrue(batch.members.get(0).getRemovedFuture().isDone());
    assertFalse(batch.members.get(0).getQuotaSettledFuture().isDone());
    assertEquals(1, f.quota.getReservedCpu("alice"));
    assertEquals(1, group.getActiveRegistrations().size());
    assertThrows(IllegalStateException.class, () -> f.group(group.context));
  }

  @Test
  public void nativeRootFinishDoesNotReleaseFiMemoryBeforePhysicalHook() throws Exception {
    Fixture f = new Fixture(2, 256);
    RuntimeSchedulingGroup group = f.group(f.context("root-limit"));
    PreparedDriverBatch batch =
        f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(f.driver(group.context, 0))), 128);
    f.scheduler.activateRuntimeBatch(batch);
    DriverTask task = f.scheduler.getReadyQueue().poll();
    assertTrue(f.scheduler.getScheduler().readyToRunning(task));
    ExecutionContext execution = new ExecutionContext();
    execution.setScheduledTimeInNanos(1);
    f.scheduler.getScheduler().runningToFinished(task, execution);
    assertEquals(0, f.quota.getReservedCpu("alice"));
    assertEquals(128, f.quota.getReservedMemory("alice"));
    assertEquals(1, f.scheduler.getTimeoutQueueTaskCount());
    f.scheduler.stopRuntimeGroup(group, null).get(5, TimeUnit.SECONDS);
    assertEquals(128, f.quota.getReservedMemory("alice"));
    f.releaseMemory();
    assertEquals(0, f.quota.getReservedMemory("alice"));
  }

  @Test
  public void unsupportedQuotaManagedDriverIsRejectedBeforeNativePublication() {
    Fixture f = new Fixture(2, 256);
    RuntimeSchedulingGroup group = f.group(f.context("unsupported"));
    IDriver unsupported = f.driver(group.context, 0);
    when(unsupported.supportsQuiescence()).thenReturn(false);
    assertThrows(
        IllegalArgumentException.class,
        () -> f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(unsupported)), 0));
    assertEquals(0, f.quota.getReservedCpu("alice"));
    assertEquals(0, f.scheduler.getQueryMapSize());
    verify(unsupported, never()).requestQuiescence();
  }

  @Test
  public void realDeadlineSentinelPreservesTimeoutAcrossEveryFragmentGroup() throws Exception {
    Fixture f = new Fixture(4, 256);
    FragmentInstanceContext firstContext = f.context("deadline-first");
    FragmentInstanceContext secondContext = f.context("deadline-second");
    IDriver first = f.driver(firstContext, 0);
    IDriver second = f.driver(secondContext, 0);
    List<Throwable> firstFailures = new ArrayList<>();
    List<Throwable> secondFailures = new ArrayList<>();
    long deadline = System.currentTimeMillis() + 1000;
    RuntimeSchedulingGroup firstGroup =
        f.scheduler.openRuntimeGroup(
            firstContext, deadline, f.session, () -> {}, firstFailures::add);
    RuntimeSchedulingGroup secondGroup =
        f.scheduler.openRuntimeGroup(
            secondContext, deadline, f.session, () -> {}, secondFailures::add);
    PreparedDriverBatch firstBatch =
        f.scheduler.prepareRuntimeDrivers(firstGroup, List.of(f.sub(first)), 0);
    PreparedDriverBatch secondBatch =
        f.scheduler.prepareRuntimeDrivers(secondGroup, List.of(f.sub(second)), 0);
    f.scheduler.activateRuntimeBatch(firstBatch);
    f.scheduler.activateRuntimeBatch(secondBatch);
    DriverTask anchor = f.scheduler.getTimeoutQueue().poll();
    new DriverTaskTimeoutSentinelThread(
            "offline-deadline-sentinel",
            new ThreadGroup("offline-deadline-test"),
            f.scheduler.getTimeoutQueue(),
            f.scheduler.getScheduler(),
            null)
        .execute(anchor);
    assertTrue(System.currentTimeMillis() >= deadline);
    Throwable timeout = anchor.getAbortCause().orElseThrow();
    assertTrue(timeout instanceof QueryTimeoutException);
    assertEquals(
        org.apache.iotdb.rpc.TSStatusCode.QUERY_TIMEOUT.getStatusCode(),
        ((QueryTimeoutException) timeout).getErrorCode());
    firstGroup.getStoppedAndRemovedFuture().get(5, TimeUnit.SECONDS);
    secondGroup.getStoppedAndRemovedFuture().get(5, TimeUnit.SECONDS);
    assertEquals(List.of(timeout), firstFailures);
    assertEquals(List.of(timeout), secondFailures);
    assertSame(timeout, firstBatch.members.get(0).task.getAbortCause().orElseThrow());
    assertSame(timeout, secondBatch.members.get(0).task.getAbortCause().orElseThrow());
    verify(first).failed(same(timeout));
    verify(second).failed(same(timeout));
    assertEquals(0, f.quota.getReservedCpu("alice"));
    assertEquals(0, f.scheduler.getQueryMapSize());
    assertEquals(0, f.scheduler.getReadyQueueTaskCount());
    assertEquals(0, f.scheduler.getTimeoutQueueTaskCount());
  }

  @Test
  public void replacedDeadlineAnchorCannotAbortCurrentQueryGroups() throws Exception {
    Fixture f = new Fixture(4, 256);
    List<Throwable> failures = new ArrayList<>();
    FragmentInstanceContext firstContext = f.context("later-deadline");
    RuntimeSchedulingGroup first =
        f.scheduler.openRuntimeGroup(firstContext, f.deadline, f.session, () -> {}, failures::add);
    PreparedDriverBatch firstBatch =
        f.scheduler.prepareRuntimeDrivers(first, List.of(f.sub(f.driver(firstContext, 0))), 0);
    DriverTask oldAnchor = f.scheduler.getTimeoutQueue().poll();
    FragmentInstanceContext secondContext = f.context("earlier-deadline");
    RuntimeSchedulingGroup second =
        f.scheduler.openRuntimeGroup(
            secondContext, f.deadline - 10000, f.session, () -> {}, failures::add);
    PreparedDriverBatch secondBatch =
        f.scheduler.prepareRuntimeDrivers(second, List.of(f.sub(f.driver(secondContext, 0))), 0);
    DriverTask currentAnchor = f.scheduler.getTimeoutQueue().poll();
    QueryTimeoutException timeout = new QueryTimeoutException();
    oldAnchor.setAbortCause(timeout);
    f.scheduler.getScheduler().toAborted(oldAnchor);
    assertTrue(failures.isEmpty());
    assertFalse(first.getStoppedAndRemovedFuture().isDone());
    assertFalse(second.getStoppedAndRemovedFuture().isDone());
    assertEquals(2, f.quota.getReservedCpu("alice"));
    currentAnchor.setAbortCause(timeout);
    f.scheduler.getScheduler().toAborted(currentAnchor);
    first.getStoppedAndRemovedFuture().get(5, TimeUnit.SECONDS);
    second.getStoppedAndRemovedFuture().get(5, TimeUnit.SECONDS);
    assertEquals(List.of(timeout, timeout), failures);
    assertSame(timeout, firstBatch.members.get(0).task.getAbortCause().orElseThrow());
    assertSame(timeout, secondBatch.members.get(0).task.getAbortCause().orElseThrow());
    assertEquals(0, f.quota.getReservedCpu("alice"));
  }

  @Test
  public void sourceTaskSqlFailurePreservesOriginalCauseAcrossEveryFragmentGroup()
      throws Exception {
    Fixture f = new Fixture(4, 256);
    FragmentInstanceContext rootContext = f.context("sql-failure-root");
    FragmentInstanceContext sourceContext = f.context("sql-failure-source");
    IDriver rootDriver = f.driver(rootContext, 0);
    IDriver sourceDriver = f.driver(sourceContext, 0);
    List<Throwable> rootFailures = new ArrayList<>();
    List<Throwable> sourceFailures = new ArrayList<>();
    RuntimeSchedulingGroup root =
        f.scheduler.openRuntimeGroup(
            rootContext, f.deadline, f.session, () -> {}, rootFailures::add);
    RuntimeSchedulingGroup source =
        f.scheduler.openRuntimeGroup(
            sourceContext, f.deadline, f.session, () -> {}, sourceFailures::add);
    PreparedDriverBatch rootBatch =
        f.scheduler.prepareRuntimeDrivers(root, List.of(f.sub(rootDriver)), 0);
    PreparedDriverBatch sourceBatch =
        f.scheduler.prepareRuntimeDrivers(source, List.of(f.sub(sourceDriver)), 0);
    f.scheduler.activateRuntimeBatch(rootBatch);
    f.scheduler.activateRuntimeBatch(sourceBatch);
    ClassCastException nativeCast = new ClassCastException("native table CAST failure");
    SemanticException sqlFailure = new SemanticException(nativeCast);
    sourceBatch.members.get(0).task.setAbortCause(sqlFailure);
    f.scheduler.getScheduler().toAborted(sourceBatch.members.get(0).task);
    root.getStoppedAndRemovedFuture().get(5, TimeUnit.SECONDS);
    source.getStoppedAndRemovedFuture().get(5, TimeUnit.SECONDS);
    assertEquals(
        org.apache.iotdb.rpc.TSStatusCode.SEMANTIC_ERROR.getStatusCode(),
        sqlFailure.getErrorCode());
    assertSame(nativeCast, sqlFailure.getCause());
    assertEquals(List.of(sqlFailure), rootFailures);
    assertEquals(List.of(sqlFailure), sourceFailures);
    assertSame(sqlFailure, rootBatch.members.get(0).task.getAbortCause().orElseThrow());
    assertSame(sqlFailure, sourceBatch.members.get(0).task.getAbortCause().orElseThrow());
    verify(rootDriver).failed(same(sqlFailure));
    verify(sourceDriver).failed(same(sqlFailure));
    assertEquals(0, f.quota.getReservedCpu("alice"));
    assertEquals(0, f.scheduler.getQueryMapSize());
    assertEquals(0, f.scheduler.getReadyQueueTaskCount());
    assertEquals(0, f.scheduler.getTimeoutQueueTaskCount());
  }

  @Test
  public void ordinaryQueryAbortKeepsPerDriverCascadeReasons() throws Exception {
    Fixture f = new Fixture(4, 256);
    RuntimeSchedulingGroup first = f.group(f.context("ordinary-abort-first"));
    RuntimeSchedulingGroup second = f.group(f.context("ordinary-abort-second"));
    IDriver firstDriver = f.driver(first.context, 0);
    IDriver secondDriver = f.driver(second.context, 0);
    PreparedDriverBatch firstBatch =
        f.scheduler.prepareRuntimeDrivers(first, List.of(f.sub(firstDriver)), 0);
    PreparedDriverBatch secondBatch =
        f.scheduler.prepareRuntimeDrivers(second, List.of(f.sub(secondDriver)), 0);
    f.scheduler.abortQuery(f.query);
    for (TaskRegistration member : List.of(firstBatch.members.get(0), secondBatch.members.get(0))) {
      Throwable cause = member.task.getAbortCause().orElseThrow();
      assertTrue(cause instanceof DriverTaskAbortedException);
      assertTrue(cause.getMessage().contains(member.getDriverTaskId().getFullId()));
      verify(member.getDriver()).failed(same(cause));
    }
    assertEquals(0, f.quota.getReservedCpu("alice"));
    assertEquals(0, f.scheduler.getQueryMapSize());
  }

  @Test
  public void observerCancellationAndOldGenerationCallbacksCannotPoisonNewGroup() throws Exception {
    Fixture f = new Fixture(2, 256);
    FragmentInstanceContext context = f.context("reuse");
    RuntimeSchedulingGroup old = f.group(context);
    SettableFuture<Void> dependency = SettableFuture.create();
    PreparedDriverBatch first =
        f.scheduler.prepareRuntimeDrivers(
            old,
            List.of(
                new DriverSubmission(
                    f.driver(context, 0), dependency, DriverSubmission.Role.DIRECT_NATIVE, 0)),
            0);
    ListenableFuture<Void> observer = first.members.get(0).getRemovedFuture();
    assertTrue(observer.cancel(false));
    DriverTask oldAnchor = f.scheduler.getTimeoutQueue().poll();
    f.scheduler.stopRuntimeGroup(old, null).get(5, TimeUnit.SECONDS);
    assertFalse(first.members.get(0).getRemovedFuture().isCancelled());
    RuntimeSchedulingGroup next = f.group(f.context("reuse"));
    PreparedDriverBatch nextBatch =
        f.scheduler.prepareRuntimeDrivers(next, List.of(f.sub(f.driver(next.context, 0))), 0);
    f.scheduler.activateRuntimeBatch(nextBatch);
    dependency.set(null);
    f.scheduler.getScheduler().toAborted(oldAnchor);
    assertEquals(1, f.scheduler.getQueryMapSize());
    assertEquals(1, f.quota.getReservedCpu("alice"));
    f.scheduler.stopRuntimeGroup(next, null).get(5, TimeUnit.SECONDS);
  }

  @Test
  public void quiescenceAndFailureCallbacksRunOutsideSchedulerLocks() throws Exception {
    Fixture f = new Fixture(2, 256);
    RuntimeSchedulingGroup group = f.group(f.context("callbacks"));
    IDriver driver = f.driver(group.context, 0);
    ExecutorService reader = Executors.newSingleThreadExecutor();
    try {
      doAnswer(
              call -> {
                reader.submit(group::getActiveRegistrations).get(5, TimeUnit.SECONDS);
                return Futures.immediateVoidFuture();
              })
          .when(driver)
          .requestQuiescence();
      doAnswer(
              call -> {
                reader.submit(group::getActiveRegistrations).get(5, TimeUnit.SECONDS);
                return null;
              })
          .when(driver)
          .failed(any());
      f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(driver)), 0);
      f.scheduler.stopRuntimeGroup(group, new IllegalStateException()).get(5, TimeUnit.SECONDS);
      verify(driver).requestQuiescence();
      verify(driver).failed(any());
    } finally {
      reader.shutdownNow();
    }
  }

  @Test
  public void oldSubmitCapturesDependencyFromOriginalUnmodifiedList() throws Exception {
    Fixture f = new Fixture(2, 256);
    FragmentInstanceContext context = f.context("old-list");
    IDriver first = f.driver(context, 0), second = f.driver(context, 1);
    SettableFuture<Void> original = SettableFuture.create();
    ExchangeOperator downstream = mock(ExchangeOperator.class);
    when(first.getDriverContext().getDownstreamOperator()).thenReturn(downstream);
    when(downstream.getBlockedDependencyDriver()).thenReturn(original);
    when(second.getDriverContext().getDependencyDriverIndex()).thenReturn(0);
    f.scheduler.submitDrivers(f.query, List.of(first, second), 60000, f.session);
    assertEquals(2, f.quota.getReservedCpu("alice"));
    assertEquals(2, f.scheduler.getQueryMap().get(f.query).get(context.getId()).size());
    original.set(null);
    f.scheduler.abortQuery(f.query);
    assertEquals(0, f.quota.getReservedCpu("alice"));
  }

  @Test
  public void missingActualRemovalProofWithholdsStopAndCpuSettlement() throws Exception {
    Fixture f = new Fixture(2, 256);
    RuntimeSchedulingGroup group = f.group(f.context("remove-failure"));
    PreparedDriverBatch batch =
        f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(f.driver(group.context, 0))), 0);
    f.scheduler.activateRuntimeBatch(batch);
    org.apache.iotdb.calc.execution.schedule.queue.IndexedBlockingReserveQueue<DriverTask> queue =
        spy(
            (org.apache.iotdb.calc.execution.schedule.queue.IndexedBlockingReserveQueue<DriverTask>)
                f.scheduler.getReadyQueue());
    doThrow(new IllegalStateException()).when(queue).remove(any(DriverTaskId.class));
    Field field = DriverScheduler.class.getDeclaredField("readyQueue");
    field.setAccessible(true);
    field.set(f.scheduler, queue);
    ListenableFuture<Void> stop = f.scheduler.stopRuntimeGroup(group, null);
    assertThrows(ExecutionException.class, stop::get);
    assertFalse(batch.members.get(0).getRemovedFuture().isDone());
    assertFalse(batch.members.get(0).getQuotaSettledFuture().isDone());
    assertEquals(1, f.quota.getReservedCpu("alice"));
    assertEquals(1, group.getActiveRegistrations().size());
    verify(f.exchange, never()).forceDeregisterFragmentInstance(any());
  }

  @Test
  public void requiredStopWaitsForAdmissionTransactionThatWasAlreadyRegistered() throws Exception {
    CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    ThrottleQuotaLimit delayed =
        new ThrottleQuotaLimit() {
          @Override
          public AdmissionTicket tryReserve(String user, long cpu, long memory) {
            entered.countDown();
            try {
              assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new AssertionError(e);
            }
            return super.tryReserve(user, cpu, memory);
          }
        };
    Fixture f = new Fixture(2, 256, 32, delayed);
    RuntimeSchedulingGroup group = f.group(f.context("inflight-admission"));
    IDriver driver = f.driver(group.context, 0);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> preparation =
          executor.submit(
              () ->
                  assertThrows(
                      IllegalStateException.class,
                      () -> f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(driver)), 0)));
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      assertEquals(1, group.getActiveRegistrations().size());
      ListenableFuture<Void> stop = f.scheduler.stopRuntimeGroup(group, null);
      assertFalse(stop.isDone());
      release.countDown();
      preparation.get(5, TimeUnit.SECONDS);
      stop.get(5, TimeUnit.SECONDS);
      assertEquals(0, f.quota.getReservedCpu("alice"));
      assertEquals(0, f.scheduler.getQueryMapSize());
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void successfulDriverQuiescenceStillWaitsForSchedulerStatisticsWriter() throws Exception {
    Fixture f = new Fixture(2, 256);
    FragmentInstanceContext context = f.context("statistics-writer");
    RuntimeSchedulingGroup group = f.group(context);
    CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    doAnswer(
            call -> {
              entered.countDown();
              assertTrue(release.await(5, TimeUnit.SECONDS));
              return null;
            })
        .when(context)
        .addReadyQueuedTime(anyLong());
    PreparedDriverBatch batch =
        f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(f.driver(context, 0))), 0);
    f.scheduler.activateRuntimeBatch(batch);
    DriverTask task = f.scheduler.getReadyQueue().poll();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<Boolean> dispatch =
          executor.submit(() -> f.scheduler.getScheduler().readyToRunning(task));
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      ListenableFuture<Void> stop = f.scheduler.stopRuntimeGroup(group, null);
      assertTrue(batch.members.get(0).getRemovedFuture().isDone());
      assertFalse(stop.isDone());
      assertFalse(batch.members.get(0).getExecutionStoppedFuture().isDone());
      assertEquals(1, f.quota.getReservedCpu("alice"));
      release.countDown();
      dispatch.get(5, TimeUnit.SECONDS);
      stop.get(5, TimeUnit.SECONDS);
      assertEquals(0, f.quota.getReservedCpu("alice"));
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void exactQuotaDeniedGrowthPreservesExistingWorkAndLaterGrowthCanSucceed()
      throws Exception {
    Fixture f = new Fixture(2, 256);
    RuntimeSchedulingGroup group = f.group(f.context("recoverable-growth"));
    IDriver original = f.driver(group.context, 0);
    PreparedDriverBatch initial =
        f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(original)), 64);
    RuntimeSchedulingGroup blocker = f.group(f.context("quota-blocker"));
    f.scheduler.prepareRuntimeDrivers(blocker, List.of(f.sub(f.driver(blocker.context, 0))), 0);
    AtomicInteger released = new AtomicInteger();
    DriverSubmission candidate = f.workerSub(group.context, 1, 16, released::incrementAndGet);
    RuntimeGrowthPreparation denied = f.scheduler.prepareRuntimeGrowth(group, List.of(candidate));
    assertTrue(denied.isQuotaDenied());
    assertTrue(denied.getAdmittedBatch().isEmpty());
    denied.getRejectionSettlementFuture().get(5, TimeUnit.SECONDS);
    assertEquals(1, denied.getCandidates().size());
    TaskRegistration rejected = denied.getCandidates().get(0);
    rejected.getRemovedFuture().get(5, TimeUnit.SECONDS);
    rejected.getExecutionStoppedFuture().get(5, TimeUnit.SECONDS);
    rejected.getQuotaSettledFuture().get(5, TimeUnit.SECONDS);
    assertThrows(ExecutionException.class, () -> rejected.batch.getActivationFuture().get());
    assertEquals(1, released.get());
    assertFalse(group.stopped);
    assertFalse(group.getStoppedAndRemovedFuture().isDone());
    assertEquals(List.of(initial.members.get(0)), group.getActiveRegistrations());
    assertEquals(2, f.quota.getReservedCpu("alice"));
    assertEquals(64, f.quota.getReservedMemory("alice"));
    assertEquals(0, f.scheduler.getReadyQueueTaskCount());
    verify(original, never()).failed(any());
    verify(candidate.driver().getDriverContext(), never()).failed(any());
    assertThrows(UnsupportedOperationException.class, () -> denied.getCandidates().clear());
    f.scheduler.stopRuntimeGroup(blocker, null).get(5, TimeUnit.SECONDS);
    RuntimeGrowthPreparation admitted =
        f.scheduler.prepareRuntimeGrowth(
            group, List.of(f.workerSub(group.context, 2, 16, () -> {})));
    assertFalse(admitted.isQuotaDenied());
    PreparedDriverBatch grown = admitted.getAdmittedBatch().orElseThrow();
    assertEquals(f.deadline, grown.members.get(0).task.getDDL());
    assertSame(
        initial.members.get(0).task.getDriverTaskHandle(),
        grown.members.get(0).task.getDriverTaskHandle());
    f.scheduler.activateRuntimeBatch(grown);
    assertEquals(2, f.quota.getReservedCpu("alice"));
    assertEquals(80, f.quota.getReservedMemory("alice"));
    assertEquals(1, f.scheduler.getReadyQueueTaskCount());
    f.scheduler.stopRuntimeGroup(group, null).get(5, TimeUnit.SECONDS);
    assertEquals(0, f.quota.getReservedCpu("alice"));
    assertEquals(64, f.quota.getReservedMemory("alice"));
    f.releaseMemory();
    assertEquals(0, f.quota.getReservedMemory("alice"));
  }

  @Test
  public void exactMemoryDeniedGrowthDoesNotReservePartialCpuOrReleaseBaseMemory()
      throws Exception {
    Fixture f = new Fixture(4, 64);
    RuntimeSchedulingGroup group = f.group(f.context("growth-memory-denial"));
    IDriver original = f.driver(group.context, 0);
    f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(original)), 64);
    RuntimeGrowthPreparation denied =
        f.scheduler.prepareRuntimeGrowth(
            group, List.of(f.workerSub(group.context, 1, 16, () -> {})));
    assertTrue(denied.isQuotaDenied());
    denied.getRejectionSettlementFuture().get(5, TimeUnit.SECONDS);
    assertEquals(1, f.quota.getReservedCpu("alice"));
    assertEquals(64, f.quota.getReservedMemory("alice"));
    assertFalse(group.stopped);
    verify(original, never()).failed(any());
    f.scheduler.stopRuntimeGroup(group, null).get(5, TimeUnit.SECONDS);
    assertEquals(64, f.quota.getReservedMemory("alice"));
    f.releaseMemory();
  }

  @Test
  public void deniedCandidateCannotBeActivatedEvenWhenItsDependencyCompletesLater()
      throws Exception {
    Fixture f = new Fixture(1, 256);
    RuntimeSchedulingGroup group = f.group(f.context("denied-activation"));
    f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(f.driver(group.context, 0))), 0);
    DriverSubmission worker = f.workerSub(group.context, 1, 0, () -> {});
    SettableFuture<Void> dependency = SettableFuture.create();
    RuntimeGrowthPreparation denied =
        f.scheduler.prepareRuntimeGrowth(
            group,
            List.of(
                new DriverSubmission(
                    worker.driver(), dependency, DriverSubmission.Role.SCAN_WORKER, 0)));
    denied.getRejectionSettlementFuture().get(5, TimeUnit.SECONDS);
    assertTrue(denied.getAdmittedBatch().isEmpty());
    dependency.set(null);
    assertEquals(0, f.scheduler.getReadyQueueTaskCount());
    assertFalse(group.stopped);
    assertThrows(
        IllegalStateException.class,
        () -> f.scheduler.activateRuntimeBatch(denied.getCandidates().get(0).batch));
    assertEquals(0, f.scheduler.getReadyQueueTaskCount());
    assertEquals(0, f.quota.getReservedCpu("alice"));
  }

  @Test
  public void growthAcceptsOnlyScanWorkersAndOtherValidationErrorsRemainTerminal()
      throws Exception {
    Fixture f = new Fixture(2, 256);
    RuntimeSchedulingGroup group = f.group(f.context("invalid-growth-role"));
    IDriver original = f.driver(group.context, 0);
    f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(original)), 0);
    assertThrows(
        IllegalArgumentException.class,
        () -> f.scheduler.prepareRuntimeGrowth(group, List.of(f.sub(f.driver(group.context, 1)))));
    assertTrue(group.stopped);
    assertEquals(0, f.quota.getReservedCpu("alice"));
    verify(original).failed(any());
  }

  @Test
  public void nonQuotaAdmissionFailureStillTerminatesExistingFragment() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    ThrottleQuotaLimit quota =
        new ThrottleQuotaLimit() {
          @Override
          public AdmissionTicket tryReserve(String user, long cpu, long memory) {
            if (attempts.incrementAndGet() == 2) throw new IllegalArgumentException();
            return super.tryReserve(user, cpu, memory);
          }
        };
    Fixture f = new Fixture(2, 256, 32, quota);
    RuntimeSchedulingGroup group = f.group(f.context("growth-unexpected-error"));
    IDriver original = f.driver(group.context, 0);
    f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(original)), 0);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            f.scheduler.prepareRuntimeGrowth(
                group, List.of(f.workerSub(group.context, 1, 0, () -> {}))));
    assertTrue(group.stopped);
    assertEquals(0, f.quota.getReservedCpu("alice"));
    verify(original).failed(any());
  }

  @Test
  public void quotaDenialRacingWithStopCannotCertifyAStillLiveFragment() throws Exception {
    CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    AtomicInteger attempts = new AtomicInteger();
    ThrottleQuotaLimit quota =
        new ThrottleQuotaLimit() {
          @Override
          public AdmissionTicket tryReserve(String user, long cpu, long memory) {
            if (attempts.incrementAndGet() == 2) {
              entered.countDown();
              try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
              }
              return null;
            }
            return super.tryReserve(user, cpu, memory);
          }
        };
    Fixture f = new Fixture(2, 256, 32, quota);
    RuntimeSchedulingGroup group = f.group(f.context("denial-stop-race"));
    f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(f.driver(group.context, 0))), 0);
    DriverSubmission candidate = f.workerSub(group.context, 1, 0, () -> {});
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<RuntimeGrowthPreparation> pending =
          executor.submit(() -> f.scheduler.prepareRuntimeGrowth(group, List.of(candidate)));
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      ListenableFuture<Void> stopped = f.scheduler.stopRuntimeGroup(group, null);
      assertFalse(stopped.isDone());
      release.countDown();
      RuntimeGrowthPreparation denied = pending.get(5, TimeUnit.SECONDS);
      assertTrue(denied.isQuotaDenied());
      assertThrows(ExecutionException.class, () -> denied.getRejectionSettlementFuture().get());
      stopped.get(5, TimeUnit.SECONDS);
      assertEquals(0, f.quota.getReservedCpu("alice"));
      assertEquals(0, f.scheduler.getQueryMapSize());
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void deadlineExpiryCannotConvertQuotaDenialIntoABenignReceipt() throws Exception {
    CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
    AtomicInteger attempts = new AtomicInteger();
    ThrottleQuotaLimit quota =
        new ThrottleQuotaLimit() {
          @Override
          public AdmissionTicket tryReserve(String user, long cpu, long memory) {
            if (attempts.incrementAndGet() == 2) {
              entered.countDown();
              try {
                assertTrue(release.await(5, TimeUnit.SECONDS));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
              }
              return null;
            }
            return super.tryReserve(user, cpu, memory);
          }
        };
    Fixture f = new Fixture(2, 256, 32, quota);
    FragmentInstanceContext context = f.context("denial-deadline");
    long originalDeadline = System.currentTimeMillis() + 1000;
    RuntimeSchedulingGroup group =
        f.scheduler.openRuntimeGroup(context, originalDeadline, f.session, () -> {}, ignored -> {});
    f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(f.driver(context, 0))), 0);
    DriverSubmission candidate = f.workerSub(context, 1, 0, () -> {});
    ExecutorService executor = Executors.newSingleThreadExecutor();
    java.util.concurrent.ScheduledExecutorService timer =
        Executors.newSingleThreadScheduledExecutor();
    try {
      Future<RuntimeGrowthPreparation> pending =
          executor.submit(() -> f.scheduler.prepareRuntimeGrowth(group, List.of(candidate)));
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      timer.schedule(
          release::countDown,
          Math.max(0, originalDeadline - System.currentTimeMillis() + 25),
          TimeUnit.MILLISECONDS);
      RuntimeGrowthPreparation denied = pending.get(5, TimeUnit.SECONDS);
      assertTrue(denied.isQuotaDenied());
      assertThrows(ExecutionException.class, () -> denied.getRejectionSettlementFuture().get());
      assertTrue(group.stopped);
      assertEquals(originalDeadline, group.getOriginalAbsoluteDeadlineMillis());
      assertEquals(0, f.quota.getReservedCpu("alice"));
    } finally {
      release.countDown();
      timer.shutdownNow();
      executor.shutdownNow();
    }
  }

  @Test
  public void admittedGrowthStillCannotActivateAfterConcurrentStop() throws Exception {
    Fixture f = new Fixture(2, 256);
    RuntimeSchedulingGroup group = f.group(f.context("growth-stop-before-activation"));
    f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(f.driver(group.context, 0))), 0);
    RuntimeGrowthPreparation growth =
        f.scheduler.prepareRuntimeGrowth(
            group, List.of(f.workerSub(group.context, 1, 16, () -> {})));
    assertFalse(growth.isQuotaDenied());
    PreparedDriverBatch batch = growth.getAdmittedBatch().orElseThrow();
    f.scheduler.stopRuntimeGroup(group, null).get(5, TimeUnit.SECONDS);
    assertThrows(IllegalStateException.class, () -> f.scheduler.activateRuntimeBatch(batch));
    assertEquals(0, f.scheduler.getReadyQueueTaskCount());
    assertEquals(0, f.quota.getReservedCpu("alice"));
    assertEquals(0, f.quota.getReservedMemory("alice"));
  }

  @Test
  public void growthPublicationFailureRollsBackAndStopsExistingNativeMembers() throws Exception {
    Fixture f = new Fixture(2, 256, 1);
    RuntimeSchedulingGroup group = f.group(f.context("growth-publication-failure"));
    IDriver original = f.driver(group.context, 0);
    PreparedDriverBatch initial =
        f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(original)), 0);
    f.scheduler.activateRuntimeBatch(initial);
    PreparedDriverBatch growth =
        f.scheduler
            .prepareRuntimeGrowth(group, List.of(f.workerSub(group.context, 1, 16, () -> {})))
            .getAdmittedBatch()
            .orElseThrow();
    assertThrows(IllegalStateException.class, () -> f.scheduler.activateRuntimeBatch(growth));
    assertTrue(group.stopped);
    assertEquals(0, f.quota.getReservedCpu("alice"));
    assertEquals(0, f.quota.getReservedMemory("alice"));
    assertEquals(0, f.scheduler.getReadyQueueTaskCount());
    verify(original).failed(any());
  }

  @Test
  public void failedDeniedCandidateCessationCannotCertifyRecoverableRollback() throws Exception {
    Fixture f = new Fixture(1, 256);
    RuntimeSchedulingGroup group = f.group(f.context("denied-cessation-failure"));
    f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(f.driver(group.context, 0))), 0);
    IllegalStateException expected = new IllegalStateException();
    RuntimeGrowthPreparation denied =
        f.scheduler.prepareRuntimeGrowth(
            group,
            List.of(
                f.workerSub(
                    group.context,
                    1,
                    16,
                    () -> {
                      throw expected;
                    })));
    assertTrue(denied.isQuotaDenied());
    assertThrows(ExecutionException.class, () -> denied.getRejectionSettlementFuture().get());
    assertTrue(group.stopped);
    assertFalse(denied.getCandidates().get(0).getQuotaSettledFuture().isDone());
    assertThrows(
        ExecutionException.class,
        () -> denied.getCandidates().get(0).getExecutionStoppedFuture().get());
  }

  @Test
  public void repeatedRetirementKeepsOnlyFiniteDirectIdsAndOneWorkerHighWater() throws Exception {
    Fixture f = new Fixture(2, 256);
    RuntimeSchedulingGroup group = f.group(f.context("bounded-worker-history"));
    f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(f.driver(group.context, 0))), 0);
    for (int id = 1; id <= 128; id++) {
      PreparedDriverBatch growth =
          f.scheduler
              .prepareRuntimeGrowth(group, List.of(f.workerSub(group.context, id, 16, () -> {})))
              .getAdmittedBatch()
              .orElseThrow();
      f.scheduler.activateRuntimeBatch(growth);
      TaskRegistration member = growth.members.get(0);
      f.scheduler.retireRuntimeWorker(group, member).get(5, TimeUnit.SECONDS);
      member.getExecutionStoppedFuture().get(5, TimeUnit.SECONDS);
      member.getQuotaSettledFuture().get(5, TimeUnit.SECONDS);
      assertEquals(1, group.getActiveRegistrations().size());
      assertEquals(1, group.initialDirectIds.size());
      assertEquals(id, group.workerIdHighWater);
      assertEquals(1, f.quota.getReservedCpu("alice"));
      assertEquals(0, f.quota.getReservedMemory("alice"));
    }
    assertEquals(1, f.scheduler.getTimeoutQueueTaskCount());
    f.scheduler.stopRuntimeGroup(group, null).get(5, TimeUnit.SECONDS);
  }

  @Test
  public void retiredWorkerIdCannotBeReusedAndOutOfOrderGrowthIsTerminal() throws Exception {
    for (boolean reuse : List.of(false, true)) {
      Fixture f = new Fixture(4, 256);
      RuntimeSchedulingGroup group = f.group(f.context("monotonic-worker-ids"));
      f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(f.driver(group.context, 0))), 0);
      if (reuse) {
        PreparedDriverBatch previous =
            f.scheduler
                .prepareRuntimeGrowth(group, List.of(f.workerSub(group.context, 1, 0, () -> {})))
                .getAdmittedBatch()
                .orElseThrow();
        f.scheduler.retireRuntimeWorker(group, previous.members.get(0)).get(5, TimeUnit.SECONDS);
      }
      List<DriverSubmission> candidates =
          reuse
              ? List.of(f.workerSub(group.context, 1, 0, () -> {}))
              : List.of(
                  f.workerSub(group.context, 2, 0, () -> {}),
                  f.workerSub(group.context, 1, 0, () -> {}));
      assertThrows(
          IllegalArgumentException.class,
          () -> f.scheduler.prepareRuntimeGrowth(group, candidates));
      assertTrue(group.stopped);
      assertEquals(0, f.quota.getReservedCpu("alice"));
    }
  }

  @Test
  public void runtimeDirectNativeIdsAreSealedAtInitialGraphRegistration() throws Exception {
    Fixture f = new Fixture(2, 256);
    RuntimeSchedulingGroup group = f.group(f.context("sealed-direct-ids"));
    f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(f.driver(group.context, 0))), 0);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            f.scheduler.prepareRuntimeDrivers(
                group, List.of(f.sub(f.driver(group.context, 1))), 0));
    assertEquals(1, group.initialDirectIds.size());
    assertTrue(group.stopped);
    assertEquals(0, f.quota.getReservedCpu("alice"));
  }

  @Test
  public void defaultAbortMessagesIdentifyEveryPipeline() throws Exception {
    for (boolean abortQuery : List.of(false, true)) {
      Fixture f = new Fixture(2, 256);
      RuntimeSchedulingGroup group = f.group(f.context("abort-pipelines"));
      IDriver first = f.driver(group.context, 2);
      IDriver second = f.driver(group.context, 7);
      PreparedDriverBatch batch =
          f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(first), f.sub(second)), 0);
      if (abortQuery) {
        f.scheduler.abortQuery(f.query);
      } else {
        f.scheduler.abortFragmentInstance(group.context.getId(), null);
      }
      group.getStoppedAndRemovedFuture().get(5, TimeUnit.SECONDS);
      String reason =
          abortQuery
              ? DriverTaskAbortedException.BY_QUERY_CASCADING_ABORTED
              : DriverTaskAbortedException.BY_FRAGMENT_ABORT_CALLED;
      for (TaskRegistration member : batch.members) {
        Throwable failure = member.task.getAbortCause().orElseThrow();
        assertEquals(
            new DriverTaskAbortedException(member.getDriverTaskId().getFullId(), reason)
                .getMessage(),
            failure.getMessage());
        verify(member.getDriver()).failed(same(failure));
      }
      assertNotEquals(
          batch.members.get(0).task.getAbortCause().orElseThrow().getMessage(),
          batch.members.get(1).task.getAbortCause().orElseThrow().getMessage());
      assertEquals(0, f.quota.getReservedCpu("alice"));
    }
  }

  @Test
  public void explicitFragmentAbortCauseRetainsOriginalFailureIdentity() throws Exception {
    Fixture f = new Fixture(2, 256);
    RuntimeSchedulingGroup group = f.group(f.context("abort-explicit-cause"));
    IDriver first = f.driver(group.context, 2);
    IDriver second = f.driver(group.context, 7);
    PreparedDriverBatch batch =
        f.scheduler.prepareRuntimeDrivers(group, List.of(f.sub(first), f.sub(second)), 0);
    Throwable cause = new IllegalStateException("original fragment failure");
    f.scheduler.abortFragmentInstance(group.context.getId(), cause);
    group.getStoppedAndRemovedFuture().get(5, TimeUnit.SECONDS);
    for (TaskRegistration member : batch.members) {
      assertSame(cause, member.task.getAbortCause().orElseThrow());
      verify(member.getDriver()).failed(same(cause));
    }
    assertEquals(0, f.quota.getReservedCpu("alice"));
  }

  private static class Fixture {
    final QueryId query = new QueryId("runtime_scheduler");
    final ThrottleQuotaLimit quota;
    final DriverScheduler scheduler;
    final IMPPDataExchangeManager exchange = mock(IMPPDataExchangeManager.class);
    final SessionInfo session = mock(SessionInfo.class);
    final long deadline = System.currentTimeMillis() + 60000;
    final List<AutoCloseable> memoryLeases = new ArrayList<>();

    Fixture(int cpus, long memory) {
      this(cpus, memory, 32);
    }

    Fixture(int cpus, long memory, int capacity) {
      this(cpus, memory, capacity, new ThrottleQuotaLimit());
    }

    Fixture(int cpus, long memory, int capacity, ThrottleQuotaLimit quota) {
      this.quota = quota;
      TThrottleQuota value = new TThrottleQuota();
      value.setThrottleLimit(Collections.emptyMap());
      value.setCpuLimit(cpus);
      value.setMemLimit(memory);
      TSetThrottleQuotaReq request = new TSetThrottleQuotaReq();
      request.setUserName("alice");
      request.setThrottleQuota(value);
      quota.setQuotas(request);
      scheduler = new DriverScheduler(quota, capacity, 32);
      scheduler.setBlockManager(exchange);
      when(session.getUserName()).thenReturn("alice");
    }

    FragmentInstanceContext context(String name) {
      FragmentInstanceContext context = mock(FragmentInstanceContext.class);
      when(context.getId()).thenReturn(new FragmentInstanceId(new PlanFragmentId(query, 0), name));
      doAnswer(
              call -> {
                memoryLeases.add(call.getArgument(0));
                return true;
              })
          .when(context)
          .registerQuotaMemoryLease(any());
      return context;
    }

    RuntimeSchedulingGroup group(FragmentInstanceContext context) {
      return scheduler.openRuntimeGroup(context, deadline, session, () -> {}, ignored -> {});
    }

    DriverContext driverContext(FragmentInstanceContext context, int id) {
      DriverTaskId taskId = new DriverTaskId(context.getId(), id);
      DriverContext driver = mock(DriverContext.class);
      when(driver.getFragmentInstanceContext()).thenReturn(context);
      when(driver.getDependencyDriverIndex()).thenReturn(-1);
      when(driver.getDriverTaskID()).thenReturn(taskId);
      return driver;
    }

    IDriver driver(FragmentInstanceContext context, int id) {
      return driver(context, id, Futures.immediateVoidFuture());
    }

    IDriver driver(FragmentInstanceContext context, int id, ListenableFuture<Void> stopped) {
      DriverTaskId taskId = new DriverTaskId(context.getId(), id);
      DriverContext contextForDriver = driverContext(context, id);
      IDriver driver = mock(IDriver.class);
      when(driver.getDriverTaskId()).thenReturn(taskId);
      when(driver.getDriverContext()).thenReturn(contextForDriver);
      when(driver.supportsQuiescence()).thenReturn(true);
      when(driver.requestQuiescence()).thenReturn(stopped);
      return driver;
    }

    DriverSubmission sub(IDriver driver) {
      return new DriverSubmission(
          driver, Futures.immediateVoidFuture(), DriverSubmission.Role.DIRECT_NATIVE, 0);
    }

    DriverSubmission workerSub(
        FragmentInstanceContext context, int id, long bytes, Runnable release) {
      RuntimeScanWorker worker =
          new RuntimeScanWorker(
              new RuntimeScanGroup(List.of(driver(context, 10000 + id))),
              driverContext(context, id),
              id,
              bytes,
              release);
      return new DriverSubmission(
          worker, Futures.immediateVoidFuture(), DriverSubmission.Role.SCAN_WORKER, bytes);
    }

    void releaseMemory() throws Exception {
      for (AutoCloseable lease : memoryLeases) lease.close();
    }
  }
}
