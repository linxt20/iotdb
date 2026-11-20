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

import org.apache.iotdb.calc.exception.MemoryNotEnoughException;
import org.apache.iotdb.calc.execution.schedule.queue.IndexedBlockingQueue;
import org.apache.iotdb.calc.execution.schedule.queue.IndexedBlockingReserveQueue;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.DataNodeMemoryConfig;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.exception.CpuNotEnoughException;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.exchange.IMPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeScanWorker;
import org.apache.iotdb.db.queryengine.execution.schedule.queue.L1PriorityQueue;
import org.apache.iotdb.db.queryengine.execution.schedule.queue.multilevelqueue.DriverTaskHandle;
import org.apache.iotdb.db.queryengine.execution.schedule.queue.multilevelqueue.MultilevelPriorityQueue;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTask;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskId;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskStatus;
import org.apache.iotdb.db.storageengine.rescon.quotas.DataNodeThrottleQuotaManager;
import org.apache.iotdb.db.storageengine.rescon.quotas.ThrottleQuotaLimit;
import org.apache.iotdb.db.storageengine.rescon.quotas.ThrottleQuotaLimit.AdmissionTicket;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/** The manager of fragment instances scheduling. */
public class DriverScheduler implements IDriverScheduler, IService {

  private static final Logger logger = LoggerFactory.getLogger(DriverScheduler.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final DataNodeMemoryConfig memoryConfig =
      IoTDBDescriptor.getInstance().getMemoryConfig();
  private static final double LEVEL_TIME_MULTIPLIER = 2;

  public static DriverScheduler getInstance() {
    return InstanceHolder.instance;
  }

  private final IndexedBlockingReserveQueue<DriverTask> readyQueue;
  private final IndexedBlockingQueue<DriverTask> timeoutQueue;
  private final Set<DriverTask> blockedTasks;
  private final Map<QueryId, Map<FragmentInstanceId, Set<DriverTask>>> queryMap;
  private final ITaskScheduler scheduler;

  private final AtomicInteger nextDriverTaskHandleId = new AtomicInteger(0);
  private final AtomicLong completedDriverTaskCount = new AtomicLong(0);
  private IMPPDataExchangeManager blockManager;
  private final Object registrationLock = new Object();
  private final Map<FragmentInstanceId, RuntimeSchedulingGroup> schedulingGroups = new HashMap<>();
  private final Map<DriverTask, TaskRegistration> registrations = new IdentityHashMap<>();
  private final Map<QueryId, DriverTask> deadlineAnchors = new HashMap<>();
  private final ThrottleQuotaLimit injectedQuota;

  private static final int QUERY_MAX_CAPACITY = config.getMaxAllowedConcurrentQueries();
  private static final int WORKER_THREAD_NUM = memoryConfig.getQueryThreadCount();
  // Cap the audit walk (query entries, FI entries and tasks). It is not used by the policy timer.
  private static final int RUNTIME_PRESSURE_WALK_BUDGET = 4096;
  private static final long CONTROL_PRESSURE_INTERVAL_NANOS =
      java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(250);
  private final Object pressureSampleLock = new Object();
  private PressureFrame lastControlPressure;
  private static final int TASK_MAX_CAPACITY = QUERY_MAX_CAPACITY * config.getDegreeOfParallelism();
  private static final long QUERY_TIMEOUT_MS = config.getQueryTimeoutThreshold();
  private final ThreadGroup workerGroups;
  private final List<AbstractDriverThread> threads;

  private DriverScheduler() {
    this(null);
  }

  @TestOnly
  public DriverScheduler(ThrottleQuotaLimit quota) {
    this(quota, TASK_MAX_CAPACITY, QUERY_MAX_CAPACITY);
  }

  @TestOnly
  public DriverScheduler(ThrottleQuotaLimit quota, int taskCapacity, int queryCapacity) {
    this.injectedQuota = quota;
    this.readyQueue =
        new MultilevelPriorityQueue(LEVEL_TIME_MULTIPLIER, taskCapacity, new DriverTask());
    this.timeoutQueue =
        new L1PriorityQueue<>(queryCapacity, new DriverTask.TimeoutComparator(), new DriverTask());
    this.queryMap = new ConcurrentHashMap<>();
    this.blockedTasks = Collections.synchronizedSet(new HashSet<>());
    this.scheduler = new Scheduler();
    this.workerGroups = new ThreadGroup("ScheduleThreads");
    this.threads = new ArrayList<>();
    this.blockManager = MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
  }

  @Override
  public void start() throws StartupException {
    for (int i = 0; i < WORKER_THREAD_NUM; i++) {
      int index = i;
      String threadName = ThreadName.QUERY_WORKER.getName() + "-" + i;
      ThreadProducer producer =
          new ThreadProducer() {
            @Override
            public void produce(
                String threadName,
                ThreadGroup workerGroups,
                IndexedBlockingQueue<DriverTask> queue,
                ThreadProducer producer) {
              DriverTaskThread newThread =
                  new DriverTaskThread(threadName, workerGroups, readyQueue, scheduler, this);
              threads.set(index, newThread);
              newThread.start();
            }
          };
      AbstractDriverThread t =
          new DriverTaskThread(threadName, workerGroups, readyQueue, scheduler, producer);
      threads.add(t);
      t.start();
    }

    String threadName = ThreadName.QUERY_SENTINEL.getName();
    ThreadProducer producer =
        new ThreadProducer() {
          @Override
          public void produce(
              String threadName,
              ThreadGroup workerGroups,
              IndexedBlockingQueue<DriverTask> queue,
              ThreadProducer producer) {
            DriverTaskTimeoutSentinelThread newThread =
                new DriverTaskTimeoutSentinelThread(
                    threadName, workerGroups, timeoutQueue, scheduler, this);
            threads.set(WORKER_THREAD_NUM, newThread);
            newThread.start();
          }
        };

    AbstractDriverThread t =
        new DriverTaskTimeoutSentinelThread(
            threadName, workerGroups, timeoutQueue, scheduler, producer);
    threads.add(t);
    t.start();
  }

  @Override
  public void stop() {
    this.threads.forEach(
        t -> {
          try {
            t.close();
            t.interrupt();
          } catch (IOException e) {
            // Only a field is set, there's no chance to throw an IOException
          }
        });
  }

  @Override
  public ServiceType getID() {
    return ServiceType.FRAGMENT_INSTANCE_MANAGER_SERVICE;
  }

  @Override
  public void submitDrivers(
      QueryId queryId, List<IDriver> drivers, long timeout, SessionInfo session)
      throws CpuNotEnoughException, MemoryNotEnoughException {
    List<IDriver> original = List.copyOf(drivers);
    List<DriverSubmission> submissions = new ArrayList<>();
    for (IDriver driver : original) {
      int index = driver.getDriverContext().getDependencyDriverIndex();
      ListenableFuture<?> dependency =
          index == -1
              ? Futures.immediateVoidFuture()
              : original
                  .get(index)
                  .getDriverContext()
                  .getDownstreamOperator()
                  .getBlockedDependencyDriver();
      submissions.add(
          new DriverSubmission(driver, dependency, DriverSubmission.Role.LEGACY_NATIVE, 0));
    }
    submitDriverBatch(
        new DriverBatchRequest(
            queryId,
            submissions,
            DriverTask.computeDeadlineTimeInMs(timeout > 0 ? timeout : QUERY_TIMEOUT_MS),
            session));
  }

  @Override
  public RuntimeSchedulingGroup openRuntimeGroup(
      FragmentInstanceContext context,
      long deadline,
      SessionInfo session,
      Runnable stopAndWake,
      Consumer<Throwable> failFragment) {
    Objects.requireNonNull(context);
    return openGroup(
        context.getId(),
        context,
        deadline,
        session,
        Objects.requireNonNull(stopAndWake),
        Objects.requireNonNull(failFragment),
        true);
  }

  private RuntimeSchedulingGroup openGroup(
      FragmentInstanceId id,
      FragmentInstanceContext context,
      long deadline,
      SessionInfo session,
      Runnable stop,
      Consumer<Throwable> fail,
      boolean runtime) {
    RuntimeSchedulingGroup group =
        new RuntimeSchedulingGroup(
            this,
            id,
            context,
            deadline,
            session,
            new DriverTaskHandle(
                getNextDriverTaskHandleId(),
                (MultilevelPriorityQueue) readyQueue,
                OptionalInt.of(Integer.MAX_VALUE)),
            stop,
            fail,
            runtime);
    synchronized (registrationLock) {
      if (schedulingGroups.containsKey(id)) {
        throw new IllegalStateException();
      }
      schedulingGroups.put(id, group);
      try {
        installDeadline(group);
      } catch (RuntimeException | Error e) {
        schedulingGroups.remove(id, group);
        throw e;
      }
    }
    return group;
  }

  public List<TaskRegistration> getRuntimeRegistrations(RuntimeSchedulingGroup group) {
    requireOwned(group);
    synchronized (registrationLock) {
      return List.copyOf(group.members.values());
    }
  }

  @Override
  public PreparedDriverBatch prepareRuntimeDrivers(
      RuntimeSchedulingGroup group, List<DriverSubmission> submissions, long baseMemory)
      throws CpuNotEnoughException, MemoryNotEnoughException {
    try {
      return prepareBatch(
          Collections.singletonMap(group, List.copyOf(submissions)),
          Collections.singletonMap(group, baseMemory));
    } catch (RuntimeException | Error | CpuNotEnoughException failure) {
      stopRuntimeGroup(group, failure);
      throw failure;
    }
  }

  @Override
  public RuntimeGrowthPreparation prepareRuntimeGrowth(
      RuntimeSchedulingGroup group, List<DriverSubmission> submissions)
      throws CpuNotEnoughException, MemoryNotEnoughException {
    try {
      requireOwned(group);
      List<DriverSubmission> candidates = List.copyOf(submissions);
      if (!group.runtime || candidates.isEmpty()) {
        throw new IllegalArgumentException();
      }
      for (DriverSubmission candidate : candidates) {
        if (candidate.role() != DriverSubmission.Role.SCAN_WORKER) {
          throw new IllegalArgumentException();
        }
      }
      synchronized (registrationLock) {
        requireLive(group);
        if (!group.initialDirectIdsSealed) {
          throw new IllegalStateException();
        }
      }
      PreparedDriverBatch admitted =
          prepareBatch(
              Collections.singletonMap(group, candidates), Collections.singletonMap(group, 0L));
      return new RuntimeGrowthPreparation(
          admitted, admitted.members, Futures.immediateVoidFuture());
    } catch (QuotaDeniedException denied) {
      return new RuntimeGrowthPreparation(
          null, denied.batch.members, rejectionSettlement(group, denied.batch));
    } catch (RuntimeException | Error | CpuNotEnoughException failure) {
      stopRuntimeGroup(group, failure);
      throw failure;
    }
  }

  private ListenableFuture<Void> rejectionSettlement(
      RuntimeSchedulingGroup group, PreparedDriverBatch batch) {
    com.google.common.util.concurrent.SettableFuture<Void> result =
        com.google.common.util.concurrent.SettableFuture.create();
    List<ListenableFuture<?>> proofs = new ArrayList<>();
    for (TaskRegistration member : batch.members) {
      proofs.add(member.getRemovedFuture());
      proofs.add(member.getExecutionStoppedFuture());
      proofs.add(member.getQuotaSettledFuture());
      member
          .getFailureFuture()
          .addListener(
              () -> {
                Throwable failure;
                try {
                  Futures.getDone(member.getFailureFuture());
                  failure = new IllegalStateException();
                } catch (Throwable notified) {
                  failure =
                      notified instanceof ExecutionException && notified.getCause() != null
                          ? notified.getCause()
                          : notified;
                }
                try {
                  result.setException(failure);
                } finally {
                  stopRuntimeGroup(group, failure);
                }
              },
              MoreExecutors.directExecutor());
    }
    ListenableFuture<?> all = Futures.successfulAsList(proofs);
    all.addListener(
        () -> {
          Throwable failure = null;
          try {
            for (ListenableFuture<?> proof : proofs) {
              Futures.getDone(proof);
            }
            synchronized (registrationLock) {
              requireLive(group);
            }
          } catch (Throwable rejected) {
            failure =
                rejected instanceof ExecutionException && rejected.getCause() != null
                    ? rejected.getCause()
                    : rejected;
          }
          if (failure == null) {
            result.set(null);
          } else {
            try {
              result.setException(failure);
            } finally {
              stopRuntimeGroup(group, failure);
            }
          }
        },
        MoreExecutors.directExecutor());
    return result;
  }

  private static final class QuotaDeniedException extends CpuNotEnoughException {
    private static final long serialVersionUID = 1L;
    private final PreparedDriverBatch batch;

    private QuotaDeniedException(PreparedDriverBatch batch) {
      super(DataNodeQueryMessages.EXCEPTION_NATIVE_SCHEDULER_BATCH_QUOTA_REJECTED_55A3C12F);
      this.batch = batch;
    }
  }

  @Override
  public void submitDriverBatch(DriverBatchRequest request)
      throws CpuNotEnoughException, MemoryNotEnoughException {
    if (request.submissions().isEmpty()) {
      return;
    }
    Map<FragmentInstanceId, List<DriverSubmission>> byFi = new LinkedHashMap<>();
    for (DriverSubmission submission : request.submissions()) {
      FragmentInstanceId id = submission.driver().getDriverTaskId().getFragmentInstanceId();
      if (!id.getQueryId().equals(request.queryId())) {
        throw new IllegalArgumentException();
      }
      byFi.computeIfAbsent(id, ignored -> new ArrayList<>()).add(submission);
    }
    Map<RuntimeSchedulingGroup, List<DriverSubmission>> grouped = new LinkedHashMap<>();
    Map<RuntimeSchedulingGroup, Long> bases = new LinkedHashMap<>();
    try {
      for (Map.Entry<FragmentInstanceId, List<DriverSubmission>> fi : byFi.entrySet()) {
        FragmentInstanceContext context =
            fi.getValue().get(0).driver().getDriverContext().getFragmentInstanceContext();
        RuntimeSchedulingGroup group =
            openGroup(
                fi.getKey(),
                context,
                request.originalAbsoluteDeadlineMillis(),
                request.sessionInfo(),
                () -> {},
                ignored -> {},
                false);
        grouped.put(group, fi.getValue());
        long graphBytes = 0;
        for (DriverSubmission submission : fi.getValue()) {
          graphBytes = Math.max(graphBytes, submission.driver().getEstimatedMemorySize());
        }
        bases.put(group, graphBytes);
      }
      activateRuntimeBatch(prepareBatch(grouped, bases));
    } catch (RuntimeException | Error | CpuNotEnoughException e) {
      for (RuntimeSchedulingGroup group : grouped.keySet()) {
        stopRuntimeGroup(group, e);
      }
      throw e;
    }
  }

  private ThrottleQuotaLimit quotaFor(RuntimeSchedulingGroup group) {
    if (group.quotaUser == null || IoTDBConstant.PATH_ROOT.equals(group.quotaUser)) {
      return null;
    }
    if (injectedQuota != null) {
      return injectedQuota;
    }
    return config.isQuotaEnable()
        ? DataNodeThrottleQuotaManager.getInstance().getThrottleQuotaLimit()
        : null;
  }

  private PreparedDriverBatch prepareBatch(
      Map<RuntimeSchedulingGroup, List<DriverSubmission>> grouped,
      Map<RuntimeSchedulingGroup, Long> bases)
      throws CpuNotEnoughException, MemoryNotEnoughException {
    PreparedDriverBatch batch = new PreparedDriverBatch(this);
    AdmissionTicket parent = null;
    List<AdmissionTicket> notTransferred = new ArrayList<>();
    List<RuntimeSchedulingGroup> admissionGroups = new ArrayList<>();
    ThrottleQuotaLimit quota = null;
    String user = null;
    long memory = 0;
    try {
      Set<DriverTaskId> ids = new HashSet<>();
      for (Map.Entry<RuntimeSchedulingGroup, List<DriverSubmission>> part : grouped.entrySet()) {
        RuntimeSchedulingGroup group = part.getKey();
        requireOwned(group);
        long base = bases.get(group);
        if (base < 0) {
          throw new IllegalArgumentException();
        }
        memory = Math.addExact(memory, base);
        ThrottleQuotaLimit selected = quotaFor(group);
        String selectedUser = selected == null ? null : group.quotaUser;
        if (!batch.members.isEmpty()
            && (!Objects.equals(user, selectedUser) || quota != selected)) {
          throw new IllegalArgumentException();
        }
        quota = selected;
        user = selectedUser;
        for (DriverSubmission submission : part.getValue()) {
          IDriver driver = submission.driver();
          DriverTaskId id = driver.getDriverTaskId();
          if (!id.getFragmentInstanceId().equals(group.id)
              || !ids.add(id)
              || driver.getDriverContext().getFragmentInstanceContext() != group.context
              || ((group.runtime || quota != null) && !driver.supportsQuiescence())) {
            throw new IllegalArgumentException();
          }
          if (submission.role() == DriverSubmission.Role.SCAN_WORKER
              && !(driver instanceof RuntimeScanWorker)) {
            throw new IllegalArgumentException();
          }
          memory = Math.addExact(memory, submission.memberMemoryQuotaBytes());
          DriverTask task =
              DriverTask.withAbsoluteDeadline(
                  driver,
                  group.deadline,
                  DriverTaskStatus.PENDING,
                  group.handle,
                  driver.getEstimatedMemorySize(),
                  driver.isHighestPriority());
          task.setRunnableGateOpen(false);
          batch.members.add(new TaskRegistration(group, batch, submission, task));
        }
      }
      synchronized (registrationLock) {
        for (RuntimeSchedulingGroup group : grouped.keySet()) {
          requireLive(group);
          if (bases.get(group) != 0 && group.baseMemoryClaimed) {
            throw new IllegalStateException();
          }
        }
        Map<RuntimeSchedulingGroup, java.util.Set<DriverTaskId>> directIds = new LinkedHashMap<>();
        Map<RuntimeSchedulingGroup, Integer> workerHighWater = new LinkedHashMap<>();
        for (RuntimeSchedulingGroup group : grouped.keySet()) {
          directIds.put(group, new HashSet<>(group.initialDirectIds));
          workerHighWater.put(group, group.workerIdHighWater);
        }
        for (TaskRegistration member : batch.members) {
          RuntimeSchedulingGroup group = member.group;
          if (group.members.containsKey(member.getDriverTaskId())) {
            throw new IllegalArgumentException();
          }
          if (group.runtime && member.submission.role() != DriverSubmission.Role.SCAN_WORKER) {
            if (group.initialDirectIdsSealed
                || !directIds.get(group).add(member.getDriverTaskId())) {
              throw new IllegalArgumentException();
            }
          }
        }
        for (TaskRegistration member : batch.members) {
          RuntimeSchedulingGroup group = member.group;
          if (group.runtime && member.submission.role() == DriverSubmission.Role.SCAN_WORKER) {
            int pipeline = member.getDriverTaskId().getPipelineId();
            if (pipeline <= workerHighWater.get(group)
                || directIds.get(group).contains(member.getDriverTaskId())) {
              throw new IllegalArgumentException();
            }
            workerHighWater.put(group, pipeline);
          }
        }
        for (RuntimeSchedulingGroup group : grouped.keySet()) {
          if (bases.get(group) != 0) {
            group.baseMemoryClaimed = true;
          }
          if (group.runtime) {
            group.initialDirectIds.addAll(directIds.get(group));
            group.initialDirectIdsSealed = true;
            group.workerIdHighWater = workerHighWater.get(group);
          }
          group.inFlightAdmissions++;
          admissionGroups.add(group);
        }
        for (TaskRegistration member : batch.members) {
          member.registered = true;
          member.group.outstandingExecutionProofs++;
          member.group.members.put(member.getDriverTaskId(), member);
          registrations.put(member.task, member);
          registerTaskToQueryMap(member.group.id.getQueryId(), member.task);
        }
      }
      if (quota != null) {
        parent = quota.tryReserve(user, batch.members.size(), memory);
        if (parent == null) {
          throw new QuotaDeniedException(batch);
        }
        for (TaskRegistration member : batch.members) {
          member.cpuTicket = Objects.requireNonNull(parent.split(1, 0));
          if (member.submission.memberMemoryQuotaBytes() > 0) {
            member.memoryTicket =
                Objects.requireNonNull(parent.split(0, member.submission.memberMemoryQuotaBytes()));
          }
        }
        for (RuntimeSchedulingGroup group : grouped.keySet()) {
          long base = bases.get(group);
          if (base != 0) {
            AdmissionTicket lease = Objects.requireNonNull(parent.split(0, base));
            notTransferred.add(lease);
            if (group.context == null || !group.context.registerQuotaMemoryLease(lease)) {
              throw new IllegalStateException();
            }
            notTransferred.remove(lease);
          }
        }
      }
      for (TaskRegistration member : batch.members) {
        synchronized (registrationLock) {
          member.admissionComplete = true;
        }
        trySettle(member);
        ListenableFuture<?> dependency = member.submission.originalDependency();
        dependency.addListener(() -> dependencyCompleted(member), MoreExecutors.directExecutor());
      }
      synchronized (registrationLock) {
        for (RuntimeSchedulingGroup group : grouped.keySet()) {
          requireLive(group);
        }
      }
      return batch;
    } catch (RuntimeException | Error | CpuNotEnoughException e) {
      rollbackBatch(batch, e);
      throw e;
    } finally {
      if (parent != null) {
        parent.close();
      }
      for (AdmissionTicket lease : notTransferred) {
        lease.close();
      }
      for (TaskRegistration member : batch.members) {
        synchronized (registrationLock) {
          member.admissionComplete = true;
        }
        trySettle(member);
      }
      for (RuntimeSchedulingGroup group : admissionGroups) {
        synchronized (registrationLock) {
          group.inFlightAdmissions--;
        }
        completeGroupIfRemoved(group);
      }
    }
  }

  private void dependencyCompleted(TaskRegistration member) {
    try {
      Futures.getDone(member.submission.originalDependency());
      member.dependencyReady = true;
      refreshGate(member);
    } catch (Throwable failure) {
      Throwable actual =
          failure instanceof ExecutionException && failure.getCause() != null
              ? failure.getCause()
              : failure;
      reportFailure(member, actual);
      stopRuntimeGroup(member.group, actual);
    }
  }

  @Override
  public void activateRuntimeBatch(PreparedDriverBatch batch) {
    if (batch.owner != this) {
      throw new IllegalArgumentException();
    }
    try {
      synchronized (registrationLock) {
        if (batch.active) {
          return;
        }
        if (batch.rolledBack || batch.publishing) {
          throw new IllegalStateException();
        }
        for (TaskRegistration member : batch.members) {
          requireLive(member.group);
        }
        batch.publishing = true;
      }
    } catch (RuntimeException | Error e) {
      failActivation(batch, e);
      throw e;
    }
    try {
      for (TaskRegistration member : batch.members) {
        member.task.lock();
        try {
          if (member.task.getStatus() != DriverTaskStatus.PENDING || member.group.stopped) {
            throw new IllegalStateException();
          }
          member.task.setStatus(DriverTaskStatus.READY);
          readyQueue.push(member.task);
          member.task.setLastEnterReadyQueueTime(System.nanoTime());
        } finally {
          member.task.unlock();
        }
      }
      synchronized (registrationLock) {
        for (TaskRegistration member : batch.members) {
          requireLive(member.group);
        }
        if (batch.rolledBack) {
          throw new IllegalStateException();
        }
        batch.active = true;
      }
      try {
        batch.activation.set(null);
      } finally {
        for (TaskRegistration member : batch.members) {
          refreshGate(member);
        }
      }
    } catch (RuntimeException | Error e) {
      failActivation(batch, e);
      throw e;
    }
  }

  private void failActivation(PreparedDriverBatch batch, Throwable failure) {
    try {
      if (!batch.active) {
        rollbackBatch(batch, failure);
      }
    } finally {
      for (TaskRegistration member : batch.members) {
        stopRuntimeGroup(member.group, failure);
      }
    }
  }

  private void refreshGate(TaskRegistration member) {
    boolean open;
    synchronized (registrationLock) {
      open =
          member.batch.active
              && !member.batch.rolledBack
              && !member.group.stopped
              && member.dependencyReady
              && !member.removing;
      member.task.setRunnableGateOpen(open);
    }
    if (open) {
      scheduler.blockedToReady(member.task);
    }
  }

  void rollbackBatch(PreparedDriverBatch batch, Throwable cause) {
    if (batch.owner != this) {
      throw new IllegalArgumentException();
    }
    synchronized (registrationLock) {
      if (batch.active) {
        throw new IllegalStateException();
      }
      batch.rolledBack = true;
    }
    try {
      batch.activation.setException(cause == null ? new IllegalStateException() : cause);
    } finally {
      for (TaskRegistration member : batch.members) {
        removeMember(member, null);
      }
    }
  }

  @Override
  public ListenableFuture<Void> retireRuntimeWorker(
      RuntimeSchedulingGroup group, TaskRegistration member) {
    requireOwned(group);
    if (member.group != group || member.submission.role() != DriverSubmission.Role.SCAN_WORKER) {
      throw new IllegalArgumentException();
    }
    removeMember(member, null);
    return member.getRemovedFuture();
  }

  @Override
  public ListenableFuture<Void> stopRuntimeGroup(RuntimeSchedulingGroup group, Throwable cause) {
    return stopRuntimeGroup(group, cause, null);
  }

  private ListenableFuture<Void> stopRuntimeGroup(
      RuntimeSchedulingGroup group, Throwable cause, String defaultDriverAbortReason) {
    requireOwned(group);
    List<TaskRegistration> snapshot;
    synchronized (registrationLock) {
      if (group.stopped) {
        return group.getStoppedAndRemovedFuture();
      }
      group.stopped = true;
      group.aborted = cause != null;
      snapshot = new ArrayList<>(group.members.values());
    }
    try {
      group.stopAndWake.run();
    } catch (Throwable failure) {
      if (cause == null) {
        cause = failure;
      }
    }
    if (cause != null) {
      try {
        group.failFragment.accept(cause);
      } catch (Throwable failure) {
        logger.warn(DataNodeQueryMessages.CLEAR_DRIVERTASK_FAILED, failure);
      }
    }
    for (TaskRegistration member : snapshot) {
      Throwable memberCause =
          defaultDriverAbortReason == null
              ? cause
              : new DriverTaskAbortedException(
                  member.getDriverTaskId().getFullId(), defaultDriverAbortReason);
      removeMember(member, memberCause);
    }
    completeGroupIfRemoved(group);
    return group.getStoppedAndRemovedFuture();
  }

  private void requireOwned(RuntimeSchedulingGroup group) {
    if (Objects.requireNonNull(group).owner != this) {
      throw new IllegalArgumentException();
    }
  }

  private void requireLive(RuntimeSchedulingGroup group) {
    requireOwned(group);
    if (group.stopped
        || schedulingGroups.get(group.id) != group
        || group.deadline <= System.currentTimeMillis()) {
      throw new IllegalStateException();
    }
  }

  private TaskRegistration findRegistration(DriverTask task) {
    synchronized (registrationLock) {
      return registrations.get(task);
    }
  }

  private boolean beginSchedulerAction(TaskRegistration member) {
    synchronized (registrationLock) {
      if (member.removing) {
        return false;
      }
      member.schedulerActions++;
      return true;
    }
  }

  private void endSchedulerAction(TaskRegistration member) {
    boolean acknowledge;
    synchronized (registrationLock) {
      member.schedulerActions--;
      acknowledge = member.schedulerActions == 0 && member.driverStopped;
    }
    if (acknowledge) {
      acknowledgeStopped(member, null);
    }
  }

  private void reportFailure(TaskRegistration member, Throwable failure) {
    try {
      member.failure.setException(failure);
    } catch (Throwable notificationFailure) {
      logger.warn(DataNodeQueryMessages.CLEAR_DRIVERTASK_FAILED, notificationFailure);
    }
  }

  private void acknowledgeStopped(TaskRegistration member, Throwable failure) {
    if (failure == null) {
      synchronized (registrationLock) {
        member.driverStopped = true;
        if (member.schedulerActions != 0) {
          return;
        }
        if (member.registered && !member.stoppedAccounted) {
          member.stoppedAccounted = true;
          member.group.outstandingExecutionProofs--;
          if (member.removed.isDone()) {
            try {
              Futures.getDone(member.removed);
              member.group.members.remove(member.getDriverTaskId(), member);
            } catch (ExecutionException | CancellationException missingProof) {
              /* Retain the owner. */
            }
          }
        }
      }
      try {
        member.stopped.set(null);
      } catch (Throwable notificationFailure) {
        reportFailure(member, notificationFailure);
      }
    } else {
      reportFailure(member, failure);
      try {
        member.group.stoppedAndRemoved.setException(failure);
      } catch (Throwable notificationFailure) {
        reportFailure(member, notificationFailure);
      }
      try {
        member.stopped.setException(failure);
      } catch (Throwable notificationFailure) {
        reportFailure(member, notificationFailure);
      }
      if (!member.group.stopped) {
        stopRuntimeGroup(member.group, failure);
      }
    }
    trySettle(member);
    if (member.registered) {
      completeGroupIfRemoved(member.group);
    }
  }

  private void removeMember(TaskRegistration member, Throwable cause) {
    synchronized (registrationLock) {
      if (member.removing) {
        return;
      }
      member.removing = true;
      member.task.setRunnableGateOpen(false);
    }
    if (!member.registered) {
      try {
        member.removed.set(null);
      } finally {
        acknowledgeStopped(member, null);
      }
      return;
    }
    try {
      unlinkTask(member.task, cause);
      synchronized (registrationLock) {
        registrations.remove(member.task);
      }
    } catch (Throwable failure) {
      reportFailure(member, failure);
      try {
        member.group.stoppedAndRemoved.setException(failure);
      } catch (Throwable notificationFailure) {
        reportFailure(member, notificationFailure);
      }
      if (!member.group.stopped) {
        stopRuntimeGroup(member.group, failure);
      }
      return;
    }
    try {
      member.removed.set(null);
    } catch (Throwable notificationFailure) {
      reportFailure(member, notificationFailure);
    }
    if (member.getDriver() instanceof RuntimeScanWorker) {
      try {
        ((RuntimeScanWorker) member.getDriver()).acknowledgeSchedulerRemoval();
      } catch (Throwable notificationFailure) {
        reportFailure(member, notificationFailure);
      }
    }
    try {
      if (member.getDriver().supportsQuiescence()) {
        ListenableFuture<Void> stopped =
            Objects.requireNonNull(member.getDriver().requestQuiescence());
        stopped.addListener(
            () -> {
              Throwable failure = null;
              try {
                Futures.getDone(stopped);
              } catch (Throwable t) {
                failure =
                    t instanceof ExecutionException && t.getCause() != null ? t.getCause() : t;
              }
              acknowledgeStopped(member, failure);
            },
            MoreExecutors.directExecutor());
      } else if (!member.everStarted) {
        acknowledgeStopped(member, null);
      } else {
        acknowledgeStopped(member, new UnsupportedOperationException());
      }
    } catch (Throwable failure) {
      acknowledgeStopped(member, failure);
    }
    if (cause != null) {
      try {
        member.getDriver().failed(cause);
      } catch (Throwable failure) {
        reportFailure(member, failure);
      }
    }
    if (!member.group.runtime) {
      synchronized (registrationLock) {
        if (member.group.members.isEmpty()) {
          member.group.stopped = true;
        }
      }
    }
    completeGroupIfRemoved(member.group);
  }

  private void trySettle(TaskRegistration member) {
    synchronized (registrationLock) {
      if (!member.admissionComplete
          || member.quotaReleased
          || !member.removed.isDone()
          || !member.stopped.isDone()) {
        return;
      }
      try {
        Futures.getDone(member.removed);
        Futures.getDone(member.stopped);
      } catch (ExecutionException | CancellationException failure) {
        return;
      }
      member.quotaReleased = true;
    }
    try {
      if (member.cpuTicket != null) {
        member.cpuTicket.close();
      }
      if (member.memoryTicket != null) {
        member.memoryTicket.close();
      }
      member.quotaSettled.set(null);
    } catch (Throwable failure) {
      reportFailure(member, failure);
      try {
        member.quotaSettled.setException(failure);
      } catch (Throwable notificationFailure) {
        reportFailure(member, notificationFailure);
      }
    }
  }

  private void completeGroupIfRemoved(RuntimeSchedulingGroup group) {
    boolean complete;
    boolean allStopped;
    boolean deregister = false;
    synchronized (registrationLock) {
      complete = group.stopped;
      if (complete) {
        for (TaskRegistration member : group.members.values()) {
          if (!member.removed.isDone()) {
            complete = false;
            break;
          }
          try {
            Futures.getDone(member.removed);
          } catch (ExecutionException | CancellationException missingProof) {
            complete = false;
            break;
          }
        }
      }
      allStopped =
          complete && group.outstandingExecutionProofs == 0 && group.inFlightAdmissions == 0;
      if (allStopped) {
        if (!group.runtime && group.aborted && !group.legacyDeregisterClaimed) {
          group.legacyDeregisterClaimed = true;
          deregister = true;
        }
        schedulingGroups.remove(group.id, group);
        boolean liveQuery =
            schedulingGroups.values().stream()
                .anyMatch(other -> other.id.getQueryId().equals(group.id.getQueryId()));
        if (!liveQuery) {
          DriverTask anchor = deadlineAnchors.remove(group.id.getQueryId());
          if (anchor != null) {
            anchor.setStatus(DriverTaskStatus.FINISHED);
            timeoutQueue.remove(anchor.getDriverTaskId());
          }
        }
      }
    }
    if (deregister) {
      try {
        blockManager.forceDeregisterFragmentInstance(
            new TFragmentInstanceId(
                group.id.getQueryId().getId(),
                group.id.getFragmentId().getId(),
                group.id.getInstanceId()));
      } catch (Throwable failure) {
        logger.warn(DataNodeQueryMessages.CLEAR_DRIVERTASK_FAILED, failure);
      }
    }
    if (complete) {
      try {
        group.removed.set(null);
      } catch (Throwable failure) {
        logger.warn(DataNodeQueryMessages.CLEAR_DRIVERTASK_FAILED, failure);
      }
      if (allStopped) {
        try {
          group.stoppedAndRemoved.set(null);
        } catch (Throwable failure) {
          logger.warn(DataNodeQueryMessages.CLEAR_DRIVERTASK_FAILED, failure);
        }
      }
    }
  }

  private void installDeadline(RuntimeSchedulingGroup group) {
    DriverTask previous = deadlineAnchors.get(group.id.getQueryId());
    if (previous != null && previous.getDDL() <= group.deadline) {
      return;
    }
    DriverTask anchor =
        DriverTask.withAbsoluteDeadline(
            new DeadlineDriver(group.id),
            group.deadline,
            DriverTaskStatus.READY,
            group.handle,
            0,
            false);
    if (previous != null) {
      previous.setStatus(DriverTaskStatus.FINISHED);
      timeoutQueue.remove(previous.getDriverTaskId());
    }
    timeoutQueue.push(anchor);
    deadlineAnchors.put(group.id.getQueryId(), anchor);
  }

  private static final class DeadlineDriver implements IDriver {
    private DriverTaskId id;

    DeadlineDriver(FragmentInstanceId fi) {
      id = new DriverTaskId(fi, Integer.MIN_VALUE);
    }

    public boolean isFinished() {
      return false;
    }

    public ListenableFuture<?> processFor(Duration duration) {
      return Futures.immediateVoidFuture();
    }

    public DriverTaskId getDriverTaskId() {
      return id;
    }

    public void setDriverTaskId(DriverTaskId value) {
      id = value;
    }

    public void close() {}

    public void failed(Throwable cause) {}

    public ISink getSink() {
      return null;
    }

    public DriverContext getDriverContext() {
      return null;
    }

    public boolean isHighestPriority() {
      return false;
    }

    public void setHighestPriority(boolean value) {}
  }

  public void registerTaskToQueryMap(QueryId queryId, DriverTask driverTask) {
    // If query has not been registered by other fragment instances,
    // add the first task as timeout checking task to timeoutQueue.
    queryMap
        .computeIfAbsent(queryId, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(
            driverTask.getDriverTaskId().getFragmentInstanceId(),
            v -> Collections.synchronizedSet(new HashSet<>()))
        .add(driverTask);
  }

  public void submitTaskToReadyQueue(DriverTask task) {
    task.lock();
    try {
      if (task.getStatus() != DriverTaskStatus.READY) {
        return;
      }
      readyQueue.push(task);
      task.setLastEnterReadyQueueTime(System.nanoTime());
    } finally {
      task.unlock();
    }
  }

  @Override
  public void abortQuery(QueryId queryId) {
    abortQuery(queryId, null, null);
  }

  /**
   * Preserve an observed deadline cause for the complete query. Claim a deadline anchor and capture
   * its exact FI/task generations in one lock section; a replaced anchor cannot abort a later
   * generation. User/legacy aborts retain their per-driver reasons.
   */
  private void abortQuery(QueryId queryId, Throwable cause, DriverTask expectedDeadlineAnchor) {
    List<RuntimeSchedulingGroup> groups;
    List<DriverTask> unmanaged = new ArrayList<>();
    synchronized (registrationLock) {
      if (expectedDeadlineAnchor != null
          && deadlineAnchors.get(queryId) != expectedDeadlineAnchor) {
        return;
      }
      groups =
          schedulingGroups.values().stream()
              .filter(group -> group.id.getQueryId().equals(queryId))
              .collect(java.util.stream.Collectors.toList());
      Map<FragmentInstanceId, Set<DriverTask>> leftovers = queryMap.get(queryId);
      if (leftovers != null) {
        for (Set<DriverTask> tasks : leftovers.values()) {
          synchronized (tasks) {
            for (DriverTask task : tasks) {
              if (!registrations.containsKey(task)) unmanaged.add(task);
            }
          }
        }
      }
    }
    for (RuntimeSchedulingGroup group : groups) {
      stopRuntimeGroup(
          group,
          cause == null
              ? new DriverTaskAbortedException(
                  group.id.getFullId(), DriverTaskAbortedException.BY_QUERY_CASCADING_ABORTED)
              : cause,
          cause == null ? DriverTaskAbortedException.BY_QUERY_CASCADING_ABORTED : null);
    }
    for (DriverTask task : unmanaged) {
      task.setAbortCause(
          cause == null
              ? new DriverTaskAbortedException(
                  task.getDriverTaskId().getFullId(),
                  DriverTaskAbortedException.BY_QUERY_CASCADING_ABORTED)
              : cause);
      clearUnmanagedTask(task, true);
    }
  }

  @Override
  public void abortFragmentInstance(FragmentInstanceId id, Throwable cause) {
    RuntimeSchedulingGroup group;
    synchronized (registrationLock) {
      group = schedulingGroups.get(id);
    }
    if (group != null) {
      stopRuntimeGroup(
          group,
          cause == null
              ? new DriverTaskAbortedException(
                  id.getFullId(), DriverTaskAbortedException.BY_FRAGMENT_ABORT_CALLED)
              : cause,
          cause == null ? DriverTaskAbortedException.BY_FRAGMENT_ABORT_CALLED : null);
    } else {
      Map<FragmentInstanceId, Set<DriverTask>> query = queryMap.get(id.getQueryId());
      Set<DriverTask> tasks = query == null ? null : query.get(id);
      if (tasks != null) {
        List<DriverTask> snapshot;
        synchronized (tasks) {
          snapshot = new ArrayList<>(tasks);
        }
        for (DriverTask task : snapshot) {
          task.setAbortCause(
              cause == null
                  ? new DriverTaskAbortedException(
                      task.getDriverTaskId().getFullId(),
                      DriverTaskAbortedException.BY_FRAGMENT_ABORT_CALLED)
                  : cause);
          clearUnmanagedTask(task, true);
        }
      }
    }
  }

  private void clearDriverTask(DriverTask task) {
    TaskRegistration member = findRegistration(task);
    if (member != null) {
      removeMember(member, task.getAbortCause().orElse(null));
      return;
    }
    clearUnmanagedTask(task, task.getStatus() != DriverTaskStatus.FINISHED);
  }

  private void clearUnmanagedTask(DriverTask task, boolean abort) {
    if (task.getStatus() == DriverTaskStatus.ABORTED) {
      return;
    }
    Throwable cause = task.getAbortCause().orElse(null);
    unlinkTask(task, cause, abort);
    timeoutQueue.remove(task.getDriverTaskId());
    if (cause != null) {
      task.getDriver().failed(cause);
    }
    if (abort) {
      blockManager.forceDeregisterFragmentInstance(
          new TFragmentInstanceId(
              task.getDriverTaskId().getQueryId().getId(),
              task.getDriverTaskId().getFragmentId().getId(),
              task.getDriverTaskId().getFragmentInstanceId().getInstanceId()));
    }
  }

  private void unlinkTask(DriverTask task, Throwable cause) {
    unlinkTask(task, cause, cause != null);
  }

  private void unlinkTask(DriverTask task, Throwable cause, boolean abort) {
    task.lock();
    try {
      DriverTaskStatus status = task.getStatus();
      task.setStatus(abort ? DriverTaskStatus.ABORTED : DriverTaskStatus.FINISHED);
      task.setRunnableGateOpen(false);
      if (cause != null) {
        task.setAbortCause(cause);
      }
      if (status == DriverTaskStatus.READY) {
        readyQueue.remove(task.getDriverTaskId());
      }
      blockedTasks.remove(task);
      readyQueue.decreaseReservedSize(task);
    } finally {
      task.unlock();
    }
    synchronized (registrationLock) {
      Map<FragmentInstanceId, Set<DriverTask>> query =
          queryMap.get(task.getDriverTaskId().getQueryId());
      if (query != null) {
        Set<DriverTask> fi = query.get(task.getDriverTaskId().getFragmentInstanceId());
        if (fi != null) {
          fi.remove(task);
          if (fi.isEmpty()) {
            query.remove(task.getDriverTaskId().getFragmentInstanceId(), fi);
          }
        }
        if (query.isEmpty()) {
          queryMap.remove(task.getDriverTaskId().getQueryId(), query);
        }
      }
    }
  }

  private int getNextDriverTaskHandleId() {
    return nextDriverTaskHandleId.getAndIncrement();
  }

  ITaskScheduler getScheduler() {
    return scheduler;
  }

  @Override
  public IDriverScheduler.RuntimeMetrics getRuntimeMetrics() {
    long active = 0;
    for (Map<FragmentInstanceId, Set<DriverTask>> instances : queryMap.values()) {
      for (Set<DriverTask> tasks : instances.values()) {
        synchronized (tasks) {
          for (DriverTask task : tasks) {
            if (task.getStatus() == DriverTaskStatus.RUNNING) active++;
          }
        }
      }
    }
    return new IDriverScheduler.RuntimeMetrics(
        active, readyQueue.size(), blockedTasks.size(), completedDriverTaskCount.get());
  }

  private record PerQueryPressure(RunnableTaskCounts tasks, int runtimeWorkerFragments) {}

  private record PressureFrame(
      long started,
      long finished,
      boolean complete,
      int visitedTasks,
      RunnableTaskCounts totals,
      Map<QueryId, PerQueryPressure> byQuery) {
    ControlPressure forQuery(QueryId query, int configuredThreads) {
      PerQueryPressure own = byQuery.get(query);
      RunnableTaskCounts ownTasks =
          own == null && complete
              ? new RunnableTaskCounts(0, 0, 0)
              : own == null ? null : own.tasks();
      RunnableTaskCounts other =
          complete
              ? new RunnableTaskCounts(
                  totals.running() - ownTasks.running(),
                  totals.runnableReady() - ownTasks.runnableReady(),
                  totals.blocked() - ownTasks.blocked())
              : null;
      return new ControlPressure(
          new RuntimePressure(
              started,
              finished,
              configuredThreads,
              complete,
              own != null,
              visitedTasks,
              ownTasks,
              other),
          own == null ? 0 : own.runtimeWorkerFragments());
    }
  }

  @Override
  public Optional<RuntimePressure> observeRuntimePressure(QueryId ownQuery) {
    Objects.requireNonNull(ownQuery);
    return Optional.of(capturePressureFrame().forQuery(ownQuery, WORKER_THREAD_NUM).sample());
  }

  @Override
  public Optional<ControlPressure> readControlPressure(QueryId ownQuery) {
    Objects.requireNonNull(ownQuery);
    synchronized (pressureSampleLock) {
      if (lastControlPressure == null
          || System.nanoTime() - lastControlPressure.finished()
              >= CONTROL_PRESSURE_INTERVAL_NANOS) {
        lastControlPressure = capturePressureFrame();
      }
      return Optional.of(lastControlPressure.forQuery(ownQuery, WORKER_THREAD_NUM));
    }
  }

  private PressureFrame capturePressureFrame() {
    long started = System.nanoTime();
    int remaining = RUNTIME_PRESSURE_WALK_BUDGET;
    int visitedTasks = 0;
    long running = 0, ready = 0, blocked = 0;
    boolean complete = true;
    Map<QueryId, PerQueryPressure> countsByQuery = new HashMap<>();
    // No task/registration locks under task-set monitors. Volatile status and gate reads are
    // best effort within the bounded sample; they do not represent an atomic cluster state.
    scan:
    for (Map.Entry<QueryId, Map<FragmentInstanceId, Set<DriverTask>>> query : queryMap.entrySet()) {
      if (--remaining < 0) {
        complete = false;
        break;
      }
      long qr = 0, qReady = 0, qb = 0;
      int runtimeFragments = 0;
      for (Set<DriverTask> tasks : query.getValue().values()) {
        if (--remaining < 0) {
          complete = false;
          break scan;
        }
        boolean runtime = false;
        synchronized (tasks) {
          for (DriverTask task : tasks) {
            if (--remaining < 0) {
              complete = false;
              break scan;
            }
            visitedTasks++;
            DriverTaskStatus status = task.getStatus();
            if (status == DriverTaskStatus.RUNNING) {
              qr++;
            } else if (status == DriverTaskStatus.READY && task.isRunnableGateOpen()) {
              qReady++;
            } else if (status == DriverTaskStatus.BLOCKED) {
              qb++;
            }
            runtime |=
                (status == DriverTaskStatus.READY
                        || status == DriverTaskStatus.RUNNING
                        || status == DriverTaskStatus.BLOCKED)
                    && task.getDriver() instanceof RuntimeScanWorker;
          }
        }
        if (runtime) runtimeFragments++;
      }
      running += qr;
      ready += qReady;
      blocked += qb;
      countsByQuery.put(
          query.getKey(),
          new PerQueryPressure(new RunnableTaskCounts(qr, qReady, qb), runtimeFragments));
    }
    return new PressureFrame(
        started,
        System.nanoTime(),
        complete,
        visitedTasks,
        new RunnableTaskCounts(running, ready, blocked),
        complete ? Map.copyOf(countsByQuery) : Map.of());
  }

  public long getReadyQueueTaskCount() {
    return readyQueue.size();
  }

  public long getReadyQueueReservedTaskCount() {
    return readyQueue.getReservedSize();
  }

  public long getBlockQueueTaskCount() {
    return blockedTasks.size();
  }

  public long getTimeoutQueueTaskCount() {
    return timeoutQueue.size();
  }

  public int getQueryMapSize() {
    return queryMap.size();
  }

  @TestOnly
  public IndexedBlockingQueue<DriverTask> getReadyQueue() {
    return readyQueue;
  }

  @TestOnly
  IndexedBlockingQueue<DriverTask> getTimeoutQueue() {
    return timeoutQueue;
  }

  @TestOnly
  Set<DriverTask> getBlockedTasks() {
    return blockedTasks;
  }

  @TestOnly
  Map<QueryId, Map<FragmentInstanceId, Set<DriverTask>>> getQueryMap() {
    return queryMap;
  }

  @TestOnly
  void setBlockManager(IMPPDataExchangeManager blockManager) {
    this.blockManager = blockManager;
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final DriverScheduler instance = new DriverScheduler();
  }

  /** the default scheduler implementation. */
  private class Scheduler implements ITaskScheduler {
    @Override
    public void blockedToReady(DriverTask task) {
      TaskRegistration action = findRegistration(task);
      if (action != null && !beginSchedulerAction(action)) {
        return;
      }
      try {
        long queued;
        task.lock();
        try {
          if (task.getStatus() != DriverTaskStatus.BLOCKED || !task.isRunnableGateOpen()) {
            return;
          }
          task.setStatus(DriverTaskStatus.READY);
          long now = System.nanoTime();
          queued = Math.max(0, now - task.getLastEnterBlockQueueTime());
          task.setLastEnterReadyQueueTime(now);
          task.resetLevelScheduledTime();
          readyQueue.repush(task);
          blockedTasks.remove(task);
        } finally {
          task.unlock();
        }
        FragmentInstanceContext context =
            task.getDriver().getDriverContext().getFragmentInstanceContext();
        if (context != null) {
          context.addBlockQueuedTime(queued);
        }
      } finally {
        if (action != null) {
          endSchedulerAction(action);
        }
      }
    }

    @Override
    public boolean readyToRunning(DriverTask task) {
      TaskRegistration member = findRegistration(task);
      if (member != null && !beginSchedulerAction(member)) {
        readyQueue.decreaseReservedSize(task);
        return false;
      }
      try {
        long queued;
        task.lock();
        try {
          if (task.getStatus() != DriverTaskStatus.READY) {
            readyQueue.decreaseReservedSize(task);
            return false;
          }
          if (!task.isRunnableGateOpen()) {
            task.setStatus(DriverTaskStatus.BLOCKED);
            task.setLastEnterBlockQueueTime(System.nanoTime());
            blockedTasks.add(task);
            return false;
          }
          if (member != null) {
            member.everStarted = true;
          }
          task.setStatus(DriverTaskStatus.RUNNING);
          queued = Math.max(0, System.nanoTime() - task.getLastEnterReadyQueueTime());
        } finally {
          task.unlock();
        }
        FragmentInstanceContext context =
            task.getDriver().getDriverContext().getFragmentInstanceContext();
        if (context != null) {
          context.addReadyQueuedTime(queued);
        }
        return true;
      } finally {
        if (member != null) {
          endSchedulerAction(member);
        }
      }
    }

    @Override
    public void runningToReady(DriverTask task, ExecutionContext context) {
      task.lock();
      try {
        if (task.getStatus() != DriverTaskStatus.RUNNING) {
          return;
        }
        task.updateSchedulePriority(context);
        task.setStatus(DriverTaskStatus.READY);
        task.setLastEnterReadyQueueTime(System.nanoTime());
        readyQueue.repush(task);
      } finally {
        task.unlock();
      }
    }

    @Override
    public void runningToBlocked(DriverTask task, ExecutionContext context) {
      task.lock();
      try {
        if (task.getStatus() != DriverTaskStatus.RUNNING) {
          return;
        }
        task.updateSchedulePriority(context);
        task.setStatus(DriverTaskStatus.BLOCKED);
        task.setLastEnterBlockQueueTime(System.nanoTime());
        blockedTasks.add(task);
      } finally {
        task.unlock();
      }
    }

    @Override
    public void runningToFinished(DriverTask task, ExecutionContext context) {
      task.lock();
      try {
        if (task.getStatus() != DriverTaskStatus.RUNNING) {
          return;
        }
        task.updateSchedulePriority(context);
        task.setStatus(DriverTaskStatus.FINISHED);
        completedDriverTaskCount.incrementAndGet();
        readyQueue.decreaseReservedSize(task);
      } finally {
        task.unlock();
      }
      // Wrapping clearDriverTask with task.lock() may lead to deadlock. For example:
      // Thread A locks a task and then try to remove it from a SynchronizedSet(instance related
      // tasks) which will try to get the lock of SynchronizedSet.
      // Thread B locks the SynchronizedSet first and then tries to lock the task.
      clearDriverTask(task);
    }

    @Override
    public void enforceTimeLimit(DriverTask task) {
      timeoutQueue.push(task);
    }

    @Override
    public void toAborted(DriverTask task) {
      if (task.getDriver() instanceof DeadlineDriver) {
        abortQuery(
            task.getDriverTaskId().getQueryId(),
            task.getAbortCause()
                .orElseGet(
                    () ->
                        new DriverTaskAbortedException(
                            task.getDriverTaskId().getFullId(),
                            DriverTaskAbortedException.BY_INTERNAL_ERROR_SCHEDULED)),
            task);
        return;
      }
      TaskRegistration member = findRegistration(task);
      if (member == null) {
        task.lock();
        try {
          if (task.isEndState()) {
            return;
          }
        } finally {
          task.unlock();
        }
      }
      Throwable originalCause = task.getAbortCause().orElse(null);
      if (originalCause != null) {
        // Publish the actual failure to every FI before any generic cascading
        // abort can make a peer's status win over the originating SQL error.
        abortQuery(task.getDriverTaskId().getQueryId(), originalCause, null);
        if (member == null) clearUnmanagedTask(task, true);
        return;
      }
      if (member == null) {
        clearUnmanagedTask(task, true);
      }
      if (member != null) {
        stopRuntimeGroup(
            member.group,
            task.getAbortCause()
                .orElseGet(
                    () ->
                        new DriverTaskAbortedException(
                            task.getDriverTaskId().getFullId(),
                            DriverTaskAbortedException.BY_INTERNAL_ERROR_SCHEDULED)));
      }
      abortQuery(task.getDriverTaskId().getQueryId());
    }
  }
}
