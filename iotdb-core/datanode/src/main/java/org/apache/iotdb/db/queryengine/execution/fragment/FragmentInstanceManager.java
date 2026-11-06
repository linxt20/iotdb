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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.calc.metric.QueryExecutionMetricSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.exception.QueryTimeoutException;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.schedule.DriverScheduler;
import org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTask;
import org.apache.iotdb.db.queryengine.metric.QueryRelatedResourceMetricSet;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.PipelineDriverFactory;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.storageengine.dataregion.IDataRegionForQuery;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.udf.api.exception.UDFException;
import org.apache.iotdb.udf.api.exception.UDFTypeMismatchException;

import io.airlift.units.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.calc.execution.schedule.queue.IndexedBlockingQueue.TOO_MANY_CONCURRENT_QUERIES_ERROR_MSG;
import static org.apache.iotdb.calc.metric.QueryExecutionMetricSet.LOCAL_EXECUTION_PLANNER;
import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceExecution.createFragmentInstanceExecution;
import static org.apache.iotdb.rpc.TSStatusCode.TOO_MANY_CONCURRENT_QUERIES_ERROR;

@SuppressWarnings("squid:S6548")
public class FragmentInstanceManager {

  private static final Logger logger = LoggerFactory.getLogger(FragmentInstanceManager.class);

  // A read-only view of the same identity map for exchange creation guards. Reading it must not
  // initialize this singleton, its schedulers or its management tasks in exchange-only callers.
  private static volatile Map<FragmentInstanceId, FragmentInstanceContext> publishedInstanceContext;
  private static volatile Map<QueryId, DataNodeQueryContext> publishedDataNodeQueryContext;

  private final Map<FragmentInstanceId, FragmentInstanceContext> instanceContext;
  private final Map<FragmentInstanceId, FragmentInstanceExecution> instanceExecution;
  private final Map<QueryId, DataNodeQueryContext> dataNodeQueryContextMap;
  private final LocalExecutionPlanner planner = LocalExecutionPlanner.getInstance();
  private final IDriverScheduler scheduler = DriverScheduler.getInstance();

  private final ScheduledExecutorService instanceManagementExecutor;
  public final ExecutorService instanceNotificationExecutor;

  private final Duration infoCacheTime;

  private final ExecutorService intoOperationExecutor;
  private final ExecutorService modelInferenceExecutor;

  private final MPPDataExchangeManager exchangeManager =
      MPPDataExchangeService.getInstance().getMPPDataExchangeManager();

  private static final QueryExecutionMetricSet QUERY_EXECUTION_METRIC_SET =
      QueryExecutionMetricSet.getInstance();

  public static FragmentInstanceManager getInstance() {
    return FragmentInstanceManager.InstanceHolder.INSTANCE;
  }

  private FragmentInstanceManager() {
    this.instanceContext = new ConcurrentHashMap<>();
    publishedInstanceContext = this.instanceContext;
    this.instanceExecution = new ConcurrentHashMap<>();
    this.dataNodeQueryContextMap = new ConcurrentHashMap<>();
    publishedDataNodeQueryContext = this.dataNodeQueryContextMap;
    this.instanceManagementExecutor =
        IoTDBThreadPoolFactory.newScheduledThreadPool(
            1, ThreadName.FRAGMENT_INSTANCE_MANAGEMENT.getName());
    this.instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            IoTDBDescriptor.getInstance().getConfig().getFragmentInstanceNotificationThreadCount(),
            ThreadName.FRAGMENT_INSTANCE_NOTIFICATION.getName());

    this.infoCacheTime = new Duration(5, TimeUnit.MINUTES);

    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        instanceManagementExecutor, this::removeOldInstances, 2000, 2000, TimeUnit.MILLISECONDS);
    ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
        instanceManagementExecutor,
        this::cancelTimeoutFlushingInstances,
        2000,
        2000,
        TimeUnit.MILLISECONDS);

    this.intoOperationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            IoTDBDescriptor.getInstance().getConfig().getIntoOperationExecutionThreadCount(),
            "into-operation-executor");

    this.modelInferenceExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            CommonDescriptor.getInstance().getConfig().getModelInferenceExecutionThreadCount(),
            "model-inference-executor");
  }

  @SuppressWarnings("squid:S1181")
  public FragmentInstanceInfo execDataQueryFragmentInstance(
      FragmentInstance instance, IDataRegionForQuery dataRegion) {
    long startTime = System.nanoTime();
    FragmentInstanceId instanceId = instance.getId();
    AtomicLong driversCount = new AtomicLong();
    try (SetThreadName fragmentInstanceName = new SetThreadName(instanceId.getFullId())) {
      AtomicReference<FragmentInstanceInfo> failedInstanceInfo = new AtomicReference<>();
      FragmentInstanceExecution execution =
          instanceExecution.computeIfAbsent(
              instanceId,
              id -> {
                FragmentInstanceStateMachine stateMachine =
                    new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);

                boolean[] contextCreated = new boolean[] {false};
                DataNodeQueryContext[] dataNodeQueryContexts = new DataNodeQueryContext[1];
                FragmentInstanceContext context =
                    instanceContext.computeIfAbsent(
                        instanceId,
                        fragmentInstanceId -> {
                          contextCreated[0] = true;
                          // Only ensure the DataNodeQueryContext when we actually create the
                          // FragmentInstanceContext, so the repeated-dispatch path (which rejects
                          // without creating a context) does not leak a context entry.
                          dataNodeQueryContexts[0] =
                              getOrCreateDataNodeQueryContext(
                                  instanceId.getQueryId(), instance.getDataNodeFINum());
                          return createFragmentInstanceContext(
                              fragmentInstanceId,
                              stateMachine,
                              instance.getSessionInfo(),
                              dataRegion,
                              instance.getGlobalTimePredicate(),
                              dataNodeQueryContextMap,
                              DriverTask.computeDeadlineTimeInMs(instance.getTimeOut()),
                              instance.isDebug(),
                              instance.isVerbose());
                        });
                rejectIfRepeatedDispatch(contextCreated[0], instanceId);
                context.setHighestPriority(instance.isHighestPriority());

                List<PipelineDriverFactory> driverFactories = new ArrayList<>();
                List<IDriver> drivers = new ArrayList<>();
                int[] createdFactoryCount = new int[] {0};
                try {
                  driverFactories =
                      planner.plan(
                          instance.getFragment().getPlanNodeTree(),
                          instance.getFragment().getTypeProvider(),
                          context,
                          dataNodeQueryContexts[0]);

                  context.initializeNumOfDrivers(driverFactories.size());
                  createDrivers(driverFactories, drivers, createdFactoryCount, context);
                  // For ShowQueries related instances, isHighestPriority == true
                  if (instance.isHighestPriority()) {
                    drivers.forEach(driver -> driver.setHighestPriority(true));
                  }

                  // get the sink of last driver
                  ISink sink = drivers.get(drivers.size() - 1).getSink();

                  driversCount.addAndGet(drivers.size());
                  FragmentInstanceExecution createdExecution =
                      createFragmentInstanceExecution(
                          scheduler,
                          instanceId,
                          context,
                          drivers,
                          sink,
                          stateMachine,
                          instance.getTimeOut(),
                          instance.isExplainAnalyze(),
                          exchangeManager);
                  context.setPlanningContext(null);
                  return createdExecution;
                } catch (Throwable t) {
                  if ("ADMISSION_REJECTED".equals(context.getAdmissionStatus())
                      && context.getPlanningContext() != null) {
                    try {
                      context.getPlanningContext().finalizeDynamicAdmission();
                    } catch (Throwable receiptFailure) {
                      logger.warn(
                          DataNodeQueryMessages.DYNAMIC_SNAPSHOT_PUBLICATION_FAILED,
                          receiptFailure);
                    }
                  }
                  if (driverFactories.isEmpty() && context.getPlanningContext() != null) {
                    driverFactories.addAll(
                        context.getPlanningContext().getPipelineDriverFactories());
                  }
                  cleanupDriverCreationFailure(
                      driverFactories, drivers, createdFactoryCount[0], context, t);
                  // deal with
                  if (t instanceof IllegalStateException
                      && TOO_MANY_CONCURRENT_QUERIES_ERROR_MSG.equals(t.getMessage())) {
                    logger.warn(TOO_MANY_CONCURRENT_QUERIES_ERROR_MSG);
                    stateMachine.failed(
                        new IoTDBException(
                            TOO_MANY_CONCURRENT_QUERIES_ERROR_MSG,
                            TOO_MANY_CONCURRENT_QUERIES_ERROR.getStatusCode()));
                  } else if (t instanceof IoTDBRuntimeException) {
                    stateMachine.failed(t);
                  } else if (t instanceof UDFTypeMismatchException) {
                    stateMachine.failed(new SemanticException(t.getMessage()));
                  } else if (t instanceof UDFException) {
                    logger.warn(DataNodeQueryMessages.EXCEPTION_HAPPENED_WHEN_EXECUTING_UDTF, t);
                    stateMachine.failed(
                        new IoTDBRuntimeException(
                            t.getMessage(), TSStatusCode.EXECUTE_UDF_ERROR.getStatusCode(), true));
                  } else {
                    logger.warn(
                        DataNodeQueryMessages.ERROR_WHEN_CREATE_FRAGMENTINSTANCEEXECUTION, t);
                    stateMachine.failed(t);
                  }
                  // cancelTask may remove the context from instanceContext while this execution is
                  // still being created. Capture the failure result from the local context before
                  // returning from computeIfAbsent instead of looking it up from the map later.
                  failedInstanceInfo.set(context.getInstanceInfo());
                  clearFIRelatedResources(instanceId);
                  return null;
                }
              });

      if (execution != null) {
        execution
            .getStateMachine()
            .addStateChangeListener(
                newState -> {
                  if (newState.isDone()) {
                    instanceExecution.remove(instanceId);
                  }
                });
        return execution.getInstanceInfo();
      } else {
        return failedInstanceInfo.get();
      }
    } finally {
      QueryRelatedResourceMetricSet.getInstance()
          .updateFragmentInstanceCount(
              instanceContext.size(), instanceExecution.size(), driversCount.get());
      QUERY_EXECUTION_METRIC_SET.recordExecutionCost(
          LOCAL_EXECUTION_PLANNER, System.nanoTime() - startTime);
    }
  }

  /**
   * If {@code instanceContext.computeIfAbsent} returned an existing {@link FragmentInstanceContext}
   * for this {@code instanceId} (i.e. {@code contextCreated} is false), the same FragmentInstance
   * has been dispatched before (e.g. an RPC retry in {@code
   * FragmentInstanceDispatcherImpl#dispatchRemote}). The previous execution may have already
   * released its resources (dataRegion == null), so reusing this cached context would run a fresh
   * driver against a released context and trigger an NPE. Reject the duplicated dispatch with
   * REPEATED_RPC_CALL instead of reusing it.
   *
   * <p>This must be called before the planning try block on purpose, so it propagates up
   * (RegionReadExecutor carries the status code) without touching the first execution's cached
   * resources.
   */
  private static void rejectIfRepeatedDispatch(
      boolean contextCreated, FragmentInstanceId instanceId) {
    if (!contextCreated) {
      throw new IoTDBRuntimeException(
          String.format(
              DataNodeQueryMessages
                  .QUERY_EXCEPTION_REPEATED_RPC_CALL_DETECTED_FOR_FRAGMENTINSTANCE_S_REJECT_BF609A26,
              instanceId.getFullId()),
          TSStatusCode.REPEATED_RPC_CALL.getStatusCode());
    }
  }

  private void createDrivers(
      List<PipelineDriverFactory> factories,
      List<IDriver> drivers,
      int[] createdCount,
      FragmentInstanceContext context) {
    for (PipelineDriverFactory factory : factories) {
      checkDriverCreationAllowed(context);
      drivers.add(factory.createDriver());
      createdCount[0]++;
      checkDriverCreationAllowed(context);
    }
  }

  private void cleanupDriverCreationFailure(
      List<PipelineDriverFactory> driverFactories,
      List<IDriver> drivers,
      int createdFactoryCount,
      FragmentInstanceContext context,
      Throwable primary) {
    for (IDriver driver : drivers) {
      try {
        driver.close();
      } catch (Throwable failure) {
        if (failure != primary) primary.addSuppressed(failure);
      }
    }
    for (int i = createdFactoryCount; i < driverFactories.size(); i++) {
      try {
        driverFactories.get(i).closeUncreated();
      } catch (Throwable failure) {
        if (failure != primary) primary.addSuppressed(failure);
      } finally {
        context.decrementNumOfUnClosedDriver();
      }
    }
    if (context.getPlanningContext() != null) {
      try {
        context.getPlanningContext().getResourceRegistry().abortUntransferred(primary);
      } finally {
        context.setPlanningContext(null);
        context.setDynamicScanAssignments(null);
      }
    }
  }

  private void checkDriverCreationAllowed(FragmentInstanceContext context) {
    if (context.isCancellationRequested() || context.getStateMachine().getState().isDone()) {
      throw new java.util.concurrent.CancellationException(
          DataNodeQueryMessages.EXCEPTION_FRAGMENT_WAS_CANCELLED_DURING_DRIVER_CREATION_D031E9B1);
    }
  }

  private void clearFIRelatedResources(FragmentInstanceId instanceId) {
    // close and remove all the handles of the fragment instance
    exchangeManager.forceDeregisterFragmentInstance(instanceId.toThrift());
    // clear MemoryPool
    exchangeManager.deRegisterFragmentInstanceFromMemoryPool(
        instanceId.getQueryId().getId(), instanceId.getFragmentInstanceId(), false);
  }

  private DataNodeQueryContext getOrCreateDataNodeQueryContext(QueryId queryId, int dataNodeFINum) {
    return dataNodeQueryContextMap.computeIfAbsent(
        queryId, queryId1 -> new DataNodeQueryContext(dataNodeFINum));
  }

  @SuppressWarnings("squid:S1181")
  public FragmentInstanceInfo execSchemaQueryFragmentInstance(
      FragmentInstance instance, ISchemaRegion schemaRegion) {
    FragmentInstanceId instanceId = instance.getId();
    AtomicReference<FragmentInstanceInfo> failedInstanceInfo = new AtomicReference<>();
    FragmentInstanceExecution execution =
        instanceExecution.computeIfAbsent(
            instanceId,
            id -> {
              FragmentInstanceStateMachine stateMachine =
                  new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);

              boolean[] contextCreated = new boolean[] {false};
              FragmentInstanceContext context =
                  instanceContext.computeIfAbsent(
                      instanceId,
                      fragmentInstanceId -> {
                        contextCreated[0] = true;
                        return createFragmentInstanceContext(
                            fragmentInstanceId,
                            stateMachine,
                            instance.getSessionInfo(),
                            instance.isDebug(),
                            instance.isVerbose());
                      });
              rejectIfRepeatedDispatch(contextCreated[0], instanceId);
              context.setHighestPriority(instance.isHighestPriority());

              List<PipelineDriverFactory> driverFactories = new ArrayList<>();
              List<IDriver> drivers = new ArrayList<>();
              int[] createdFactoryCount = new int[] {0};
              try {
                driverFactories =
                    planner.plan(
                        instance.getFragment().getPlanNodeTree(),
                        instance.getFragment().getTypeProvider(),
                        context,
                        schemaRegion);

                context.initializeNumOfDrivers(driverFactories.size());
                createDrivers(driverFactories, drivers, createdFactoryCount, context);
                // get the sink of last driver
                ISink sink = drivers.get(drivers.size() - 1).getSink();

                FragmentInstanceExecution createdExecution =
                    createFragmentInstanceExecution(
                        scheduler,
                        instanceId,
                        context,
                        drivers,
                        sink,
                        stateMachine,
                        instance.getTimeOut(),
                        false,
                        exchangeManager);
                context.setPlanningContext(null);
                return createdExecution;
              } catch (Throwable t) {
                if (driverFactories.isEmpty() && context.getPlanningContext() != null) {
                  driverFactories.addAll(context.getPlanningContext().getPipelineDriverFactories());
                }
                cleanupDriverCreationFailure(
                    driverFactories, drivers, createdFactoryCount[0], context, t);
                // deal with
                if (t instanceof IllegalStateException
                    && TOO_MANY_CONCURRENT_QUERIES_ERROR_MSG.equals(t.getMessage())) {
                  logger.warn(TOO_MANY_CONCURRENT_QUERIES_ERROR_MSG);
                  stateMachine.failed(
                      new IoTDBException(
                          TOO_MANY_CONCURRENT_QUERIES_ERROR_MSG,
                          TOO_MANY_CONCURRENT_QUERIES_ERROR.getStatusCode()));
                } else if (t instanceof IoTDBRuntimeException) {
                  stateMachine.failed(t);
                } else {
                  logger.warn(DataNodeQueryMessages.EXECUTE_ERROR_CAUSED_BY, t);
                  stateMachine.failed(t);
                }
                // See execDataQueryFragmentInstance for why this result must not be fetched from
                // instanceContext after computeIfAbsent returns.
                failedInstanceInfo.set(context.getInstanceInfo());
                clearFIRelatedResources(instanceId);
                return null;
              }
            });
    if (execution != null) {
      execution
          .getStateMachine()
          .addStateChangeListener(
              newState -> {
                if (newState.isDone()) {
                  instanceExecution.remove(instanceId);
                }
              });
      return execution.getInstanceInfo();
    } else {
      return failedInstanceInfo.get();
    }
  }

  /** Aborts a FragmentInstance. keep FragmentInstanceContext for later state tracking */
  public FragmentInstanceInfo abortFragmentInstance(FragmentInstanceId fragmentInstanceId) {
    instanceExecution.remove(fragmentInstanceId);
    FragmentInstanceContext context = instanceContext.get(fragmentInstanceId);
    if (context != null) {
      context.abort();
      return context.getInstanceInfo();
    }
    return null;
  }

  public enum CancellationOrigin {
    LOCAL_CANCEL_TASK,
    RPC_CANCEL_QUERY
  }

  /** Cancels a FragmentInstance, including an identity that has not been dispatched yet. */
  public FragmentInstanceInfo cancelTask(FragmentInstanceId instanceId, boolean hasThrowable) {
    return cancelTask(instanceId, hasThrowable, CancellationOrigin.LOCAL_CANCEL_TASK);
  }

  public FragmentInstanceInfo cancelTask(
      FragmentInstanceId instanceId, boolean hasThrowable, CancellationOrigin origin) {
    logger.debug(DataNodeQueryMessages.CANCEL_FI);
    requireNonNull(instanceId, DataNodeQueryMessages.EXCEPTION_TASKID_IS_NULL_E1221EB2);
    // Use the same identity map as dispatch. A late dispatch sees this cancellation context and
    // the existing repeated-dispatch guard rejects it without planning or creating any drivers.
    FragmentInstanceContext context =
        instanceContext.computeIfAbsent(
            instanceId,
            id ->
                FragmentInstanceContext.createCancellationContext(
                    id,
                    new FragmentInstanceStateMachine(id, instanceNotificationExecutor),
                    hasThrowable));
    context.requestCancellation();
    instanceExecution.remove(instanceId);
    if (hasThrowable) context.cancel();
    else context.finished();
    if (context.wasCancelledBeforeDispatch()) {
      // Tombstone publication precedes the exchange creation lock. That lock both closes existing
      // producer-owned queues and prevents a consumer from creating a queue after this cleanup.
      exchangeManager.cancelUnstartedFragmentInstance(instanceId.toThrift());
      org.apache.iotdb.db.queryengine.execution.exchange.ResourceAuditManager.getInstance()
          .recordCancellationBeforeDispatch(
              instanceId.getQueryId().getId(),
              instanceId.getFullId(),
              origin.name(),
              context.getStateMachine().getState().name());
    }
    return context.getInstanceInfo();
  }

  /**
   * A real query-termination entry point. Planned but undispatched fragments have no data/schema
   * context from which to settle a shared-context slot. Remove the query's map entry here instead
   * of guessing which slots to decrement; existing fragments retain their referenced context.
   */
  public void cancelQuery(
      QueryId queryId,
      List<FragmentInstanceId> unfinishedIds,
      boolean hasThrowable,
      CancellationOrigin origin) {
    requireNonNull(queryId, DataNodeQueryMessages.EXCEPTION_QUERYID_CAN_NOT_BE_NULL_DOT_16639DBE);
    requireNonNull(
        unfinishedIds,
        String.format(
            DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918,
            "unfinishedIds"));
    // Validate the entire request before cancelling anything or entering the query-map cleanup.
    for (FragmentInstanceId id : unfinishedIds) {
      requireNonNull(id, DataNodeQueryMessages.EXCEPTION_INSTANCEID_IS_NULL_343234DC);
      if (!queryId.equals(id.getQueryId())) {
        throw new IllegalArgumentException(
            String.format(
                DataNodeQueryMessages.EXCEPTION_CANCEL_FRAGMENT_QUERY_MISMATCH,
                id.getFullId(),
                queryId.getId()));
      }
    }
    Throwable failure = null;
    try {
      for (FragmentInstanceId id : unfinishedIds) {
        try {
          cancelTask(id, hasThrowable, origin);
        } catch (RuntimeException | Error cancellationFailure) {
          if (failure == null) failure = cancellationFailure;
          else if (failure != cancellationFailure) failure.addSuppressed(cancellationFailure);
        }
      }
    } finally {
      dataNodeQueryContextMap.remove(queryId);
    }
    if (failure instanceof RuntimeException) throw (RuntimeException) failure;
    if (failure instanceof Error) throw (Error) failure;
  }

  /** Lock-free lookup of the same identity map, without initializing the manager singleton. */
  public static boolean isFragmentCancellationRequested(FragmentInstanceId instanceId) {
    Map<FragmentInstanceId, FragmentInstanceContext> contexts = publishedInstanceContext;
    if (contexts == null) return false;
    FragmentInstanceContext context = contexts.get(instanceId);
    return context != null && context.isCancellationRequested();
  }

  /** Existing shared-query metadata identity, without starting the manager singleton. */
  public static boolean hasDataNodeQueryContext(QueryId queryId) {
    Map<QueryId, DataNodeQueryContext> contexts = publishedDataNodeQueryContext;
    return contexts != null && contexts.containsKey(queryId);
  }

  /**
   * Gets the info for the specified fragment instance.
   *
   * <p>NOTE: this design assumes that only fragment instances that will eventually exist are
   * queried.
   */
  public FragmentInstanceInfo getInstanceInfo(FragmentInstanceId instanceId) {
    requireNonNull(instanceId, DataNodeQueryMessages.EXCEPTION_INSTANCEID_IS_NULL_343234DC);
    FragmentInstanceContext context = instanceContext.get(instanceId);
    // Preserve cancellation's existing public lookup behavior while retaining the internal
    // identity guard until the ordinary terminal-context cache expires.
    if (context == null || context.isCancellationRequested()) {
      return null;
    }
    return context.getInstanceInfo();
  }

  public TFetchFragmentInstanceStatisticsResp getFragmentInstanceStatistics(
      FragmentInstanceId instanceId) {
    requireNonNull(instanceId, DataNodeQueryMessages.EXCEPTION_INSTANCEID_IS_NULL_343234DC);
    // If the instance is still running, we directly get the statistics from instanceExecution
    FragmentInstanceExecution fragmentInstanceExecution = instanceExecution.get(instanceId);
    if (fragmentInstanceExecution != null) {
      try {
        fragmentInstanceExecution.lockStatistics();
        if (!fragmentInstanceExecution.isStaticsRemoved()) {
          return fragmentInstanceExecution.buildStatistics();
        }
      } finally {
        fragmentInstanceExecution.unlockStatistics();
      }
    }
    // If the instance has finished, we get the statistics which was cached in the instanceContext
    // when instanceExecution was removed.
    FragmentInstanceContext context = instanceContext.get(instanceId);
    if (context == null) {
      return notFoundStatisticsResponse(instanceId);
    }
    TFetchFragmentInstanceStatisticsResp statisticsResp = context.getFragmentInstanceStatistics();
    return statisticsResp == null ? notFoundStatisticsResponse(instanceId) : statisticsResp;
  }

  private TFetchFragmentInstanceStatisticsResp notFoundStatisticsResponse(
      FragmentInstanceId instanceId) {
    TFetchFragmentInstanceStatisticsResp response = new TFetchFragmentInstanceStatisticsResp();
    response.setStatus(
        new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage(
                String.format(
                    DataNodeQueryMessages.MESSAGE_FRAGMENT_INSTANCE_NOT_FOUND_ARG_A6E7A870,
                    instanceId)));
    response.setFragmentInstanceId(instanceId.toThrift());
    response.addToValidityReasons("NOT_FOUND");
    return response;
  }

  private void removeOldInstances() {
    long oldestAllowedInstance = System.currentTimeMillis() - infoCacheTime.toMillis();
    instanceContext
        .entrySet()
        .removeIf(
            entry -> {
              long endTime = entry.getValue().getEndTime();
              return endTime != -1 && endTime <= oldestAllowedInstance;
            });
  }

  private void cancelTimeoutFlushingInstances() {
    long now = System.currentTimeMillis();
    instanceExecution.forEach(
        (key, execution) -> {
          if (execution.getStateMachine().getState() == FragmentInstanceState.FLUSHING
              && (now - execution.getStartTime()) > execution.getTimeoutInMs()) {
            execution
                .getStateMachine()
                .failed(
                    new QueryTimeoutException(
                        String.format(
                            DataNodeQueryMessages
                                .QUERY_EXCEPTION_QUERY_HAS_EXECUTED_MORE_THAN_SMS_AND_NOW_IS_IN_FLUSHING_4BF7535B,
                            execution.getTimeoutInMs())));
          }
        });
    Coordinator.getInstance().cleanUpStaleQueries();
  }

  public ExecutorService getIntoOperationExecutor() {
    return intoOperationExecutor;
  }

  public ExecutorService getModelInferenceExecutor() {
    return modelInferenceExecutor;
  }

  private static class InstanceHolder {

    private InstanceHolder() {}

    private static final FragmentInstanceManager INSTANCE = new FragmentInstanceManager();
  }
}
