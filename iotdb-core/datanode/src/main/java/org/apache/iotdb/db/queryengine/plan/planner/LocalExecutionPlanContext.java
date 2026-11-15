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
package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.plan.planner.ITableOperatorGeneratorContext;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.calc.plan.planner.memory.PipelineMemoryEstimator;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.plan.analyze.ITableTypeProvider;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.queryengine.execution.exchange.RuntimeExchangeBlockCodec;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue.MemoryReservationKey;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeEndpointFrontier;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation;
import org.apache.iotdb.db.queryengine.plan.analyze.TemplatedInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.memory.CheckedTablePipelineMemoryEstimator;
import org.apache.iotdb.db.queryengine.plan.planner.memory.PipelineMemoryEstimatorFactory;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

// Attention: We should use thread-safe data structure for members that are shared by all pipelines
public class LocalExecutionPlanContext implements ITableOperatorGeneratorContext {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalExecutionPlanContext.class);

  // Save operators in this pipeline, a new one will be created when creating another pipeline
  private final DriverContext driverContext;
  private final AtomicInteger nextOperatorId;
  private final TypeProvider typeProvider;
  private final Map<IDeviceID, Set<String>> allSensorsMap;
  private int degreeOfParallelism =
      IoTDBDescriptor.getInstance().getConfig().getDegreeOfParallelism();
  // this is shared with all subContexts
  private AtomicInteger nextPipelineId;
  private List<PipelineDriverFactory> pipelineDriverFactories;
  private List<ExchangeOperator> exchangeOperatorList = new ArrayList<>();
  private int exchangeSumNum = 0;
  private final long fragmentExchangeMaxBytes;
  private Map<ISink, Long> legacySinkLimits = new IdentityHashMap<>();
  private final LocalExecutionResourceRegistry resourceRegistry;
  private final DynamicScanAssignmentRegistry dynamicScanAssignments;
  private final DynamicParallelPlanningService dynamicParallelPlanningService;
  private final RuntimePlanningState runtimePlanningState;

  public DynamicScanAssignmentRegistry getDynamicScanAssignments() {
    return dynamicScanAssignments;
  }

  public DynamicParallelPlanningService getDynamicParallelPlanningService() {
    return dynamicParallelPlanningService;
  }

  public void finalizeDynamicAdmission() {
    if (!dynamicScanAssignments.isEnabled()) return;
    for (DynamicDecisionReceipt receipt :
        dynamicScanAssignments.finishAdmission(
            getInstanceContext().getAdmissionStatus(),
            getInstanceContext().getAdmissionReason(),
            getInstanceContext().getAdmissionMemoryAccount() == null
                ? null
                : getInstanceContext().getAdmissionMemoryAccount().snapshot())) {
      try {
        DynamicParallelPlanningService.historyStore().publishAdmission(receipt);
      } catch (Throwable failure) {
        LOGGER.warn(DataNodeQueryMessages.DYNAMIC_SNAPSHOT_PUBLICATION_FAILED, failure);
      }
    }
  }

  public void freezeDynamicScanAssignments() {
    java.util.Set<Integer> pipelines = new java.util.HashSet<>();
    pipelines.add(getDriverContext().getDriverTaskID().getPipelineId());
    for (PipelineDriverFactory factory : pipelineDriverFactories)
      pipelines.add(factory.getDriverContext().getDriverTaskID().getPipelineId());
    dynamicScanAssignments.freeze(pipelines);
  }

  private LocalExecutionResourceRegistry.Snapshot frozenResourceSnapshot;

  private List<TSDataType> cachedDataTypes;

  // left is cached last value in last query
  // right is full path and DataType for each cached last value
  private List<Pair<TimeValuePair, Pair<Binary, TSDataType>>> cachedLastValueAndPathList;

  // whether we need to update last cache
  private boolean needUpdateLastCache;
  private boolean needUpdateNullEntry;

  public final DataNodeQueryContext dataNodeQueryContext;

  // null for all time partitions
  // empty for zero time partitions
  // use AtomicReference not for thread-safe, just for updating same field in different pipeline
  private AtomicReference<List<Long>> timePartitions = new AtomicReference<>();

  /** Records the parent of each pipeline. The order of each list does not matter for now. */
  private Map<PlanNodeId, List<PipelineMemoryEstimator>> parentPlanNodeIdToMemoryEstimator =
      new ConcurrentHashMap<>();

  // for data region
  public LocalExecutionPlanContext(
      TypeProvider typeProvider,
      FragmentInstanceContext instanceContext,
      DataNodeQueryContext dataNodeQueryContext) {
    this.fragmentExchangeMaxBytes =
        IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance();
    this.typeProvider = typeProvider;
    this.allSensorsMap = new ConcurrentHashMap<>();
    this.nextOperatorId = new AtomicInteger(0);
    this.nextPipelineId = new AtomicInteger(0);
    this.driverContext = new DataDriverContext(instanceContext, getNextPipelineId());
    this.pipelineDriverFactories = new ArrayList<>();
    this.dataNodeQueryContext = dataNodeQueryContext;
    this.resourceRegistry = new LocalExecutionResourceRegistry();
    this.dynamicScanAssignments =
        new DynamicScanAssignmentRegistry(
            IoTDBDescriptor.getInstance().getConfig().isEnableDynamicScanParallelism());
    this.dynamicParallelPlanningService = new DynamicParallelPlanningService();
    this.runtimePlanningState = new RuntimePlanningState();
    instanceContext.setDynamicScanAssignments(dynamicScanAssignments);
    instanceContext.setPlanningContext(this);
    resourceRegistry.bindResourceAudit(instanceContext.getId());
  }

  // For creating subContext, differ from parent context mainly in driver context
  public LocalExecutionPlanContext(LocalExecutionPlanContext parentContext) {
    this.fragmentExchangeMaxBytes = parentContext.fragmentExchangeMaxBytes;
    this.legacySinkLimits = parentContext.legacySinkLimits;
    this.nextOperatorId = parentContext.nextOperatorId;
    this.typeProvider = parentContext.typeProvider;
    this.allSensorsMap = parentContext.allSensorsMap;
    this.nextPipelineId = parentContext.nextPipelineId;
    this.pipelineDriverFactories = parentContext.pipelineDriverFactories;
    this.degreeOfParallelism = parentContext.degreeOfParallelism;
    this.exchangeSumNum = parentContext.exchangeSumNum;
    this.exchangeOperatorList = parentContext.exchangeOperatorList;
    this.cachedDataTypes = parentContext.cachedDataTypes;
    this.driverContext =
        parentContext.getDriverContext().createSubDriverContext(getNextPipelineId());
    this.dataNodeQueryContext = parentContext.dataNodeQueryContext;
    this.timePartitions = parentContext.timePartitions;
    this.parentPlanNodeIdToMemoryEstimator = parentContext.parentPlanNodeIdToMemoryEstimator;
    this.resourceRegistry = parentContext.resourceRegistry;
    this.dynamicScanAssignments = parentContext.dynamicScanAssignments;
    this.dynamicParallelPlanningService = parentContext.dynamicParallelPlanningService;
    this.runtimePlanningState = parentContext.runtimePlanningState;
  }

  // for schema region
  public LocalExecutionPlanContext(
      final TypeProvider typeProvider,
      final FragmentInstanceContext instanceContext,
      final ISchemaRegion schemaRegion) {
    this.fragmentExchangeMaxBytes =
        IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance();
    this.allSensorsMap = new ConcurrentHashMap<>();
    this.typeProvider = typeProvider;
    this.nextOperatorId = new AtomicInteger(0);
    this.nextPipelineId = new AtomicInteger(0);

    this.driverContext =
        new SchemaDriverContext(instanceContext, schemaRegion, getNextPipelineId());
    this.pipelineDriverFactories = new ArrayList<>();
    this.dataNodeQueryContext = null;
    this.resourceRegistry = new LocalExecutionResourceRegistry();
    this.dynamicScanAssignments =
        new DynamicScanAssignmentRegistry(
            IoTDBDescriptor.getInstance().getConfig().isEnableDynamicScanParallelism());
    this.dynamicParallelPlanningService = new DynamicParallelPlanningService();
    this.runtimePlanningState = new RuntimePlanningState();
    instanceContext.setDynamicScanAssignments(dynamicScanAssignments);
    instanceContext.setPlanningContext(this);
    resourceRegistry.bindResourceAudit(instanceContext.getId());
  }

  public void registerPipelineRoot(Operator operation) {
    java.util.Collection<? extends Operator> sources =
        driverContext instanceof DataDriverContext
            ? ((DataDriverContext) driverContext).getSourceOperators()
            : java.util.Collections.emptyList();
    resourceRegistry.adoptOperatorRoot(operation, sources);
  }

  public void addPipelineDriverFactory(
      Operator operation, DriverContext driverContext, long estimatedMemorySize) {
    addPipelineDriverFactoryAndReturn(operation, driverContext, estimatedMemorySize);
  }

  public PipelineDriverFactory addPipelineDriverFactoryAndReturn(
      Operator operation, DriverContext driverContext, long estimatedMemorySize) {
    registerPipelineRoot(operation);
    PipelineDriverFactory factory =
        new PipelineDriverFactory(operation, driverContext, estimatedMemorySize, resourceRegistry);
    pipelineDriverFactories.add(factory);
    return factory;
  }

  /**
   * Each time we construct a pipeline, we should also construct the memory estimator for the
   * pipeline.
   *
   * @param operation the root operator of the pipeline
   * @param parentPlanNodeId the parent plan node id of the root of the pipeline
   * @param root the root node of the pipeline
   * @param dependencyPipelineIndex the index of the dependency pipeline, -1 if no dependency
   */
  public PipelineMemoryEstimator constructPipelineMemoryEstimator(
      final Operator operation,
      @Nullable final PlanNodeId parentPlanNodeId,
      final PlanNode root,
      final int dependencyPipelineIndex) {
    PipelineMemoryEstimator currentPipelineMemoryEstimator =
        getInstanceContext().getSessionInfo() != null
                && getInstanceContext().getSessionInfo().getSqlDialect() == SqlDialect.TABLE
            ? new CheckedTablePipelineMemoryEstimator(operation, dependencyPipelineIndex)
            : PipelineMemoryEstimatorFactory.createPipelineMemoryEstimator(
                operation, root, dependencyPipelineIndex);
    // As OperatorTreeGenerator traverse the tree in a post-order way, all the children of current
    // pipeline have been recorded in the map.
    List<PipelineMemoryEstimator> childrenMemoryEstimators =
        parentPlanNodeIdToMemoryEstimator.get(root.getPlanNodeId());
    if (childrenMemoryEstimators != null) {
      currentPipelineMemoryEstimator.addChildren(childrenMemoryEstimators);
    }
    if (parentPlanNodeId != null) {
      parentPlanNodeIdToMemoryEstimator
          .computeIfAbsent(parentPlanNodeId, k -> new LinkedList<>())
          .add(currentPipelineMemoryEstimator);
    }
    return currentPipelineMemoryEstimator;
  }

  public LocalExecutionPlanContext createSubContext() {
    return new LocalExecutionPlanContext(this);
  }

  public FragmentInstanceId getFragmentInstanceId() {
    return driverContext.getFragmentInstanceContext().getId();
  }

  public long getOuterQueryDeadlineMs() {
    return driverContext.getFragmentInstanceContext().getOuterQueryDeadlineMs();
  }

  public List<PipelineDriverFactory> getPipelineDriverFactories() {
    return pipelineDriverFactories;
  }

  public PipelineDriverFactory getCurrentPipelineDriverFactory() {
    return pipelineDriverFactories.get(pipelineDriverFactories.size() - 1);
  }

  public int getPipelineNumber() {
    return pipelineDriverFactories.size();
  }

  public DriverContext getDriverContext() {
    return driverContext;
  }

  public int getDegreeOfParallelism() {
    return degreeOfParallelism;
  }

  public void setDegreeOfParallelism(int degreeOfParallelism) {
    this.degreeOfParallelism = degreeOfParallelism;
  }

  private int getNextPipelineId() {
    return nextPipelineId.getAndIncrement();
  }

  public boolean isInputDriver() {
    return driverContext.isInputDriver();
  }

  public int getNextOperatorId() {
    return nextOperatorId.getAndIncrement();
  }

  public int getExchangeSumNum() {
    return exchangeSumNum;
  }

  public LocalExecutionResourceRegistry getResourceRegistry() {
    return resourceRegistry;
  }

  /** Freeze the physical resource snapshot without changing the legacy effective quota. */
  public LocalExecutionResourceRegistry.Snapshot freezeResourceQuota() {
    long fragmentBudgetBytes =
        IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance();
    frozenResourceSnapshot =
        resourceRegistry.freeze(fragmentBudgetBytes, getMaxBytesOneHandleCanReserve());
    return frozenResourceSnapshot;
  }

  public LocalExecutionResourceRegistry.Snapshot getFrozenResourceSnapshot() {
    return frozenResourceSnapshot;
  }

  public void setExchangeSumNum(int exchangeSumNum) {
    this.exchangeSumNum = exchangeSumNum;
  }

  /** Original per-handle quota for tree planning and independent table FI accounting. */
  public long getMaxBytesOneHandleCanReserve() {
    return exchangeSumNum == 0
        ? fragmentExchangeMaxBytes
        : fragmentExchangeMaxBytes / exchangeSumNum;
  }

  public void addExchangeSumNum(int addValue) {
    this.exchangeSumNum += addValue;
  }

  public void addExchangeOperator(ExchangeOperator exchangeOperator) {
    this.exchangeOperatorList.add(exchangeOperator);
    getInstanceContext()
        .recordActualExchangeEdge(
            getFragmentInstanceId().getFullId()
                + "/pipeline/"
                + getDriverContext().getDriverTaskID().getPipelineId()
                + "/operator/"
                + exchangeOperator.getOperatorContext().getOperatorId()
                + "/source/"
                + exchangeOperator.getSourceId().getId());
    if (exchangeOperator.getSourceHandle() instanceof ISourceHandle) {
      resourceRegistry.registerSourceHandle(
          exchangeOperator.getSourceHandle(),
          LocalExecutionResourceRegistry.memoryKey(
              getFragmentInstanceId(), exchangeOperator.getSourceId().getId()));
    }
  }

  public void recordLegacySinkQuota(ISink sink, long cap) {
    resourceRegistry.recordLegacySinkQuota(sink, cap);
  }

  public void recordLegacySourceQuota(ISourceHandle source, long cap) {
    resourceRegistry.recordLegacySourceQuota(source, cap);
  }

  public void setMaxBytesOneHandleCanReserve() {
    long maxBytesOneHandleCanReserve = getMaxBytesOneHandleCanReserve();
    LOGGER.debug(
        DataNodeQueryMessages
            .MAXBYTESONEHANDLECANRESERVE_FOR_EXCHANGEOPERATOR_IS_ARG_EXCHANGESUMNUM_IS_ARG,
        maxBytesOneHandleCanReserve,
        exchangeSumNum);
    exchangeOperatorList.forEach(
        exchangeOperator ->
            resourceRegistry.recordLegacySourceQuota(
                exchangeOperator.getSourceHandle(), maxBytesOneHandleCanReserve));
  }

  /** Called after operator generation, before root memory estimation and factory registration. */
  public void setExchangeMemoryLimits(PlanNode plan, MemoryPool memoryPool) {
    if (isRuntimeParallelismEnabled()) {
      RuntimeExchangeBudget budget = getRuntimeExchangeBudget();
      if (runtimePlanningState.pool != memoryPool) {
        throw new IllegalStateException(
            DataNodeQueryMessages.EXCEPTION_RUNTIME_SCAN_DESCRIPTOR_STATE);
      }
      legacySinkLimits.forEach(
          (sink, legacyLimit) -> {
            sink.setMaxBytesCanReserve(budget.outputCapBytes());
            resourceRegistry.observePublishedQuota(sink, "SINK", budget.outputCapBytes());
          });
      exchangeOperatorList.forEach(
          exchange -> {
            ISourceHandle source = exchange.getSourceHandle();
            long cap =
                source instanceof SourceHandle ? budget.inputCapBytes() : budget.outputCapBytes();
            source.setMaxBytesCanReserve(cap);
            resourceRegistry.observePublishedQuota(source, "SOURCE", cap);
          });
      return;
    }
    // Only remote sources charge this FI's INPUT. Local inputs are owned by the producing FI;
    // internal pipeline queues cannot enable sharing because of the pipeline check below.
    long inputExchangeCount =
        exchangeOperatorList.stream()
            .filter(exchange -> exchange.getSourceHandle() instanceof SourceHandle)
            .count();
    int outputSinkCount = legacySinkLimits.size();
    long inputMaxBytes =
        inputExchangeCount == 0
            ? 0
            : (outputSinkCount == 0 ? fragmentExchangeMaxBytes : fragmentExchangeMaxBytes / 2);
    long outputMaxBytes = outputSinkCount == 0 ? 0 : fragmentExchangeMaxBytes - inputMaxBytes;
    long maxBlockBytes = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    boolean sharedBudget =
        (inputMaxBytes > 0 || outputMaxBytes > 0)
            && SharedExchangeBudgetEligibility.isEligible(plan)
            // Only the root pipeline may exist. Child factories are registered during generation;
            // the root factory is registered after its memory has been estimated below.
            && pipelineDriverFactories.isEmpty()
            && driverContext.getDependencyDriverIndex() == -1
            && (inputExchangeCount == 0 || inputMaxBytes >= maxBlockBytes)
            && (outputSinkCount == 0 || outputMaxBytes >= maxBlockBytes);
    FragmentInstanceContext instanceContext = driverContext.getFragmentInstanceContext();
    instanceContext.initializeExchangeMemoryBudget(
        sharedBudget ? inputMaxBytes : 0, sharedBudget ? outputMaxBytes : 0);
    if (sharedBudget) {
      memoryPool.registerFragmentInstanceSharedBudget(
          instanceContext.getId().getQueryId().getId(),
          instanceContext.getId().getFragmentInstanceId());
      LOGGER.debug(
          DataNodeQueryMessages.LOG_SHARED_EXCHANGE_BUDGET_FOR_FI_ARG_INPUT_ARG_OUTPUT_ARG_98AE1109,
          instanceContext.getId(),
          instanceContext.getInputMaxBytes(),
          instanceContext.getOutputMaxBytes());
    } else {
      LOGGER.debug(
          DataNodeQueryMessages
              .MAXBYTESONEHANDLECANRESERVE_FOR_EXCHANGEOPERATOR_IS_ARG_EXCHANGESUMNUM_IS_ARG,
          getMaxBytesOneHandleCanReserve(),
          exchangeSumNum);
    }
    legacySinkLimits.forEach(
        (sink, legacyLimit) -> {
          long effectiveLimit = sharedBudget ? instanceContext.getOutputMaxBytes() : legacyLimit;
          sink.setMaxBytesCanReserve(effectiveLimit);
          resourceRegistry.observePublishedQuota(sink, "SINK", effectiveLimit);
        });
    long sourceLimit =
        sharedBudget ? instanceContext.getInputMaxBytes() : getMaxBytesOneHandleCanReserve();
    exchangeOperatorList.forEach(
        exchangeOperator -> {
          ISourceHandle source = exchangeOperator.getSourceHandle();
          source.setMaxBytesCanReserve(sourceLimit);
          resourceRegistry.observePublishedQuota(source, "SOURCE", sourceLimit);
        });
  }

  public Set<String> getAllSensors(IDeviceID deviceId, String sensorId) {
    Set<String> allSensors = allSensorsMap.computeIfAbsent(deviceId, k -> new HashSet<>());
    allSensors.add(sensorId);
    return allSensors;
  }

  public void setNeedUpdateLastCache(boolean needUpdateLastCache) {
    this.needUpdateLastCache = needUpdateLastCache;
  }

  public void addCachedLastValue(
      TimeValuePair timeValuePair, String fullPath, TSDataType dataType) {
    if (cachedLastValueAndPathList == null) {
      cachedLastValueAndPathList = new ArrayList<>();
    }
    cachedLastValueAndPathList.add(
        new Pair<>(
            timeValuePair,
            new Pair<>(new Binary(fullPath, TSFileConfig.STRING_CHARSET), dataType)));
  }

  public List<Pair<TimeValuePair, Pair<Binary, TSDataType>>> getCachedLastValueAndPathList() {
    return cachedLastValueAndPathList;
  }

  public void setISink(ISink sink) {
    requireNonNull(sink, DataNodeQueryMessages.EXCEPTION_SINK_IS_NULL_E33854B4);
    checkArgument(
        driverContext.getSink() == null,
        DataNodeQueryMessages.EXCEPTION_THERE_MUST_BE_AT_MOST_ONE_SINKNODE_A965AFE7);
    if (sink instanceof ISinkHandle) {
      legacySinkLimits.put(sink, getMaxBytesOneHandleCanReserve());
    }
    driverContext.setSink(sink);
    if (sink instanceof ISinkHandle) {
      resourceRegistry.registerSinkHandle(
          (ISinkHandle) sink, getFragmentInstanceId(), getFragmentInstanceId().getFullId());
    } else {
      resourceRegistry.registerSink(sink);
    }
  }

  public void setCachedDataTypes(List<TSDataType> cachedDataTypes) {
    this.cachedDataTypes = cachedDataTypes;
  }

  public List<TSDataType> getCachedDataTypes() {
    return cachedDataTypes;
  }

  public TypeProvider getTypeProvider() {
    return typeProvider;
  }

  @Override
  public ITableTypeProvider getTableTypeProvider() {
    return typeProvider;
  }

  @Override
  public MemoryReservationManager getMemoryReservationManager() {
    return driverContext.getFragmentInstanceContext().getMemoryReservationContext();
  }

  public FragmentInstanceContext getInstanceContext() {
    return driverContext.getFragmentInstanceContext();
  }

  public boolean isNeedUpdateLastCache() {
    return needUpdateLastCache;
  }

  public boolean isNeedUpdateNullEntry() {
    return needUpdateNullEntry;
  }

  public void setNeedUpdateNullEntry(boolean needUpdateNullEntry) {
    this.needUpdateNullEntry = needUpdateNullEntry;
  }

  public Filter getGlobalTimeFilter() {
    return driverContext.getFragmentInstanceContext().getGlobalTimeFilter();
  }

  public Optional<List<Long>> getTimePartitions() {
    return Optional.ofNullable(timePartitions.get());
  }

  public void setTimePartitions(List<Long> timePartitions) {
    this.timePartitions.set(timePartitions);
  }

  public ZoneId getZoneId() {
    return driverContext.getFragmentInstanceContext().getSessionInfo().getZoneId();
  }

  public boolean isBuildPlanUseTemplate() {
    return typeProvider.getTemplatedInfo() != null;
  }

  public TemplatedInfo getTemplatedInfo() {
    return typeProvider.getTemplatedInfo();
  }

  public Map<PlanNodeId, List<PipelineMemoryEstimator>> getParentPlanNodeIdToMemoryEstimator() {
    return parentPlanNodeIdToMemoryEstimator;
  }

  public void invalidateParentPlanNodeIdToMemoryEstimator() {
    parentPlanNodeIdToMemoryEstimator = null;
  }

  /** The runtime mode freezes one FI-side budget before any child context is built. */
  public record RuntimeExchangeBudget(
      long inputCapBytes,
      long outputCapBytes,
      long nativeOutputHeadroomBytes,
      long maxCompactRetainedBlockBytes,
      Map<String, Long> groupPrepaidBudgets,
      Set<String> approvedRawScanPlanNodeIds,
      long maxOriginalScanBlockRetainedBytes,
      Map<String, List<RuntimeExchangeBlockCodec.Bounds>> outputEndpointBounds,
      Map<String, RuntimeExchangeBlockCodec.Bounds> inputEndpointBounds) {
    public RuntimeExchangeBudget(
        long inputCapBytes,
        long outputCapBytes,
        long nativeOutputHeadroomBytes,
        long maxCompactRetainedBlockBytes,
        Map<String, Long> groupPrepaidBudgets,
        Set<String> approvedRawScanPlanNodeIds,
        long maxOriginalScanBlockRetainedBytes) {
      this(
          inputCapBytes,
          outputCapBytes,
          nativeOutputHeadroomBytes,
          maxCompactRetainedBlockBytes,
          groupPrepaidBudgets,
          approvedRawScanPlanNodeIds,
          maxOriginalScanBlockRetainedBytes,
          Map.of(),
          Map.of());
    }

    public RuntimeExchangeBudget {
      groupPrepaidBudgets = Map.copyOf(groupPrepaidBudgets);
      approvedRawScanPlanNodeIds = Set.copyOf(approvedRawScanPlanNodeIds);
      Map<String, List<RuntimeExchangeBlockCodec.Bounds>> outputs = new LinkedHashMap<>();
      outputEndpointBounds.forEach((id, bounds) -> outputs.put(id, List.copyOf(bounds)));
      outputEndpointBounds = Map.copyOf(outputs);
      inputEndpointBounds = Map.copyOf(inputEndpointBounds);
      long nativeOutputPrepaid = 0;
      for (Map.Entry<String, List<RuntimeExchangeBlockCodec.Bounds>> entry :
          outputEndpointBounds.entrySet()) {
        if (entry.getKey().isBlank() || entry.getValue().isEmpty()) {
          throw new IllegalArgumentException(
              DataNodeQueryMessages.EXCEPTION_INVALID_RUNTIME_SCAN_LAYOUT);
        }
        for (RuntimeExchangeBlockCodec.Bounds bounds : entry.getValue()) {
          if (bounds.schema() == null) {
            throw new IllegalArgumentException(
                DataNodeQueryMessages.EXCEPTION_INVALID_RUNTIME_SCAN_LAYOUT);
          }
          nativeOutputPrepaid = Math.addExact(nativeOutputPrepaid, bounds.outputEnvelopeBytes());
        }
      }
      long nativeInputPrepaid = 0;
      for (Map.Entry<String, RuntimeExchangeBlockCodec.Bounds> entry :
          inputEndpointBounds.entrySet()) {
        if (entry.getKey().isBlank() || entry.getValue().schema() == null) {
          throw new IllegalArgumentException(
              DataNodeQueryMessages.EXCEPTION_INVALID_RUNTIME_SCAN_LAYOUT);
        }
        nativeInputPrepaid =
            Math.addExact(nativeInputPrepaid, entry.getValue().inputEnvelopeBytes());
      }
      if (nativeOutputPrepaid > nativeOutputHeadroomBytes || nativeInputPrepaid > inputCapBytes) {
        throw new IllegalArgumentException(
            DataNodeQueryMessages.EXCEPTION_RUNTIME_SCAN_BUDGET_EXHAUSTED);
      }
      if (inputCapBytes < 0
          || outputCapBytes <= 0
          || nativeOutputHeadroomBytes < 0
          || nativeOutputHeadroomBytes > outputCapBytes
          || maxCompactRetainedBlockBytes <= 0
          || maxOriginalScanBlockRetainedBytes <= 0
          || !groupPrepaidBudgets.keySet().containsAll(approvedRawScanPlanNodeIds)) {
        throw new IllegalArgumentException(
            DataNodeQueryMessages.EXCEPTION_INVALID_RUNTIME_SCAN_LAYOUT);
      }
      long prepaid = 0;
      for (Map.Entry<String, Long> entry : groupPrepaidBudgets.entrySet()) {
        if (entry.getKey().isBlank() || entry.getValue() <= 0) {
          throw new IllegalArgumentException(
              DataNodeQueryMessages.EXCEPTION_INVALID_RUNTIME_SCAN_LAYOUT);
        }
        prepaid = Math.addExact(prepaid, entry.getValue());
      }
      if (prepaid > outputCapBytes - nativeOutputHeadroomBytes) {
        throw new IllegalArgumentException(
            DataNodeQueryMessages.EXCEPTION_RUNTIME_SCAN_BUDGET_EXHAUSTED);
      }
    }
  }

  private static final class RuntimePlanningState {
    private RuntimeExchangeBudget budget;
    private MemoryPool pool;
    private final Map<String, RuntimeScanLayout> layouts = new LinkedHashMap<>();
    private final Map<String, RuntimeScanGroupDescriptor.Builder> groups = new LinkedHashMap<>();
    private final List<RuntimeFrontierReservation> ownedFrontiers = new ArrayList<>();
    private RuntimePlanBindings bindings;
    private final Map<String, List<RuntimeEndpointFrontier>> outputFrontiers =
        new LinkedHashMap<>();
    private final Map<String, RuntimeEndpointFrontier> inputFrontiers = new LinkedHashMap<>();
  }

  public void initializeRuntimeExchangeBudget(MemoryPool pool, RuntimeExchangeBudget budget) {
    requireNonNull(pool);
    requireNonNull(budget);
    if (runtimePlanningState.budget != null
        || !pipelineDriverFactories.isEmpty()
        || Math.addExact(budget.inputCapBytes(), budget.outputCapBytes())
            > fragmentExchangeMaxBytes) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_RUNTIME_SCAN_DESCRIPTOR_STATE);
    }
    pool.registerFragmentInstanceSharedBudget(
        getFragmentInstanceId().getQueryId().getId(),
        getFragmentInstanceId().getFragmentInstanceId());
    getInstanceContext()
        .initializeExchangeMemoryBudget(budget.inputCapBytes(), budget.outputCapBytes());
    runtimePlanningState.pool = pool;
    runtimePlanningState.budget = budget;
    // Static schema, envelope totals and supported plan shapes were validated before this first
    // physical reservation. Own each successful payment before any following construction throws.
    for (Map.Entry<String, List<RuntimeExchangeBlockCodec.Bounds>> output :
        budget.outputEndpointBounds().entrySet()) {
      List<RuntimeEndpointFrontier> bindings = new ArrayList<>();
      runtimePlanningState.outputFrontiers.put(output.getKey(), bindings);
      for (int channel = 0; channel < output.getValue().size(); channel++) {
        RuntimeExchangeBlockCodec.Bounds bounds = output.getValue().get(channel);
        bindings.add(
            prepayRuntimeEndpoint(
                "__runtime_endpoint__/output/" + output.getKey() + "/" + channel,
                MemorySide.OUTPUT,
                bounds.outputEnvelopeBytes(),
                budget.outputCapBytes(),
                bounds));
      }
    }
    for (Map.Entry<String, RuntimeExchangeBlockCodec.Bounds> input :
        budget.inputEndpointBounds().entrySet()) {
      RuntimeExchangeBlockCodec.Bounds bounds = input.getValue();
      runtimePlanningState.inputFrontiers.put(
          input.getKey(),
          prepayRuntimeEndpoint(
              "__runtime_endpoint__/input/" + input.getKey(),
              MemorySide.INPUT,
              bounds.inputEnvelopeBytes(),
              budget.inputCapBytes(),
              bounds));
    }
  }

  private RuntimeEndpointFrontier prepayRuntimeEndpoint(
      String id, MemorySide side, long bytes, long cap, RuntimeExchangeBlockCodec.Bounds bounds) {
    MemoryReservationKey key =
        new MemoryReservationKey(
            getFragmentInstanceId().getQueryId().getId(),
            getFragmentInstanceId().getFragmentInstanceId(),
            id);
    RuntimeFrontierReservation owner =
        RuntimeFrontierReservation.tryCreate(runtimePlanningState.pool, key, side, 1, bytes, cap)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        DataNodeQueryMessages.EXCEPTION_RUNTIME_SCAN_BUDGET_EXHAUSTED));
    runtimePlanningState.ownedFrontiers.add(owner);
    resourceRegistry.registerRuntimeFrontier(getFragmentInstanceId().getFullId() + "/" + id, owner);
    return new RuntimeEndpointFrontier(owner, 0, bounds);
  }

  public RuntimeEndpointFrontier getRuntimeOutputFrontier(String planNodeId, int channel) {
    List<RuntimeEndpointFrontier> frontiers = runtimePlanningState.outputFrontiers.get(planNodeId);
    if (frontiers == null || channel < 0 || channel >= frontiers.size()) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_RUNTIME_SCAN_DESCRIPTOR_STATE);
    }
    return frontiers.get(channel);
  }

  public RuntimeEndpointFrontier getRuntimeInputFrontier(String planNodeId) {
    RuntimeEndpointFrontier frontier = runtimePlanningState.inputFrontiers.get(planNodeId);
    if (frontier == null) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_RUNTIME_SCAN_DESCRIPTOR_STATE);
    }
    return frontier;
  }

  public boolean isRuntimeParallelismEnabled() {
    return runtimePlanningState.budget != null;
  }

  public RuntimeExchangeBudget getRuntimeExchangeBudget() {
    if (!isRuntimeParallelismEnabled()) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_RUNTIME_SCAN_BUDGET_NOT_INITIALIZED);
    }
    return runtimePlanningState.budget;
  }

  public boolean isApprovedRuntimeScan(String source) {
    return isRuntimeParallelismEnabled()
        && getRuntimeExchangeBudget().approvedRawScanPlanNodeIds().contains(source);
  }

  /**
   * A paid raw scan uses runtime lanes; explicit approval only overrides native split admission.
   */
  public boolean isRuntimeScan(String source) {
    return isRuntimeParallelismEnabled()
        && getRuntimeExchangeBudget().groupPrepaidBudgets().containsKey(source);
  }

  public int getRuntimeMaxLogicalLanes(String source) {
    RuntimeExchangeBudget budget = getRuntimeExchangeBudget();
    Long prepaid = budget.groupPrepaidBudgets().get(source);
    if (prepaid == null) {
      throw new IllegalStateException(DataNodeQueryMessages.EXCEPTION_INVALID_RUNTIME_SCAN_LAYOUT);
    }
    long possible = prepaid / budget.maxCompactRetainedBlockBytes();
    if (possible < 1) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_RUNTIME_SCAN_BUDGET_EXHAUSTED);
    }
    return (int) Math.min((long) degreeOfParallelism, possible);
  }

  public void registerRuntimeLayout(String source, RuntimeScanLayout layout) {
    if (!isRuntimeScan(source)
        || runtimePlanningState.bindings != null
        || runtimePlanningState.groups.containsKey(source)) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_RUNTIME_SCAN_DESCRIPTOR_STATE);
    }
    requireNonNull(layout);
    if (layout.getLaneCount() > getRuntimeMaxLogicalLanes(source)) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_RUNTIME_SCAN_BUDGET_EXHAUSTED);
    }
    for (ScanParallelPlan.WorkUnit unit : layout.getLanePlan().getWorkUnits()) {
      if (!source.equals(unit.getSourceKey().getPlanNodeId())) {
        throw new IllegalStateException(
            DataNodeQueryMessages.EXCEPTION_INVALID_RUNTIME_SCAN_LAYOUT);
      }
    }
    runtimePlanningState.layouts.put(source, layout);
  }

  public RuntimeScanLayout getRuntimeLayout(ScanParallelPlan plan) {
    if (plan.getWorkUnits().isEmpty()) return null;
    String source = plan.getWorkUnits().get(0).getSourceKey().getPlanNodeId();
    return runtimePlanningState.layouts.get(source);
  }

  public RuntimeScanGroupDescriptor.Builder beginRuntimeScanGroup(
      String source, RuntimeScanLayout layout) {
    if (!isRuntimeScan(source)
        || runtimePlanningState.bindings != null
        || runtimePlanningState.groups.containsKey(source)
        || runtimePlanningState.layouts.get(source) != layout) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_RUNTIME_SCAN_DESCRIPTOR_STATE);
    }
    RuntimeExchangeBudget budget = getRuntimeExchangeBudget();
    long bytes =
        Math.multiplyExact((long) layout.getLaneCount(), budget.maxCompactRetainedBlockBytes());
    if (bytes > budget.groupPrepaidBudgets().get(source)) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_RUNTIME_SCAN_BUDGET_EXHAUSTED);
    }
    String groupId = getFragmentInstanceId().getFullId() + "/scan/" + source;
    MemoryReservationKey key =
        new MemoryReservationKey(
            getFragmentInstanceId().getQueryId().getId(),
            getFragmentInstanceId().getFragmentInstanceId(),
            "__runtime_frontier__/" + source);
    RuntimeFrontierReservation reservation =
        RuntimeFrontierReservation.tryCreate(
                runtimePlanningState.pool,
                key,
                layout.getLaneCount(),
                budget.maxCompactRetainedBlockBytes(),
                budget.outputCapBytes())
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        DataNodeQueryMessages.EXCEPTION_RUNTIME_SCAN_BUDGET_EXHAUSTED));
    // Own the paid reservation before any queue or factory construction can throw.
    runtimePlanningState.ownedFrontiers.add(reservation);
    try {
      resourceRegistry.registerRuntimeFrontier(groupId, reservation);
    } catch (Throwable failure) {
      try {
        reservation.requestClose();
      } catch (Throwable cleanup) {
        if (cleanup != failure) failure.addSuppressed(cleanup);
      }
      throw failure;
    }
    RuntimeScanGroupDescriptor.Builder group =
        new RuntimeScanGroupDescriptor.Builder(groupId, source, layout, reservation);
    runtimePlanningState.groups.put(source, group);
    return group;
  }

  public void registerRuntimeLane(
      RuntimeScanGroupDescriptor.Builder group,
      int lane,
      PipelineDriverFactory factory,
      ExchangeOperator ownExchange,
      LocalSinkChannel sink,
      ISourceHandle source) {
    if (!runtimePlanningState.groups.containsValue(group)) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_RUNTIME_SCAN_DESCRIPTOR_STATE);
    }
    group.registerLane(lane, factory, ownExchange, sink, source);
  }

  public List<RuntimeFrontierReservation> getOwnedRuntimeFrontiers() {
    return List.copyOf(runtimePlanningState.ownedFrontiers);
  }

  public List<RuntimeScanGroupDescriptor> getRuntimeGroupDescriptors() {
    return runtimePlanningState.bindings == null
        ? Collections.emptyList()
        : runtimePlanningState.bindings.getGroups();
  }

  public RuntimePlanBindings getRuntimePlanBindings() {
    if (runtimePlanningState.bindings == null) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_RUNTIME_SCAN_DESCRIPTOR_STATE);
    }
    return runtimePlanningState.bindings;
  }

  /** Freeze against the original factory order before any wrapper replaces native submissions. */
  public RuntimePlanBindings freezeRuntimeGroups(List<PipelineDriverFactory> originalFactoryOrder) {
    if (runtimePlanningState.bindings != null) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_RUNTIME_SCAN_DESCRIPTOR_STATE);
    }
    List<PipelineDriverFactory> originals = List.copyOf(originalFactoryOrder);
    if (originals.size() != pipelineDriverFactories.size()) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_INVALID_RUNTIME_SCAN_DEPENDENCY);
    }
    Map<PipelineDriverFactory, Integer> indices = new IdentityHashMap<>();
    Set<Integer> pipelineIds = new HashSet<>();
    for (int i = 0; i < originals.size(); i++) {
      PipelineDriverFactory factory = originals.get(i);
      if (factory != pipelineDriverFactories.get(i)
          || indices.put(factory, i) != null
          || !pipelineIds.add(factory.getDriverContext().getPipelineId())
          || factory.getDriverContext().getFragmentInstanceContext() != getInstanceContext()) {
        throw new IllegalStateException(
            DataNodeQueryMessages.EXCEPTION_INVALID_RUNTIME_SCAN_DEPENDENCY);
      }
    }
    // Validate the entire native dependency graph before capturing any lazy close future.
    int[] marks = new int[originals.size()];
    for (int origin = 0; origin < originals.size(); origin++) {
      if (marks[origin] != 0) continue;
      int current = origin;
      while (current != -1 && marks[current] == 0) {
        marks[current] = 1;
        int next = originals.get(current).getDependencyPipelineIndex();
        if (next < -1 || next >= originals.size()) {
          throw new IllegalStateException(
              DataNodeQueryMessages.EXCEPTION_INVALID_RUNTIME_SCAN_DEPENDENCY);
        }
        current = next;
      }
      if (current != -1 && marks[current] == 1) {
        throw new IllegalStateException(
            DataNodeQueryMessages.EXCEPTION_INVALID_RUNTIME_SCAN_DEPENDENCY);
      }
      current = origin;
      while (current != -1 && marks[current] == 1) {
        marks[current] = 2;
        current = originals.get(current).getDependencyPipelineIndex();
      }
    }
    List<ListenableFuture<?>> dependencies = new ArrayList<>(originals.size());
    for (int i = 0; i < originals.size(); i++) {
      int dependency = originals.get(i).getDependencyPipelineIndex();
      if (dependency == -1) {
        dependencies.add(Futures.immediateVoidFuture());
      } else {
        if (dependency < 0 || dependency >= originals.size() || dependency == i) {
          throw new IllegalStateException(
              DataNodeQueryMessages.EXCEPTION_INVALID_RUNTIME_SCAN_DEPENDENCY);
        }
        ExchangeOperator consumer =
            originals.get(dependency).getDriverContext().getDownstreamOperator();
        if (consumer == null) {
          throw new IllegalStateException(
              DataNodeQueryMessages.EXCEPTION_INVALID_RUNTIME_SCAN_DEPENDENCY);
        }
        dependencies.add(consumer.getBlockedDependencyDriver());
      }
    }
    List<RuntimeScanGroupDescriptor> groups = new ArrayList<>();
    for (RuntimeScanGroupDescriptor.Builder builder : runtimePlanningState.groups.values()) {
      groups.add(builder.freeze(indices, dependencies));
    }
    RuntimePlanBindings result = new RuntimePlanBindings(originals, dependencies, groups);
    runtimePlanningState.bindings = result;
    return result;
  }

  public static final class RuntimePlanBindings {
    private final List<PipelineDriverFactory> originals;
    private final List<ListenableFuture<?>> dependencies;
    private final List<RuntimeScanGroupDescriptor> groups;
    private final Map<PipelineDriverFactory, Integer> indices = new IdentityHashMap<>();
    private final Map<PipelineDriverFactory, RuntimeScanGroupDescriptor> membership =
        new IdentityHashMap<>();

    private RuntimePlanBindings(
        List<PipelineDriverFactory> originals,
        List<ListenableFuture<?>> dependencies,
        List<RuntimeScanGroupDescriptor> groups) {
      this.originals = List.copyOf(originals);
      this.dependencies = List.copyOf(dependencies);
      this.groups = List.copyOf(groups);
      for (int i = 0; i < originals.size(); i++) indices.put(originals.get(i), i);
      for (RuntimeScanGroupDescriptor group : groups)
        for (RuntimeScanGroupDescriptor.Lane lane : group.getLanes())
          if (membership.put(lane.getFactory(), group) != null)
            throw new IllegalStateException(
                DataNodeQueryMessages.EXCEPTION_RUNTIME_SCAN_DESCRIPTOR_STATE);
    }

    public List<PipelineDriverFactory> getOriginalFactories() {
      return originals;
    }

    public List<ListenableFuture<?>> getStartDependencies() {
      List<ListenableFuture<?>> result = new ArrayList<>(dependencies.size());
      for (ListenableFuture<?> future : dependencies)
        result.add(Futures.nonCancellationPropagating(future));
      return Collections.unmodifiableList(result);
    }

    public List<RuntimeScanGroupDescriptor> getGroups() {
      return groups;
    }

    public ListenableFuture<?> startDependencyFor(PipelineDriverFactory factory) {
      Integer index = indices.get(factory);
      if (index == null) {
        throw new IllegalArgumentException(
            DataNodeQueryMessages.EXCEPTION_INVALID_RUNTIME_SCAN_DEPENDENCY);
      }
      return Futures.nonCancellationPropagating(dependencies.get(index));
    }

    public RuntimeScanGroupDescriptor groupForFactory(PipelineDriverFactory factory) {
      return membership.get(factory);
    }
  }
}
