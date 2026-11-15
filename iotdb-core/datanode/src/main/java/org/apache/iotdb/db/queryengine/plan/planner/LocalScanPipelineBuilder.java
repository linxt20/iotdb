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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.operator.process.RuntimeCompactOutputOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Common construction steps for a local scan pipeline and its parent exchange.
 *
 * <p>The scan-specific code creates the root operator and describes its plan-node ownership. This
 * builder then performs the same sub-context, local queue, driver factory, memory estimator and
 * parent source construction for every scan shape. The scan-specific factory may build a plain scan
 * root or an ordered local merge root; the parent still receives one exchange per pipeline.
 */
public final class LocalScanPipelineBuilder {

  private static final MPPDataExchangeManager MPP_DATA_EXCHANGE_MANAGER =
      MPPDataExchangeService.getInstance().getMPPDataExchangeManager();

  private LocalScanPipelineBuilder() {}

  @FunctionalInterface
  public interface PipelineFactory {
    PipelineSpec create(int pipelineIndex, LocalExecutionPlanContext subContext);
  }

  /** Immutable description of the scan-specific part of one pipeline. */
  public static final class PipelineSpec {
    private final Operator rootOperator;
    private final PlanNode rootPlanNode;
    private final PlanNodeId memoryParentPlanNodeId;
    private final PlanNodeId upstreamPlanNodeId;

    public PipelineSpec(
        Operator rootOperator,
        PlanNode rootPlanNode,
        PlanNodeId memoryParentPlanNodeId,
        PlanNodeId upstreamPlanNodeId) {
      this.rootOperator =
          Objects.requireNonNull(
              rootOperator,
              () ->
                  String.format(
                      DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918,
                      "rootOperator"));
      this.rootPlanNode =
          Objects.requireNonNull(
              rootPlanNode,
              () ->
                  String.format(
                      DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918,
                      "rootPlanNode"));
      this.memoryParentPlanNodeId = memoryParentPlanNodeId;
      this.upstreamPlanNodeId =
          Objects.requireNonNull(
              upstreamPlanNodeId,
              () ->
                  String.format(
                      DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918,
                      "upstreamPlanNodeId"));
    }
  }

  /** Build pipelines using the immutable static plan and return parent exchange operators. */
  public static List<Operator> build(
      LocalExecutionPlanContext parentContext, ScanParallelPlan plan, PipelineFactory factory) {
    Objects.requireNonNull(
        plan,
        () ->
            String.format(
                DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918, "plan"));
    RuntimeScanLayout runtimeLayout = parentContext.getRuntimeLayout(plan);
    if (runtimeLayout != null && plan.getMode() != ScanParallelPlan.Mode.SERIAL) {
      return buildRuntime(parentContext, runtimeLayout, factory);
    }
    return build(
        parentContext,
        plan.getBuckets().size(),
        (index, subContext) -> {
          subContext
              .getDynamicScanAssignments()
              .registerBucket(
                  subContext.getDriverContext().getDriverTaskID().getPipelineId(), plan, index);
          return factory.create(index, subContext);
        });
  }

  /** Build all unordered scan pipelines and return the parent exchange operators in order. */
  public static List<Operator> build(
      LocalExecutionPlanContext parentContext, int pipelineCount, PipelineFactory factory) {
    Objects.requireNonNull(
        parentContext,
        () ->
            String.format(
                DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918,
                "parentContext"));
    Objects.requireNonNull(
        factory,
        () ->
            String.format(
                DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918, "factory"));
    if (pipelineCount < 0) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_PIPELINECOUNT_MUST_NOT_BE_NEGATIVE_AB4936D3);
    }

    List<Operator> exchangeOperators = new ArrayList<>(pipelineCount);
    for (int pipelineIndex = 0; pipelineIndex < pipelineCount; pipelineIndex++) {
      LocalExecutionPlanContext subContext = parentContext.createSubContext();
      PipelineSpec spec =
          Objects.requireNonNull(
              factory.create(pipelineIndex, subContext),
              () ->
                  String.format(
                      DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918,
                      "pipelineSpec"));

      // Own the composite before exchange construction can fail and adopt its source leaves.
      subContext.registerPipelineRoot(spec.rootOperator);

      ISinkChannel localSinkChannel =
          MPP_DATA_EXCHANGE_MANAGER.createLocalSinkChannelForPipeline(
              subContext.getDriverContext(),
              spec.upstreamPlanNodeId.getId(),
              parentContext.getResourceRegistry()::registerQueue);
      if (!(localSinkChannel instanceof LocalSinkChannel)) {
        throw new IllegalStateException(
            DataNodeQueryMessages
                .EXCEPTION_UNORDERED_SCAN_PIPELINE_MUST_USE_A_LOCAL_SINK_CHANNEL_8F69FB47);
      }
      subContext.setISink(localSinkChannel);
      parentContext
          .getResourceRegistry()
          .registerQueue(((LocalSinkChannel) localSinkChannel).getSharedTsBlockQueue());
      parentContext.getResourceRegistry().registerSink(localSinkChannel);
      subContext.addPipelineDriverFactory(spec.rootOperator, subContext.getDriverContext(), 0);
      subContext.constructPipelineMemoryEstimator(
          spec.rootOperator, spec.memoryParentPlanNodeId, spec.rootPlanNode, -1);

      ISourceHandle sourceHandle =
          MPP_DATA_EXCHANGE_MANAGER.createLocalSourceHandleForPipeline(
              ((LocalSinkChannel) localSinkChannel).getSharedTsBlockQueue(),
              parentContext.getDriverContext());
      parentContext
          .getResourceRegistry()
          .registerSourceHandle(
              sourceHandle,
              ((LocalSinkChannel) localSinkChannel)
                  .getSharedTsBlockQueue()
                  .getMemoryReservationKey());
      ExchangeOperator exchangeOperator =
          new ExchangeOperator(
              parentContext
                  .getDriverContext()
                  .addOperatorContext(
                      parentContext.getNextOperatorId(),
                      null,
                      ExchangeOperator.class.getSimpleName()),
              sourceHandle,
              spec.upstreamPlanNodeId,
              spec.rootOperator.calculateMaxReturnSize());
      parentContext.addExchangeOperator(exchangeOperator);
      exchangeOperators.add(exchangeOperator);
    }
    parentContext.addExchangeSumNum(exchangeOperators.size());
    return exchangeOperators;
  }

  /** Construct stable logical lanes; worker leases never own or close these resources. */
  public static List<Operator> buildRuntime(
      LocalExecutionPlanContext parentContext, RuntimeScanLayout layout, PipelineFactory factory) {
    Objects.requireNonNull(parentContext);
    Objects.requireNonNull(layout);
    Objects.requireNonNull(factory);
    ScanParallelPlan plan = layout.getLanePlan();
    if (plan.getMode() == ScanParallelPlan.Mode.SERIAL || plan.getWorkUnits().isEmpty()) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_INVALID_RUNTIME_SCAN_LAYOUT);
    }
    String sourceId = plan.getWorkUnits().get(0).getSourceKey().getPlanNodeId();
    RuntimeScanGroupDescriptor.Builder group =
        parentContext.beginRuntimeScanGroup(sourceId, layout);
    LocalExecutionPlanContext.RuntimeExchangeBudget budget =
        parentContext.getRuntimeExchangeBudget();
    List<Operator> exchanges = new ArrayList<>(layout.getLaneCount());
    try {
      for (int lane = 0; lane < layout.getLaneCount(); lane++) {
        final int laneIndex = lane;
        LocalExecutionPlanContext subContext = parentContext.createSubContext();
        subContext
            .getDynamicScanAssignments()
            .registerBucket(subContext.getDriverContext().getPipelineId(), plan, lane);
        PipelineSpec spec = Objects.requireNonNull(factory.create(lane, subContext));
        Operator root =
            new RuntimeCompactOutputOperator(
                spec.rootOperator,
                budget.maxCompactRetainedBlockBytes(),
                budget.maxOriginalScanBlockRetainedBytes());
        // Register the composite before allocating a queue, including its source leaves.
        subContext.registerPipelineRoot(root);
        ISinkChannel sinkChannel =
            MPP_DATA_EXCHANGE_MANAGER.createLocalSinkChannelForPipeline(
                subContext.getDriverContext(),
                spec.upstreamPlanNodeId.getId(),
                group.getReservation(),
                lane,
                queue -> {
                  group.captureQueue(laneIndex, queue);
                  parentContext.getResourceRegistry().registerQueue(queue);
                });
        if (!(sinkChannel instanceof LocalSinkChannel)) {
          throw new IllegalStateException(
              DataNodeQueryMessages
                  .EXCEPTION_UNORDERED_SCAN_PIPELINE_MUST_USE_A_LOCAL_SINK_CHANNEL_8F69FB47);
        }
        LocalSinkChannel sink = (LocalSinkChannel) sinkChannel;
        subContext.setISink(sink);
        parentContext.getResourceRegistry().registerQueue(sink.getSharedTsBlockQueue());
        parentContext.getResourceRegistry().registerSink(sink);
        PipelineDriverFactory driverFactory =
            subContext.addPipelineDriverFactoryAndReturn(root, subContext.getDriverContext(), 0);
        subContext.constructPipelineMemoryEstimator(
            root, spec.memoryParentPlanNodeId, spec.rootPlanNode, -1);
        ISourceHandle source =
            MPP_DATA_EXCHANGE_MANAGER.createLocalSourceHandleForPipeline(
                sink.getSharedTsBlockQueue(), parentContext.getDriverContext());
        parentContext
            .getResourceRegistry()
            .registerSourceHandle(source, sink.getSharedTsBlockQueue().getMemoryReservationKey());
        ExchangeOperator exchange =
            new ExchangeOperator(
                parentContext
                    .getDriverContext()
                    .addOperatorContext(
                        parentContext.getNextOperatorId(),
                        null,
                        ExchangeOperator.class.getSimpleName()),
                source,
                spec.upstreamPlanNodeId,
                root.calculateMaxReturnSize());
        driverFactory.setDownstreamOperator(exchange);
        parentContext.addExchangeOperator(exchange);
        parentContext.registerRuntimeLane(group, lane, driverFactory, exchange, sink, source);
        exchanges.add(exchange);
      }
      group.completeConstruction();
      parentContext.addExchangeSumNum(exchanges.size());
      return exchanges;
    } catch (Throwable failure) {
      group.abortBuild(failure);
      throw failure;
    }
  }
}
