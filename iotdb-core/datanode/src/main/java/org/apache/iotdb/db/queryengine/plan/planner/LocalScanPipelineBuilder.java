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
}
