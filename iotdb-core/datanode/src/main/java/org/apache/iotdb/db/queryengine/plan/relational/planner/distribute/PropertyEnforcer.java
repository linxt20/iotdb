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

package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.QueryId;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Enforces convergence and ordering; specialized grouped hash construction stays in the generator.
 */
public class PropertyEnforcer {
  public enum EnforcerKind {
    NONE,
    COLLECT,
    SORT,
    MERGE_SORT
  }

  public enum Outcome {
    SATISFIED,
    ENFORCED,
    FALLBACK
  }

  public static final class PropertyEnforcementResult {
    private final List<PlanNode> branches;
    private final PlanProperties providedBefore;
    private final PlanProperties providedAfter;
    private final List<PlanNodeId> insertedNodeIds;
    private final EnforcerKind actualEnforcer;
    private final Outcome outcome;
    @Nullable private final FallbackReason fallbackReason;

    private PropertyEnforcementResult(
        List<PlanNode> branches,
        PlanProperties before,
        PlanProperties after,
        List<PlanNodeId> insertedNodeIds,
        EnforcerKind kind,
        Outcome outcome,
        @Nullable FallbackReason fallbackReason) {
      this.branches = List.copyOf(branches);
      this.providedBefore = before;
      this.providedAfter = after;
      this.insertedNodeIds = List.copyOf(insertedNodeIds);
      this.actualEnforcer = kind;
      this.outcome = outcome;
      this.fallbackReason = fallbackReason;
    }

    public List<PlanNode> getBranches() {
      return branches;
    }

    public PlanProperties getProvidedBefore() {
      return providedBefore;
    }

    public PlanProperties getProvidedAfter() {
      return providedAfter;
    }

    public List<PlanNodeId> getInsertedNodeIds() {
      return insertedNodeIds;
    }

    public EnforcerKind getActualEnforcer() {
      return actualEnforcer;
    }

    public Outcome getOutcome() {
      return outcome;
    }

    @Nullable
    public FallbackReason getFallbackReason() {
      return fallbackReason;
    }
  }

  public PropertyEnforcementResult enforce(
      PlanProperties required,
      List<PlanNode> branches,
      PlanPropertyProvider.ProvidedProperties provided,
      QueryId queryId,
      Map<PlanNodeId, OrderingScheme> recordedOrderings) {
    checkArgument(
        branches != null,
        DataNodeQueryMessages.EXCEPTION_CHILDRENNODES_SHOULD_NOT_BE_NULL_DOT_0C93B063);
    checkArgument(
        !branches.isEmpty(),
        DataNodeQueryMessages.EXCEPTION_CHILDRENNODES_SHOULD_NOT_BE_EMPTY_DOT_E5555FD9);
    for (PlanNode branch : branches) {
      checkArgument(
          branch.getOutputSymbols().containsAll(required.getDistribution().getKeys())
              && (!required.isOrdered()
                  || branch.getOutputSymbols().containsAll(required.getOrdering().getOrderBy())),
          DataNodeQueryMessages
              .EXCEPTION_REQUIRED_PROPERTY_SYMBOLS_ARE_MISSING_FROM_A_BRANCH_OUTPUT_826CB07E);
    }
    PlanProperties before = provided.getProperties();
    FallbackReason reason = provided.getFallbackReason();
    if (before.satisfies(required)) {
      return new PropertyEnforcementResult(
          branches,
          before,
          before,
          List.of(),
          EnforcerKind.NONE,
          reason == null ? Outcome.SATISFIED : Outcome.FALLBACK,
          reason);
    }

    boolean converge = !before.satisfiesDistribution(required);
    if (required.getDistribution().isPartitioned() && converge && reason == null) {
      reason = FallbackReason.PARTITIONING_UNSUPPORTED;
    }
    List<Symbol> outputs = branches.get(0).getOutputSymbols();
    if (converge) {
      for (PlanNode branch : branches) {
        checkArgument(
            outputs.equals(branch.getOutputSymbols()),
            DataNodeQueryMessages
                .EXCEPTION_CANNOT_ENFORCE_PLAN_PROPERTIES_FOR_INCOMPATIBLE_BRANCH_OUTPUT_SYMBOLS_5B0AFEA7);
      }
    }
    List<PlanNodeId> inserted = new ArrayList<>();
    List<PlanNode> result = new ArrayList<>(branches);
    EnforcerKind kind = EnforcerKind.NONE;
    if (required.isOrdered() && !before.satisfiesOrdering(required)) {
      result.clear();
      // Preserve the baseline: when any input is unordered, sort every input.
      for (PlanNode branch : branches) {
        SortNode sort =
            new SortNode(queryId.genPlanNodeId(), branch, required.getOrdering(), false, false);
        recordedOrderings.put(sort.getPlanNodeId(), required.getOrdering());
        inserted.add(sort.getPlanNodeId());
        result.add(sort);
      }
      kind = EnforcerKind.SORT;
    }
    if (converge && (result.size() > 1 || kind == EnforcerKind.NONE)) {
      PlanNode glue;
      if (required.isOrdered()) {
        MergeSortNode merge =
            new MergeSortNode(queryId.genPlanNodeId(), required.getOrdering(), outputs);
        result.forEach(merge::addChild);
        recordedOrderings.put(merge.getPlanNodeId(), required.getOrdering());
        glue = merge;
        kind = EnforcerKind.MERGE_SORT;
      } else {
        CollectNode collect = new CollectNode(queryId.genPlanNodeId(), outputs);
        result.forEach(collect::addChild);
        glue = collect;
        kind = EnforcerKind.COLLECT;
      }
      inserted.add(glue.getPlanNodeId());
      result = List.of(glue);
    }
    PlanProperties after =
        PlanProperties.of(
            converge ? DistributionProperty.single() : before.getDistribution(),
            required.isOrdered() ? required.getOrdering() : null);
    return new PropertyEnforcementResult(
        result,
        before,
        after,
        inserted,
        kind,
        reason == null ? Outcome.ENFORCED : Outcome.FALLBACK,
        reason);
  }
}
