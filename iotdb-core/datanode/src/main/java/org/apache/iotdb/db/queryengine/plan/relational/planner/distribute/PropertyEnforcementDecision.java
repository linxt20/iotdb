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
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.PropertyEnforcer.EnforcerKind;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.PropertyEnforcer.Outcome;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.PropertyEnforcer.PropertyEnforcementResult;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributedPlanGenerator.GroupByHashRepartitionAdmission;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/** Immutable values captured while planning, with no retained mutable plan nodes or input lists. */
public final class PropertyEnforcementDecision {
  public enum Event {
    ENFORCEMENT,
    GROUP_BY_ADMISSION,
    HASH_REPARTITION,
    JOIN_HASH_FALLBACK,
    SPECIALIZED
  }

  private final String ruleSource;
  private final Event event;
  private final List<PlanNodeId> inputIds;
  private final List<PlanNodeId> outputIds;
  private final PlanProperties required;
  private final PlanProperties providedBefore;
  private final PlanProperties providedAfter;
  private final List<PlanNodeId> insertedNodeIds;
  private final EnforcerKind actualEnforcer;
  private final Outcome outcome;
  @Nullable private final FallbackReason fallbackReason;
  @Nullable private final GroupByHashRepartitionAdmission.RejectionReason groupByRejectionReason;
  private final List<PlanNodeId> sourceIds;
  private final List<PlanNodeId> bucketIds;
  private final List<List<PlanNodeId>> exchangeIds;

  private PropertyEnforcementDecision(
      String ruleSource,
      Event event,
      List<PlanNodeId> inputIds,
      List<PlanNodeId> outputIds,
      PlanProperties required,
      PlanProperties before,
      PlanProperties after,
      List<PlanNodeId> insertedNodeIds,
      EnforcerKind kind,
      Outcome outcome,
      @Nullable FallbackReason reason,
      @Nullable GroupByHashRepartitionAdmission.RejectionReason groupReason,
      List<PlanNodeId> sourceIds,
      List<PlanNodeId> bucketIds,
      List<List<PlanNodeId>> exchangeIds) {
    this.ruleSource = ruleSource;
    this.event = event;
    this.inputIds = List.copyOf(inputIds);
    this.outputIds = List.copyOf(outputIds);
    this.required = required;
    this.providedBefore = before;
    this.providedAfter = after;
    this.insertedNodeIds = List.copyOf(insertedNodeIds);
    this.actualEnforcer = kind;
    this.outcome = outcome;
    this.fallbackReason = reason;
    this.groupByRejectionReason = groupReason;
    this.sourceIds = List.copyOf(sourceIds);
    this.bucketIds = List.copyOf(bucketIds);
    this.exchangeIds =
        exchangeIds.stream().map(List::copyOf).collect(Collectors.toUnmodifiableList());
  }

  public static PropertyEnforcementDecision enforcement(
      String ruleSource,
      PlanProperties required,
      List<PlanNode> inputs,
      PropertyEnforcementResult result) {
    return new PropertyEnforcementDecision(
        ruleSource,
        Event.ENFORCEMENT,
        ids(inputs),
        ids(result.getBranches()),
        required,
        result.getProvidedBefore(),
        result.getProvidedAfter(),
        result.getInsertedNodeIds(),
        result.getActualEnforcer(),
        result.getOutcome(),
        result.getFallbackReason(),
        null,
        List.of(),
        List.of(),
        List.of());
  }

  public static PropertyEnforcementDecision groupByAdmission(
      String ruleSource,
      PlanProperties required,
      PlanProperties provided,
      List<PlanNode> branches,
      GroupByHashRepartitionAdmission admission) {
    return new PropertyEnforcementDecision(
        ruleSource,
        Event.GROUP_BY_ADMISSION,
        ids(branches),
        ids(branches),
        required,
        provided,
        provided,
        List.of(),
        EnforcerKind.NONE,
        admission.isAdmitted() ? Outcome.SATISFIED : Outcome.FALLBACK,
        admission.toFallbackReason(),
        admission.getRejectionReason(),
        List.of(),
        List.of(),
        List.of());
  }

  public static PropertyEnforcementDecision specialized(
      String ruleSource,
      Event event,
      PlanProperties required,
      PlanProperties before,
      PlanProperties after,
      List<PlanNodeId> inputs,
      List<PlanNodeId> outputs,
      EnforcerKind kind,
      Outcome outcome,
      @Nullable FallbackReason fallbackReason,
      List<PlanNodeId> sourceIds,
      List<PlanNodeId> bucketIds,
      List<List<PlanNodeId>> exchangeIds) {
    return new PropertyEnforcementDecision(
        ruleSource,
        event,
        inputs,
        outputs,
        required,
        before,
        after,
        List.of(),
        kind,
        outcome,
        fallbackReason,
        null,
        sourceIds,
        bucketIds,
        exchangeIds);
  }

  private static List<PlanNodeId> ids(List<PlanNode> nodes) {
    return nodes.stream().map(PlanNode::getPlanNodeId).collect(Collectors.toList());
  }

  public String getRuleSource() {
    return ruleSource;
  }

  public Event getEvent() {
    return event;
  }

  public List<PlanNodeId> getInputIds() {
    return inputIds;
  }

  public List<PlanNodeId> getOutputIds() {
    return outputIds;
  }

  public PlanProperties getRequired() {
    return required;
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

  @Nullable
  public GroupByHashRepartitionAdmission.RejectionReason getGroupByRejectionReason() {
    return groupByRejectionReason;
  }

  public List<PlanNodeId> getSourceIds() {
    return sourceIds;
  }

  public List<PlanNodeId> getBucketIds() {
    return bucketIds;
  }

  public List<List<PlanNodeId>> getExchangeIds() {
    return exchangeIds;
  }

  @Nullable
  public String getReasonCode() {
    if (groupByRejectionReason != null) {
      return "GROUP_BY_ADMISSION:" + groupByRejectionReason.name();
    }
    return fallbackReason == null ? null : "FALLBACK:" + fallbackReason.name();
  }

  /** Renders only captured values, so later plan mutation cannot change a decision's text. */
  public String render() {
    String reason = getReasonCode() == null ? "-" : getReasonCode();
    if (event == Event.HASH_REPARTITION) {
      return String.format(
          DataNodeQueryMessages
              .MESSAGE_ARG_RULE_ARG_EVENT_HASH_REPARTITION_APPLIED_REQUIRED_ARG_PROVIDED_ARG_TABLEHASHPARTITIONINGSHUFFLESINKNODE_TABLE_HASH_V1_SOURCES_ARG_PARTITIONS_ARG_N_X_P_EXPERIMENTAL_PATH_HASH_TOPOLOGY_SOURCEIDS_ARG_BUCKETIDS_ARG_EXCHANGEIDS_ARG_316A0F0F,
          sourceIds,
          ruleSource,
          describe(required),
          describe(providedBefore),
          sourceIds.size(),
          bucketIds.size(),
          sourceIds,
          bucketIds,
          exchangeIds);
    }
    if (event == Event.JOIN_HASH_FALLBACK) {
      return String.format(
          DataNodeQueryMessages
              .MESSAGE_ARG_RULE_ARG_REQUIRED_COMPATIBLE_PARTITIONED_JOINKEYS_ON_BOTH_INPUTS_PROVIDED_ARG_SERIAL_JOIN_FALLBACK_ARG_INPUTS_ARG_OUTPUTS_ARG_E30CFBBA,
          outputIds,
          ruleSource,
          describe(providedBefore),
          reason,
          inputIds,
          outputIds);
    }
    if (event == Event.SPECIALIZED) {
      return String.format(
          DataNodeQueryMessages
              .MESSAGE_ARG_RULE_ARG_EVENT_SPECIALIZED_INPUTS_ARG_OUTPUTS_ARG_PROVIDED_ARG_REASON_ARG_04563442,
          outputIds,
          ruleSource,
          inputIds,
          outputIds,
          describe(providedAfter),
          reason);
    }
    String enforcer;
    switch (actualEnforcer) {
      case COLLECT:
        enforcer = "CollectNode";
        break;
      case SORT:
        enforcer = "SortNode";
        break;
      case MERGE_SORT:
        enforcer = "MergeSortNode";
        break;
      default:
        enforcer = actualEnforcer.name();
    }
    return String.format(
        DataNodeQueryMessages
            .MESSAGE_ARG_RULE_ARG_EVENT_ARG_INPUTS_ARG_OUTPUTS_ARG_REQUIRED_ARG_PROVIDED_ARG_PROVIDEDAFTER_ARG_ARG_OUTCOME_ARG_REASON_ARG_67571BFD,
        inputIds,
        ruleSource,
        event,
        inputIds,
        outputIds,
        describe(required),
        describe(providedBefore),
        describe(providedAfter),
        enforcer,
        outcome,
        reason);
  }

  private static String describe(PlanProperties properties) {
    return properties + (properties.isOrdered() ? " ordering=" + properties.getOrdering() : "");
  }
}
