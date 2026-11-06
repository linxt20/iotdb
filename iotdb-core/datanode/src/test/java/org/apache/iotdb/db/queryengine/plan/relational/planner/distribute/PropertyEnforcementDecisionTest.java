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
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributedPlanGenerator.GroupByHashRepartitionAdmission;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableDistributedPlanGenerator.GroupByHashRepartitionAdmission.RejectionReason;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.PlanPropertyProviderTest.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class PropertyEnforcementDecisionTest {
  private boolean originalHashFlag;

  @Before
  public void captureAndEnableHash() {
    originalHashFlag =
        IoTDBDescriptor.getInstance().getConfig().isEnableTableGroupByHashRepartition();
    IoTDBDescriptor.getInstance().getConfig().setEnableTableGroupByHashRepartition(true);
  }

  @After
  public void restoreHash() {
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableTableGroupByHashRepartition(originalHashFlag);
  }

  @Test
  public void reasonCountsAndBothExplicitMappingsAreFixed() {
    assertEquals(13, FallbackReason.values().length);
    assertEquals(14, RejectionReason.values().length);
    for (RejectionReason reason : RejectionReason.values()) {
      GroupByHashRepartitionAdmission admission = GroupByHashRepartitionAdmission.rejected(reason);
      assertFalse(admission.isAdmitted());
      assertSame(reason, admission.getRejectionReason());
      if (reason == RejectionReason.MULTIPLE_GROUPING_SETS) {
        assertSame(FallbackReason.MULTIPLE_GROUPING_SETS, admission.toFallbackReason());
      } else if (reason == RejectionReason.DISTINCT_OR_MASKED_AGGREGATION) {
        assertSame(FallbackReason.DISTINCT_OR_MASKED_AGGREGATION, admission.toFallbackReason());
      } else {
        assertNull(reason.name(), admission.toFallbackReason());
      }
    }
    assertTrue(GroupByHashRepartitionAdmission.admitted().isAdmitted());
    assertNull(GroupByHashRepartitionAdmission.admitted().getRejectionReason());
    assertNull(GroupByHashRepartitionAdmission.admitted().toFallbackReason());
  }

  @Test
  public void decisionCopiesIdsAndPropertiesRatherThanRetainingInputNodes() {
    List<PlanNode> inputs = new ArrayList<>(List.of(values("a"), values("b")));
    Map<PlanNodeId, OrderingScheme> orders = new HashMap<>();
    PropertyEnforcer.PropertyEnforcementResult result =
        new PropertyEnforcer()
            .enforce(
                PlanProperties.singleUnordered(),
                inputs,
                new PlanPropertyProvider().providedByBranches(inputs, null, orders),
                new QueryId("decision"),
                orders);
    PropertyEnforcementDecision d =
        PropertyEnforcementDecision.enforcement(
            "visitOutput:input", PlanProperties.singleUnordered(), inputs, result);
    inputs.get(0).setPlanNodeId(new PlanNodeId("changed"));
    inputs.clear();
    assertEquals(List.of(new PlanNodeId("a"), new PlanNodeId("b")), d.getInputIds());
    assertEquals(1, d.getOutputIds().size());
    assertEquals("visitOutput:input", d.getRuleSource());
    assertEquals(PropertyEnforcer.EnforcerKind.COLLECT, d.getActualEnforcer());
    assertEquals(PropertyEnforcer.Outcome.ENFORCED, d.getOutcome());
    assertNull(d.getReasonCode());
    try {
      d.getInputIds().clear();
      fail("mutable snapshot");
    } catch (UnsupportedOperationException expected) {
    }
  }

  @Test
  public void topologyIdsAreDeepCopiedIncludingEachExchangeRow() {
    List<PlanNodeId> sources = new ArrayList<>(List.of(new PlanNodeId("s")));
    List<PlanNodeId> buckets = new ArrayList<>(List.of(new PlanNodeId("b")));
    List<PlanNodeId> row = new ArrayList<>(List.of(new PlanNodeId("e")));
    List<List<PlanNodeId>> matrix = new ArrayList<>(List.of(row));
    PropertyEnforcementDecision d =
        PropertyEnforcementDecision.specialized(
            "visitAggregation:hash",
            PropertyEnforcementDecision.Event.HASH_REPARTITION,
            PlanProperties.any(),
            PlanProperties.any(),
            PlanProperties.any(),
            sources,
            buckets,
            PropertyEnforcer.EnforcerKind.NONE,
            PropertyEnforcer.Outcome.ENFORCED,
            null,
            sources,
            buckets,
            matrix);
    sources.clear();
    buckets.clear();
    row.clear();
    matrix.clear();
    assertEquals(List.of(new PlanNodeId("s")), d.getSourceIds());
    assertEquals(List.of(new PlanNodeId("b")), d.getBucketIds());
    assertEquals(List.of(List.of(new PlanNodeId("e"))), d.getExchangeIds());
    try {
      d.getExchangeIds().get(0).clear();
      fail("mutable nested list");
    } catch (UnsupportedOperationException expected) {
    }
  }

  @Test
  public void admissionCodesKeepTheirDomainIncludingTheTwelveUnmappedCodes() {
    for (RejectionReason reason : RejectionReason.values()) {
      PropertyEnforcementDecision d =
          PropertyEnforcementDecision.groupByAdmission(
              "visitAggregation:admission",
              PlanProperties.any(),
              PlanProperties.any(),
              List.of(values("a")),
              GroupByHashRepartitionAdmission.rejected(reason));
      assertSame(reason, d.getGroupByRejectionReason());
      assertEquals("GROUP_BY_ADMISSION:" + reason.name(), d.getReasonCode());
    }
  }

  private TableDistributedPlanGenerator generator() {
    MPPQueryContext context = mock(MPPQueryContext.class);
    when(context.getQueryId()).thenReturn(new QueryId("admission"));
    return new TableDistributedPlanGenerator(context, null, new SymbolAllocator(), null);
  }

  private AggregationNode aggregate() {
    AggregationNode node = mock(AggregationNode.class);
    when(node.getStep()).thenReturn(AggregationNode.Step.SINGLE);
    when(node.getGroupingKeys()).thenReturn(List.of(KEY));
    when(node.getGroupingSetCount()).thenReturn(1);
    when(node.getPreGroupedSymbols()).thenReturn(List.of());
    when(node.getHashSymbol()).thenReturn(Optional.empty());
    when(node.getGroupIdSymbol()).thenReturn(Optional.empty());
    when(node.getAggregations()).thenReturn(Map.of());
    return node;
  }

  private PlanNode scan() {
    DeviceTableScanNode node = mock(DeviceTableScanNode.class);
    when(node.getOutputSymbols()).thenReturn(List.of(KEY));
    return node;
  }

  private void rejects(RejectionReason expected, Consumer<AggregationNode> change) {
    AggregationNode node = aggregate();
    change.accept(node);
    GroupByHashRepartitionAdmission result =
        generator().evaluateGroupByHashRepartition(node, List.of(scan()));
    assertSame(expected.name(), expected, result.getRejectionReason());
  }

  @Test
  public void allAggregationShapeRejectionsAreReachable() {
    rejects(
        RejectionReason.UNSUPPORTED_AGGREGATION_STEP,
        n -> when(n.getStep()).thenReturn(AggregationNode.Step.FINAL));
    rejects(
        RejectionReason.EMPTY_GROUPING_KEYS, n -> when(n.getGroupingKeys()).thenReturn(List.of()));
    rejects(
        RejectionReason.MULTIPLE_GROUPING_SETS, n -> when(n.getGroupingSetCount()).thenReturn(2));
    rejects(
        RejectionReason.EMPTY_GROUPING_SET, n -> when(n.hasEmptyGroupingSet()).thenReturn(true));
    rejects(
        RejectionReason.PRE_GROUPED_AGGREGATION,
        n -> when(n.getPreGroupedSymbols()).thenReturn(List.of(KEY)));
    rejects(RejectionReason.STREAMABLE_AGGREGATION, n -> when(n.isStreamable()).thenReturn(true));
    rejects(RejectionReason.ORDERED_AGGREGATION, n -> when(n.hasOrderings()).thenReturn(true));
    rejects(
        RejectionReason.PREEXISTING_HASH_SYMBOL,
        n -> when(n.getHashSymbol()).thenReturn(Optional.of(KEY)));
    rejects(
        RejectionReason.GROUP_ID_PRESENT,
        n -> when(n.getGroupIdSymbol()).thenReturn(Optional.of(KEY)));
    for (boolean distinct : List.of(true, false)) {
      rejects(
          RejectionReason.DISTINCT_OR_MASKED_AGGREGATION,
          n -> {
            AggregationNode.Aggregation agg = mock(AggregationNode.Aggregation.class);
            when(agg.isDistinct()).thenReturn(distinct);
            when(agg.hasMask()).thenReturn(!distinct);
            when(n.getAggregations()).thenReturn(Map.of(OTHER, agg));
          });
    }
  }

  @Test
  public void sourceAndMissingKeyRejectionsAreReachableAndSuccessHasNoReason() {
    TableDistributedPlanGenerator generator = generator();
    AggregationNode node = aggregate();
    assertSame(
        RejectionReason.EMPTY_HASH_SOURCES,
        generator.evaluateGroupByHashRepartition(node, List.of()).getRejectionReason());
    assertSame(
        RejectionReason.UNSUPPORTED_HASH_SOURCE,
        generator.evaluateGroupByHashRepartition(node, List.of(values("a"))).getRejectionReason());
    DeviceTableScanNode missing = mock(DeviceTableScanNode.class);
    when(missing.getOutputSymbols()).thenReturn(List.of(OTHER));
    assertSame(
        RejectionReason.MISSING_GROUPING_KEY_OUTPUT,
        generator.evaluateGroupByHashRepartition(node, List.of(missing)).getRejectionReason());
    GroupByHashRepartitionAdmission success =
        generator.evaluateGroupByHashRepartition(node, List.of(scan()));
    assertTrue(success.isAdmitted());
    assertNull(success.getRejectionReason());
  }

  @Test
  public void rejectionPriorityFollowsBaselinePredicateOrder() {
    IoTDBDescriptor.getInstance().getConfig().setEnableTableGroupByHashRepartition(false);
    assertSame(
        RejectionReason.GROUP_BY_HASH_DISABLED,
        generator().evaluateGroupByHashRepartition(aggregate(), List.of()).getRejectionReason());
    IoTDBDescriptor.getInstance().getConfig().setEnableTableGroupByHashRepartition(true);
    AggregationNode multiple = aggregate();
    when(multiple.getGroupingSetCount()).thenReturn(2);
    AggregationNode.Aggregation distinct = mock(AggregationNode.Aggregation.class);
    when(distinct.isDistinct()).thenReturn(true);
    when(multiple.getAggregations()).thenReturn(Map.of(OTHER, distinct));
    assertSame(
        RejectionReason.MULTIPLE_GROUPING_SETS,
        generator().evaluateGroupByHashRepartition(multiple, List.of(scan())).getRejectionReason());
  }
}
