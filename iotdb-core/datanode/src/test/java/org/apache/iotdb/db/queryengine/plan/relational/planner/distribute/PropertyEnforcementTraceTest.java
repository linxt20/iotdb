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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableLogicalPlanner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.DEFAULT_WARNING;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SESSION_INFO;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.PlanPropertyProviderTest.*;
import static org.junit.Assert.*;

public class PropertyEnforcementTraceTest {
  private boolean property, hash, ordered;
  private int dataNode;

  @Before
  public void saveConfiguration() {
    var c = IoTDBDescriptor.getInstance().getConfig();
    property = c.isEnablePropertyDrivenPlanning();
    hash = c.isEnableTableGroupByHashRepartition();
    ordered = c.isEnableOrderedParallelScan();
    dataNode = c.getDataNodeId();
    c.setEnablePropertyDrivenPlanning(true);
    c.setEnableTableGroupByHashRepartition(false);
    c.setEnableOrderedParallelScan(false);
    c.setDataNodeId(1);
  }

  @After
  public void restoreConfiguration() {
    var c = IoTDBDescriptor.getInstance().getConfig();
    c.setEnablePropertyDrivenPlanning(property);
    c.setEnableTableGroupByHashRepartition(hash);
    c.setEnableOrderedParallelScan(ordered);
    c.setDataNodeId(dataNode);
  }

  private static PropertyEnforcementDecision decision(
      PlanProperties required,
      List<PlanNode> inputs,
      OrderingScheme observed,
      Map<PlanNodeId, OrderingScheme> orders) {
    var result =
        new PropertyEnforcer()
            .enforce(
                required,
                inputs,
                new PlanPropertyProvider().providedByBranches(inputs, observed, orders),
                new QueryId("trace_render"),
                orders);
    return PropertyEnforcementDecision.enforcement("trace-test", required, inputs, result);
  }

  @Test
  public void textDistinguishesSatisfiedCollectSortMergeAndFallback() {
    PlanNode a = values("a"), b = values("b");
    List<PropertyEnforcementDecision> cases = new ArrayList<>();
    cases.add(decision(PlanProperties.singleUnordered(), List.of(a), null, new HashMap<>()));
    cases.add(decision(PlanProperties.singleUnordered(), List.of(a, b), null, new HashMap<>()));
    cases.add(
        decision(
            PlanProperties.singleUnordered().withOrdering(ASC), List.of(a), null, new HashMap<>()));
    cases.add(
        decision(
            PlanProperties.singleUnordered().withOrdering(ASC),
            List.of(a, b),
            ASC,
            new HashMap<>(Map.of(a.getPlanNodeId(), ASC, b.getPlanNodeId(), ASC))));
    cases.add(
        decision(
            PlanProperties.requirementFor(DistributionProperty.partitioned(List.of(KEY)), null),
            List.of(a, b),
            null,
            new HashMap<>()));
    String[] nodes = {"NONE", "CollectNode", "SortNode", "MergeSortNode", "CollectNode"};
    for (int i = 0; i < cases.size(); i++) {
      var d = cases.get(i);
      String text = d.render();
      assertEquals(text, d.render());
      assertTrue(text.contains("rule=trace-test"));
      assertTrue(text.contains("-> " + nodes[i]));
      assertTrue(text.contains("outcome=" + d.getOutcome()));
      assertTrue(text.contains("inputs=" + d.getInputIds()));
      assertTrue(text.contains("outputs=" + d.getOutputIds()));
      assertTrue(text.contains("required="));
      assertTrue(text.contains("provided="));
    }
    assertTrue(cases.get(2).render().contains("ASC NULLS LAST"));
    assertTrue(cases.get(4).render().contains("FALLBACK:PARTITIONING_UNSUPPORTED"));
    String frozen = cases.get(1).render();
    a.setPlanNodeId(new PlanNodeId("mutated"));
    assertEquals(frozen, cases.get(1).render());
  }

  @Test
  public void hashTextContainsTheCapturedSourceBucketAndExchangeMatrix() {
    List<PlanNodeId> sources = List.of(new PlanNodeId("s1"), new PlanNodeId("s2"));
    List<PlanNodeId> buckets = List.of(new PlanNodeId("b1"), new PlanNodeId("b2"));
    List<List<PlanNodeId>> exchanges =
        List.of(
            List.of(new PlanNodeId("e11"), new PlanNodeId("e12")),
            List.of(new PlanNodeId("e21"), new PlanNodeId("e22")));
    PlanProperties partitioned =
        PlanProperties.requirementFor(DistributionProperty.partitioned(List.of(KEY)), null);
    var d =
        PropertyEnforcementDecision.specialized(
            "visitAggregation:hash-topology",
            PropertyEnforcementDecision.Event.HASH_REPARTITION,
            partitioned,
            PlanProperties.any(),
            partitioned,
            sources,
            buckets,
            PropertyEnforcer.EnforcerKind.NONE,
            PropertyEnforcer.Outcome.ENFORCED,
            null,
            sources,
            buckets,
            exchanges);
    String text = d.render();
    assertTrue(text.contains("event=HASH_REPARTITION_APPLIED"));
    assertTrue(text.contains("sources=2 partitions=2"));
    assertTrue(text.contains("sourceIds=" + sources));
    assertTrue(text.contains("bucketIds=" + buckets));
    assertTrue(text.contains("exchangeIds=" + exchanges));
  }

  @Test
  public void joinAndAdmissionTextRetainTheReasonDomain() {
    var d =
        PropertyEnforcementDecision.specialized(
            "visitJoin:hash-fallback",
            PropertyEnforcementDecision.Event.JOIN_HASH_FALLBACK,
            PlanProperties.any(),
            PlanProperties.any(),
            PlanProperties.singleUnordered(),
            List.of(new PlanNodeId("left"), new PlanNodeId("right")),
            List.of(new PlanNodeId("join")),
            PropertyEnforcer.EnforcerKind.NONE,
            PropertyEnforcer.Outcome.FALLBACK,
            FallbackReason.NO_EQUI_JOIN_KEYS,
            List.of(),
            List.of(),
            List.of());
    assertTrue(d.render().contains("FALLBACK:NO_EQUI_JOIN_KEYS"));
    for (var reason :
        TableDistributedPlanGenerator.GroupByHashRepartitionAdmission.RejectionReason.values()) {
      var admission =
          PropertyEnforcementDecision.groupByAdmission(
              "visitAggregation:admission",
              PlanProperties.any(),
              PlanProperties.any(),
              List.of(values("a")),
              TableDistributedPlanGenerator.GroupByHashRepartitionAdmission.rejected(reason));
      assertTrue(admission.render().contains("GROUP_BY_ADMISSION:" + reason.name()));
      assertTrue(admission.render().contains("outcome=FALLBACK"));
    }
  }

  @Test
  public void plannerTextAndSnapshotSurviveLaterGeneratorAndPlanMutation() throws Exception {
    String sql = "SELECT * FROM testdb.table1";
    MPPQueryContext context =
        new MPPQueryContext(sql, new QueryId("frozen_trace"), SESSION_INFO, null, null);
    context.setStartTime(System.currentTimeMillis());
    context.setTimeOut(60000);
    Analysis analysis = analyzeSQL(sql, TEST_MATADATA, context);
    analysis.setDataPartitionInfo(MockTableModelDataPartition.constructDataPartition("testdb"));
    SymbolAllocator allocator = new SymbolAllocator();
    LogicalQueryPlan logical =
        new TableLogicalPlanner(context, TEST_MATADATA, SESSION_INFO, allocator, DEFAULT_WARNING)
            .plan(analysis);
    TableDistributedPlanner planner =
        new TableDistributedPlanner(analysis, allocator, logical, TEST_MATADATA, null);
    PlanNode physical =
        planner.generateDistributedPlanWithOptimize(
            new TableDistributedPlanGenerator.PlanContext());
    List<PropertyEnforcementDecision> frozen = planner.getPropertyEnforcementDecisions();
    assertFalse(frozen.isEmpty());
    List<String> input = new ArrayList<>(List.of("plan"));
    List<String> text = planner.appendRuleTrace(input);
    List<String> records =
        frozen.stream().map(PropertyEnforcementDecision::render).collect(Collectors.toList());
    assertEquals(List.of("plan"), input);
    assertEquals(text, planner.appendRuleTrace(input));
    try {
      frozen.clear();
      fail("mutable planner snapshot");
    } catch (UnsupportedOperationException expected) {
    }
    Field field = TableDistributedPlanner.class.getDeclaredField("distributedPlanGenerator");
    field.setAccessible(true);
    TableDistributedPlanGenerator generator = (TableDistributedPlanGenerator) field.get(planner);
    int size = frozen.size();
    generator.enforce(PlanProperties.singleUnordered(), List.of(values("later")), null);
    assertEquals(size + 1, generator.getPropertyEnforcementDecisions().size());
    physical.getChildren().get(0).setPlanNodeId(new PlanNodeId("changed_after_freeze"));
    assertEquals(size, planner.getPropertyEnforcementDecisions().size());
    assertEquals(
        records,
        frozen.stream().map(PropertyEnforcementDecision::render).collect(Collectors.toList()));
    assertEquals(text, planner.appendRuleTrace(input));
  }
}
