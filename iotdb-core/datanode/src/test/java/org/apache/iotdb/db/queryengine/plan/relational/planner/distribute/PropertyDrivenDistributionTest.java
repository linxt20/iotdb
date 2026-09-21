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
import org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.RowNumberNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValuesNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableLogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableHashPartitioningShuffleSinkNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.DEFAULT_WARNING;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SESSION_INFO;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

/**
 * Plan shape assertions for the property driven distribution rules. Nothing here starts the
 * execution engine: the planner is run on a mocked partition and the resulting plan is inspected.
 *
 * <p>The EXPLAIN ANALYZE cases are the regression test for the bug where wrapping a query in
 * EXPLAIN ANALYZE silently changed the plan it was supposed to report on: the scan's parent became
 * an ExplainAnalyzeNode, which merged its child without ever marking the scan as parallelizable, so
 * every query observed that way ran with a single scan driver.
 */
@RunWith(Parameterized.class)
public class PropertyDrivenDistributionTest {

  private static final String SINGLE_REGION_DB = "testdb";

  /**
   * Every case below is run twice: once with the legacy heuristic and once with the property driven
   * rules. Both have to produce the same answers, which is what makes turning the flag on a non
   * event.
   */
  @Parameterized.Parameters(name = "enable_property_driven_planning={0}")
  public static Object[] flagValues() {
    return new Object[] {false, true};
  }

  @Parameterized.Parameter public boolean propertyDrivenPlanning;

  @BeforeClass
  public static void setUp() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
  }

  @Before
  public void applyFlag() {
    setPropertyDrivenPlanning(propertyDrivenPlanning);
  }

  /**
   * A plain query whose parent imposes no ordering: the scan may be split into parallel drivers.
   */
  @Test
  public void plainQueryAllowsParallelScan() {
    List<DeviceTableScanNode> scans = planAndCollectScans("SELECT * FROM testdb.table1");

    assertEquals(1, scans.size());
    assertTrue(scans.get(0).isAllowParallelScan());
  }

  /**
   * The same query wrapped in EXPLAIN ANALYZE must produce a scan with the same parallelism as the
   * plain query, otherwise EXPLAIN ANALYZE reports on a plan that is not the one a user gets.
   */
  @Test
  public void explainAnalyzeAllowsParallelScanLikePlainQuery() {
    List<DeviceTableScanNode> scans =
        planAndCollectScans("EXPLAIN ANALYZE SELECT * FROM testdb.table1");

    assertEquals(1, scans.size());
    assertTrue(
        "EXPLAIN ANALYZE must not downgrade the scan parallelism of the query it wraps",
        scans.get(0).isAllowParallelScan());
  }

  /**
   * An ordering requirement has to survive the EXPLAIN ANALYZE wrapper as well: when the parent
   * requires an order, the scan must not be split, exactly as in the plain query case.
   */
  @Test
  public void orderedQueryForbidsParallelScan() {
    List<DeviceTableScanNode> plain =
        planAndCollectScans("SELECT * FROM testdb.table1 ORDER BY time");
    List<DeviceTableScanNode> explained =
        planAndCollectScans("EXPLAIN ANALYZE SELECT * FROM testdb.table1 ORDER BY time");

    assertFalse(plain.isEmpty());
    assertFalse(explained.isEmpty());
    for (DeviceTableScanNode scan : plain) {
      assertFalse(
          "an ordering requirement must keep the scan on a single stream",
          scan.isAllowParallelScan());
    }
    for (DeviceTableScanNode scan : explained) {
      assertFalse(
          "EXPLAIN ANALYZE must not turn an ordered scan into a parallel one",
          scan.isAllowParallelScan());
    }
  }

  /**
   * A LIMIT with an ORDER BY is planned as a TopK. A TopK sitting at the root has to see all rows
   * in one branch, but a TopK is also pushed down below that root, and those pushed down copies do
   * not converge their input. This locks in that shape, so that expressing the rules in terms of
   * required properties does not turn the TopK requirement into a blanket Single that would
   * needlessly serialize what is below it.
   */
  @Test
  public void topKDoesNotConvergeItsOwnChildren() {
    List<PlanNode> nodes =
        planAndCollectNodes("SELECT * FROM testdb.table1 ORDER BY time LIMIT 10");

    List<PlanNode> topKs =
        nodes.stream().filter(node -> node instanceof TopKNode).collect(Collectors.toList());
    assertFalse("expected the query to be planned with a TopK", topKs.isEmpty());
    for (PlanNode topK : topKs) {
      for (PlanNode child : topK.getChildren()) {
        assertFalse(
            "a TopK merges its branches itself, it must not get a CollectNode/MergeSortNode below it",
            child instanceof CollectNode || child instanceof MergeSortNode);
      }
    }
    assertAllScansForbidParallelism(
        nodes,
        "a global TopK is order-sensitive, so this static path must not split its scan drivers");
  }

  /**
   * A parent requirement is not evidence that its child already has the required property. In
   * particular, a one-input MergeSort cannot sort an unordered stream, so the property enforcer
   * must insert a SortNode when the required ordering is absent from the physical child.
   */
  @Test
  public void orderedRequirementOnUnorderedChildInsertsSortEnforcer() {
    assumeFalse(propertyDrivenPlanning);
    setPropertyDrivenPlanning(true);

    Symbol time = new Symbol("time");
    OrderingScheme timeAscending =
        new OrderingScheme(ImmutableList.of(time), ImmutableMap.of(time, SortOrder.ASC_NULLS_LAST));
    ValuesNode unorderedChild =
        new ValuesNode(
            new PlanNodeId("unordered_values"), ImmutableList.of(time), Collections.emptyList());

    TableDistributedPlanGenerator generator =
        new TableDistributedPlanGenerator(
            new MPPQueryContext(
                "property enforcement test",
                new QueryId("property_enforcement_test"),
                SESSION_INFO,
                null,
                null),
            null,
            null,
            null);
    PlanNode enforced =
        generator.enforce(
            PlanProperties.of(DistributionProperty.single(), timeAscending),
            Collections.singletonList(unorderedChild),
            null);

    assertTrue(
        "an unordered physical child cannot satisfy an ordered requirement",
        enforced instanceof SortNode);
    assertEquals(unorderedChild, enforced.getChildren().get(0));
    assertTrue(
        "the trace must distinguish the ordered requirement from the unordered physical input",
        generator.getRuleTrace().get(0).contains("required=Single+Ordered"));
    assertTrue(generator.getRuleTrace().get(0).contains("provided=Single"));
    assertTrue(generator.getRuleTrace().get(0).contains("SortNode"));
  }

  /**
   * A two-branch unordered consumer makes the distribution decision explicit: Arbitrary inputs do
   * not satisfy a Single requirement, so CollectNode is the enforcer. This is the minimal
   * executable regression for the required/provided/enforcer vocabulary used by the SQL trace.
   */
  @Test
  public void unorderedBranchesRecordArbitraryToSingleCollectEnforcement() {
    assumeFalse(propertyDrivenPlanning);
    setPropertyDrivenPlanning(true);

    Symbol value = new Symbol("value");
    TableDistributedPlanGenerator generator =
        new TableDistributedPlanGenerator(
            new MPPQueryContext(
                "property enforcement collect test",
                new QueryId("property_enforcement_collect_test"),
                SESSION_INFO,
                null,
                null),
            null,
            null,
            null);
    PlanNode enforced =
        generator.enforce(
            PlanProperties.singleUnordered(),
            ImmutableList.of(
                new ValuesNode(
                    new PlanNodeId("first_values"),
                    ImmutableList.of(value),
                    Collections.emptyList()),
                new ValuesNode(
                    new PlanNodeId("second_values"),
                    ImmutableList.of(value),
                    Collections.emptyList())),
            null);

    assertTrue(enforced instanceof CollectNode);
    assertEquals(2, enforced.getChildren().size());
    String trace = generator.getRuleTrace().get(0);
    assertTrue(trace.contains("required=Single"));
    assertTrue(trace.contains("provided=Arbitrary"));
    assertTrue(trace.contains("CollectNode"));
  }

  /**
   * A {@code Partitioned(keys)} requirement must not be advertised as parallel hash distribution
   * until the exchange can route individual rows by those keys. The current safe enforcer is a
   * {@link CollectNode}: it reduces the input to one branch, which trivially keeps every group
   * together but deliberately does not claim a repartition speedup.
   */
  @Test
  public void partitionedRequirementConservativelyCollectsUntilHashExchangeExists() {
    assumeFalse(propertyDrivenPlanning);
    setPropertyDrivenPlanning(true);

    Symbol group = new Symbol("group");
    ValuesNode left =
        new ValuesNode(
            new PlanNodeId("left_values"), ImmutableList.of(group), Collections.emptyList());
    ValuesNode right =
        new ValuesNode(
            new PlanNodeId("right_values"), ImmutableList.of(group), Collections.emptyList());
    TableDistributedPlanGenerator generator =
        new TableDistributedPlanGenerator(
            new MPPQueryContext(
                "partitioned enforcement test",
                new QueryId("partitioned_enforcement_test"),
                SESSION_INFO,
                null,
                null),
            null,
            null,
            null);

    PlanNode enforced =
        generator.enforce(
            PlanProperties.of(DistributionProperty.partitioned(ImmutableList.of(group)), null),
            ImmutableList.of(left, right),
            null);

    assertTrue(
        "without a key-aware exchange, collecting is the only sound Partitioned(key) enforcer",
        enforced instanceof CollectNode);
    assertTrue(
        "the EXPLAIN trace must describe a collect, not pretend that a hash repartition happened",
        generator.getRuleTrace().get(0).contains("Partitioned[group]"));
    assertTrue(generator.getRuleTrace().get(0).contains("CollectNode"));
  }

  /**
   * A real grouped aggregation is the first intended consumer of {@code Partitioned(groupKeys)}.
   * The contract and row partitioner alone are not enough to make that true: the table fragmenter
   * still creates one identity sink for each exchange edge, whereas this shape needs every partial
   * aggregation to have a channel to every final partition. Until that N-to-N topology and the
   * hash-aware sink operator are both present, the planner must keep the existing final
   * aggregation/Collect shape and must never emit the declarative hash-sink node.
   */
  @Test
  public void groupedAggregationDoesNotSelectUnwiredHashExchange() {
    List<PlanNode> nodes =
        planAndCollectNodes("SELECT s1, count(*) FROM testdb.table1 GROUP BY s1");

    assertTrue(nodes.stream().anyMatch(node -> node instanceof AggregationNode));
    assertFalse(
        "a hash contract without N-to-N fragment wiring must not be selected for GROUP BY",
        nodes.stream().anyMatch(node -> node instanceof TableHashPartitioningShuffleSinkNode));
    assertAllScansForbidParallelism(
        nodes,
        "until the final grouping aggregation consumes Partitioned(groupKeys), GROUP BY stays on the Collect fallback");
  }

  /** Plans the statement against a single data region and returns every node in the plan. */
  private static List<PlanNode> planAndCollectNodes(String sql) {
    List<PlanNode> nodes = new ArrayList<>();
    plan(sql).getFragments().forEach(fragment -> collectAll(fragment.getPlanNodeTree(), nodes));
    return nodes;
  }

  private static void collectAll(PlanNode node, List<PlanNode> nodes) {
    if (node == null) {
      return;
    }
    nodes.add(node);
    node.getChildren().forEach(child -> collectAll(child, nodes));
  }

  /**
   * The property driven rules are meant to reproduce the plans the old heuristic produced, so that
   * turning the flag on is not a behaviour change. Rather than arguing that from the code, plan the
   * same statements both ways and compare the resulting shapes.
   */
  @Test
  public void propertyDrivenRulesProduceTheSamePlanAsTheHeuristic() {
    // This case drives the flag itself, so it only needs to run once.
    assumeFalse(propertyDrivenPlanning);

    String[] statements = {
      "SELECT * FROM testdb.table1",
      "SELECT * FROM testdb.table1 ORDER BY time",
      "SELECT * FROM testdb.table1 ORDER BY time LIMIT 10",
      "SELECT * FROM testdb.table1 LIMIT 10",
      "SELECT * FROM testdb.table1 WHERE s1 > 1",
      "SELECT * FROM testdb.table1 WHERE s1 > 1 ORDER BY time",
      "SELECT s1 + 1 AS projected_s1 FROM testdb.table1",
      "SELECT count(*) FROM testdb.table1",
      "SELECT s1, count(*) FROM testdb.table1 GROUP BY s1",
      "SELECT * FROM testdb.table1 t1 JOIN testdb.table2 t2 ON t1.time = t2.time",
      "SELECT s1 FROM testdb.table1 UNION ALL SELECT s1 FROM testdb.table1",
      "SELECT row_number() OVER () FROM testdb.table1",
      "SELECT row_number() OVER (ORDER BY time) FROM testdb.table1",
      "SELECT count(*) OVER () FROM testdb.table1",
      "SELECT count(s1) OVER (ORDER BY time) FROM testdb.table1",
      "EXPLAIN ANALYZE SELECT * FROM testdb.table1",
    };

    for (String sql : statements) {
      setPropertyDrivenPlanning(false);
      String legacy = planShape(sql);
      setPropertyDrivenPlanning(true);
      String propertyDriven = planShape(sql);

      assertEquals(
          "turning on enable_property_driven_planning must not change the plan for: " + sql,
          legacy,
          propertyDriven);
    }
  }

  @After
  public void restoreFlag() {
    setPropertyDrivenPlanning(false);
  }

  private static void setPropertyDrivenPlanning(boolean enabled) {
    IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(enabled);
  }

  /** A textual rendering of the plan shape: node types, nesting, and scan parallelism. */
  private static String planShape(String sql) {
    StringBuilder shape = new StringBuilder();
    plan(sql).getFragments().forEach(fragment -> appendShape(fragment.getPlanNodeTree(), 0, shape));
    return shape.toString();
  }

  private static void appendShape(PlanNode node, int depth, StringBuilder shape) {
    if (node == null) {
      return;
    }
    for (int i = 0; i < depth; i++) {
      shape.append("  ");
    }
    shape.append(node.getClass().getSimpleName());
    if (node instanceof DeviceTableScanNode) {
      shape
          .append("(parallel=")
          .append(((DeviceTableScanNode) node).isAllowParallelScan())
          .append(')');
    }
    shape.append('\n');
    node.getChildren().forEach(child -> appendShape(child, depth + 1, shape));
  }

  /**
   * The rule trace is what makes the decisions inspectable from SQL: EXPLAIN has to show which node
   * required what, what its children provided, and which enforcer was inserted. It must only appear
   * when the rules are actually in charge.
   */
  @Test
  public void explainShowsThePropertyEnforcementTraceOnlyWhenEnabled() {
    // This case drives the flag itself, so it only needs to run once.
    assumeFalse(propertyDrivenPlanning);

    setPropertyDrivenPlanning(false);
    assertFalse(
        "the legacy heuristic takes no property decisions, so it must not print a trace",
        explainText("SELECT * FROM testdb.table1").contains("Property enforcement:"));

    setPropertyDrivenPlanning(true);
    String explained = explainText("SELECT * FROM testdb.table1");
    assertTrue(
        "EXPLAIN must show the property enforcement trace when the rules are enabled",
        explained.contains("Property enforcement:"));
    assertTrue(
        "each traced decision must state what was required and what was provided, got:\n"
            + explained,
        explained.contains("required=") && explained.contains("provided="));
  }

  /** Tests the production planner trace attachment without requiring a running cluster. */
  private static String explainText(String sql) {
    MPPQueryContext queryContext =
        new MPPQueryContext(sql, new QueryId("explain_test"), SESSION_INFO, null, null);
    queryContext.setExplainType(MPPQueryContext.ExplainType.EXPLAIN);
    queryContext.setInnerTriggeredQuery(true);
    return String.join("\n", plan(sql, queryContext).getPlanText());
  }

  /**
   * A WHERE clause puts a FilterNode between the output and the scan. That must not cost the scan
   * its parallelism: with a single data region the filter is the scan's only parent, so if it
   * attached the scan directly instead of merging it, nothing would ever mark the scan as
   * splittable and no filtered query could run in parallel.
   */
  @Test
  public void filteredQueryAllowsParallelScan() {
    List<DeviceTableScanNode> scans =
        planAndCollectScans("SELECT * FROM testdb.table1 WHERE s1 > 1");

    assertFalse(scans.isEmpty());
    for (DeviceTableScanNode scan : scans) {
      assertTrue(
          "a WHERE clause must not prevent the scan from being split", scan.isAllowParallelScan());
    }
  }

  /** The same, with an ordering requirement on top: the scan has to stay on a single stream. */
  @Test
  public void filteredOrderedQueryForbidsParallelScan() {
    List<DeviceTableScanNode> scans =
        planAndCollectScans("SELECT * FROM testdb.table1 WHERE s1 > 1 ORDER BY time");

    assertFalse(scans.isEmpty());
    for (DeviceTableScanNode scan : scans) {
      assertFalse(
          "an ordering requirement must still keep a filtered scan on a single stream",
          scan.isAllowParallelScan());
    }
  }

  /**
   * A computed project is deliberately kept above its child rather than copied to each scan branch.
   * It therefore has no consuming Collect/MergeSort enforcer in the current static planner, and the
   * safe fallback is a non-splittable scan. This is an explicit coverage gap, not evidence that
   * projection has become data-parallel.
   */
  @Test
  public void computedProjectDocumentsTheNoEnforcerFallback() {
    List<PlanNode> nodes = planAndCollectNodes("SELECT s1 + 1 AS projected_s1 FROM testdb.table1");
    assertTrue(nodes.stream().anyMatch(node -> node instanceof ProjectNode));
    assertAllScansForbidParallelism(
        nodes,
        "without a branch-copying or collecting project rule, the computed projection stays serial");
  }

  /**
   * A scalar aggregation is a global consumer. Its final aggregation is the serialization point;
   * unlike a filter/project it therefore cannot promise scan-driver parallelism in the current
   * static plan. This documents the intentional fallback until partial/final aggregation gains a
   * consumed Partitioned(keys) property.
   */
  @Test
  public void scalarAggregationUsesAForcedSerializationFallback() {
    List<PlanNode> nodes = planAndCollectNodes("SELECT count(*) FROM testdb.table1");
    assertTrue(nodes.stream().anyMatch(node -> node instanceof AggregationNode));
    assertAllScansForbidParallelism(
        nodes, "a scalar final aggregation must keep its input on one logical stream");
  }

  /**
   * UNION ALL is not an ordering-preserving convergence point. It keeps branches separate, so no
   * Collect/MergeSort enforcer is inserted below the union and the static planner does not mark
   * either scan as locally splittable.
   */
  @Test
  public void unionKeepsBranchSerializationUntilAnExplicitConsumer() {
    List<PlanNode> nodes =
        planAndCollectNodes("SELECT s1 FROM testdb.table1 UNION ALL SELECT s1 FROM testdb.table1");
    assertTrue(nodes.stream().anyMatch(node -> node instanceof UnionNode));
    assertFalse(
        "Union itself must not manufacture a global Collect/MergeSort enforcer",
        nodes.stream()
            .anyMatch(node -> node instanceof CollectNode || node instanceof MergeSortNode));
    assertAllScansForbidParallelism(
        nodes, "without an explicit consuming enforcer union branches retain the safe fallback");
  }

  /**
   * Equi-join inputs are sorted/merged by the existing merge-sort join path. The scans must stay
   * serial in this P0 implementation because Partitioned(joinKeys) is still not executable: there
   * is no hash repartition exchange yet. The assertion is intentionally a guardrail, not a claim
   * that joins have already become data-parallel.
   */
  @Test
  public void equiJoinDocumentsThePartitionedKeysFallback() {
    List<PlanNode> nodes =
        planAndCollectNodes(
            "SELECT * FROM testdb.table1 t1 JOIN testdb.table2 t2 ON t1.time = t2.time");
    assertTrue(nodes.stream().anyMatch(node -> node instanceof JoinNode));
    assertTrue(
        "merge-sort join must expose an ordering enforcer on at least one input",
        nodes.stream().anyMatch(node -> node instanceof SortNode || node instanceof MergeSortNode));
    assertAllScansForbidParallelism(
        nodes,
        "until Partitioned(joinKeys) is consumed by a hash exchange, equi-join scans stay serial");
  }

  /**
   * A RowNumber without ORDER BY has no synthetic SortNode below it and must still be plannable.
   */
  @Test
  public void unorderedRowNumberPlansWithoutSyntheticSort() {
    List<PlanNode> nodes = planAndCollectNodes("SELECT row_number() OVER () FROM testdb.table1");
    assertTrue(nodes.stream().anyMatch(node -> node instanceof RowNumberNode));
    assertFalse(
        "row_number() OVER () must not invent an ordering enforcer",
        nodes.stream().anyMatch(node -> node instanceof SortNode || node instanceof MergeSortNode));
  }

  /**
   * A window without ORDER BY has no ordering requirement. It may still introduce other physical
   * operators (for example, a global aggregation), so this test deliberately does not infer a
   * scan-driver marker from that semantic fact.
   */
  @Test
  public void unorderedWindowPlansWithoutOrderingRequirement() {
    List<PlanNode> nodes = planAndCollectNodes("SELECT count(*) OVER () FROM testdb.table1");
    assertTrue(nodes.stream().anyMatch(node -> node instanceof WindowNode));
    assertFalse(
        "an order-free window must not invent a SortNode/MergeSortNode",
        nodes.stream().anyMatch(node -> node instanceof SortNode || node instanceof MergeSortNode));
  }

  /**
   * In contrast, a RowNumber whose result depends on the order of its input must keep the scan on
   * one stream. This guards the order-sensitive side of the shared merge path.
   */
  @Test
  public void orderedRowNumberForbidsParallelScan() {
    List<PlanNode> nodes =
        planAndCollectNodes("SELECT row_number() OVER (ORDER BY time) FROM testdb.table1");

    assertTrue(
        "the ordered ranking path must retain an order-sensitive physical operator",
        nodes.stream().anyMatch(node -> node instanceof RowNumberNode || node instanceof WindowNode));
    assertAllScansForbidParallelism(
        nodes, "an order-sensitive RowNumber must not split its scan into parallel drivers");
  }

  /**
   * An ordered aggregate window is subject to the same ordering contract as ordered RowNumber.
   * The physical source may satisfy that order directly or through a SortNode, so the invariant is
   * the WindowNode and its non-splittable scan rather than an implementation-specific glue shape.
   */
  @Test
  public void orderedWindowForbidsParallelScan() {
    List<PlanNode> nodes =
        planAndCollectNodes("SELECT count(s1) OVER (ORDER BY time) FROM testdb.table1");

    assertTrue(nodes.stream().anyMatch(node -> node instanceof WindowNode));
    assertAllScansForbidParallelism(
        nodes, "an ordered window must not split its scan into parallel drivers");
  }

  /** Plans the statement against a single data region and returns every scan node in the plan. */
  private static List<DeviceTableScanNode> planAndCollectScans(String sql) {
    List<DeviceTableScanNode> scans = new ArrayList<>();
    plan(sql).getFragments().forEach(fragment -> collectScans(fragment.getPlanNodeTree(), scans));
    return scans;
  }

  private static DistributedQueryPlan plan(String sql) {
    // A fresh context per plan: an MPPQueryContext remembers whether the statement it analyzed was
    // an EXPLAIN ANALYZE, so reusing one across these cases would make a plain query be planned as
    // if it were still explaining the previous one.
    return plan(
        sql,
        new MPPQueryContext(sql, new QueryId("property_driven_test"), SESSION_INFO, null, null));
  }

  private static DistributedQueryPlan plan(String sql, MPPQueryContext queryContext) {
    Analysis analysis = analyzeSQL(sql, TEST_MATADATA, queryContext);
    // force all devices into a single data region, so that any parallelism observed comes from
    // splitting one scan rather than from having one scan per region
    analysis.setDataPartitionInfo(
        MockTableModelDataPartition.constructSingleRegionDataPartition(SINGLE_REGION_DB));

    SymbolAllocator symbolAllocator = new SymbolAllocator();
    LogicalQueryPlan logicalQueryPlan =
        new TableLogicalPlanner(
                queryContext, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);

    return new TableDistributedPlanner(
            analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA, null)
        .plan();
  }

  private static void collectScans(PlanNode node, List<DeviceTableScanNode> scans) {
    if (node == null) {
      return;
    }
    if (node instanceof DeviceTableScanNode) {
      scans.add((DeviceTableScanNode) node);
    }
    node.getChildren().forEach(child -> collectScans(child, scans));
  }

  private static void assertAllScansForbidParallelism(List<PlanNode> nodes, String message) {
    List<DeviceTableScanNode> scans =
        nodes.stream()
            .filter(DeviceTableScanNode.class::isInstance)
            .map(DeviceTableScanNode.class::cast)
            .collect(Collectors.toList());
    assertFalse("expected a table scan", scans.isEmpty());
    scans.forEach(scan -> assertFalse(message, scan.isAllowParallelScan()));
  }
}
