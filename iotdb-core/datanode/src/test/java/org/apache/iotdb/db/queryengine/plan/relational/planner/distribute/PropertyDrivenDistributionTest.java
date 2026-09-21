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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValuesNode;
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

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.TsBlock;
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
  }

  /**
   * A scan that already provides device-then-time order normally eliminates its SortNode. The
   * ordered local-parallel experiment keeps that node as a serialized execution marker so the
   * DataNode can construct its merge tree even when the fragment runs remotely.
   */
  @Test
  public void orderedParallelScanRetainsNaturalSortAsExecutionMarker() {
    assumeFalse(propertyDrivenPlanning);
    try {
      IoTDBDescriptor.getInstance().getConfig().setEnableOrderedParallelScan(false);
      assertFalse(
          planAndCollectNodes("SELECT * FROM testdb.table1 ORDER BY tag1, tag2, tag3, time")
              .stream()
              .anyMatch(node -> node instanceof SortNode));

      IoTDBDescriptor.getInstance().getConfig().setEnableOrderedParallelScan(true);
      assertTrue(
          planAndCollectNodes("SELECT * FROM testdb.table1 ORDER BY tag1, tag2, tag3, time")
              .stream()
              .anyMatch(node -> node instanceof SortNode));
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setEnableOrderedParallelScan(false);
    }
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

    PlanNode enforced =
        new TableDistributedPlanGenerator(
                new MPPQueryContext(
                    "property enforcement test",
                    new QueryId("property_enforcement_test"),
                    SESSION_INFO,
                    null,
                    null),
                null,
                null,
                null)
            .enforce(
                PlanProperties.of(DistributionProperty.single(), timeAscending),
                Collections.singletonList(unorderedChild),
                null);

    assertTrue(
        "an unordered physical child cannot satisfy an ordered requirement",
        enforced instanceof SortNode);
    assertEquals(unorderedChild, enforced.getChildren().get(0));
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
  public void memorySourceExplainShowsThePropertyEnforcementTraceOnlyWhenEnabled() {
    // This case drives the flag itself, so it only needs to run once.
    assumeFalse(propertyDrivenPlanning);

    setPropertyDrivenPlanning(false);
    assertFalse(
        "the legacy heuristic takes no property decisions, so it must not print a trace",
        memorySourceExplainText("SELECT * FROM testdb.table1").contains("Property enforcement:"));

    setPropertyDrivenPlanning(true);
    String explained = memorySourceExplainText("SELECT * FROM testdb.table1");
    assertTrue(
        "EXPLAIN must show the property enforcement trace when the rules are enabled",
        explained.contains("Property enforcement:"));
    assertTrue(
        "each traced decision must state what was required and what was provided, got:\n"
            + explained,
        explained.contains("required=") && explained.contains("provided="));
  }

  /** Exercises the table-model EXPLAIN memory-source path used by the product query flow. */
  private static String memorySourceExplainText(String sql) {
    MPPQueryContext queryContext =
        new MPPQueryContext(
            "EXPLAIN " + sql, new QueryId("explain_test"), SESSION_INFO, null, null);
    Analysis analysis = analyzeSQL("EXPLAIN " + sql, TEST_MATADATA, queryContext);
    analysis.setDataPartitionInfo(
        MockTableModelDataPartition.constructSingleRegionDataPartition(SINGLE_REGION_DB));
    TsBlock result = analysis.constructResultForMemorySource(queryContext);
    StringBuilder explained = new StringBuilder();
    for (int position = 0; position < result.getPositionCount(); position++) {
      explained
          .append(
              result.getColumn(0).getBinary(position).getStringValue(TSFileConfig.STRING_CHARSET))
          .append('\n');
    }
    return explained.toString();
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
   * A RowNumber without an order-sensitive input can collect its fragment branches in any order. It
   * must therefore use the common merge path and retain scan parallelism, rather than silently
   * bypassing the marker as the old hand-written CollectNode path did.
   */
  @Test
  public void unorderedRowNumberAllowsParallelScan() {
    assertAllScansAllowParallel("SELECT row_number() OVER () FROM testdb.table1");
  }

  /**
   * Window functions without an ORDER BY also have no input ordering requirement. This is the
   * window counterpart of {@link #unorderedRowNumberAllowsParallelScan()}.
   */
  @Test
  public void unorderedWindowAllowsParallelScan() {
    assertAllScansAllowParallel("SELECT count(*) OVER () FROM testdb.table1");
  }

  /**
   * In contrast, a RowNumber whose result depends on the order of its input must keep the scan on
   * one stream. This guards the order-sensitive side of the shared merge path.
   */
  @Test
  public void orderedRowNumberForbidsParallelScan() {
    List<DeviceTableScanNode> scans =
        planAndCollectScans("SELECT row_number() OVER (ORDER BY time) FROM testdb.table1");

    assertFalse(scans.isEmpty());
    for (DeviceTableScanNode scan : scans) {
      assertFalse(
          "an order-sensitive RowNumber must not split its scan into parallel drivers",
          scan.isAllowParallelScan());
    }
  }

  private static void assertAllScansAllowParallel(String sql) {
    List<DeviceTableScanNode> scans = planAndCollectScans(sql);

    assertFalse(scans.isEmpty());
    for (DeviceTableScanNode scan : scans) {
      assertTrue(
          "an unordered operator must not prevent scan parallelism", scan.isAllowParallelScan());
    }
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
}
