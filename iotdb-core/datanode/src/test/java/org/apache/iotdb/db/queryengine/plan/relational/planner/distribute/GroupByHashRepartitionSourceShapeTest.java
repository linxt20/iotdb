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

import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableLogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableHashPartitioningShuffleSinkNode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.DEFAULT_WARNING;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SESSION_INFO;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Which source subtrees the grouped hash exchange accepts.
 *
 * <p>The admission rule is a property of the subtree, not a test on the aggregation's direct child.
 * A source stands for one scan region, so the subtree has to be a pipeline over exactly one scan
 * leaf. {@code FilterNode} and {@code ProjectNode} are the two pass-throughs the planner produces
 * in between, and both are reachable: a predicate that cannot be pushed into the scan becomes a
 * filter above it, and a grouping key that is a computed expression becomes a project above it. The
 * assertions below pin both ends of the rule - the shapes that must be admitted, and the subtrees
 * that span more than one scan and must stay on the collect path.
 */
public class GroupByHashRepartitionSourceShapeTest {

  private static final String SINGLE_REGION_DB = "testdb";
  private static final int SOURCE_COUNT = 3;
  private static final int PARTITION_COUNT = 2;

  private boolean originalProperty;
  private boolean originalHash;
  private boolean originalOrdered;
  private int originalBuckets;
  private int originalDataNodeId;

  @Before
  public void applyConfiguration() {
    var config = IoTDBDescriptor.getInstance().getConfig();
    originalProperty = config.isEnablePropertyDrivenPlanning();
    originalHash = config.isEnableTableGroupByHashRepartition();
    originalOrdered = config.isEnableOrderedParallelScan();
    originalBuckets = config.getTableGroupByHashRepartitionPartitionCount();
    originalDataNodeId = config.getDataNodeId();
    config.setDataNodeId(1);
    config.setEnablePropertyDrivenPlanning(false);
    config.setEnableTableGroupByHashRepartition(false);
    config.setEnableOrderedParallelScan(false);
    config.setTableGroupByHashRepartitionPartitionCount(PARTITION_COUNT);
  }

  @After
  public void restoreConfiguration() {
    var config = IoTDBDescriptor.getInstance().getConfig();
    config.setEnablePropertyDrivenPlanning(originalProperty);
    config.setEnableTableGroupByHashRepartition(originalHash);
    config.setEnableOrderedParallelScan(originalOrdered);
    config.setTableGroupByHashRepartitionPartitionCount(originalBuckets);
    config.setDataNodeId(originalDataNodeId);
  }

  @Test
  public void aFilterThatCannotBePushedIntoTheScanKeepsTheHashPath() {
    // IS NOT NULL cannot become a pushdown predicate, so the filter survives above the scan.
    assertExecutableSourceByBucketMatrix(
        "SELECT s1, count(*) FROM testdb.table1 WHERE s1 IS NOT NULL GROUP BY s1");
  }

  @Test
  public void aComputedGroupingKeyBehindAProjectKeepsTheHashPath() {
    assertExecutableSourceByBucketMatrix(
        "SELECT s1 + 1, count(*) FROM testdb.table1 GROUP BY s1 + 1");
  }

  @Test
  public void nestedFilterAndProjectOverOneScanKeepsTheHashPath() {
    PlannedShape shape =
        planAndCollect(
            "SELECT s1 + 1, count(*) FROM testdb.table1" + " WHERE s1 IS NOT NULL GROUP BY s1 + 1");
    assertMatrixIsExecutable(shape);

    // The recursion has to walk through both pass-throughs, not stop at the first one.
    PlanNode partialAggregation = shape.hashSinks.get(0).getChildren().get(0);
    assertEquals("AggregationNode", partialAggregation.getClass().getSimpleName());
    PlanNode project = partialAggregation.getChildren().get(0);
    assertEquals("ProjectNode", project.getClass().getSimpleName());
    PlanNode filter = project.getChildren().get(0);
    assertEquals("FilterNode", filter.getClass().getSimpleName());
    assertEquals("DeviceTableScanNode", filter.getChildren().get(0).getClass().getSimpleName());
  }

  @Test
  public void aPlainScanStillTakesTheHashPath() {
    assertExecutableSourceByBucketMatrix("SELECT s1, count(*) FROM testdb.table1 GROUP BY s1");
  }

  /**
   * The tag path must stay off the hash exchange.
   *
   * <p>This is a regression test on the observable plan, not a test of the source rule. The
   * measured reason the tag query is refused is the aggregation-shape conditions: it arrives with
   * {@code step=FINAL}, non-empty pre-grouped symbols and {@code streamable=true}. The assertion
   * therefore holds whether or not {@code isHashRepartitionSource} excludes {@link
   * org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode}, which
   * reverse verification confirms - forcing the source rule to admit every subtree still leaves
   * this test green while the union and join cases fail. What the test does buy is that a later
   * relaxation of the aggregation conditions cannot quietly start routing the tag path through the
   * hash exchange.
   */
  @Test
  public void theTagPathStaysOffTheHashPath() {
    assertStaysOnTheCollectPath("SELECT tag1, count(*) FROM testdb.table1 GROUP BY tag1");
  }

  @Test
  public void aUnionOfTwoTablesStaysOffTheHashPath() {
    // One UnionNode child stands for six scans. Routing it as a single source would silently make
    // the source count stop matching the number of scans.
    assertStaysOnTheCollectPath(
        "SELECT s1, count(*) FROM (SELECT s1 FROM testdb.table1"
            + " UNION ALL SELECT s1 FROM testdb.table2) u GROUP BY s1");
  }

  @Test
  public void aJoinStaysOffTheHashPath() {
    assertStaysOnTheCollectPath(
        "SELECT t1.s1, count(*) FROM testdb.table1 t1"
            + " JOIN testdb.table2 t2 ON t1.s1 = t2.s1 GROUP BY t1.s1");
  }

  private static void assertExecutableSourceByBucketMatrix(String sql) {
    PlannedShape shape = planAndCollect(sql);
    assertEquals(
        "one hash sink per source, so the source count must match the scan count",
        SOURCE_COUNT,
        shape.hashSinks.size());
    assertMatrixIsExecutable(shape);
  }

  private static void assertMatrixIsExecutable(PlannedShape shape) {
    HashPartitioningDescriptor descriptor = shape.hashSinks.get(0).getPartitioningDescriptor();
    assertEquals(PARTITION_COUNT, descriptor.getPartitionCount());
    List<PlanNodeId> sourceSinkIds =
        shape.hashSinks.stream().map(PlanNode::getPlanNodeId).collect(Collectors.toList());
    List<List<PlanNodeId>> exchangeIds =
        shape.hashSinks.stream()
            .map(
                sink ->
                    sink.getDownStreamChannelLocationList().stream()
                        .map(channel -> new PlanNodeId(channel.getRemotePlanNodeId()))
                        .collect(Collectors.toList()))
            .collect(Collectors.toList());
    assertTrue(
        exchangeIds.toString(),
        exchangeIds.stream().allMatch(ids -> ids.size() == PARTITION_COUNT));

    // Remote exchange transport fetches a ShuffleSinkHandle by its channel index, so a bucket that
    // read channel 0 for every source would silently aggregate one bucket's rows P times.
    Map<PlanNodeId, ExchangeNode> exchangesById =
        shape.nodes.stream()
            .filter(ExchangeNode.class::isInstance)
            .map(ExchangeNode.class::cast)
            .collect(Collectors.toMap(PlanNode::getPlanNodeId, exchange -> exchange));
    for (TableHashPartitioningShuffleSinkNode hashSink : shape.hashSinks) {
      for (int channelIndex = 0;
          channelIndex < hashSink.getDownStreamChannelLocationList().size();
          channelIndex++) {
        PlanNodeId exchangeId =
            new PlanNodeId(
                hashSink
                    .getDownStreamChannelLocationList()
                    .get(channelIndex)
                    .getRemotePlanNodeId());
        assertEquals(
            "each final bucket must fetch its corresponding source shuffle channel",
            channelIndex,
            exchangesById.get(exchangeId).getIndexOfUpstreamSinkHandle());
      }
    }

    TableGroupByHashRepartitionTopology.FragmentTopologyValidation validation =
        TableGroupByHashRepartitionTopology.create(descriptor, sourceSinkIds, exchangeIds)
            .validateFragmentOwnership(shape.fragmentIdByNode);
    assertTrue(
        "every source/bucket edge must terminate in a distinct materialized final fragment: "
            + validation.getFailures(),
        validation.isExecutable());
  }

  private static void assertStaysOnTheCollectPath(String sql) {
    IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableTableGroupByHashRepartition(true);

    PlannedShape shape = planAndCollect(sql);
    assertFalse(
        "a subtree that is not a pipeline over one scan leaf must not be routed as a hash source: "
            + sql,
        shape.nodes.stream().anyMatch(TableHashPartitioningShuffleSinkNode.class::isInstance));
    assertTrue(
        "falling back has to leave a merge node that collects every source: " + sql,
        shape.nodes.stream()
            .anyMatch(node -> node instanceof CollectNode || node instanceof MergeSortNode));
  }

  private static PlannedShape planAndCollect(String sql) {
    IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableTableGroupByHashRepartition(true);
    DistributedQueryPlan plan =
        plan(sql, MockTableModelDataPartition.constructDataPartition(SINGLE_REGION_DB));
    List<PlanNode> nodes = new ArrayList<>();
    Map<PlanNodeId, String> fragmentIdByNode = new HashMap<>();
    List<TableHashPartitioningShuffleSinkNode> hashSinks = new ArrayList<>();
    for (PlanFragment fragment : plan.getFragments()) {
      collect(
          fragment.getPlanNodeTree(),
          nodes,
          fragmentIdByNode,
          fragment.getId().toString(),
          hashSinks);
    }
    return new PlannedShape(nodes, fragmentIdByNode, hashSinks);
  }

  private static void collect(
      PlanNode node,
      List<PlanNode> nodes,
      Map<PlanNodeId, String> fragmentIdByNode,
      String fragmentId,
      List<TableHashPartitioningShuffleSinkNode> hashSinks) {
    if (node == null) {
      return;
    }
    nodes.add(node);
    fragmentIdByNode.put(node.getPlanNodeId(), fragmentId);
    if (node instanceof TableHashPartitioningShuffleSinkNode) {
      hashSinks.add((TableHashPartitioningShuffleSinkNode) node);
    }
    for (PlanNode child : node.getChildren()) {
      collect(child, nodes, fragmentIdByNode, fragmentId, hashSinks);
    }
  }

  private static final class PlannedShape {
    private final List<PlanNode> nodes;
    private final Map<PlanNodeId, String> fragmentIdByNode;
    private final List<TableHashPartitioningShuffleSinkNode> hashSinks;

    private PlannedShape(
        List<PlanNode> nodes,
        Map<PlanNodeId, String> fragmentIdByNode,
        List<TableHashPartitioningShuffleSinkNode> hashSinks) {
      this.nodes = nodes;
      this.fragmentIdByNode = fragmentIdByNode;
      this.hashSinks = hashSinks;
    }
  }

  private static DistributedQueryPlan plan(String sql, DataPartition dataPartition) {
    MPPQueryContext queryContext =
        new MPPQueryContext(sql, new QueryId("source_shape"), SESSION_INFO, null, null);
    Analysis analysis = analyzeSQL(sql, TEST_MATADATA, queryContext);
    analysis.setDataPartitionInfo(dataPartition);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    LogicalQueryPlan logicalQueryPlan =
        new TableLogicalPlanner(
                queryContext, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);
    return new TableDistributedPlanner(
            analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA, null)
        .plan();
  }
}
