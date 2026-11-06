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
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Planner guardrails for the deliberately narrow executable GROUP BY hash exchange.
 *
 * <p>These assertions inspect fragments after {@link SubPlanGenerator} has split the shared hash
 * source. They therefore prove that the two final exchanges remain distinct plan nodes while the
 * source holds both channels; an assertion only against the logical tree could accidentally pass
 * for two aliases of one serial exchange.
 */
public class SingleSourceGroupByHashRepartitionPlanningTest {

  private static final String SINGLE_REGION_DB = "testdb";

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
    config.setEnablePropertyDrivenPlanning(false);
    config.setEnableTableGroupByHashRepartition(false);
    config.setEnableOrderedParallelScan(false);
    config.setTableGroupByHashRepartitionPartitionCount(2);
    config.setDataNodeId(1);
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
  public void directSingleSourceGroupByBuildsOneHashSourceAndTwoFinalExchanges() {
    IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableTableGroupByHashRepartition(true);

    List<PlanNode> nodes =
        planAndCollectNodes("SELECT s1, count(*) FROM testdb.table1 GROUP BY s1");
    List<TableHashPartitioningShuffleSinkNode> hashSinks =
        nodes.stream()
            .filter(TableHashPartitioningShuffleSinkNode.class::isInstance)
            .map(TableHashPartitioningShuffleSinkNode.class::cast)
            .collect(Collectors.toList());
    assertEquals(1, hashSinks.size());
    TableHashPartitioningShuffleSinkNode hashSink = hashSinks.get(0);
    assertEquals(2, hashSink.getPartitioningDescriptor().getPartitionCount());
    assertEquals(2, hashSink.getDownStreamChannelLocationList().size());
    assertEquals(
        2,
        nodes.stream()
            .filter(ExchangeNode.class::isInstance)
            .map(ExchangeNode.class::cast)
            .map(exchange -> exchange.getPlanNodeId().toString())
            .filter(
                exchangeId ->
                    hashSink.getDownStreamChannelLocationList().stream()
                        .anyMatch(channel -> exchangeId.equals(channel.getRemotePlanNodeId())))
            .count());
    assertTrue(
        "the selected node must expose its versioned hash contract",
        hashSink.getPartitioningDescriptor().getPartitioningSymbols().toString().contains("s1"));
  }

  @Test
  public void switchOffKeepsTheCollectFallback() {
    assertFalse(
        planAndCollectNodes("SELECT s1, count(*) FROM testdb.table1 GROUP BY s1").stream()
            .anyMatch(TableHashPartitioningShuffleSinkNode.class::isInstance));
  }

  @Test
  public void aComputedGroupingExpressionTakesTheHashPath() {
    IoTDBDescriptor.getInstance().getConfig().setEnableTableGroupByHashRepartition(true);

    // The project between the scan and the partial aggregation does not change the routing answer:
    // the sink hashes the partial aggregation's output, which carries the grouping key whatever
    // expression produced it. The source-shape rule and the N x P ownership of this form are
    // asserted in GroupByHashRepartitionSourceShapeTest.
    assertTrue(
        planAndCollectNodes("SELECT s1 + 1, count(*) FROM testdb.table1 GROUP BY s1 + 1").stream()
            .anyMatch(TableHashPartitioningShuffleSinkNode.class::isInstance));
  }

  @Test
  public void directMultiSourceGroupByBuildsExecutableSourceByBucketFragments() {
    IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableTableGroupByHashRepartition(true);

    DistributedQueryPlan plan =
        plan(
            "SELECT s1, count(*) FROM testdb.table1 GROUP BY s1",
            MockTableModelDataPartition.constructDataPartition(SINGLE_REGION_DB));
    List<PlanNode> nodes = new ArrayList<>();
    Map<PlanNodeId, String> fragmentIdByNode = new HashMap<>();
    plan.getFragments()
        .forEach(
            fragment ->
                collectAll(
                    fragment.getPlanNodeTree(),
                    nodes,
                    fragmentIdByNode,
                    fragment.getId().toString()));
    List<TableHashPartitioningShuffleSinkNode> hashSinks =
        nodes.stream()
            .filter(TableHashPartitioningShuffleSinkNode.class::isInstance)
            .map(TableHashPartitioningShuffleSinkNode.class::cast)
            .collect(Collectors.toList());

    assertTrue(
        "the multi-region fixture must produce more than one partial source", hashSinks.size() > 1);
    HashPartitioningDescriptor descriptor = hashSinks.get(0).getPartitioningDescriptor();
    List<PlanNodeId> sourceSinkIds =
        hashSinks.stream().map(PlanNode::getPlanNodeId).collect(Collectors.toList());
    List<List<PlanNodeId>> exchangeIds =
        hashSinks.stream()
            .map(
                sink ->
                    sink.getDownStreamChannelLocationList().stream()
                        .map(channel -> new PlanNodeId(channel.getRemotePlanNodeId()))
                        .collect(Collectors.toList()))
            .collect(Collectors.toList());
    assertTrue(exchangeIds.stream().allMatch(ids -> ids.size() == descriptor.getPartitionCount()));

    // Remote exchange transport fetches a ShuffleSinkHandle by its channel index. Verify every
    // source-to-bucket exchange consumes the matching channel, not channel 0 for every bucket.
    Map<PlanNodeId, ExchangeNode> exchangesById =
        nodes.stream()
            .filter(ExchangeNode.class::isInstance)
            .map(ExchangeNode.class::cast)
            .collect(Collectors.toMap(PlanNode::getPlanNodeId, exchange -> exchange));
    for (TableHashPartitioningShuffleSinkNode hashSink : hashSinks) {
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
            .validateFragmentOwnership(fragmentIdByNode);
    assertTrue(
        "every source/bucket edge must terminate in a distinct materialized final fragment: "
            + validation.getFailures(),
        validation.isExecutable());
  }

  @Test
  public void mixedPlainAndDistinctAggregationKeepsTheCollectFallback() {
    IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableTableGroupByHashRepartition(true);

    assertFalse(
        "one distinct aggregation anywhere in the list must reject the whole hash path",
        planAndCollectNodes(
                "SELECT s1, count(*), count(distinct s2) FROM testdb.table1 GROUP BY s1")
            .stream()
            .anyMatch(TableHashPartitioningShuffleSinkNode.class::isInstance));
  }

  @Test
  public void distinctGroupingIsRepartitionedByItsOwnGroupingKeys() {
    IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(true);
    IoTDBDescriptor.getInstance().getConfig().setEnableTableGroupByHashRepartition(true);

    List<TableHashPartitioningShuffleSinkNode> hashSinks =
        planAndCollectNodes("SELECT s1, count(distinct s2) FROM testdb.table1 GROUP BY s1").stream()
            .filter(TableHashPartitioningShuffleSinkNode.class::isInstance)
            .map(TableHashPartitioningShuffleSinkNode.class::cast)
            .collect(Collectors.toList());
    assertEquals("the single-region fixture yields exactly one hash source", 1, hashSinks.size());
    List<Symbol> partitionKeys =
        hashSinks.get(0).getPartitioningDescriptor().getPartitioningSymbols();
    assertEquals(
        "the distinct argument must join the grouping keys so every bucket dedups completely",
        2,
        partitionKeys.size());
    assertTrue(partitionKeys.toString().contains("s2"));
    assertTrue(partitionKeys.toString().contains("s1"));
  }

  private static List<PlanNode> planAndCollectNodes(String sql) {
    List<PlanNode> nodes = new ArrayList<>();
    plan(sql).getFragments().forEach(fragment -> collectAll(fragment.getPlanNodeTree(), nodes));
    return nodes;
  }

  private static DistributedQueryPlan plan(String sql) {
    return plan(
        sql, MockTableModelDataPartition.constructSingleRegionDataPartition(SINGLE_REGION_DB));
  }

  private static DistributedQueryPlan plan(String sql, DataPartition dataPartition) {
    return planWithTypes(sql, dataPartition).plan;
  }

  private static final class PlannedQuery {
    private final DistributedQueryPlan plan;
    private final SymbolAllocator allocator;
    private final MPPQueryContext context;

    private PlannedQuery(
        DistributedQueryPlan plan, SymbolAllocator allocator, MPPQueryContext context) {
      this.plan = plan;
      this.allocator = allocator;
      this.context = context;
    }
  }

  private static PlannedQuery planWithTypes(String sql, DataPartition dataPartition) {
    MPPQueryContext queryContext =
        new MPPQueryContext(
            sql, new QueryId("single_source_group_by_hash"), SESSION_INFO, null, null);
    queryContext.setStartTime(System.currentTimeMillis());
    queryContext.setTimeOut(60000);
    Analysis analysis = analyzeSQL(sql, TEST_MATADATA, queryContext);
    analysis.setDataPartitionInfo(dataPartition);
    SymbolAllocator symbolAllocator = new SymbolAllocator();
    LogicalQueryPlan logicalQueryPlan =
        new TableLogicalPlanner(
                queryContext, TEST_MATADATA, SESSION_INFO, symbolAllocator, DEFAULT_WARNING)
            .plan(analysis);
    DistributedQueryPlan plan =
        new TableDistributedPlanner(
                analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA, null)
            .plan();
    return new PlannedQuery(plan, symbolAllocator, queryContext);
  }

  private static void collectAll(PlanNode node, List<PlanNode> nodes) {
    if (node == null) {
      return;
    }
    nodes.add(node);
    node.getChildren().forEach(child -> collectAll(child, nodes));
  }

  private static void collectAll(
      PlanNode node,
      List<PlanNode> nodes,
      Map<PlanNodeId, String> fragmentIdByNode,
      String fragmentId) {
    if (node == null) {
      return;
    }
    nodes.add(node);
    fragmentIdByNode.put(node.getPlanNodeId(), fragmentId);
    node.getChildren().forEach(child -> collectAll(child, nodes, fragmentIdByNode, fragmentId));
  }

  @Test
  public void singleSourceHashKeysKeepTheirTypesThroughFragmentsAndServiceEncoding()
      throws Exception {
    assertTypesThroughFragments(
        MockTableModelDataPartition.constructSingleRegionDataPartition(SINGLE_REGION_DB), 1);
  }

  @Test
  public void multipleSourceHashKeysKeepTheirTypesThroughFragmentsAndServiceEncoding()
      throws Exception {
    assertTypesThroughFragments(
        MockTableModelDataPartition.constructDataPartition(SINGLE_REGION_DB), 3);
  }

  private static void assertTypesThroughFragments(DataPartition partitions, int expectedSources)
      throws Exception {
    for (boolean property : new boolean[] {false, true}) {
      var config = IoTDBDescriptor.getInstance().getConfig();
      config.setEnablePropertyDrivenPlanning(property);
      config.setEnableTableGroupByHashRepartition(true);
      PlannedQuery query =
          planWithTypes("SELECT s1, count(*) FROM testdb.table1 GROUP BY s1", partitions);
      int sources = 0, finalGroups = 0;
      for (PlanFragment fragment : query.plan.getFragments()) {
        TypeProvider types = fragment.getTypeProvider();
        assertNotNull("every read fragment must retain its extracted types", types);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        types.serialize(new DataOutputStream(bytes));
        ByteBuffer encoded = ByteBuffer.wrap(bytes.toByteArray());
        TypeProvider decoded = TypeProvider.deserialize(encoded);
        assertEquals("type decoding must consume the complete provider", 0, encoded.remaining());
        List<PlanNode> nodes = new ArrayList<>();
        collectAll(fragment.getPlanNodeTree(), nodes);
        for (PlanNode node : nodes) {
          List<Symbol> keys = new ArrayList<>();
          if (node instanceof TableHashPartitioningShuffleSinkNode) {
            sources++;
            keys.addAll(
                ((TableHashPartitioningShuffleSinkNode) node)
                    .getPartitioningDescriptor()
                    .getPartitioningSymbols());
          } else if (node instanceof AggregationNode) {
            AggregationNode aggregation = (AggregationNode) node;
            keys.addAll(aggregation.getGroupingKeys());
            if (aggregation.getStep() == AggregationNode.Step.FINAL && !keys.isEmpty())
              finalGroups++;
          }
          for (Symbol key : keys) {
            var planned = query.allocator.getTypes().getTableModelType(key);
            assertEquals(
                key.toString(),
                planned.getTypeEnum(),
                query.context.getTypeProvider().getTableModelType(key).getTypeEnum());
            assertEquals(
                key.toString(), planned.getTypeEnum(), types.getTableModelType(key).getTypeEnum());
            assertEquals(
                key.toString(),
                planned.getTypeEnum(),
                decoded.getTableModelType(key).getTypeEnum());
          }
        }
      }
      assertEquals(expectedSources, sources);
      assertEquals("both hash buckets need a typed final aggregation", 2, finalGroups);
    }
  }
}
