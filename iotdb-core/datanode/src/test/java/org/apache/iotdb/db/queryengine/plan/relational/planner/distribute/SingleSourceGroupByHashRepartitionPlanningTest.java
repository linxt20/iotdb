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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableLogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableHashPartitioningShuffleSinkNode;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.DEFAULT_WARNING;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SESSION_INFO;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

  @BeforeClass
  public static void setUpClass() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
  }

  @Before
  @After
  public void resetExperimentalFlags() {
    IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(false);
    IoTDBDescriptor.getInstance().getConfig().setEnableTableGroupByHashRepartition(false);
    IoTDBDescriptor.getInstance().getConfig().setTableGroupByHashRepartitionPartitionCount(2);
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
  public void switchOffAndNonDirectInputKeepTheCollectFallback() {
    assertFalse(
        planAndCollectNodes("SELECT s1, count(*) FROM testdb.table1 GROUP BY s1").stream()
            .anyMatch(TableHashPartitioningShuffleSinkNode.class::isInstance));

    IoTDBDescriptor.getInstance().getConfig().setEnableTableGroupByHashRepartition(true);
    assertFalse(
        "a computed grouping expression introduces a Project input and must wait for the general N x N path",
        planAndCollectNodes(
                "SELECT s1 + 1, count(*) FROM testdb.table1 GROUP BY s1 + 1")
            .stream()
            .anyMatch(TableHashPartitioningShuffleSinkNode.class::isInstance));
  }

  private static List<PlanNode> planAndCollectNodes(String sql) {
    List<PlanNode> nodes = new ArrayList<>();
    plan(sql).getFragments().forEach(fragment -> collectAll(fragment.getPlanNodeTree(), nodes));
    return nodes;
  }

  private static DistributedQueryPlan plan(String sql) {
    MPPQueryContext queryContext =
        new MPPQueryContext(
            sql, new QueryId("single_source_group_by_hash"), SESSION_INFO, null, null);
    Analysis analysis = analyzeSQL(sql, TEST_MATADATA, queryContext);
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

  private static void collectAll(PlanNode node, List<PlanNode> nodes) {
    if (node == null) {
      return;
    }
    nodes.add(node);
    node.getChildren().forEach(child -> collectAll(child, nodes));
  }
}
