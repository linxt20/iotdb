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
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.DEFAULT_WARNING;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SESSION_INFO;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Plan shape assertions for the property driven distribution rules. Nothing here starts the
 * execution engine: the planner is run on a mocked partition and the resulting plan is inspected.
 *
 * <p>The EXPLAIN ANALYZE cases are the regression test for the bug where wrapping a query in
 * EXPLAIN ANALYZE silently changed the plan it was supposed to report on: the scan's parent became
 * an ExplainAnalyzeNode, which merged its child without ever marking the scan as parallelizable, so
 * every query observed that way ran with a single scan driver.
 */
public class PropertyDrivenDistributionTest {

  private static final String SINGLE_REGION_DB = "testdb";

  @BeforeClass
  public static void setUp() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
  }

  /** A plain query whose parent imposes no ordering: the scan may be split into parallel drivers. */
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

  /** Plans the statement against a single data region and returns every scan node in the plan. */
  private static List<DeviceTableScanNode> planAndCollectScans(String sql) {
    // A fresh context per plan: an MPPQueryContext remembers whether the statement it analyzed was
    // an EXPLAIN ANALYZE, so reusing one across these cases would make a plain query be planned as
    // if it were still explaining the previous one.
    MPPQueryContext queryContext =
        new MPPQueryContext(sql, new QueryId("property_driven_test"), SESSION_INFO, null, null);

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

    DistributedQueryPlan distributedQueryPlan =
        new TableDistributedPlanner(analysis, symbolAllocator, logicalQueryPlan, TEST_MATADATA, null)
            .plan();

    List<DeviceTableScanNode> scans = new ArrayList<>();
    distributedQueryPlan
        .getFragments()
        .forEach(fragment -> collectScans(fragment.getPlanNodeTree(), scans));
    return scans;
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
