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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.execution.operator.process.TableReadyCollectOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;

import org.junit.*;

import java.util.*;

import static org.junit.Assert.*;

public class ScanSourceIdentityIntegrationTest {
  private final DeviceTableScanParallelPipelineTest fixtures =
      new DeviceTableScanParallelPipelineTest();
  private final DataNodeTableOperatorGenerator generator =
      new DataNodeTableOperatorGenerator(new TableMetadataImpl());

  @BeforeClass
  public static void setup() {
    DeviceTableScanParallelPipelineTest.setUp();
  }

  @AfterClass
  public static void cleanup() {
    DeviceTableScanParallelPipelineTest.tearDown();
  }

  @Test
  public void serialScanRetainsItsWorkUnitWithoutInventingAnExchange() throws Exception {
    LocalExecutionPlanContext context = fixtures.createLocalExecutionPlanContext("serial_identity");
    context.setDegreeOfParallelism(1);
    DeviceTableScanNode node = fixtures.initDeviceTableScanNode(4);
    node.setAllowParallelScan(true);
    Operator root = node.accept(generator, context);
    try {
      assertEquals(1, context.getInstanceContext().getWorkUnitIds().size());
      assertEquals(1, context.getInstanceContext().getBucketIds().size());
      assertTrue(context.getInstanceContext().getExchangeEdgeIds().isEmpty());
      assertEquals("DOP_NOT_GREATER_THAN_ONE", context.getInstanceContext().getSplitReason());
    } finally {
      root.close();
    }
  }

  @Test
  public void twoSourcesHaveAStableUnionInsteadOfOverwritingEachOther() throws Exception {
    LocalExecutionPlanContext forward = twoSources(false), reverse = twoSources(true);
    assertEquals(2, forward.getInstanceContext().getWorkUnitIds().size());
    assertEquals(Arrays.asList(0, 1), forward.getInstanceContext().getBucketIds());
    assertEquals(
        forward.getInstanceContext().getWorkUnitIds(),
        reverse.getInstanceContext().getWorkUnitIds());
    assertEquals(
        forward.getInstanceContext().getPlanIdentityChecksum(),
        reverse.getInstanceContext().getPlanIdentityChecksum());
  }

  private LocalExecutionPlanContext twoSources(boolean reverse) throws Exception {
    LocalExecutionPlanContext context =
        fixtures.createLocalExecutionPlanContext("multi_source_identity");
    context.setDegreeOfParallelism(1);
    List<String> ids =
        reverse ? Arrays.asList("source_b", "source_a") : Arrays.asList("source_a", "source_b");
    for (String id : ids) {
      DeviceTableScanNode node = fixtures.initDeviceTableScanNode(4);
      node.setPlanNodeId(new PlanNodeId(id));
      node.setAllowParallelScan(false);
      Operator scan = node.accept(generator, context);
      scan.close();
    }
    return context;
  }

  @Test
  public void exchangeIdentitiesReferToTheConstructedOperators() throws Exception {
    LocalExecutionPlanContext context = fixtures.createLocalExecutionPlanContext("edge_identity");
    context.setDegreeOfParallelism(2);
    DeviceTableScanNode node = fixtures.initDeviceTableScanNode(4);
    node.setAllowParallelScan(true);
    Operator root = node.accept(generator, context);
    try {
      List<String> edges = context.getInstanceContext().getExchangeEdgeIds();
      assertEquals(2, edges.size());
      for (Operator child : ((TableReadyCollectOperator) root).getChildren()) {
        ExchangeOperator exchange = (ExchangeOperator) child;
        String expected =
            context.getFragmentInstanceId().getFullId()
                + "/pipeline/"
                + context.getDriverContext().getDriverTaskID().getPipelineId()
                + "/operator/"
                + exchange.getOperatorContext().getOperatorId()
                + "/source/"
                + exchange.getSourceId().getId();
        assertTrue(edges.contains(expected));
      }
    } finally {
      root.close();
      for (PipelineDriverFactory factory : context.getPipelineDriverFactories())
        factory.closeUncreated();
    }
  }

  @Test
  public void fragmentWithoutManagedScanRetainsRealExchangeEdges() {
    LocalExecutionPlanContext context = fixtures.createLocalExecutionPlanContext("root_edges");
    context.getInstanceContext().recordActualExchangeEdge("root/actual-exchange");
    org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode root =
        org.mockito.Mockito.mock(
            org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode.class);
    org.mockito.Mockito.when(root.getPlanNodeId()).thenReturn(new PlanNodeId("root-plan"));
    LocalExecutionPlanner.recordFragmentPlanIdentity(root, context);
    assertEquals("NO_MANAGED_SCAN", context.getInstanceContext().getSplitMode());
    assertEquals(0, context.getInstanceContext().getPlannedDop());
    assertTrue(context.getInstanceContext().getWorkUnitIds().isEmpty());
    assertEquals(
        Collections.singletonList("root/actual-exchange"),
        context.getInstanceContext().getExchangeEdgeIds());
    assertNotNull(context.getInstanceContext().getPlanIdentityChecksum());
  }
}
