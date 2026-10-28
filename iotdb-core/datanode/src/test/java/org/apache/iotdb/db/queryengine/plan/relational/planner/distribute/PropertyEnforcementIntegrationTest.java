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
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableLogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableHashPartitioningShuffleSinkNode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.DEFAULT_WARNING;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SESSION_INFO;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.PlanPropertyProviderTest.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class PropertyEnforcementIntegrationTest {
  private boolean originalPropertyFlag;
  private boolean originalHashFlag;
  private boolean originalOrderedFlag;
  private int originalBuckets;
  private int originalDataNodeId;
  private final PlanPropertyProvider provider = spy(new PlanPropertyProvider());
  private final PropertyEnforcer enforcer = spy(new PropertyEnforcer());

  @Before
  public void saveConfiguration() {
    originalPropertyFlag =
        IoTDBDescriptor.getInstance().getConfig().isEnablePropertyDrivenPlanning();
    originalHashFlag =
        IoTDBDescriptor.getInstance().getConfig().isEnableTableGroupByHashRepartition();
    originalOrderedFlag = IoTDBDescriptor.getInstance().getConfig().isEnableOrderedParallelScan();
    originalBuckets =
        IoTDBDescriptor.getInstance().getConfig().getTableGroupByHashRepartitionPartitionCount();
    originalDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
  }

  @After
  public void restoreConfiguration() {
    IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(originalPropertyFlag);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setEnableTableGroupByHashRepartition(originalHashFlag);
    IoTDBDescriptor.getInstance().getConfig().setEnableOrderedParallelScan(originalOrderedFlag);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setTableGroupByHashRepartitionPartitionCount(originalBuckets);
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(originalDataNodeId);
  }

  private TableDistributedPlanGenerator generator() {
    MPPQueryContext context = mock(MPPQueryContext.class);
    when(context.getQueryId()).thenReturn(new QueryId("property_integration"));
    return new TableDistributedPlanGenerator(context, null, null, null, provider, enforcer);
  }

  private OutputNode outputOver(List<PlanNode> branches) {
    PlanNode logicalChild = mock(PlanNode.class);
    when(logicalChild.getOutputSymbols()).thenReturn(List.of(KEY));
    when(logicalChild.accept(any(), any())).thenAnswer(call -> branches);
    return new OutputNode(new PlanNodeId("output"), logicalChild, List.of("key"), List.of(KEY));
  }

  @Test
  public void enabledFacadeDelegatesOnceAndRecordsAllActualIds() {
    IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(true);
    List<PlanNode> inputs = List.of(values("a"), values("b"));
    TableDistributedPlanGenerator generator = generator();
    PlanNode output =
        generator
            .genResult(outputOver(inputs), new TableDistributedPlanGenerator.PlanContext())
            .get(0);
    PlanNode collect = output.getChildren().get(0);
    assertTrue(collect instanceof CollectNode);
    assertEquals(inputs, collect.getChildren());
    verify(provider, times(1)).providedByBranches(eq(inputs), isNull(), anyMap());
    verify(enforcer, times(1))
        .enforce(eq(PlanProperties.singleUnordered()), eq(inputs), any(), any(), anyMap());
    PropertyEnforcementDecision d = generator.getPropertyEnforcementDecisions().get(0);
    assertEquals("visitOutput:input", d.getRuleSource());
    assertEquals(List.of(new PlanNodeId("a"), new PlanNodeId("b")), d.getInputIds());
    assertEquals(List.of(collect.getPlanNodeId()), d.getOutputIds());
    assertEquals(PropertyEnforcer.EnforcerKind.COLLECT, d.getActualEnforcer());
  }

  @Test
  public void disabledFacadeKeepsLegacyPathAndDoesNotCallInjectedComponents() {
    IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(false);
    List<PlanNode> inputs = List.of(values("a"), values("b"));
    TableDistributedPlanGenerator generator = generator();
    PlanNode output =
        generator
            .genResult(outputOver(inputs), new TableDistributedPlanGenerator.PlanContext())
            .get(0);
    assertTrue(output.getChildren().get(0) instanceof CollectNode);
    assertEquals(inputs, output.getChildren().get(0).getChildren());
    verifyZeroInteractions(provider, enforcer);
    assertTrue(generator.getPropertyEnforcementDecisions().isEmpty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void singleNodeCompatibilityFacadeRejectsMultipleOutputBranches() {
    generator().enforce(PlanProperties.any(), List.of(values("a"), values("b")), null);
  }

  @Test
  public void unknownSingleInputGetsExplicitCollectAndUnknownReason() {
    IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(true);
    PlanNode unknown = unknown("u");
    TableDistributedPlanGenerator generator = generator();
    PlanNode output =
        generator
            .genResult(
                outputOver(List.of(unknown)), new TableDistributedPlanGenerator.PlanContext())
            .get(0);
    assertTrue(output.getChildren().get(0) instanceof CollectNode);
    assertEquals(List.of(unknown), output.getChildren().get(0).getChildren());
    assertEquals(
        FallbackReason.UNKNOWN_PROVIDER,
        generator.getPropertyEnforcementDecisions().get(0).getFallbackReason());
  }

  @Test
  public void defaultFourArgumentConstructorRemainsUsable() {
    MPPQueryContext context = mock(MPPQueryContext.class);
    when(context.getQueryId()).thenReturn(new QueryId("compatible_constructor"));
    TableDistributedPlanGenerator generator =
        new TableDistributedPlanGenerator(context, null, null, null);
    PlanNode input = values("a");
    assertSame(input, generator.enforce(PlanProperties.singleUnordered(), List.of(input), null));
  }

  private static void collectNodes(PlanNode node, Map<PlanNodeId, PlanNode> nodes) {
    if (nodes.putIfAbsent(node.getPlanNodeId(), node) != null) return;
    for (PlanNode child : node.getChildren()) collectNodes(child, nodes);
  }

  @Test
  public void allEightSwitchCombinationsKeepHashSelectionIndependentOfDiagnostics() {
    for (boolean property : List.of(false, true)) {
      for (boolean hash : List.of(false, true)) {
        for (boolean ordered : List.of(false, true)) {
          IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(property);
          IoTDBDescriptor.getInstance().getConfig().setEnableTableGroupByHashRepartition(hash);
          IoTDBDescriptor.getInstance().getConfig().setEnableOrderedParallelScan(ordered);
          IoTDBDescriptor.getInstance().getConfig().setTableGroupByHashRepartitionPartitionCount(2);
          IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
          String sql = "SELECT s1, count(*) FROM testdb.table1 GROUP BY s1";
          MPPQueryContext context =
              new MPPQueryContext(
                  sql,
                  new QueryId("event_" + property + "_" + hash + "_" + ordered),
                  SESSION_INFO,
                  null,
                  null);
          context.setStartTime(System.currentTimeMillis());
          context.setTimeOut(60000);
          Analysis analysis = analyzeSQL(sql, TEST_MATADATA, context);
          SymbolAllocator allocator = new SymbolAllocator();
          LogicalQueryPlan logical =
              new TableLogicalPlanner(
                      context, TEST_MATADATA, SESSION_INFO, allocator, DEFAULT_WARNING)
                  .plan(analysis);
          analysis.setDataPartitionInfo(
              MockTableModelDataPartition.constructDataPartition("testdb"));
          TableDistributedPlanGenerator generator =
              new TableDistributedPlanGenerator(context, analysis, allocator, null);
          PlanNode root =
              generator
                  .genResult(logical.getRootNode(), new TableDistributedPlanGenerator.PlanContext())
                  .get(0);
          Map<PlanNodeId, PlanNode> nodes = new LinkedHashMap<>();
          collectNodes(root, nodes);
          Set<PlanNodeId> sinks =
              nodes.values().stream()
                  .filter(TableHashPartitioningShuffleSinkNode.class::isInstance)
                  .map(PlanNode::getPlanNodeId)
                  .collect(Collectors.toSet());
          assertEquals(hash, !sinks.isEmpty());
          List<PropertyEnforcementDecision> decisions = generator.getPropertyEnforcementDecisions();
          if (!property) {
            assertTrue(decisions.isEmpty());
            continue;
          }
          List<PropertyEnforcementDecision> hashEvents =
              decisions.stream()
                  .filter(d -> d.getEvent() == PropertyEnforcementDecision.Event.HASH_REPARTITION)
                  .collect(Collectors.toList());
          assertEquals(hash ? 1 : 0, hashEvents.size());
          PropertyEnforcementDecision admission =
              decisions.stream()
                  .filter(d -> d.getEvent() == PropertyEnforcementDecision.Event.GROUP_BY_ADMISSION)
                  .findFirst()
                  .orElseThrow();
          assertEquals(
              hash ? null : "GROUP_BY_ADMISSION:GROUP_BY_HASH_DISABLED", admission.getReasonCode());
          if (hash) {
            PropertyEnforcementDecision event = hashEvents.get(0);
            assertEquals(sinks, new HashSet<>(event.getSourceIds()));
            assertEquals(2, event.getBucketIds().size());
            assertEquals(sinks.size(), event.getExchangeIds().size());
            Set<PlanNodeId> exchangeIds = new HashSet<>();
            for (List<PlanNodeId> row : event.getExchangeIds()) {
              assertEquals(2, row.size());
              for (PlanNodeId exchange : row) {
                assertTrue(nodes.get(exchange) instanceof ExchangeNode);
                assertTrue(exchangeIds.add(exchange));
              }
            }
            for (PlanNodeId bucket : event.getBucketIds()) {
              assertTrue(nodes.get(bucket) instanceof AggregationNode);
              assertEquals(
                  AggregationNode.Step.FINAL, ((AggregationNode) nodes.get(bucket)).getStep());
            }
            assertTrue(event.getProvidedAfter().satisfies(event.getRequired()));
            assertNull(event.getFallbackReason());
          }
        }
      }
    }
  }

  @Test
  public void anUnknownScanProviderRetainsItsExistingScanEligibility() {
    IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(true);
    for (boolean initial : List.of(false, true)) {
      var scan =
          mock(
              org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode
                  .class);
      when(scan.getPlanNodeId()).thenReturn(new PlanNodeId("unknown_scan_" + initial));
      when(scan.getOutputSymbols()).thenReturn(List.of(KEY));
      when(scan.isAllowParallelScan()).thenReturn(initial);
      TableDistributedPlanGenerator generator = generator();
      PlanNode result = generator.enforce(PlanProperties.singleUnordered(), List.of(scan), null);
      assertTrue(result instanceof CollectNode);
      verify(scan, never()).setAllowParallelScan(anyBoolean());
      assertEquals(initial, scan.isAllowParallelScan());
      assertEquals(
          FallbackReason.UNKNOWN_PROVIDER,
          generator.getPropertyEnforcementDecisions().get(0).getFallbackReason());
    }
  }
}
