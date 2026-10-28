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
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValuesNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.MockTableModelDataPartition;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile.ExternalTsFileQueryResource;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile.ExternalTsFileQueryResource.DeviceTaskPartition;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.spill.DeviceEntryDataSet;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.spill.DeviceEntryMaterializer;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolAllocator;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableLogicalPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CopyToNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExternalTsFileAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.IntoNode;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.AnalyzerTest.analyzeSQL;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestMetadata.DEVICE_VIEW_TEST_TABLE;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestMetadata.TREE_VIEW_DB;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.DEFAULT_WARNING;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.SESSION_INFO;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.TestUtils.TEST_MATADATA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Baselines for specialized consumers and exceptional multi-branch inputs. */
public class SpecializedPlanningBaselineTest {
  private static final AtomicInteger IDS = new AtomicInteger();
  private boolean property, hash, ordered;
  private int dataNode;
  private String sortDirectory;

  @Before
  public void saveConfiguration() {
    var c = IoTDBDescriptor.getInstance().getConfig();
    property = c.isEnablePropertyDrivenPlanning();
    hash = c.isEnableTableGroupByHashRepartition();
    ordered = c.isEnableOrderedParallelScan();
    dataNode = c.getDataNodeId();
    sortDirectory = c.getSortTmpDir();
    c.setDataNodeId(1);
    c.setEnableTableGroupByHashRepartition(false);
    c.setEnableOrderedParallelScan(false);
  }

  @After
  public void restoreConfiguration() {
    var c = IoTDBDescriptor.getInstance().getConfig();
    c.setEnablePropertyDrivenPlanning(property);
    c.setEnableTableGroupByHashRepartition(hash);
    c.setEnableOrderedParallelScan(ordered);
    c.setDataNodeId(dataNode);
    c.setSortTmpDir(sortDirectory);
    DataNodeTableCache.getInstance().invalid(TREE_VIEW_DB);
  }

  private static PlanNodeId id() {
    return new PlanNodeId("a0_" + IDS.incrementAndGet());
  }

  private static final class Fixture {
    final MPPQueryContext context;
    final Analysis analysis;
    final SymbolAllocator allocator = new SymbolAllocator();
    final LogicalQueryPlan logical;

    Fixture(String sql) {
      context =
          new MPPQueryContext(
              sql, new QueryId("a0_special_" + IDS.incrementAndGet()), SESSION_INFO, null, null);
      context.setStartTime(System.currentTimeMillis());
      context.setTimeOut(60000);
      analysis = analyzeSQL(sql, TEST_MATADATA, context);
      logical =
          new TableLogicalPlanner(context, TEST_MATADATA, SESSION_INFO, allocator, DEFAULT_WARNING)
              .plan(analysis);
      if (!sql.contains(TREE_VIEW_DB))
        analysis.setDataPartitionInfo(MockTableModelDataPartition.constructDataPartition("testdb"));
    }

    TableDistributedPlanGenerator generator() {
      return new TableDistributedPlanGenerator(context, analysis, allocator, null);
    }

    List<PlanNode> distribute(PlanNode root) {
      return generator().genResult(root, new TableDistributedPlanGenerator.PlanContext());
    }
  }

  private static List<PlanNode> nodes(PlanNode root) {
    List<PlanNode> result = new ArrayList<>();
    walk(root, result);
    return result;
  }

  private static void walk(PlanNode node, List<PlanNode> out) {
    out.add(node);
    for (PlanNode child : node.getChildren()) if (child != null) walk(child, out);
  }

  private static <T> T find(PlanNode root, Class<T> type) {
    return nodes(root).stream().filter(type::isInstance).map(type::cast).findFirst().orElseThrow();
  }

  private static PlanNode branches(List<Symbol> outputs) {
    List<PlanNode> values =
        Arrays.asList(
            new ValuesNode(id(), outputs, Collections.emptyList()),
            new ValuesNode(id(), outputs, Collections.emptyList()));
    PlanNode source = mock(PlanNode.class);
    when(source.getOutputSymbols()).thenReturn(outputs);
    when(source.accept(any(), any())).thenAnswer(call -> values);
    return source;
  }

  private static void assertCollected(PlanNode consumer) {
    assertEquals(1, consumer.getChildren().size());
    PlanNode merge = consumer.getChildren().get(0);
    assertTrue(merge instanceof CollectNode);
    assertEquals(2, merge.getChildren().size());
    assertNotEquals(
        merge.getChildren().get(0).getPlanNodeId(), merge.getChildren().get(1).getPlanNodeId());
  }

  @Test
  public void diffProjectKeepsBothUnorderedBranches() {
    for (boolean flag : new boolean[] {false, true}) {
      IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(flag);
      Fixture f = new Fixture("SELECT * FROM testdb.table1");
      Symbol input = new Symbol("s1"), output = new Symbol("delta");
      ProjectNode project =
          new ProjectNode(
              id(),
              branches(Collections.singletonList(input)),
              Assignments.builder()
                  .put(
                      output,
                      new FunctionCall(
                          QualifiedName.of("diff"),
                          Collections.singletonList(input.toSymbolReference())))
                  .build());
      List<PlanNode> result = f.distribute(project);
      assertEquals(1, result.size());
      assertCollected(result.get(0));
    }
  }

  @Test
  public void diffFilterKeepsBothUnorderedBranches() {
    for (boolean flag : new boolean[] {false, true}) {
      IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(flag);
      Fixture f = new Fixture("SELECT * FROM testdb.table1");
      Symbol input = new Symbol("s1");
      FunctionCall diff =
          new FunctionCall(
              QualifiedName.of("diff"), Collections.singletonList(input.toSymbolReference()));
      FilterNode filter =
          new FilterNode(
              id(),
              branches(Collections.singletonList(input)),
              new ComparisonExpression(
                  ComparisonExpression.Operator.GREATER_THAN, diff, new LongLiteral("0")));
      assertCollected(f.distribute(filter).get(0));
    }
  }

  @Test
  public void distinctOrMaskedAggregationKeepsSingleStepOverBothBranches() {
    for (boolean flag : new boolean[] {false, true}) {
      IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(flag);
      Fixture f = new Fixture("SELECT count(DISTINCT s1), sum(s2) FROM testdb.table1");
      AggregationNode aggregate = find(f.logical.getRootNode(), AggregationNode.class);
      assertTrue(
          aggregate.getAggregations().values().stream()
              .anyMatch(a -> a.isDistinct() || a.hasMask()));
      aggregate.setChild(branches(aggregate.getChild().getOutputSymbols()));
      AggregationNode result = (AggregationNode) f.distribute(aggregate).get(0);
      assertEquals(AggregationNode.Step.SINGLE, result.getStep());
      assertCollected(result);
    }
  }

  @Test
  public void unorderedWindowConvergesExceptionalBranchInputs() {
    for (boolean flag : new boolean[] {false, true}) {
      IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(flag);
      Fixture f = new Fixture("SELECT sum(s1) OVER () FROM testdb.table1");
      WindowNode window = find(f.logical.getRootNode(), WindowNode.class);
      window.setChild(branches(window.getChild().getOutputSymbols()));
      assertCollected(f.distribute(window).get(0));
    }
  }

  @Test
  public void setSemanticTableFunctionConvergesUnpartitionedInputs() {
    for (boolean flag : new boolean[] {false, true}) {
      IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(flag);
      Fixture f = new Fixture("SELECT * FROM TABLE(REPEAT(DATA => TABLE(testdb.table1), N => 2))");
      TableFunctionProcessorNode function =
          find(f.logical.getRootNode(), TableFunctionProcessorNode.class);
      assertFalse(function.isRowSemantic());
      function.setChild(branches(function.getChild().getOutputSymbols()));
      assertCollected(f.distribute(function).get(0));
    }
  }

  @Test
  public void scalarExternalAggregationRetainsPartialFinalContract() {
    for (boolean flag : new boolean[] {false, true}) {
      IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(flag);
      Fixture f = new Fixture("SELECT count(s1) FROM testdb.table1");
      AggregationTableScanNode template =
          find(f.logical.getRootNode(), AggregationTableScanNode.class);
      ExternalTsFileQueryResource resource = mock(ExternalTsFileQueryResource.class);
      List<DeviceEntry> entries = template.getDeviceEntries();
      assertTrue(entries.size() > 1);
      when(resource.getSharedDeviceEntries()).thenReturn(entries);
      DeviceTaskPartition left = mock(DeviceTaskPartition.class),
          right = mock(DeviceTaskPartition.class);
      when(left.getPlanNodeId()).thenReturn(id());
      when(right.getPlanNodeId()).thenReturn(id());
      when(left.getDeviceEntryIndexes()).thenReturn(Collections.singletonList(0));
      when(right.getDeviceEntryIndexes()).thenReturn(Collections.singletonList(1));
      when(left.getPartitionIndex()).thenReturn(0);
      when(right.getPartitionIndex()).thenReturn(1);
      when(resource.getDeviceTaskPartitions()).thenReturn(Arrays.asList(left, right));
      ExternalTsFileAggregationScanNode external =
          new ExternalTsFileAggregationScanNode(
              id(),
              template.getQualifiedObjectName(),
              template.getOutputSymbols(),
              template.getAssignments(),
              template.getTagAndAttributeIndexMap(),
              template.getScanOrder(),
              template.getTimePredicate().orElse(null),
              template.getPushDownPredicate(),
              template.getPushDownLimit(),
              template.getPushDownOffset(),
              template.isPushLimitToEachDevice(),
              template.containsNonAlignedDevice(),
              template.getProjection(),
              template.getAggregations(),
              template.getGroupingSets(),
              template.getPreGroupedSymbols(),
              AggregationNode.Step.SINGLE,
              template.getGroupIdSymbol(),
              resource,
              Collections.emptyList(),
              -1,
              null);
      AggregationNode result = (AggregationNode) f.distribute(external).get(0);
      assertEquals(AggregationNode.Step.FINAL, result.getStep());
      assertCollected(result);
      for (PlanNode partial : result.getChild().getChildren()) {
        assertTrue(partial instanceof ExternalTsFileAggregationScanNode);
        assertEquals(
            AggregationNode.Step.PARTIAL, ((ExternalTsFileAggregationScanNode) partial).getStep());
      }
    }
  }

  private static void spillScans(Fixture fixture) throws Exception {
    Path dir =
        Files.createTempDirectory(Path.of(System.getProperty("user.dir")), "a0-special-spill-");
    IoTDBDescriptor.getInstance().getConfig().setSortTmpDir(dir.toString());
    int count = 0;
    for (PlanNode node : nodes(fixture.logical.getRootNode())) {
      if (!(node instanceof DeviceTableScanNode)) continue;
      DeviceTableScanNode scan = (DeviceTableScanNode) node;
      List<DeviceEntry> entries = new ArrayList<>(scan.getDeviceEntries());
      assertFalse(entries.isEmpty());
      DeviceEntryDataSet dataset;
      try (DeviceEntryMaterializer m =
          new DeviceEntryMaterializer(
              fixture.context.getQueryId().getId(), id(), 128, true, fixture.context)) {
        for (DeviceEntry e : entries) m.append(e);
        m.forceSpill();
        dataset = m.finish();
      }
      assertTrue(dataset.isSpilled());
      scan.setCoordinatorDeviceEntryDataSet(dataset);
      count++;
    }
    assertTrue(count > 0);
  }

  @Test
  public void spilledScalarAggregationKeepsPartialFinalStages() throws Exception {
    for (boolean flag : new boolean[] {false, true}) {
      IoTDBDescriptor.getInstance().getConfig().setEnablePropertyDrivenPlanning(flag);
      Fixture f = new Fixture("SELECT count(s1) FROM testdb.table1");
      AggregationTableScanNode source =
          find(f.logical.getRootNode(), AggregationTableScanNode.class);
      source.setStep(AggregationNode.Step.SINGLE);
      spillScans(f);
      List<PlanNode> result = f.distribute(source);
      assertEquals(1, result.size());
      List<PlanNode> all = nodes(result.get(0));
      assertTrue(
          all.stream()
              .anyMatch(
                  n ->
                      n instanceof AggregationNode
                          && ((AggregationNode) n).getStep() == AggregationNode.Step.FINAL));
      assertTrue(
          all.stream()
              .anyMatch(
                  n ->
                      n instanceof AggregationTableScanNode
                          && ((AggregationTableScanNode) n).getStep()
                              == AggregationNode.Step.PARTIAL));
    }
  }

  @Test
  public void spilledTagGroupingPreservesAllSourceBranches() throws Exception {
    Fixture f =
        new Fixture(
            "SELECT * FROM TABLE(REPEAT(DATA => TABLE(testdb.table1) PARTITION BY (tag1,tag2,tag3), N => 2))");
    spillScans(f);
    List<PlanNode> result = f.distribute(f.logical.getRootNode());
    assertEquals(1, result.size());
    assertTrue(
        nodes(result.get(0)).stream().filter(n -> n instanceof DeviceTableScanNode).count() > 1);
  }

  @Test
  public void unorderedInsertConvergesRegionOutputs() {
    Fixture f = new Fixture("INSERT INTO testdb.table3 SELECT * FROM testdb.table2");
    List<PlanNode> result = f.distribute(f.logical.getRootNode());
    assertEquals(1, result.size());
    AggregationNode finalCount = find(result.get(0), AggregationNode.class);
    assertEquals(AggregationNode.Step.FINAL, finalCount.getStep());
    assertTrue(finalCount.getChild() instanceof CollectNode);
    assertTrue(finalCount.getChild().getChildren().size() > 1);
    for (PlanNode child : finalCount.getChild().getChildren())
      assertTrue(child instanceof IntoNode);
  }

  @Test
  public void orderedPatternRecognizesAfterExplicitInputMerge() {
    Fixture f =
        new Fixture(
            "SELECT * FROM testdb.table1 MATCH_RECOGNIZE (MEASURES A.s1 AS col1 PATTERN (A B+) DEFINE B AS B.s2 > 5) AS m");
    PatternRecognitionNode pattern = find(f.logical.getRootNode(), PatternRecognitionNode.class);
    pattern.setChild(branches(pattern.getChild().getOutputSymbols()));
    assertCollected(f.distribute(pattern).get(0));
  }

  @Test
  public void scalarTreeViewAggregationRetainsRegionConvergence() throws Exception {
    installTreeView();
    Fixture f =
        new Fixture(
            "SELECT count(s1) FROM " + TREE_VIEW_DB + ".\"" + DEVICE_VIEW_TEST_TABLE + "\"");
    AggregationTreeDeviceViewScanNode scan =
        find(f.logical.getRootNode(), AggregationTreeDeviceViewScanNode.class);
    scan.setStep(AggregationNode.Step.SINGLE);
    AggregationNode finalNode = (AggregationNode) f.distribute(scan).get(0);
    assertEquals(AggregationNode.Step.FINAL, finalNode.getStep());
    assertTrue(finalNode.getChild().getChildren().size() > 1);
  }

  @Test
  public void spilledTreeViewAggregationRetainsRegionConvergence() throws Exception {
    installTreeView();
    Fixture f =
        new Fixture(
            "SELECT count(s1) FROM " + TREE_VIEW_DB + ".\"" + DEVICE_VIEW_TEST_TABLE + "\"");
    AggregationTreeDeviceViewScanNode scan =
        find(f.logical.getRootNode(), AggregationTreeDeviceViewScanNode.class);
    scan.setStep(AggregationNode.Step.SINGLE);
    spillScans(f);
    AggregationNode finalNode = (AggregationNode) f.distribute(scan).get(0);
    assertEquals(AggregationNode.Step.FINAL, finalNode.getStep());
    assertTrue(finalNode.getChild().getChildren().size() > 1);
  }

  private static void installTreeView() {
    TsTable table = new TsTable(DEVICE_VIEW_TEST_TABLE);
    table.addProp(TsTable.TTL_PROPERTY, Long.toString(Long.MAX_VALUE));
    table.addProp(TreeViewSchema.TREE_PATH_PATTERN, "root.test.**");
    DataNodeTableCache.getInstance().preUpdateTable(TREE_VIEW_DB, table, null);
    DataNodeTableCache.getInstance().commitUpdateTable(TREE_VIEW_DB, DEVICE_VIEW_TEST_TABLE, null);
  }

  @Test
  public void copyToPreservesItsUnconditionalRootCollect() {
    Fixture f = new Fixture("SELECT * FROM testdb.table1");
    Symbol symbol = new Symbol("s1");
    List<Symbol> outputs = Collections.singletonList(symbol);
    var options =
        mock(org.apache.iotdb.db.queryengine.execution.operator.process.copyto.CopyToOptions.class);
    when(options.getOutputSymbols()).thenReturn(outputs);
    CopyToNode copy =
        new CopyToNode(
            id(),
            branches(outputs),
            "unused-baseline-export",
            options,
            outputs,
            f.analysis.getRespDatasetHeader(),
            outputs);
    assertCollected(f.distribute(copy).get(0));
  }

  @Test
  public void spilledGroupingOnFieldInsertsRealSortBeforeMerge() throws Exception {
    Fixture f = new Fixture("SELECT * FROM testdb.table1");
    DeviceTableScanNode scan = find(f.logical.getRootNode(), DeviceTableScanNode.class);
    Symbol value = new Symbol("s1");
    org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme order =
        new org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme(
            Collections.singletonList(value),
            Collections.singletonMap(
                value,
                org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder
                    .ASC_NULLS_LAST));
    GroupNode group = new GroupNode(id(), scan, order, 1);
    spillScans(f);
    List<PlanNode> result = f.distribute(group);
    assertEquals(1, result.size());
    List<PlanNode> all = nodes(result.get(0));
    assertTrue(
        all.stream()
            .anyMatch(
                n -> n instanceof SortNode && ((SortNode) n).getOrderingScheme().equals(order)));
    assertTrue(all.stream().filter(n -> n instanceof DeviceTableScanNode).count() > 1);
  }
}
