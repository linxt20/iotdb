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
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.*;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValuesNode;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceFetchNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryCountNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryScanNode;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.*;

public class PlanPropertyProviderTest {
  static final Symbol KEY = new Symbol("key");
  static final Symbol OTHER = new Symbol("other");
  static final OrderingScheme ASC =
      new OrderingScheme(List.of(KEY), Map.of(KEY, SortOrder.ASC_NULLS_LAST));
  static final OrderingScheme DESC =
      new OrderingScheme(List.of(KEY), Map.of(KEY, SortOrder.DESC_NULLS_LAST));

  static ValuesNode values(String id) {
    return new ValuesNode(new PlanNodeId(id), List.of(KEY), List.of());
  }

  static ValuesNode unknown(String id) {
    return new ValuesNode(new PlanNodeId(id), List.of(KEY), List.of()) {};
  }

  private final PlanPropertyProvider provider = new PlanPropertyProvider();
  private final Map<PlanNodeId, OrderingScheme> orderings = new HashMap<>();

  @Test
  public void nodeOutputAndParallelBranchSetHaveDifferentDistribution() {
    PlanNode a = values("a"), b = values("b");
    assertEquals(
        PlanProperties.singleUnordered(), provider.providedBy(a, orderings).getProperties());
    assertEquals(
        PlanProperties.any(),
        provider.providedByBranches(List.of(a, b), null, orderings).getProperties());
  }

  @Test
  public void allBranchesNeedMatchingPhysicalOrdering() {
    PlanNode a = values("a"), b = values("b");
    orderings.put(a.getPlanNodeId(), ASC);
    assertFalse(
        provider.providedByBranches(List.of(a, b), ASC, orderings).getProperties().isOrdered());
    orderings.put(b.getPlanNodeId(), DESC);
    assertFalse(
        provider.providedByBranches(List.of(a, b), ASC, orderings).getProperties().isOrdered());
    orderings.put(b.getPlanNodeId(), ASC);
    assertEquals(
        ASC,
        provider.providedByBranches(List.of(a, b), ASC, orderings).getProperties().getOrdering());
  }

  @Test
  public void absentPhysicalObservationDoesNotInventOrdering() {
    PlanNode a = values("a");
    orderings.put(a.getPlanNodeId(), ASC);
    assertFalse(
        provider.providedByBranches(List.of(a), null, orderings).getProperties().isOrdered());
  }

  @Test
  public void sortProvidesItsOwnOrderButConflictingRecordRemovesTheGuarantee() {
    SortNode sort = new SortNode(new PlanNodeId("s"), values("a"), ASC, false, false);
    assertEquals(ASC, provider.providedBy(sort, orderings).getProperties().getOrdering());
    orderings.put(sort.getPlanNodeId(), DESC);
    assertFalse(provider.providedBy(sort, orderings).getProperties().isOrdered());
    assertFalse(
        provider.providedByBranches(List.of(sort), DESC, orderings).getProperties().isOrdered());
  }

  @Test
  public void sortWithoutPhysicalRecordCannotProveTheBranchObservation() {
    SortNode sort = new SortNode(new PlanNodeId("s"), values("a"), ASC, false, false);
    assertFalse(
        provider.providedByBranches(List.of(sort), ASC, orderings).getProperties().isOrdered());
  }

  @Test
  public void mergeSortAndStreamSortExposeTheirDeclaredOrder() {
    MergeSortNode merge = new MergeSortNode(new PlanNodeId("m"), ASC, List.of(KEY));
    merge.addChild(values("a"));
    assertEquals(ASC, provider.providedBy(merge, orderings).getProperties().getOrdering());
    StreamSortNode stream =
        new StreamSortNode(new PlanNodeId("s"), values("b"), ASC, false, false, 0);
    assertEquals(ASC, provider.providedBy(stream, orderings).getProperties().getOrdering());
  }

  @Test
  public void collectClearsEvenAStaleOrderingRecord() {
    CollectNode collect = new CollectNode(new PlanNodeId("c"), List.of(KEY));
    collect.addChild(values("a"));
    orderings.put(collect.getPlanNodeId(), ASC);
    assertFalse(provider.providedBy(collect, orderings).getProperties().isOrdered());
  }

  @Test
  public void projectOnlyKeepsAnOrderWhoseSymbolsSurvive() {
    ProjectNode project =
        new ProjectNode(
            new PlanNodeId("p"), values("a"), Assignments.of(OTHER, KEY.toSymbolReference()));
    orderings.put(project.getPlanNodeId(), ASC);
    assertFalse(provider.providedBy(project, orderings).getProperties().isOrdered());
    ProjectNode identity =
        new ProjectNode(
            new PlanNodeId("i"), values("b"), Assignments.of(KEY, KEY.toSymbolReference()));
    orderings.put(identity.getPlanNodeId(), ASC);
    assertEquals(ASC, provider.providedBy(identity, orderings).getProperties().getOrdering());
  }

  @Test
  public void genericWrappersReadOnlyTheirProvenOutputOrder() {
    PlanNode a = values("a");
    List<PlanNode> wrappers =
        List.of(
            new ValueFillNode(new PlanNodeId("fill"), a, new LongLiteral("0")),
            new GapFillNode(new PlanNodeId("gap"), a, 0, 10, 0, 1, KEY, List.of()),
            new LimitNode(new PlanNodeId("limit"), a, 1, Optional.empty()),
            new OffsetNode(new PlanNodeId("offset"), a, 1),
            new EnforceSingleRowNode(new PlanNodeId("single"), a),
            new AssignUniqueId(new PlanNodeId("unique"), a, OTHER),
            new MarkDistinctNode(
                new PlanNodeId("distinct"), a, OTHER, List.of(KEY), Optional.empty()));
    for (PlanNode wrapper : wrappers) {
      assertEquals(
          wrapper.getClass().getSimpleName(),
          PlanPropertyProvider.Coverage.KNOWN,
          provider.providedBy(wrapper, orderings).getCoverage());
      assertFalse(provider.providedBy(wrapper, orderings).getProperties().isOrdered());
      orderings.put(wrapper.getPlanNodeId(), ASC);
      assertEquals(
          wrapper.getClass().getSimpleName(),
          ASC,
          provider.providedBy(wrapper, orderings).getProperties().getOrdering());
    }
  }

  @Test
  public void allThreeDeviceMetadataOutputsAreCovered() {
    List<PlanNode> nodes =
        List.of(
            new TableDeviceQueryScanNode(
                new PlanNodeId("scan"), "db", "t", List.of(), null, List.of(), null, -1, false),
            new TableDeviceQueryCountNode(
                new PlanNodeId("count"), "db", "t", List.of(), null, List.of()),
            new TableDeviceFetchNode(
                new PlanNodeId("fetch"), "db", "t", List.of(), List.of(), List.of(), null));
    for (PlanNode node : nodes) {
      PlanPropertyProvider.ProvidedProperties output = provider.providedBy(node, orderings);
      assertEquals(
          node.getClass().getSimpleName(),
          PlanPropertyProvider.Coverage.KNOWN,
          output.getCoverage());
      assertEquals(PlanProperties.singleUnordered(), output.getProperties());
    }
  }

  @Test
  public void unknownNodeDoesNotBecomeSingleBecauseItOccupiesOneListSlot() {
    PlanNode node = unknown("u");
    orderings.put(node.getPlanNodeId(), ASC);
    PlanPropertyProvider.ProvidedProperties output = provider.providedBy(node, orderings);
    assertEquals(PlanPropertyProvider.Coverage.UNKNOWN, output.getCoverage());
    assertEquals(FallbackReason.UNKNOWN_PROVIDER, output.getFallbackReason());
    assertEquals(PlanProperties.any(), output.getProperties());
    assertEquals(
        PlanPropertyProvider.Coverage.UNKNOWN,
        provider.providedByBranches(List.of(node), ASC, orderings).getCoverage());
  }

  @Test
  public void oneUnknownBranchMakesTheSetConservative() {
    PlanNode a = values("a"), b = unknown("b");
    orderings.put(a.getPlanNodeId(), ASC);
    orderings.put(b.getPlanNodeId(), ASC);
    PlanPropertyProvider.ProvidedProperties output =
        provider.providedByBranches(List.of(a, b), ASC, orderings);
    assertEquals(PlanPropertyProvider.Coverage.UNKNOWN, output.getCoverage());
    assertEquals(PlanProperties.any(), output.getProperties());
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyBranchesAreRejected() {
    provider.providedByBranches(List.of(), null, orderings);
  }
}
