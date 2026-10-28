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
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValuesNode;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.PropertyEnforcer.*;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.PlanPropertyProviderTest.*;
import static org.junit.Assert.*;

public class PropertyEnforcerTest {
  private final PropertyEnforcer enforcer = new PropertyEnforcer();
  private final PlanPropertyProvider provider = new PlanPropertyProvider();
  private final Map<PlanNodeId, OrderingScheme> orderings = new HashMap<>();
  private final QueryId queryId = new QueryId("component_test");

  private PropertyEnforcementResult run(
      PlanProperties required, List<PlanNode> branches, OrderingScheme observed) {
    return enforcer.enforce(
        required,
        branches,
        provider.providedByBranches(branches, observed, orderings),
        queryId,
        orderings);
  }

  @Test
  public void alreadySingleIsReturnedUnchanged() {
    PlanNode a = values("a");
    PropertyEnforcementResult r = run(PlanProperties.singleUnordered(), List.of(a), null);
    assertSame(a, r.getBranches().get(0));
    assertEquals(Outcome.SATISFIED, r.getOutcome());
    assertEquals(EnforcerKind.NONE, r.getActualEnforcer());
    assertTrue(r.getInsertedNodeIds().isEmpty());
    assertNull(r.getFallbackReason());
  }

  @Test
  public void arbitraryRequirementKeepsBothBranchesAndTheirOrder() {
    PlanNode a = values("a"), b = values("b");
    PropertyEnforcementResult r = run(PlanProperties.any(), List.of(a, b), null);
    assertEquals(List.of(a, b), r.getBranches());
    assertEquals(Outcome.SATISFIED, r.getOutcome());
    assertEquals(PlanProperties.any(), r.getProvidedAfter());
  }

  @Test
  public void unorderedSingleCollectsEveryInput() {
    List<PlanNode> inputs = List.of(values("a"), values("b"));
    PropertyEnforcementResult r = run(PlanProperties.singleUnordered(), inputs, null);
    assertTrue(r.getBranches().get(0) instanceof CollectNode);
    assertEquals(inputs, r.getBranches().get(0).getChildren());
    assertEquals(1, r.getInsertedNodeIds().size());
    assertEquals(Outcome.ENFORCED, r.getOutcome());
    assertEquals(PlanProperties.singleUnordered(), r.getProvidedAfter());
  }

  @Test
  public void unorderedSingleInputGetsASortRatherThanAnIneffectiveOneInputMerge() {
    PlanNode a = values("a");
    PropertyEnforcementResult r =
        run(PlanProperties.singleUnordered().withOrdering(ASC), List.of(a), null);
    assertEquals(EnforcerKind.SORT, r.getActualEnforcer());
    assertTrue(r.getBranches().get(0) instanceof SortNode);
    assertSame(a, r.getBranches().get(0).getChildren().get(0));
    assertEquals(ASC, orderings.get(r.getBranches().get(0).getPlanNodeId()));
  }

  @Test
  public void matchingOrderedBranchesNeedOnlyMergeSort() {
    PlanNode a = values("a"), b = values("b");
    orderings.put(a.getPlanNodeId(), ASC);
    orderings.put(b.getPlanNodeId(), ASC);
    PropertyEnforcementResult r =
        run(PlanProperties.singleUnordered().withOrdering(ASC), List.of(a, b), ASC);
    assertEquals(EnforcerKind.MERGE_SORT, r.getActualEnforcer());
    assertTrue(r.getBranches().get(0) instanceof MergeSortNode);
    assertEquals(List.of(a, b), r.getBranches().get(0).getChildren());
    assertEquals(1, r.getInsertedNodeIds().size());
    assertTrue(r.getProvidedAfter().satisfies(PlanProperties.singleUnordered().withOrdering(ASC)));
  }

  @Test
  public void oneUnorderedBranchMakesEveryInputGetASortBeforeMerge() {
    PlanNode a = values("a"), b = values("b");
    orderings.put(a.getPlanNodeId(), ASC);
    PropertyEnforcementResult r =
        run(PlanProperties.singleUnordered().withOrdering(ASC), List.of(a, b), ASC);
    assertEquals(3, r.getInsertedNodeIds().size());
    assertEquals(EnforcerKind.MERGE_SORT, r.getActualEnforcer());
    List<PlanNode> sorted = r.getBranches().get(0).getChildren();
    assertTrue(sorted.get(0) instanceof SortNode);
    assertTrue(sorted.get(1) instanceof SortNode);
    assertSame(a, sorted.get(0).getChildren().get(0));
    assertSame(b, sorted.get(1).getChildren().get(0));
  }

  @Test
  public void parentRequirementNeverProvesChildOrdering() {
    PlanNode a = values("a");
    orderings.put(a.getPlanNodeId(), DESC);
    PropertyEnforcementResult r =
        run(PlanProperties.singleUnordered().withOrdering(ASC), List.of(a), DESC);
    assertEquals(EnforcerKind.SORT, r.getActualEnforcer());
    assertEquals(DESC, r.getProvidedBefore().getOrdering());
    assertEquals(ASC, r.getProvidedAfter().getOrdering());
  }

  @Test
  public void arbitraryOrderedRequirementSortsAndRetainsBothBranches() {
    List<PlanNode> inputs = List.of(values("a"), values("b"));
    PropertyEnforcementResult r = run(PlanProperties.any().withOrdering(ASC), inputs, null);
    assertEquals(2, r.getBranches().size());
    assertEquals(EnforcerKind.SORT, r.getActualEnforcer());
    assertTrue(r.getProvidedAfter().getDistribution().isArbitrary());
    assertEquals(ASC, r.getProvidedAfter().getOrdering());
  }

  @Test
  public void genericPartitionRequirementConservativelyCollectsWithExplicitFallback() {
    List<PlanNode> inputs = List.of(values("a"), values("b"));
    PlanProperties required =
        PlanProperties.requirementFor(DistributionProperty.partitioned(List.of(KEY)), null);
    PropertyEnforcementResult r = run(required, inputs, null);
    assertEquals(EnforcerKind.COLLECT, r.getActualEnforcer());
    assertEquals(Outcome.FALLBACK, r.getOutcome());
    assertEquals(FallbackReason.PARTITIONING_UNSUPPORTED, r.getFallbackReason());
    assertTrue(r.getProvidedAfter().satisfies(required));
  }

  @Test
  public void singleAlreadySatisfiesPartitioningWithoutFallback() {
    PropertyEnforcementResult r =
        run(
            PlanProperties.requirementFor(DistributionProperty.partitioned(List.of(KEY)), null),
            List.of(values("a")),
            null);
    assertEquals(Outcome.SATISFIED, r.getOutcome());
    assertNull(r.getFallbackReason());
  }

  @Test
  public void unknownSingleNeedsExplicitCollectToBecomeKnownSingle() {
    PlanNode unknown = unknown("u");
    PropertyEnforcementResult r = run(PlanProperties.singleUnordered(), List.of(unknown), null);
    assertEquals(EnforcerKind.COLLECT, r.getActualEnforcer());
    assertEquals(List.of(unknown), r.getBranches().get(0).getChildren());
    assertEquals(FallbackReason.UNKNOWN_PROVIDER, r.getFallbackReason());
    assertEquals(Outcome.FALLBACK, r.getOutcome());
    assertEquals(PlanProperties.singleUnordered(), r.getProvidedAfter());
  }

  @Test
  public void unknownArbitraryPreservesBothInputsAndRecordsFallback() {
    PlanNode a = unknown("a"), b = unknown("b");
    PropertyEnforcementResult r = run(PlanProperties.any(), List.of(a, b), null);
    assertEquals(List.of(a, b), r.getBranches());
    assertEquals(EnforcerKind.NONE, r.getActualEnforcer());
    assertEquals(Outcome.FALLBACK, r.getOutcome());
    assertEquals(FallbackReason.UNKNOWN_PROVIDER, r.getFallbackReason());
  }

  @Test
  public void unknownInputsCanBeSafelySortedAndMergedWithBothInputsPreserved() {
    PlanNode a = unknown("a"), b = values("b");
    PropertyEnforcementResult r =
        run(PlanProperties.singleUnordered().withOrdering(ASC), List.of(a, b), null);
    assertEquals(EnforcerKind.MERGE_SORT, r.getActualEnforcer());
    assertEquals(FallbackReason.UNKNOWN_PROVIDER, r.getFallbackReason());
    assertSame(a, r.getBranches().get(0).getChildren().get(0).getChildren().get(0));
    assertSame(b, r.getBranches().get(0).getChildren().get(1).getChildren().get(0));
  }

  @Test
  public void returnedListsCannotBeChangedByTheCaller() {
    List<PlanNode> inputs = new ArrayList<>(List.of(values("a"), values("b")));
    PropertyEnforcementResult r = run(PlanProperties.any(), inputs, null);
    inputs.clear();
    assertEquals(2, r.getBranches().size());
    try {
      r.getBranches().clear();
      fail("mutable output list");
    } catch (UnsupportedOperationException expected) {
    }
    try {
      r.getInsertedNodeIds().add(new PlanNodeId("fake"));
      fail("mutable inserted IDs");
    } catch (UnsupportedOperationException expected) {
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void incompatibleBranchOutputsAreRejectedBeforeMerging() {
    PlanNode a = values("a"), b = new ValuesNode(new PlanNodeId("b"), List.of(OTHER), List.of());
    run(PlanProperties.singleUnordered(), List.of(a, b), null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void missingOrderingSymbolsAreRejected() {
    run(
        PlanProperties.singleUnordered().withOrdering(DESC),
        List.of(new ValuesNode(new PlanNodeId("a"), List.of(OTHER), List.of())),
        null);
  }

  @Test
  public void unorderedCollectDropsAnExistingLocalOrderingGuarantee() {
    PlanNode a = values("a"), b = values("b");
    orderings.put(a.getPlanNodeId(), ASC);
    orderings.put(b.getPlanNodeId(), ASC);
    PropertyEnforcementResult r = run(PlanProperties.singleUnordered(), List.of(a, b), ASC);
    assertEquals(EnforcerKind.COLLECT, r.getActualEnforcer());
    assertFalse("Collect may interleave sorted inputs", r.getProvidedAfter().isOrdered());
    assertEquals(
        r.getProvidedAfter(),
        provider.providedBy(r.getBranches().get(0), orderings).getProperties());
  }

  @Test
  public void unknownInputsAreAllRetainedByAnUnorderedCollect() {
    PlanNode a = unknown("a"), b = unknown("b");
    PropertyEnforcementResult r = run(PlanProperties.singleUnordered(), List.of(a, b), null);
    assertEquals(EnforcerKind.COLLECT, r.getActualEnforcer());
    assertEquals(List.of(a, b), r.getBranches().get(0).getChildren());
    assertEquals(Outcome.FALLBACK, r.getOutcome());
    assertEquals(FallbackReason.UNKNOWN_PROVIDER, r.getFallbackReason());
  }

  @Test(expected = IllegalArgumentException.class)
  public void missingPartitionKeysAreRejectedEvenForAnAlreadySingleInput() {
    run(
        PlanProperties.requirementFor(DistributionProperty.partitioned(List.of(OTHER)), null),
        List.of(values("a")),
        null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void missingPartitionKeysAreRejectedForEveryInputBeforeCollect() {
    run(
        PlanProperties.requirementFor(DistributionProperty.partitioned(List.of(OTHER)), null),
        List.of(values("a"), values("b")),
        null);
  }
}
