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

import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.SortOrder;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the shared property types used by the property driven distribution rules. These
 * only exercise the property algebra itself, they do not build any plan.
 */
public class DistributionPropertyTest {

  private static final Symbol A = new Symbol("a");
  private static final Symbol B = new Symbol("b");

  @Test
  public void singleSatisfiesEverything() {
    // A single branch has nothing to merge and trivially keeps every group together, so it can be
    // fed to any parent without an enforcing node in between.
    assertTrue(DistributionProperty.single().satisfies(DistributionProperty.single()));
    assertTrue(DistributionProperty.single().satisfies(DistributionProperty.arbitrary()));
    assertTrue(
        DistributionProperty.single()
            .satisfies(DistributionProperty.partitioned(Collections.singletonList(A))));
  }

  @Test
  public void onlySingleSatisfiesRequiredSingle() {
    assertFalse(DistributionProperty.arbitrary().satisfies(DistributionProperty.single()));
    assertFalse(
        DistributionProperty.partitioned(Collections.singletonList(A))
            .satisfies(DistributionProperty.single()));
  }

  @Test
  public void everythingSatisfiesRequiredArbitrary() {
    assertTrue(DistributionProperty.arbitrary().satisfies(DistributionProperty.arbitrary()));
    assertTrue(
        DistributionProperty.partitioned(Collections.singletonList(A))
            .satisfies(DistributionProperty.arbitrary()));
  }

  @Test
  public void partitioningOnFewerKeysSatisfiesPartitioningOnMore() {
    DistributionProperty onA = DistributionProperty.partitioned(Collections.singletonList(A));
    DistributionProperty onAB = DistributionProperty.partitioned(Arrays.asList(A, B));

    // Partitioning on {a} already keeps together everything partitioning on {a, b} would, so it
    // satisfies it; the converse splits a group across branches and does not.
    assertTrue(onA.satisfies(onAB));
    assertFalse(onAB.satisfies(onA));
    assertTrue(onA.satisfies(onA));
  }

  @Test
  public void partitioningOnNoKeyIsArbitrary() {
    // Partitioning on no key cannot tell branches apart, which is what arbitrary means.
    assertSame(DistributionProperty.arbitrary(), DistributionProperty.partitioned(emptyKeys()));
  }

  @Test
  public void keyOrderDoesNotChangeEquality() {
    assertEquals(
        DistributionProperty.partitioned(Arrays.asList(A, B)),
        DistributionProperty.partitioned(Arrays.asList(B, A)));
    assertNotEquals(
        DistributionProperty.partitioned(Collections.singletonList(A)),
        DistributionProperty.partitioned(Arrays.asList(A, B)));
  }

  @Test
  public void unorderedResultDoesNotSatisfyOrderingRequirement() {
    PlanProperties singleUnordered = PlanProperties.singleUnordered();

    // Distribution alone is not enough: a parent that also wants an ordering needs an enforcing
    // node (a MergeSort) even when the data already is in a single branch.
    assertTrue(singleUnordered.satisfies(PlanProperties.singleUnordered()));
    assertTrue(singleUnordered.satisfies(PlanProperties.any()));
    assertFalse(PlanProperties.any().satisfies(PlanProperties.singleUnordered()));
  }

  @Test
  public void distributionSatisfactionTruthTable() {
    DistributionProperty[] values = {
      DistributionProperty.single(),
      DistributionProperty.arbitrary(),
      DistributionProperty.partitioned(Collections.singletonList(A))
    };
    boolean[][] expected = {
      {true, true, true},
      {false, true, false},
      {false, true, true}
    };
    for (int provided = 0; provided < values.length; provided++) {
      for (int required = 0; required < values.length; required++) {
        assertEquals(expected[provided][required], values[provided].satisfies(values[required]));
      }
    }
  }

  @Test
  public void disjointPartitioningsDoNotSatisfyEachOther() {
    DistributionProperty onA = DistributionProperty.partitioned(Collections.singletonList(A));
    DistributionProperty onB = DistributionProperty.partitioned(Collections.singletonList(B));
    assertFalse(onA.satisfies(onB));
    assertFalse(onB.satisfies(onA));
  }

  @Test
  public void logicalPartitioningCompatibilityPreservesThePhysicalBoundary() {
    DistributionProperty onAB = DistributionProperty.partitioned(Arrays.asList(A, B));
    DistributionProperty onBA = DistributionProperty.partitioned(Arrays.asList(B, A));
    DistributionProperty onA = DistributionProperty.partitioned(Collections.singletonList(A));
    assertTrue(DistributionProperty.single().isSingle());
    assertFalse(DistributionProperty.single().isArbitrary());
    assertFalse(DistributionProperty.single().isPartitioned());
    assertTrue(DistributionProperty.arbitrary().isArbitrary());
    assertFalse(DistributionProperty.arbitrary().isPartitioned());
    assertTrue(onAB.isPartitioned());
    assertFalse(onAB.isArbitrary());
    assertTrue(onAB.isCompatiblePartitioning(onBA));
    assertFalse(onAB.isCompatiblePartitioning(onA));
    assertFalse(onAB.isCompatiblePartitioning(DistributionProperty.single()));
    assertFalse(DistributionProperty.single().isCompatiblePartitioning(onAB));
    assertFalse(
        DistributionProperty.arbitrary()
            .isCompatiblePartitioning(DistributionProperty.arbitrary()));
    assertFalse(DistributionProperty.partitioned(emptyKeys()).isPartitioned());
    assertNotEquals(
        new HashPartitioningDescriptor(
            Arrays.asList(A, B),
            HashPartitioningDescriptor.HashVersion.TABLE_HASH_V1,
            HashPartitioningDescriptor.NullRouting.HASH_NULL,
            2),
        new HashPartitioningDescriptor(
            Arrays.asList(B, A),
            HashPartitioningDescriptor.HashVersion.TABLE_HASH_V1,
            HashPartitioningDescriptor.NullRouting.HASH_NULL,
            2));
  }

  @Test
  public void distributionAndOrderingFailuresRemainIndependent() {
    OrderingScheme ascending = order(A, SortOrder.ASC_NULLS_LAST);
    PlanProperties required =
        PlanProperties.requirementFor(DistributionProperty.single(), ascending);
    PlanProperties unordered = PlanProperties.any();
    assertFalse(unordered.satisfiesDistribution(required));
    assertFalse(unordered.satisfiesOrdering(required));
    assertFalse(unordered.satisfies(required));
    PlanProperties ordered = unordered.withOrdering(ascending);
    assertFalse(ordered.satisfiesDistribution(required));
    assertTrue(ordered.satisfiesOrdering(required));
    assertFalse(ordered.satisfies(required));
    PlanProperties single = unordered.withDistribution(DistributionProperty.single());
    assertTrue(single.satisfiesDistribution(required));
    assertFalse(single.satisfiesOrdering(required));
    assertFalse(single.satisfies(required));
    assertTrue(ordered.withDistribution(DistributionProperty.single()).satisfies(required));
    assertFalse(unordered.isOrdered());
    assertEquals(ascending, required.getOrdering());
  }

  @Test
  public void immutableConversionsPreserveTheOtherAxis() {
    OrderingScheme ascending = order(A, SortOrder.ASC_NULLS_LAST);
    OrderingScheme descending = order(A, SortOrder.DESC_NULLS_FIRST);
    PlanProperties original =
        PlanProperties.requirementFor(DistributionProperty.single(), ascending);
    PlanProperties redistributed = original.withDistribution(DistributionProperty.arbitrary());
    PlanProperties reordered = original.withOrdering(descending);
    PlanProperties cleared = original.withOrdering(null);
    assertEquals(PlanProperties.of(DistributionProperty.single(), ascending), original);
    assertEquals(DistributionProperty.arbitrary(), redistributed.getDistribution());
    assertEquals(ascending, redistributed.getOrdering());
    assertEquals(DistributionProperty.single(), reordered.getDistribution());
    assertEquals(descending, reordered.getOrdering());
    assertFalse(cleared.isOrdered());
    assertEquals(DistributionProperty.single(), cleared.getDistribution());
    assertEquals(ascending, original.getOrdering());
  }

  @Test
  public void orderingRequirementsUseCompleteSchemes() {
    OrderingScheme ascending = order(A, SortOrder.ASC_NULLS_LAST);
    OrderingScheme descending = order(A, SortOrder.DESC_NULLS_LAST);
    OrderingScheme otherColumn = order(B, SortOrder.ASC_NULLS_LAST);
    java.util.Map<Symbol, SortOrder> columns = new java.util.LinkedHashMap<>();
    columns.put(A, SortOrder.ASC_NULLS_LAST);
    columns.put(B, SortOrder.ASC_NULLS_LAST);
    OrderingScheme twoColumns = new OrderingScheme(Arrays.asList(A, B), columns);
    PlanProperties required =
        PlanProperties.requirementFor(DistributionProperty.single(), ascending);
    assertTrue(
        PlanProperties.of(DistributionProperty.single(), ascending).satisfiesOrdering(required));
    assertFalse(
        PlanProperties.of(DistributionProperty.single(), descending).satisfiesOrdering(required));
    assertFalse(
        PlanProperties.of(DistributionProperty.single(), otherColumn).satisfiesOrdering(required));
    assertFalse(
        PlanProperties.of(DistributionProperty.single(), twoColumns).satisfiesOrdering(required));
    assertFalse(PlanProperties.singleUnordered().satisfiesOrdering(required));
    assertTrue(required.satisfiesOrdering(PlanProperties.singleUnordered()));
  }

  private static OrderingScheme order(Symbol symbol, SortOrder sortOrder) {
    return new OrderingScheme(
        Collections.singletonList(symbol), Collections.singletonMap(symbol, sortOrder));
  }

  private static java.util.List<Symbol> emptyKeys() {
    return Collections.emptyList();
  }
}
