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

  private static java.util.List<Symbol> emptyKeys() {
    return Collections.emptyList();
  }
}
