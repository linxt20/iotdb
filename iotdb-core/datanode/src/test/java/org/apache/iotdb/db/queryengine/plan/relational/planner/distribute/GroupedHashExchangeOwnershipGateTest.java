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

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.TableGroupByHashRepartitionTopology.FragmentTopologyFailure;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * The production gate that checks a grouped hash exchange still owns its fragments after the split.
 *
 * <p>The split is the only stage that assigns fragments, so this is the only place a lost ownership
 * shows up. The shape that matters is the serial one: every bucket's exchange in a single fragment.
 * That shape produces a plan that runs and returns correct rows, which is exactly why it needs an
 * explicit gate rather than being left to an end-to-end test to notice.
 */
public class GroupedHashExchangeOwnershipGateTest {

  private static final PlanNodeId S0 = new PlanNodeId("source-0");
  private static final PlanNodeId S1 = new PlanNodeId("source-1");

  /** Exchange for source {@code s} into bucket {@code b}, named the way the builder allocates ids. */
  private static final PlanNodeId E00 = new PlanNodeId("exchange-s0-b0");
  private static final PlanNodeId E01 = new PlanNodeId("exchange-s0-b1");
  private static final PlanNodeId E02 = new PlanNodeId("exchange-s0-b2");
  private static final PlanNodeId E10 = new PlanNodeId("exchange-s1-b0");
  private static final PlanNodeId E11 = new PlanNodeId("exchange-s1-b1");
  private static final PlanNodeId E12 = new PlanNodeId("exchange-s1-b2");

  private static HashPartitioningDescriptor threeBuckets() {
    return new HashPartitioningDescriptor(
        List.of(new Symbol("group_key")),
        HashPartitioningDescriptor.HashVersion.TABLE_HASH_V1,
        HashPartitioningDescriptor.NullRouting.HASH_NULL,
        3);
  }

  private static TableGroupByHashRepartitionTopology twoSourcesThreeBuckets() {
    return TableGroupByHashRepartitionTopology.create(
        threeBuckets(),
        Arrays.asList(S0, S1),
        Arrays.asList(
            Arrays.asList(E00, E01, E02),
            Arrays.asList(E10, E11, E12)));
  }

  /** One fragment per source, one per bucket, each bucket's exchanges together. */
  private static Map<PlanNodeId, String> correctOwnership() {
    Map<PlanNodeId, String> map = new HashMap<>();
    map.put(S0, "fragment-source-0");
    map.put(S1, "fragment-source-1");
    map.put(E00, "fragment-bucket-0");
    map.put(E10, "fragment-bucket-0");
    map.put(E01, "fragment-bucket-1");
    map.put(E11, "fragment-bucket-1");
    map.put(E02, "fragment-bucket-2");
    map.put(E12, "fragment-bucket-2");
    return map;
  }

  @Test
  public void aSplitThatGaveEveryBucketItsOwnFragmentPasses() {
    TableDistributedPlanner.checkGroupedHashExchangeOwnership(
        java.util.Collections.singletonList(twoSourcesThreeBuckets()), correctOwnership());
  }

  @Test
  public void noTopologyAtAllPassesSoQueriesOffTheHashPathAreUnaffected() {
    TableDistributedPlanner.checkGroupedHashExchangeOwnership(
        java.util.Collections.emptyList(), new HashMap<>());
  }

  /**
   * The serial shape. Every exchange lands in one fragment, so the three buckets are computed one
   * after another. The rows are still correct, which is what makes this worth a gate.
   */
  @Test
  public void everyBucketInOneFragmentIsRefused() {
    Map<PlanNodeId, String> serial = new HashMap<>();
    serial.put(S0, "fragment-source-0");
    serial.put(S1, "fragment-source-1");
    for (PlanNodeId exchange : new PlanNodeId[] {E00, E01, E02, E10, E11, E12}) {
      serial.put(exchange, "fragment-serial-final");
    }

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                TableDistributedPlanner.checkGroupedHashExchangeOwnership(
                    java.util.Collections.singletonList(twoSourcesThreeBuckets()), serial));
    assertTrue(thrown.getMessage().contains("2 sources x 3 buckets"));
    assertTrue(thrown.getMessage().contains(FragmentTopologyFailure.PARTITIONS_SHARE_FINAL_FRAGMENT.toString()));
  }

  @Test
  public void twoSourcesInOneFragmentAreRefused() {
    Map<PlanNodeId, String> shared = correctOwnership();
    shared.put(S1, "fragment-source-0");

    assertEquals(
        FragmentTopologyFailure.SOURCES_SHARE_FRAGMENT,
        soleFailureOf(shared));
  }

  @Test
  public void anExchangeWithNoFragmentIsRefusedRatherThanSilentlyDropped() {
    Map<PlanNodeId, String> missing = correctOwnership();
    missing.remove(E02);
    missing.remove(E12);

    assertEquals(
        FragmentTopologyFailure.MISSING_DESTINATION_FRAGMENT,
        soleFailureOf(missing));
  }

  /**
   * The split assigns the exchange of one bucket to a second fragment. Both buckets still have their
   * own final fragment, so the per-partition rule holds, but bucket 1 now also owns a fragment that
   * bucket 2 claims.
   */
  @Test
  public void oneBucketSplitAcrossTwoFragmentsIsRefused() {
    Map<PlanNodeId, String> split = correctOwnership();
    split.put(E11, "fragment-bucket-2");

    assertEquals(
        FragmentTopologyFailure.PARTITION_EXCHANGES_HAVE_DIFFERENT_FRAGMENTS,
        soleFailureOf(split));
  }

  /**
   * A serial merge breaks three rules at once, and the gate has to name all of them. Stopping at the
   * first would hide the source-sharing half of the damage, which is the half that says the partial
   * aggregations are not running side by side either.
   */
  @Test
  public void theFailureSetIsReportedWholeRatherThanStoppingAtTheFirst() {
    Map<PlanNodeId, String> broken = new HashMap<>();
    broken.put(S0, "fragment-serial");
    broken.put(S1, "fragment-serial");
    for (PlanNodeId exchange : new PlanNodeId[] {E00, E01, E02, E10, E11, E12}) {
      broken.put(exchange, "fragment-serial");
    }

    String message = gateMessageFor(broken);
    assertTrue(message, message.contains(FragmentTopologyFailure.SOURCES_SHARE_FRAGMENT.toString()));
    assertTrue(
        message,
        message.contains(FragmentTopologyFailure.PARTITIONS_SHARE_FINAL_FRAGMENT.toString()));
    assertTrue(
        message,
        message.contains(FragmentTopologyFailure.SOURCE_AND_FINAL_SHARE_FRAGMENT.toString()));
  }

  /**
   * Runs the gate the way production does and returns the one failure it names. Every case here has
   * exactly one broken rule, so a single expected value keeps the assertion about the gate's output
   * rather than about {@code validateFragmentOwnership}, which the topology test already covers.
   */
  private static FragmentTopologyFailure soleFailureOf(Map<PlanNodeId, String> fragmentIdByNode) {
    String message = gateMessageFor(fragmentIdByNode);
    Set<FragmentTopologyFailure> named = new java.util.HashSet<>();
    for (FragmentTopologyFailure failure : FragmentTopologyFailure.values()) {
      if (message.contains(failure.toString())) {
        named.add(failure);
      }
    }
    assertEquals(message, 1, named.size());
    return named.iterator().next();
  }

  private static String gateMessageFor(Map<PlanNodeId, String> fragmentIdByNode) {
    try {
      TableDistributedPlanner.checkGroupedHashExchangeOwnership(
          java.util.Collections.singletonList(twoSourcesThreeBuckets()), fragmentIdByNode);
    } catch (IllegalStateException e) {
      return e.getMessage();
    }
    throw new AssertionError("the gate accepted a fragment map it must refuse");
  }
}
