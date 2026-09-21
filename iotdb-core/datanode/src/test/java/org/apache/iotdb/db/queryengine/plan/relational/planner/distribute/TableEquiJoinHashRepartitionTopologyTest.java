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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Tests the 2 x P topology gate required before a hash equi-join may be selected. */
public class TableEquiJoinHashRepartitionTopologyTest {

  @Test
  public void twoCompatibleInputsExposeOneChannelForEveryJoinBucket() {
    TableEquiJoinHashRepartitionTopology topology = createTopology();

    assertEquals(3, topology.getPartitionCount());
    assertEquals(descriptor("left_key", 3), topology.getLeftPartitioning());
    assertEquals(descriptor("right_key", 3), topology.getRightPartitioning());
    assertEquals("left-exchange-1", topology.getLeftUpstreamExchangeId(1).toString());
    assertEquals("right-exchange-2", topology.getRightUpstreamExchangeId(2).toString());
    assertEquals(
        Arrays.asList("left-exchange-0", "left-exchange-1", "left-exchange-2"),
        topology.getLeftDownstreamChannels().stream()
            .map(channel -> channel.getRemotePlanNodeId())
            .collect(java.util.stream.Collectors.toList()));
  }

  @Test
  public void acceptsOnlyOneClonedFinalJoinFragmentPerBucket() {
    TableEquiJoinHashRepartitionTopology topology = createTopology();
    Map<PlanNodeId, String> fragments = new HashMap<>();
    fragments.put(new PlanNodeId("left-source"), "left-partial");
    fragments.put(new PlanNodeId("right-source"), "right-partial");
    for (int partition = 0; partition < 3; partition++) {
      fragments.put(new PlanNodeId("left-exchange-" + partition), "final-" + partition);
      fragments.put(new PlanNodeId("right-exchange-" + partition), "final-" + partition);
    }

    TableEquiJoinHashRepartitionTopology.FragmentTopologyValidation validation =
        topology.validateFragmentOwnership(fragments);

    assertTrue(validation.isExecutable());
    assertTrue(validation.getFailures().isEmpty());
  }

  @Test
  public void rejectsInputsWithDifferentHashContracts() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            TableEquiJoinHashRepartitionTopology.create(
                descriptor("left_key", 2),
                descriptor("right_key", 3),
                new PlanNodeId("left-source"),
                new PlanNodeId("right-source"),
                List.of(new PlanNodeId("left-0"), new PlanNodeId("left-1")),
                List.of(new PlanNodeId("right-0"), new PlanNodeId("right-1"))));
  }

  @Test
  public void rejectsAPlanWhereTheTwoInputsOfOneBucketLandInDifferentFragments() {
    TableEquiJoinHashRepartitionTopology topology = createTopology();
    Map<PlanNodeId, String> fragments = new HashMap<>();
    fragments.put(new PlanNodeId("left-source"), "left-partial");
    fragments.put(new PlanNodeId("right-source"), "right-partial");
    for (int partition = 0; partition < 3; partition++) {
      fragments.put(new PlanNodeId("left-exchange-" + partition), "final-" + partition);
      fragments.put(
          new PlanNodeId("right-exchange-" + partition),
          partition == 1 ? "wrong-final" : "final-" + partition);
    }

    assertTrue(
        topology
            .validateFragmentOwnership(fragments)
            .getFailures()
            .contains(
                TableEquiJoinHashRepartitionTopology.FragmentTopologyFailure
                    .BUCKET_INPUTS_HAVE_DIFFERENT_FINAL_FRAGMENTS));
  }

  @Test
  public void rejectsSerialFinalFragmentAliasedAsMultipleBuckets() {
    TableEquiJoinHashRepartitionTopology topology = createTopology();
    Map<PlanNodeId, String> fragments = new HashMap<>();
    fragments.put(new PlanNodeId("left-source"), "left-partial");
    fragments.put(new PlanNodeId("right-source"), "right-partial");
    for (int partition = 0; partition < 3; partition++) {
      fragments.put(new PlanNodeId("left-exchange-" + partition), "one-serial-final");
      fragments.put(new PlanNodeId("right-exchange-" + partition), "one-serial-final");
    }

    assertTrue(
        topology
            .validateFragmentOwnership(fragments)
            .getFailures()
            .contains(
                TableEquiJoinHashRepartitionTopology.FragmentTopologyFailure
                    .PARTITIONS_SHARE_FINAL_FRAGMENT));
  }

  private static TableEquiJoinHashRepartitionTopology createTopology() {
    return TableEquiJoinHashRepartitionTopology.create(
        descriptor("left_key", 3),
        descriptor("right_key", 3),
        new PlanNodeId("left-source"),
        new PlanNodeId("right-source"),
        List.of(
            new PlanNodeId("left-exchange-0"),
            new PlanNodeId("left-exchange-1"),
            new PlanNodeId("left-exchange-2")),
        List.of(
            new PlanNodeId("right-exchange-0"),
            new PlanNodeId("right-exchange-1"),
            new PlanNodeId("right-exchange-2")));
  }

  private static HashPartitioningDescriptor descriptor(String symbol, int partitionCount) {
    return new HashPartitioningDescriptor(
        List.of(new Symbol(symbol)),
        HashPartitioningDescriptor.HashVersion.TABLE_HASH_V1,
        HashPartitioningDescriptor.NullRouting.HASH_NULL,
        partitionCount);
  }
}
