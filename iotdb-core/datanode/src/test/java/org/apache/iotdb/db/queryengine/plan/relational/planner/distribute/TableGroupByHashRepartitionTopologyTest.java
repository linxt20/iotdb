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

public class TableGroupByHashRepartitionTopologyTest {

  @Test
  public void testEveryPartialSourceExposesEveryFinalPartition() {
    TableGroupByHashRepartitionTopology topology = createThreeByTwoTopology();

    assertEquals(3, topology.getSourceCount());
    assertEquals(2, topology.getPartitionCount());
    assertEquals(
        Arrays.asList("exchange-00", "exchange-01"),
        topology.getDownstreamChannelsForSource(0).stream()
            .map(channel -> channel.getRemotePlanNodeId())
            .collect(java.util.stream.Collectors.toList()));
    assertEquals(
        Arrays.asList("exchange-20", "exchange-21"),
        topology.getDownstreamChannelsForSource(2).stream()
            .map(channel -> channel.getRemotePlanNodeId())
            .collect(java.util.stream.Collectors.toList()));
    assertEquals(0, topology.getDownstreamChannelIndexForPartition(0));
    assertEquals(1, topology.getDownstreamChannelIndexForPartition(1));
  }

  @Test
  public void testEveryFinalPartitionReceivesEveryPartialSource() {
    TableGroupByHashRepartitionTopology topology = createThreeByTwoTopology();

    assertEquals(
        Arrays.asList(
            new PlanNodeId("exchange-00"),
            new PlanNodeId("exchange-10"),
            new PlanNodeId("exchange-20")),
        topology.getUpstreamExchangeNodeIdsForPartition(0));
    assertEquals(
        Arrays.asList(
            new PlanNodeId("exchange-01"),
            new PlanNodeId("exchange-11"),
            new PlanNodeId("exchange-21")),
        topology.getUpstreamExchangeNodeIdsForPartition(1));
  }

  @Test
  public void testRejectsMatrixThatCannotDescribeOneChannelPerHashBucket() {
    HashPartitioningDescriptor descriptor = descriptor(2);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            TableGroupByHashRepartitionTopology.create(
                descriptor,
                Arrays.asList(new PlanNodeId("source-0")),
                Arrays.asList(Arrays.asList(new PlanNodeId("exchange-0")))));
  }

  @Test
  public void testRejectsExchangeSharedByTwoSources() {
    HashPartitioningDescriptor descriptor = descriptor(1);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            TableGroupByHashRepartitionTopology.create(
                descriptor,
                Arrays.asList(new PlanNodeId("source-0"), new PlanNodeId("source-1")),
                Arrays.asList(
                    Arrays.asList(new PlanNodeId("exchange-0")),
                    Arrays.asList(new PlanNodeId("exchange-0")))));
  }

  @Test
  public void testTwoSourcesAndThreeBucketsRequireDistinctFinalFragmentOwnership() {
    TableGroupByHashRepartitionTopology topology = createTwoByThreeTopology();
    Map<PlanNodeId, String> fragmentIdByNode = new HashMap<>();
    fragmentIdByNode.put(new PlanNodeId("source-0"), "partial-0");
    fragmentIdByNode.put(new PlanNodeId("source-1"), "partial-1");
    fragmentIdByNode.put(new PlanNodeId("exchange-00"), "final-0");
    fragmentIdByNode.put(new PlanNodeId("exchange-10"), "final-0");
    fragmentIdByNode.put(new PlanNodeId("exchange-01"), "final-1");
    fragmentIdByNode.put(new PlanNodeId("exchange-11"), "final-1");
    fragmentIdByNode.put(new PlanNodeId("exchange-02"), "final-2");
    fragmentIdByNode.put(new PlanNodeId("exchange-12"), "final-2");

    TableGroupByHashRepartitionTopology.FragmentTopologyValidation validation =
        topology.validateFragmentOwnership(fragmentIdByNode);

    assertTrue(validation.isExecutable());
    assertTrue(validation.getFailures().isEmpty());
  }

  @Test
  public void testRejectsUnclonedFinalBucketsEvenWhenEveryMatrixEdgeExists() {
    TableGroupByHashRepartitionTopology topology = createTwoByThreeTopology();
    Map<PlanNodeId, String> fragmentIdByNode = new HashMap<>();
    fragmentIdByNode.put(new PlanNodeId("source-0"), "partial-0");
    fragmentIdByNode.put(new PlanNodeId("source-1"), "partial-1");
    for (int source = 0; source < 2; source++) {
      for (int partition = 0; partition < 3; partition++) {
        fragmentIdByNode.put(
            new PlanNodeId("exchange-" + source + partition), "one-serial-final-fragment");
      }
    }

    TableGroupByHashRepartitionTopology.FragmentTopologyValidation validation =
        topology.validateFragmentOwnership(fragmentIdByNode);

    assertTrue(
        validation
            .getFailures()
            .contains(
                TableGroupByHashRepartitionTopology.FragmentTopologyFailure
                    .PARTITIONS_SHARE_FINAL_FRAGMENT));
  }

  @Test
  public void testRejectsFinalBucketWhoseSourcesLandInDifferentFragments() {
    TableGroupByHashRepartitionTopology topology = createTwoByThreeTopology();
    Map<PlanNodeId, String> fragmentIdByNode = new HashMap<>();
    fragmentIdByNode.put(new PlanNodeId("source-0"), "partial-0");
    fragmentIdByNode.put(new PlanNodeId("source-1"), "partial-1");
    fragmentIdByNode.put(new PlanNodeId("exchange-00"), "final-0a");
    fragmentIdByNode.put(new PlanNodeId("exchange-10"), "final-0b");
    fragmentIdByNode.put(new PlanNodeId("exchange-01"), "final-1");
    fragmentIdByNode.put(new PlanNodeId("exchange-11"), "final-1");
    fragmentIdByNode.put(new PlanNodeId("exchange-02"), "final-2");
    fragmentIdByNode.put(new PlanNodeId("exchange-12"), "final-2");

    TableGroupByHashRepartitionTopology.FragmentTopologyValidation validation =
        topology.validateFragmentOwnership(fragmentIdByNode);

    assertTrue(
        validation
            .getFailures()
            .contains(
                TableGroupByHashRepartitionTopology.FragmentTopologyFailure
                    .PARTITION_EXCHANGES_HAVE_DIFFERENT_FRAGMENTS));
  }

  private static TableGroupByHashRepartitionTopology createThreeByTwoTopology() {
    return TableGroupByHashRepartitionTopology.create(
        descriptor(2),
        Arrays.asList(
            new PlanNodeId("source-0"), new PlanNodeId("source-1"), new PlanNodeId("source-2")),
            Arrays.asList(
                Arrays.asList(new PlanNodeId("exchange-00"), new PlanNodeId("exchange-01")),
                Arrays.asList(new PlanNodeId("exchange-10"), new PlanNodeId("exchange-11")),
                Arrays.asList(new PlanNodeId("exchange-20"), new PlanNodeId("exchange-21"))));
  }

  private static TableGroupByHashRepartitionTopology createTwoByThreeTopology() {
    return TableGroupByHashRepartitionTopology.create(
        descriptor(3),
        Arrays.asList(new PlanNodeId("source-0"), new PlanNodeId("source-1")),
        Arrays.asList(
            Arrays.asList(
                new PlanNodeId("exchange-00"),
                new PlanNodeId("exchange-01"),
                new PlanNodeId("exchange-02")),
            Arrays.asList(
                new PlanNodeId("exchange-10"),
                new PlanNodeId("exchange-11"),
                new PlanNodeId("exchange-12"))));
  }

  private static HashPartitioningDescriptor descriptor(int partitionCount) {
    return new HashPartitioningDescriptor(
        List.of(new Symbol("group_key")),
        HashPartitioningDescriptor.HashVersion.TABLE_HASH_V1,
        HashPartitioningDescriptor.NullRouting.HASH_NULL,
        partitionCount);
  }
}
