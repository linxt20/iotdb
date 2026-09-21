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
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The fragment-level N x N wiring contract for a grouped table-model hash exchange.
 *
 * <p>One partial aggregation fragment is represented by a source sink. For every hash bucket it has
 * one downstream channel to an exchange in the corresponding final aggregation fragment. A
 * table-model {@code ExchangeNode} currently stores exactly one upstream, so a destination
 * partition owns one exchange node per source, rather than sharing a single exchange node among
 * sources.
 *
 * <p>This is deliberately only a topology builder. It is not selected by the planner until all of
 * the following are present: a default-off GROUP BY rewrite, cloned final aggregation fragments,
 * hash-aware {@code ShuffleSinkHandle} routing, and exchange/operator support for the resulting
 * channels. Keeping this contract separate prevents the existing round-robin shuffle from being
 * mistaken for a key-partitioned aggregation.
 */
public final class TableGroupByHashRepartitionTopology {

  private final HashPartitioningDescriptor partitioningDescriptor;
  private final List<PlanNodeId> sourceSinkNodeIds;
  private final List<List<PlanNodeId>> downstreamExchangeNodeIds;

  private TableGroupByHashRepartitionTopology(
      HashPartitioningDescriptor partitioningDescriptor,
      List<PlanNodeId> sourceSinkNodeIds,
      List<List<PlanNodeId>> downstreamExchangeNodeIds) {
    this.partitioningDescriptor = partitioningDescriptor;
    this.sourceSinkNodeIds = sourceSinkNodeIds;
    this.downstreamExchangeNodeIds = downstreamExchangeNodeIds;
  }

  /**
   * Builds a topology matrix whose rows are partial-aggregation sources and whose columns are hash
   * partitions.
   *
   * @param downstreamExchangeNodeIds source-by-partition matrix; every cell is a distinct exchange
   *     owned by the final fragment for that partition
   */
  public static TableGroupByHashRepartitionTopology create(
      HashPartitioningDescriptor partitioningDescriptor,
      List<PlanNodeId> sourceSinkNodeIds,
      List<List<PlanNodeId>> downstreamExchangeNodeIds) {
    Objects.requireNonNull(partitioningDescriptor);
    List<PlanNodeId> sourceIds = List.copyOf(Objects.requireNonNull(sourceSinkNodeIds));
    List<List<PlanNodeId>> exchangeIds = copyMatrix(downstreamExchangeNodeIds);
    validate(partitioningDescriptor, sourceIds, exchangeIds);
    return new TableGroupByHashRepartitionTopology(partitioningDescriptor, sourceIds, exchangeIds);
  }

  public HashPartitioningDescriptor getPartitioningDescriptor() {
    return partitioningDescriptor;
  }

  public int getSourceCount() {
    return sourceSinkNodeIds.size();
  }

  public int getPartitionCount() {
    return partitioningDescriptor.getPartitionCount();
  }

  /** Returns the downstream channels that must be attached to one partial aggregation sink. */
  public List<DownStreamChannelLocation> getDownstreamChannelsForSource(int sourceIndex) {
    checkSourceIndex(sourceIndex);
    List<DownStreamChannelLocation> channels = new ArrayList<>(getPartitionCount());
    for (PlanNodeId exchangeNodeId : downstreamExchangeNodeIds.get(sourceIndex)) {
      channels.add(new DownStreamChannelLocation(exchangeNodeId.toString()));
    }
    return channels;
  }

  /**
   * Returns one exchange node for every source that feeds a final aggregation partition.
   *
   * <p>The list order is the source order. It is also the required order for the final fragment
   * builder when it creates the exchange children of the final aggregation node.
   */
  public List<PlanNodeId> getUpstreamExchangeNodeIdsForPartition(int partitionIndex) {
    checkPartitionIndex(partitionIndex);
    List<PlanNodeId> exchangeNodeIds = new ArrayList<>(getSourceCount());
    for (List<PlanNodeId> sourceExchangeIds : downstreamExchangeNodeIds) {
      exchangeNodeIds.add(sourceExchangeIds.get(partitionIndex));
    }
    return exchangeNodeIds;
  }

  /**
   * Returns the sink-handle channel index which the source must use for a hash bucket.
   *
   * <p>Bucket and downstream-channel indexes deliberately match so the row router can send a
   * partitioner's bucket directly to the channel for its final fragment.
   */
  public int getDownstreamChannelIndexForPartition(int partitionIndex) {
    checkPartitionIndex(partitionIndex);
    return partitionIndex;
  }

  /**
   * Checks whether a split plan has the fragment ownership required to execute this topology.
   *
   * <p>A matrix is not executable merely because all of its sink-to-exchange edges exist. Every
   * partial source must have its own source fragment. For each bucket, all source-specific
   * exchanges must live in one final fragment, and final fragments for different buckets must be
   * distinct. The latter condition is what prevents a planner from representing P final buckets
   * as P branches of one serial fragment.
   *
   * <p>The caller supplies the plan-fragment identity for the source sinks and exchanges in this
   * topology. It may include other plan nodes; they are ignored. This method deliberately does
   * not inspect DataNode placement. Placement and result-equivalence are separate runtime gates.
   */
  public FragmentTopologyValidation validateFragmentOwnership(
      Map<PlanNodeId, String> fragmentIdByNode) {
    Objects.requireNonNull(fragmentIdByNode);
    EnumSet<FragmentTopologyFailure> failures = EnumSet.noneOf(FragmentTopologyFailure.class);

    Set<String> sourceFragmentIds = new HashSet<>();
    for (PlanNodeId sourceSinkNodeId : sourceSinkNodeIds) {
      String sourceFragmentId = fragmentIdByNode.get(sourceSinkNodeId);
      if (sourceFragmentId == null) {
        failures.add(FragmentTopologyFailure.MISSING_SOURCE_FRAGMENT);
      } else if (!sourceFragmentIds.add(sourceFragmentId)) {
        failures.add(FragmentTopologyFailure.SOURCES_SHARE_FRAGMENT);
      }
    }

    Set<String> finalFragmentIds = new HashSet<>();
    for (int partitionIndex = 0; partitionIndex < getPartitionCount(); partitionIndex++) {
      String finalFragmentId = null;
      for (PlanNodeId exchangeNodeId : getUpstreamExchangeNodeIdsForPartition(partitionIndex)) {
        String exchangeFragmentId = fragmentIdByNode.get(exchangeNodeId);
        if (exchangeFragmentId == null) {
          failures.add(FragmentTopologyFailure.MISSING_DESTINATION_FRAGMENT);
        } else if (finalFragmentId == null) {
          finalFragmentId = exchangeFragmentId;
        } else if (!finalFragmentId.equals(exchangeFragmentId)) {
          failures.add(FragmentTopologyFailure.PARTITION_EXCHANGES_HAVE_DIFFERENT_FRAGMENTS);
        }
      }
      if (finalFragmentId != null && !finalFragmentIds.add(finalFragmentId)) {
        failures.add(FragmentTopologyFailure.PARTITIONS_SHARE_FINAL_FRAGMENT);
      }
    }

    if (!Collections.disjoint(sourceFragmentIds, finalFragmentIds)) {
      failures.add(FragmentTopologyFailure.SOURCE_AND_FINAL_SHARE_FRAGMENT);
    }
    return new FragmentTopologyValidation(failures);
  }

  /** Reasons why a source-by-bucket matrix cannot yet be scheduled as an N x P exchange. */
  public enum FragmentTopologyFailure {
    MISSING_SOURCE_FRAGMENT,
    SOURCES_SHARE_FRAGMENT,
    MISSING_DESTINATION_FRAGMENT,
    PARTITION_EXCHANGES_HAVE_DIFFERENT_FRAGMENTS,
    PARTITIONS_SHARE_FINAL_FRAGMENT,
    SOURCE_AND_FINAL_SHARE_FRAGMENT
  }

  /** Immutable result of {@link #validateFragmentOwnership(Map)}. */
  public static final class FragmentTopologyValidation {
    private final Set<FragmentTopologyFailure> failures;

    private FragmentTopologyValidation(Set<FragmentTopologyFailure> failures) {
      this.failures = Collections.unmodifiableSet(EnumSet.copyOf(failures));
    }

    public boolean isExecutable() {
      return failures.isEmpty();
    }

    public Set<FragmentTopologyFailure> getFailures() {
      return failures;
    }
  }

  private void checkPartitionIndex(int partitionIndex) {
    if (partitionIndex < 0 || partitionIndex >= getPartitionCount()) {
      throw new IllegalArgumentException();
    }
  }

  private void checkSourceIndex(int sourceIndex) {
    if (sourceIndex < 0 || sourceIndex >= getSourceCount()) {
      throw new IllegalArgumentException();
    }
  }

  private static List<List<PlanNodeId>> copyMatrix(
      List<List<PlanNodeId>> downstreamExchangeNodeIds) {
    Objects.requireNonNull(downstreamExchangeNodeIds);
    List<List<PlanNodeId>> copied = new ArrayList<>(downstreamExchangeNodeIds.size());
    for (List<PlanNodeId> row : downstreamExchangeNodeIds) {
      copied.add(List.copyOf(Objects.requireNonNull(row)));
    }
    return List.copyOf(copied);
  }

  private static void validate(
      HashPartitioningDescriptor partitioningDescriptor,
      List<PlanNodeId> sourceSinkNodeIds,
      List<List<PlanNodeId>> downstreamExchangeNodeIds) {
    if (sourceSinkNodeIds.isEmpty()
        || sourceSinkNodeIds.size() != downstreamExchangeNodeIds.size()
        || new HashSet<>(sourceSinkNodeIds).size() != sourceSinkNodeIds.size()) {
      throw new IllegalArgumentException();
    }

    Set<PlanNodeId> exchangeNodeIds = new HashSet<>();
    for (List<PlanNodeId> sourceExchangeNodeIds : downstreamExchangeNodeIds) {
      if (sourceExchangeNodeIds.size() != partitioningDescriptor.getPartitionCount()) {
        throw new IllegalArgumentException();
      }
      for (PlanNodeId exchangeNodeId : sourceExchangeNodeIds) {
        if (!exchangeNodeIds.add(Objects.requireNonNull(exchangeNodeId))) {
          throw new IllegalArgumentException();
        }
      }
    }
  }
}
