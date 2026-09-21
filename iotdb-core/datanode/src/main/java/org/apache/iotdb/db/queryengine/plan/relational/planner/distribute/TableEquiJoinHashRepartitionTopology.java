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
 * The required two-input, P-bucket wiring contract for a table-model hash equi-join.
 *
 * <p>An equi-join needs a different topology from GROUP BY. Both inputs must be hashed with
 * compatible contracts and each final bucket must own exactly two exchange nodes: one for the
 * left input and one for the right input. The exchange nodes must live in the same cloned final
 * join fragment. Reusing one final fragment for multiple buckets is not a parallel join.
 *
 * <p>This class deliberately does not select a plan. It is a gate for the future default-off
 * rewrite: selection additionally requires a hash join executor that handles SQL null semantics,
 * duplicate keys, memory reservation and blocked inputs. Until that executor exists, {@link
 * TableEquiJoinHashRepartitionGuard} keeps the merge-sort fallback.
 */
public final class TableEquiJoinHashRepartitionTopology {

  private final HashPartitioningDescriptor leftPartitioning;
  private final HashPartitioningDescriptor rightPartitioning;
  private final PlanNodeId leftSourceSinkId;
  private final PlanNodeId rightSourceSinkId;
  private final List<PlanNodeId> leftExchangeIds;
  private final List<PlanNodeId> rightExchangeIds;

  private TableEquiJoinHashRepartitionTopology(
      HashPartitioningDescriptor leftPartitioning,
      HashPartitioningDescriptor rightPartitioning,
      PlanNodeId leftSourceSinkId,
      PlanNodeId rightSourceSinkId,
      List<PlanNodeId> leftExchangeIds,
      List<PlanNodeId> rightExchangeIds) {
    this.leftPartitioning = leftPartitioning;
    this.rightPartitioning = rightPartitioning;
    this.leftSourceSinkId = leftSourceSinkId;
    this.rightSourceSinkId = rightSourceSinkId;
    this.leftExchangeIds = leftExchangeIds;
    this.rightExchangeIds = rightExchangeIds;
  }

  /**
   * Creates the exact 2 x P source-to-final-bucket matrix required by a hash equi-join.
   *
   * <p>Left and right symbols need not have the same names; compatibility means they have equal
   * arity, hash version, null routing, and partition count. Their positional correspondence comes
   * from the {@code JoinNode.EquiJoinClause} list used to build the two descriptors.
   */
  public static TableEquiJoinHashRepartitionTopology create(
      HashPartitioningDescriptor leftPartitioning,
      HashPartitioningDescriptor rightPartitioning,
      PlanNodeId leftSourceSinkId,
      PlanNodeId rightSourceSinkId,
      List<PlanNodeId> leftExchangeIds,
      List<PlanNodeId> rightExchangeIds) {
    Objects.requireNonNull(leftPartitioning);
    Objects.requireNonNull(rightPartitioning);
    PlanNodeId leftSink = Objects.requireNonNull(leftSourceSinkId);
    PlanNodeId rightSink = Objects.requireNonNull(rightSourceSinkId);
    List<PlanNodeId> leftExchanges = List.copyOf(Objects.requireNonNull(leftExchangeIds));
    List<PlanNodeId> rightExchanges = List.copyOf(Objects.requireNonNull(rightExchangeIds));
    validate(leftPartitioning, rightPartitioning, leftSink, rightSink, leftExchanges, rightExchanges);
    return new TableEquiJoinHashRepartitionTopology(
        leftPartitioning, rightPartitioning, leftSink, rightSink, leftExchanges, rightExchanges);
  }

  public int getPartitionCount() {
    return leftPartitioning.getPartitionCount();
  }

  public HashPartitioningDescriptor getLeftPartitioning() {
    return leftPartitioning;
  }

  public HashPartitioningDescriptor getRightPartitioning() {
    return rightPartitioning;
  }

  public List<DownStreamChannelLocation> getLeftDownstreamChannels() {
    return toChannels(leftExchangeIds);
  }

  public List<DownStreamChannelLocation> getRightDownstreamChannels() {
    return toChannels(rightExchangeIds);
  }

  public PlanNodeId getLeftUpstreamExchangeId(int partition) {
    checkPartition(partition);
    return leftExchangeIds.get(partition);
  }

  public PlanNodeId getRightUpstreamExchangeId(int partition) {
    checkPartition(partition);
    return rightExchangeIds.get(partition);
  }

  /**
   * Verifies the fragment ownership that makes every bucket an independently executable join.
   *
   * <p>For bucket {@code i}, its left and right exchanges must be placed in one final fragment.
   * Final fragments for different buckets must differ, and no source sink may be placed in a final
   * fragment. DataNode placement and result equivalence remain separate runtime acceptance gates.
   */
  public FragmentTopologyValidation validateFragmentOwnership(
      Map<PlanNodeId, String> fragmentIdByNode) {
    Objects.requireNonNull(fragmentIdByNode);
    EnumSet<FragmentTopologyFailure> failures = EnumSet.noneOf(FragmentTopologyFailure.class);
    String leftSourceFragment = fragmentIdByNode.get(leftSourceSinkId);
    String rightSourceFragment = fragmentIdByNode.get(rightSourceSinkId);
    if (leftSourceFragment == null) {
      failures.add(FragmentTopologyFailure.MISSING_LEFT_SOURCE_FRAGMENT);
    }
    if (rightSourceFragment == null) {
      failures.add(FragmentTopologyFailure.MISSING_RIGHT_SOURCE_FRAGMENT);
    }

    Set<String> finalFragments = new HashSet<>();
    for (int partition = 0; partition < getPartitionCount(); partition++) {
      String leftFinalFragment = fragmentIdByNode.get(leftExchangeIds.get(partition));
      String rightFinalFragment = fragmentIdByNode.get(rightExchangeIds.get(partition));
      if (leftFinalFragment == null) {
        failures.add(FragmentTopologyFailure.MISSING_LEFT_DESTINATION_FRAGMENT);
      }
      if (rightFinalFragment == null) {
        failures.add(FragmentTopologyFailure.MISSING_RIGHT_DESTINATION_FRAGMENT);
      }
      if (leftFinalFragment != null
          && rightFinalFragment != null
          && !leftFinalFragment.equals(rightFinalFragment)) {
        failures.add(FragmentTopologyFailure.BUCKET_INPUTS_HAVE_DIFFERENT_FINAL_FRAGMENTS);
      }
      String finalFragment = leftFinalFragment == null ? rightFinalFragment : leftFinalFragment;
      if (finalFragment != null && !finalFragments.add(finalFragment)) {
        failures.add(FragmentTopologyFailure.PARTITIONS_SHARE_FINAL_FRAGMENT);
      }
    }
    if ((leftSourceFragment != null && finalFragments.contains(leftSourceFragment))
        || (rightSourceFragment != null && finalFragments.contains(rightSourceFragment))) {
      failures.add(FragmentTopologyFailure.SOURCE_AND_FINAL_SHARE_FRAGMENT);
    }
    return new FragmentTopologyValidation(failures);
  }

  public enum FragmentTopologyFailure {
    MISSING_LEFT_SOURCE_FRAGMENT,
    MISSING_RIGHT_SOURCE_FRAGMENT,
    MISSING_LEFT_DESTINATION_FRAGMENT,
    MISSING_RIGHT_DESTINATION_FRAGMENT,
    BUCKET_INPUTS_HAVE_DIFFERENT_FINAL_FRAGMENTS,
    PARTITIONS_SHARE_FINAL_FRAGMENT,
    SOURCE_AND_FINAL_SHARE_FRAGMENT
  }

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

  private static List<DownStreamChannelLocation> toChannels(List<PlanNodeId> exchangeIds) {
    List<DownStreamChannelLocation> channels = new ArrayList<>(exchangeIds.size());
    for (PlanNodeId exchangeId : exchangeIds) {
      channels.add(new DownStreamChannelLocation(exchangeId.toString()));
    }
    return channels;
  }

  private void checkPartition(int partition) {
    if (partition < 0 || partition >= getPartitionCount()) {
      throw new IllegalArgumentException();
    }
  }

  private static void validate(
      HashPartitioningDescriptor leftPartitioning,
      HashPartitioningDescriptor rightPartitioning,
      PlanNodeId leftSourceSinkId,
      PlanNodeId rightSourceSinkId,
      List<PlanNodeId> leftExchangeIds,
      List<PlanNodeId> rightExchangeIds) {
    if (!areCompatible(leftPartitioning, rightPartitioning)
        || leftSourceSinkId.equals(rightSourceSinkId)
        || leftExchangeIds.size() != leftPartitioning.getPartitionCount()
        || rightExchangeIds.size() != leftPartitioning.getPartitionCount()) {
      throw new IllegalArgumentException();
    }
    Set<PlanNodeId> allIds = new HashSet<>();
    allIds.add(leftSourceSinkId);
    allIds.add(rightSourceSinkId);
    for (PlanNodeId exchangeId : leftExchangeIds) {
      if (!allIds.add(Objects.requireNonNull(exchangeId))) {
        throw new IllegalArgumentException();
      }
    }
    for (PlanNodeId exchangeId : rightExchangeIds) {
      if (!allIds.add(Objects.requireNonNull(exchangeId))) {
        throw new IllegalArgumentException();
      }
    }
  }

  private static boolean areCompatible(
      HashPartitioningDescriptor leftPartitioning, HashPartitioningDescriptor rightPartitioning) {
    return leftPartitioning.getPartitioningSymbols().size()
            == rightPartitioning.getPartitioningSymbols().size()
        && leftPartitioning.getHashVersion() == rightPartitioning.getHashVersion()
        && leftPartitioning.getNullRouting() == rightPartitioning.getNullRouting()
        && leftPartitioning.getPartitionCount() == rightPartitioning.getPartitionCount();
  }
}
