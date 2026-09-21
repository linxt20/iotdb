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
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValuesNode;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.junit.Assert.assertEquals;

/** Unit tests for the join selection boundary, independent of fragment placement. */
public class TableEquiJoinHashRepartitionGuardTest {

  @Test
  public void supportedLookingInnerEquiJoinStillWaitsForHashJoinExecutorAndTwoSidedBuckets() {
    assertEquals(
        TableEquiJoinHashRepartitionGuard.FallbackReason
            .HASH_JOIN_EXECUTOR_AND_TWO_SIDED_BUCKET_TOPOLOGY_REQUIRED,
        TableEquiJoinHashRepartitionGuard.getFallbackReason(createJoin(JoinNode.JoinType.INNER)));
  }

  @Test
  public void outerJoinIsRejectedBeforeAnyHashExchangeShapeIsConsidered() {
    assertEquals(
        TableEquiJoinHashRepartitionGuard.FallbackReason.NON_INNER_JOIN,
        TableEquiJoinHashRepartitionGuard.getFallbackReason(createJoin(JoinNode.JoinType.LEFT)));
  }

  @Test
  public void floatingPointKeyFallsBackBeforeAnyHashExchangeIsBuilt() {
    JoinNode join = createJoin(JoinNode.JoinType.INNER);
    TypeProvider types = TypeProvider.viewOf(new HashMap<>());
    types.putTableModelType(join.getCriteria().get(0).getLeft(), DOUBLE);
    types.putTableModelType(join.getCriteria().get(0).getRight(), DOUBLE);

    assertEquals(
        TableEquiJoinHashRepartitionGuard.FallbackReason.FLOATING_POINT_HASH_SEMANTICS_UNSUPPORTED,
        TableEquiJoinHashRepartitionGuard.getFallbackReason(join, types));
  }

  @Test
  public void integerKeyRemainsEligibleButStillRequiresExecutorAndTopology() {
    JoinNode join = createJoin(JoinNode.JoinType.INNER);
    TypeProvider types = TypeProvider.viewOf(new HashMap<>());
    types.putTableModelType(join.getCriteria().get(0).getLeft(), INT32);
    types.putTableModelType(join.getCriteria().get(0).getRight(), INT32);

    assertEquals(
        TableEquiJoinHashRepartitionGuard.FallbackReason
            .HASH_JOIN_EXECUTOR_AND_TWO_SIDED_BUCKET_TOPOLOGY_REQUIRED,
        TableEquiJoinHashRepartitionGuard.getFallbackReason(join, types));
  }

  private static JoinNode createJoin(JoinNode.JoinType joinType) {
    Symbol leftKey = new Symbol("left_key");
    Symbol rightKey = new Symbol("right_key");
    ValuesNode left =
        new ValuesNode(
            new PlanNodeId("left_values"), ImmutableList.of(leftKey), Collections.emptyList());
    ValuesNode right =
        new ValuesNode(
            new PlanNodeId("right_values"), ImmutableList.of(rightKey), Collections.emptyList());
    return new JoinNode(
        new PlanNodeId("join"),
        joinType,
        left,
        right,
        ImmutableList.of(new JoinNode.EquiJoinClause(leftKey, rightKey)),
        Optional.empty(),
        ImmutableList.of(leftKey),
        ImmutableList.of(rightKey),
        Optional.empty(),
        Optional.empty());
  }
}
