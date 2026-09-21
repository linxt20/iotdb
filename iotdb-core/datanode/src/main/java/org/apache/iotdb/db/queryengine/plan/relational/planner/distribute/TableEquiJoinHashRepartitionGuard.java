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

import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.JoinNode;

/**
 * Conservative selection gate for table-model key-hash equi-joins.
 *
 * <p>Hashing both join inputs is not enough to make the existing table join executable. The
 * current {@code TableOperatorGenerator} creates a {@code MergeSortInnerJoinOperator}; a hash
 * shuffle destroys the input ordering it consumes. The calc layer has a deliberately unselected,
 * local blocking hash-inner-join primitive, but it is not a distributed feature: planner selection,
 * two-input bucket ownership, build-side memory admission/spill, and server E2E correctness are
 * still required. Until those gates exist, this class makes every fallback explicit instead of
 * allowing an apparently compatible equi-join to accidentally select the group-by-only exchange
 * path.
 */
final class TableEquiJoinHashRepartitionGuard {

  private TableEquiJoinHashRepartitionGuard() {}

  static FallbackReason getFallbackReason(JoinNode node) {
    if (node.getJoinType() != JoinNode.JoinType.INNER) {
      return FallbackReason.NON_INNER_JOIN;
    }
    if (node.isCrossJoin() || node.getCriteria().isEmpty()) {
      return FallbackReason.NO_EQUI_JOIN_KEYS;
    }
    if (node.getAsofCriteria().isPresent()) {
      return FallbackReason.ASOF_JOIN;
    }
    if (node.getFilter().isPresent()) {
      return FallbackReason.RESIDUAL_JOIN_FILTER;
    }
    return FallbackReason.HASH_JOIN_EXECUTOR_AND_TWO_SIDED_BUCKET_TOPOLOGY_REQUIRED;
  }

  enum FallbackReason {
    NON_INNER_JOIN,
    NO_EQUI_JOIN_KEYS,
    ASOF_JOIN,
    RESIDUAL_JOIN_FILTER,
    HASH_JOIN_EXECUTOR_AND_TWO_SIDED_BUCKET_TOPOLOGY_REQUIRED
  }
}
