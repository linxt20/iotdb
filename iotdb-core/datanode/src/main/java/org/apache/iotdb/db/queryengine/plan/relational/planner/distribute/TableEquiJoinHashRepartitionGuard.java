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
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;

import org.apache.tsfile.read.common.type.Type;

/**
 * Conservative selection gate for table-model key-hash equi-joins.
 *
 * <p>Hashing both join inputs is not enough to make the existing table join executable. The current
 * {@code TableOperatorGenerator} creates a {@code MergeSortInnerJoinOperator}; a hash shuffle
 * destroys the input ordering it consumes. The calc layer has a deliberately unselected, local
 * blocking hash-inner-join primitive, but it is not a distributed feature: planner selection,
 * two-input bucket ownership, build-side memory admission/spill, and server E2E correctness are
 * still required. Until those gates exist, this class makes every fallback explicit instead of
 * allowing an apparently compatible equi-join to accidentally select the group-by-only exchange
 * path.
 */
final class TableEquiJoinHashRepartitionGuard {

  private TableEquiJoinHashRepartitionGuard() {}

  static FallbackReason getFallbackReason(JoinNode node) {
    return getFallbackReason(node, null);
  }

  /**
   * Returns the reason a join must stay on the merge-sort path, including the key-type contract
   * required by a future repartitioned implementation.
   *
   * <p>The type check is deliberately performed before a hash-exchange rewrite is considered.
   * {@link org.apache.iotdb.db.queryengine.execution.exchange.sink.TsBlockHashPartitioner} hashes
   * the physical column objects, whereas the local hash join canonicalizes floating zero. A
   * distributed join could therefore send {@code -0.0} and {@code +0.0} to different buckets even
   * when the executor regards them as equal. Keeping floating keys on merge-sort is conservative
   * until both sides use one explicitly versioned SQL hash function.
   *
   * <p>Nullable keys do not make a type ineligible. SQL null semantics are handled at execution:
   * null-key rows are routed but never inserted into or matched by the hash table. The current
   * method still returns a fallback for every otherwise eligible key, because the two-sided
   * topology and distributed executor are not selected yet.
   */
  static FallbackReason getFallbackReason(JoinNode node, TypeProvider typeProvider) {
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
    if (typeProvider != null) {
      for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
        Type type = typeProvider.getTableModelType(clause.getLeft());
        if (type.getTypeEnum() == org.apache.tsfile.enums.TSDataType.FLOAT
            || type.getTypeEnum() == org.apache.tsfile.enums.TSDataType.DOUBLE) {
          return FallbackReason.FLOATING_POINT_HASH_SEMANTICS_UNSUPPORTED;
        }
        if (!isRepartitionSafeHashKeyType(type)) {
          return FallbackReason.UNSUPPORTED_HASH_KEY_TYPE;
        }
      }
    }
    return FallbackReason.HASH_JOIN_EXECUTOR_AND_TWO_SIDED_BUCKET_TOPOLOGY_REQUIRED;
  }

  private static boolean isRepartitionSafeHashKeyType(Type type) {
    switch (type.getTypeEnum()) {
      case INT32:
      case DATE:
      case INT64:
      case TIMESTAMP:
      case BOOLEAN:
      case STRING:
      case BLOB:
      case TEXT:
        return true;
      default:
        return false;
    }
  }

  enum FallbackReason {
    NON_INNER_JOIN,
    NO_EQUI_JOIN_KEYS,
    ASOF_JOIN,
    RESIDUAL_JOIN_FILTER,
    FLOATING_POINT_HASH_SEMANTICS_UNSUPPORTED,
    UNSUPPORTED_HASH_KEY_TYPE,
    HASH_JOIN_EXECUTOR_AND_TWO_SIDED_BUCKET_TOPOLOGY_REQUIRED
  }
}
