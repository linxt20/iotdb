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
   * <p>The type check is deliberately performed before a hash-exchange rewrite is considered. The
   * router hashes the SQL type the planner resolved for each partitioning symbol, and the local
   * hash join hashes the value it actually compares. Those are the same quantity only when the two
   * sides agree on one canonical form, so a coerced pair that changes the form would send rows the
   * executor calls equal to different buckets. Keeping such a pair on merge-sort is conservative
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
        // Both sides, not just the left. A left key that passes and a right key that does not would
        // let a floating or boolean-to-integer key through the gate unseen.
        Type leftType = resolvedType(typeProvider, clause.getLeft());
        Type rightType = resolvedType(typeProvider, clause.getRight());
        if (leftType == null || rightType == null) {
          return FallbackReason.UNKNOWN_HASH_KEY_TYPE;
        }
        for (Type type : new Type[] {leftType, rightType}) {
          if (type.getTypeEnum() == org.apache.tsfile.read.common.type.TypeEnum.FLOAT
              || type.getTypeEnum() == org.apache.tsfile.read.common.type.TypeEnum.DOUBLE) {
            return FallbackReason.FLOATING_POINT_HASH_SEMANTICS_UNSUPPORTED;
          }
          if (!isRepartitionSafeHashKeyType(type)) {
            return FallbackReason.UNSUPPORTED_HASH_KEY_TYPE;
          }
        }
        // Coercion must not change the canonical form. BOOLEAN and INT32 hash differently under
        // HashKeyCanonicalizer, and the two integer widths hash a negative value differently, so a
        // coerced pair would split rows the executor calls equal.
        if (canonicalForm(leftType) != canonicalForm(rightType)) {
          return FallbackReason.COERCED_KEY_TYPES_DISAGREE;
        }
      }
    }
    return FallbackReason.HASH_JOIN_EXECUTOR_AND_TWO_SIDED_BUCKET_TOPOLOGY_REQUIRED;
  }

  /**
   * The type the analyzer resolved, or null when it resolved none. A missing type is treated as a
   * refusal rather than passed through, because the caller cannot tell an absent symbol from a
   * symbol whose type was never recorded.
   */
  private static Type resolvedType(TypeProvider typeProvider, Symbol symbol) {
    return typeProvider.isSymbolExist(symbol) ? typeProvider.getTableModelType(symbol) : null;
  }

  /**
   * The canonical form a type's values hash into, ignoring the bucket count. Two types share a form
   * exactly when every value representable in both hashes the same way.
   *
   * <p>The two integer widths are separate forms on purpose. {@code Integer.hashCode} is the
   * identity, while {@code Long.hashCode} folds the two halves and so returns the complement of a
   * negative int-range value: measured on this baseline, {@code Integer.hashCode(-5)} is {@code -5}
   * and {@code Long.hashCode(-5L)} is {@code 0}. Non-negative values do agree, so a probe that only
   * tries the value 5 reads the two widths as interchangeable, but they are not. The refusal is
   * stated per type rather than per value because the planner sees types, not values.
   */
  private static CanonicalForm canonicalForm(Type type) {
    switch (type.getTypeEnum()) {
      case INT32:
      case DATE:
        return CanonicalForm.INTEGER32;
      case INT64:
      case TIMESTAMP:
        return CanonicalForm.INTEGER64;
      case BOOLEAN:
        return CanonicalForm.BOOLEAN;
      case STRING:
      case BLOB:
      case TEXT:
        return CanonicalForm.BINARY;
      default:
        return CanonicalForm.NONE;
    }
  }

  private enum CanonicalForm {
    INTEGER32,
    INTEGER64,
    BOOLEAN,
    BINARY,
    NONE
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
}
