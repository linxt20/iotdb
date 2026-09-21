/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * How the rows of an intermediate result are spread over the parallel branches of a plan.
 *
 * <p>This is one of the two orthogonal axes used to decide where parallel branches have to be
 * merged back together. The other axis is ordering, which is tracked separately by the {@code
 * nodeOrderingMap} of {@link TableDistributedPlanGenerator}; a node that needs both a specific
 * distribution and an ordering states them independently.
 *
 * <p>A parent node declares what it <em>requires</em> from its children, a child reports what it
 * <em>provides</em>. When the provided property does not satisfy the required one, the planner
 * inserts a node that enforces it (a {@code CollectNode} for an unordered merge, a {@code
 * MergeSortNode} for an order preserving merge).
 *
 * <p>Note that {@code Broadcast} is deliberately absent: it is only meaningful once exchanges can
 * repartition data, which is out of scope here.
 */
public class DistributionProperty {

  public enum Type {
    /**
     * All rows are produced by a single branch. Required by nodes that have to observe the whole
     * result at once, such as a global aggregation without group by, a global sort, a global
     * limit/offset, a top k, and the output node.
     */
    SINGLE,
    /**
     * Rows are spread over the branches so that all rows sharing the same value of {@link #keys}
     * end up in the same branch. Required by nodes that can work on one group at a time, such as a
     * final aggregation with a group by.
     */
    PARTITIONED,
    /**
     * Rows are spread over the branches in no particular way. This is what a node requires when it
     * can process rows independently of each other, and what a partial aggregation provides.
     */
    ARBITRARY,
  }

  private static final DistributionProperty SINGLE = new DistributionProperty(Type.SINGLE);
  private static final DistributionProperty ARBITRARY = new DistributionProperty(Type.ARBITRARY);

  private final Type type;

  /** Non empty only when {@link #type} is {@link Type#PARTITIONED}. */
  private final Set<Symbol> keys;

  private DistributionProperty(Type type) {
    this(type, Collections.emptySet());
  }

  private DistributionProperty(Type type, Set<Symbol> keys) {
    this.type = type;
    this.keys = keys;
  }

  public static DistributionProperty single() {
    return SINGLE;
  }

  public static DistributionProperty arbitrary() {
    return ARBITRARY;
  }

  public static DistributionProperty partitioned(List<Symbol> keys) {
    // A partitioning on no key at all cannot tell branches apart, which is exactly what arbitrary
    // means. Collapsing it here keeps the satisfies() check below free of special cases.
    if (keys.isEmpty()) {
      return ARBITRARY;
    }
    return new DistributionProperty(
        Type.PARTITIONED, Collections.unmodifiableSet(new LinkedHashSet<>(keys)));
  }

  public Type getType() {
    return type;
  }

  public Set<Symbol> getKeys() {
    return keys;
  }

  public boolean isSingle() {
    return type == Type.SINGLE;
  }

  /**
   * Whether a result with this property can be fed to a parent that requires {@code required}
   * without inserting a node to enforce it.
   *
   * <p>A single branch satisfies everything: there is nothing to merge, and all rows of any group
   * are trivially together. A partitioning satisfies a required partitioning when it is on the same
   * keys or on a subset of them, because partitioning on fewer keys keeps together everything that
   * partitioning on more keys would.
   */
  public boolean satisfies(DistributionProperty required) {
    if (required.type == Type.ARBITRARY || this.type == Type.SINGLE) {
      return true;
    }
    if (required.type == Type.SINGLE) {
      return false;
    }
    return this.type == Type.PARTITIONED && required.keys.containsAll(this.keys);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof DistributionProperty)) {
      return false;
    }
    DistributionProperty other = (DistributionProperty) obj;
    return type == other.type && keys.equals(other.keys);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, keys);
  }

  @Override
  public String toString() {
    if (type == Type.PARTITIONED) {
      return "Partitioned" + keys;
    }
    return type == Type.SINGLE ? "Single" : "Arbitrary";
  }
}
