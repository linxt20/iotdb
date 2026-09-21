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

import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * The distribution and the ordering of an intermediate result, i.e. the two orthogonal axes a
 * parent node compares its requirements against.
 *
 * <p>The ordering axis deliberately reuses the existing {@link OrderingScheme} and the existing
 * {@code nodeOrderingMap} of {@link TableDistributedPlanGenerator} rather than introducing a
 * parallel notion of ordering; this type only gives the two axes a single name so that the plan
 * rules and the execution layer can talk about them the same way. A {@code null} ordering means the
 * result is not ordered, which is the same convention {@code nodeOrderingMap} already uses.
 */
public class PlanProperties {

  private final DistributionProperty distribution;

  @Nullable private final OrderingScheme ordering;

  private PlanProperties(DistributionProperty distribution, @Nullable OrderingScheme ordering) {
    this.distribution = distribution;
    this.ordering = ordering;
  }

  public static PlanProperties of(
      DistributionProperty distribution, @Nullable OrderingScheme ordering) {
    return new PlanProperties(distribution, ordering);
  }

  /** The properties a node requires when it only needs all rows in one branch, in any order. */
  public static PlanProperties singleUnordered() {
    return new PlanProperties(DistributionProperty.single(), null);
  }

  /** The properties a node requires when it does not constrain its child at all. */
  public static PlanProperties any() {
    return new PlanProperties(DistributionProperty.arbitrary(), null);
  }

  public DistributionProperty getDistribution() {
    return distribution;
  }

  @Nullable
  public OrderingScheme getOrdering() {
    return ordering;
  }

  public boolean isOrdered() {
    return ordering != null;
  }

  /**
   * Whether a result with these properties can be fed to a parent requiring {@code required}
   * without inserting a node to enforce them. An ordering requirement is only satisfied by the very
   * same ordering, since a differently ordered result has to be sorted again anyway.
   */
  public boolean satisfies(PlanProperties required) {
    if (!distribution.satisfies(required.distribution)) {
      return false;
    }
    return required.ordering == null || required.ordering.equals(ordering);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof PlanProperties)) {
      return false;
    }
    PlanProperties other = (PlanProperties) obj;
    return distribution.equals(other.distribution) && Objects.equals(ordering, other.ordering);
  }

  @Override
  public int hashCode() {
    return Objects.hash(distribution, ordering);
  }

  @Override
  public String toString() {
    return distribution + (ordering == null ? "" : "+Ordered" + ordering.getOrderBy());
  }
}
