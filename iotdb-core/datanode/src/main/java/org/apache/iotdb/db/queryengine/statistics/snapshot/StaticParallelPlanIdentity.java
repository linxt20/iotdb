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
package org.apache.iotdb.db.queryengine.statistics.snapshot;

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Fragment-level identity of a static scan plan. */
public final class StaticParallelPlanIdentity {
  private final String planIdentityChecksum;
  private final String queryId;
  private final String fragmentInstanceId;
  private final long planGeneration;
  private final int plannedDop;
  private final int actualPipelineCount;
  private final List<String> workUnitIds;
  private final List<Integer> bucketIds;
  private final List<String> exchangeEdgeIds;
  private final String splitReason;

  public StaticParallelPlanIdentity(
      String planIdentityChecksum,
      String queryId,
      String fragmentInstanceId,
      long planGeneration,
      int plannedDop,
      int actualPipelineCount,
      List<String> workUnitIds,
      List<Integer> bucketIds,
      List<String> exchangeEdgeIds,
      String splitReason) {
    this.planIdentityChecksum = require(planIdentityChecksum, "planIdentityChecksum");
    this.queryId = require(queryId, "queryId");
    this.fragmentInstanceId = require(fragmentInstanceId, "fragmentInstanceId");
    if (planGeneration < 0 || plannedDop < 0 || actualPipelineCount < 0) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_PLAN_IDENTITY_COUNTERS_MUST_BE_NON_NEGATIVE_52E192F8);
    }
    this.planGeneration = planGeneration;
    this.plannedDop = plannedDop;
    this.actualPipelineCount = actualPipelineCount;
    this.workUnitIds = immutable(workUnitIds);
    this.bucketIds = immutable(bucketIds);
    this.exchangeEdgeIds = immutable(exchangeEdgeIds);
    this.splitReason = splitReason;
  }

  public String getPlanIdentityChecksum() {
    return planIdentityChecksum;
  }

  public String getQueryId() {
    return queryId;
  }

  public String getFragmentInstanceId() {
    return fragmentInstanceId;
  }

  public long getPlanGeneration() {
    return planGeneration;
  }

  public int getPlannedDop() {
    return plannedDop;
  }

  public int getActualPipelineCount() {
    return actualPipelineCount;
  }

  public List<String> getWorkUnitIds() {
    return workUnitIds;
  }

  public List<Integer> getBucketIds() {
    return bucketIds;
  }

  public List<String> getExchangeEdgeIds() {
    return exchangeEdgeIds;
  }

  public String getSplitReason() {
    return splitReason;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof StaticParallelPlanIdentity)) {
      return false;
    }
    StaticParallelPlanIdentity that = (StaticParallelPlanIdentity) other;
    return planGeneration == that.planGeneration
        && plannedDop == that.plannedDop
        && actualPipelineCount == that.actualPipelineCount
        && planIdentityChecksum.equals(that.planIdentityChecksum)
        && queryId.equals(that.queryId)
        && fragmentInstanceId.equals(that.fragmentInstanceId)
        && workUnitIds.equals(that.workUnitIds)
        && bucketIds.equals(that.bucketIds)
        && exchangeEdgeIds.equals(that.exchangeEdgeIds)
        && java.util.Objects.equals(splitReason, that.splitReason);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(
        planIdentityChecksum,
        queryId,
        fragmentInstanceId,
        planGeneration,
        plannedDop,
        actualPipelineCount,
        workUnitIds,
        bucketIds,
        exchangeEdgeIds,
        splitReason);
  }

  private static String require(String value, String name) {
    if (value == null || value.isEmpty())
      throw new IllegalArgumentException(
          String.format(DataNodeQueryMessages.EXCEPTION_ARG_MUST_NOT_BE_EMPTY_313AFCE2, name));
    return value;
  }

  private static <T> List<T> immutable(List<T> values) {
    return values == null || values.isEmpty()
        ? Collections.emptyList()
        : Collections.unmodifiableList(new ArrayList<>(values));
  }
}
