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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.statistics.snapshot.RuntimeSnapshotValidity;
import org.apache.iotdb.db.queryengine.statistics.snapshot.RuntimeSnapshotValiditySet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Immutable input shared by static scheduling and historical cost calibration. */
public final class StaticParallelScheduleInput {
  private final int inputDop;
  private final List<Long> estimatedCosts;
  private final String rawSplitReason;
  private final String canonicalFallbackReason;
  private final RuntimeSnapshotValiditySet validity;

  public StaticParallelScheduleInput(
      int inputDop,
      List<Long> estimatedCosts,
      String rawSplitReason,
      String canonicalFallbackReason,
      RuntimeSnapshotValiditySet validity) {
    if (inputDop < 0) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_INPUTDOP_MUST_BE_NON_NEGATIVE_A43328B5);
    }
    if (rawSplitReason == null || canonicalFallbackReason == null) {
      throw new NullPointerException(DataNodeQueryMessages.EXCEPTION_SPLIT_REASON_2528562A);
    }
    this.inputDop = inputDop;
    this.estimatedCosts = immutable(estimatedCosts);
    this.rawSplitReason = rawSplitReason;
    this.canonicalFallbackReason = canonicalFallbackReason;
    this.validity = validity == null ? RuntimeSnapshotValiditySet.empty() : validity;
  }

  public int getInputDop() {
    return inputDop;
  }

  public List<Long> getEstimatedCosts() {
    return estimatedCosts;
  }

  public String getRawSplitReason() {
    return rawSplitReason;
  }

  public String getCanonicalFallbackReason() {
    return canonicalFallbackReason;
  }

  public RuntimeSnapshotValiditySet getValidity() {
    return validity;
  }

  public boolean hasCompleteCostEstimate() {
    if (estimatedCosts.isEmpty()) {
      return false;
    }
    for (Long cost : estimatedCosts) {
      if (cost == null || cost < 0) {
        return false;
      }
    }
    return true;
  }

  public boolean isUsableForScheduling() {
    return inputDop > 0
        && hasCompleteCostEstimate()
        && validity.isEmpty()
        && !canonicalFallbackReason.equals("INVALID_COST")
        && !canonicalFallbackReason.equals("PLAN_ERROR");
  }

  public StaticParallelScheduleInput withValidity(RuntimeSnapshotValidity value) {
    return new StaticParallelScheduleInput(
        inputDop, estimatedCosts, rawSplitReason, canonicalFallbackReason, validity.plus(value));
  }

  private static List<Long> immutable(List<Long> values) {
    return values == null || values.isEmpty()
        ? Collections.emptyList()
        : Collections.unmodifiableList(new ArrayList<>(values));
  }
}
