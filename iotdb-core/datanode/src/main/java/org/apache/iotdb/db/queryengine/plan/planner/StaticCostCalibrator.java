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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalLong;

/**
 * Deterministic historical cost calibrator. Only complete, same-key samples are admitted; missing
 * or unsupported runtime fields are never silently treated as zero.
 */
public final class StaticCostCalibrator {

  public OptionalLong medianCost(
      Collection<HistoricalSample> samples,
      String sourceId,
      String mode,
      String queryShape,
      String endpoint) {
    if (samples == null || sourceId == null || mode == null || queryShape == null) {
      return OptionalLong.empty();
    }
    List<Long> costs = new ArrayList<>();
    for (HistoricalSample sample : samples) {
      if (sample.matches(sourceId, mode, queryShape, endpoint) && sample.isUsable()) {
        costs.add(sample.cost);
      }
    }
    if (costs.isEmpty()) {
      return OptionalLong.empty();
    }
    costs.sort(Comparator.naturalOrder());
    return OptionalLong.of(costs.get((costs.size() - 1) / 2));
  }

  public StaticParallelScheduleInput buildInput(
      int inputDop,
      Collection<HistoricalSample> samples,
      String sourceId,
      String mode,
      String queryShape,
      String endpoint,
      String rawSplitReason,
      String canonicalFallbackReason) {
    OptionalLong estimate = medianCost(samples, sourceId, mode, queryShape, endpoint);
    if (estimate.isEmpty()) {
      return new StaticParallelScheduleInput(
          inputDop,
          java.util.Collections.emptyList(),
          rawSplitReason,
          canonicalFallbackReason,
          org.apache.iotdb.db.queryengine.statistics.snapshot.RuntimeSnapshotValiditySet.of(
              RuntimeSnapshotValidity.NOT_FOUND));
    }
    return new StaticParallelScheduleInput(
        inputDop,
        java.util.Collections.singletonList(estimate.getAsLong()),
        rawSplitReason,
        canonicalFallbackReason,
        org.apache.iotdb.db.queryengine.statistics.snapshot.RuntimeSnapshotValiditySet.empty());
  }

  public static final class HistoricalSample {
    private final String sourceId;
    private final String mode;
    private final String queryShape;
    private final String endpoint;
    private final long cost;
    private final boolean complete;
    private final boolean fresh;

    public HistoricalSample(
        String sourceId,
        String mode,
        String queryShape,
        String endpoint,
        long cost,
        boolean complete,
        boolean fresh) {
      if (sourceId == null || mode == null || queryShape == null) {
        throw new NullPointerException(DataNodeQueryMessages.EXCEPTION_SAMPLE_KEY_3434C941);
      }
      this.sourceId = sourceId;
      this.mode = mode;
      this.queryShape = queryShape;
      this.endpoint = endpoint;
      this.cost = cost;
      this.complete = complete;
      this.fresh = fresh;
    }

    public boolean matches(String sourceId, String mode, String queryShape, String endpoint) {
      return this.sourceId.equals(sourceId)
          && this.mode.equals(mode)
          && this.queryShape.equals(queryShape)
          && (endpoint == null || endpoint.equals(this.endpoint));
    }

    public boolean isUsable() {
      return complete && fresh && cost >= 0;
    }

    public long getCost() {
      return cost;
    }
  }
}
