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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.db.queryengine.statistics.snapshot.DynamicSnapshotHistoryStore;

import java.util.*;

/** Read-through cost view of the same bounded store. Retains no sample or predictor cache. */
public final class DynamicCostHistoryStore {
  public static final int MAX_PROJECTED_FEEDBACKS = 2;
  private final DynamicSnapshotHistoryStore store;

  public DynamicCostHistoryStore(DynamicSnapshotHistoryStore store) {
    this.store = store;
  }

  public static final class Estimate {
    private final DynamicScanGrouping grouping;
    private final List<DynamicParallelScheduleInput.Observation> observations;
    private final List<String> rejected;
    private final List<DynamicFeedbackIdentity> inputs;
    private final Integer previousDop;

    Estimate(
        DynamicScanGrouping grouping,
        List<DynamicParallelScheduleInput.Observation> observations,
        List<String> rejected,
        List<DynamicFeedbackIdentity> inputs) {
      this(grouping, observations, rejected, inputs, null);
    }

    Estimate(
        DynamicScanGrouping grouping,
        List<DynamicParallelScheduleInput.Observation> observations,
        List<String> rejected,
        List<DynamicFeedbackIdentity> inputs,
        Integer previousDop) {
      this.previousDop = previousDop;
      this.grouping = grouping;
      this.observations = Collections.unmodifiableList(new ArrayList<>(observations));
      this.rejected = Collections.unmodifiableList(new ArrayList<>(rejected));
      this.inputs = Collections.unmodifiableList(new ArrayList<>(inputs));
    }

    public DynamicScanGrouping getGrouping() {
      return grouping;
    }

    public List<DynamicParallelScheduleInput.Observation> getObservations() {
      return observations;
    }

    public List<DynamicFeedbackIdentity> getInputs() {
      return inputs;
    }

    public Integer getPreviousDop() {
      return previousDop;
    }

    public List<String> getRejected() {
      return rejected;
    }
  }

  public Estimate estimate(
      DynamicQueryShape shape, String endpoint, DynamicScanGrouping staticGrouping) {
    return estimateFromEntries(shape, store.history(shape, endpoint), staticGrouping);
  }

  public Estimate estimateFromEntries(
      DynamicQueryShape shape,
      List<DynamicSnapshotHistoryStore.Entry> entries,
      DynamicScanGrouping staticGrouping) {
    List<DynamicParallelScheduleInput.Observation> observations = new ArrayList<>();
    List<String> rejected = new ArrayList<>();
    List<DynamicFeedbackIdentity> inputs = new ArrayList<>();
    DynamicScanGrouping latest = staticGrouping;
    Integer previousDop = null;
    DynamicSnapshotAggregator aggregator = new DynamicSnapshotAggregator();
    for (int i = entries.size() - 1; i >= 0 && observations.size() < MAX_PROJECTED_FEEDBACKS; i--) {
      DynamicSnapshotHistoryStore.Entry entry = entries.get(i);
      DynamicSourcePlan source = null;
      for (DynamicSourcePlan candidate : entry.getRecord().getSourcePlans().values()) {
        if (candidate.getShape().equals(shape)) {
          if (source != null) {
            source = null;
            break;
          }
          source = candidate;
        }
      }
      if (source == null) {
        rejected.add(entry.getRecord().getRuntimeIdentity() + ":AMBIGUOUS_SOURCE");
        continue;
      }
      DynamicSnapshotAggregator.Result result = aggregator.aggregate(entry.getRecord());
      Optional<DynamicSnapshotAggregator.Projection> projection =
          aggregator.project(result, source, staticGrouping);
      if (projection.isEmpty()) {
        rejected.add(
            entry.getRecord().getRuntimeIdentity()
                + ":"
                + (result.isValid()
                    ? DynamicSnapshotAggregator.Reason.WORK_COVERAGE_MISMATCH
                    : result.getReason()));
        continue;
      }
      DynamicSnapshotAggregator.Projection cost = projection.get();
      if (observations.isEmpty()) {
        latest = cost.getGrouping();
        previousDop = source.getPlan().getBuckets().size();
      }
      inputs.add(new DynamicFeedbackIdentity(entry, source));
      observations.add(
          new DynamicParallelScheduleInput.Observation(
              entry.getSequence(), cost.getSemanticCosts(), cost.getBucketCosts()));
    }
    Collections.reverse(observations);
    Collections.reverse(inputs);
    return new Estimate(latest, observations, rejected, inputs, previousDop);
  }
}
