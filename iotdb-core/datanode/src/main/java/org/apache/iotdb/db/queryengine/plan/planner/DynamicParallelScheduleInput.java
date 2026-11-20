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

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Inputs contain only feedback already authenticated by the snapshot/coverage aggregator. */
public final class DynamicParallelScheduleInput {
  record SemanticExtrema(String heavy, String light) {
    static <T extends Comparable<T>> SemanticExtrema select(Map<String, T> costs, T zero) {
      String heavy = null;
      String light = null;
      T max = zero;
      T min = null;
      for (Map.Entry<String, T> entry : costs.entrySet()) {
        T cost = entry.getValue();
        if (cost.compareTo(zero) <= 0) {
          continue;
        }
        String key = entry.getKey();
        int high = cost.compareTo(max);
        if (high > 0 || (high == 0 && key.compareTo(heavy) < 0)) {
          max = cost;
          heavy = key;
        }
        if (min == null) {
          min = cost;
          light = key;
        } else {
          int low = cost.compareTo(min);
          if (low < 0 || (low == 0 && key.compareTo(light) < 0)) {
            min = cost;
            light = key;
          }
        }
      }
      return min == null || max.compareTo(min) == 0
          ? new SemanticExtrema(null, null)
          : new SemanticExtrema(heavy, light);
    }
  }

  public static final class Observation {
    private final long sequence;
    private final Map<String, Long> projectedCosts;
    private final List<Long> bucketCosts;
    private final SemanticExtrema extrema;

    public Observation(long sequence, Map<String, Long> projectedCosts, List<Long> bucketCosts) {
      this(sequence, projectedCosts, bucketCosts, null);
    }

    Observation(
        long sequence,
        Map<String, Long> projectedCosts,
        List<Long> bucketCosts,
        SemanticExtrema exactExtrema) {
      if (sequence <= 0 || projectedCosts == null || bucketCosts == null) {
        throw new IllegalArgumentException(
            DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_PARALLEL_POLICY_INPUT_FAD0AEB0);
      }
      TreeMap<String, Long> copy = new TreeMap<>();
      for (Map.Entry<String, Long> e : projectedCosts.entrySet()) {
        if (e.getKey() == null
            || e.getKey().isEmpty()
            || e.getValue() == null
            || e.getValue() < 0) {
          throw new IllegalArgumentException(
              DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_PARALLEL_POLICY_INPUT_FAD0AEB0);
        }
        copy.put(e.getKey(), e.getValue());
      }
      for (Long cost : bucketCosts) {
        if (cost == null || cost < 0) {
          throw new IllegalArgumentException(
              DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_PARALLEL_POLICY_INPUT_FAD0AEB0);
        }
      }
      this.sequence = sequence;
      this.projectedCosts = Collections.unmodifiableMap(copy);
      this.bucketCosts = Collections.unmodifiableList(new ArrayList<>(bucketCosts));
      extrema = exactExtrema == null ? SemanticExtrema.select(copy, 0L) : exactExtrema;
    }

    SemanticExtrema getExtrema() {
      return extrema;
    }

    public long getSequence() {
      return sequence;
    }

    public Map<String, Long> getProjectedCosts() {
      return projectedCosts;
    }

    public List<Long> getBucketCosts() {
      return bucketCosts;
    }
  }

  private final int inputDop, previousDop, workUnitCount;
  private final ScanParallelPlan.FallbackReason qualificationReason;
  private final boolean admissionRejected;
  private final List<Observation> observations;

  public DynamicParallelScheduleInput(
      int inputDop,
      int previousDop,
      int workUnitCount,
      ScanParallelPlan.FallbackReason qualificationReason,
      boolean trustedAdmissionRejected,
      List<Observation> observations) {
    if (inputDop < 1 || previousDop < 1 || workUnitCount < 0 || observations == null) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_PARALLEL_POLICY_INPUT_FAD0AEB0);
    }
    TreeMap<Long, Observation> sorted = new TreeMap<>();
    for (Observation observation : observations) {
      if (observation == null || sorted.put(observation.sequence, observation) != null) {
        throw new IllegalArgumentException(
            DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_PARALLEL_POLICY_INPUT_FAD0AEB0);
      }
    }
    this.inputDop = inputDop;
    this.previousDop = previousDop;
    this.workUnitCount = workUnitCount;
    this.qualificationReason = qualificationReason;
    this.admissionRejected = trustedAdmissionRejected;
    this.observations = Collections.unmodifiableList(new ArrayList<>(sorted.values()));
  }

  public int getInputDop() {
    return inputDop;
  }

  public int getPreviousDop() {
    return previousDop;
  }

  public int getWorkUnitCount() {
    return workUnitCount;
  }

  public ScanParallelPlan.FallbackReason getQualificationReason() {
    return qualificationReason;
  }

  public boolean isAdmissionRejected() {
    return admissionRejected;
  }

  public List<Observation> getObservations() {
    return observations;
  }
}
