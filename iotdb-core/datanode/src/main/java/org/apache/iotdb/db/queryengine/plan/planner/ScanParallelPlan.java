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

/** Immutable result of the pure scan split calculation. */
public final class ScanParallelPlan {

  public enum Mode {
    SERIAL,
    DEVICE,
    TIME_PARTITION,
    ORDERED
  }

  public enum FallbackReason {
    NONE,
    DOP_NOT_GREATER_THAN_ONE,
    SINGLE_DEVICE,
    SPILLED_DEVICE_ENTRIES,
    GLOBAL_LIMIT_OR_OFFSET,
    PARALLEL_SCAN_NOT_ALLOWED,
    PARALLEL_SCAN_NOT_SELECTED,
    ORDERING_NOT_PROVEN,
    EMPTY_INPUT,
    INVALID_METADATA,
    OVERFLOW,
    ESTIMATE_UNAVAILABLE,
    INVALID_COST,
    PLAN_ERROR,
    UNSUPPORTED_TIME_PARTITION_RANGE
  }

  private final Mode mode;
  private final FallbackReason fallbackReason;
  private final String rawSplitReason;
  private final String canonicalFallbackReason;
  private final int inputDop;
  private final int targetParallelism;
  private final List<List<String>> deviceGroups;
  private final List<List<Long>> timePartitionGroups;
  private final List<WorkUnit> workUnits;
  private final List<Bucket> buckets;
  private final String planIdentityChecksum;

  ScanParallelPlan(
      Mode mode,
      FallbackReason fallbackReason,
      int inputDop,
      int targetParallelism,
      List<List<String>> deviceGroups,
      List<List<Long>> timePartitionGroups,
      List<WorkUnit> workUnits,
      List<Bucket> buckets) {
    this.mode = mode;
    this.fallbackReason = fallbackReason;
    this.rawSplitReason = fallbackReason.name();
    this.canonicalFallbackReason = canonicalReason(fallbackReason);
    this.inputDop = inputDop;
    this.targetParallelism = targetParallelism;
    this.deviceGroups = immutableNestedCopy(deviceGroups);
    this.timePartitionGroups = immutableNestedCopy(timePartitionGroups);
    this.workUnits = Collections.unmodifiableList(new ArrayList<>(workUnits));
    this.buckets = Collections.unmodifiableList(new ArrayList<>(buckets));
    this.planIdentityChecksum = StaticParallelPlanChecksum.compute(this);
  }

  public Mode getMode() {
    return mode;
  }

  public FallbackReason getFallbackReason() {
    return fallbackReason;
  }

  public String getRawSplitReason() {
    return rawSplitReason;
  }

  public String getCanonicalFallbackReason() {
    return canonicalFallbackReason;
  }

  public int getInputDop() {
    return inputDop;
  }

  public int getTargetParallelism() {
    return targetParallelism;
  }

  public List<List<String>> getDeviceGroups() {
    return deviceGroups;
  }

  public List<List<Long>> getTimePartitionGroups() {
    return timePartitionGroups;
  }

  public List<WorkUnit> getWorkUnits() {
    return workUnits;
  }

  public List<Bucket> getBuckets() {
    return buckets;
  }

  public String getPlanIdentityChecksum() {
    return planIdentityChecksum;
  }

  public String getPlanChecksum() {
    return planIdentityChecksum;
  }

  public int getPipelineCount() {
    return buckets.size();
  }

  static ScanParallelPlan withTimePartitionGroups(
      ScanParallelPlan base, List<List<Long>> actualTimeGroups) {
    List<WorkUnit> units = new ArrayList<>();
    String queryId =
        base.workUnits.isEmpty() ? "unknown" : base.workUnits.get(0).sourceKey.getQueryId();
    String fragmentId =
        base.workUnits.isEmpty() ? "unknown" : base.workUnits.get(0).sourceKey.getFragmentId();
    String planNodeId =
        base.workUnits.isEmpty() ? "unknown" : base.workUnits.get(0).sourceKey.getPlanNodeId();
    for (int deviceIndex = 0; deviceIndex < base.deviceGroups.size(); deviceIndex++) {
      List<String> devices = base.deviceGroups.get(deviceIndex);
      for (int timeIndex = 0; timeIndex < actualTimeGroups.size(); timeIndex++) {
        SourceKey key =
            new SourceKey(
                queryId,
                fragmentId,
                planNodeId,
                base.mode.name(),
                Integer.toString(timeIndex),
                Integer.toString(deviceIndex));
        units.add(
            new WorkUnit(
                key,
                devices,
                actualTimeGroups.get(timeIndex),
                devices.size(),
                !base.workUnits.isEmpty() && base.workUnits.get(0).estimateSupported));
      }
    }
    int bucketCount = Math.max(1, base.buckets.size());
    List<List<WorkUnit>> contents = new ArrayList<>();
    long[] costs = new long[bucketCount];
    for (int i = 0; i < bucketCount; i++) contents.add(new ArrayList<>());
    units.sort(
        java.util.Comparator.comparingLong(WorkUnit::getEstimatedCost)
            .reversed()
            .thenComparing(WorkUnit::getSourceId));
    for (WorkUnit unit : units) {
      int selected = 0;
      for (int i = 1; i < bucketCount; i++) if (costs[i] < costs[selected]) selected = i;
      contents.get(selected).add(unit);
      costs[selected] =
          costs[selected] > Long.MAX_VALUE - unit.getEstimatedCost()
              ? Long.MAX_VALUE
              : costs[selected] + unit.getEstimatedCost();
    }
    List<Bucket> rebuilt = new ArrayList<>();
    for (int i = 0; i < bucketCount; i++) rebuilt.add(new Bucket(i, contents.get(i), costs[i]));
    return new ScanParallelPlan(
        base.mode,
        base.fallbackReason,
        base.inputDop,
        base.targetParallelism,
        base.deviceGroups,
        actualTimeGroups,
        units,
        rebuilt);
  }

  private static <T> List<List<T>> immutableNestedCopy(List<List<T>> groups) {
    List<List<T>> copy = new ArrayList<>(groups.size());
    for (List<T> group : groups) {
      copy.add(Collections.unmodifiableList(new ArrayList<>(group)));
    }
    return Collections.unmodifiableList(copy);
  }

  public static final class SourceKey {
    private final String queryId;
    private final String fragmentId;
    private final String planNodeId;
    private final String mode;
    private final String partitionId;
    private final String deviceGroupId;

    public SourceKey(
        String queryId,
        String fragmentId,
        String planNodeId,
        String mode,
        String partitionId,
        String deviceGroupId) {
      this.queryId = requireNonNull(queryId, "queryId");
      this.fragmentId = requireNonNull(fragmentId, "fragmentId");
      this.planNodeId = requireNonNull(planNodeId, "planNodeId");
      this.mode = requireNonNull(mode, "mode");
      this.partitionId = requireNonNull(partitionId, "partitionId");
      this.deviceGroupId = requireNonNull(deviceGroupId, "deviceGroupId");
    }

    public String getQueryId() {
      return queryId;
    }

    public String getFragmentId() {
      return fragmentId;
    }

    public String getPlanNodeId() {
      return planNodeId;
    }

    public String getMode() {
      return mode;
    }

    public String getPartitionId() {
      return partitionId;
    }

    public String getDeviceGroupId() {
      return deviceGroupId;
    }

    public String canonicalId() {
      return part(queryId)
          + part(fragmentId)
          + part(planNodeId)
          + part(mode)
          + part(partitionId)
          + part(deviceGroupId);
    }

    private static String part(String value) {
      return value.length() + ":" + value + "|";
    }
  }

  public static final class WorkUnit {
    private final SourceKey sourceKey;
    private final List<String> devices;
    private final List<Long> timePartitions;
    private final long estimatedCost;
    private final boolean estimateSupported;

    public WorkUnit(
        SourceKey sourceKey,
        List<String> devices,
        List<Long> timePartitions,
        long estimatedCost,
        boolean estimateSupported) {
      if (sourceKey == null) {
        throw new NullPointerException(DataNodeQueryMessages.EXCEPTION_SOURCEKEY_C1E2B835);
      }
      this.sourceKey = sourceKey;
      this.devices = immutableCopy(devices, "devices");
      this.timePartitions = immutableCopy(timePartitions, "timePartitions");
      this.estimatedCost = estimatedCost;
      this.estimateSupported = estimateSupported;
    }

    public SourceKey getSourceKey() {
      return sourceKey;
    }

    public String getSourceId() {
      return sourceKey.canonicalId();
    }

    public List<String> getDevices() {
      return devices;
    }

    public List<Long> getTimePartitions() {
      return timePartitions;
    }

    public long getEstimatedCost() {
      return estimatedCost;
    }

    public boolean isEstimateSupported() {
      return estimateSupported;
    }
  }

  public static final class Bucket {
    private final int bucketId;
    private final List<WorkUnit> workUnits;
    private final long estimatedCost;

    public Bucket(int bucketId, List<WorkUnit> workUnits, long estimatedCost) {
      if (bucketId < 0) {
        throw new IllegalArgumentException(
            DataNodeQueryMessages.EXCEPTION_BUCKETID_MUST_BE_NON_NEGATIVE_90E5ABD9);
      }
      this.bucketId = bucketId;
      this.workUnits = Collections.unmodifiableList(new ArrayList<>(workUnits));
      this.estimatedCost = estimatedCost;
    }

    public int getBucketId() {
      return bucketId;
    }

    public List<WorkUnit> getWorkUnits() {
      return workUnits;
    }

    public long getEstimatedCost() {
      return estimatedCost;
    }

    public String getDriverPipelineId() {
      return Integer.toString(bucketId);
    }
  }

  private static String canonicalReason(FallbackReason reason) {
    switch (reason) {
      case NONE:
        return "NONE";
      case PARALLEL_SCAN_NOT_SELECTED:
        return "PARALLEL_SCAN_NOT_SELECTED";
      case ORDERING_NOT_PROVEN:
        return "ORDERING_NOT_PROVEN";
      case EMPTY_INPUT:
        return "EMPTY_INPUT";
      case SPILLED_DEVICE_ENTRIES:
        return "SPILLED_DEVICE_ENTRIES";
      case ESTIMATE_UNAVAILABLE:
        return "ESTIMATE_UNAVAILABLE";
      case UNSUPPORTED_TIME_PARTITION_RANGE:
        return "UNSUPPORTED_TIME_PARTITION_RANGE";
      case INVALID_COST:
      case INVALID_METADATA:
      case OVERFLOW:
        return "INVALID_COST";
      default:
        return "PLAN_ERROR";
    }
  }

  private static <T> List<T> immutableCopy(List<T> value, String name) {
    if (value == null) {
      throw new NullPointerException(
          String.format(
              DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918, name));
    }
    return Collections.unmodifiableList(new ArrayList<>(value));
  }

  private static String requireNonNull(String value, String name) {
    if (value == null) {
      throw new NullPointerException(
          String.format(
              DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918, name));
    }
    return value;
  }
}
