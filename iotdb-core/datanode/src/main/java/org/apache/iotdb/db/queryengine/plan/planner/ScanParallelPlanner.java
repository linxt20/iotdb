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
import java.util.Comparator;
import java.util.List;

/** Pure planner for device and time-partition split descriptions. */
public final class ScanParallelPlanner {

  public ScanParallelPlan plan(Input input) {
    if (input.devices.isEmpty() || (input.timePartitionMorsel && input.timePartitions.isEmpty())) {
      return serial(input, ScanParallelPlan.FallbackReason.EMPTY_INPUT);
    }
    if (hasDuplicates(input.devices) || hasDuplicates(input.timePartitions)) {
      return serial(input, ScanParallelPlan.FallbackReason.INVALID_METADATA);
    }
    if (input.deviceCosts != null && input.deviceCosts.size() != input.devices.size()) {
      return serial(input, ScanParallelPlan.FallbackReason.INVALID_METADATA);
    }
    if (input.deviceCosts != null) {
      for (Long cost : input.deviceCosts) {
        if (cost == null || cost < 0) {
          return serial(input, ScanParallelPlan.FallbackReason.INVALID_COST);
        }
      }
      if (hasCostOverflow(input)) {
        return serial(input, ScanParallelPlan.FallbackReason.OVERFLOW);
      }
    }
    if (input.dop <= 1) {
      return serial(input, ScanParallelPlan.FallbackReason.DOP_NOT_GREATER_THAN_ONE);
    }
    if (input.devices.size() <= 1) {
      return serial(input, ScanParallelPlan.FallbackReason.SINGLE_DEVICE);
    }
    if (input.spilledDeviceEntries) {
      return serial(input, ScanParallelPlan.FallbackReason.SPILLED_DEVICE_ENTRIES);
    }
    if (input.hasGlobalLimitOrOffset) {
      return serial(input, ScanParallelPlan.FallbackReason.GLOBAL_LIMIT_OR_OFFSET);
    }
    if (input.ordered && !input.orderingProven) {
      return serial(input, ScanParallelPlan.FallbackReason.ORDERING_NOT_PROVEN);
    }
    if (!input.allowParallelScan && !input.ordered) {
      return serial(input, ScanParallelPlan.FallbackReason.PARALLEL_SCAN_NOT_ALLOWED);
    }

    ScanParallelPlan.Mode mode =
        input.ordered ? ScanParallelPlan.Mode.ORDERED : ScanParallelPlan.Mode.DEVICE;
    long possibleWorkUnits = input.devices.size();
    if (input.timePartitionMorsel && input.timePartitions.size() > 1 && !input.ordered) {
      possibleWorkUnits = safeProduct(input.devices.size(), input.timePartitions.size());
      if (possibleWorkUnits < 0) {
        return serial(input, ScanParallelPlan.FallbackReason.OVERFLOW);
      }
      mode = ScanParallelPlan.Mode.TIME_PARTITION;
    }
    int target = (int) Math.min((long) input.dop, possibleWorkUnits);

    if (target <= 0) {
      return serial(input, ScanParallelPlan.FallbackReason.OVERFLOW);
    }
    int timeGroupCount =
        mode == ScanParallelPlan.Mode.TIME_PARTITION
            ? Math.min(input.timePartitions.size(), target)
            : 1;
    int deviceGroupCount =
        mode == ScanParallelPlan.Mode.TIME_PARTITION
            ? Math.max(1, Math.min(input.devices.size(), target / timeGroupCount))
            : Math.min(input.devices.size(), target);
    List<List<String>> deviceGroups = partitionIntoGroups(input.devices, deviceGroupCount);
    List<List<Long>> timeGroups =
        mode == ScanParallelPlan.Mode.TIME_PARTITION
            ? partitionIntoGroups(input.timePartitions, timeGroupCount)
            : Collections.singletonList(input.timePartitions);
    List<ScanParallelPlan.WorkUnit> workUnits =
        buildWorkUnits(input, mode, deviceGroups, timeGroups);
    target = Math.min(target, Math.max(1, workUnits.size()));
    return buildPlan(
        mode,
        input.deviceCosts == null
            ? ScanParallelPlan.FallbackReason.ESTIMATE_UNAVAILABLE
            : ScanParallelPlan.FallbackReason.NONE,
        input,
        target,
        deviceGroups,
        timeGroups,
        workUnits,
        target);
  }

  public static Estimate estimateDopByBytes(
      List<Long> fileSizes, long targetBytes, int deviceCount) {
    if (targetBytes <= 0 || deviceCount <= 0 || fileSizes == null || fileSizes.isEmpty()) {
      return new Estimate(Estimate.Status.INVALID_METADATA, deviceCount);
    }
    long total = 0;
    for (Long size : fileSizes) {
      if (size == null || size < 0) {
        return new Estimate(Estimate.Status.INVALID_METADATA, deviceCount);
      }
      total = saturatedAdd(total, size);
    }
    long estimated = total == 0 ? 1 : total / targetBytes + (total % targetBytes == 0 ? 0 : 1);
    return new Estimate(
        Estimate.Status.OK, (int) Math.max(1, Math.min((long) deviceCount, estimated)));
  }

  public static long safeProduct(int left, int right) {
    if (left < 0 || right < 0) {
      return -1;
    }
    return (long) left * right;
  }

  public static <T> List<List<T>> partitionIntoGroups(List<T> values, int groupCount) {
    if (groupCount <= 0) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_GROUPCOUNT_MUST_BE_POSITIVE_AA4A1FB8);
    }
    if (values.isEmpty()) {
      return Collections.emptyList();
    }
    int actual = Math.min(groupCount, values.size());
    List<List<T>> result = new ArrayList<>(actual);
    int base = values.size() / actual;
    int remainder = values.size() % actual;
    int offset = 0;
    for (int i = 0; i < actual; i++) {
      int size = base + (i < remainder ? 1 : 0);
      result.add(new ArrayList<>(values.subList(offset, offset + size)));
      offset += size;
    }
    return result;
  }

  private static ScanParallelPlan serial(Input input, ScanParallelPlan.FallbackReason reason) {
    List<List<String>> devices =
        input.devices.isEmpty()
            ? Collections.emptyList()
            : Collections.singletonList(input.devices);
    List<List<Long>> partitions =
        input.timePartitions.isEmpty()
            ? Collections.emptyList()
            : Collections.singletonList(input.timePartitions);
    List<ScanParallelPlan.WorkUnit> workUnits =
        buildWorkUnits(input, ScanParallelPlan.Mode.SERIAL, devices, partitions);
    return buildPlan(
        ScanParallelPlan.Mode.SERIAL, reason, input, 1, devices, partitions, workUnits, 1);
  }

  private static ScanParallelPlan buildPlan(
      ScanParallelPlan.Mode mode,
      ScanParallelPlan.FallbackReason reason,
      Input input,
      int target,
      List<List<String>> deviceGroups,
      List<List<Long>> timeGroups,
      List<ScanParallelPlan.WorkUnit> workUnits,
      int bucketCount) {
    List<ScanParallelPlan.Bucket> buckets = assignLpt(workUnits, Math.max(1, bucketCount));
    return new ScanParallelPlan(
        mode, reason, input.dop, target, deviceGroups, timeGroups, workUnits, buckets);
  }

  private static List<ScanParallelPlan.WorkUnit> buildWorkUnits(
      Input input,
      ScanParallelPlan.Mode mode,
      List<List<String>> deviceGroups,
      List<List<Long>> timeGroups) {
    List<ScanParallelPlan.WorkUnit> workUnits = new ArrayList<>();
    for (int deviceIndex = 0; deviceIndex < deviceGroups.size(); deviceIndex++) {
      List<String> devices = deviceGroups.get(deviceIndex);
      long deviceCost = 0;
      boolean estimateSupported =
          input.deviceCosts != null && input.deviceCosts.size() == input.devices.size();
      if (!estimateSupported) {
        deviceCost = devices.size();
      } else {
        for (String device : devices) {
          int index = input.devices.indexOf(device);
          Long cost = input.deviceCosts.get(index);
          if (cost == null || cost < 0) {
            deviceCost = devices.size();
            estimateSupported = false;
            break;
          }
          deviceCost = safeAdd(deviceCost, cost);
        }
      }
      for (int timeIndex = 0; timeIndex < timeGroups.size(); timeIndex++) {
        List<Long> partitions = timeGroups.get(timeIndex);
        long cost = safeMultiply(deviceCost, Math.max(1, partitions.size()));
        ScanParallelPlan.SourceKey sourceKey =
            new ScanParallelPlan.SourceKey(
                input.queryId,
                input.fragmentId,
                input.planNodeId,
                mode.name(),
                Integer.toString(timeIndex),
                Integer.toString(deviceIndex));
        workUnits.add(
            new ScanParallelPlan.WorkUnit(sourceKey, devices, partitions, cost, estimateSupported));
      }
    }
    return workUnits;
  }

  private static List<ScanParallelPlan.Bucket> assignLpt(
      List<ScanParallelPlan.WorkUnit> workUnits, int bucketCount) {
    List<List<ScanParallelPlan.WorkUnit>> contents = new ArrayList<>(bucketCount);
    long[] costs = new long[bucketCount];
    for (int i = 0; i < bucketCount; i++) {
      contents.add(new ArrayList<ScanParallelPlan.WorkUnit>());
    }
    List<ScanParallelPlan.WorkUnit> sorted = new ArrayList<>(workUnits);
    sorted.sort(
        Comparator.comparingLong(ScanParallelPlan.WorkUnit::getEstimatedCost)
            .reversed()
            .thenComparing(ScanParallelPlan.WorkUnit::getSourceId));
    for (ScanParallelPlan.WorkUnit workUnit : sorted) {
      int selected = 0;
      for (int i = 1; i < bucketCount; i++) {
        if (costs[i] < costs[selected]) {
          selected = i;
        }
      }
      contents.get(selected).add(workUnit);
      costs[selected] = safeAdd(costs[selected], workUnit.getEstimatedCost());
    }
    List<ScanParallelPlan.Bucket> buckets = new ArrayList<>(bucketCount);
    for (int i = 0; i < bucketCount; i++) {
      buckets.add(new ScanParallelPlan.Bucket(i, contents.get(i), costs[i]));
    }
    return buckets;
  }

  private static <T> boolean hasDuplicates(List<T> values) {
    return values.size() != new java.util.HashSet<T>(values).size();
  }

  private static boolean hasCostOverflow(Input input) {
    long total = 0;
    for (Long cost : input.deviceCosts) {
      if (cost > Long.MAX_VALUE - total) {
        return true;
      }
      total += cost;
      if (cost > 0 && input.timePartitions.size() > Long.MAX_VALUE / cost) {
        return true;
      }
    }
    return false;
  }

  private static long safeMultiply(long left, long right) {
    if (left < 0 || right < 0 || (left > 0 && right > Long.MAX_VALUE / left)) {
      return Long.MAX_VALUE;
    }
    return left * right;
  }

  private static long safeAdd(long left, long right) {
    if (right > 0 && left > Long.MAX_VALUE - right) {
      return Long.MAX_VALUE;
    }
    return left + right;
  }

  private static long saturatedAdd(long left, long right) {
    return safeAdd(left, right);
  }

  public static final class Estimate {
    public enum Status {
      OK,
      INVALID_METADATA
    }

    private final Status status;
    private final int pipelineCount;

    private Estimate(Status status, int pipelineCount) {
      this.status = status;
      this.pipelineCount = pipelineCount;
    }

    public Status getStatus() {
      return status;
    }

    public int getPipelineCount() {
      return pipelineCount;
    }
  }

  public static final class Input {
    private final List<String> devices;
    private final List<Long> timePartitions;
    private final List<Long> deviceCosts;
    private final int dop;
    private final boolean allowParallelScan;
    private final boolean spilledDeviceEntries;
    private final boolean hasGlobalLimitOrOffset;
    private final boolean ordered;
    private final boolean orderingProven;
    private final boolean timePartitionMorsel;
    private final String queryId;
    private final String fragmentId;
    private final String planNodeId;

    public Input(
        List<String> devices,
        List<Long> timePartitions,
        int dop,
        boolean allowParallelScan,
        boolean spilledDeviceEntries,
        boolean hasGlobalLimitOrOffset,
        boolean ordered,
        boolean orderingProven,
        boolean timePartitionMorsel) {
      this(
          devices,
          timePartitions,
          null,
          dop,
          allowParallelScan,
          spilledDeviceEntries,
          hasGlobalLimitOrOffset,
          ordered,
          orderingProven,
          timePartitionMorsel,
          "",
          "",
          "");
    }

    public Input(
        List<String> devices,
        List<Long> timePartitions,
        List<Long> deviceCosts,
        int dop,
        boolean allowParallelScan,
        boolean spilledDeviceEntries,
        boolean hasGlobalLimitOrOffset,
        boolean ordered,
        boolean orderingProven,
        boolean timePartitionMorsel) {
      this(
          devices,
          timePartitions,
          deviceCosts,
          dop,
          allowParallelScan,
          spilledDeviceEntries,
          hasGlobalLimitOrOffset,
          ordered,
          orderingProven,
          timePartitionMorsel,
          "",
          "",
          "");
    }

    public Input(
        List<String> devices,
        List<Long> timePartitions,
        List<Long> deviceCosts,
        int dop,
        boolean allowParallelScan,
        boolean spilledDeviceEntries,
        boolean hasGlobalLimitOrOffset,
        boolean ordered,
        boolean orderingProven,
        boolean timePartitionMorsel,
        String queryId,
        String fragmentId,
        String planNodeId) {
      this.devices = immutableCopy(devices, "devices");
      this.timePartitions = immutableCopy(timePartitions, "timePartitions");
      this.deviceCosts = deviceCosts == null ? null : immutableCopy(deviceCosts, "deviceCosts");
      this.dop = dop;
      this.allowParallelScan = allowParallelScan;
      this.spilledDeviceEntries = spilledDeviceEntries;
      this.hasGlobalLimitOrOffset = hasGlobalLimitOrOffset;
      this.ordered = ordered;
      this.orderingProven = orderingProven;
      this.timePartitionMorsel = timePartitionMorsel;
      this.queryId = requireNonNull(queryId, "queryId");
      this.fragmentId = requireNonNull(fragmentId, "fragmentId");
      this.planNodeId = requireNonNull(planNodeId, "planNodeId");
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
}
