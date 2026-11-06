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

import org.apache.iotdb.db.queryengine.statistics.snapshot.DriverRuntimeSnapshot;
import org.apache.iotdb.db.queryengine.statistics.snapshot.DynamicSnapshotRecord;

import java.math.BigInteger;
import java.util.*;

/** Checks complete ownership before attributing each real driver's wall clock once. */
public final class DynamicSnapshotAggregator {
  public enum Reason {
    VALID,
    UNSUPPORTED_SCHEMA,
    NOT_FINISHED,
    UNKNOWN_SOURCE_OR_BUCKET,
    DUPLICATE_OWNERSHIP,
    WORK_COVERAGE_MISMATCH,
    MISSING_MAPPING,
    MISSING_WALL_TIME,
    INVALID_COST,
    OVERFLOW
  }

  private record BucketKey(String source, int bucket) {}

  private record Owned(BucketKey key, ScanParallelPlan.Bucket bucket, DynamicSourcePlan source) {}

  public static final class Result {
    private final Reason reason;
    private final Map<String, List<DynamicBucketRuntime>> sources;

    Result(Reason reason, Map<String, List<DynamicBucketRuntime>> sources) {
      this.reason = reason;
      TreeMap<String, List<DynamicBucketRuntime>> copy = new TreeMap<>();
      for (Map.Entry<String, List<DynamicBucketRuntime>> e : sources.entrySet()) {
        copy.put(e.getKey(), Collections.unmodifiableList(new ArrayList<>(e.getValue())));
      }
      this.sources = Collections.unmodifiableMap(copy);
    }

    public Reason getReason() {
      return reason;
    }

    public boolean isValid() {
      return reason == Reason.VALID;
    }

    public Map<String, List<DynamicBucketRuntime>> getSources() {
      return sources;
    }
  }

  public Result aggregate(DynamicSnapshotRecord record) {
    if (!Integer.valueOf(2).equals(record.getSnapshot().getSchemaVersion())) {
      return invalid(Reason.UNSUPPORTED_SCHEMA);
    }
    if (!record.isFinishedV2()) {
      return invalid(Reason.NOT_FINISHED);
    }
    Map<BucketKey, Owned> expected = new HashMap<>();
    Map<String, List<DynamicBucketRuntime>> results = new TreeMap<>();
    Set<BucketKey> seen = new HashSet<>();
    for (DynamicSourcePlan source : record.getSourcePlans().values()) {
      results.put(source.getSourcePlanNodeId(), new ArrayList<>());
      for (ScanParallelPlan.Bucket bucket : source.getPlan().getBuckets()) {
        BucketKey key = new BucketKey(source.getSourcePlanNodeId(), bucket.getBucketId());
        if (expected.put(key, new Owned(key, bucket, source)) != null) {
          return invalid(Reason.WORK_COVERAGE_MISMATCH);
        }
      }
    }
    try {
      for (DriverRuntimeSnapshot driver : record.getRuntimeView().getDrivers()) {
        List<DynamicDriverBucketAssignment> assignments = driver.getScanAssignments();
        if (assignments == null || assignments.isEmpty()) {
          continue;
        }
        List<Owned> owned = new ArrayList<>();
        for (DynamicDriverBucketAssignment assignment : assignments) {
          BucketKey key = new BucketKey(assignment.getSourcePlanNodeId(), assignment.getBucketId());
          Owned bucket = expected.get(key);
          if (bucket == null) {
            return invalid(Reason.UNKNOWN_SOURCE_OR_BUCKET);
          }
          if (!seen.add(key)) {
            return invalid(Reason.DUPLICATE_OWNERSHIP);
          }
          Set<String> ids = new HashSet<>();
          for (ScanParallelPlan.WorkUnit unit : bucket.bucket.getWorkUnits()) {
            if (!ids.add(unit.getSourceId())) {
              return invalid(Reason.WORK_COVERAGE_MISMATCH);
            }
          }
          if (!ids.equals(new HashSet<>(assignment.getWorkUnitIds()))) {
            return invalid(Reason.WORK_COVERAGE_MISMATCH);
          }
          owned.add(bucket);
        }
        owned.sort(
            Comparator.comparing((Owned value) -> value.key.source)
                .thenComparingInt(value -> value.key.bucket));
        List<BigInteger> weights = new ArrayList<>();
        boolean anyWork = false;
        for (Owned bucket : owned) {
          BigInteger weight = BigInteger.ZERO;
          for (ScanParallelPlan.WorkUnit unit : bucket.bucket.getWorkUnits()) {
            weight = weight.add(weight(unit));
            anyWork = true;
          }
          weights.add(weight);
        }
        if (anyWork
            && (driver.getWallNanos() == null || !driver.isFieldSupported("driverWallNanos"))) {
          return invalid(Reason.MISSING_WALL_TIME);
        }
        long[] costs = anyWork ? split(driver.getWallNanos(), weights) : new long[owned.size()];
        for (int i = 0; i < owned.size(); i++) {
          Owned bucket = owned.get(i);
          List<ScanParallelPlan.WorkUnit> units = bucket.bucket.getWorkUnits();
          List<BigInteger> unitWeights = new ArrayList<>();
          for (ScanParallelPlan.WorkUnit unit : units) {
            unitWeights.add(weight(unit));
          }
          long[] unitCosts = units.isEmpty() ? new long[0] : split(costs[i], unitWeights);
          TreeMap<DynamicWorkUnitKey, Long> work = new TreeMap<>();
          for (int j = 0; j < units.size(); j++) {
            ScanParallelPlan.WorkUnit unit = units.get(j);
            DynamicWorkUnitKey key =
                new DynamicWorkUnitKey(
                    bucket.source.getShape().getRegionId(),
                    bucket.source.getShape().getSourceSemanticKey(),
                    unit.getDevices(),
                    unit.getTimePartitions());
            if (work.put(key, unitCosts[j]) != null) {
              return invalid(Reason.WORK_COVERAGE_MISMATCH);
            }
          }
          DynamicCostSource origin =
              units.isEmpty()
                  ? DynamicCostSource.EQUAL_WORK
                  : owned.size() == 1
                      ? DynamicCostSource.MEASURED_BUCKET
                      : DynamicCostSource.BUCKET_AVERAGE;
          results
              .get(bucket.key.source)
              .add(
                  new DynamicBucketRuntime(
                      bucket.key.source, bucket.key.bucket, costs[i], origin, work));
        }
      }
      if (seen.size() != expected.size()) {
        return invalid(Reason.MISSING_MAPPING);
      }
      for (List<DynamicBucketRuntime> buckets : results.values()) {
        buckets.sort(Comparator.comparingInt(DynamicBucketRuntime::getBucketId));
        Set<DynamicWorkUnitKey> work = new HashSet<>();
        long total = 0;
        for (DynamicBucketRuntime bucket : buckets) {
          total = Math.addExact(total, bucket.getCostNanos());
          for (DynamicWorkUnitKey key : bucket.getWorkCosts().keySet()) {
            if (!work.add(key)) {
              return invalid(Reason.WORK_COVERAGE_MISMATCH);
            }
          }
        }
      }
      return new Result(Reason.VALID, results);
    } catch (ArithmeticException e) {
      return invalid(Reason.OVERFLOW);
    } catch (IllegalArgumentException e) {
      return invalid(Reason.INVALID_COST);
    }
  }

  private static BigInteger weight(ScanParallelPlan.WorkUnit unit) {
    if (unit.getEstimatedCost() < 0) {
      throw new IllegalArgumentException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_INVALID_DYNAMIC_SCAN_GROUPING_8A0E09F6);
    }
    if (unit.getEstimatedCost() > 0) {
      return BigInteger.valueOf(unit.getEstimatedCost());
    }
    return BigInteger.valueOf(unit.getDevices().size())
        .multiply(BigInteger.valueOf(Math.max(1, unit.getTimePartitions().size())));
  }

  private static long[] split(long total, List<BigInteger> weights) {
    if (total < 0) {
      throw new IllegalArgumentException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_INVALID_DYNAMIC_SCAN_GROUPING_8A0E09F6);
    }
    BigInteger sum = BigInteger.ZERO;
    for (BigInteger weight : weights) {
      sum = sum.add(weight);
    }
    long[] result = new long[weights.size()];
    if (sum.signum() == 0) {
      if (total != 0) {
        throw new ArithmeticException();
      }
      return result;
    }
    long used = 0;
    for (int i = 0; i < weights.size(); i++) {
      result[i] = BigInteger.valueOf(total).multiply(weights.get(i)).divide(sum).longValueExact();
      used = Math.addExact(used, result[i]);
    }
    long remainder = total - used;
    for (int i = 0; i < result.length && remainder > 0; i++) {
      if (weights.get(i).signum() > 0) {
        result[i]++;
        remainder--;
      }
    }
    if (remainder != 0) {
      throw new ArithmeticException();
    }
    return result;
  }

  private static Result invalid(Reason reason) {
    return new Result(reason, Collections.emptyMap());
  }

  public static final class Projection {
    private final DynamicScanGrouping grouping;
    private final Map<String, Long> semanticCosts;
    private final List<Long> bucketCosts;

    Projection(DynamicScanGrouping grouping, Map<String, Long> costs, List<Long> bucketCosts) {
      this.grouping = grouping;
      semanticCosts = Collections.unmodifiableMap(new TreeMap<>(costs));
      this.bucketCosts = Collections.unmodifiableList(new ArrayList<>(bucketCosts));
    }

    public DynamicScanGrouping getGrouping() {
      return grouping;
    }

    public Map<String, Long> getSemanticCosts() {
      return semanticCosts;
    }

    public List<Long> getBucketCosts() {
      return bucketCosts;
    }
  }

  public Optional<Projection> project(
      Result result, DynamicSourcePlan source, DynamicScanGrouping target) {
    if (target.getMode() != source.getShape().getMode()
        || !result.isValid()
        || !result.getSources().containsKey(source.getSourcePlanNodeId())) {
      return Optional.empty();
    }
    try {
      List<String> allDevices = new ArrayList<>();
      for (List<String> group : target.getDeviceGroups()) {
        allDevices.addAll(group);
      }
      if (!allDevices.equals(source.getShape().getDevices())
          || new HashSet<>(allDevices).size() != allDevices.size()
          || target.getWorkUnitCount() > DynamicScanGrouping.MAX_WORK_UNITS) {
        return Optional.empty();
      }
      List<Long> oldTimes = new ArrayList<>();
      for (List<Long> group : source.getPlan().getTimePartitionGroups()) {
        oldTimes.addAll(group);
      }
      List<Long> newTimes = new ArrayList<>();
      for (List<Long> group : target.getTimeGroups()) {
        newTimes.addAll(group);
      }
      if (!new HashSet<>(oldTimes).equals(new HashSet<>(newTimes))
          || new HashSet<>(newTimes).size() != newTimes.size()) {
        return Optional.empty();
      }
      List<DynamicWorkUnitKey> targets = new ArrayList<>();
      for (List<String> devices : target.getDeviceGroups()) {
        if (devices.isEmpty()) {
          return Optional.empty();
        }
        for (List<Long> times : target.getTimeGroups()) {
          if (times.isEmpty() && !newTimes.isEmpty()) {
            return Optional.empty();
          }
          targets.add(
              new DynamicWorkUnitKey(
                  source.getShape().getRegionId(),
                  source.getShape().getSourceSemanticKey(),
                  devices,
                  times));
        }
      }
      if (targets.isEmpty()) {
        return Optional.empty();
      }
      long[] totals = new long[targets.size()];
      boolean[] estimated = new boolean[targets.size()];
      List<Long> bucketCosts = new ArrayList<>();
      for (DynamicBucketRuntime bucket : result.getSources().get(source.getSourcePlanNodeId())) {
        if (!bucket.getWorkCosts().isEmpty()) {
          bucketCosts.add(bucket.getCostNanos());
        }
        for (Map.Entry<DynamicWorkUnitKey, Long> old : bucket.getWorkCosts().entrySet()) {
          Set<String> oldDevices = new HashSet<>(old.getKey().getDevices());
          Set<Long> oldPartitions = new HashSet<>(old.getKey().getTimePartitions());
          List<BigInteger> weights = new ArrayList<>();
          BigInteger covered = BigInteger.ZERO;
          for (DynamicWorkUnitKey next : targets) {
            long d = next.getDevices().stream().filter(oldDevices::contains).count();
            long t =
                oldPartitions.isEmpty() && next.getTimePartitions().isEmpty()
                    ? 1
                    : next.getTimePartitions().stream().filter(oldPartitions::contains).count();
            BigInteger value = BigInteger.valueOf(d).multiply(BigInteger.valueOf(t));
            weights.add(value);
            covered = covered.add(value);
          }
          BigInteger expected =
              BigInteger.valueOf(oldDevices.size())
                  .multiply(BigInteger.valueOf(Math.max(1, oldPartitions.size())));
          if (!covered.equals(expected)) {
            return Optional.empty();
          }
          long[] split = split(old.getValue(), weights);
          for (int i = 0; i < targets.size(); i++) {
            totals[i] = Math.addExact(totals[i], split[i]);
            if (weights.get(i).signum() > 0
                && (!old.getKey().equals(targets.get(i))
                    || bucket.getCostSource() != DynamicCostSource.MEASURED_BUCKET
                    || bucket.getWorkCosts().size() != 1)) {
              estimated[i] = true;
            }
          }
        }
      }
      List<Long> costs = new ArrayList<>();
      List<DynamicCostSource> sources = new ArrayList<>();
      Map<String, Long> semantics = new TreeMap<>();
      for (int i = 0; i < targets.size(); i++) {
        costs.add(totals[i]);
        sources.add(
            estimated[i] ? DynamicCostSource.BUCKET_AVERAGE : DynamicCostSource.MEASURED_BUCKET);
        semantics.put(targets.get(i).canonicalId(), totals[i]);
      }
      return Optional.of(new Projection(target.withCosts(costs, sources), semantics, bucketCosts));
    } catch (ArithmeticException | IllegalArgumentException e) {
      return Optional.empty();
    }
  }
}
