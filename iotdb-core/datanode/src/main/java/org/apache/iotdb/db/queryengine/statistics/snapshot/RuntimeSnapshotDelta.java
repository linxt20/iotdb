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

package org.apache.iotdb.db.queryengine.statistics.snapshot;

import java.util.*;

/** Same-query counter intervals. Invalid scope or counters never become scheduling deltas. */
public final class RuntimeSnapshotDelta {
  private final long fromGeneration, toGeneration, intervalNanos;
  private final Map<RuntimeSnapshotDriverKey, Long> outputRows, wallNanos, tsBlocks;
  private final RuntimeSnapshotValiditySet validity;

  private RuntimeSnapshotDelta(
      long from,
      long to,
      long interval,
      Map<RuntimeSnapshotDriverKey, Long> rows,
      Map<RuntimeSnapshotDriverKey, Long> wall,
      Map<RuntimeSnapshotDriverKey, Long> blocks,
      RuntimeSnapshotValiditySet validity) {
    fromGeneration = from;
    toGeneration = to;
    intervalNanos = interval;
    outputRows = Collections.unmodifiableMap(new LinkedHashMap<>(rows));
    wallNanos = Collections.unmodifiableMap(new LinkedHashMap<>(wall));
    tsBlocks = Collections.unmodifiableMap(new LinkedHashMap<>(blocks));
    this.validity = validity;
  }

  public static RuntimeSnapshotDelta between(
      QueryRuntimeSnapshot previous, QueryRuntimeSnapshot current) {
    RuntimeSnapshotValiditySet validity = RuntimeSnapshotValiditySet.empty();
    for (RuntimeSnapshotValidity reason : previous.getValidity().asSet())
      validity = validity.plus(reason);
    for (RuntimeSnapshotValidity reason : current.getValidity().asSet())
      validity = validity.plus(reason);
    Map<RuntimeSnapshotDriverKey, Long> rows = new LinkedHashMap<>(),
        wall = new LinkedHashMap<>(),
        blocks = new LinkedHashMap<>();
    long interval = 0;
    try {
      interval = Math.subtractExact(current.getCapturedAtNanos(), previous.getCapturedAtNanos());
    } catch (ArithmeticException e) {
      validity = validity.plus(RuntimeSnapshotValidity.OVERFLOW);
    }
    if (!previous.getQueryId().equals(current.getQueryId())
        || current.getGeneration() <= previous.getGeneration())
      validity = validity.plus(RuntimeSnapshotValidity.STALE);
    if (interval <= 0) validity = validity.plus(RuntimeSnapshotValidity.CLOCK_INVALID);
    if (!validity.isEmpty())
      return new RuntimeSnapshotDelta(
          previous.getGeneration(),
          current.getGeneration(),
          interval,
          rows,
          wall,
          blocks,
          validity);
    Map<String, FragmentRuntimeSnapshot> oldFragments = new LinkedHashMap<>(),
        newFragments = new LinkedHashMap<>();
    for (FragmentRuntimeSnapshot f : previous.getFragments())
      if (oldFragments.put(f.getFragmentInstanceId(), f) != null)
        validity = validity.plus(RuntimeSnapshotValidity.INVALID_VALUE);
    for (FragmentRuntimeSnapshot f : current.getFragments())
      if (newFragments.put(f.getFragmentInstanceId(), f) != null)
        validity = validity.plus(RuntimeSnapshotValidity.INVALID_VALUE);
    if (!oldFragments.keySet().equals(newFragments.keySet()))
      validity = validity.plus(RuntimeSnapshotValidity.PARTIAL);
    for (FragmentRuntimeSnapshot now : newFragments.values()) {
      FragmentRuntimeSnapshot old = oldFragments.get(now.getFragmentInstanceId());
      if (old == null) continue;
      if (!old.getQueryId().equals(current.getQueryId())
          || !now.getQueryId().equals(current.getQueryId())
          || now.getGeneration() <= old.getGeneration()) {
        validity = validity.plus(RuntimeSnapshotValidity.STALE);
        continue;
      }
      if (!Objects.equals(old.getSourceEndpoint(), now.getSourceEndpoint())
          || !Objects.equals(old.getSourceNodeId(), now.getSourceNodeId())) {
        validity = validity.plus(RuntimeSnapshotValidity.ENDPOINT_MISMATCH);
        continue;
      }
      if (old.getPlanIdentity() == null || now.getPlanIdentity() == null) {
        validity = validity.plus(RuntimeSnapshotValidity.UNSUPPORTED_FIELD);
        continue;
      }
      if (!old.getPlanIdentity().equals(now.getPlanIdentity())) {
        validity = validity.plus(RuntimeSnapshotValidity.STALE);
        continue;
      }
      if (!old.getValidity().isEmpty() || !now.getValidity().isEmpty()) {
        for (RuntimeSnapshotValidity reason : old.getValidity().asSet())
          validity = validity.plus(reason);
        for (RuntimeSnapshotValidity reason : now.getValidity().asSet())
          validity = validity.plus(reason);
        continue;
      }
      Map<RuntimeSnapshotDriverKey, DriverRuntimeSnapshot>
          oldDrivers = drivers(previous.getQueryId(), old),
          newDrivers = drivers(current.getQueryId(), now);
      if (oldDrivers.size() != old.getDrivers().size()
          || newDrivers.size() != now.getDrivers().size()) {
        validity = validity.plus(RuntimeSnapshotValidity.INVALID_VALUE);
        continue;
      }
      if (!oldDrivers.keySet().equals(newDrivers.keySet()))
        validity = validity.plus(RuntimeSnapshotValidity.PARTIAL);
      for (Map.Entry<RuntimeSnapshotDriverKey, DriverRuntimeSnapshot> e : newDrivers.entrySet()) {
        DriverRuntimeSnapshot before = oldDrivers.get(e.getKey()), after = e.getValue();
        if (before == null) continue;
        if (after.getSampleGeneration() <= before.getSampleGeneration()
            || !Objects.equals(before.getScanAssignments(), after.getScanAssignments())) {
          validity = validity.plus(RuntimeSnapshotValidity.STALE);
          continue;
        }
        if (before.getWallNanos() == null || after.getWallNanos() == null) {
          validity = validity.plus(RuntimeSnapshotValidity.UNSUPPORTED_FIELD);
          continue;
        }
        if (after.getWallNanos() < before.getWallNanos()) {
          validity = validity.plus(RuntimeSnapshotValidity.INVALID_VALUE);
          continue;
        }
        if ((before.getTsBlocks() == null) != (after.getTsBlocks() == null)) {
          validity = validity.plus(RuntimeSnapshotValidity.UNSUPPORTED_FIELD);
        } else if (before.getTsBlocks() != null) {
          if (after.getTsBlocks() < before.getTsBlocks())
            validity = validity.plus(RuntimeSnapshotValidity.INVALID_VALUE);
          else blocks.put(e.getKey(), after.getTsBlocks() - before.getTsBlocks());
        }
        wall.put(e.getKey(), after.getWallNanos() - before.getWallNanos());
        if (before.getOutputRows() != null && after.getOutputRows() != null) {
          if (after.getOutputRows() < before.getOutputRows())
            validity = validity.plus(RuntimeSnapshotValidity.INVALID_VALUE);
          else rows.put(e.getKey(), after.getOutputRows() - before.getOutputRows());
        }
      }
    }
    // Partial or invalid intervals are diagnostic only; consumers cannot accidentally use a subset.
    if (!validity.isEmpty()) {
      rows.clear();
      wall.clear();
      blocks.clear();
    }
    return new RuntimeSnapshotDelta(
        previous.getGeneration(), current.getGeneration(), interval, rows, wall, blocks, validity);
  }

  private static Map<RuntimeSnapshotDriverKey, DriverRuntimeSnapshot> drivers(
      String query, FragmentRuntimeSnapshot fragment) {
    Map<RuntimeSnapshotDriverKey, DriverRuntimeSnapshot> result = new LinkedHashMap<>();
    for (DriverRuntimeSnapshot driver : fragment.getDrivers()) {
      if (!fragment.getFragmentInstanceId().equals(driver.getFragmentInstanceId())
          && !(query + "." + fragment.getFragmentInstanceId())
              .equals(driver.getFragmentInstanceId())) continue;
      result.put(
          new RuntimeSnapshotDriverKey(
              query,
              fragment.getFragmentInstanceId(),
              driver.getDriverPipelineId(),
              driver.getDriverTaskId()),
          driver);
    }
    return result;
  }

  public long getFromGeneration() {
    return fromGeneration;
  }

  public long getToGeneration() {
    return toGeneration;
  }

  public long getIntervalNanos() {
    return intervalNanos;
  }

  public Map<RuntimeSnapshotDriverKey, Long> getOutputRowsByDriverKey() {
    return outputRows;
  }

  public Map<RuntimeSnapshotDriverKey, Long> getWallNanosByDriverKey() {
    return wallNanos;
  }

  public Map<RuntimeSnapshotDriverKey, Long> getTsBlocksByDriverKey() {
    return tsBlocks;
  }

  /** Compatibility view omits ambiguous task labels; scheduling uses the structured-key maps. */
  public Map<String, Long> getOutputRowsByDriver() {
    Map<String, Long> values = new LinkedHashMap<>();
    Set<String> ambiguous = new HashSet<>();
    for (Map.Entry<RuntimeSnapshotDriverKey, Long> e : outputRows.entrySet()) {
      String task = e.getKey().getTaskId();
      if (values.putIfAbsent(task, e.getValue()) != null) ambiguous.add(task);
    }
    for (String task : ambiguous) values.remove(task);
    return Collections.unmodifiableMap(values);
  }

  public RuntimeSnapshotValiditySet getValidity() {
    return validity;
  }
}
