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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Non-negative per-driver counter delta for dynamic scheduling inputs. */
public final class RuntimeSnapshotDelta {

  private final long fromGeneration;
  private final long toGeneration;
  private final long intervalNanos;
  private final Map<String, Long> outputRowsByDriver;
  private final RuntimeSnapshotValiditySet validity;

  private RuntimeSnapshotDelta(
      long fromGeneration,
      long toGeneration,
      long intervalNanos,
      Map<String, Long> outputRowsByDriver,
      RuntimeSnapshotValiditySet validity) {
    this.fromGeneration = fromGeneration;
    this.toGeneration = toGeneration;
    this.intervalNanos = intervalNanos;
    this.outputRowsByDriver = Collections.unmodifiableMap(new LinkedHashMap<>(outputRowsByDriver));
    this.validity = validity;
  }

  public static RuntimeSnapshotDelta between(
      QueryRuntimeSnapshot previous, QueryRuntimeSnapshot current) {
    RuntimeSnapshotValiditySet validity = RuntimeSnapshotValiditySet.empty();
    if (!previous.getQueryId().equals(current.getQueryId())
        || current.getGeneration() < previous.getGeneration()
        || current.getCapturedAtNanos() < previous.getCapturedAtNanos()) {
      validity = validity.plus(RuntimeSnapshotValidity.STALE);
    }
    long interval = current.getCapturedAtNanos() - previous.getCapturedAtNanos();
    Map<String, Long> previousRows = collectRows(previous);
    Map<String, Long> currentRows = collectRows(current);
    Map<String, Long> deltas = new LinkedHashMap<>();
    for (Map.Entry<String, Long> entry : currentRows.entrySet()) {
      Long old = previousRows.get(entry.getKey());
      if (old == null || entry.getValue() < old) {
        validity = validity.plus(RuntimeSnapshotValidity.INVALID_VALUE);
      } else {
        deltas.put(entry.getKey(), entry.getValue() - old);
      }
    }
    return new RuntimeSnapshotDelta(
        previous.getGeneration(), current.getGeneration(), interval, deltas, validity);
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

  public Map<String, Long> getOutputRowsByDriver() {
    return outputRowsByDriver;
  }

  public RuntimeSnapshotValiditySet getValidity() {
    return validity;
  }

  private static Map<String, Long> collectRows(QueryRuntimeSnapshot snapshot) {
    Map<String, Long> rows = new LinkedHashMap<>();
    for (FragmentRuntimeSnapshot fragment : snapshot.getFragments()) {
      for (DriverRuntimeSnapshot driver : fragment.getDrivers()) {
        if (driver.getOutputRows() != null) {
          rows.put(driver.getDriverTaskId(), driver.getOutputRows());
        }
      }
    }
    return rows;
  }
}
