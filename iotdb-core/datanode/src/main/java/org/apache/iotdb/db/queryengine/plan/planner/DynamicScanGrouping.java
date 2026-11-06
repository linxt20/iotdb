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

/** Immutable external groups. Costs are in device-major, partition-minor order. */
public final class DynamicScanGrouping {
  public static final int MAX_WORK_UNITS = 1024;
  public static final long MAX_GROUPED_REFERENCES = 1_048_576L;
  private final ScanParallelPlan.Mode mode;
  private final List<List<String>> deviceGroups;
  private final List<List<Long>> timeGroups;
  private final List<Long> costs;
  private final List<DynamicCostSource> sources;

  public DynamicScanGrouping(
      ScanParallelPlan.Mode mode,
      List<List<String>> devices,
      List<List<Long>> partitions,
      List<Long> costs,
      List<DynamicCostSource> sources) {
    if (mode == null || devices == null || partitions == null || costs == null || sources == null) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SCAN_GROUPING_8A0E09F6);
    }
    this.mode = mode;
    this.deviceGroups = copyGroups(devices);
    this.timeGroups = copyGroups(partitions);
    this.costs = Collections.unmodifiableList(new ArrayList<>(costs));
    this.sources = Collections.unmodifiableList(new ArrayList<>(sources));
  }

  private static <T> List<List<T>> copyGroups(List<List<T>> groups) {
    List<List<T>> result = new ArrayList<>();
    for (List<T> group : groups) {
      if (group == null) {
        throw new IllegalArgumentException(
            DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SCAN_GROUPING_8A0E09F6);
      }
      result.add(Collections.unmodifiableList(new ArrayList<>(group)));
    }
    return Collections.unmodifiableList(result);
  }

  public ScanParallelPlan.Mode getMode() {
    return mode;
  }

  public List<List<String>> getDeviceGroups() {
    return deviceGroups;
  }

  public List<List<Long>> getTimeGroups() {
    return timeGroups;
  }

  public List<Long> getCosts() {
    return costs;
  }

  public List<DynamicCostSource> getCostSources() {
    return sources;
  }

  public long getWorkUnitCount() {
    return (long) deviceGroups.size() * timeGroups.size();
  }

  public long getGroupedReferenceCount() {
    long devices = 0, partitions = 0;
    for (List<String> group : deviceGroups) {
      devices = Math.addExact(devices, group.size());
    }
    for (List<Long> group : timeGroups) {
      partitions = Math.addExact(partitions, group.size());
    }
    return Math.addExact(
        Math.multiplyExact(devices, timeGroups.size()),
        Math.multiplyExact(partitions, deviceGroups.size()));
  }

  public DynamicScanGrouping withCosts(List<Long> values, List<DynamicCostSource> origins) {
    return new DynamicScanGrouping(mode, deviceGroups, timeGroups, values, origins);
  }
}
