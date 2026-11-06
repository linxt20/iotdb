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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Independently retained source coverage. Runtime work IDs alone cannot support regrouping. */
public final class DynamicSourcePlan {
  private final DynamicQueryShape shape;
  private final ScanParallelPlan plan;
  private final DynamicScanGrouping grouping;
  private final String sourceId, groupingChecksum;
  private final long retainedBytes;

  public DynamicSourcePlan(
      DynamicQueryShape shape, ScanParallelPlan plan, DynamicScanGrouping selectedGrouping) {
    if (shape == null || plan == null || plan.getWorkUnits().isEmpty()) {
      throw new IllegalArgumentException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    }
    List<String> coverage = new ArrayList<>();
    for (List<String> group : plan.getDeviceGroups()) {
      coverage.addAll(group);
    }
    if (!coverage.equals(shape.getDevices())) {
      throw new IllegalArgumentException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    }
    this.shape = shape;
    this.plan = plan;
    sourceId = plan.getWorkUnits().get(0).getSourceKey().getPlanNodeId();
    List<Long> costs = new ArrayList<>();
    List<DynamicCostSource> sources = new ArrayList<>();
    for (ScanParallelPlan.WorkUnit unit : plan.getWorkUnits()) {
      if (!sourceId.equals(unit.getSourceKey().getPlanNodeId())) {
        throw new IllegalArgumentException(
            org.apache.iotdb.db.i18n.DataNodeQueryMessages
                .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
      }
      costs.add(unit.getEstimatedCost());
      sources.add(
          unit.isEstimateSupported()
              ? DynamicCostSource.TSFILE_BYTES
              : DynamicCostSource.EQUAL_WORK);
    }
    if (selectedGrouping == null) {
      grouping =
          new DynamicScanGrouping(
              plan.getMode(),
              plan.getDeviceGroups(),
              plan.getTimePartitionGroups().isEmpty()
                  ? Collections.singletonList(Collections.emptyList())
                  : plan.getTimePartitionGroups(),
              costs,
              sources);
    } else {
      if (selectedGrouping.getMode() != plan.getMode()
          || !selectedGrouping.getCosts().equals(costs)
          || selectedGrouping.getCostSources().size() != costs.size()
          || !selectedGrouping.getDeviceGroups().equals(plan.getDeviceGroups())
          || !selectedGrouping.getTimeGroups().equals(plan.getTimePartitionGroups())) {
        throw new IllegalArgumentException(
            org.apache.iotdb.db.i18n.DataNodeQueryMessages
                .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
      }
      grouping = selectedGrouping;
    }
    for (int i = 0; i < sources.size(); i++) {
      if (grouping.getCostSources().get(i).isEstimateSupported()
          != plan.getWorkUnits().get(i).isEstimateSupported()) {
        throw new IllegalArgumentException(
            org.apache.iotdb.db.i18n.DataNodeQueryMessages
                .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
      }
    }
    groupingChecksum = DynamicScanGroupingChecksum.compute(grouping, plan);
    long bytes = 1024 + shape.estimatedRetainedBytes();
    for (ScanParallelPlan.WorkUnit unit : plan.getWorkUnits()) {
      bytes =
          Math.addExact(
              bytes,
              256L
                  + 2L * unit.getSourceId().length()
                  + 8L * unit.getDevices().size()
                  + 24L * unit.getTimePartitions().size());
      for (String device : unit.getDevices()) {
        bytes = Math.addExact(bytes, 40L + 2L * device.length());
      }
    }
    for (List<String> group : grouping.getDeviceGroups()) {
      bytes = Math.addExact(bytes, 24L + 8L * group.size());
      for (String device : group) {
        bytes = Math.addExact(bytes, 40L + 2L * device.length());
      }
    }
    for (List<Long> group : grouping.getTimeGroups()) {
      bytes = Math.addExact(bytes, 24L + 24L * group.size());
    }
    for (ScanParallelPlan.Bucket bucket : plan.getBuckets()) {
      bytes = Math.addExact(bytes, 64L + 8L * bucket.getWorkUnits().size());
    }
    retainedBytes = bytes;
  }

  public DynamicQueryShape getShape() {
    return shape;
  }

  public ScanParallelPlan getPlan() {
    return plan;
  }

  public DynamicScanGrouping getGrouping() {
    return grouping;
  }

  public String getSourcePlanNodeId() {
    return sourceId;
  }

  public String getPlanIdentityChecksum() {
    return plan.getPlanIdentityChecksum();
  }

  public String getGroupingChecksum() {
    return groupingChecksum;
  }

  public long estimatedRetainedBytes() {
    return retainedBytes;
  }

  public boolean verifies() {
    return plan.getPlanIdentityChecksum().equals(StaticParallelPlanChecksum.compute(plan))
        && groupingChecksum.equals(DynamicScanGroupingChecksum.compute(grouping, plan));
  }
}
