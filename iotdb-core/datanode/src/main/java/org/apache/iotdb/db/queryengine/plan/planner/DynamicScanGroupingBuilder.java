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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Bounds materialized groups independently of the requested number of buckets. */
public final class DynamicScanGroupingBuilder {
  public DynamicScanGrouping build(ScanParallelPlanner.Input input) {
    if (input.getDevices().isEmpty()
        || (input.isTimePartitionMorsel() && input.getTimePartitions().isEmpty())) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SCAN_GROUPING_8A0E09F6);
    }
    boolean byTime =
        input.isTimePartitionMorsel() && input.getTimePartitions().size() > 1 && !input.isOrdered();
    int t = 1, d = 1;
    long bestCount = 0, bestRefs = Long.MAX_VALUE;
    int maxD = Math.min(input.getDevices().size(), DynamicScanGrouping.MAX_WORK_UNITS);
    for (int candidateD = 1; candidateD <= maxD; candidateD++) {
      int maxT =
          byTime
              ? Math.min(
                  input.getTimePartitions().size(), DynamicScanGrouping.MAX_WORK_UNITS / candidateD)
              : 1;
      for (int candidateT = 1; candidateT <= maxT; candidateT++) {
        long count = (long) candidateD * candidateT;
        long refs =
            (long) input.getDevices().size() * candidateT
                + (long) input.getTimePartitions().size() * candidateD;
        if (refs <= DynamicScanGrouping.MAX_GROUPED_REFERENCES
            && (count > bestCount
                || count == bestCount && (refs < bestRefs || refs == bestRefs && candidateT > t))) {
          d = candidateD;
          t = candidateT;
          bestCount = count;
          bestRefs = refs;
        }
      }
    }
    List<List<String>> devices = ScanParallelPlanner.partitionIntoGroups(input.getDevices(), d);
    List<List<Long>> times =
        byTime
            ? ScanParallelPlanner.partitionIntoGroups(input.getTimePartitions(), t)
            : Collections.singletonList(input.getTimePartitions());
    Map<String, Long> fileCosts = new HashMap<>();
    boolean available =
        input.getDeviceCosts() != null
            && input.getDeviceCosts().size() == input.getDevices().size();
    if (available) {
      for (int i = 0; i < input.getDevices().size(); i++) {
        Long cost = input.getDeviceCosts().get(i);
        if (cost == null || cost < 0) {
          available = false;
          break;
        }
        fileCosts.put(input.getDevices().get(i), cost);
      }
    }
    List<Long> costs = new ArrayList<>();
    List<DynamicCostSource> sources = new ArrayList<>();
    for (List<String> dg : devices) {
      long sum = 0;
      for (String device : dg) {
        sum = Math.addExact(sum, available ? fileCosts.get(device) : 1L);
      }
      for (List<Long> tg : times) {
        costs.add(Math.multiplyExact(sum, Math.max(1, tg.size())));
        sources.add(available ? DynamicCostSource.TSFILE_BYTES : DynamicCostSource.EQUAL_WORK);
      }
    }
    return new DynamicScanGrouping(
        input.isOrdered()
            ? ScanParallelPlan.Mode.ORDERED
            : byTime ? ScanParallelPlan.Mode.TIME_PARTITION : ScanParallelPlan.Mode.DEVICE,
        devices,
        times,
        costs,
        sources);
  }
}
