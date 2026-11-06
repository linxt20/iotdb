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

package org.apache.iotdb.db.queryengine.statistics;

import org.apache.iotdb.mpp.rpc.thrift.TOperatorStatistics;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class OrderedStatisticsMergeTest {
  @Test
  public void orderedLocalMergeRootsRemainSeparateFromTheirScans() {
    Map<String, TOperatorStatistics> operators = new HashMap<>();
    operators.put(
        "scan-ordered-local-merge-0",
        statistic("TableMergeSortOperator", "Merge sort branches", "2", 4));
    operators.put(
        "scan-ordered-local-merge-1",
        statistic("TableMergeSortOperator", "Merge sort branches", "2", 6));
    operators.put("scan-ordered-0-0", statistic("TableScanOperator", "DeviceNumber", "1", 4));
    operators.put("scan-ordered-1-0", statistic("TableScanOperator", "DeviceNumber", "1", 6));
    StatisticsMergeUtil.mergeOperatorStatisticsIfDuplicate(operators);
    assertEquals(3, operators.size());
    assertEquals("TableScanOperator", operators.get("scan").getOperatorType());
    assertEquals(10, operators.get("scan").getOutputRows());
    assertEquals(4, operators.get("scan-ordered-local-merge-0").getOutputRows());
    assertEquals(6, operators.get("scan-ordered-local-merge-1").getOutputRows());
  }

  @Test
  public void numericAggregationRetainsOnlySupportedValuesWithoutMutatingInputs() {
    Map<String, String> a = new HashMap<>();
    a.put("both", "3");
    a.put("absent", "10");
    a.put("overflow", Long.toString(Long.MAX_VALUE));
    a.put("text", "unknown");
    Map<String, String> b = Map.of("both", "7", "overflow", "1", "text", "4");
    Map<String, String> merged =
        SpecifiedInfoMergerFactory.getMerger("TableMergeSortOperator").merge(a, b);
    assertEquals(Collections.singletonMap("both", "10"), merged);
    assertEquals("3", a.get("both"));
    assertEquals("10", a.get("absent"));
  }

  @Test
  public void missingSpecifiedInfoDoesNotFailSnapshotFreezing() {
    assertTrue(
        SpecifiedInfoMergerFactory.getMerger("ExchangeOperator")
            .merge(null, Collections.singletonMap("bytes", "10"))
            .isEmpty());
  }

  private static TOperatorStatistics statistic(String type, String key, String value, long rows) {
    TOperatorStatistics s = new TOperatorStatistics();
    s.setOperatorType(type);
    s.setOutputRows(rows);
    s.setSpecifiedInfo(new HashMap<>(Collections.singletonMap(key, value)));
    return s;
  }

  @Test
  public void typedBlockCountsRemainAlignedWithTheExistingMetricTreeCounter() {
    TOperatorStatistics a = statistic("TableMergeSortOperator", "__mt_tsBlockOutputCount", "2", 4);
    TOperatorStatistics b = statistic("TableMergeSortOperator", "__mt_tsBlockOutputCount", "3", 6);
    a.setTsBlockCount(2);
    b.setTsBlockCount(3);
    StatisticsMergeUtil.merge(a, b);
    assertEquals(5, a.getTsBlockCount());
    assertEquals("5", a.getSpecifiedInfo().get("__mt_tsBlockOutputCount"));
  }
}
