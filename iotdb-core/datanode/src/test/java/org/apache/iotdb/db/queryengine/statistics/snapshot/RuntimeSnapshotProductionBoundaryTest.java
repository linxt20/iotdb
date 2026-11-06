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

import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TDriverRuntimeSnapshot;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class RuntimeSnapshotProductionBoundaryTest {
  private TFetchFragmentInstanceStatisticsResp response(boolean managed) {
    TFetchFragmentInstanceStatisticsResp response = new TFetchFragmentInstanceStatisticsResp();
    response.setFragmentInstanceId(new TFragmentInstanceId("q", 1, "0"));
    response.setSchemaVersion(1);
    response.setSnapshotGeneration(7L);
    response.setCapturedAtNanos(100L);
    response.setSourceEndpoint("127.0.0.1:12345");
    response.setState("FINISHED");
    response.setIsTerminal(true);
    response.setPlannedDop(managed ? 1 : 0);
    response.setActualPipelineCount(1);
    response.setPlanGeneration(1L);
    response.setPlanIdentityChecksum("plan");
    response.setWorkUnitIds(managed ? List.of("source:device:0") : List.of());
    response.setBucketIds(managed ? List.of(0) : List.of());
    response.setExchangeEdgeIds(List.of("actual-edge"));
    response.setSplitReason(managed ? "NONE" : "NO_MANAGED_SCAN");
    TDriverRuntimeSnapshot driver = new TDriverRuntimeSnapshot();
    driver.setFragmentInstanceId("q.1.0");
    driver.setDriverPipelineId("0");
    driver.setDriverTaskId("q.1.0.0");
    driver.setDriverSampleGeneration(7L);
    driver.setDriverOutputRows(24L);
    driver.setDriverTsBlockCount(1L);
    response.setDriverSnapshots(new ArrayList<>(List.of(driver)));
    response.setSnapshotChecksum(RuntimeSnapshotAdapters.fromResponse(response).getChecksum());
    return response;
  }

  private QueryRuntimeSnapshot merge(TFetchFragmentInstanceStatisticsResp response) {
    FragmentInstanceId key = FragmentInstanceId.fromThrift(response.getFragmentInstanceId());
    RuntimeSnapshotFetchResult fetched =
        new RuntimeSnapshotFetchResult(Map.of(key, response), Map.of());
    assertTrue(fetched.getErrors().isEmpty());
    return fetched.merge("q", 1L, 200L, Set.of("1.0"), Set.of(), null);
  }

  @Test
  public void productionFullDriverIdSurvivesFetchMergeAndView() {
    QueryRuntimeSnapshot merged = merge(response(true));
    assertTrue(merged.getValidity().asSet().toString(), merged.getValidity().isEmpty());
    RuntimeSnapshotView view = new RuntimeSnapshotView(merged);
    assertEquals(1, view.getActualDriverCount("1.0"));
    assertEquals(Long.valueOf(24L), view.getDrivers().get(0).getOutputRows());
  }

  @Test
  public void exchangeOnlyRootHasZeroScanDopAndPreservesActualEdge() {
    QueryRuntimeSnapshot merged = merge(response(false));
    assertTrue(merged.getValidity().asSet().toString(), merged.getValidity().isEmpty());
    RuntimeSnapshotView view = new RuntimeSnapshotView(merged);
    assertEquals(0, view.getPlanIdentity("1.0").getPlannedDop());
    assertEquals(List.of("actual-edge"), view.getExchangeEdgeIds("1.0"));
  }

  @Test
  public void zeroScanDopWithManagedWorkIsInvalid() {
    TFetchFragmentInstanceStatisticsResp response = response(true);
    response.setPlannedDop(0);
    response.unsetSnapshotChecksum();
    assertTrue(merge(response).getValidity().contains(RuntimeSnapshotValidity.INVALID_VALUE));
  }

  @Test
  public void responseMapCannotAliasAnotherFragment() {
    TFetchFragmentInstanceStatisticsResp response = response(true);
    FragmentInstanceId wrong = FragmentInstanceId.fromThrift(new TFragmentInstanceId("q", 2, "0"));
    RuntimeSnapshotFetchResult fetched =
        new RuntimeSnapshotFetchResult(Map.of(wrong, response), Map.of());
    assertEquals("FRAGMENT_INSTANCE_MISMATCH", fetched.getErrors().get(wrong));
    assertTrue(fetched.getSnapshots().isEmpty());
    assertTrue(fetched.getResponses().isEmpty());
  }

  @Test
  public void shortAndFullAliasesCannotDuplicateOneDriver() {
    TFetchFragmentInstanceStatisticsResp response = response(true);
    TDriverRuntimeSnapshot alias = response.getDriverSnapshots().get(0).deepCopy();
    alias.setFragmentInstanceId("1.0");
    response.addToDriverSnapshots(alias);
    assertThrows(
        IllegalArgumentException.class, () -> RuntimeSnapshotAdapters.fromResponse(response));
  }
}
