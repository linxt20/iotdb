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

import org.apache.iotdb.mpp.rpc.thrift.TDriverRuntimeSnapshot;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RuntimeSnapshotAdaptersTest {
  @Test
  public void testResponseAdapterPreservesIdentityAndUnsupportedFields() {
    TDriverRuntimeSnapshot driver = new TDriverRuntimeSnapshot();
    driver.setFragmentInstanceId("q.1.i0");
    driver.setDriverPipelineId("2");
    driver.setDriverTaskId("task-2");
    driver.setDriverSampleGeneration(7L);
    driver.setDriverOutputRows(11L);
    driver.setDriverTsBlockCount(3L);
    TFetchFragmentInstanceStatisticsResp response = new TFetchFragmentInstanceStatisticsResp();
    response.setFragmentInstanceId(new TFragmentInstanceId("q", 1, "i0"));
    response.setSnapshotGeneration(7L);
    response.setCapturedAtNanos(100L);
    response.setSourceNodeId("n0");
    response.setSourceEndpoint("n0:9000");
    response.setState("RUNNING");
    response.setPlannedDop(2);
    response.setActualPipelineCount(3);
    response.setPlanGeneration(1L);
    response.setPlanIdentityChecksum("plan");
    response.setWorkUnitIds(Collections.singletonList("w0"));
    response.setBucketIds(Collections.singletonList(0));
    response.setDriverSnapshots(Collections.singletonList(driver));

    FragmentRuntimeSnapshot snapshot = RuntimeSnapshotAdapters.fromResponse(response);
    assertEquals("q", snapshot.getQueryId());
    assertEquals("1.i0", snapshot.getFragmentInstanceId());
    assertEquals("plan", snapshot.getPlanIdentity().getPlanIdentityChecksum());
    assertEquals(Long.valueOf(11L), snapshot.getDrivers().get(0).getOutputRows());
    assertTrue(
        snapshot
            .getDrivers()
            .get(0)
            .getFieldValidityReason("driverCpuNanos")
            .startsWith("UNSUPPORTED_FIELD:"));
    assertTrue(
        snapshot
            .getDrivers()
            .get(0)
            .getFieldValidityReason("driverOutputBytes")
            .startsWith("UNSUPPORTED_FIELD:"));
  }
}
