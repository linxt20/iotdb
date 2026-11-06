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

package org.apache.iotdb.db.queryengine.statistics;

import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.mpp.rpc.thrift.TDriverRuntimeSnapshot;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.mpp.rpc.thrift.TQueryStatistics;

import org.junit.Test;

import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class FragmentInstanceStatisticsDrawerTest {

  @Test
  public void testRuntimeSnapshotFieldsAreRendered() {
    LimitNode planNode =
        new LimitNode(new PlanNodeId("node1"), null, 10, java.util.Optional.empty());
    FragmentInstanceId id = new FragmentInstanceId(new PlanFragmentId("q", 1), "inst-0");
    FragmentInstance instance =
        new FragmentInstance(
            new PlanFragment(new PlanFragmentId("q", 1), planNode),
            id,
            null,
            null,
            60000L,
            new SessionInfo(0, "root", ZoneId.systemDefault()),
            false,
            false);
    TFetchFragmentInstanceStatisticsResp response = new TFetchFragmentInstanceStatisticsResp();
    response.setDataRegion("root.db.region1");
    response.setIp("127.0.0.1");
    response.setState("FINISHED");
    response.setStartTimeInMS(1L);
    response.setEndTimeInMS(2L);
    response.setQueryStatistics(new TQueryStatistics());
    response.setOperatorStatisticsMap(new HashMap<>());
    response.setSchemaVersion(1);
    response.setSnapshotGeneration(2L);
    response.setSnapshotChecksum("checksum");
    response.setPlannedDop(2);
    response.setActualPipelineCount(2);
    response.setSplitMode("DEVICE");
    TDriverRuntimeSnapshot driver = new TDriverRuntimeSnapshot();
    driver.setFragmentInstanceId(id.toString());
    driver.setDriverPipelineId("1");
    driver.setDriverTaskId("q.1-1");
    driver.setDriverSampleGeneration(2L);
    driver.setDriverWallNanos(10L);
    response.setDriverSnapshots(Collections.singletonList(driver));

    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> all = new HashMap<>();
    all.put(id, response);
    java.util.List<StatisticLine> lines =
        new FragmentInstanceStatisticsDrawer()
            .renderFragmentInstances(Collections.singletonList(instance), all, false);
    String rendered =
        lines.stream().map(StatisticLine::getValue).reduce("", (a, b) -> a + "\n" + b);
    assertTrue(rendered.contains("generation: 2"));
    assertTrue(rendered.contains("driverWallNanos: 10"));
    assertTrue(rendered.contains("plannedDop: 2"));
  }
}
