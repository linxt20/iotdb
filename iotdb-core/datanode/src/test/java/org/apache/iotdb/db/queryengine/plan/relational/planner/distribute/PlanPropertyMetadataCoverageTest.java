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

package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceFetchNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryCountNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryScanNode;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class PlanPropertyMetadataCoverageTest {
  private int previousDataNodeId;

  @Before
  public void initializeDataNodeIdentity() {
    var config = IoTDBDescriptor.getInstance().getConfig();
    previousDataNodeId = config.getDataNodeId();
    config.setDataNodeId(0);
  }

  @After
  public void restoreDataNodeIdentity() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(previousDataNodeId);
  }

  private void assertOutput(int sourceCount, int kind) {
    TSeriesPartitionSlot first = new TSeriesPartitionSlot(0), second = new TSeriesPartitionSlot(1);
    Map<TSeriesPartitionSlot, TRegionReplicaSet> regions = new LinkedHashMap<>();
    regions.put(
        first,
        new TRegionReplicaSet(
            new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 1), List.of()));
    if (sourceCount == 2)
      regions.put(
          second,
          new TRegionReplicaSet(
              new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 2), List.of()));
    SchemaPartition schema = mock(SchemaPartition.class);
    when(schema.getSchemaPartitionMap()).thenReturn(Map.of("db", regions));
    Analysis analysis = mock(Analysis.class);
    when(analysis.getSchemaPartitionInfo()).thenReturn(schema);
    MPPQueryContext context = mock(MPPQueryContext.class);
    when(context.getQueryId()).thenReturn(new QueryId("metadata_properties"));
    TableDistributedPlanGenerator generator =
        new TableDistributedPlanGenerator(context, analysis, null, null);
    TableDistributedPlanGenerator.PlanContext planContext =
        new TableDistributedPlanGenerator.PlanContext();
    PlanNodeId id = new PlanNodeId("metadata");
    List<PlanNode> branches;
    if (kind == 0) {
      branches =
          generator.visitTableDeviceQueryScan(
              new TableDeviceQueryScanNode(
                  id, "db", "t", List.of(), null, List.of(), null, -1, false),
              planContext);
    } else if (kind == 1) {
      branches =
          generator.visitTableDeviceQueryCount(
              new TableDeviceQueryCountNode(id, "db", "t", List.of(), null, List.of()),
              planContext);
    } else {
      IDeviceID a = mock(IDeviceID.class), b = mock(IDeviceID.class);
      when(schema.calculateDeviceGroupId(a)).thenReturn(first);
      when(schema.calculateDeviceGroupId(b)).thenReturn(second);
      branches =
          generator.visitTableDeviceFetch(
              new TableDeviceFetchNode(
                  id,
                  "db",
                  "t",
                  List.of(new Object[] {"a"}, new Object[] {"b"}),
                  List.of(a, b),
                  List.of(),
                  null),
              planContext);
    }
    assertEquals(sourceCount, branches.size());
    assertEquals(
        sourceCount,
        new HashSet<>(branches.stream().map(PlanNode::getPlanNodeId).collect(Collectors.toList()))
            .size());
    PlanPropertyProvider provider = new PlanPropertyProvider();
    for (PlanNode branch : branches) {
      assertEquals(
          PlanPropertyProvider.Coverage.KNOWN, provider.providedBy(branch, Map.of()).getCoverage());
      assertEquals(
          PlanProperties.singleUnordered(), provider.providedBy(branch, Map.of()).getProperties());
    }
    PlanPropertyProvider.ProvidedProperties provided =
        provider.providedByBranches(branches, null, Map.of());
    assertEquals(PlanPropertyProvider.Coverage.KNOWN, provided.getCoverage());
    assertEquals(sourceCount == 1, provided.getProperties().getDistribution().isSingle());
    assertNotNull(planContext.mostUsedRegion);
    if (kind == 2) {
      assertEquals(
          2,
          branches.stream()
              .map(TableDeviceFetchNode.class::cast)
              .mapToInt(n -> n.getDeviceIdList().size())
              .sum());
    }
  }

  @Test
  public void oneSchemaRegionQueryScan() {
    assertOutput(1, 0);
  }

  @Test
  public void twoSchemaRegionQueryScan() {
    assertOutput(2, 0);
  }

  @Test
  public void oneSchemaRegionQueryCount() {
    assertOutput(1, 1);
  }

  @Test
  public void twoSchemaRegionQueryCount() {
    assertOutput(2, 1);
  }

  @Test
  public void oneSchemaRegionDeviceFetch() {
    assertOutput(1, 2);
  }

  @Test
  public void twoSchemaRegionDeviceFetch() {
    assertOutput(2, 2);
  }
}
