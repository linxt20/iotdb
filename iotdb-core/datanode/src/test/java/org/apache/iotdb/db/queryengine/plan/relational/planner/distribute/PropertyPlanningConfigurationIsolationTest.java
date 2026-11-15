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
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.partition.ExecutorType;
import org.apache.iotdb.commons.partition.QueryExecutor;
import org.apache.iotdb.commons.partition.StorageExecutor;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.relational.planner.WindowFunctionOptimizationTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class PropertyPlanningConfigurationIsolationTest {
  private State original;

  private static final class State {
    final boolean property, hash, ordered, runtime;
    final int buckets, dataNode, dop;

    State() {
      var c = IoTDBDescriptor.getInstance().getConfig();
      property = c.isEnablePropertyDrivenPlanning();
      hash = c.isEnableTableGroupByHashRepartition();
      ordered = c.isEnableOrderedParallelScan();
      runtime = c.isEnableRuntimeScanParallelism();
      buckets = c.getTableGroupByHashRepartitionPartitionCount();
      dataNode = c.getDataNodeId();
      dop = c.getDegreeOfParallelism();
    }

    void restore() {
      var c = IoTDBDescriptor.getInstance().getConfig();
      c.setEnablePropertyDrivenPlanning(property);
      c.setEnableTableGroupByHashRepartition(hash);
      c.setEnableOrderedParallelScan(ordered);
      c.setEnableRuntimeScanParallelism(runtime);
      c.setTableGroupByHashRepartitionPartitionCount(buckets);
      c.setDataNodeId(dataNode);
      c.setDegreeOfParallelism(dop);
    }

    List<Object> values() {
      return List.of(property, hash, ordered, runtime, buckets, dataNode, dop);
    }
  }

  @Before
  public void saveConfiguration() {
    original = new State();
  }

  @After
  public void restoreConfiguration() {
    original.restore();
  }

  @Test
  public void initiallyEnabledConfigurationIsRestored() {
    verifyRestoration(true);
  }

  @Test
  public void initiallyDisabledConfigurationIsRestored() {
    verifyRestoration(false);
  }

  @Test
  public void schemaMergeGraphSelectsLegacyProtocolForEveryFragment() {
    FragmentInstance root = protocolFragment(0, new QueryExecutor(new TDataNodeLocation()));
    FragmentInstance firstSchema =
        protocolFragment(1, regionExecutor(TConsensusGroupType.SchemaRegion));
    FragmentInstance secondSchema =
        protocolFragment(2, regionExecutor(TConsensusGroupType.SchemaRegion));
    List<FragmentInstance> graph = Arrays.asList(firstSchema, root, secondSchema);
    graph.forEach(instance -> instance.setRuntimeExecutionEnabled(true));

    TableDistributedPlanner.configureRuntimeExecution(graph, true);

    graph.forEach(instance -> assertFalse(instance.isRuntimeExecutionEnabled()));
  }

  @Test
  public void dataMergeGraphSelectsRuntimeProtocolForEveryFragment() {
    FragmentInstance root = protocolFragment(0, new QueryExecutor(new TDataNodeLocation()));
    FragmentInstance firstData =
        protocolFragment(1, regionExecutor(TConsensusGroupType.DataRegion));
    FragmentInstance secondData =
        protocolFragment(2, regionExecutor(TConsensusGroupType.DataRegion));
    List<FragmentInstance> graph = Arrays.asList(firstData, root, secondData);

    TableDistributedPlanner.configureRuntimeExecution(graph, true);

    graph.forEach(instance -> assertTrue(instance.isRuntimeExecutionEnabled()));
  }

  @Test
  public void runtimeDisabledResetsEveryFragmentToLegacyProtocol() {
    List<FragmentInstance> graph =
        Arrays.asList(
            protocolFragment(0, new QueryExecutor(new TDataNodeLocation())),
            protocolFragment(1, regionExecutor(TConsensusGroupType.DataRegion)));
    graph.forEach(instance -> instance.setRuntimeExecutionEnabled(true));

    TableDistributedPlanner.configureRuntimeExecution(graph, false);

    graph.forEach(instance -> assertFalse(instance.isRuntimeExecutionEnabled()));
  }

  @Test
  public void unboundExecutorPreventsPartialRuntimeProtocolSelection() {
    FragmentInstance root = protocolFragment(0, new QueryExecutor(new TDataNodeLocation()));
    FragmentInstance unbound = protocolFragment(1, null);
    TableDistributedPlanner.configureRuntimeExecution(Arrays.asList(root, unbound), true);
    assertFalse(root.isRuntimeExecutionEnabled());
    assertFalse(unbound.isRuntimeExecutionEnabled());
  }

  @Test
  public void queryExecutorOnlyGraphCanSelectRuntimeProtocol() {
    FragmentInstance root = protocolFragment(0, new QueryExecutor(new TDataNodeLocation()));
    TableDistributedPlanner.configureRuntimeExecution(Collections.singletonList(root), true);
    assertTrue(root.isRuntimeExecutionEnabled());
  }

  private static StorageExecutor regionExecutor(TConsensusGroupType type) {
    return new StorageExecutor(
        new TRegionReplicaSet(
            new TConsensusGroupId(type, 1), Collections.singletonList(new TDataNodeLocation())));
  }

  private static FragmentInstance protocolFragment(int index, ExecutorType executor) {
    PlanFragmentId id = new PlanFragmentId("protocol_graph", index);
    FragmentInstance instance =
        new FragmentInstance(
            new PlanFragment(id, null),
            id.genFragmentInstanceId(),
            null,
            QueryType.READ,
            1000,
            null,
            false,
            false);
    instance.setExecutorAndHost(executor);
    return instance;
  }

  private static void verifyRestoration(boolean enabled) {
    var c = IoTDBDescriptor.getInstance().getConfig();
    c.setEnablePropertyDrivenPlanning(enabled);
    c.setEnableTableGroupByHashRepartition(enabled);
    c.setEnableOrderedParallelScan(enabled);
    c.setTableGroupByHashRepartitionPartitionCount(5);
    c.setDataNodeId(17);
    c.setDegreeOfParallelism(3);
    State before = new State();
    Class<?>[] suite = {
      PropertyDrivenDistributionTest.class,
      SingleSourceGroupByHashRepartitionPlanningTest.class,
      GroupByHashRepartitionSourceShapeTest.class,
      WindowFunctionOptimizationTest.class,
      PlanPropertyMetadataCoverageTest.class
    };
    int expectedTests = Request.classes(suite).getRunner().testCount();
    assertTrue("The configuration-isolation suite must contain tests", expectedTests > 0);
    Result result = new JUnitCore().run(suite);
    assertTrue(result.getFailures().toString(), result.wasSuccessful());
    // Bind to the actual parameterized suite so additions keep the completeness check current.
    assertEquals(0, result.getIgnoreCount());
    assertEquals(expectedTests, result.getRunCount());
    assertEquals(before.values(), new State().values());
  }
}
