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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.WindowFunctionOptimizationTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;

import java.util.List;

import static org.junit.Assert.*;

public class PropertyPlanningConfigurationIsolationTest {
  private State original;

  private static final class State {
    final boolean property, hash, ordered;
    final int buckets, dataNode, dop;

    State() {
      var c = IoTDBDescriptor.getInstance().getConfig();
      property = c.isEnablePropertyDrivenPlanning();
      hash = c.isEnableTableGroupByHashRepartition();
      ordered = c.isEnableOrderedParallelScan();
      buckets = c.getTableGroupByHashRepartitionPartitionCount();
      dataNode = c.getDataNodeId();
      dop = c.getDegreeOfParallelism();
    }

    void restore() {
      var c = IoTDBDescriptor.getInstance().getConfig();
      c.setEnablePropertyDrivenPlanning(property);
      c.setEnableTableGroupByHashRepartition(hash);
      c.setEnableOrderedParallelScan(ordered);
      c.setTableGroupByHashRepartitionPartitionCount(buckets);
      c.setDataNodeId(dataNode);
      c.setDegreeOfParallelism(dop);
    }

    List<Object> values() {
      return List.of(property, hash, ordered, buckets, dataNode, dop);
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

  private static void verifyRestoration(boolean enabled) {
    var c = IoTDBDescriptor.getInstance().getConfig();
    c.setEnablePropertyDrivenPlanning(enabled);
    c.setEnableTableGroupByHashRepartition(enabled);
    c.setEnableOrderedParallelScan(enabled);
    c.setTableGroupByHashRepartitionPartitionCount(5);
    c.setDataNodeId(17);
    c.setDegreeOfParallelism(3);
    State before = new State();
    Result result =
        new JUnitCore()
            .run(
                PropertyDrivenDistributionTest.class,
                SingleSourceGroupByHashRepartitionPlanningTest.class,
                GroupByHashRepartitionSourceShapeTest.class,
                WindowFunctionOptimizationTest.class,
                PlanPropertyMetadataCoverageTest.class);
    assertTrue(result.getFailures().toString(), result.wasSuccessful());
    assertEquals(79, result.getRunCount());
    assertEquals(before.values(), new State().values());
  }
}
