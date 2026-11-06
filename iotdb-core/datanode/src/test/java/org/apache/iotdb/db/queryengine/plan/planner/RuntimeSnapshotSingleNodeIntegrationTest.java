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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Single-node planning/execution-boundary fixture for runtime snapshot identity fields. */
public class RuntimeSnapshotSingleNodeIntegrationTest {

  @BeforeClass
  public static void setUp() {
    DeviceTableScanParallelPipelineTest.setUp();
  }

  @AfterClass
  public static void tearDown() {
    DeviceTableScanParallelPipelineTest.tearDown();
  }

  @Test
  public void testSingleNodeScanPublishesStructuredPlanMetadata() throws Exception {
    DeviceTableScanParallelPipelineTest fixtures = new DeviceTableScanParallelPipelineTest();
    LocalExecutionPlanContext context =
        fixtures.createLocalExecutionPlanContext("runtime_snapshot");
    context.setDegreeOfParallelism(4);
    org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode node =
        fixtures.initDeviceTableScanNode(4);
    node.setAllowParallelScan(true);

    Operator root =
        node.accept(new DataNodeTableOperatorGenerator(new TableMetadataImpl()), context);
    try {
      FragmentInstanceContext instanceContext = context.getInstanceContext();
      assertEquals(4, instanceContext.getPlannedDop());
      assertEquals("DEVICE", instanceContext.getSplitMode());
      assertTrue(instanceContext.getActualPipelineCount() > 0);
      assertTrue(context.getPipelineNumber() > 0);
      for (PipelineDriverFactory factory : context.getPipelineDriverFactories()) {
        assertTrue(factory.getDriverContext().getDriverTaskID().getPipelineId() >= 0);
        assertEquals(TableScanOperator.class, factory.getOperation().getClass());
      }
    } finally {
      root.close();
    }
  }
}
