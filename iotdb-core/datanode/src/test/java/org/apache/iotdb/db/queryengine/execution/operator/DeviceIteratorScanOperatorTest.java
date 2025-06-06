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

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.DeviceIteratorScanOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions.getDefaultSeriesScanOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DeviceIteratorScanOperatorTest {

  private static final String DEVICE_ITERATOR_SCAN_OPERATOR_TEST =
      "root.DeviceIteratorScanOperatorTest";
  private static final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();

  private static final List<TsFileResource> seqResources = new ArrayList<>();
  private static final List<TsFileResource> unSeqResources = new ArrayList<>();

  private static final double DELTA = 0.000001;

  @BeforeClass
  public static void setUp() throws MetadataException, IOException, WriteProcessException {
    AlignedSeriesTestUtil.setUp(
        measurementSchemas, seqResources, unSeqResources, DEVICE_ITERATOR_SCAN_OPERATOR_TEST);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    AlignedSeriesTestUtil.tearDown(seqResources, unSeqResources);
  }

  @Test
  public void test1() {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "test-instance-notification");
    DeviceIteratorScanOperator operator = null;
    try {
      QueryId queryId = new QueryId("stub_query");
      FragmentInstanceId instanceId =
          new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
      FragmentInstanceContext fragmentInstanceContext =
          createFragmentInstanceContext(instanceId, stateMachine);
      DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
      PlanNodeId planNodeId = new PlanNodeId("1");
      driverContext.addOperatorContext(
          1, planNodeId, DeviceIteratorScanOperator.class.getSimpleName());

      List<DeviceEntry> deviceEntries =
          Arrays.asList(
              new AlignedDeviceEntry(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      DEVICE_ITERATOR_SCAN_OPERATOR_TEST + ".device0"),
                  new Binary[0]),
              new AlignedDeviceEntry(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      DEVICE_ITERATOR_SCAN_OPERATOR_TEST + ".device0"),
                  new Binary[0]),
              new AlignedDeviceEntry(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(
                      DEVICE_ITERATOR_SCAN_OPERATOR_TEST + ".device0"),
                  new Binary[0]));

      DeviceIteratorScanOperator.DeviceChildOperatorTreeGenerator generator =
          new DeviceIteratorScanOperator.DeviceChildOperatorTreeGenerator() {

            private Operator currentDeviceRootOperator;

            @Override
            public boolean keepOffsetAndLimitOperatorAfterDeviceIterator() {
              return true;
            }

            @Override
            public void generateCurrentDeviceOperatorTree(DeviceEntry deviceEntry) {
              AlignedFullPath alignedPath =
                  new AlignedFullPath(
                      deviceEntry.getDeviceID(),
                      measurementSchemas.stream()
                          .map(IMeasurementSchema::getMeasurementName)
                          .collect(Collectors.toList()),
                      measurementSchemas.stream()
                          .map(m -> (IMeasurementSchema) m)
                          .collect(Collectors.toList()));
              currentDeviceRootOperator =
                  new AlignedSeriesScanOperator(
                      driverContext.getOperatorContexts().get(0),
                      planNodeId,
                      alignedPath,
                      Ordering.ASC,
                      getDefaultSeriesScanOptions(alignedPath),
                      false,
                      null,
                      -1);
            }

            @Override
            public Operator getCurrentDeviceRootOperator() {
              return currentDeviceRootOperator;
            }

            @Override
            public List<Operator> getCurrentDeviceDataSourceOperators() {
              return Collections.singletonList(currentDeviceRootOperator);
            }

            @Override
            public Operator getCurrentDeviceStartCloseOperator() {
              return currentDeviceRootOperator;
            }
          };
      operator =
          new DeviceIteratorScanOperator(
              driverContext.getOperatorContexts().get(0), deviceEntries, generator);
      operator.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));

      int count = 0;
      while (operator.isBlocked().isDone() && operator.hasNext()) {
        if (count % 500 == 0) {
          count = 0;
        }
        TsBlock tsBlock = operator.next();
        if (tsBlock == null) {
          continue;
        }
        assertEquals(measurementSchemas.size(), tsBlock.getValueColumnCount());
        assertTrue(tsBlock.getColumn(0) instanceof BooleanColumn);
        assertTrue(tsBlock.getColumn(1) instanceof IntColumn);
        assertTrue(tsBlock.getColumn(2) instanceof LongColumn);
        assertTrue(tsBlock.getColumn(3) instanceof FloatColumn);
        assertTrue(tsBlock.getColumn(4) instanceof DoubleColumn);
        assertTrue(tsBlock.getColumn(5) instanceof BinaryColumn);

        for (int i = 0; i < tsBlock.getPositionCount(); i++, count++) {
          assertEquals(count, tsBlock.getTimeByIndex(i));
          int delta = 0;
          if ((long) count < 200) {
            delta = 20000;
          } else if ((long) count < 260
              || ((long) count >= 300 && (long) count < 380)
              || (long) count >= 400) {
            delta = 10000;
          }
          assertEquals((delta + (long) count) % 2 == 0, tsBlock.getColumn(0).getBoolean(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(1).getInt(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(2).getLong(i));
          assertEquals(delta + (long) count, tsBlock.getColumn(3).getFloat(i), DELTA);
          assertEquals(delta + (long) count, tsBlock.getColumn(4).getDouble(i), DELTA);
          assertEquals(
              String.valueOf(delta + (long) count), tsBlock.getColumn(5).getBinary(i).toString());
        }
      }
      assertEquals(500, count);

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    } finally {
      if (operator != null) {
        try {
          operator.close();
        } catch (Exception ignored) {
        }
      }
      instanceNotificationExecutor.shutdown();
    }
  }
}
