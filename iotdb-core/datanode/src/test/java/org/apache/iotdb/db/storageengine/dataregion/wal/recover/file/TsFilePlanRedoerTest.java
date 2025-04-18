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
package org.apache.iotdb.db.storageengine.dataregion.wal.recover.file;

import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.memtable.IMemTable;
import org.apache.iotdb.db.storageengine.dataregion.memtable.ReadOnlyMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.TsFileUtilsForRecoverTest;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.TSRecord;
import org.apache.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.tsfile.write.record.datapoint.DoubleDataPoint;
import org.apache.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.tsfile.write.record.datapoint.StringDataPoint;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TsFilePlanRedoerTest {
  private static final String SG_NAME = "root.recover_sg";
  private static final IDeviceID DEVICE1_NAME =
      IDeviceID.Factory.DEFAULT_FACTORY.create(SG_NAME.concat(".d1"));
  private static final IDeviceID DEVICE2_NAME =
      IDeviceID.Factory.DEFAULT_FACTORY.create(SG_NAME.concat(".d2"));
  private static final IDeviceID DEVICE3_NAME =
      IDeviceID.Factory.DEFAULT_FACTORY.create(SG_NAME.concat(".d3"));
  private static final String FILE_NAME =
      TsFileUtilsForRecoverTest.getTestTsFilePath(SG_NAME, 0, 0, 1);
  private TsFileResource tsFileResource;
  private CompressionType compressionType;
  boolean prevIsAutoCreateSchemaEnabled;
  boolean prevIsEnablePartialInsert;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();

    // set recover config, avoid creating deleted time series when recovering wal
    prevIsAutoCreateSchemaEnabled =
        IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    //    IoTDBDescriptor.getInstance().getConfig().setAutoCreateSchemaEnabled(false);
    prevIsEnablePartialInsert = IoTDBDescriptor.getInstance().getConfig().isEnablePartialInsert();
    IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(true);
    compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
  }

  @After
  public void tearDown() throws Exception {
    if (tsFileResource != null) {
      tsFileResource.close();
    }
    File modsFile = ModificationFile.getExclusiveMods(new File(FILE_NAME));
    if (modsFile.exists()) {
      modsFile.delete();
    }
    EnvironmentUtils.cleanEnv();
    // reset config
    //    IoTDBDescriptor.getInstance()
    //        .getConfig()
    //        .setAutoCreateSchemaEnabled(prevIsAutoCreateSchemaEnabled);
    IoTDBDescriptor.getInstance().getConfig().setEnablePartialInsert(prevIsEnablePartialInsert);
  }

  @Test
  public void testRedoInsertRowPlan() throws Exception {
    // generate .tsfile and update resource in memory
    File file = new File(FILE_NAME);
    generateCompleteFile(file);
    tsFileResource = new TsFileResource(file);
    tsFileResource.updateStartTime(DEVICE1_NAME, 1);
    tsFileResource.updateEndTime(DEVICE1_NAME, 2);
    tsFileResource.updateStartTime(DEVICE2_NAME, 3);
    tsFileResource.updateEndTime(DEVICE2_NAME, 4);

    // generate InsertRowPlan
    long time = 5;
    TSDataType[] dataTypes = new TSDataType[] {TSDataType.FLOAT, TSDataType.DOUBLE};
    Object[] columns = new Object[] {1f, 1.0d};
    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId("0"),
            new PartialPath(DEVICE2_NAME),
            false,
            new String[] {"s1", "s2"},
            dataTypes,
            time,
            columns,
            false);
    insertRowNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.FLOAT),
          new MeasurementSchema("s2", TSDataType.DOUBLE),
        });

    // redo InsertTabletPlan, vsg processor is used to test IdTable, don't test IdTable here
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource);
    planRedoer.redoInsert(insertRowNode);

    // check data in memTable
    IMemTable recoveryMemTable = planRedoer.getRecoveryMemTable();
    // check d2.s1
    NonAlignedFullPath fullPath =
        new NonAlignedFullPath(
            DEVICE2_NAME, new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    ReadOnlyMemChunk memChunk =
        recoveryMemTable.query(new QueryContext(), fullPath, Long.MIN_VALUE, null, null);
    IPointReader iterator = memChunk.getPointReader();
    time = 5;
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      assertEquals(time, timeValuePair.getTimestamp());
      assertEquals(1, timeValuePair.getValue().getFloat(), 0.0001);
      ++time;
    }
    assertEquals(6, time);
    // check d2.s2
    fullPath =
        new NonAlignedFullPath(
            DEVICE2_NAME, new MeasurementSchema("s2", TSDataType.DOUBLE, TSEncoding.RLE));
    memChunk = recoveryMemTable.query(new QueryContext(), fullPath, Long.MIN_VALUE, null, null);
    iterator = memChunk.getPointReader();
    time = 5;
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      assertEquals(time, timeValuePair.getTimestamp());
      assertEquals(1, timeValuePair.getValue().getDouble(), 0.0001);
      ++time;
    }
    assertEquals(6, time);
  }

  @Test
  public void testRedoInsertAlignedRowPlan() throws Exception {
    // generate .tsfile and update resource in memory
    File file = new File(FILE_NAME);
    generateCompleteFile(file);
    tsFileResource = new TsFileResource(file);
    tsFileResource.updateStartTime(DEVICE3_NAME, 5);
    tsFileResource.updateEndTime(DEVICE3_NAME, 5);

    // generate InsertRowPlan
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.INT32, TSDataType.INT64, TSDataType.BOOLEAN, TSDataType.FLOAT, TSDataType.TEXT
        };
    Object[] columns =
        new Object[] {1, 1L, true, 1.0f, new Binary("1", TSFileConfig.STRING_CHARSET)};

    InsertRowNode insertRowNode1 =
        new InsertRowNode(
            new PlanNodeId("0"),
            new PartialPath(DEVICE3_NAME),
            true,
            new String[] {"s1", "s2", "s3", "s4", "s5"},
            dataTypes,
            5,
            columns,
            false);
    insertRowNode1.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
          new MeasurementSchema("s2", TSDataType.INT64),
          new MeasurementSchema("s3", TSDataType.BOOLEAN),
          new MeasurementSchema("s4", TSDataType.FLOAT),
          new MeasurementSchema("s5", TSDataType.TEXT),
        });

    InsertRowNode insertRowNode2 =
        new InsertRowNode(
            new PlanNodeId("0"),
            new PartialPath(DEVICE3_NAME),
            true,
            new String[] {"s1", "s2", "s3", "s4", "s5"},
            dataTypes,
            6,
            columns,
            false);
    insertRowNode2.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
          new MeasurementSchema("s2", TSDataType.INT64),
          new MeasurementSchema("s3", TSDataType.BOOLEAN),
          new MeasurementSchema("s4", TSDataType.FLOAT),
          new MeasurementSchema("s5", TSDataType.TEXT),
        });

    // redo InsertTabletPlan, vsg processor is used to test IdTable, don't test IdTable here
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource);
    planRedoer.redoInsert(insertRowNode1);
    planRedoer.redoInsert(insertRowNode2);

    // check data in memTable
    IMemTable recoveryMemTable = planRedoer.getRecoveryMemTable();
    // check d3
    AlignedFullPath fullPath =
        new AlignedFullPath(
            DEVICE3_NAME,
            Arrays.asList("s1", "s2", "s3", "s4", "s5"),
            Arrays.asList(
                new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE),
                new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE),
                new MeasurementSchema("s3", TSDataType.BOOLEAN, TSEncoding.RLE),
                new MeasurementSchema("s4", TSDataType.FLOAT, TSEncoding.RLE),
                new MeasurementSchema("s5", TSDataType.TEXT, TSEncoding.PLAIN)));
    ReadOnlyMemChunk memChunk =
        recoveryMemTable.query(new QueryContext(), fullPath, Long.MIN_VALUE, null, null);
    IPointReader iterator = memChunk.getPointReader();
    int time = 6;
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      assertEquals(time, timeValuePair.getTimestamp());
      assertEquals(1, timeValuePair.getValue().getVector()[0].getInt());
      assertEquals(1L, timeValuePair.getValue().getVector()[1].getLong());
      assertTrue(timeValuePair.getValue().getVector()[2].getBoolean());
      assertEquals(1, timeValuePair.getValue().getVector()[3].getFloat(), 0.00001);
      assertEquals(BytesUtils.valueOf("1"), timeValuePair.getValue().getVector()[4].getBinary());
      ++time;
    }
    assertEquals(7, time);
  }

  @Test
  public void testRedoInsertTabletPlan() throws Exception {
    // generate .tsfile and update resource in memory
    File file = new File(FILE_NAME);
    generateCompleteFile(file);
    tsFileResource = new TsFileResource(file);
    tsFileResource.updateStartTime(DEVICE1_NAME, 1);
    tsFileResource.updateEndTime(DEVICE1_NAME, 2);
    tsFileResource.updateStartTime(DEVICE2_NAME, 3);
    tsFileResource.updateEndTime(DEVICE2_NAME, 4);

    // generate InsertTabletPlan
    long[] times = {5, 6, 7, 8};
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());

    Object[] columns = new Object[2];
    columns[0] = new int[times.length];
    columns[1] = new long[times.length];

    for (int r = 0; r < times.length; r++) {
      ((int[]) columns[0])[r] = 100;
      ((long[]) columns[1])[r] = 10000;
    }

    BitMap[] bitMaps = new BitMap[dataTypes.size()];
    for (int i = 0; i < dataTypes.size(); i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(times.length);
      }
      // mark value of time=8 as null
      bitMaps[i].mark(3);
    }

    InsertTabletNode insertTabletNode =
        new InsertTabletNode(
            new PlanNodeId("0"),
            new PartialPath(DEVICE1_NAME),
            false,
            new String[] {"s1", "s2"},
            new TSDataType[] {TSDataType.INT32, TSDataType.INT64},
            null,
            times,
            bitMaps,
            columns,
            times.length);
    insertTabletNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
          new MeasurementSchema("s2", TSDataType.INT64),
        });

    // redo InsertTabletPlan, vsg processor is used to test IdTable, don't test IdTable here
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource);
    planRedoer.redoInsert(insertTabletNode);

    // check data in memTable
    IMemTable recoveryMemTable = planRedoer.getRecoveryMemTable();
    // check d1.s1
    NonAlignedFullPath fullPath =
        new NonAlignedFullPath(
            DEVICE1_NAME, new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE));
    ReadOnlyMemChunk memChunk =
        recoveryMemTable.query(new QueryContext(), fullPath, Long.MIN_VALUE, null, null);
    IPointReader iterator = memChunk.getPointReader();
    int time = 5;
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      assertEquals(time, timeValuePair.getTimestamp());
      assertEquals(100, timeValuePair.getValue().getInt());
      ++time;
    }
    assertEquals(8, time);
    // check d1.s2
    fullPath =
        new NonAlignedFullPath(
            DEVICE1_NAME, new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    memChunk = recoveryMemTable.query(new QueryContext(), fullPath, Long.MIN_VALUE, null, null);
    iterator = memChunk.getPointReader();
    time = 5;
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      assertEquals(time, timeValuePair.getTimestamp());
      assertEquals(10000, timeValuePair.getValue().getLong());
      ++time;
    }
    assertEquals(8, time);
  }

  @Test
  public void testRedoInsertAlignedTabletPlan() throws Exception {
    // generate .tsfile and update resource in memory
    File file = new File(FILE_NAME);
    generateCompleteFile(file);
    tsFileResource = new TsFileResource(file);
    tsFileResource.updateStartTime(DEVICE3_NAME, 5);
    tsFileResource.updateStartTime(DEVICE3_NAME, 5);

    // generate InsertTabletPlan
    long[] times = {6, 7, 8, 9};
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());
    dataTypes.add(TSDataType.BOOLEAN.ordinal());
    dataTypes.add(TSDataType.FLOAT.ordinal());
    dataTypes.add(TSDataType.TEXT.ordinal());

    Object[] columns = new Object[5];
    columns[0] = new int[times.length];
    columns[1] = new long[times.length];
    columns[2] = new boolean[times.length];
    columns[3] = new float[times.length];
    columns[4] = new Binary[times.length];

    for (int r = 0; r < times.length; r++) {
      ((int[]) columns[0])[r] = (r + 1) * 100;
      ((long[]) columns[1])[r] = (r + 1) * 100;
      ((boolean[]) columns[2])[r] = true;
      ((float[]) columns[3])[r] = (r + 1) * 100;
      ((Binary[]) columns[4])[r] = BytesUtils.valueOf((r + 1) * 100 + "");
    }

    BitMap[] bitMaps = new BitMap[dataTypes.size()];
    for (int i = 0; i < dataTypes.size(); i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(times.length);
      }
      // mark value of time=9 as null
      bitMaps[i].mark(3);
    }

    InsertTabletNode insertTabletNode =
        new InsertTabletNode(
            new PlanNodeId("0"),
            new PartialPath(DEVICE3_NAME),
            true,
            new String[] {"s1", "s2", "s3", "s4", "s5"},
            new TSDataType[] {
              TSDataType.INT32,
              TSDataType.INT64,
              TSDataType.BOOLEAN,
              TSDataType.FLOAT,
              TSDataType.TEXT
            },
            null,
            times,
            bitMaps,
            columns,
            times.length);
    insertTabletNode.setMeasurementSchemas(
        new MeasurementSchema[] {
          new MeasurementSchema("s1", TSDataType.INT32),
          new MeasurementSchema("s2", TSDataType.INT64),
          new MeasurementSchema("s3", TSDataType.BOOLEAN),
          new MeasurementSchema("s4", TSDataType.FLOAT),
          new MeasurementSchema("s5", TSDataType.TEXT),
        });

    // redo InsertTabletPlan, vsg processor is used to test IdTable, don't test IdTable here
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource);
    planRedoer.redoInsert(insertTabletNode);

    // check data in memTable
    IMemTable recoveryMemTable = planRedoer.getRecoveryMemTable();
    // check d3
    AlignedFullPath fullPath =
        new AlignedFullPath(
            DEVICE3_NAME,
            Arrays.asList("s1", "s2", "s3", "s4", "s5"),
            Arrays.asList(
                new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE),
                new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE),
                new MeasurementSchema("s3", TSDataType.BOOLEAN, TSEncoding.RLE),
                new MeasurementSchema("s4", TSDataType.FLOAT, TSEncoding.RLE),
                new MeasurementSchema("s5", TSDataType.TEXT, TSEncoding.PLAIN)));
    ReadOnlyMemChunk memChunk =
        recoveryMemTable.query(new QueryContext(), fullPath, Long.MIN_VALUE, null, null);
    IPointReader iterator = memChunk.getPointReader();
    int time = 6;
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      assertEquals(time, timeValuePair.getTimestamp());
      assertEquals((time - 5) * 100, timeValuePair.getValue().getVector()[0].getInt());
      assertEquals((time - 5) * 100L, timeValuePair.getValue().getVector()[1].getLong());
      assertEquals(true, timeValuePair.getValue().getVector()[2].getBoolean());
      assertEquals((time - 5) * 100, timeValuePair.getValue().getVector()[3].getFloat(), 0.00001);
      assertEquals(
          BytesUtils.valueOf((time - 5) * 100 + ""),
          timeValuePair.getValue().getVector()[4].getBinary());
      ++time;
    }
    assertEquals(9, time);
  }

  @Test
  public void testRedoOverLapPlanIntoSeqFile() throws Exception {
    // generate .tsfile and update resource in memory
    File file = new File(FILE_NAME);
    generateCompleteFile(file);
    tsFileResource = new TsFileResource(file);
    tsFileResource.updateStartTime(DEVICE1_NAME, 1);
    tsFileResource.updateEndTime(DEVICE1_NAME, 2);
    tsFileResource.updateStartTime(DEVICE2_NAME, 3);
    tsFileResource.updateEndTime(DEVICE2_NAME, 4);

    // generate InsertTabletPlan, time=3 and time=4 are overlap
    long[] times = {1, 2};
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());

    Object[] columns = new Object[2];
    columns[0] = new int[times.length];
    columns[1] = new long[times.length];

    for (int r = 0; r < times.length; r++) {
      ((int[]) columns[0])[r] = 100;
      ((long[]) columns[1])[r] = 10000;
    }

    InsertTabletNode insertTabletNode =
        new InsertTabletNode(
            new PlanNodeId("0"),
            new PartialPath(DEVICE1_NAME),
            false,
            new String[] {"s1", "s2"},
            new TSDataType[] {TSDataType.INT32, TSDataType.INT64},
            new MeasurementSchema[] {
              new MeasurementSchema("s1", TSDataType.INT32),
              new MeasurementSchema("s2", TSDataType.INT64),
            },
            times,
            null,
            columns,
            times.length);

    // redo InsertTabletPlan, vsg processor is used to test IdTable, don't test IdTable here
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource);
    planRedoer.redoInsert(insertTabletNode);

    // check data in memTable
    IMemTable recoveryMemTable = planRedoer.getRecoveryMemTable();
    assertTrue(recoveryMemTable.isEmpty());
  }

  @Test
  public void testRedoOverLapPlanIntoUnseqFile() throws Exception {
    // generate .tsfile and update resource in memory
    File file = new File(FILE_NAME);
    generateCompleteFile(file);
    tsFileResource = new TsFileResource(file);
    tsFileResource.updateStartTime(DEVICE1_NAME, 1);
    tsFileResource.updateEndTime(DEVICE1_NAME, 2);
    tsFileResource.updateStartTime(DEVICE2_NAME, 3);
    tsFileResource.updateEndTime(DEVICE2_NAME, 4);

    // generate InsertTabletPlan, time=3 and time=4 are overlap
    long[] times = {1, 2};
    List<Integer> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.INT32.ordinal());
    dataTypes.add(TSDataType.INT64.ordinal());

    Object[] columns = new Object[2];
    columns[0] = new int[times.length];
    columns[1] = new long[times.length];

    for (int r = 0; r < times.length; r++) {
      ((int[]) columns[0])[r] = 100;
      ((long[]) columns[1])[r] = 10000;
    }

    InsertTabletNode insertTabletNode =
        new InsertTabletNode(
            new PlanNodeId("0"),
            new PartialPath(DEVICE1_NAME),
            false,
            new String[] {"s1", "s2"},
            new TSDataType[] {TSDataType.INT32, TSDataType.INT64},
            new MeasurementSchema[] {
              new MeasurementSchema("s1", TSDataType.INT32),
              new MeasurementSchema("s2", TSDataType.INT64),
            },
            times,
            null,
            columns,
            times.length);

    // redo InsertTabletPlan, vsg processor is used to test IdTable, don't test IdTable here
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource);
    planRedoer.redoInsert(insertTabletNode);

    // check data in memTable
    IMemTable recoveryMemTable = planRedoer.getRecoveryMemTable();
    // check d1.s1
    NonAlignedFullPath fullPath =
        new NonAlignedFullPath(
            DEVICE1_NAME, new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE));
    ReadOnlyMemChunk memChunk =
        recoveryMemTable.query(new QueryContext(), fullPath, Long.MIN_VALUE, null, null);
    assertTrue(memChunk == null || memChunk.isEmpty());
    // check d1.s2
    fullPath =
        new NonAlignedFullPath(
            DEVICE1_NAME, new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    memChunk = recoveryMemTable.query(new QueryContext(), fullPath, Long.MIN_VALUE, null, null);
    assertTrue(memChunk == null || memChunk.isEmpty());
  }

  @Test
  public void testRedoDeleteDataNode() throws Exception {
    // generate .tsfile and update resource in memory
    File file = new File(FILE_NAME);
    generateCompleteFile(file);
    tsFileResource = new TsFileResource(file);
    tsFileResource.updateStartTime(DEVICE1_NAME, 1);
    tsFileResource.updateEndTime(DEVICE1_NAME, 2);
    tsFileResource.updateStartTime(DEVICE2_NAME, 3);
    tsFileResource.updateEndTime(DEVICE2_NAME, 4);

    // generate DeleteDataNode
    DeleteDataNode deleteDataNode =
        new DeleteDataNode(
            new PlanNodeId(""),
            Collections.singletonList(new MeasurementPath(DEVICE1_NAME, "**")),
            Long.MIN_VALUE,
            Long.MAX_VALUE);

    // redo DeleteDataNode, vsg processor is used to test IdTable, don't test IdTable here
    File modsFile = ModificationFile.getExclusiveMods(new File(FILE_NAME));
    assertFalse(modsFile.exists());
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource);
    planRedoer.redoDelete(deleteDataNode);
    assertTrue(modsFile.exists());
  }

  @Test
  public void testRedoRelationalInsertTablet()
      throws IOException,
          WriteProcessException,
          org.apache.iotdb.db.exception.WriteProcessException {
    // generate .tsfile and update resource in memory
    File file = new File(FILE_NAME);
    generateCompleteFile(file);
    tsFileResource = new TsFileResource(file);
    IDeviceID existDevice = IDeviceID.Factory.DEFAULT_FACTORY.create("table1.tag1");
    tsFileResource.updateStartTime(existDevice, 0);
    tsFileResource.updateEndTime(existDevice, 1000);

    long[] times = new long[] {110L, 111L, 112L, 113L, 114L, 115L, 116L};
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.STRING);
    dataTypes.add(TSDataType.INT32);
    dataTypes.add(TSDataType.BOOLEAN);

    Object[] columns = new Object[3];
    columns[0] = new Binary[times.length];
    columns[1] = new int[times.length];
    columns[2] = new boolean[times.length];

    for (int r = 0; r < times.length; r++) {
      ((Binary[]) columns[0])[r] = new Binary("tag" + (int) (r / 3), TSFileConfig.STRING_CHARSET);
      ((int[]) columns[1])[r] = 100 + r;
      ((boolean[]) columns[2])[r] = (r % 2 == 0);
    }

    BitMap[] bitMaps = new BitMap[dataTypes.size()];
    for (int i = 0; i < dataTypes.size(); i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(times.length);
      }
    }

    RelationalInsertTabletNode relationalInsertTabletNode =
        new RelationalInsertTabletNode(
            new PlanNodeId(""),
            new PartialPath("table1", false),
            false,
            new String[] {
              "t", "s1", "s2",
            },
            dataTypes.toArray(new TSDataType[3]),
            new MeasurementSchema[] {
              new MeasurementSchema("t", TSDataType.STRING),
              new MeasurementSchema("s1", TSDataType.INT32),
              new MeasurementSchema("s2", TSDataType.BOOLEAN),
            },
            times,
            bitMaps,
            columns,
            times.length,
            new TsTableColumnCategory[] {
              TsTableColumnCategory.TAG, TsTableColumnCategory.FIELD, TsTableColumnCategory.FIELD,
            });

    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource);
    planRedoer.redoInsert(relationalInsertTabletNode);
    IMemTable recoveryMemTable = planRedoer.getRecoveryMemTable();
    Assert.assertEquals(2, recoveryMemTable.getMemTableMap().size());
    Assert.assertFalse(recoveryMemTable.getMemTableMap().containsKey(existDevice));
  }

  @Test
  public void testRedoAlignedInsertAfterDeleteTimeseries() throws Exception {
    // generate .tsfile and update resource in memory
    File file = new File(FILE_NAME);
    generateCompleteFile(file);
    tsFileResource = new TsFileResource(file);
    tsFileResource.updateStartTime(DEVICE3_NAME, 5);
    tsFileResource.updateStartTime(DEVICE3_NAME, 5);

    // generate InsertTabletPlan
    long[] times = {6, 7, 8, 9};
    List<Integer> dataTypes =
        Arrays.asList(
            TSDataType.INT32.ordinal(),
            TSDataType.INT64.ordinal(),
            TSDataType.BOOLEAN.ordinal(),
            TSDataType.FLOAT.ordinal(),
            TSDataType.TEXT.ordinal());
    Object[] columns =
        new Object[] {
          new int[times.length],
          new long[times.length],
          new boolean[times.length],
          new float[times.length],
          new Binary[times.length]
        };
    for (int r = 0; r < times.length; r++) {
      ((int[]) columns[0])[r] = (r + 1) * 100;
      ((long[]) columns[1])[r] = (r + 1) * 100;
      ((boolean[]) columns[2])[r] = true;
      ((float[]) columns[3])[r] = (r + 1) * 100;
      ((Binary[]) columns[4])[r] = BytesUtils.valueOf((r + 1) * 100 + "");
    }
    BitMap[] bitMaps = new BitMap[dataTypes.size()];
    for (int i = 0; i < dataTypes.size(); i++) {
      if (bitMaps[i] == null) {
        bitMaps[i] = new BitMap(times.length);
      }
      // mark value of time=9 as null
      bitMaps[i].mark(3);
    }
    MeasurementSchema[] schemas =
        new MeasurementSchema[] {
          null,
          new MeasurementSchema("s2", TSDataType.INT64),
          new MeasurementSchema("s3", TSDataType.BOOLEAN),
          new MeasurementSchema("s4", TSDataType.FLOAT),
          null
        };
    InsertTabletNode insertTabletNode =
        new InsertTabletNode(
            new PlanNodeId(""),
            new PartialPath(DEVICE3_NAME),
            true,
            new String[] {null, "s2", "s3", "s4", null},
            new TSDataType[] {
              TSDataType.INT32,
              TSDataType.INT64,
              TSDataType.BOOLEAN,
              TSDataType.FLOAT,
              TSDataType.TEXT
            },
            schemas,
            times,
            bitMaps,
            columns,
            times.length);
    // redo InsertTabletPlan, data region is used to test IdTable, don't test IdTable here
    TsFilePlanRedoer planRedoer = new TsFilePlanRedoer(tsFileResource);
    insertTabletNode.setMeasurementSchemas(schemas);
    planRedoer.redoInsert(insertTabletNode);

    // generate InsertRowPlan
    int time = 9;
    TSDataType[] dataTypes2 =
        new TSDataType[] {
          TSDataType.INT32, TSDataType.INT64, TSDataType.BOOLEAN, TSDataType.FLOAT, TSDataType.TEXT
        };
    Object[] columns2 =
        new Object[] {400, 400L, true, 400.0f, new Binary("400", TSFileConfig.STRING_CHARSET)};
    // redo InsertTabletPlan, data region is used to test IdTable, don't test IdTable here
    InsertRowNode insertRowNode =
        new InsertRowNode(
            new PlanNodeId(""),
            new PartialPath(DEVICE3_NAME),
            true,
            new String[] {null, "s2", "s3", "s4", null},
            dataTypes2,
            time,
            columns2,
            false);
    insertRowNode.setMeasurementSchemas(schemas);
    planRedoer.redoInsert(insertRowNode);

    // check data in memTable
    IMemTable recoveryMemTable = planRedoer.getRecoveryMemTable();
    // check d3
    AlignedFullPath fullPath =
        new AlignedFullPath(
            DEVICE3_NAME,
            Arrays.asList("s1", "s2", "s3", "s4", "s5"),
            Arrays.asList(
                new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE),
                new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE),
                new MeasurementSchema("s3", TSDataType.BOOLEAN, TSEncoding.RLE),
                new MeasurementSchema("s4", TSDataType.FLOAT, TSEncoding.RLE),
                new MeasurementSchema("s5", TSDataType.TEXT, TSEncoding.PLAIN)));
    ReadOnlyMemChunk memChunk =
        recoveryMemTable.query(new QueryContext(), fullPath, Long.MIN_VALUE, null, null);
    IPointReader iterator = memChunk.getPointReader();
    time = 6;
    while (iterator.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = iterator.nextTimeValuePair();
      assertEquals(time, timeValuePair.getTimestamp());
      assertEquals(null, timeValuePair.getValue().getVector()[0]);
      assertEquals((time - 5) * 100L, timeValuePair.getValue().getVector()[1].getLong());
      assertEquals(true, timeValuePair.getValue().getVector()[2].getBoolean());
      assertEquals((time - 5) * 100, timeValuePair.getValue().getVector()[3].getFloat(), 0.00001);
      assertEquals(null, timeValuePair.getValue().getVector()[4]);
      time++;
    }
    assertEquals(10, time);
  }

  private void generateCompleteFile(File tsFile) throws IOException, WriteProcessException {
    try (TsFileWriter writer = new TsFileWriter(tsFile)) {
      writer.registerTimeseries(
          new Path(DEVICE1_NAME), new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE));
      writer.registerTimeseries(
          new Path(DEVICE1_NAME), new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
      writer.registerTimeseries(
          new Path(DEVICE2_NAME), new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
      writer.registerTimeseries(
          new Path(DEVICE2_NAME), new MeasurementSchema("s2", TSDataType.DOUBLE, TSEncoding.RLE));
      writer.registerAlignedTimeseries(
          new Path(DEVICE3_NAME),
          Arrays.asList(
              new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE),
              new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE),
              new MeasurementSchema("s3", TSDataType.BOOLEAN, TSEncoding.RLE),
              new MeasurementSchema("s4", TSDataType.FLOAT, TSEncoding.RLE),
              new MeasurementSchema("s5", TSDataType.TEXT, TSEncoding.PLAIN)));
      writer.writeRecord(
          new TSRecord(DEVICE1_NAME, 1)
              .addTuple(new IntDataPoint("s1", 1))
              .addTuple(new LongDataPoint("s2", 1)));
      writer.writeRecord(
          new TSRecord(DEVICE1_NAME, 2)
              .addTuple(new IntDataPoint("s1", 2))
              .addTuple(new LongDataPoint("s2", 2)));
      writer.writeRecord(
          new TSRecord(DEVICE2_NAME, 3)
              .addTuple(new FloatDataPoint("s1", 3))
              .addTuple(new DoubleDataPoint("s2", 3)));
      writer.writeRecord(
          new TSRecord(DEVICE2_NAME, 4)
              .addTuple(new FloatDataPoint("s1", 4))
              .addTuple(new DoubleDataPoint("s2", 4)));
      writer.writeRecord(
          new TSRecord(DEVICE3_NAME, 5)
              .addTuple(new IntDataPoint("s1", 5))
              .addTuple(new LongDataPoint("s2", 5))
              .addTuple(new BooleanDataPoint("s3", true))
              .addTuple(new FloatDataPoint("s4", 5))
              .addTuple(new StringDataPoint("s5", BytesUtils.valueOf("5"))));
    }
  }
}
