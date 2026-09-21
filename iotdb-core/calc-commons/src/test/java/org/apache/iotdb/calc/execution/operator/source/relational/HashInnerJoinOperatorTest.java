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

package org.apache.iotdb.calc.execution.operator.source.relational;

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.utils.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.calc.plan.planner.CommonOperatorUtils.TIME_COLUMN_TEMPLATE;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests the local primitive only; planner selection remains deliberately disabled. */
public class HashInnerJoinOperatorTest {

  @Test
  public void testDuplicateKeysProduceCartesianMatchesAndNullKeysDoNotMatch() throws Exception {
    HashInnerJoinOperator operator =
        newHashJoin(
            blocks(rows(new Integer[][] {{1, 10}, {1, 11}, {null, 12}, {3, 13}})),
            blocks(rows(new Integer[][] {{1, 100}, {1, 101}, {null, 102}})));

    List<String> actual = drain(operator);

    assertEquals(
        Arrays.asList("1,10,1,100", "1,10,1,101", "1,11,1,100", "1,11,1,101"), actual);
  }

  @Test
  public void testEmptyBuildOrProbeProducesNoRows() throws Exception {
    HashInnerJoinOperator emptyBuild =
        newHashJoin(blocks(rows(new Integer[][] {{1, 10}})), Collections.emptyList());
    HashInnerJoinOperator emptyProbe =
        newHashJoin(Collections.emptyList(), blocks(rows(new Integer[][] {{1, 100}})));

    assertTrue(drain(emptyBuild).isEmpty());
    assertTrue(drain(emptyProbe).isEmpty());
    assertTrue(emptyBuild.isFinished());
    assertTrue(emptyProbe.isFinished());
  }

  @Test
  public void testBuildSideMayArriveInMultipleBlocks() throws Exception {
    HashInnerJoinOperator operator =
        newHashJoin(
            blocks(rows(new Integer[][] {{2, 20}})),
            blocks(rows(new Integer[][] {{1, 100}}), rows(new Integer[][] {{2, 200}})));

    assertEquals(Collections.singletonList("2,20,2,200"), drain(operator));
  }

  private static HashInnerJoinOperator newHashJoin(List<TsBlock> probe, List<TsBlock> build) {
    TestOperatorContext context = new TestOperatorContext();
    return new HashInnerJoinOperator(
        context,
        new ListSourceOperator(context, probe),
        new int[] {0},
        new int[] {0, 1},
        new ListSourceOperator(context, build),
        new int[] {0},
        new int[] {0, 1},
        Collections.singletonList(INT32),
        Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32, TSDataType.INT32));
  }

  private static List<String> drain(HashInnerJoinOperator operator) throws Exception {
    List<String> rows = new ArrayList<>();
    while (operator.hasNext()) {
      TsBlock block = operator.next();
      if (block == null) {
        continue;
      }
      for (int position = 0; position < block.getPositionCount(); position++) {
        rows.add(
            value(block, 0, position)
                + ","
                + value(block, 1, position)
                + ","
                + value(block, 2, position)
                + ","
                + value(block, 3, position));
      }
    }
    operator.close();
    return rows;
  }

  private static String value(TsBlock block, int column, int position) {
    return block.getColumn(column).isNull(position)
        ? "null"
        : Integer.toString(block.getColumn(column).getInt(position));
  }

  private static List<TsBlock> blocks(TsBlock... blocks) {
    return Arrays.asList(blocks);
  }

  private static TsBlock rows(Integer[][] values) {
    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(TSDataType.INT32, TSDataType.INT32));
    for (Integer[] row : values) {
      for (int column = 0; column < row.length; column++) {
        if (row[column] == null) {
          builder.getColumnBuilder(column).appendNull();
        } else {
          builder.getColumnBuilder(column).writeInt(row[column]);
        }
      }
    }
    builder.declarePositions(values.length);
    return builder.build(new RunLengthEncodedColumn(TIME_COLUMN_TEMPLATE, values.length));
  }

  private static class ListSourceOperator implements Operator {
    private final CommonOperatorContext context;
    private final List<TsBlock> blocks;
    private int index;

    private ListSourceOperator(CommonOperatorContext context, List<TsBlock> blocks) {
      this.context = context;
      this.blocks = blocks;
    }

    @Override
    public CommonOperatorContext getOperatorContext() {
      return context;
    }

    @Override
    public TsBlock next() {
      return blocks.get(index++);
    }

    @Override
    public boolean hasNext() {
      return index < blocks.size();
    }

    @Override
    public void close() {}

    @Override
    public boolean isFinished() {
      return !hasNext();
    }

    @Override
    public long calculateMaxPeekMemory() {
      return 0;
    }

    @Override
    public long calculateMaxReturnSize() {
      return 0;
    }

    @Override
    public long calculateRetainedSizeAfterCallingNext() {
      return 0;
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }
  }

  private static class TestOperatorContext extends CommonOperatorContext {
    private final TestMemoryReservationManager memoryReservationManager =
        new TestMemoryReservationManager();

    private TestOperatorContext() {
      super(0, new PlanNodeId("hash-join-test"), "HashInnerJoinOperator");
    }

    @Override
    public MemoryReservationManager getMemoryReservationContext() {
      return memoryReservationManager;
    }

    @Override
    public int getFragmentId() {
      return 0;
    }

    @Override
    public int getPipelineId() {
      return 0;
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }
  }

  private static class TestMemoryReservationManager implements MemoryReservationManager {
    private long reserved;

    @Override
    public void reserveMemoryCumulatively(long size) {
      reserved += size;
    }

    @Override
    public void reserveMemoryImmediately() {}

    @Override
    public void reserveMemoryImmediately(long size) {}

    @Override
    public void releaseMemoryCumulatively(long size) {
      reserved -= size;
    }

    @Override
    public void releaseMemoryImmediately(long size) {}

    @Override
    public void releaseAllReservedMemory() {
      reserved = 0;
    }

    @Override
    public Pair<Long, Long> releaseMemoryVirtually(long size) {
      return new Pair<>(0L, 0L);
    }

    @Override
    public void reserveMemoryVirtually(long bytesToBeReserved, long bytesAlreadyReserved) {}

    @Override
    public void setHighestPriority(boolean isHighestPriority) {}
  }
}
