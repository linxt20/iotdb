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

package org.apache.iotdb.db.queryengine.execution.exchange.sink;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TsBlockHashPartitionerTest {

  @Test
  public void testEqualKeysUseTheSameBucketAcrossBlocksAndNullsAreDeterministic() {
    TsBlockHashPartitioner partitioner =
        new TsBlockHashPartitioner(
            7, new int[] {0}, new Type[] {TypeFactory.getType(TSDataType.INT32)});
    TsBlock firstBlock = buildBlock(new Integer[] {1, 2, null, 1}, new int[] {10, 20, 30, 11});
    TsBlock secondBlock = buildBlock(new Integer[] {null, 2, 3, 1}, new int[] {31, 21, 40, 12});

    Map<Integer, Integer> bucketByKey = new HashMap<>();
    assertRowsAndRecordBuckets(partitioner.partition(firstBlock), bucketByKey);
    assertRowsAndRecordBuckets(partitioner.partition(secondBlock), bucketByKey);

    assertEquals(4, bucketByKey.size());
    assertEquals(partitioner.getBucket(firstBlock, 0), partitioner.getBucket(secondBlock, 3));
    assertEquals(partitioner.getBucket(firstBlock, 2), partitioner.getBucket(secondBlock, 0));
  }

  @Test
  public void testCompositeKeyControlsRoutingAndAllRowsArePreservedOnce() {
    TsBlockHashPartitioner partitioner =
        new TsBlockHashPartitioner(
            5,
            new int[] {0, 1},
            new Type[] {
              TypeFactory.getType(TSDataType.INT32), TypeFactory.getType(TSDataType.INT32)
            });
    TsBlock input = buildCompositeBlock();

    List<TsBlockHashPartitioner.Partition> partitions = partitioner.partition(input);

    Map<String, Integer> bucketByKey = new HashMap<>();
    Set<String> actualRows = new HashSet<>();
    for (TsBlockHashPartitioner.Partition partition : partitions) {
      TsBlock block = partition.getTsBlock();
      for (int position = 0; position < block.getPositionCount(); position++) {
        String key =
            block.getColumn(0).getInt(position) + ":" + block.getColumn(1).getInt(position);
        Integer previousBucket = bucketByKey.putIfAbsent(key, partition.getBucket());
        if (previousBucket != null) {
          assertEquals(previousBucket.intValue(), partition.getBucket());
        }
        assertTrue(
            actualRows.add(
                block.getTimeByIndex(position)
                    + ":"
                    + block.getColumn(0).getInt(position)
                    + ":"
                    + block.getColumn(1).getInt(position)
                    + ":"
                    + block.getColumn(2).getInt(position)));
      }
    }

    Set<String> expectedRows =
        new HashSet<>(Arrays.asList("0:1:10:100", "1:1:20:200", "2:2:10:300", "3:1:10:101"));
    assertEquals(expectedRows, actualRows);
    assertEquals(3, bucketByKey.size());
    assertEquals(partitioner.getBucket(input, 0), partitioner.getBucket(input, 3));
  }

  @Test
  public void testScalarAndBinaryColumnObjectHashesAreStableAcrossBlocks() {
    // One partitioner per case. The constructor takes the SQL type of each key column, so a single
    // partitioner cannot be fed every physical type any more: that is the point of the contract.
    Object[][] cases =
        new Object[][] {
          {TSDataType.BOOLEAN, true},
          {TSDataType.INT32, 42},
          {TSDataType.INT64, 42L},
          {TSDataType.FLOAT, 4.25F},
          {TSDataType.DOUBLE, 4.25D},
          {TSDataType.TIMESTAMP, 1234L},
          {TSDataType.DATE, 1234},
          {TSDataType.TEXT, new Binary("text", StandardCharsets.UTF_8)},
          {TSDataType.STRING, new Binary("string", StandardCharsets.UTF_8)},
          {TSDataType.BLOB, new Binary(new byte[] {1, 2, 3})}
        };

    for (Object[] testCase : cases) {
      TSDataType dataType = (TSDataType) testCase[0];
      Object value = testCase[1];
      Type keyType = TypeFactory.getType(dataType);
      TsBlockHashPartitioner partitioner =
          new TsBlockHashPartitioner(11, new int[] {0}, new Type[] {keyType});
      assertEquals(
          dataType.name(),
          partitioner.getBucket(buildSingleValueBlock(dataType, value), 0),
          partitioner.getBucket(buildSingleValueBlock(dataType, copyValue(value)), 0));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsTimeColumnAsPartitionKey() {
    TsBlock input = buildBlock(new Integer[] {1}, new int[] {10});

    new TsBlockHashPartitioner(
            2, new int[] {-1}, new Type[] {TypeFactory.getType(TSDataType.INT32)})
        .partition(input);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsEmptyKeyList() {
    new TsBlockHashPartitioner(2, new int[0], new Type[0]);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsDuplicateKeyIndexes() {
    new TsBlockHashPartitioner(
        2,
        new int[] {0, 0},
        new Type[] {TypeFactory.getType(TSDataType.INT32), TypeFactory.getType(TSDataType.INT32)});
  }


  @Test
  public void testEmptyBlockRoutesNoRows() {
    TsBlockHashPartitioner partitioner =
        new TsBlockHashPartitioner(
            3, new int[] {0}, new Type[] {TypeFactory.getType(TSDataType.INT32)});
    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(TSDataType.INT32));

    assertTrue(partitioner.partition(builder.build()).isEmpty());
  }

  /**
   * The scale case the routing contract has to hold at: 120 distinct keys times 50 rows each, with
   * the keys interleaved rather than sorted. A partitioner that grouped by position instead of by key
   * would show up as a split key, not as a wrong row count, which is why the per-key assertion comes
   * before the row-count one.
   */
  @Test
  public void testRepeatedKeysAcrossASixThousandRowBlockKeepEveryKeyInOneBucket() {
    int bucketCount = 3;
    int distinctKeys = 120;
    int rowsPerKey = 50;
    TsBlockHashPartitioner partitioner =
        new TsBlockHashPartitioner(
            bucketCount, new int[] {0}, new Type[] {TypeFactory.getType(TSDataType.INT32)});

    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(TSDataType.INT32, TSDataType.INT64));
    for (int position = 0; position < distinctKeys * rowsPerKey; position++) {
      builder.getTimeColumnBuilder().writeLong(position);
      builder.getColumnBuilder(0).writeInt(position % distinctKeys);
      builder.getColumnBuilder(1).writeLong(position);
      builder.declarePosition();
    }
    TsBlock input = builder.build();

    Map<Integer, Integer> bucketByKey = new HashMap<>();
    int[] bucketRowCounts = new int[bucketCount];
    Set<Long> seenRows = new HashSet<>();
    for (TsBlockHashPartitioner.Partition partition : partitioner.partition(input)) {
      TsBlock block = partition.getTsBlock();
      bucketRowCounts[partition.getBucket()] += block.getPositionCount();
      for (int position = 0; position < block.getPositionCount(); position++) {
        int key = block.getColumn(0).getInt(position);
        Integer previousBucket = bucketByKey.putIfAbsent(key, partition.getBucket());
        if (previousBucket != null) {
          assertEquals("key " + key, previousBucket.intValue(), partition.getBucket());
        }
        assertTrue(seenRows.add(block.getColumn(1).getLong(position)));
      }
    }

    assertEquals(distinctKeys, bucketByKey.size());
    assertEquals(distinctKeys * rowsPerKey, seenRows.size());
    // 120 keys over 3 buckets is 40 keys per bucket, and 40 keys times 50 rows is 2,000 rows.
    assertEquals(2000, bucketRowCounts[0]);
    assertEquals(2000, bucketRowCounts[1]);
    assertEquals(2000, bucketRowCounts[2]);
  }

  private static void assertRowsAndRecordBuckets(
      List<TsBlockHashPartitioner.Partition> partitions, Map<Integer, Integer> bucketByKey) {
    int rowCount = 0;
    for (TsBlockHashPartitioner.Partition partition : partitions) {
      TsBlock block = partition.getTsBlock();
      for (int position = 0; position < block.getPositionCount(); position++) {
        Integer key =
            block.getColumn(0).isNull(position) ? null : block.getColumn(0).getInt(position);
        Integer previousBucket = bucketByKey.putIfAbsent(key, partition.getBucket());
        if (previousBucket != null) {
          assertEquals(previousBucket.intValue(), partition.getBucket());
        }
        rowCount++;
      }
    }
    assertTrue(rowCount > 0);
  }

  private static TsBlock buildBlock(Integer[] keys, int[] payloads) {
    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(TSDataType.INT32, TSDataType.INT32));
    for (int position = 0; position < keys.length; position++) {
      builder.getTimeColumnBuilder().writeLong(position);
      if (keys[position] == null) {
        builder.getColumnBuilder(0).appendNull();
      } else {
        builder.getColumnBuilder(0).writeInt(keys[position]);
      }
      builder.getColumnBuilder(1).writeInt(payloads[position]);
      builder.declarePosition();
    }
    return builder.build();
  }

  private static TsBlock buildCompositeBlock() {
    TsBlockBuilder builder =
        new TsBlockBuilder(Arrays.asList(TSDataType.INT32, TSDataType.INT32, TSDataType.INT32));
    int[][] rows = new int[][] {{1, 10, 100}, {1, 20, 200}, {2, 10, 300}, {1, 10, 101}};
    for (int position = 0; position < rows.length; position++) {
      builder.getTimeColumnBuilder().writeLong(position);
      builder.getColumnBuilder(0).writeInt(rows[position][0]);
      builder.getColumnBuilder(1).writeInt(rows[position][1]);
      builder.getColumnBuilder(2).writeInt(rows[position][2]);
      builder.declarePosition();
    }
    return builder.build();
  }

  private static TsBlock buildSingleValueBlock(TSDataType dataType, Object value) {
    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(dataType));
    builder.getTimeColumnBuilder().writeLong(0);
    builder.getColumnBuilder(0).writeObject(value);
    builder.declarePosition();
    return builder.build();
  }

  private static Object copyValue(Object value) {
    if (value instanceof Binary) {
      return new Binary(((Binary) value).getValues().clone());
    }
    return value;
  }
}
