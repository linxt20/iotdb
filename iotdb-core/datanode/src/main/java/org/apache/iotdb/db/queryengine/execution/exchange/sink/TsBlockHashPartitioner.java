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

import org.apache.iotdb.calc.execution.relational.HashKeyCanonicalizer;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.external.commons.lang3.Validate;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Splits a {@link TsBlock} into hash buckets without changing the rows carried by the block.
 *
 * <p>The key indexes address value columns, not the time column. Each key index is paired with the
 * SQL type the planner resolved for that symbol, because the hash is a SQL hash and not a physical
 * one: {@link HashKeyCanonicalizer} is the single rule shared with the consuming hash join. Hashing
 * the raw physical object instead would send {@code -0.0} and {@code +0.0} to different buckets
 * while the executor regards them as equal.
 *
 * <p>A null key component contributes a deterministic zero hash. Therefore rows with equal keys,
 * including equal null positions in composite keys, are assigned to the same bucket independently
 * of TsBlock boundaries. A NaN component is routed by its canonical hash and, per the join
 * contract, never matches another row.
 *
 * <p>For any row whose key has no null and no NaN component, the bucket equals the join's internal
 * key hash reduced modulo the bucket count. That identity is asserted by {@code
 * HashKeyCanonicalizerTest}.
 */
public class TsBlockHashPartitioner {

  private final int bucketCount;
  private final int[] keyColumnIndexes;
  private final Type[] keyColumnTypes;

  public TsBlockHashPartitioner(int bucketCount, int[] keyColumnIndexes, Type[] keyColumnTypes) {
    Validate.isTrue(
        bucketCount > 0, DataNodeQueryMessages.EXCEPTION_BUCKETCOUNT_MUST_BE_POSITIVE_47976923);
    Validate.notNull(
        keyColumnIndexes,
        DataNodeQueryMessages.EXCEPTION_KEYCOLUMNINDEXES_MUST_NOT_BE_NULL_0D281214);
    Validate.isTrue(
        keyColumnIndexes.length > 0,
        DataNodeQueryMessages.EXCEPTION_KEYCOLUMNINDEXES_MUST_NOT_BE_EMPTY_892EAD41);
    Validate.notNull(
        keyColumnTypes, DataNodeQueryMessages.EXCEPTION_KEYCOLUMNTYPES_MUST_NOT_BE_NULL_5C3A17B9);
    Validate.isTrue(
        keyColumnTypes.length == keyColumnIndexes.length,
        DataNodeQueryMessages.EXCEPTION_KEYCOLUMNTYPES_LENGTH_MUST_MATCH_KEYCOLUMNINDEXES_8B41D0C7);
    Set<Integer> uniqueKeyColumnIndexes = new HashSet<>();
    for (int keyColumnIndex : keyColumnIndexes) {
      Validate.isTrue(
          uniqueKeyColumnIndexes.add(keyColumnIndex),
          DataNodeQueryMessages.EXCEPTION_KEYCOLUMNINDEXES_MUST_NOT_CONTAIN_DUPLICATES_79E02554);
    }
    for (Type keyColumnType : keyColumnTypes) {
      Validate.isTrue(
          HashKeyCanonicalizer.isEncodable(keyColumnType),
          DataNodeQueryMessages.EXCEPTION_KEYCOLUMNTYPE_HAS_NO_CANONICAL_ENCODING_2F6B9E14,
          keyColumnType);
    }
    this.bucketCount = bucketCount;
    this.keyColumnIndexes = keyColumnIndexes.clone();
    this.keyColumnTypes = keyColumnTypes.clone();
  }

  /**
   * Returns one non-empty TsBlock for each bucket that received rows. The returned blocks retain
   * the input columns through positional views; callers must not mutate either the input block or
   * its columns while the returned blocks are in use.
   */
  public List<Partition> partition(TsBlock tsBlock) {
    Validate.notNull(tsBlock, DataNodeQueryMessages.EXCEPTION_TSBLOCK_CANNOT_BE_NULL_E7EA3BDA);
    validateKeyColumnIndexes(tsBlock);
    int positionCount = tsBlock.getPositionCount();
    if (positionCount == 0) {
      return Collections.emptyList();
    }

    int[] rowBuckets = new int[positionCount];
    int[] bucketSizes = new int[bucketCount];
    for (int position = 0; position < positionCount; position++) {
      int bucket = getBucketUnchecked(tsBlock, position);
      rowBuckets[position] = bucket;
      bucketSizes[bucket]++;
    }

    int[][] positionsByBucket = new int[bucketCount][];
    for (int bucket = 0; bucket < bucketCount; bucket++) {
      positionsByBucket[bucket] = new int[bucketSizes[bucket]];
    }
    int[] nextPositionByBucket = new int[bucketCount];
    for (int position = 0; position < positionCount; position++) {
      int bucket = rowBuckets[position];
      positionsByBucket[bucket][nextPositionByBucket[bucket]++] = position;
    }

    List<Partition> partitions = new ArrayList<>();
    for (int bucket = 0; bucket < bucketCount; bucket++) {
      if (bucketSizes[bucket] > 0) {
        partitions.add(new Partition(bucket, copyPositions(tsBlock, positionsByBucket[bucket])));
      }
    }
    return Collections.unmodifiableList(partitions);
  }

  /** Returns the bucket for a row. The row must belong to the supplied TsBlock. */
  public int getBucket(TsBlock tsBlock, int position) {
    Validate.notNull(tsBlock, DataNodeQueryMessages.EXCEPTION_TSBLOCK_CANNOT_BE_NULL_E7EA3BDA);
    validateKeyColumnIndexes(tsBlock);
    Validate.isTrue(
        position >= 0 && position < tsBlock.getPositionCount(),
        DataNodeQueryMessages.EXCEPTION_INDEX_IS_NOT_VALID_2AB4FB3A);

    return getBucketUnchecked(tsBlock, position);
  }

  private int getBucketUnchecked(TsBlock tsBlock, int position) {
    int[] componentHashes = new int[keyColumnIndexes.length];
    for (int keyIndex = 0; keyIndex < keyColumnIndexes.length; keyIndex++) {
      componentHashes[keyIndex] =
          HashKeyCanonicalizer.routingHash(
              tsBlock.getColumn(keyColumnIndexes[keyIndex]), position, keyColumnTypes[keyIndex]);
    }
    return Math.floorMod(HashKeyCanonicalizer.foldComponentHashes(componentHashes), bucketCount);
  }

  private void validateKeyColumnIndexes(TsBlock tsBlock) {
    for (int keyColumnIndex : keyColumnIndexes) {
      Validate.isTrue(
          keyColumnIndex >= 0 && keyColumnIndex < tsBlock.getValueColumnCount(),
          DataNodeQueryMessages.EXCEPTION_KEYCOLUMNINDEX_IS_OUT_OF_BOUNDS_EE53523F);
    }
  }

  private static TsBlock copyPositions(TsBlock input, int[] positions) {
    Column[] valueColumns = input.getValueColumns();
    Column[] partitionedValueColumns = new Column[valueColumns.length];
    for (int columnIndex = 0; columnIndex < valueColumns.length; columnIndex++) {
      partitionedValueColumns[columnIndex] =
          valueColumns[columnIndex].getPositions(positions, 0, positions.length);
    }
    return new TsBlock(
        positions.length,
        input.getTimeColumn().getPositions(positions, 0, positions.length),
        partitionedValueColumns);
  }

  public static class Partition {

    private final int bucket;
    private final TsBlock tsBlock;

    private Partition(int bucket, TsBlock tsBlock) {
      this.bucket = bucket;
      this.tsBlock = tsBlock;
    }

    public int getBucket() {
      return bucket;
    }

    public TsBlock getTsBlock() {
      return tsBlock;
    }
  }
}
