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

import org.apache.iotdb.calc.execution.operator.AbstractOperator;
import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.plan.planner.CommonOperatorUtils;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.queryengine.execution.MemoryEstimationHelper;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A local, blocking build-side hash implementation for an inner equi-join.
 *
 * <p>This operator deliberately has a narrow contract: its two inputs must already be in the same
 * hash bucket, it supports only the types accepted by {@link
 * org.apache.iotdb.calc.execution.operator.process.join.merge.comparator.JoinKeyComparatorFactory},
 * and it implements only SQL inner-equi semantics. In particular, a row whose key contains NULL or
 * NaN is never inserted or matched. The narrow contract makes it suitable as the execution-side
 * primitive for a future 2 x P repartitioned join topology without changing the existing merge-sort
 * join path.
 *
 * <p>The right child is fully materialized before the left child is read. Every retained build
 * block and the index structure are cumulatively reserved through the operator memory manager.
 * Spill, outer join semantics, residual predicates, and dynamic build-side selection intentionally
 * remain outside this first primitive.
 */
public class HashInnerJoinOperator extends AbstractOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(HashInnerJoinOperator.class);
  private static final long ROW_REFERENCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(RowReference.class);
  private static final long HASH_JOIN_KEY_INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(HashJoinKey.class);
  // Includes a HashMap entry and one reference retained by the per-key ArrayList. It is purposely
  // conservative; the value arrays and block payload are separately accounted below.
  private static final long NEW_KEY_INDEX_OVERHEAD = 64L;
  private static final long ROW_INDEX_OVERHEAD = ROW_REFERENCE_SIZE + Long.BYTES;
  // Key values reference the retained build blocks. The extra allowance covers primitive boxing
  // when a column implementation materializes a key value while preserving a conservative bound.
  private static final long KEY_VALUE_REFERENCE_OVERHEAD = 32L;

  private final Operator probeSource;
  private final Operator buildSource;
  private final int[] probeJoinKeyPositions;
  private final int[] buildJoinKeyPositions;
  private final int[] probeOutputSymbolIdx;
  private final int[] buildOutputSymbolIdx;
  private final List<Type> joinKeyTypes;
  private final TsBlockBuilder resultBuilder;
  private final MemoryReservationManager memoryReservationManager;

  private final Map<HashJoinKey, List<RowReference>> buildIndex = new HashMap<>();
  private final List<TsBlock> buildBlocks = new ArrayList<>();

  private boolean buildFinished;
  private boolean probeFinished;
  private TsBlock cachedProbeBlock;
  private int probePosition;
  private List<RowReference> currentMatches;
  private int currentMatchPosition;
  private long reservedBytes;
  private long maxReservedBytes;

  public HashInnerJoinOperator(
      CommonOperatorContext operatorContext,
      Operator probeSource,
      int[] probeJoinKeyPositions,
      int[] probeOutputSymbolIdx,
      Operator buildSource,
      int[] buildJoinKeyPositions,
      int[] buildOutputSymbolIdx,
      List<Type> joinKeyTypes,
      List<TSDataType> dataTypes) {
    if (probeJoinKeyPositions.length == 0
        || probeJoinKeyPositions.length != buildJoinKeyPositions.length
        || probeJoinKeyPositions.length != joinKeyTypes.size()) {
      throw new IllegalArgumentException("Hash inner join keys must be non-empty and aligned");
    }
    this.operatorContext = operatorContext;
    this.probeSource = probeSource;
    this.probeJoinKeyPositions = probeJoinKeyPositions;
    this.probeOutputSymbolIdx = probeOutputSymbolIdx;
    this.buildSource = buildSource;
    this.buildJoinKeyPositions = buildJoinKeyPositions;
    this.buildOutputSymbolIdx = buildOutputSymbolIdx;
    this.joinKeyTypes = List.copyOf(joinKeyTypes);
    this.resultBuilder = new TsBlockBuilder(dataTypes);
    this.memoryReservationManager = operatorContext.getMemoryReservationContext();
  }

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }

    long start = System.nanoTime();
    long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    if (!buildFinished) {
      buildOneBlock();
      return null;
    }
    if (buildIndex.isEmpty()) {
      probeFinished = true;
      return null;
    }

    while (!resultBuilder.isFull() && System.nanoTime() - start < maxRuntime) {
      if (currentMatches != null) {
        appendCurrentMatch();
        continue;
      }
      if (!ensureProbeBlock()) {
        break;
      }

      HashJoinKey probeKey = createKey(cachedProbeBlock, probeJoinKeyPositions, probePosition);
      List<RowReference> matches = probeKey == null ? null : buildIndex.get(probeKey);
      if (matches == null || matches.isEmpty()) {
        advanceProbePosition();
      } else {
        currentMatches = matches;
        currentMatchPosition = 0;
      }
    }

    if (resultBuilder.isEmpty()) {
      return null;
    }
    resultTsBlock =
        resultBuilder.build(
            new RunLengthEncodedColumn(
                CommonOperatorUtils.TIME_COLUMN_TEMPLATE, resultBuilder.getPositionCount()));
    resultBuilder.reset();
    return checkTsBlockSizeAndGetResult();
  }

  private void buildOneBlock() throws Exception {
    if (!buildSource.hasNextWithTimer()) {
      buildFinished = true;
      return;
    }
    TsBlock block = buildSource.nextWithTimer();
    if (block == null || block.isEmpty()) {
      return;
    }
    buildBlocks.add(block);
    reserve(block.getRetainedSizeInBytes());
    for (int position = 0; position < block.getPositionCount(); position++) {
      HashJoinKey key = createKey(block, buildJoinKeyPositions, position);
      if (key == null) {
        continue;
      }
      List<RowReference> rows = buildIndex.get(key);
      if (rows == null) {
        rows = new ArrayList<>();
        buildIndex.put(key, rows);
        reserve(NEW_KEY_INDEX_OVERHEAD + key.getEstimatedSize());
      }
      rows.add(new RowReference(block, position));
      reserve(ROW_INDEX_OVERHEAD);
    }
  }

  private boolean ensureProbeBlock() throws Exception {
    if (cachedProbeBlock != null && probePosition < cachedProbeBlock.getPositionCount()) {
      return true;
    }
    cachedProbeBlock = null;
    probePosition = 0;
    if (!probeSource.hasNextWithTimer()) {
      probeFinished = true;
      return false;
    }
    TsBlock block = probeSource.nextWithTimer();
    if (block == null || block.isEmpty()) {
      return false;
    }
    cachedProbeBlock = block;
    return true;
  }

  private void appendCurrentMatch() {
    RowReference buildRow = currentMatches.get(currentMatchPosition++);
    appendRow(cachedProbeBlock, probePosition, probeOutputSymbolIdx, 0);
    appendRow(
        buildRow.block,
        buildRow.position,
        buildOutputSymbolIdx,
        probeOutputSymbolIdx.length);
    resultBuilder.declarePosition();
    if (currentMatchPosition == currentMatches.size()) {
      currentMatches = null;
      currentMatchPosition = 0;
      advanceProbePosition();
    }
  }

  private void appendRow(TsBlock block, int position, int[] outputPositions, int outputOffset) {
    for (int i = 0; i < outputPositions.length; i++) {
      ColumnBuilder output = resultBuilder.getColumnBuilder(outputOffset + i);
      Column input = block.getColumn(outputPositions[i]);
      if (input.isNull(position)) {
        output.appendNull();
      } else {
        output.write(input, position);
      }
    }
  }

  private void advanceProbePosition() {
    probePosition++;
    if (cachedProbeBlock != null && probePosition >= cachedProbeBlock.getPositionCount()) {
      cachedProbeBlock = null;
      probePosition = 0;
    }
  }

  private HashJoinKey createKey(TsBlock block, int[] positions, int row) {
    Object[] values = new Object[positions.length];
    for (int keyIndex = 0; keyIndex < positions.length; keyIndex++) {
      Column column = block.getColumn(positions[keyIndex]);
      if (column.isNull(row)) {
        return null;
      }
      Object value = canonicalValue(column, row, joinKeyTypes.get(keyIndex));
      if (value == null) {
        return null;
      }
      values[keyIndex] = value;
    }
    return new HashJoinKey(values);
  }

  private static Object canonicalValue(Column column, int position, Type type) {
    switch (type.getTypeEnum()) {
      case INT32:
      case DATE:
        return column.getInt(position);
      case INT64:
      case TIMESTAMP:
        return column.getLong(position);
      case FLOAT:
        float floatValue = column.getFloat(position);
        if (Float.isNaN(floatValue)) {
          return null;
        }
        return floatValue == 0.0F ? 0.0F : floatValue;
      case DOUBLE:
        double doubleValue = column.getDouble(position);
        if (Double.isNaN(doubleValue)) {
          return null;
        }
        return doubleValue == 0.0D ? 0.0D : doubleValue;
      case BOOLEAN:
        return column.getBoolean(position);
      case STRING:
      case BLOB:
      case TEXT:
        return column.getBinary(position);
      default:
        throw new UnsupportedOperationException("Unsupported hash join key type: " + type);
    }
  }

  private void reserve(long bytes) {
    if (bytes <= 0) {
      return;
    }
    reservedBytes += bytes;
    memoryReservationManager.reserveMemoryCumulatively(bytes);
    if (reservedBytes > maxReservedBytes) {
      maxReservedBytes = reservedBytes;
      operatorContext.recordSpecifiedInfo(
          CommonOperatorUtils.MAX_RESERVED_MEMORY, Long.toString(maxReservedBytes));
    }
  }

  @Override
  public boolean hasNext() throws Exception {
    if (retainedTsBlock != null) {
      return true;
    }
    if (!buildFinished) {
      return true;
    }
    if (buildIndex.isEmpty()) {
      return false;
    }
    return currentMatches != null
        || cachedProbeBlock != null
        || (!probeFinished && probeSource.hasNextWithTimer());
  }

  @Override
  public boolean isFinished() throws Exception {
    if (retainedTsBlock != null || !buildFinished) {
      return false;
    }
    if (buildIndex.isEmpty()) {
      return true;
    }
    return currentMatches == null
        && cachedProbeBlock == null
        && (probeFinished || !probeSource.hasNextWithTimer());
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    if (!buildFinished) {
      return buildSource.isBlocked();
    }
    if (currentMatches != null || cachedProbeBlock != null) {
      return NOT_BLOCKED;
    }
    return probeSource.isBlocked();
  }

  @Override
  public void close() throws Exception {
    try {
      probeSource.close();
    } finally {
      try {
        buildSource.close();
      } finally {
        if (reservedBytes > 0) {
          memoryReservationManager.releaseMemoryCumulatively(reservedBytes);
          reservedBytes = 0;
        }
        buildIndex.clear();
        buildBlocks.clear();
        cachedProbeBlock = null;
        currentMatches = null;
        resultTsBlock = null;
        retainedTsBlock = null;
      }
    }
  }

  @Override
  public long calculateMaxPeekMemory() {
    return Math.max(
        Math.max(
            probeSource.calculateMaxPeekMemoryWithCounter(),
            buildSource.calculateMaxPeekMemoryWithCounter()),
        calculateRetainedSizeAfterCallingNext() + calculateMaxReturnSize());
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return probeSource.calculateRetainedSizeAfterCallingNext()
        + buildSource.calculateRetainedSizeAfterCallingNext()
        + reservedBytes
        + maxReturnSize;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(probeSource)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(buildSource)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + resultBuilder.getRetainedSizeInBytes()
        + reservedBytes;
  }

  private static final class RowReference {
    private final TsBlock block;
    private final int position;

    private RowReference(TsBlock block, int position) {
      this.block = block;
      this.position = position;
    }
  }

  private static final class HashJoinKey {
    private final Object[] values;
    private final int hashCode;

    private HashJoinKey(Object[] values) {
      this.values = values;
      this.hashCode = Arrays.hashCode(values);
    }

    private long getEstimatedSize() {
      return HASH_JOIN_KEY_INSTANCE_SIZE
          + RamUsageEstimator.alignObjectSize(
              RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
                  + (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * values.length)
          + KEY_VALUE_REFERENCE_OVERHEAD * values.length;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof HashJoinKey && Arrays.equals(values, ((HashJoinKey) other).values);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }
}
