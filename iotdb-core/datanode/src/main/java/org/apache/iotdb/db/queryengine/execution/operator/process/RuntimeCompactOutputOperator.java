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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.IntToLongFunction;
import org.apache.iotdb.calc.exception.MemoryNotEnoughException;
import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DictionaryColumn;
import org.apache.tsfile.read.common.block.column.DictionaryId;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.NullColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

/**
 * Copies a delegate's output into independently owned, bounded blocks without changing row order.
 *
 * <p>One input and its cursor survive between calls. Input envelopes measure entire supported
 * backing arrays, including region views and dictionary values, rather than trusting TsFile's
 * position-count-based retained estimate. Unknown column implementations are rejected.
 *
 * <p>Rows are selected before allocating any copy. Fixed arrays are allocated once per call; failed
 * size estimates are halved before allocation. No rejected candidate is rebuilt while its storage
 * could still be live. The final TsFile retained size is checked before publishing.
 *
 * <p>This operator is confined to its logical lane. Its input and copy workspace must be admitted
 * separately for every lane; the output credit does not pay for the retained input.
 */
public final class RuntimeCompactOutputOperator implements Operator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(RuntimeCompactOutputOperator.class);
  private static final int MAX_COLUMN_DEPTH = 64;
  private static final long TIME_SIZE = RamUsageEstimator.shallowSizeOfInstance(TimeColumn.class);
  private static final long BOOLEAN_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(BooleanColumn.class);
  private static final long INT_SIZE = RamUsageEstimator.shallowSizeOfInstance(IntColumn.class);
  private static final long LONG_SIZE = RamUsageEstimator.shallowSizeOfInstance(LongColumn.class);
  private static final long FLOAT_SIZE = RamUsageEstimator.shallowSizeOfInstance(FloatColumn.class);
  private static final long DOUBLE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DoubleColumn.class);
  private static final long BINARY_COLUMN_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(BinaryColumn.class);
  private static final long BINARY_SIZE = RamUsageEstimator.shallowSizeOfInstance(Binary.class);
  private static final long NULL_SIZE = RamUsageEstimator.shallowSizeOfInstance(NullColumn.class);
  private static final long RLE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(RunLengthEncodedColumn.class);
  private static final long DICTIONARY_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DictionaryColumn.class);
  private static final long DICTIONARY_ID_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DictionaryId.class);

  @FunctionalInterface
  private interface FlatColumnCopier {
    Column copy(Column source, int start, int rows, boolean[] nulls);
  }

  private record CopyStrategy(
      long shallowColumnBytes,
      IntToLongFunction valueArrayBytes,
      boolean binary,
      FlatColumnCopier copier) {}

  private static final CopyStrategy BOOLEAN_COPY =
      new CopyStrategy(
          BOOLEAN_SIZE,
          RamUsageEstimator::sizeOfBooleanArray,
          false,
          RuntimeCompactOutputOperator::copyBooleans);
  private static final CopyStrategy INT_COPY =
      new CopyStrategy(
          INT_SIZE,
          RamUsageEstimator::sizeOfIntArray,
          false,
          RuntimeCompactOutputOperator::copyInts);
  private static final CopyStrategy LONG_COPY =
      new CopyStrategy(
          LONG_SIZE,
          RamUsageEstimator::sizeOfLongArray,
          false,
          RuntimeCompactOutputOperator::copyLongs);
  private static final CopyStrategy FLOAT_COPY =
      new CopyStrategy(
          FLOAT_SIZE,
          RamUsageEstimator::sizeOfFloatArray,
          false,
          RuntimeCompactOutputOperator::copyFloats);
  private static final CopyStrategy DOUBLE_COPY =
      new CopyStrategy(
          DOUBLE_SIZE,
          RamUsageEstimator::sizeOfDoubleArray,
          false,
          RuntimeCompactOutputOperator::copyDoubles);
  private static final CopyStrategy BINARY_COPY =
      new CopyStrategy(
          BINARY_COLUMN_SIZE,
          RamUsageEstimator::sizeOfObjectArray,
          true,
          RuntimeCompactOutputOperator::copyBinaries);

  private static final TypeService<CopyStrategy> COPY_COLUMN_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> BOOLEAN_COPY;
            case INT32, DATE -> INT_COPY;
            case INT64, TIMESTAMP -> LONG_COPY;
            case FLOAT -> FLOAT_COPY;
            case DOUBLE -> DOUBLE_COPY;
            case TEXT, STRING, BLOB -> BINARY_COPY;
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(
                        String.format(
                            DataNodeQueryMessages.EXCEPTION_UNSUPPORTED_DATA_TYPE_ARG_751BF348,
                            type.getTypeEnum()))
                    .setChecked(true);
          };

  static {
    COPY_COLUMN_SERVICE.check();
  }

  private static CopyStrategy copyStrategy(Column source) {
    try {
      return COPY_COLUMN_SERVICE.call(Type.fromTsDataType(source.getDataType()));
    } catch (UnSupportedDataTypeException unsupported) {
      throw unsupported(source);
    }
  }

  private static CopyStrategy copyStrategy(Column source, TSDataType plannedType) {
    if (plannedType == null) return copyStrategy(source);
    CopyStrategy target;
    try {
      target = COPY_COLUMN_SERVICE.call(Type.fromTsDataType(plannedType));
    } catch (UnSupportedDataTypeException unsupported) {
      throw unsupported(source);
    }
    if (isUntypedNull(source)) return target;
    CopyStrategy actual = copyStrategy(source);
    if (actual == target
        || (source.getDataType() == TSDataType.INT32 && plannedType == TSDataType.INT64)) {
      return target;
    }
    throw new IllegalArgumentException("Native output type differs from planned runtime schema");
  }

  private final Operator delegate;
  private final long maxOutputRetainedBytes;
  private final long maxInputRetainedBytes;
  private final long workspaceUpperBoundBytes;
  private final List<TSDataType> plannedSchema;

  private TsBlock pending;
  private int nextPosition;
  private long pendingRetainedBytes;
  private boolean closed;

  public RuntimeCompactOutputOperator(
      Operator delegate, long maxOutputRetainedBytes, long maxInputRetainedBytes) {
    this(delegate, maxOutputRetainedBytes, maxInputRetainedBytes, null);
  }

  /** The immutable wire schema also controls row selection before allocating an expanded copy. */
  public RuntimeCompactOutputOperator(
      Operator delegate,
      long maxOutputRetainedBytes,
      long maxInputRetainedBytes,
      List<TSDataType> plannedSchema) {
    this.delegate = Objects.requireNonNull(delegate);
    if (maxOutputRetainedBytes <= 0 || maxInputRetainedBytes <= 0) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.RUNTIME_COMPACT_OUTPUT_LIMIT_MUST_BE_POSITIVE);
    }
    this.maxOutputRetainedBytes = maxOutputRetainedBytes;
    this.maxInputRetainedBytes = maxInputRetainedBytes;
    this.workspaceUpperBoundBytes = estimateWorkspaceUpperBound(maxOutputRetainedBytes);
    this.plannedSchema = plannedSchema == null ? null : List.copyOf(plannedSchema);
  }

  /**
   * Includes the copied block's actual arrays and the transient Optional wrappers used by TsFile
   * constructors. The block's column reference array is included even though TsFile omits it.
   */
  public static long estimateWorkspaceUpperBound(long maxOutputRetainedBytes) {
    if (maxOutputRetainedBytes <= 0) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.RUNTIME_COMPACT_OUTPUT_LIMIT_MUST_BE_POSITIVE);
    }
    return add(add(maxOutputRetainedBytes, maxOutputRetainedBytes), 256);
  }

  public long getWorkspaceUpperBoundBytes() {
    return workspaceUpperBoundBytes;
  }

  @Override
  public CommonOperatorContext getOperatorContext() {
    return delegate.getOperatorContext();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return closed || pending != null
        ? NOT_BLOCKED
        : nonCancellationPropagating(delegate.isBlocked());
  }

  @Override
  public boolean hasNext() throws Exception {
    return !closed && (pending != null || delegate.hasNext());
  }

  @Override
  public boolean isFinished() throws Exception {
    return closed || (pending == null && delegate.isFinished());
  }

  @Override
  public TsBlock next() throws Exception {
    if (closed) {
      return null;
    }
    if (pending == null) {
      if (!delegate.isBlocked().isDone()) {
        return null;
      }
      // The wrapper shares the delegate's context. Timer wrappers here would count the call twice.
      TsBlock input = delegate.next();
      if (input == null) {
        return null;
      }
      long retained = measureInputRetainedBytes(input);
      requireFits(retained, maxInputRetainedBytes);
      if (plannedSchema != null && plannedSchema.size() != input.getValueColumnCount()) {
        throw new IllegalArgumentException(
            "Native output width differs from planned runtime schema");
      }
      if (input.isEmpty()) {
        return null;
      }
      pending = input;
      pendingRetainedBytes = retained;
      nextPosition = 0;
    }

    int rows = chooseRowCount();
    TsBlock output = copyRows(pending, nextPosition, rows, plannedSchema);
    requireFits(output.getRetainedSizeInBytes(), maxOutputRetainedBytes);
    nextPosition += rows;
    if (nextPosition == pending.getPositionCount()) {
      pending = null;
      pendingRetainedBytes = 0;
      nextPosition = 0;
    }
    return output;
  }

  /**
   * Flatten one already bounded native output, including dictionary views produced by hash
   * partitioning. The caller owns an admitted output-envelope credit before invoking this method.
   * All backing and copy sizes are checked before allocating the independent payload arrays.
   */
  public static TsBlock copyAllRowsBounded(
      TsBlock input, long maxInputRetainedBytes, long maxOutputRetainedBytes) {
    return copyAllRowsBounded(input, maxInputRetainedBytes, maxOutputRetainedBytes, null);
  }

  /**
   * Canonicalize native output to its frozen planned wire schema before strict codec validation.
   * Native constant folding can represent an INT64 literal by an IntColumn. Only the lossless
   * INT32-to-INT64 widening is admitted; unrelated types and narrowing remain errors. Untyped nulls
   * are materialized with the planned physical type. The expanded copy is bounded before
   * allocation.
   */
  public static TsBlock copyAllRowsBounded(
      TsBlock input,
      long maxInputRetainedBytes,
      long maxOutputRetainedBytes,
      List<TSDataType> plannedSchema) {
    if (maxInputRetainedBytes <= 0 || maxOutputRetainedBytes <= 0) {
      throw new IllegalArgumentException();
    }
    requireFits(measureInputRetainedBytes(input), maxInputRetainedBytes);
    if (plannedSchema != null && plannedSchema.size() != input.getValueColumnCount()) {
      throw new IllegalArgumentException("Native output width differs from planned runtime schema");
    }
    requireFits(
        estimateCopiedRetainedBytes(input, 0, input.getPositionCount(), plannedSchema),
        maxOutputRetainedBytes);
    TsBlock output = copyRows(input, 0, input.getPositionCount(), plannedSchema);
    requireFits(output.getRetainedSizeInBytes(), maxOutputRetainedBytes);
    return output;
  }

  private int chooseRowCount() {
    int remaining = pending.getPositionCount() - nextPosition;
    // Every copied row needs at least one eight-byte time value.
    int rows = (int) Math.min((long) remaining, Math.max(1L, maxOutputRetainedBytes / Long.BYTES));
    while (true) {
      long retained = estimateCopiedRetainedBytes(pending, nextPosition, rows, plannedSchema);
      if (retained <= maxOutputRetainedBytes) {
        return rows;
      }
      if (rows == 1) {
        throw memoryLimit(retained, maxOutputRetainedBytes);
      }
      rows = Math.max(1, rows / 2);
    }
  }

  private static long estimateCopiedRetainedBytes(TsBlock input, int start, int rows) {
    return estimateCopiedRetainedBytes(input, start, rows, null);
  }

  private static long estimateCopiedRetainedBytes(
      TsBlock input, int start, int rows, List<TSDataType> plannedSchema) {
    long bytes =
        add(add(TsBlock.INSTANCE_SIZE, TIME_SIZE), RamUsageEstimator.sizeOfLongArray(rows));
    bytes = add(bytes, RamUsageEstimator.sizeOfObjectArray(input.getValueColumnCount()));
    for (int c = 0; c < input.getValueColumnCount(); c++) {
      Column source = input.getColumn(c);
      if (plannedSchema == null && isUntypedNull(source)) {
        bytes = add(bytes, NULL_SIZE);
        continue;
      }
      CopyStrategy strategy =
          copyStrategy(source, plannedSchema == null ? null : plannedSchema.get(c));
      bytes = add(bytes, RamUsageEstimator.sizeOfBooleanArray(rows));
      bytes =
          add(
              bytes,
              add(strategy.shallowColumnBytes(), strategy.valueArrayBytes().applyAsLong(rows)));
      if (strategy.binary()) {
        for (int row = start; row < start + rows; row++) {
          if (!source.isNull(row)) {
            Binary binary = source.getBinary(row);
            if (binary == null || binary.getValues() == null) {
              throw unsupported(source);
            }
            bytes =
                add(
                    bytes,
                    add(BINARY_SIZE, RamUsageEstimator.sizeOfByteArray(binary.getValues().length)));
          }
        }
      }
    }
    return bytes;
  }

  private static TsBlock copyRows(TsBlock input, int start, int rows) {
    return copyRows(input, start, rows, null);
  }

  private static TsBlock copyRows(
      TsBlock input, int start, int rows, List<TSDataType> plannedSchema) {
    long[] times = new long[rows];
    Column time = input.getTimeColumn();
    for (int row = 0; row < rows; row++) {
      if (time.isNull(start + row)) {
        throw unsupported(time);
      }
      times[row] = time.getLong(start + row);
    }
    Column[] columns = new Column[input.getValueColumnCount()];
    for (int c = 0; c < columns.length; c++) {
      columns[c] =
          copyColumn(
              input.getColumn(c), start, rows, plannedSchema == null ? null : plannedSchema.get(c));
    }
    // All arrays above are owned by this result; this avoids a second Column[] clone.
    return TsBlock.wrapBlocksWithoutCopy(rows, new TimeColumn(rows, times), columns);
  }

  private static Column copyColumn(Column source, int start, int rows, TSDataType plannedType) {
    if (plannedType == null && isUntypedNull(source)) {
      return new NullColumn(rows);
    }
    boolean[] nulls = new boolean[rows];
    for (int row = 0; row < rows; row++) {
      nulls[row] = source.isNull(start + row);
    }
    CopyStrategy strategy = copyStrategy(source, plannedType);
    if (plannedType != null && strategy == INT_COPY && isUntypedNull(source)) {
      return new IntColumn(rows, Optional.of(nulls), new int[rows], plannedType);
    }
    return strategy.copier().copy(source, start, rows, nulls);
  }

  private static Column copyBooleans(Column source, int start, int rows, boolean[] nulls) {
    boolean[] values = new boolean[rows];
    for (int row = 0; row < rows; row++) {
      if (!nulls[row]) values[row] = source.getBoolean(start + row);
    }
    return new BooleanColumn(rows, Optional.of(nulls), values);
  }

  private static Column copyInts(Column source, int start, int rows, boolean[] nulls) {
    int[] values = new int[rows];
    for (int row = 0; row < rows; row++) {
      if (!nulls[row]) values[row] = source.getInt(start + row);
    }
    return new IntColumn(rows, Optional.of(nulls), values, source.getDataType());
  }

  private static Column copyLongs(Column source, int start, int rows, boolean[] nulls) {
    long[] values = new long[rows];
    for (int row = 0; row < rows; row++) {
      if (!nulls[row]) values[row] = source.getLong(start + row);
    }
    return new LongColumn(0, rows, nulls, values);
  }

  private static Column copyFloats(Column source, int start, int rows, boolean[] nulls) {
    float[] values = new float[rows];
    for (int row = 0; row < rows; row++) {
      if (!nulls[row]) values[row] = source.getFloat(start + row);
    }
    return new FloatColumn(rows, Optional.of(nulls), values);
  }

  private static Column copyDoubles(Column source, int start, int rows, boolean[] nulls) {
    double[] values = new double[rows];
    for (int row = 0; row < rows; row++) {
      if (!nulls[row]) values[row] = source.getDouble(start + row);
    }
    return new DoubleColumn(rows, Optional.of(nulls), values);
  }

  private static Column copyBinaries(Column source, int start, int rows, boolean[] nulls) {
    Binary[] values = new Binary[rows];
    for (int row = 0; row < rows; row++) {
      if (!nulls[row]) {
        byte[] bytes = source.getBinary(start + row).getValues();
        values[row] = new Binary(Arrays.copyOf(bytes, bytes.length));
      }
    }
    return new BinaryColumn(0, rows, nulls, values);
  }

  /**
   * Conservative size of all supported objects retained by the input, including backing storage
   * outside a region view. Aliased arrays or dictionary values may be counted more than once.
   */
  public static long measureInputRetainedBytes(TsBlock block) {
    Objects.requireNonNull(block);
    if (block.getClass() != TsBlock.class) {
      throw new MemoryNotEnoughException(
          String.format(
              DataNodeQueryMessages.RUNTIME_COMPACT_OUTPUT_UNSUPPORTED_COLUMN,
              block.getClass().getName()));
    }
    long bytes =
        add(
            TsBlock.INSTANCE_SIZE,
            RamUsageEstimator.sizeOfObjectArray(block.getValueColumnCount()));
    bytes = add(bytes, measureColumn(block.getTimeColumn(), 0));
    for (int c = 0; c < block.getValueColumnCount(); c++) {
      bytes = add(bytes, measureColumn(block.getColumn(c), 0));
    }
    return Math.max(bytes, block.getRetainedSizeInBytes());
  }

  private static long measureColumn(Column column, int depth) {
    if (column == null || depth > MAX_COLUMN_DEPTH) {
      throw unsupported(column);
    }
    Class<?> kind = column.getClass();
    long bytes = columnShallowBytes(column);
    if (kind == RunLengthEncodedColumn.class) {
      return add(bytes, measureColumn(((RunLengthEncodedColumn) column).getValue(), depth + 1));
    }
    if (kind == DictionaryColumn.class) {
      DictionaryColumn dictionary = (DictionaryColumn) column;
      bytes = add(bytes, RamUsageEstimator.sizeOf(dictionary.getRawIds()));
      if (dictionary.getDictionarySourceId() != null) {
        if (dictionary.getDictionarySourceId().getClass() != DictionaryId.class) {
          throw unsupported(column);
        }
        bytes = add(bytes, DICTIONARY_ID_SIZE);
      }
      return add(bytes, measureColumn(dictionary.getDictionary(), depth + 1));
    }
    if (kind == NullColumn.class) {
      return bytes;
    }
    if (kind == TimeColumn.class) {
      return add(bytes, RamUsageEstimator.sizeOf(column.getLongs()));
    }
    if (kind == BooleanColumn.class) {
      bytes = add(bytes, RamUsageEstimator.sizeOf(column.getBooleans()));
    } else if (kind == IntColumn.class) {
      bytes = add(bytes, RamUsageEstimator.sizeOf(column.getInts()));
    } else if (kind == LongColumn.class) {
      bytes = add(bytes, RamUsageEstimator.sizeOf(column.getLongs()));
    } else if (kind == FloatColumn.class) {
      bytes = add(bytes, RamUsageEstimator.sizeOf(column.getFloats()));
    } else if (kind == DoubleColumn.class) {
      bytes = add(bytes, RamUsageEstimator.sizeOf(column.getDoubles()));
    } else if (kind == BinaryColumn.class) {
      Binary[] values = column.getBinaries();
      bytes = add(bytes, RamUsageEstimator.sizeOfObjectArray(values.length));
      for (Binary value : values) {
        if (value != null) {
          if (value.getClass() != Binary.class) {
            throw unsupported(column);
          }
          bytes = add(bytes, BINARY_SIZE);
          if (value.getValues() != null) {
            bytes = add(bytes, RamUsageEstimator.sizeOf(value.getValues()));
          }
        }
      }
    } else {
      throw unsupported(column);
    }
    // Without a bitmap this getter would allocate one; mayHaveNull guards that branch.
    if (column.mayHaveNull()) {
      bytes = add(bytes, RamUsageEstimator.sizeOf(column.isNull()));
    }
    return bytes;
  }

  private static long columnShallowBytes(Column column) {
    Class<?> kind = column.getClass();
    if (kind == TimeColumn.class) {
      return TIME_SIZE;
    } else if (kind == BooleanColumn.class) {
      return BOOLEAN_SIZE;
    } else if (kind == IntColumn.class) {
      return INT_SIZE;
    } else if (kind == LongColumn.class) {
      return LONG_SIZE;
    } else if (kind == FloatColumn.class) {
      return FLOAT_SIZE;
    } else if (kind == DoubleColumn.class) {
      return DOUBLE_SIZE;
    } else if (kind == BinaryColumn.class) {
      return BINARY_COLUMN_SIZE;
    } else if (kind == NullColumn.class) {
      return NULL_SIZE;
    } else if (kind == RunLengthEncodedColumn.class) {
      return RLE_SIZE;
    } else if (kind == DictionaryColumn.class) {
      return DICTIONARY_SIZE;
    }
    throw unsupported(column);
  }

  private static boolean isUntypedNull(Column column) {
    for (int depth = 0; depth <= MAX_COLUMN_DEPTH; depth++) {
      if (column.getClass() == NullColumn.class) {
        return true;
      }
      if (column.getClass() == RunLengthEncodedColumn.class) {
        column = ((RunLengthEncodedColumn) column).getValue();
      } else if (column.getClass() == DictionaryColumn.class) {
        column = ((DictionaryColumn) column).getDictionary();
      } else {
        return false;
      }
    }
    throw unsupported(column);
  }

  private static long add(long left, long right) {
    return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
  }

  private static void requireFits(long required, long limit) {
    if (required > limit) {
      throw memoryLimit(required, limit);
    }
  }

  private static MemoryNotEnoughException memoryLimit(long required, long limit) {
    return new MemoryNotEnoughException(
        String.format(
            DataNodeQueryMessages.RUNTIME_COMPACT_OUTPUT_MEMORY_LIMIT_EXCEEDED, required, limit));
  }

  private static MemoryNotEnoughException unsupported(Column column) {
    return new MemoryNotEnoughException(
        String.format(
            DataNodeQueryMessages.RUNTIME_COMPACT_OUTPUT_UNSUPPORTED_COLUMN,
            column == null ? "null" : column.getClass().getName()));
  }

  @Override
  public void close() throws Exception {
    if (closed) {
      return;
    }
    closed = true;
    pending = null;
    pendingRetainedBytes = 0;
    nextPosition = 0;
    delegate.close();
  }

  @Override
  public long calculateMaxPeekMemory() {
    long compact =
        add(
            add(delegate.calculateRetainedSizeAfterCallingNext(), maxInputRetainedBytes),
            workspaceUpperBoundBytes);
    return add(INSTANCE_SIZE, Math.max(delegate.calculateMaxPeekMemory(), compact));
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxOutputRetainedBytes;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return add(
        INSTANCE_SIZE,
        add(delegate.calculateRetainedSizeAfterCallingNext(), maxInputRetainedBytes));
  }

  @Override
  public long ramBytesUsed() {
    return add(INSTANCE_SIZE, add(delegate.ramBytesUsed(), pendingRetainedBytes));
  }
}
