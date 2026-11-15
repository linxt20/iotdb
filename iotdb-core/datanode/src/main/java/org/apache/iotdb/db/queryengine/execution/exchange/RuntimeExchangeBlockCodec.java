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

package org.apache.iotdb.db.queryengine.execution.exchange;

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.execution.operator.process.RuntimeCompactOutputOperator;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnEncoding;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.service.TypeService;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Objects;

/**
 * Allocation preflight for the TsFile 2.5.1-261023-SNAPSHOT block format.
 *
 * <p>The approved dependency has SHA-256
 * 78904b9375fc4617f2ff3d807084a439afb2da820181eac10578d532f9366413. A dependency update requires
 * revalidating the format and allocation bounds before runtime frontier mode can use it. This class
 * neither serializes nor allocates decoded value vectors.
 *
 * <p>Only concrete primitive/Binary columns and one level of RLE are supported. The wire walk
 * validates all lengths before TsBlockSerde can allocate arrays. Schema is supplied by planning; a
 * block cannot authorize its own types. DATE, TIMESTAMP, STRING and BLOB use the existing primitive
 * wire representations.
 */
public final class RuntimeExchangeBlockCodec {

  private static final int MAX_ARRAY_LENGTH = Integer.MAX_VALUE - 8;
  private static final long RLE_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(RunLengthEncodedColumn.class);
  private static final long BINARY_BYTES = RamUsageEstimator.shallowSizeOfInstance(Binary.class);

  /** A TypeService resolves each admitted logical type to its verified physical wire layout. */
  private record ColumnLayout(
      TSDataType wireType,
      int primitiveWidth,
      ColumnEncoding encoding,
      int encodingNumber,
      Class<?> flatColumnClass,
      boolean allowsTimeColumn,
      long decodedShallowBytes) {
    boolean supportsClass(Class<?> kind) {
      return kind == flatColumnClass || (allowsTimeColumn && kind == TimeColumn.class);
    }
  }

  private static final ColumnLayout BOOLEAN_LAYOUT =
      new ColumnLayout(
          TSDataType.BOOLEAN,
          1,
          ColumnEncoding.BYTE_ARRAY,
          0,
          BooleanColumn.class,
          false,
          RamUsageEstimator.shallowSizeOfInstance(BooleanColumn.class));
  private static final ColumnLayout INT_LAYOUT =
      new ColumnLayout(
          TSDataType.INT32,
          4,
          ColumnEncoding.INT32_ARRAY,
          1,
          IntColumn.class,
          false,
          RamUsageEstimator.shallowSizeOfInstance(IntColumn.class));
  private static final ColumnLayout LONG_LAYOUT =
      new ColumnLayout(
          TSDataType.INT64,
          8,
          ColumnEncoding.INT64_ARRAY,
          2,
          LongColumn.class,
          true,
          Math.max(
              RamUsageEstimator.shallowSizeOfInstance(LongColumn.class),
              RamUsageEstimator.shallowSizeOfInstance(TimeColumn.class)));
  private static final ColumnLayout FLOAT_LAYOUT =
      new ColumnLayout(
          TSDataType.FLOAT,
          4,
          ColumnEncoding.INT32_ARRAY,
          1,
          FloatColumn.class,
          false,
          RamUsageEstimator.shallowSizeOfInstance(FloatColumn.class));
  private static final ColumnLayout DOUBLE_LAYOUT =
      new ColumnLayout(
          TSDataType.DOUBLE,
          8,
          ColumnEncoding.INT64_ARRAY,
          2,
          DoubleColumn.class,
          false,
          RamUsageEstimator.shallowSizeOfInstance(DoubleColumn.class));
  private static final ColumnLayout BINARY_LAYOUT =
      new ColumnLayout(
          TSDataType.TEXT,
          0,
          ColumnEncoding.BINARY_ARRAY,
          3,
          BinaryColumn.class,
          false,
          RamUsageEstimator.shallowSizeOfInstance(BinaryColumn.class));

  private static final TypeService<ColumnLayout> COLUMN_LAYOUT_SERVICE =
      type ->
          switch (type.getTypeEnum()) {
            case BOOLEAN -> BOOLEAN_LAYOUT;
            case INT32, DATE -> INT_LAYOUT;
            case INT64, TIMESTAMP -> LONG_LAYOUT;
            case FLOAT -> FLOAT_LAYOUT;
            case DOUBLE -> DOUBLE_LAYOUT;
            case TEXT, STRING, BLOB -> BINARY_LAYOUT;
            case OBJECT, ROW, UNKNOWN, VECTOR ->
                throw new UnSupportedDataTypeException(
                        String.format(
                            DataNodeQueryMessages.EXCEPTION_UNSUPPORTED_DATA_TYPE_ARG_751BF348,
                            type.getTypeEnum()))
                    .setChecked(true);
          };

  static {
    COLUMN_LAYOUT_SERVICE.check();
  }

  private static ColumnLayout columnLayout(TSDataType type) {
    try {
      return COLUMN_LAYOUT_SERVICE.call(Type.fromTsDataType(Objects.requireNonNull(type)));
    } catch (UnSupportedDataTypeException unsupported) {
      throw invalid("unsupported admitted data type");
    }
  }

  private RuntimeExchangeBlockCodec() {}

  /**
   * Fixed admission limits. The seven-argument form is useful for envelope calculations only;
   * {@link #withSchema(List)} must bind the exact value-column schema before a codec operation. The
   * output envelope is max(5R + 256, R + 3 max(L, S) + W), including the separate compaction-copy
   * peak. The input envelope is 2S + W; decoded return/scratch admission remains the
   * ExchangeOperator's responsibility.
   */
  public record Bounds(
      int maxRows,
      int maxColumns,
      long maxRetainedBytes,
      long maxLogicalBytes,
      int maxSerializedBytes,
      long maxDecodedBytes,
      long maxScratchBytes,
      List<TSDataType> schema) {
    public Bounds {
      if (maxRows <= 0
          || maxRows > MAX_ARRAY_LENGTH
          || maxColumns < 0
          || maxColumns > MAX_ARRAY_LENGTH
          || maxRetainedBytes <= 0
          || maxLogicalBytes <= 0
          || maxLogicalBytes > MAX_ARRAY_LENGTH
          || maxSerializedBytes <= 0
          || maxSerializedBytes > MAX_ARRAY_LENGTH
          || maxDecodedBytes <= 0
          || maxScratchBytes <= 0) {
        throw invalid("invalid runtime block bounds");
      }
      if (schema != null) {
        schema = List.copyOf(schema);
        if (schema.size() > maxColumns) {
          throw invalid("schema exceeds admitted column count");
        }
        for (TSDataType type : schema) {
          wireType(type);
        }
        fits(add(9, multiply(2, schema.size())), maxSerializedBytes, "block header");
      }
      // Reject overflowing admission arithmetic before any physical owner is created.
      outputEnvelope(maxRetainedBytes, maxLogicalBytes, maxSerializedBytes, maxScratchBytes);
      inputEnvelope(maxSerializedBytes, maxScratchBytes);
    }

    public Bounds(
        int maxRows,
        int maxColumns,
        long maxRetainedBytes,
        long maxLogicalBytes,
        int maxSerializedBytes,
        long maxDecodedBytes,
        long maxScratchBytes) {
      this(
          maxRows,
          maxColumns,
          maxRetainedBytes,
          maxLogicalBytes,
          maxSerializedBytes,
          maxDecodedBytes,
          maxScratchBytes,
          null);
    }

    public Bounds withSchema(List<TSDataType> exactSchema) {
      return new Bounds(
          maxRows,
          maxColumns,
          maxRetainedBytes,
          maxLogicalBytes,
          maxSerializedBytes,
          maxDecodedBytes,
          maxScratchBytes,
          Objects.requireNonNull(exactSchema));
    }

    public long outputEnvelopeBytes() {
      return outputEnvelope(maxRetainedBytes, maxLogicalBytes, maxSerializedBytes, maxScratchBytes);
    }

    public long inputEnvelopeBytes() {
      return inputEnvelope(maxSerializedBytes, maxScratchBytes);
    }
  }

  public record EncodingPlan(
      int encodedLength,
      long retainedBytes,
      long logicalSizeBytes,
      long scratchUpperBound,
      long peakBytes) {}

  public record DecodingPlan(
      int encodedLength, long decodedRetainedUpperBound, long scratchUpperBound) {}

  public static EncodingPlan inspect(TsBlock block, Bounds bounds) {
    Objects.requireNonNull(block);
    List<TSDataType> schema = requireSchema(bounds);
    if (block.getClass() != TsBlock.class || block.getValueColumnCount() != schema.size()) {
      throw invalid("block schema differs from admitted schema");
    }
    int rows = block.getPositionCount();
    rows(rows, bounds);
    long encoded = add(9, multiply(2, schema.size()));
    long scratch = fixedScratch(schema.size());
    Measure time = inspectColumn(block.getTimeColumn(), TSDataType.INT64, rows, false, true);
    encoded = add(encoded, time.encoded);
    scratch = add(scratch, time.scratch);
    fits(encoded, bounds.maxSerializedBytes(), "encoded block");
    for (int c = 0; c < schema.size(); c++) {
      Measure column =
          inspectColumn(block.getColumn(c), wireType(schema.get(c)), rows, false, false);
      encoded = add(encoded, column.encoded);
      scratch = add(scratch, column.scratch);
      fits(encoded, bounds.maxSerializedBytes(), "encoded block");
      fits(scratch, bounds.maxScratchBytes(), "serialization scratch");
    }
    // This existing walker includes region backing arrays and all Binary backing, without copying.
    long retained = RuntimeCompactOutputOperator.measureInputRetainedBytes(block);
    long logical = block.getSizeInBytes();
    nonNegative(logical, "logical block size");
    fits(retained, bounds.maxRetainedBytes(), "retained block");
    fits(logical, bounds.maxLogicalBytes(), "logical block");
    fits(scratch, bounds.maxScratchBytes(), "serialization scratch");
    long peak = outputEnvelope(retained, logical, encoded, scratch);
    fits(peak, bounds.outputEnvelopeBytes(), "serialization peak");
    return new EncodingPlan((int) encoded, retained, logical, scratch, peak);
  }

  public static DecodingPlan validateEncoded(ByteBuffer encoded, Bounds bounds) {
    Objects.requireNonNull(encoded);
    List<TSDataType> schema = requireSchema(bounds);
    if (encoded.order() != ByteOrder.BIG_ENDIAN) {
      throw invalid("TsBlock wire format requires big endian");
    }
    int length = encoded.remaining();
    fits(length, bounds.maxSerializedBytes(), "encoded block");
    ByteBuffer in = encoded.duplicate().order(ByteOrder.BIG_ENDIAN);
    require(in, Integer.BYTES);
    int columns = in.getInt();
    if (columns < 0 || columns > bounds.maxColumns() || columns != schema.size()) {
      throw invalid("wire column count differs from admitted schema");
    }
    long header = add(9, multiply(2, columns));
    fits(header, length, "wire header");
    int typesStart = in.position();
    for (int c = 0; c < columns; c++) {
      TSDataType actual = readWireType(in.get());
      if (wireType(actual) != wireType(schema.get(c))) {
        throw invalid("wire type differs from admitted schema");
      }
    }
    int rows = in.getInt();
    rows(rows, bounds);
    int encodingsStart = in.position();
    for (int c = 0; c <= columns; c++) {
      int encoding = Byte.toUnsignedInt(in.get());
      if (encoding > 4) {
        throw invalid("unsupported wire column encoding");
      }
    }
    long decoded = add(TsBlock.INSTANCE_SIZE, objectArrayBytes(columns));
    long scratch = fixedScratch(columns);
    for (int c = -1; c < columns; c++) {
      TSDataType type = c < 0 ? TSDataType.INT64 : readWireType(in.get(typesStart + c));
      int encoding = Byte.toUnsignedInt(in.get(encodingsStart + c + 1));
      Measure column = validateColumn(in, type, rows, encoding, false, c < 0, bounds);
      decoded = add(decoded, column.retained);
      scratch = add(scratch, column.scratch);
      fits(decoded, bounds.maxDecodedBytes(), "decoded block");
      fits(scratch, bounds.maxScratchBytes(), "decoder scratch");
    }
    if (in.hasRemaining()) {
      throw invalid("trailing bytes after encoded block");
    }
    // No byte[], primitive[], Binary[] or schema-sized collection was allocated by this walk.
    return new DecodingPlan(length, decoded, scratch);
  }

  private static Measure inspectColumn(
      Column column, TSDataType expected, int positions, boolean nested, boolean time) {
    if (column == null || column.getPositionCount() != positions) {
      throw invalid("column shape differs from admitted schema");
    }
    if (column.getClass() == RunLengthEncodedColumn.class) {
      if (nested || column.getEncoding() != ColumnEncoding.RLE) {
        throw invalid("nested RLE is unsupported");
      }
      Measure value =
          inspectColumn(((RunLengthEncodedColumn) column).getValue(), expected, 1, true, time);
      return new Measure(add(1, value.encoded), add(RLE_BYTES, value.retained), value.scratch);
    }
    int width = primitiveWidth(expected);
    Class<?> kind = column.getClass();
    if (!supportedClass(kind, expected)
        || wireType(column.getDataType()) != expected
        || column.getEncoding() != expectedEncoding(expected)) {
      throw invalid("unsupported column implementation or encoding");
    }
    boolean nullable = column.mayHaveNull();
    int nonNull = 0;
    for (int row = 0; row < positions; row++) {
      boolean isNull = column.isNull(row);
      if (isNull && (!nullable || time)) {
        throw invalid("invalid null indicator or nullable time column");
      }
      if (!isNull) {
        nonNull++;
      }
    }
    long packed = packedBytes(positions);
    long bytes = add(1, nullable ? packed : 0);
    long scratch = nullable ? byteArrayBytes(packed) : 0;
    if (expected == TSDataType.BOOLEAN) {
      bytes = add(bytes, packed);
      scratch = add(scratch, byteArrayBytes(packed));
    } else if (expected == TSDataType.TEXT) {
      for (int row = 0; row < positions; row++) {
        if (!column.isNull(row)) {
          Binary binary = column.getBinary(row);
          if (binary == null
              || binary.getClass() != Binary.class
              || binary.getValues() == null
              || binary.getLength() != binary.getValues().length) {
            throw invalid("invalid Binary backing");
          }
          bytes = add(bytes, add(Integer.BYTES, binary.getValues().length));
        }
      }
    } else {
      bytes = add(bytes, multiply(nonNull, width));
    }
    return new Measure(bytes, 0, scratch);
  }

  private static Measure validateColumn(
      ByteBuffer in,
      TSDataType type,
      int positions,
      int encoding,
      boolean nested,
      boolean time,
      Bounds bounds) {
    if (encoding == 4) {
      if (nested) {
        throw invalid("nested RLE is unsupported");
      }
      require(in, 1);
      int innerEncoding = Byte.toUnsignedInt(in.get());
      if (innerEncoding >= 4) {
        throw invalid("unsupported RLE value encoding");
      }
      Measure inner = validateColumn(in, type, 1, innerEncoding, true, time, bounds);
      return new Measure(0, add(RLE_BYTES, inner.retained), inner.scratch);
    }
    if (encoding != encodingNumber(type)) {
      throw invalid("column encoding does not match wire type");
    }
    require(in, 1);
    int nullable = Byte.toUnsignedInt(in.get());
    if (nullable > 1) {
      throw invalid("invalid null indicator");
    }
    int nullStart = -1;
    long scratch = 0;
    // TsFile's retained estimator includes a position-sized null array even when absent.
    // Paying that conservative estimate also makes its post-decode retained check sound.
    long retained = add(shallowColumn(type), booleanArrayBytes(positions));
    if (nullable == 1) {
      nullStart = in.position();
      int packed = packedBytes(positions);
      require(in, packed);
      checkPadding(in, positions, packed);
      in.position(in.position() + packed);
      scratch = byteArrayBytes(packed);
    }
    if (type == TSDataType.BOOLEAN) {
      retained = add(retained, booleanArrayBytes(positions));
      fits(retained, bounds.maxDecodedBytes(), "decoded column");
      int packed = packedBytes(positions);
      require(in, packed);
      checkPadding(in, positions, packed);
      in.position(in.position() + packed);
      scratch = add(scratch, byteArrayBytes(packed));
    } else if (type == TSDataType.TEXT) {
      retained = add(retained, objectArrayBytes(positions));
      fits(retained, bounds.maxDecodedBytes(), "decoded column");
      for (int row = 0; row < positions; row++) {
        if (!wireNull(in, nullStart, row)) {
          require(in, Integer.BYTES);
          int binaryLength = in.getInt();
          if (binaryLength < 0 || binaryLength > MAX_ARRAY_LENGTH) {
            throw invalid("invalid Binary length");
          }
          require(in, binaryLength);
          retained = add(retained, add(BINARY_BYTES, byteArrayBytes(binaryLength)));
          fits(retained, bounds.maxDecodedBytes(), "decoded Binary column");
          in.position(in.position() + binaryLength);
        }
      }
    } else {
      retained = add(retained, primitiveArrayBytes(positions, primitiveWidth(type)));
      fits(retained, bounds.maxDecodedBytes(), "decoded column");
      long nonNull = positions;
      if (nullStart >= 0) {
        nonNull = 0;
        for (int row = 0; row < positions; row++) {
          if (!wireNull(in, nullStart, row)) {
            nonNull++;
          } else if (time) {
            throw invalid("nullable time column");
          }
        }
      }
      long valueBytes = multiply(nonNull, primitiveWidth(type));
      require(in, valueBytes);
      in.position((int) add(in.position(), valueBytes));
    }
    return new Measure(0, retained, scratch);
  }

  private static boolean wireNull(ByteBuffer in, int start, int row) {
    return start >= 0 && (in.get(start + row / 8) & (0x80 >>> (row % 8))) != 0;
  }

  private static void checkPadding(ByteBuffer in, int positions, int packed) {
    int remainder = positions & 7;
    if (remainder != 0
        && (Byte.toUnsignedInt(in.get(in.position() + packed - 1)) & ((1 << (8 - remainder)) - 1))
            != 0) {
      throw invalid("nonzero bitmap padding");
    }
  }

  private static boolean supportedClass(Class<?> kind, TSDataType type) {
    return columnLayout(type).supportsClass(kind);
  }

  private static TSDataType readWireType(byte encoded) {
    return switch (encoded) {
      case 0 -> TSDataType.BOOLEAN;
      case 1 -> TSDataType.INT32;
      case 2 -> TSDataType.INT64;
      case 3 -> TSDataType.FLOAT;
      case 4 -> TSDataType.DOUBLE;
      case 5 -> TSDataType.TEXT;
      case 8 -> TSDataType.TIMESTAMP;
      case 9 -> TSDataType.DATE;
      default -> throw invalid("unsupported wire data type");
    };
  }

  private static TSDataType wireType(TSDataType type) {
    return columnLayout(type).wireType();
  }

  private static int primitiveWidth(TSDataType type) {
    return columnLayout(type).primitiveWidth();
  }

  private static ColumnEncoding expectedEncoding(TSDataType type) {
    return columnLayout(type).encoding();
  }

  private static int encodingNumber(TSDataType type) {
    return columnLayout(type).encodingNumber();
  }

  private static long shallowColumn(TSDataType type) {
    return columnLayout(type).decodedShallowBytes();
  }

  private static List<TSDataType> requireSchema(Bounds bounds) {
    Objects.requireNonNull(bounds);
    if (bounds.schema() == null) {
      throw invalid("exact planned schema is required");
    }
    return bounds.schema();
  }

  private static void rows(int rows, Bounds bounds) {
    if (rows < 0 || rows > bounds.maxRows() || rows > MAX_ARRAY_LENGTH) {
      throw invalid("row count exceeds admitted bounds");
    }
  }

  private static int packedBytes(int positions) {
    return (int) ((positions + 7L) / 8);
  }

  private static long fixedScratch(int columns) {
    // Covers both TsBlockSerde metadata lists/backing arrays, its temporary Column[],
    // the TsBlock constructor's copy, Optional wrappers and bounded preflight records.
    return add(4096, multiply(512, add(columns, 1)));
  }

  private static long byteArrayBytes(long size) {
    return primitiveArrayBytes(size, 1);
  }

  private static long booleanArrayBytes(long size) {
    return primitiveArrayBytes(size, 1);
  }

  private static long objectArrayBytes(long size) {
    return primitiveArrayBytes(size, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
  }

  private static long primitiveArrayBytes(long size, int width) {
    long raw = add(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER, multiply(size, width));
    long alignment = RamUsageEstimator.NUM_BYTES_OBJECT_ALIGNMENT;
    return multiply((add(raw, alignment - 1) / alignment), alignment);
  }

  private static long outputEnvelope(long retained, long logical, long serialized, long scratch) {
    long copyPeak = add(multiply(5, retained), 256);
    long serializationPeak =
        add(add(retained, multiply(3, Math.max(logical, serialized))), scratch);
    return Math.max(copyPeak, serializationPeak);
  }

  private static long inputEnvelope(long serialized, long scratch) {
    return add(multiply(2, serialized), scratch);
  }

  private static long add(long a, long b) {
    try {
      return Math.addExact(a, b);
    } catch (ArithmeticException overflow) {
      throw invalid("runtime block size overflow");
    }
  }

  private static long multiply(long a, long b) {
    try {
      return Math.multiplyExact(a, b);
    } catch (ArithmeticException overflow) {
      throw invalid("runtime block size overflow");
    }
  }

  private static void require(ByteBuffer in, long bytes) {
    if (bytes < 0 || bytes > in.remaining()) {
      throw invalid("truncated encoded block");
    }
  }

  private static void fits(long value, long limit, String field) {
    nonNegative(value, field);
    if (value > limit) {
      throw invalid(field + " exceeds admitted bound");
    }
  }

  private static void nonNegative(long value, String field) {
    if (value < 0) {
      throw invalid("negative " + field);
    }
  }

  private static IllegalArgumentException invalid(String reason) {
    return new IllegalArgumentException(reason);
  }

  private record Measure(long encoded, long retained, long scratch) {}
}
