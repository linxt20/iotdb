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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.iotdb.db.queryengine.execution.exchange.RuntimeExchangeBlockCodec.Bounds;
import org.apache.iotdb.db.queryengine.execution.exchange.RuntimeExchangeBlockCodec.DecodingPlan;
import org.apache.iotdb.db.queryengine.execution.exchange.RuntimeExchangeBlockCodec.EncodingPlan;
import org.apache.iotdb.db.queryengine.execution.operator.process.RuntimeCompactOutputOperator;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DictionaryColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.FloatColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.NullColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.apache.tsfile.utils.Binary;
import org.junit.Test;

public class RuntimeExchangeBlockCodecTest {

  private static final List<TSDataType> ALL_TYPES =
      List.of(
          TSDataType.BOOLEAN,
          TSDataType.INT32,
          TSDataType.INT64,
          TSDataType.FLOAT,
          TSDataType.DOUBLE,
          TSDataType.TEXT);
  private static final TsBlockSerde SERDE = new TsBlockSerde();

  @Test
  public void plannedConstantAndUntypedNullBecomeStrictTypedWireBeforeSerialization()
      throws Exception {
    TsBlock nativeOutput =
        new TsBlock(
            new TimeColumn(3, new long[] {1, 2, 3}),
            new RunLengthEncodedColumn(new IntColumn(1, Optional.empty(), new int[] {1}), 3),
            new NullColumn(3));
    Bounds admission = bounds(List.of(TSDataType.INT64, TSDataType.STRING));
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeExchangeBlockCodec.inspect(nativeOutput, admission));
    TsBlock canonical =
        RuntimeCompactOutputOperator.copyAllRowsBounded(
            nativeOutput, 65536, 65536, admission.schema());
    assertRoundTrip(canonical, admission);
    assertEquals(TSDataType.INT64, canonical.getColumn(0).getDataType());
    for (int row = 0; row < 3; row++) {
      assertEquals(1L, canonical.getColumn(0).getLong(row));
      assertTrue(canonical.getColumn(1).isNull(row));
    }
  }

  @Test
  public void primitiveAndBinaryLengthsMatchActualSerdeAcrossBitmapBoundaries() throws Exception {
    for (int rows = 0; rows <= 17; rows++) {
      for (boolean nullable : List.of(false, true)) {
        assertRoundTrip(allTypes(rows, nullable), bounds(ALL_TYPES));
      }
    }
  }

  @Test
  public void oneLevelRleSupportsValuesNullsAndTime() throws Exception {
    Column value = new IntColumn(1, Optional.empty(), new int[] {42});
    Column binary =
        new BinaryColumn(1, Optional.empty(), new Binary[] {new Binary(new byte[] {1, 2, 3})});
    Column nullValue = new DoubleColumn(1, Optional.of(new boolean[] {true}), new double[] {0});
    for (int rows : List.of(0, 1, 8, 17)) {
      TsBlock block =
          new TsBlock(
              rows,
              new RunLengthEncodedColumn(new TimeColumn(1, new long[] {77}), rows),
              new RunLengthEncodedColumn(value, rows),
              new RunLengthEncodedColumn(binary, rows),
              new RunLengthEncodedColumn(nullValue, rows));
      assertRoundTrip(block, bounds(List.of(TSDataType.INT32, TSDataType.TEXT, TSDataType.DOUBLE)));
    }
  }

  @Test
  public void timeOnlyBlocksHaveTheirOwnRetainedAndEncodingBound() throws Exception {
    assertRoundTrip(new TsBlock(new TimeColumn(3, new long[] {1, 2, 3})), bounds(List.of()));
  }

  @Test
  public void logicalAliasesMatchVerifiedPrimitiveWireRepresentation() throws Exception {
    TsBlock block =
        new TsBlock(
            new TimeColumn(1, new long[] {1}),
            new IntColumn(1, Optional.empty(), new int[] {3}, TSDataType.DATE),
            new LongColumn(1, Optional.empty(), new long[] {4}),
            new BinaryColumn(1, Optional.empty(), new Binary[] {new Binary(new byte[] {5})}),
            new BinaryColumn(1, Optional.empty(), new Binary[] {new Binary(new byte[] {6})}));
    Bounds bounds =
        bounds(List.of(TSDataType.DATE, TSDataType.TIMESTAMP, TSDataType.STRING, TSDataType.BLOB));
    EncodingPlan encoding = RuntimeExchangeBlockCodec.inspect(block, bounds);
    ByteBuffer encoded = SERDE.serialize(block);
    assertEquals(encoding.encodedLength(), encoded.remaining());
    RuntimeExchangeBlockCodec.validateEncoded(encoded, bounds);
    // The approved serde normalizes a flat DATE IntColumn during serialization.
    assertEquals(TSDataType.INT32.getType(), encoded.get(4));
  }

  @Test
  public void schemaMustBeBoundAndCopiedBeforeAnyCodecOperation() throws Exception {
    Bounds unbound = new Bounds(32, 16, 1 << 20, 1 << 20, 1 << 20, 1 << 20, 1 << 17);
    TsBlock block = oneInt();
    ByteBuffer encoded = SERDE.serialize(block);
    assertThrows(
        IllegalArgumentException.class, () -> RuntimeExchangeBlockCodec.inspect(block, unbound));
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeExchangeBlockCodec.validateEncoded(encoded, unbound));
    List<TSDataType> schema = new ArrayList<>(List.of(TSDataType.INT32));
    Bounds bound = unbound.withSchema(schema);
    schema.set(0, TSDataType.TEXT);
    assertEquals(List.of(TSDataType.INT32), bound.schema());
    assertThrows(UnsupportedOperationException.class, () -> bound.schema().clear());
    RuntimeExchangeBlockCodec.inspect(block, bound);
  }

  @Test
  public void claimedHeaderTypesCannotReplacePlannedSchema() throws Exception {
    TsBlock block = oneInt();
    Bounds wrongType = bounds(List.of(TSDataType.FLOAT));
    assertThrows(
        IllegalArgumentException.class, () -> RuntimeExchangeBlockCodec.inspect(block, wrongType));
    ByteBuffer encoded = SERDE.serialize(block);
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeExchangeBlockCodec.validateEncoded(encoded, wrongType));
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeExchangeBlockCodec.validateEncoded(encoded, bounds(List.of())));
  }

  @Test
  public void directReadOnlyAndSlicedBuffersUseOnlyRemainingBytes() throws Exception {
    TsBlock block = oneBinary();
    Bounds bounds = bounds(List.of(TSDataType.TEXT));
    ByteBuffer wire = SERDE.serialize(block);
    ByteBuffer direct = ByteBuffer.allocateDirect(wire.remaining() + 16);
    direct.position(8);
    direct.put(wire.duplicate());
    direct.limit(direct.position());
    direct.position(8);
    for (ByteBuffer view :
        List.of(direct, direct.asReadOnlyBuffer(), direct.slice().asReadOnlyBuffer())) {
      int position = view.position();
      int limit = view.limit();
      DecodingPlan plan = RuntimeExchangeBlockCodec.validateEncoded(view, bounds);
      assertEquals(wire.remaining(), plan.encodedLength());
      assertEquals(position, view.position());
      assertEquals(limit, view.limit());
    }
  }

  @Test
  public void everyTruncatedPrefixFailsBeforeDeserialization() throws Exception {
    ByteBuffer encoded = SERDE.serialize(oneBinary());
    Bounds bounds = bounds(List.of(TSDataType.TEXT));
    for (int size = 0; size < encoded.remaining(); size++) {
      ByteBuffer prefix = encoded.duplicate();
      prefix.limit(size);
      assertThrows(
          IllegalArgumentException.class,
          () -> RuntimeExchangeBlockCodec.validateEncoded(prefix, bounds));
    }
  }

  @Test
  public void trailingBytesAndWrongByteOrderAreRejected() throws Exception {
    ByteBuffer encoded = SERDE.serialize(oneInt());
    ByteBuffer trailing = ByteBuffer.allocate(encoded.remaining() + 1);
    trailing.put(encoded.duplicate()).put((byte) 0).flip();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RuntimeExchangeBlockCodec.validateEncoded(trailing, bounds(List.of(TSDataType.INT32))));
    ByteBuffer little = encoded.duplicate().order(ByteOrder.LITTLE_ENDIAN);
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeExchangeBlockCodec.validateEncoded(little, bounds(List.of(TSDataType.INT32))));
  }

  @Test
  public void invalidColumnAndRowCountsDoNotReachArrayAllocation() throws Exception {
    Bounds bounds = bounds(List.of(TSDataType.INT32));
    ByteBuffer valid = SERDE.serialize(oneInt());
    for (int columns : List.of(-1, 0, 2, Integer.MAX_VALUE)) {
      ByteBuffer bad = copy(valid);
      bad.putInt(0, columns);
      assertThrows(
          IllegalArgumentException.class,
          () -> RuntimeExchangeBlockCodec.validateEncoded(bad, bounds));
    }
    for (int rows : List.of(-1, 1025, Integer.MAX_VALUE)) {
      ByteBuffer bad = copy(valid);
      bad.putInt(5, rows);
      assertThrows(
          IllegalArgumentException.class,
          () -> RuntimeExchangeBlockCodec.validateEncoded(bad, bounds));
    }
  }

  @Test
  public void unsupportedTypesEncodingsAndTypeEncodingMismatchFail() throws Exception {
    ByteBuffer valid = SERDE.serialize(oneInt());
    Bounds bounds = bounds(List.of(TSDataType.INT32));
    for (int type : List.of(6, 7, 10, 11, 12, 127)) {
      ByteBuffer bad = copy(valid);
      bad.put(4, (byte) type);
      assertThrows(
          IllegalArgumentException.class,
          () -> RuntimeExchangeBlockCodec.validateEncoded(bad, bounds));
    }
    for (int encoding : List.of(0, 2, 3, 5, 6, 127)) {
      ByteBuffer bad = copy(valid);
      bad.put(10, (byte) encoding);
      assertThrows(
          IllegalArgumentException.class,
          () -> RuntimeExchangeBlockCodec.validateEncoded(bad, bounds));
    }
  }

  @Test
  public void malformedNullIndicatorsAndMaskPaddingFail() throws Exception {
    TsBlock block =
        new TsBlock(
            new TimeColumn(1, new long[] {1}),
            new IntColumn(1, Optional.of(new boolean[] {true}), new int[] {0}));
    ByteBuffer valid = SERDE.serialize(block);
    Bounds bounds = bounds(List.of(TSDataType.INT32));
    ByteBuffer badFlag = copy(valid);
    badFlag.put(20, (byte) 2);
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeExchangeBlockCodec.validateEncoded(badFlag, bounds));
    ByteBuffer badPadding = copy(valid);
    badPadding.put(21, (byte) 0x81);
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeExchangeBlockCodec.validateEncoded(badPadding, bounds));
  }

  @Test
  public void malformedBooleanValuePaddingFails() throws Exception {
    TsBlock block =
        new TsBlock(
            new TimeColumn(1, new long[] {1}),
            new BooleanColumn(1, Optional.empty(), new boolean[] {true}));
    ByteBuffer bad = copy(SERDE.serialize(block));
    bad.put(21, (byte) 0x81);
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeExchangeBlockCodec.validateEncoded(bad, bounds(List.of(TSDataType.BOOLEAN))));
  }

  @Test
  public void malformedBinaryLengthsFailWithoutAllocatingBinaryPayload() throws Exception {
    ByteBuffer valid = SERDE.serialize(oneBinary());
    for (int length : List.of(-1, Integer.MAX_VALUE, 4096)) {
      ByteBuffer bad = copy(valid);
      bad.putInt(21, length);
      assertThrows(
          IllegalArgumentException.class,
          () -> RuntimeExchangeBlockCodec.validateEncoded(bad, bounds(List.of(TSDataType.TEXT))));
    }
  }

  @Test
  public void nestedRleAndDictionaryWireAreRejectedBeforeDecoding() throws Exception {
    TsBlock block =
        new TsBlock(
            new TimeColumn(1, new long[] {1}),
            new RunLengthEncodedColumn(new IntColumn(1, Optional.empty(), new int[] {2}), 1));
    ByteBuffer wire = SERDE.serialize(block);
    ByteBuffer nested = copy(wire);
    nested.put(20, (byte) 4);
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeExchangeBlockCodec.validateEncoded(nested, bounds(List.of(TSDataType.INT32))));
    ByteBuffer dictionary = copy(wire);
    dictionary.put(20, (byte) 5);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RuntimeExchangeBlockCodec.validateEncoded(
                dictionary, bounds(List.of(TSDataType.INT32))));
  }

  @Test
  public void dictionaryAndUntypedNullObjectsAreRejectedBeforeSerialization() {
    Column dictionary =
        DictionaryColumn.create(
            2, new IntColumn(3, Optional.empty(), new int[] {4, 5, 6}), new int[] {2, 0});
    assertTrue(dictionary instanceof DictionaryColumn);
    TsBlock block = new TsBlock(new TimeColumn(2, new long[] {1, 2}), dictionary);
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeExchangeBlockCodec.inspect(block, bounds(List.of(TSDataType.INT32))));
    TsBlock untypedNull = new TsBlock(new TimeColumn(1, new long[] {1}), new NullColumn(1));
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeExchangeBlockCodec.inspect(untypedNull, bounds(List.of(TSDataType.INT32))));
  }

  @Test
  public void nullableTimeIsRejectedOnBothSides() throws Exception {
    TsBlock block =
        new TsBlock(
            1,
            new LongColumn(1, Optional.of(new boolean[] {true}), new long[] {0}),
            new IntColumn(1, Optional.empty(), new int[] {1}));
    Bounds bounds = bounds(List.of(TSDataType.INT32));
    assertThrows(
        IllegalArgumentException.class, () -> RuntimeExchangeBlockCodec.inspect(block, bounds));
    ByteBuffer wire = SERDE.serialize(block);
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeExchangeBlockCodec.validateEncoded(wire, bounds));
  }

  @Test
  public void retainedLogicalSerializedRowsAndScratchLimitsAreEnforced() throws Exception {
    TsBlock block = allTypes(9, true);
    Bounds normal = bounds(ALL_TYPES);
    EncodingPlan plan = RuntimeExchangeBlockCodec.inspect(block, normal);
    ByteBuffer encoded = SERDE.serialize(block);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RuntimeExchangeBlockCodec.inspect(
                block,
                new Bounds(
                    1024,
                    16,
                    plan.retainedBytes() - 1,
                    1 << 20,
                    1 << 20,
                    1 << 20,
                    1 << 17,
                    ALL_TYPES)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            RuntimeExchangeBlockCodec.inspect(
                block,
                new Bounds(
                    1024,
                    16,
                    1 << 20,
                    plan.logicalSizeBytes() - 1,
                    1 << 20,
                    1 << 20,
                    1 << 17,
                    ALL_TYPES)));
    Bounds shortWire =
        new Bounds(
            1024, 16, 1 << 20, 1 << 20, plan.encodedLength() - 1, 1 << 20, 1 << 17, ALL_TYPES);
    assertThrows(
        IllegalArgumentException.class, () -> RuntimeExchangeBlockCodec.inspect(block, shortWire));
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeExchangeBlockCodec.validateEncoded(encoded, shortWire));
    Bounds fewRows = new Bounds(8, 16, 1 << 20, 1 << 20, 1 << 20, 1 << 20, 1 << 17, ALL_TYPES);
    assertThrows(
        IllegalArgumentException.class, () -> RuntimeExchangeBlockCodec.inspect(block, fewRows));
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeExchangeBlockCodec.validateEncoded(encoded, fewRows));
    Bounds scratch = new Bounds(1024, 16, 1 << 20, 1 << 20, 1 << 20, 1 << 20, 1, ALL_TYPES);
    assertThrows(
        IllegalArgumentException.class, () -> RuntimeExchangeBlockCodec.inspect(block, scratch));
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeExchangeBlockCodec.validateEncoded(encoded, scratch));
  }

  @Test
  public void decodedAllocationIsAdmittedBeforeSerdeAndExactLimitIsAccepted() throws Exception {
    TsBlock block = allTypes(9, true);
    ByteBuffer wire = SERDE.serialize(block);
    DecodingPlan plan = RuntimeExchangeBlockCodec.validateEncoded(wire, bounds(ALL_TYPES));
    Bounds shortDecoded =
        new Bounds(
            1024,
            16,
            1 << 20,
            1 << 20,
            1 << 20,
            plan.decodedRetainedUpperBound() - 1,
            1 << 17,
            ALL_TYPES);
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeExchangeBlockCodec.validateEncoded(wire, shortDecoded));
    Bounds exact =
        new Bounds(
            1024,
            16,
            1 << 20,
            1 << 20,
            1 << 20,
            plan.decodedRetainedUpperBound(),
            1 << 17,
            ALL_TYPES);
    RuntimeExchangeBlockCodec.validateEncoded(wire, exact);
    TsBlock decoded = SERDE.deserialize(wire.duplicate());
    assertTrue(
        RuntimeCompactOutputOperator.measureInputRetainedBytes(decoded)
            <= plan.decodedRetainedUpperBound());
  }

  @Test
  public void tinyRegionStillPaysForWholeRetainedBacking() {
    TsBlock region = allTypes(1024, false).getRegion(100, 1);
    EncodingPlan plan = RuntimeExchangeBlockCodec.inspect(region, bounds(ALL_TYPES));
    assertTrue(plan.retainedBytes() > 1024);
    Bounds smallRetained =
        new Bounds(1024, 16, 1024, 1 << 20, 1 << 20, 1 << 20, 1 << 17, ALL_TYPES);
    assertThrows(
        IllegalArgumentException.class,
        () -> RuntimeExchangeBlockCodec.inspect(region, smallRetained));
  }

  @Test
  public void boundConstructionRejectsInvalidTypesDimensionsAndOverflow() {
    assertThrows(IllegalArgumentException.class, () -> new Bounds(0, 1, 1, 1, 1, 1, 1));
    assertThrows(IllegalArgumentException.class, () -> new Bounds(1, -1, 1, 1, 1, 1, 1));
    assertThrows(IllegalArgumentException.class, () -> new Bounds(1, 1, -1, 1, 1, 1, 1));
    assertThrows(
        IllegalArgumentException.class, () -> new Bounds(1, 1, 1, Integer.MAX_VALUE, 16, 16, 16));
    assertThrows(
        IllegalArgumentException.class, () -> new Bounds(1, 1, Long.MAX_VALUE, 16, 16, 16, 16));
    assertThrows(
        IllegalArgumentException.class, () -> new Bounds(1, 1, 16, 16, 16, 16, Long.MAX_VALUE));
    for (TSDataType unsupported :
        List.of(TSDataType.UNKNOWN, TSDataType.VECTOR, TSDataType.OBJECT)) {
      assertThrows(IllegalArgumentException.class, () -> bounds(List.of(unsupported)));
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> new Bounds(1, 0, 16, 16, 16, 16, 16).withSchema(List.of(TSDataType.INT32)));
  }

  @Test
  public void envelopeFormulasKeepInputAndOutputOwnershipDistinct() {
    Bounds bounds = new Bounds(1, 1, 100, 200, 300, 400, 50);
    assertEquals(1050, bounds.outputEnvelopeBytes());
    assertEquals(650, bounds.inputEnvelopeBytes());
    Bounds copyDominates = new Bounds(1, 1, 1000, 1, 1, 1, 1);
    assertEquals(5256, copyDominates.outputEnvelopeBytes());
  }

  private static Bounds bounds(List<TSDataType> schema) {
    return new Bounds(1024, 16, 1 << 20, 1 << 20, 1 << 20, 1 << 20, 1 << 17).withSchema(schema);
  }

  private static TsBlock oneInt() {
    return new TsBlock(
        new TimeColumn(1, new long[] {1}), new IntColumn(1, Optional.empty(), new int[] {4}));
  }

  private static TsBlock oneBinary() {
    return new TsBlock(
        new TimeColumn(1, new long[] {1}),
        new BinaryColumn(1, Optional.empty(), new Binary[] {new Binary(new byte[] {1, 2, 3})}));
  }

  private static ByteBuffer copy(ByteBuffer source) {
    ByteBuffer result = ByteBuffer.allocate(source.remaining());
    result.put(source.duplicate()).flip();
    return result;
  }

  private static TsBlock allTypes(int rows, boolean nullable) {
    long[] times = new long[rows];
    boolean[] nulls = new boolean[rows];
    boolean[] booleans = new boolean[rows];
    int[] ints = new int[rows];
    long[] longs = new long[rows];
    float[] floats = new float[rows];
    double[] doubles = new double[rows];
    Binary[] binaries = new Binary[rows];
    for (int row = 0; row < rows; row++) {
      times[row] = row + 1;
      nulls[row] = nullable && row % 3 == 1;
      booleans[row] = row % 2 == 0;
      ints[row] = row - 3;
      longs[row] = row * 1000000000L;
      floats[row] = row + 0.25F;
      doubles[row] = row + 0.125;
      binaries[row] = new Binary(new byte[row % 5]);
    }
    Optional<boolean[]> mask = nullable ? Optional.of(nulls) : Optional.empty();
    return new TsBlock(
        new TimeColumn(rows, times),
        new BooleanColumn(rows, mask, booleans),
        new IntColumn(rows, mask, ints),
        new LongColumn(rows, mask, longs),
        new FloatColumn(rows, mask, floats),
        new DoubleColumn(rows, mask, doubles),
        new BinaryColumn(rows, mask, binaries));
  }

  private static void assertRoundTrip(TsBlock block, Bounds bounds) throws Exception {
    EncodingPlan encoding = RuntimeExchangeBlockCodec.inspect(block, bounds);
    ByteBuffer encoded = SERDE.serialize(block);
    assertEquals(encoding.encodedLength(), encoded.remaining());
    int originalPosition = encoded.position();
    DecodingPlan decoding = RuntimeExchangeBlockCodec.validateEncoded(encoded, bounds);
    assertEquals(originalPosition, encoded.position());
    TsBlock decoded = SERDE.deserialize(encoded.duplicate());
    assertEquals(block.getPositionCount(), decoded.getPositionCount());
    assertTrue(
        RuntimeCompactOutputOperator.measureInputRetainedBytes(decoded)
            <= decoding.decodedRetainedUpperBound());
    for (int row = 0; row < block.getPositionCount(); row++) {
      assertEquals(block.getTimeByIndex(row), decoded.getTimeByIndex(row));
      for (int c = 0; c < block.getValueColumnCount(); c++) {
        Column expected = block.getColumn(c);
        Column actual = decoded.getColumn(c);
        assertEquals(expected.isNull(row), actual.isNull(row));
        if (!expected.isNull(row)) {
          assertEquals(expected.getObject(row), actual.getObject(row));
        }
      }
    }
    assertTrue(encoding.peakBytes() <= bounds.outputEnvelopeBytes());
  }
}
