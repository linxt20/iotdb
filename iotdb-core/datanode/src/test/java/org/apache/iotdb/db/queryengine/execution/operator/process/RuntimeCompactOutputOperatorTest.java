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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iotdb.calc.exception.MemoryNotEnoughException;
import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.exchange.RuntimeExchangeBlockCodec;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.DictionaryColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.NullColumn;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class RuntimeCompactOutputOperatorTest {

  @Test
  public void testPlannedInt64ChunkingUsesExpandedWidthAndPreservesDuplicateRows()
      throws Exception {
    int rows = 1024;
    long[] times = new long[rows];
    for (int row = 0; row < rows; row++) times[row] = row / 2;
    Column[] values = new Column[10];
    for (int c = 0; c < values.length; c++) {
      values[c] =
          new RunLengthEncodedColumn(new IntColumn(1, Optional.empty(), new int[] {c - 5}), rows);
    }
    TsBlock input = new TsBlock(new TimeColumn(rows, times), values);
    // Native INT32 copies fit; only their correct frozen INT64 representation needs multiple
    // blocks.
    RuntimeCompactOutputOperator.copyAllRowsBounded(input, 65536, 65536);
    List<TSDataType> schema = new ArrayList<>(Collections.nCopies(values.length, TSDataType.INT64));
    Assert.assertThrows(
        MemoryNotEnoughException.class,
        () -> RuntimeCompactOutputOperator.copyAllRowsBounded(input, 65536, 65536, schema));
    SequenceOperator delegate = new SequenceOperator(input);
    RuntimeCompactOutputOperator operator =
        new RuntimeCompactOutputOperator(delegate, 65536, 65536, schema);
    // The caller cannot change the schema while a partially consumed block remains pending.
    schema.set(0, TSDataType.DOUBLE);
    List<TSDataType> frozenSchema = Collections.nCopies(values.length, TSDataType.INT64);
    int copied = 0;
    int chunks = 0;
    List<TsBlock> previousChunks = new ArrayList<>();
    while (operator.hasNext()) {
      TsBlock output = operator.next();
      Assert.assertNotNull(output);
      Assert.assertTrue(RuntimeCompactOutputOperator.measureInputRetainedBytes(output) <= 65536);
      Assert.assertTrue(output.getRetainedSizeInBytes() <= 65536);
      RuntimeExchangeBlockCodec.inspect(output, plannedChunkBounds(frozenSchema));
      for (TsBlock previous : previousChunks) {
        Assert.assertNotSame(
            previous.getTimeColumn().getLongs(), output.getTimeColumn().getLongs());
        for (int c = 0; c < values.length; c++) {
          Assert.assertNotSame(previous.getColumn(c).getLongs(), output.getColumn(c).getLongs());
        }
      }
      previousChunks.add(output);
      for (int row = 0; row < output.getPositionCount(); row++, copied++) {
        Assert.assertEquals(copied / 2, output.getTimeByIndex(row));
        for (int c = 0; c < values.length; c++) {
          Assert.assertEquals(LongColumn.class, output.getColumn(c).getClass());
          Assert.assertEquals((long) c - 5, output.getColumn(c).getLong(row));
        }
      }
      chunks++;
    }
    Assert.assertEquals(rows, copied);
    Assert.assertTrue(chunks > 1);
    Assert.assertEquals(1, delegate.nextCalls);
  }

  @Test
  public void testPlannedNullChunkingMaterializesAllWireTypesWithoutLosingDuplicateRows()
      throws Exception {
    int rows = 1024;
    List<TSDataType> schema =
        List.of(
            TSDataType.BOOLEAN,
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
            TSDataType.STRING,
            TSDataType.BLOB,
            TSDataType.DATE,
            TSDataType.TIMESTAMP);
    long[] times = new long[rows];
    for (int row = 0; row < rows; row++) times[row] = row / 2;
    Column[] values = new Column[schema.size()];
    for (int c = 0; c < values.length; c++) values[c] = new NullColumn(rows);
    TsBlock input = new TsBlock(new TimeColumn(rows, times), values);
    RuntimeCompactOutputOperator.copyAllRowsBounded(input, 65536, 65536);
    Assert.assertThrows(
        MemoryNotEnoughException.class,
        () -> RuntimeCompactOutputOperator.copyAllRowsBounded(input, 65536, 65536, schema));
    SequenceOperator delegate = new SequenceOperator(input);
    RuntimeCompactOutputOperator operator =
        new RuntimeCompactOutputOperator(delegate, 65536, 65536, schema);
    int copied = 0;
    int chunks = 0;
    List<TsBlock> previousChunks = new ArrayList<>();
    while (operator.hasNext()) {
      TsBlock output = operator.next();
      Assert.assertNotNull(output);
      Assert.assertTrue(RuntimeCompactOutputOperator.measureInputRetainedBytes(output) <= 65536);
      Assert.assertTrue(output.getRetainedSizeInBytes() <= 65536);
      RuntimeExchangeBlockCodec.inspect(output, plannedChunkBounds(schema));
      for (TsBlock previous : previousChunks) {
        Assert.assertNotSame(
            previous.getTimeColumn().getLongs(), output.getTimeColumn().getLongs());
        for (int c = 0; c < values.length; c++) {
          Assert.assertNotSame(previous.getColumn(c), output.getColumn(c));
        }
      }
      previousChunks.add(output);
      for (int row = 0; row < output.getPositionCount(); row++, copied++) {
        Assert.assertEquals(copied / 2, output.getTimeByIndex(row));
        for (int c = 0; c < values.length; c++) {
          Assert.assertFalse(output.getColumn(c) instanceof NullColumn);
          Assert.assertTrue(output.getColumn(c).isNull(row));
        }
      }
      chunks++;
    }
    Assert.assertEquals(rows, copied);
    Assert.assertTrue(chunks > 1);
    Assert.assertEquals(1, delegate.nextCalls);
  }

  private static RuntimeExchangeBlockCodec.Bounds plannedChunkBounds(List<TSDataType> schema) {
    // Keep the real 64 KiB output bound; other codec envelopes are independently generous here.
    return new RuntimeExchangeBlockCodec.Bounds(
        8192, schema.size(), 65536, 131072, 262144, 524288, 1048576, schema);
  }

  @Test
  public void testPlannedInt64CanonicalizesFoldedInt32ConstantAndKeepsIndependentBacking() {
    IntColumn literal = new IntColumn(1, Optional.empty(), new int[] {1});
    TsBlock input =
        new TsBlock(
            new TimeColumn(3, new long[] {1, 2, 3}), new RunLengthEncodedColumn(literal, 3));
    TsBlock output =
        RuntimeCompactOutputOperator.copyAllRowsBounded(
            input, 65536, 65536, List.of(TSDataType.INT64));
    Assert.assertEquals(LongColumn.class, output.getColumn(0).getClass());
    Assert.assertEquals(TSDataType.INT64, output.getColumn(0).getDataType());
    literal.getInts()[0] = 9;
    for (int row = 0; row < 3; row++) Assert.assertEquals(1L, output.getColumn(0).getLong(row));
  }

  @Test
  public void testPlannedInt64PreservesEveryInt32BoundaryAndNull() {
    int[] values = {Integer.MIN_VALUE, -1, 0, Integer.MAX_VALUE, 7};
    TsBlock input =
        new TsBlock(
            new TimeColumn(5, new long[] {1, 2, 3, 4, 5}),
            new IntColumn(
                5, Optional.of(new boolean[] {false, false, false, false, true}), values));
    TsBlock output =
        RuntimeCompactOutputOperator.copyAllRowsBounded(
            input, 65536, 65536, List.of(TSDataType.INT64));
    for (int row = 0; row < 4; row++) {
      Assert.assertEquals((long) values[row], output.getColumn(0).getLong(row));
    }
    Assert.assertTrue(output.getColumn(0).isNull(4));
    TsBlock doubleInput =
        new TsBlock(
            new TimeColumn(1, new long[] {1}),
            new org.apache.tsfile.read.common.block.column.DoubleColumn(
                1, Optional.empty(), new double[] {1.5}));
    Assert.assertThrows(
        IllegalArgumentException.class,
        () ->
            RuntimeCompactOutputOperator.copyAllRowsBounded(
                doubleInput, 65536, 65536, List.of(TSDataType.INT64)));
  }

  @Test
  public void testPlannedSchemaMaterializesUntypedNullsForEverySupportedPhysicalType() {
    List<TSDataType> schema =
        List.of(
            TSDataType.BOOLEAN,
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.DATE,
            TSDataType.TIMESTAMP,
            TSDataType.STRING,
            TSDataType.BLOB);
    Column[] values = new Column[schema.size()];
    Arrays.fill(values, new NullColumn(3));
    TsBlock input = new TsBlock(new TimeColumn(3, new long[] {1, 2, 3}), values);
    TsBlock output = RuntimeCompactOutputOperator.copyAllRowsBounded(input, 65536, 65536, schema);
    for (int c = 0; c < schema.size(); c++) {
      Assert.assertFalse(output.getColumn(c) instanceof NullColumn);
      for (int row = 0; row < 3; row++) Assert.assertTrue(output.getColumn(c).isNull(row));
    }
  }

  @Test
  public void testPlannedSchemaRejectsNarrowingUnrelatedTypesAndWrongWidth() {
    TsBlock input =
        new TsBlock(
            new TimeColumn(1, new long[] {1}),
            new LongColumn(1, Optional.empty(), new long[] {Long.MAX_VALUE}));
    for (List<TSDataType> schema :
        List.of(List.of(TSDataType.INT32), List.of(TSDataType.STRING), List.<TSDataType>of())) {
      Assert.assertThrows(
          IllegalArgumentException.class,
          () -> RuntimeCompactOutputOperator.copyAllRowsBounded(input, 65536, 65536, schema));
    }
  }

  @Test
  public void testPlannedWidenedArrayIsChargedBeforeOutputAllocation() {
    int rows = 1024;
    TsBlock input =
        new TsBlock(
            new TimeColumn(rows, new long[rows]),
            new RunLengthEncodedColumn(new IntColumn(1, Optional.empty(), new int[] {1}), rows));
    TsBlock nativeCopy = RuntimeCompactOutputOperator.copyAllRowsBounded(input, 65536, 65536);
    long oldWidthBound = RuntimeCompactOutputOperator.measureInputRetainedBytes(nativeCopy);
    Assert.assertThrows(
        MemoryNotEnoughException.class,
        () ->
            RuntimeCompactOutputOperator.copyAllRowsBounded(
                input, 65536, oldWidthBound, List.of(TSDataType.INT64)));
    TsBlock widened =
        RuntimeCompactOutputOperator.copyAllRowsBounded(
            input, 65536, 65536, List.of(TSDataType.INT64));
    long exact = RuntimeCompactOutputOperator.measureInputRetainedBytes(widened);
    Assert.assertEquals(
        exact,
        RuntimeCompactOutputOperator.measureInputRetainedBytes(
            RuntimeCompactOutputOperator.copyAllRowsBounded(
                input, 65536, exact, List.of(TSDataType.INT64))));
    Assert.assertThrows(
        MemoryNotEnoughException.class,
        () ->
            RuntimeCompactOutputOperator.copyAllRowsBounded(
                input, 65536, exact - 1, List.of(TSDataType.INT64)));
  }

  @Test
  public void testNativeWholeBlockCopyFlattensPartitionViewWithIndependentBacking() {
    TsBlock original = allTypesBlock(12);
    int[] ids = {11, 5, 11, 2};
    Column[] columns = new Column[original.getValueColumnCount()];
    for (int c = 0; c < columns.length; c++) {
      columns[c] = DictionaryColumn.create(ids.length, original.getColumn(c), ids);
    }
    TsBlock partition =
        new TsBlock(
            ids.length,
            DictionaryColumn.create(ids.length, original.getTimeColumn(), ids),
            columns);
    List<String> expected = rows(partition);
    TsBlock copy = RuntimeCompactOutputOperator.copyAllRowsBounded(partition, 65536, 65536);
    Assert.assertEquals(expected, rows(copy));
    for (Column column : copy.getValueColumns()) {
      Assert.assertFalse(column instanceof DictionaryColumn);
      Assert.assertFalse(column instanceof RunLengthEncodedColumn);
    }
    ids[0] = 0;
    original.getTimeColumn().getLongs()[11] = -99;
    Assert.assertEquals(expected, rows(copy));
  }

  @Test
  public void testNativeWholeBlockCopyRejectsInputBackingAndOutputBounds() {
    TsBlock input = allTypesBlock(3);
    Assert.assertThrows(
        MemoryNotEnoughException.class,
        () -> RuntimeCompactOutputOperator.copyAllRowsBounded(input, 1, 65536));
    Assert.assertThrows(
        MemoryNotEnoughException.class,
        () -> RuntimeCompactOutputOperator.copyAllRowsBounded(input, 65536, 1));
  }

  @Test
  public void testTypeServiceCopyPreservesExactBoundaryAcrossLogicalAliasesAndNulls() {
    TsBlock input = allTypesBlock(4);
    long inputBytes = RuntimeCompactOutputOperator.measureInputRetainedBytes(input);
    TsBlock reference = RuntimeCompactOutputOperator.copyAllRowsBounded(input, inputBytes, 65536);
    long exactOutputBytes = RuntimeCompactOutputOperator.measureInputRetainedBytes(reference);

    TsBlock exact =
        RuntimeCompactOutputOperator.copyAllRowsBounded(input, inputBytes, exactOutputBytes);
    Assert.assertEquals(rows(input), rows(exact));
    Assert.assertEquals(
        exactOutputBytes, RuntimeCompactOutputOperator.measureInputRetainedBytes(exact));
    for (int c = 0; c < input.getValueColumnCount(); c++) {
      Assert.assertEquals(input.getColumn(c).getDataType(), exact.getColumn(c).getDataType());
    }
    Assert.assertThrows(
        MemoryNotEnoughException.class,
        () ->
            RuntimeCompactOutputOperator.copyAllRowsBounded(
                input, inputBytes, exactOutputBytes - 1));
  }

  @Test
  public void testMultipleCompactBlocksPreserveAllTypesNullsAndOrder() throws Exception {
    TsBlock input = allTypesBlock(37);
    List<String> expected = rows(input);
    SequenceOperator delegate = new SequenceOperator(input);
    RuntimeCompactOutputOperator operator =
        new RuntimeCompactOutputOperator(
            delegate, 2048, RuntimeCompactOutputOperator.measureInputRetainedBytes(input));
    List<String> actual = new ArrayList<>();
    int chunks = 0;
    while (operator.hasNext()) {
      TsBlock output = operator.next();
      Assert.assertNotNull(output);
      Assert.assertTrue(output.getRetainedSizeInBytes() <= 2048);
      Assert.assertTrue(RuntimeCompactOutputOperator.measureInputRetainedBytes(output) <= 2048);
      Assert.assertTrue(output.getPositionCount() > 0);
      for (int c = 0; c < input.getValueColumnCount(); c++) {
        Assert.assertEquals(input.getColumn(c).getDataType(), output.getColumn(c).getDataType());
      }
      actual.addAll(rows(output));
      chunks++;
    }
    Assert.assertTrue(chunks > 1);
    Assert.assertEquals(expected, actual);
    Assert.assertEquals(multiset(expected), multiset(actual));
    Assert.assertTrue(operator.isFinished());
    Assert.assertEquals(1, delegate.nextCalls);
    Assert.assertSame(delegate.context, operator.getOperatorContext());
    operator.close();
  }

  @Test
  public void testEveryPrimitiveAndBinaryHasIndependentBacking() throws Exception {
    TsBlock input = allTypesBlock(3);
    SequenceOperator delegate = new SequenceOperator(input);
    RuntimeCompactOutputOperator operator =
        new RuntimeCompactOutputOperator(delegate, 65536, 65536);
    TsBlock output = operator.next();
    List<String> expected = rows(output);

    Assert.assertNotSame(input.getTimeColumn().getLongs(), output.getTimeColumn().getLongs());
    input.getTimeColumn().getLongs()[0] = -999;
    for (int c = 0; c < input.getValueColumnCount(); c++) {
      Column original = input.getColumn(c);
      Column copy = output.getColumn(c);
      Assert.assertNotSame(original, copy);
      Assert.assertFalse(copy.isNull(0));
      if (original.mayHaveNull()) {
        Assert.assertNotSame(original.isNull(), copy.isNull());
        original.isNull()[0] = true;
      }
      switch (original.getDataType()) {
        case BOOLEAN:
          Assert.assertNotSame(original.getBooleans(), copy.getBooleans());
          original.getBooleans()[0] = !copy.getBoolean(0);
          break;
        case INT32:
        case DATE:
          Assert.assertNotSame(original.getInts(), copy.getInts());
          original.getInts()[0] = -999;
          break;
        case INT64:
        case TIMESTAMP:
          Assert.assertNotSame(original.getLongs(), copy.getLongs());
          original.getLongs()[0] = -999;
          break;
        case FLOAT:
          Assert.assertNotSame(original.getFloats(), copy.getFloats());
          original.getFloats()[0] = -999;
          break;
        case DOUBLE:
          Assert.assertNotSame(original.getDoubles(), copy.getDoubles());
          original.getDoubles()[0] = -999;
          break;
        case TEXT:
        case STRING:
        case BLOB:
          Assert.assertNotSame(original.getBinaries(), copy.getBinaries());
          Binary before = original.getBinaries()[0];
          Binary after = copy.getBinary(0);
          Assert.assertNotSame(before, after);
          Assert.assertNotSame(before.getValues(), after.getValues());
          before.getValues()[0] ^= 0x7f;
          break;
        default:
          Assert.fail("The fixture must use supported physical types");
      }
    }
    Assert.assertEquals(expected, rows(output));
    operator.close();
  }

  @Test
  public void testRegionWithLargeBackingCannotBypassInputEnvelope() throws Exception {
    TsBlock whole = longBlock(1024);
    TsBlock region = whole.getRegion(321, 2);
    long reported = region.getRetainedSizeInBytes();
    long actual = RuntimeCompactOutputOperator.measureInputRetainedBytes(region);
    Assert.assertTrue(actual > reported);
    RuntimeCompactOutputOperator operator =
        new RuntimeCompactOutputOperator(new SequenceOperator(region), 1024, reported);
    try {
      operator.next();
      Assert.fail("The complete region backing must be charged");
    } catch (MemoryNotEnoughException expected) {
      // Rejected input is never installed as the wrapper's pending block.
    } finally {
      operator.close();
    }
  }

  @Test
  public void testRegionOutputDoesNotRetainOriginalArrays() throws Exception {
    TsBlock whole = longBlock(1024);
    TsBlock region = whole.getRegion(321, 2);
    RuntimeCompactOutputOperator operator =
        new RuntimeCompactOutputOperator(
            new SequenceOperator(region),
            1024,
            RuntimeCompactOutputOperator.measureInputRetainedBytes(region));
    TsBlock output = operator.next();
    Assert.assertEquals(rows(region), rows(output));
    Assert.assertEquals(2, output.getTimeColumn().getLongs().length);
    Assert.assertEquals(2, output.getColumn(0).getLongs().length);
    Assert.assertNotSame(whole.getTimeColumn().getLongs(), output.getTimeColumn().getLongs());
    Assert.assertNotSame(whole.getColumn(0).getLongs(), output.getColumn(0).getLongs());
    whole.getTimeColumn().getLongs()[321] = -123;
    whole.getColumn(0).getLongs()[321] = -456;
    Assert.assertEquals(321, output.getTimeByIndex(0));
    Assert.assertEquals(3210, output.getColumn(0).getLong(0));
    operator.close();
  }

  @Test
  public void testDictionaryAndRleAreMeasuredAndCopiedToPlainColumns() throws Exception {
    Binary[] values = {
      new Binary("first", StandardCharsets.UTF_8),
      new Binary("second", StandardCharsets.UTF_8),
      new Binary("third", StandardCharsets.UTF_8)
    };
    Column dictionary = new BinaryColumn(3, Optional.empty(), values);
    int[] ids = new int[512];
    for (int i = 0; i < ids.length; i++) {
      ids[i] = i % 3;
    }
    Column encoded = DictionaryColumn.create(9, dictionary, ids);
    Column repeated =
        new RunLengthEncodedColumn(new LongColumn(1, Optional.empty(), new long[] {42}), 9);
    Column time = new RunLengthEncodedColumn(new TimeColumn(1, new long[] {123}), 9);
    TsBlock input = new TsBlock(9, time, encoded, repeated);
    long retained = RuntimeCompactOutputOperator.measureInputRetainedBytes(input);
    Assert.assertTrue(retained >= ids.length * Integer.BYTES);
    RuntimeCompactOutputOperator operator =
        new RuntimeCompactOutputOperator(new SequenceOperator(input), 1024, retained);
    List<String> actual = new ArrayList<>();
    while (operator.hasNext()) {
      TsBlock output = operator.next();
      Assert.assertFalse(output.getTimeColumn() instanceof RunLengthEncodedColumn);
      Assert.assertFalse(output.getColumn(0) instanceof DictionaryColumn);
      Assert.assertFalse(output.getColumn(1) instanceof RunLengthEncodedColumn);
      Assert.assertTrue(RuntimeCompactOutputOperator.measureInputRetainedBytes(output) <= 1024);
      actual.addAll(rows(output));
    }
    Assert.assertEquals(rows(input), actual);
    operator.close();
  }

  @Test
  public void testUntypedNullColumnIsPreservedWithoutSharedState() throws Exception {
    TsBlock input = new TsBlock(4, new TimeColumn(4, new long[] {1, 2, 3, 4}), new NullColumn(4));
    RuntimeCompactOutputOperator operator =
        new RuntimeCompactOutputOperator(new SequenceOperator(input), 1024, 1024);
    TsBlock output = operator.next();
    Assert.assertEquals(rows(input), rows(output));
    Assert.assertNotSame(input.getColumn(0), output.getColumn(0));
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(output.getColumn(0).isNull(i));
    }
    operator.close();
  }

  @Test
  public void testOversizedSingleValueFailsWithoutGrowingOutputBudget() throws Exception {
    TsBlockBuilder builder = new TsBlockBuilder(1, Collections.singletonList(TSDataType.TEXT));
    builder.getTimeColumnBuilder().writeLong(1);
    builder.getColumnBuilder(0).writeBinary(new Binary(new byte[8192]));
    builder.declarePosition();
    TsBlock input = builder.build();
    RuntimeCompactOutputOperator operator =
        new RuntimeCompactOutputOperator(
            new SequenceOperator(input),
            512,
            RuntimeCompactOutputOperator.measureInputRetainedBytes(input));
    try {
      operator.next();
      Assert.fail("A row cannot silently expand its output credit");
    } catch (MemoryNotEnoughException expected) {
      Assert.assertEquals(512, operator.calculateMaxReturnSize());
    } finally {
      operator.close();
    }
  }

  @Test
  public void testInputEnvelopeRejectsWholeBlockBeforeCopy() throws Exception {
    TsBlock input = longBlock(32);
    long retained = RuntimeCompactOutputOperator.measureInputRetainedBytes(input);
    SequenceOperator delegate = new SequenceOperator(input);
    RuntimeCompactOutputOperator operator =
        new RuntimeCompactOutputOperator(delegate, 512, retained - 1);
    try {
      operator.next();
      Assert.fail("An oversized delegate block must fail explicitly");
    } catch (MemoryNotEnoughException expected) {
      Assert.assertEquals(1, delegate.nextCalls);
    } finally {
      operator.close();
    }
  }

  @Test
  public void testUnknownColumnImplementationIsRejected() throws Exception {
    Column unknown = Mockito.mock(Column.class);
    TsBlock input = new TsBlock(1, new TimeColumn(1, new long[] {1}), unknown);
    RuntimeCompactOutputOperator operator =
        new RuntimeCompactOutputOperator(new SequenceOperator(input), 1024, 1024);
    try {
      operator.next();
      Assert.fail("Unknown backing ownership must not fall back to a payload estimate");
    } catch (MemoryNotEnoughException expected) {
      Mockito.verify(unknown, Mockito.never()).getDataType();
    } finally {
      operator.close();
    }
  }

  @Test
  public void testPendingRowsOverrideDelegateBlockedAndFinishedState() throws Exception {
    TsBlock input = longBlock(64);
    SequenceOperator delegate = new SequenceOperator(input);
    SettableFuture<Void> blocked = SettableFuture.create();
    delegate.blocked = blocked;
    RuntimeCompactOutputOperator operator = new RuntimeCompactOutputOperator(delegate, 384, 65536);
    Assert.assertFalse(operator.isBlocked().isDone());
    Assert.assertNull(operator.next());
    Assert.assertEquals(0, delegate.nextCalls);
    blocked.set(null);

    TsBlock first = operator.next();
    Assert.assertNotNull(first);
    Assert.assertTrue(first.getPositionCount() < input.getPositionCount());
    Assert.assertTrue(delegate.isFinished());
    delegate.blocked = SettableFuture.create();
    Assert.assertTrue(operator.hasNext());
    Assert.assertFalse(operator.isFinished());
    Assert.assertTrue(operator.isBlocked().isDone());
    int count = first.getPositionCount();
    while (operator.hasNext()) {
      count += operator.next().getPositionCount();
    }
    Assert.assertEquals(64, count);
    Assert.assertTrue(operator.isFinished());
    operator.close();
  }

  @Test
  public void testBlockedFutureCancellationDoesNotCancelDelegate() throws Exception {
    SequenceOperator delegate = new SequenceOperator(longBlock(1));
    SettableFuture<Void> blocked = SettableFuture.create();
    delegate.blocked = blocked;
    RuntimeCompactOutputOperator operator = new RuntimeCompactOutputOperator(delegate, 1024, 1024);
    operator.isBlocked().cancel(false);
    Assert.assertFalse(blocked.isCancelled());
    Assert.assertFalse(operator.isBlocked().isDone());
    operator.close();
  }

  @Test
  public void testEmptyAndUnavailableBlocksDoNotLoseLaterRows() throws Exception {
    SequenceOperator delegate = new SequenceOperator(null, longBlock(0), longBlock(3));
    RuntimeCompactOutputOperator operator = new RuntimeCompactOutputOperator(delegate, 1024, 1024);
    Assert.assertNull(operator.next());
    Assert.assertFalse(operator.isFinished());
    Assert.assertNull(operator.next());
    Assert.assertFalse(operator.isFinished());
    Assert.assertEquals(3, operator.next().getPositionCount());
    Assert.assertTrue(operator.isFinished());
    operator.close();
  }

  @Test
  public void testCloseReleasesPendingInputAndClosesDelegateOnceEvenIfCloseThrows()
      throws Exception {
    SequenceOperator delegate = new SequenceOperator(longBlock(64));
    RuntimeCompactOutputOperator operator = new RuntimeCompactOutputOperator(delegate, 384, 65536);
    operator.next();
    long retainedBefore = operator.ramBytesUsed();
    Exception failure = new Exception("close failure");
    delegate.closeFailure = failure;
    try {
      operator.close();
      Assert.fail("The delegate close failure must remain visible");
    } catch (Exception expected) {
      Assert.assertSame(failure, expected);
    }
    operator.close();
    Assert.assertEquals(1, delegate.closeCalls);
    Assert.assertTrue(operator.ramBytesUsed() < retainedBefore);
    Assert.assertFalse(operator.hasNext());
    Assert.assertTrue(operator.isFinished());
    Assert.assertTrue(operator.isBlocked().isDone());
    Assert.assertNull(operator.next());
  }

  @Test
  public void testMemoryEstimatesChargeInputWorkspaceAndEveryLane() throws Exception {
    SequenceOperator delegate = new SequenceOperator(longBlock(64));
    RuntimeCompactOutputOperator operator = new RuntimeCompactOutputOperator(delegate, 512, 4096);
    long workspace = RuntimeCompactOutputOperator.estimateWorkspaceUpperBound(512);
    Assert.assertEquals(workspace, operator.getWorkspaceUpperBoundBytes());
    Assert.assertEquals(1280, workspace);
    Assert.assertEquals(512, operator.calculateMaxReturnSize());
    Assert.assertTrue(
        operator.calculateMaxPeekMemory() >= 4096 + workspace + delegate.retainedBytes);
    Assert.assertTrue(
        operator.calculateMaxPeekMemory() <= 4096 + workspace + delegate.retainedBytes + 1024);
    Assert.assertTrue(
        operator.calculateRetainedSizeAfterCallingNext() >= 4096 + delegate.retainedBytes);

    RuntimeCompactOutputOperator secondLane =
        new RuntimeCompactOutputOperator(new SequenceOperator(longBlock(1)), 512, 4096);
    Assert.assertEquals(operator.calculateMaxPeekMemory(), secondLane.calculateMaxPeekMemory());
    Assert.assertTrue(
        operator.calculateMaxPeekMemory() + secondLane.calculateMaxPeekMemory()
            >= 2 * (4096 + workspace + delegate.retainedBytes));
    operator.close();
    secondLane.close();
  }

  @Test
  public void testRejectsNonpositiveLimits() {
    for (long[] limits : new long[][] {{0, 1}, {1, 0}, {-1, 1}, {1, -1}}) {
      try {
        new RuntimeCompactOutputOperator(new SequenceOperator(), limits[0], limits[1]);
        Assert.fail("Both envelopes must be positive");
      } catch (IllegalArgumentException expected) {
        // Invalid admission sizes cannot reach the copying path.
      }
    }
  }

  private static TsBlock allTypesBlock(int count) {
    List<TSDataType> types =
        Arrays.asList(
            TSDataType.BOOLEAN,
            TSDataType.INT32,
            TSDataType.INT64,
            TSDataType.FLOAT,
            TSDataType.DOUBLE,
            TSDataType.TEXT,
            TSDataType.STRING,
            TSDataType.BLOB,
            TSDataType.DATE,
            TSDataType.TIMESTAMP);
    TsBlockBuilder builder = new TsBlockBuilder(Math.max(1, count), types);
    for (int row = 0; row < count; row++) {
      builder.getTimeColumnBuilder().writeLong(row * 1000L);
      for (int c = 0; c < types.size(); c++) {
        if (row % 5 == 1) {
          builder.getColumnBuilder(c).appendNull();
          continue;
        }
        switch (types.get(c)) {
          case BOOLEAN:
            builder.getColumnBuilder(c).writeBoolean(row % 2 == 0);
            break;
          case INT32:
            builder.getColumnBuilder(c).writeInt(row - 11);
            break;
          case DATE:
            builder.getColumnBuilder(c).writeInt(20261101 + row % 20);
            break;
          case INT64:
          case TIMESTAMP:
            builder.getColumnBuilder(c).writeLong(10000000000L + row);
            break;
          case FLOAT:
            builder.getColumnBuilder(c).writeFloat(row + 0.25f);
            break;
          case DOUBLE:
            builder.getColumnBuilder(c).writeDouble(row + 0.125d);
            break;
          case TEXT:
          case STRING:
          case BLOB:
            builder
                .getColumnBuilder(c)
                .writeBinary(new Binary("value-" + row + "-column-" + c, StandardCharsets.UTF_8));
            break;
          default:
            throw new AssertionError();
        }
      }
      builder.declarePosition();
    }
    return builder.build();
  }

  private static TsBlock longBlock(int count) {
    TsBlockBuilder builder =
        new TsBlockBuilder(Math.max(1, count), Collections.singletonList(TSDataType.INT64));
    for (int row = 0; row < count; row++) {
      builder.getTimeColumnBuilder().writeLong(row);
      builder.getColumnBuilder(0).writeLong(row * 10L);
      builder.declarePosition();
    }
    return builder.build();
  }

  private static List<String> rows(TsBlock block) {
    List<String> result = new ArrayList<>();
    for (int row = 0; row < block.getPositionCount(); row++) {
      StringBuilder text = new StringBuilder().append(block.getTimeByIndex(row));
      for (int c = 0; c < block.getValueColumnCount(); c++) {
        Column column = block.getColumn(c);
        text.append('|');
        if (column.isNull(row)) {
          text.append("NULL");
        } else {
          TSDataType type = column.getDataType();
          text.append(type).append(':');
          if (type.isTextStringOrBlob()) {
            text.append(Arrays.toString(column.getBinary(row).getValues()));
          } else {
            text.append(column.getObject(row));
          }
        }
      }
      result.add(text.toString());
    }
    return result;
  }

  private static Map<String, Integer> multiset(List<String> rows) {
    Map<String, Integer> result = new HashMap<>();
    for (String row : rows) {
      result.merge(row, 1, Integer::sum);
    }
    return result;
  }

  private static final class SequenceOperator implements Operator {
    private final CommonOperatorContext context = Mockito.mock(CommonOperatorContext.class);
    private final List<TsBlock> blocks;
    private final long retainedBytes = 200;
    private ListenableFuture<?> blocked = NOT_BLOCKED;
    private int position;
    private int nextCalls;
    private int closeCalls;
    private boolean closed;
    private Exception closeFailure;

    private SequenceOperator(TsBlock... blocks) {
      this.blocks = Arrays.asList(blocks);
    }

    @Override
    public CommonOperatorContext getOperatorContext() {
      return context;
    }

    @Override
    public ListenableFuture<?> isBlocked() {
      return blocked;
    }

    @Override
    public TsBlock next() {
      nextCalls++;
      return closed || position >= blocks.size() ? null : blocks.get(position++);
    }

    @Override
    public boolean hasNext() {
      return !closed && position < blocks.size();
    }

    @Override
    public boolean isFinished() {
      return closed || position == blocks.size();
    }

    @Override
    public void close() throws Exception {
      closeCalls++;
      closed = true;
      if (closeFailure != null) {
        throw closeFailure;
      }
    }

    @Override
    public long calculateMaxPeekMemory() {
      return 1000;
    }

    @Override
    public long calculateMaxReturnSize() {
      return 1000;
    }

    @Override
    public long calculateRetainedSizeAfterCallingNext() {
      return retainedBytes;
    }

    @Override
    public long ramBytesUsed() {
      return 128;
    }
  }
}
