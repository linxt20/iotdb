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

package org.apache.iotdb.calc.execution.relational;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeEnum;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * The contract that keeps the router and the hash join from disagreeing.
 *
 * <p>The load-bearing assertion is {@link
 * #routingHashAgreesWithTheCanonicalValueHashForEveryType()}. It is the reason a distributed join
 * does not silently drop matches, so it is written as a direct comparison rather than as a pair of
 * independently computed expectations.
 */
public class HashKeyCanonicalizerTest {

  private static final Type INT32 = TypeFactory.getType(TSDataType.INT32);
  private static final Type INT64 = TypeFactory.getType(TSDataType.INT64);
  private static final Type FLOAT = TypeFactory.getType(TSDataType.FLOAT);
  private static final Type DOUBLE = TypeFactory.getType(TSDataType.DOUBLE);
  private static final Type BOOLEAN = TypeFactory.getType(TSDataType.BOOLEAN);
  private static final Type TEXT = TypeFactory.getType(TSDataType.TEXT);

  /**
   * Signed zero is the regression this class exists for. The baseline hashed the raw physical
   * object, which put {@code -0.0} and {@code +0.0} in different buckets for 60 of the 63 bucket
   * counts from 2 to 64 while the join executor treated them as equal.
   */
  @Test
  public void signedZeroRoutesLikeTheValueItEquals() {
    TsBlock block = buildFloats(-0.0F, 0.0F, 1.5F);
    assertEquals(
        HashKeyCanonicalizer.routingHash(block.getColumn(0), 0, FLOAT),
        HashKeyCanonicalizer.routingHash(block.getColumn(0), 1, FLOAT));

    TsBlock doubles = buildDoubles(-0.0D, 0.0D);
    assertEquals(
        HashKeyCanonicalizer.routingHash(doubles.getColumn(0), 0, DOUBLE),
        HashKeyCanonicalizer.routingHash(doubles.getColumn(0), 1, DOUBLE));
  }

  /** Every bucket count from 2 to 64 must agree, not just the one the old probe happened to hit. */
  @Test
  public void signedZeroAgreesAtEveryBucketCountFromTwoToSixtyFour() {
    TsBlock block = buildFloats(-0.0F, 0.0F);
    for (int bucketCount = 2; bucketCount <= 64; bucketCount++) {
      int left =
          Math.floorMod(
              HashKeyCanonicalizer.routingHash(block.getColumn(0), 0, FLOAT), bucketCount);
      int right =
          Math.floorMod(
              HashKeyCanonicalizer.routingHash(block.getColumn(0), 1, FLOAT), bucketCount);
      assertEquals("bucket count " + bucketCount + " split signed zero", left, right);
    }
  }

  @Test
  public void signedZeroIsOneCanonicalValue() {
    TsBlock block = buildFloats(-0.0F, 0.0F);
    assertEquals(
        HashKeyCanonicalizer.canonicalValue(block.getColumn(0), 0, FLOAT),
        HashKeyCanonicalizer.canonicalValue(block.getColumn(0), 1, FLOAT));
  }

  /**
   * The identity the two consumers rely on. For a key with no null and no NaN component, the router
   * must choose the same bucket the join's own key hash chooses. Written as one comparison so a
   * future change to either side fails here rather than in an end-to-end run.
   */
  @Test
  public void routingHashEqualsTheJoinKeyHashForPlainValues() {
    // Negative values on purpose: the fold identity has to hold for them too, and the negative
    // integer case is where Integer.hashCode and Long.hashCode stop agreeing with each other.
    Object[] key = new Object[] {-7, -5L, Boolean.FALSE, binary("k")};
    Type[] types = new Type[] {INT32, INT64, BOOLEAN, TEXT};
    TsBlock block = buildRow(key, types);

    int[] components = new int[types.length];
    for (int index = 0; index < types.length; index++) {
      components[index] = HashKeyCanonicalizer.routingHash(block.getColumn(index), 0, types[index]);
    }

    // The join hashes its Object[] key with Arrays.hashCode, which folds the same way.
    assertEquals(Arrays.hashCode(key), HashKeyCanonicalizer.foldComponentHashes(components));
  }

  /**
   * The invariant that keeps a distributed join correct. Two rows the executor calls equal must
   * land in the same bucket, so each component's routing hash has to agree with the hash of the
   * value the executor compares. This is the assertion the whole class exists for.
   */
  @Test
  public void routingHashAgreesWithTheCanonicalValueHashForEveryType() {
    Object[] values = new Object[] {-7, -5L, -0.0F, -0.0D, Boolean.FALSE, binary("k")};
    Type[] types = new Type[] {INT32, INT64, FLOAT, DOUBLE, BOOLEAN, TEXT};
    TsBlock block = buildRow(values, types);
    for (int index = 0; index < types.length; index++) {
      Object canonical =
          HashKeyCanonicalizer.canonicalValue(block.getColumn(index), 0, types[index]);
      assertEquals(
          types[index].getTypeEnum().toString(),
          canonical.hashCode(),
          HashKeyCanonicalizer.routingHash(block.getColumn(index), 0, types[index]));
    }
  }

  /**
   * The one documented disagreement. A NaN row is routed but never matches, so its bucket is
   * unconstrained; what matters is that routing stays deterministic across calls and blocks.
   */
  @Test
  public void nanIsRoutedDeterministicallyButNeverMatches() {
    TsBlock first = buildDoubles(Double.NaN, 1.0D);
    TsBlock second = buildDoubles(Double.NaN, 1.0D);
    int firstHash = HashKeyCanonicalizer.routingHash(first.getColumn(0), 0, DOUBLE);
    assertEquals(firstHash, HashKeyCanonicalizer.routingHash(second.getColumn(0), 0, DOUBLE));

    assertNull(HashKeyCanonicalizer.canonicalValue(first.getColumn(0), 0, DOUBLE));
  }

  @Test
  public void sqlNullContributesTheDocumentedConstant() {
    TsBlock block = buildNullableInts(null, 3, null);
    assertEquals(
        HashKeyCanonicalizer.NULL_COMPONENT_HASH,
        HashKeyCanonicalizer.routingHash(block.getColumn(0), 0, INT32));
    assertEquals(3, HashKeyCanonicalizer.routingHash(block.getColumn(0), 1, INT32));
    assertEquals(
        HashKeyCanonicalizer.routingHash(block.getColumn(0), 0, INT32),
        HashKeyCanonicalizer.routingHash(block.getColumn(0), 2, INT32));
  }

  /**
   * The two integer widths do not hash a shared value the same way once the value is negative.
   * {@code Integer.hashCode} is the identity, while {@code Long.hashCode} folds the two halves and
   * so returns the complement for a negative int-range value. The consequence is not a routing bug.
   * The router types each key column from the same symbol the join does, so a coerced pair is
   * hashed at one width on both sides and the two rows still land together. What the measurement
   * rules out is the weaker claim that the widths are interchangeable, which is why the planner
   * gate has to resolve the coercion instead of assuming it away.
   */
  @Test
  public void integerWidthsAgreeOnlyForNonNegativeValues() {
    for (int value : new int[] {0, 1, 12345, Integer.MAX_VALUE}) {
      TsBlock narrow = buildInts(value);
      TsBlock wide = buildLongs((long) value);
      assertEquals(
          "value " + value,
          HashKeyCanonicalizer.routingHash(narrow.getColumn(0), 0, INT32),
          HashKeyCanonicalizer.routingHash(wide.getColumn(0), 0, INT64));
    }
    for (int value : new int[] {Integer.MIN_VALUE, -42, -1}) {
      TsBlock narrow = buildInts(value);
      TsBlock wide = buildLongs((long) value);
      assertFalse(
          "value " + value,
          HashKeyCanonicalizer.routingHash(narrow.getColumn(0), 0, INT32)
              == HashKeyCanonicalizer.routingHash(wide.getColumn(0), 0, INT64));
    }
  }

  @Test
  public void booleanAndIntegerDoNotAgreeSoTheGateMustRefuseTheCoercion() {
    TsBlock bools = buildBooleans(Boolean.TRUE);
    TsBlock ints = buildInts(1);
    assertFalse(
        HashKeyCanonicalizer.routingHash(bools.getColumn(0), 0, BOOLEAN)
            == HashKeyCanonicalizer.routingHash(ints.getColumn(0), 0, INT32));
  }

  @Test
  public void binaryKeysHashByContentAcrossSeparateBlocks() {
    TsBlock first = buildBinaries(binary("same"));
    TsBlock second = buildBinaries(binary("same"));
    TsBlock other = buildBinaries(binary("other"));
    assertEquals(
        HashKeyCanonicalizer.routingHash(first.getColumn(0), 0, TEXT),
        HashKeyCanonicalizer.routingHash(second.getColumn(0), 0, TEXT));
    assertFalse(
        HashKeyCanonicalizer.routingHash(first.getColumn(0), 0, TEXT)
            == HashKeyCanonicalizer.routingHash(other.getColumn(0), 0, TEXT));
  }

  /**
   * The supported set, written out rather than derived from the implementation. A new TypeEnum that
   * someone forgets to handle fails here instead of throwing at query time.
   */
  @Test
  public void encodableTypesAreExactlyTheOnesTheSwitchNames() {
    Set<TypeEnum> encodable =
        new HashSet<>(
            Arrays.asList(
                TypeEnum.INT32,
                TypeEnum.INT64,
                TypeEnum.FLOAT,
                TypeEnum.DOUBLE,
                TypeEnum.BOOLEAN,
                TypeEnum.TEXT,
                TypeEnum.TIMESTAMP,
                TypeEnum.DATE,
                TypeEnum.BLOB,
                TypeEnum.STRING));
    for (TypeEnum typeEnum : TypeEnum.values()) {
      assertEquals(
          typeEnum + " encodability",
          encodable.contains(typeEnum),
          HashKeyCanonicalizer.isEncodable(type(typeEnum)));
    }
  }

  /** The refusal is the contract, so it is asserted on both entry points. */
  @Test
  public void unencodableTypeIsRefusedRatherThanSilentlyMishashed() {
    Type unsupported = type(TypeEnum.OBJECT);
    assertFalse(HashKeyCanonicalizer.isEncodable(unsupported));

    TsBlock block = buildInts(1);
    try {
      HashKeyCanonicalizer.routingHash(block.getColumn(0), 0, unsupported);
      fail("routingHash must refuse OBJECT");
    } catch (UnsupportedOperationException expected) {
      // the contract
    }
    try {
      HashKeyCanonicalizer.canonicalValue(block.getColumn(0), 0, unsupported);
      fail("canonicalValue must refuse OBJECT");
    } catch (UnsupportedOperationException expected) {
      // the contract
    }
  }

  // ---------------------------------------------------------------- helpers

  private static Type type(TypeEnum typeEnum) {
    switch (typeEnum) {
      case INT32:
        return INT32;
      case INT64:
        return INT64;
      case FLOAT:
        return FLOAT;
      case DOUBLE:
        return DOUBLE;
      case BOOLEAN:
        return BOOLEAN;
      case STRING:
      case TEXT:
      case BLOB:
        return TEXT;
      default:
        // ROW, UNKNOWN, OBJECT and VECTOR have no canonical encoding. They only exist here so a
        // test can ask the class to refuse them.
        return new UnsupportedType(typeEnum);
    }
  }

  /** A type with no canonical encoding, so the refusal path can be asserted. */
  private static final class UnsupportedType implements Type {

    private final TypeEnum typeEnum;

    private UnsupportedType(TypeEnum typeEnum) {
      this.typeEnum = typeEnum;
    }

    @Override
    public TypeEnum getTypeEnum() {
      return typeEnum;
    }

    @Override
    public String getDisplayName() {
      return typeEnum.name();
    }

    @Override
    public boolean isComparable() {
      return false;
    }

    @Override
    public boolean isOrderable() {
      return false;
    }

    @Override
    public List<Type> getTypeParameters() {
      return Collections.emptyList();
    }

    @Override
    public ColumnBuilder createColumnBuilder(int expectedEntries) {
      throw new UnsupportedOperationException();
    }

    @Override
    public TsPrimitiveType deserialize(ByteBuffer buffer) {
      throw new UnsupportedOperationException();
    }
  }

  private static Binary binary(String value) {
    return new Binary(value.getBytes(StandardCharsets.UTF_8));
  }

  private static TsBlock buildFloats(float... values) {
    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(TSDataType.FLOAT));
    for (int position = 0; position < values.length; position++) {
      builder.getTimeColumnBuilder().writeLong(position);
      builder.getColumnBuilder(0).writeFloat(values[position]);
      builder.declarePosition();
    }
    return builder.build();
  }

  private static TsBlock buildDoubles(double... values) {
    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(TSDataType.DOUBLE));
    for (int position = 0; position < values.length; position++) {
      builder.getTimeColumnBuilder().writeLong(position);
      builder.getColumnBuilder(0).writeDouble(values[position]);
      builder.declarePosition();
    }
    return builder.build();
  }

  private static TsBlock buildInts(int... values) {
    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(TSDataType.INT32));
    for (int position = 0; position < values.length; position++) {
      builder.getTimeColumnBuilder().writeLong(position);
      builder.getColumnBuilder(0).writeInt(values[position]);
      builder.declarePosition();
    }
    return builder.build();
  }

  private static TsBlock buildLongs(long... values) {
    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(TSDataType.INT64));
    for (int position = 0; position < values.length; position++) {
      builder.getTimeColumnBuilder().writeLong(position);
      builder.getColumnBuilder(0).writeLong(values[position]);
      builder.declarePosition();
    }
    return builder.build();
  }

  private static TsBlock buildNullableInts(Integer... values) {
    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(TSDataType.INT32));
    for (int position = 0; position < values.length; position++) {
      builder.getTimeColumnBuilder().writeLong(position);
      if (values[position] == null) {
        builder.getColumnBuilder(0).appendNull();
      } else {
        builder.getColumnBuilder(0).writeInt(values[position]);
      }
      builder.declarePosition();
    }
    return builder.build();
  }

  private static TsBlock buildBooleans(Boolean... values) {
    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(TSDataType.BOOLEAN));
    for (int position = 0; position < values.length; position++) {
      builder.getTimeColumnBuilder().writeLong(position);
      builder.getColumnBuilder(0).writeBoolean(values[position]);
      builder.declarePosition();
    }
    return builder.build();
  }

  private static TsBlock buildBinaries(Binary... values) {
    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(TSDataType.TEXT));
    for (int position = 0; position < values.length; position++) {
      builder.getTimeColumnBuilder().writeLong(position);
      builder.getColumnBuilder(0).writeBinary(values[position]);
      builder.declarePosition();
    }
    return builder.build();
  }

  /** One row, one column per key component, so the fold can be checked component by component. */
  private static TsBlock buildRow(Object[] values, Type[] types) {
    TSDataType[] dataTypes = new TSDataType[types.length];
    for (int index = 0; index < types.length; index++) {
      dataTypes[index] = physicalType(types[index]);
    }
    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(dataTypes));
    builder.getTimeColumnBuilder().writeLong(0);
    for (int index = 0; index < values.length; index++) {
      writeValue(builder.getColumnBuilder(index), values[index], types[index]);
    }
    builder.declarePosition();
    return builder.build();
  }

  private static TSDataType physicalType(Type type) {
    switch (type.getTypeEnum()) {
      case INT32:
      case DATE:
        return TSDataType.INT32;
      case INT64:
      case TIMESTAMP:
        return TSDataType.INT64;
      case FLOAT:
        return TSDataType.FLOAT;
      case DOUBLE:
        return TSDataType.DOUBLE;
      case BOOLEAN:
        return TSDataType.BOOLEAN;
      default:
        return TSDataType.TEXT;
    }
  }

  private static void writeValue(
      org.apache.tsfile.block.column.ColumnBuilder builder, Object value, Type type) {
    switch (type.getTypeEnum()) {
      case INT32:
      case DATE:
        builder.writeInt((Integer) value);
        break;
      case INT64:
      case TIMESTAMP:
        builder.writeLong((Long) value);
        break;
      case FLOAT:
        builder.writeFloat((Float) value);
        break;
      case DOUBLE:
        builder.writeDouble((Double) value);
        break;
      case BOOLEAN:
        builder.writeBoolean((Boolean) value);
        break;
      default:
        builder.writeBinary((Binary) value);
        break;
    }
  }
}
