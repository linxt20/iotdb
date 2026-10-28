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

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.type.Type;

/**
 * The one rule that turns a relational key component into a routing hash and into an equality key.
 *
 * <p>Two consumers must agree on this rule, or a distributed join silently drops matches. {@code
 * TsBlockHashPartitioner} decides which bucket a row is sent to. {@code HashInnerJoinOperator}
 * decides which rows are equal inside a bucket. Before this class existed the first hashed the raw
 * physical object and the second normalized floating zero, so {@code -0.0} and {@code +0.0} were
 * sent to different buckets while the executor regarded them as equal. Measured on the 2.0.11
 * baseline: the two object hashes are {@code -2147483648} and {@code 0}, and 60 of the 63 bucket
 * counts from 2 to 64 separate them.
 *
 * <p>The rule has two entry points because the two consumers need different things from a key that
 * is not a plain comparable value:
 *
 * <ul>
 *   <li>{@link #routingHash} answers which bucket. Every row gets one, including a row whose key is
 *       SQL null or NaN, because a sink must forward every row it is given.
 *   <li>{@link #canonicalValue} answers whether two rows are equal. It returns null for SQL null
 *       and for NaN, meaning the row is never inserted into or matched by a hash table. The caller
 *       must have already ruled out a SQL null.
 * </ul>
 *
 * <p>A NaN-keyed row is therefore routed deterministically but never matches anything, which is the
 * inner-join semantics the local operator already implements. The two entry points disagree for
 * that row, and the disagreement is harmless: no other row can be equal to it, so its bucket is
 * unconstrained.
 *
 * <p>The fold over components is the one {@link java.util.Arrays#hashCode(Object[])} uses, so a
 * composite key routes the same way the join hashes it. For any row without a null or NaN component
 * the bucket is exactly the join's internal hash reduced modulo the bucket count.
 */
public final class HashKeyCanonicalizer {

  /** A SQL null contributes this under {@code NullRouting.HASH_NULL}. */
  public static final int NULL_COMPONENT_HASH = 0;

  private HashKeyCanonicalizer() {}

  /**
   * The bucket contribution of one key component. A SQL null contributes {@link
   * #NULL_COMPONENT_HASH}; every other value, including NaN, contributes the hash of its canonical
   * form.
   *
   * @throws UnsupportedOperationException when the type has no canonical encoding
   */
  public static int routingHash(Column column, int position, Type type) {
    if (column.isNull(position)) {
      return NULL_COMPONENT_HASH;
    }
    switch (type.getTypeEnum()) {
      case INT32:
      case DATE:
        return Integer.hashCode(column.getInt(position));
      case INT64:
      case TIMESTAMP:
        return Long.hashCode(column.getLong(position));
      case BOOLEAN:
        return Boolean.hashCode(column.getBoolean(position));
      case STRING:
      case BLOB:
      case TEXT:
        return column.getBinary(position).hashCode();
      case FLOAT:
        return Float.hashCode(canonicalFloat(column.getFloat(position)));
      case DOUBLE:
        return Double.hashCode(canonicalDouble(column.getDouble(position)));
      default:
        throw new UnsupportedOperationException(
            "Unsupported hash key type for routing: " + type.getTypeEnum());
    }
  }

  /**
   * The value a hash table uses for one key component, or null when the row must never be inserted
   * or matched. The caller must have already ruled out a SQL null.
   *
   * @throws UnsupportedOperationException when the type has no canonical encoding
   */
  public static Object canonicalValue(Column column, int position, Type type) {
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
        return canonicalFloat(floatValue);
      case DOUBLE:
        double doubleValue = column.getDouble(position);
        if (Double.isNaN(doubleValue)) {
          return null;
        }
        return canonicalDouble(doubleValue);
      case BOOLEAN:
        return column.getBoolean(position);
      case STRING:
      case BLOB:
      case TEXT:
        return column.getBinary(position);
      default:
        throw new UnsupportedOperationException(
            "Unsupported hash key type for equality: " + type.getTypeEnum());
    }
  }

  /**
   * Folds per-component hashes in key order. The formula matches {@link
   * java.util.Arrays#hashCode(Object[])} so a composite key routes the same way the join hashes it.
   */
  public static int foldComponentHashes(int[] componentHashes) {
    int hash = 1;
    for (int componentHash : componentHashes) {
      hash = 31 * hash + componentHash;
    }
    return hash;
  }

  /**
   * The routing-side normalizer. Signed zero has one canonical form, so that it hashes like the
   * value it equals. NaN is deliberately left alone here: a sink must forward every row it is
   * given, so {@link #routingHash} has to stay total. {@link #canonicalValue} is where NaN is
   * dropped.
   */
  private static Float canonicalFloat(float value) {
    return value == 0.0F ? 0.0F : value;
  }

  private static Double canonicalDouble(double value) {
    return value == 0.0D ? 0.0D : value;
  }

  /** The types this class can encode, so a planner gate can ask before it commits to a plan. */
  public static boolean isEncodable(Type type) {
    switch (type.getTypeEnum()) {
      case INT32:
      case DATE:
      case INT64:
      case TIMESTAMP:
      case BOOLEAN:
      case STRING:
      case BLOB:
      case TEXT:
      case FLOAT:
      case DOUBLE:
        return true;
      default:
        return false;
    }
  }
}
