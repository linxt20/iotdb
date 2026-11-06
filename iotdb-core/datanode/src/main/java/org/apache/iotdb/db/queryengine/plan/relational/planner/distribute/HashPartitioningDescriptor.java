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

package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A serializable declaration of the rows that a table-model hash exchange must colocate.
 *
 * <p>The declaration deliberately contains no hashing implementation. In particular, existing
 * {@code ShuffleSinkHandle} round-robin routing does not satisfy this contract. A future router
 * must apply the versioned hash to the ordered symbols, preserve the null rule, and route to one of
 * exactly {@link #partitionCount} output partitions before this descriptor can be selected by the
 * distributed planner.
 */
public class HashPartitioningDescriptor {

  /** The version identifies the byte-level hash contract that a router must implement. */
  public enum HashVersion {
    TABLE_HASH_V1
  }

  /** How a SQL null is represented when the router computes the partition hash. */
  public enum NullRouting {
    HASH_NULL
  }

  private final List<Symbol> partitioningSymbols;
  private final HashVersion hashVersion;
  private final NullRouting nullRouting;
  private final int partitionCount;

  public HashPartitioningDescriptor(
      List<Symbol> partitioningSymbols,
      HashVersion hashVersion,
      NullRouting nullRouting,
      int partitionCount) {
    this.partitioningSymbols = List.copyOf(Objects.requireNonNull(partitioningSymbols));
    this.hashVersion = Objects.requireNonNull(hashVersion);
    this.nullRouting = Objects.requireNonNull(nullRouting);
    this.partitionCount = partitionCount;
    validate();
  }

  private void validate() {
    if (partitioningSymbols.isEmpty() || partitionCount <= 0) {
      throw new IllegalArgumentException();
    }
    Set<Symbol> distinctSymbols = new HashSet<>(partitioningSymbols);
    if (distinctSymbols.size() != partitioningSymbols.size()) {
      throw new IllegalArgumentException();
    }
  }

  public List<Symbol> getPartitioningSymbols() {
    return partitioningSymbols;
  }

  public HashVersion getHashVersion() {
    return hashVersion;
  }

  public NullRouting getNullRouting() {
    return nullRouting;
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(partitioningSymbols.size(), byteBuffer);
    for (Symbol partitioningSymbol : partitioningSymbols) {
      Symbol.serialize(partitioningSymbol, byteBuffer);
    }
    ReadWriteIOUtils.write(hashVersion.ordinal(), byteBuffer);
    ReadWriteIOUtils.write(nullRouting.ordinal(), byteBuffer);
    ReadWriteIOUtils.write(partitionCount, byteBuffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(partitioningSymbols.size(), stream);
    for (Symbol partitioningSymbol : partitioningSymbols) {
      Symbol.serialize(partitioningSymbol, stream);
    }
    ReadWriteIOUtils.write(hashVersion.ordinal(), stream);
    ReadWriteIOUtils.write(nullRouting.ordinal(), stream);
    ReadWriteIOUtils.write(partitionCount, stream);
  }

  public static HashPartitioningDescriptor deserialize(ByteBuffer byteBuffer) {
    int symbolCount = ReadWriteIOUtils.readInt(byteBuffer);
    if (symbolCount <= 0) {
      throw new IllegalArgumentException();
    }
    List<Symbol> partitioningSymbols = new ArrayList<>(symbolCount);
    for (int i = 0; i < symbolCount; i++) {
      partitioningSymbols.add(Symbol.deserialize(byteBuffer));
    }
    return new HashPartitioningDescriptor(
        partitioningSymbols,
        enumByOrdinal(HashVersion.values(), ReadWriteIOUtils.readInt(byteBuffer)),
        enumByOrdinal(NullRouting.values(), ReadWriteIOUtils.readInt(byteBuffer)),
        ReadWriteIOUtils.readInt(byteBuffer));
  }

  private static <T extends Enum<T>> T enumByOrdinal(T[] values, int ordinal) {
    if (ordinal < 0 || ordinal >= values.length) {
      throw new IllegalArgumentException();
    }
    return values[ordinal];
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof HashPartitioningDescriptor)) {
      return false;
    }
    HashPartitioningDescriptor other = (HashPartitioningDescriptor) obj;
    return partitionCount == other.partitionCount
        && partitioningSymbols.equals(other.partitioningSymbols)
        && hashVersion == other.hashVersion
        && nullRouting == other.nullRouting;
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitioningSymbols, hashVersion, nullRouting, partitionCount);
  }
}
