/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array;

import java.util.Arrays;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.BigArrays.INITIAL_SEGMENTS;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.BigArrays.SEGMENT_SIZE;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.BigArrays.offset;
import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.array.BigArrays.segment;
import static org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOf;
import static org.apache.tsfile.utils.RamUsageEstimator.shallowSizeOfInstance;
import static org.apache.tsfile.utils.RamUsageEstimator.sizeOfBooleanArray;

// Note: this code was forked from fastutil (http://fastutil.di.unimi.it/)
// Copyright (C) 2010-2013 Sebastiano Vigna
public final class BooleanBigArray {
  private static final long INSTANCE_SIZE = shallowSizeOfInstance(BooleanBigArray.class);
  private static final long SIZE_OF_SEGMENT = sizeOfBooleanArray(SEGMENT_SIZE);

  private final boolean initialValue;

  private boolean[][] array;
  private long capacity;
  private int segments;

  /** Creates a new big array containing one initial segment */
  public BooleanBigArray() {
    this(false);
  }

  public BooleanBigArray(boolean initialValue) {
    this.initialValue = initialValue;
    array = new boolean[INITIAL_SEGMENTS][];
    allocateNewSegment();
  }

  /** Returns the size of this big array in bytes. */
  public long sizeOf() {
    return INSTANCE_SIZE + shallowSizeOf(array) + (segments * SIZE_OF_SEGMENT);
  }

  /**
   * Returns the element of this big array at specified index.
   *
   * @param index a position in this big array.
   * @return the element of this big array at the specified position.
   */
  public boolean get(long index) {
    return array[segment(index)][offset(index)];
  }

  /**
   * Sets the element of this big array at specified index.
   *
   * @param index a position in this big array.
   */
  public void set(long index, boolean value) {
    array[segment(index)][offset(index)] = value;
  }

  /**
   * Ensures this big array is at least the specified length. If the array is smaller, segments are
   * added until the array is larger then the specified length.
   */
  public void ensureCapacity(long length) {
    if (capacity > length) {
      return;
    }

    grow(length);
  }

  public long getCapacity() {
    return capacity;
  }

  /** Fills the entire big array with the specified value. */
  public void fill(boolean value) {
    for (boolean[] segment : array) {
      if (segment == null) {
        return;
      }
      Arrays.fill(segment, value);
    }
  }

  public void reset() {
    fill(initialValue);
  }

  /**
   * Copies this array, beginning at the specified sourceIndex, to the specified destinationIndex of
   * the destination array. A subsequence of this array's components are copied to the destination
   * array referenced by {@code destination}. The number of components copied is equal to the {@code
   * length} argument. The components at positions {@code sourceIndex} through {@code
   * sourceIndex+length-1} in this array are copied into positions {@code destinationIndex} through
   * {@code destinationIndex+length-1}, respectively, of the destination array.
   */
  public void copyTo(
      long sourceIndex, BooleanBigArray destination, long destinationIndex, long length) {
    while (length > 0) {
      int startSegment = segment(sourceIndex);
      int startOffset = offset(sourceIndex);
      int destinationStartSegment = segment(destinationIndex);
      int destinationStartOffset = offset(destinationIndex);

      int copyLength = Math.min(SEGMENT_SIZE - startOffset, SEGMENT_SIZE - destinationStartOffset);
      copyLength =
          Math.min(copyLength, length > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) length);

      System.arraycopy(
          array[startSegment],
          startOffset,
          destination.array[destinationStartSegment],
          destinationStartOffset,
          copyLength);

      sourceIndex += copyLength;
      destinationIndex += copyLength;
      length -= copyLength;
    }
  }

  private void grow(long length) {
    // how many segments are required to get to the length?
    int requiredSegments = segment(length) + 1;

    // grow base array if necessary
    if (array.length < requiredSegments) {
      array = Arrays.copyOf(array, requiredSegments);
    }

    // add new segments
    while (segments < requiredSegments) {
      allocateNewSegment();
    }
  }

  private void allocateNewSegment() {
    boolean[] newSegment = new boolean[SEGMENT_SIZE];
    if (initialValue) {
      Arrays.fill(newSegment, initialValue);
    }
    array[segments] = newSegment;
    capacity += SEGMENT_SIZE;
    segments++;
  }
}
