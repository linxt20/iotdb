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

package org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.comparator;

import org.apache.tsfile.read.common.block.TsBlock;

import java.util.Optional;

public class DescBinaryTypeJoinKeyComparator implements JoinKeyComparator {

  private static final DescBinaryTypeJoinKeyComparator INSTANCE =
      new DescBinaryTypeJoinKeyComparator();

  private DescBinaryTypeJoinKeyComparator() {
    // hide constructor
  }

  public static DescBinaryTypeJoinKeyComparator getInstance() {
    return INSTANCE;
  }

  @Override
  public Optional<Boolean> lessThan(
      TsBlock left,
      int leftColumnIndex,
      int leftRowIndex,
      TsBlock right,
      int rightColumnIndex,
      int rightRowIndex) {
    if (left.getColumn(leftColumnIndex).isNull(leftRowIndex)
        || right.getColumn(rightColumnIndex).isNull(rightRowIndex)) {
      return Optional.empty();
    }

    return Optional.of(
        left.getColumn(leftColumnIndex)
                .getBinary(leftRowIndex)
                .compareTo(right.getColumn(rightColumnIndex).getBinary(rightRowIndex))
            > 0);
  }

  @Override
  public Optional<Boolean> equalsTo(
      TsBlock left,
      int leftColumnIndex,
      int leftRowIndex,
      TsBlock right,
      int rightColumnIndex,
      int rightRowIndex) {
    if (left.getColumn(leftColumnIndex).isNull(leftRowIndex)
        || right.getColumn(rightColumnIndex).isNull(rightRowIndex)) {
      return Optional.empty();
    }

    return Optional.of(
        left.getColumn(leftColumnIndex)
            .getBinary(leftRowIndex)
            .equals(right.getColumn(rightColumnIndex).getBinary(rightRowIndex)));
  }

  @Override
  public Optional<Boolean> lessThanOrEqual(
      TsBlock left,
      int leftColumnIndex,
      int leftRowIndex,
      TsBlock right,
      int rightColumnIndex,
      int rightRowIndex) {
    if (left.getColumn(leftColumnIndex).isNull(leftRowIndex)
        || right.getColumn(rightColumnIndex).isNull(rightRowIndex)) {
      return Optional.empty();
    }

    return Optional.of(
        left.getColumn(leftColumnIndex)
                .getBinary(leftRowIndex)
                .compareTo(right.getColumn(rightColumnIndex).getBinary(rightRowIndex))
            >= 0);
  }
}
