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

package org.apache.iotdb.db.queryengine.plan.planner.memory;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.plan.planner.memory.PipelineMemoryEstimator;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

/** Table pipelines may consume all children concurrently; reject invalid estimates at admission. */
public class CheckedTablePipelineMemoryEstimator extends PipelineMemoryEstimator {
  public CheckedTablePipelineMemoryEstimator(Operator root, int dependencyPipelineIndex) {
    super(root, dependencyPipelineIndex);
  }

  private static long nonNegative(long bytes) {
    if (bytes < 0) {
      throw new IllegalArgumentException(
          String.format(
              DataNodeQueryMessages.EXCEPTION_NEGATIVE_TABLE_PIPELINE_MEMORY_ESTIMATE_ARG_40A30A53,
              bytes));
    }
    return bytes;
  }

  private static long add(long left, long right) {
    return Math.addExact(nonNegative(left), nonNegative(right));
  }

  private static CheckedTablePipelineMemoryEstimator tableChild(PipelineMemoryEstimator child) {
    if (!(child instanceof CheckedTablePipelineMemoryEstimator)) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages
              .EXCEPTION_TABLE_PIPELINE_CONTAINS_AN_UNCHECKED_CHILD_ESTIMATOR_55F7B9C4);
    }
    return (CheckedTablePipelineMemoryEstimator) child;
  }

  @Override
  public long calculateEstimatedRunningMemorySize() {
    long bytes = nonNegative(root.calculateMaxPeekMemoryWithCounter());
    for (PipelineMemoryEstimator child : children) {
      bytes = add(bytes, tableChild(child).calculateEstimatedRunningMemorySize());
    }
    return bytes;
  }

  @Override
  protected long calculateRetainedMemorySize() {
    long bytes = nonNegative(root.ramBytesUsed());
    for (PipelineMemoryEstimator child : children) {
      bytes = add(bytes, tableChild(child).calculateRetainedMemorySize());
    }
    return bytes;
  }

  @Override
  public long getEstimatedMemoryUsageInBytes() {
    return add(calculateEstimatedRunningMemorySize(), calculateRetainedMemorySize());
  }
}
