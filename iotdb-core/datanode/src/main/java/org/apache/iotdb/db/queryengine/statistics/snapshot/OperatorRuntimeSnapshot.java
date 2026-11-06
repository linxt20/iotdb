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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.statistics.snapshot;

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.mpp.rpc.thrift.TOperatorStatistics;

/** Immutable operator-level runtime values. */
public final class OperatorRuntimeSnapshot {

  private final String planNodeId;
  private final String operatorType;
  private final long executionNanos;
  private final long calls;
  private final long outputRows;
  private final long tsBlocks;
  private final Long estimatedMemory;
  private final Long exchangeBytes;

  public OperatorRuntimeSnapshot(
      String planNodeId,
      String operatorType,
      long executionNanos,
      long calls,
      long outputRows,
      long tsBlocks,
      Long estimatedMemory,
      Long exchangeBytes) {
    this.planNodeId = requireText(planNodeId, "planNodeId");
    this.operatorType = requireText(operatorType, "operatorType");
    this.executionNanos = nonNegative(executionNanos, "executionNanos");
    this.calls = nonNegative(calls, "calls");
    this.outputRows = nonNegative(outputRows, "outputRows");
    this.tsBlocks = nonNegative(tsBlocks, "tsBlocks");
    this.estimatedMemory = optionalNonNegative(estimatedMemory, "estimatedMemory");
    this.exchangeBytes = optionalNonNegative(exchangeBytes, "exchangeBytes");
  }

  public String getPlanNodeId() {
    return planNodeId;
  }

  public String getOperatorType() {
    return operatorType;
  }

  public long getExecutionNanos() {
    return executionNanos;
  }

  public long getCalls() {
    return calls;
  }

  public long getOutputRows() {
    return outputRows;
  }

  public long getTsBlocks() {
    return tsBlocks;
  }

  public Long getEstimatedMemory() {
    return estimatedMemory;
  }

  public Long getExchangeBytes() {
    return exchangeBytes;
  }

  /** Adapt the existing Explain Analyze operator metric without inventing missing values. */
  public static OperatorRuntimeSnapshot fromThrift(TOperatorStatistics statistics) {
    if (statistics == null) {
      throw new NullPointerException(DataNodeQueryMessages.EXCEPTION_STATISTICS_A912A94D);
    }
    return new OperatorRuntimeSnapshot(
        statistics.getPlanNodeId(),
        statistics.getOperatorType(),
        statistics.getTotalExecutionTimeInNanos(),
        statistics.getNextCalledCount(),
        statistics.getOutputRows(),
        statistics.isSetTsBlockCount() ? statistics.getTsBlockCount() : 0L,
        statistics.isSetMemoryUsage() ? statistics.getMemoryUsage() : null,
        statistics.isSetOutputBytes() ? statistics.getOutputBytes() : null);
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(DataNodeQueryMessages.EXCEPTION_ARG_MUST_NOT_BE_EMPTY_313AFCE2, name));
    }
    return value;
  }

  private static long nonNegative(long value, String name) {
    if (value < 0) {
      throw new IllegalArgumentException(
          String.format(DataNodeQueryMessages.EXCEPTION_ARG_MUST_BE_NON_NEGATIVE_F31E7CAD, name));
    }
    return value;
  }

  private static Long optionalNonNegative(Long value, String name) {
    if (value != null && value < 0) {
      throw new IllegalArgumentException(
          String.format(DataNodeQueryMessages.EXCEPTION_ARG_MUST_BE_NON_NEGATIVE_F31E7CAD, name));
    }
    return value;
  }
}
