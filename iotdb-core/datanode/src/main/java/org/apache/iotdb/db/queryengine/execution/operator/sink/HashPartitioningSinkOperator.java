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

package org.apache.iotdb.db.queryengine.execution.operator.sink;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.IChannelRoutingSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.TsBlockHashPartitioner;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Collections;
import java.util.List;

/**
 * A terminal operator that applies table-model hash partitioning before a shuffle exchange.
 *
 * <p>Routing happens here rather than through {@link IChannelRoutingSinkHandle#send(TsBlock)},
 * whose ordinary implementation may use round-robin scheduling. The operator returns no output: it
 * writes every non-empty partition directly to its designated channel, preserving the driver's
 * normal sink lifecycle for no-more-block and abort handling.
 */
public class HashPartitioningSinkOperator implements Operator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(HashPartitioningSinkOperator.class);

  private final OperatorContext operatorContext;
  private final Operator child;
  private final IChannelRoutingSinkHandle sinkHandle;
  private final TsBlockHashPartitioner partitioner;
  private List<TsBlockHashPartitioner.Partition> pending = Collections.emptyList();
  private int nextPartition;

  public HashPartitioningSinkOperator(
      OperatorContext operatorContext,
      Operator child,
      IChannelRoutingSinkHandle sinkHandle,
      TsBlockHashPartitioner partitioner) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.sinkHandle = sinkHandle;
    this.partitioner = partitioner;
    sinkHandle.useExplicitChannelRouting();
  }

  @Override
  public boolean hasNext() throws Exception {
    return !sinkHandle.isClosed() && (hasPendingPartition() || child.hasNextWithTimer());
  }

  @Override
  public TsBlock next() throws Exception {
    if (!hasPendingPartition()) {
      TsBlock input = child.nextWithTimer();
      if (input == null || input.isEmpty()) {
        return null;
      }
      pending = partitioner.partition(input);
      nextPartition = 0;
    }
    while (hasPendingPartition()) {
      TsBlockHashPartitioner.Partition partition = pending.get(nextPartition);
      if (!sinkHandle.isChannelNotFull(partition.getBucket()).isDone()) {
        return null;
      }
      sinkHandle.sendToChannel(partition.getBucket(), partition.getTsBlock());
      nextPartition++;
    }
    pending = Collections.emptyList();
    nextPartition = 0;
    return null;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return hasPendingPartition()
        ? sinkHandle.isChannelNotFull(pending.get(nextPartition).getBucket())
        : child.isBlocked();
  }

  @Override
  public boolean isFinished() throws Exception {
    return sinkHandle.isClosed() || (!hasPendingPartition() && child.isFinished());
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public void close() throws Exception {
    try {
      child.close();
    } finally {
      pending = Collections.emptyList();
      nextPartition = 0;
    }
  }

  @Override
  public long calculateMaxPeekMemory() {
    return child.calculateMaxPeekMemoryWithCounter() + calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long calculateMaxReturnSize() {
    return child.calculateMaxReturnSize();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 2 * child.calculateMaxReturnSize();
  }

  private boolean hasPendingPartition() {
    return nextPartition < pending.size();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(child)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sinkHandle);
  }
}
