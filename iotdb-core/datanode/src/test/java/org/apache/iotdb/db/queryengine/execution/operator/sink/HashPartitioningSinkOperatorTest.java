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
import org.apache.iotdb.db.queryengine.execution.exchange.sink.IChannelRoutingSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.TsBlockHashPartitioner;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class HashPartitioningSinkOperatorTest {

  @Test
  public void testRoutesEveryBucketToItsExactChannel() throws Exception {
    TsBlock input = buildBlock();
    Operator child = Mockito.mock(Operator.class);
    IChannelRoutingSinkHandle sinkHandle = Mockito.mock(IChannelRoutingSinkHandle.class);
    Mockito.when(child.nextWithTimer()).thenReturn(input);
    Mockito.doReturn(Futures.immediateVoidFuture())
        .when(sinkHandle)
        .isChannelNotFull(Mockito.anyInt());
    HashPartitioningSinkOperator operator =
        new HashPartitioningSinkOperator(
            Mockito.mock(OperatorContext.class),
            child,
            sinkHandle,
            new TsBlockHashPartitioner(
                7, new int[] {0}, new Type[] {TypeFactory.getType(TSDataType.INT32)}));

    assertNull(operator.next());

    ArgumentCaptor<Integer> channelCaptor = ArgumentCaptor.forClass(Integer.class);
    ArgumentCaptor<TsBlock> blockCaptor = ArgumentCaptor.forClass(TsBlock.class);
    Mockito.verify(sinkHandle, Mockito.atLeastOnce())
        .sendToChannel(channelCaptor.capture(), blockCaptor.capture());
    List<Integer> channels = channelCaptor.getAllValues();
    List<TsBlock> partitionedBlocks = blockCaptor.getAllValues();
    assertTrue(channels.size() <= 3);
    for (int blockIndex = 0; blockIndex < partitionedBlocks.size(); blockIndex++) {
      TsBlock partitionedBlock = partitionedBlocks.get(blockIndex);
      int channel = channels.get(blockIndex);
      for (int position = 0; position < partitionedBlock.getPositionCount(); position++) {
        assertTrue(channel >= 0 && channel < 7);
        assertTrue(
            channel
                == new TsBlockHashPartitioner(
                        7, new int[] {0}, new Type[] {TypeFactory.getType(TSDataType.INT32)})
                    .getBucket(partitionedBlock, position));
      }
    }
    Mockito.verify(sinkHandle, Mockito.never()).send(Mockito.any());
  }

  @Test
  public void testBlockedPendingPartitionSurvivesUntilItsChannelIsReady() throws Exception {
    TsBlock input = buildBlock();
    Operator child = Mockito.mock(Operator.class);
    IChannelRoutingSinkHandle sinkHandle = Mockito.mock(IChannelRoutingSinkHandle.class);
    SettableFuture<Void> channelAvailable = SettableFuture.create();
    Mockito.doReturn(Futures.immediateVoidFuture()).when(child).isBlocked();
    Mockito.when(child.nextWithTimer()).thenReturn(input);
    Mockito.when(child.isFinished()).thenReturn(true);
    Mockito.doReturn(channelAvailable).when(sinkHandle).isChannelNotFull(Mockito.anyInt());
    HashPartitioningSinkOperator operator =
        new HashPartitioningSinkOperator(
            Mockito.mock(OperatorContext.class),
            child,
            sinkHandle,
            new TsBlockHashPartitioner(
                32, new int[] {0}, new Type[] {TypeFactory.getType(TSDataType.INT32)}));

    assertTrue(operator.isBlocked().isDone());
    Mockito.verify(sinkHandle, Mockito.never()).isAllChannelsNotFull();
    operator.next();
    assertSame(channelAvailable, operator.isBlocked());
    assertTrue(operator.hasNext());
    org.junit.Assert.assertFalse(operator.isFinished());
    operator.next();
    Mockito.verify(child, Mockito.times(1)).nextWithTimer();
    Mockito.verify(sinkHandle, Mockito.never()).sendToChannel(Mockito.anyInt(), Mockito.any());

    channelAvailable.set(null);
    operator.next();
    assertTrue(operator.isFinished());
    ArgumentCaptor<TsBlock> sent = ArgumentCaptor.forClass(TsBlock.class);
    Mockito.verify(sinkHandle, Mockito.atLeastOnce())
        .sendToChannel(Mockito.anyInt(), sent.capture());
    org.junit.Assert.assertEquals(
        3, sent.getAllValues().stream().mapToInt(TsBlock::getPositionCount).sum());
    Mockito.verify(child, Mockito.times(1)).nextWithTimer();
  }

  @Test
  public void testCloseClearsBlockedPendingPartitions() throws Exception {
    Operator child = Mockito.mock(Operator.class);
    IChannelRoutingSinkHandle sink = Mockito.mock(IChannelRoutingSinkHandle.class);
    Mockito.when(child.nextWithTimer()).thenReturn(buildBlock());
    Mockito.doReturn(SettableFuture.create()).when(sink).isChannelNotFull(Mockito.anyInt());
    HashPartitioningSinkOperator operator =
        new HashPartitioningSinkOperator(
            Mockito.mock(OperatorContext.class),
            child,
            sink,
            new TsBlockHashPartitioner(
                32, new int[] {0}, new Type[] {TypeFactory.getType(TSDataType.INT32)}));
    operator.next();
    operator.close();
    Mockito.verify(child).close();
    org.junit.Assert.assertFalse(operator.hasNext());
    Mockito.verify(sink, Mockito.never()).sendToChannel(Mockito.anyInt(), Mockito.any());
  }

  private static TsBlock buildBlock() {
    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(TSDataType.INT32, TSDataType.INT32));
    for (int position = 0; position < 3; position++) {
      builder.getTimeColumnBuilder().writeLong(position);
      builder.getColumnBuilder(0).writeInt(position + 1);
      builder.getColumnBuilder(1).writeInt((position + 1) * 10);
      builder.declarePosition();
    }
    return builder.build();
  }
}
