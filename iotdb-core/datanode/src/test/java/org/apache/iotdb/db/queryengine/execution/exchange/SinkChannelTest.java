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

package org.apache.iotdb.db.queryengine.execution.exchange;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SinkListener;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.mpp.rpc.thrift.TEndOfDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TNewDataBlockEvent;

import org.apache.thrift.TException;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SinkChannelTest {
  private static final int DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  @Test
  public void testOneTimeNotBlockedSend() {
    final String queryId = "q0";
    final long mockTsBlockSize = 128 * 1024L;
    final int numOfMockTsBlock = 1;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final String localPlanNodeId = "fragmentSink_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");

    // Construct a mock LocalMemoryManager that returns unblocked futures.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> mockClientManager =
        Mockito.mock(IClientManager.class);
    // Construct a mock client.
    SyncDataNodeMPPDataExchangeServiceClient mockClient =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    try {
      Mockito.when(mockClientManager.borrowClient(remoteEndpoint)).thenReturn(mockClient);
      Mockito.doNothing()
          .when(mockClient)
          .onEndOfDataBlockEvent(Mockito.any(TEndOfDataBlockEvent.class));
      Mockito.doNothing()
          .when(mockClient)
          .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));
    } catch (ClientManagerException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    // Construct a mock SinkListener.
    SinkListener mockSinkListener = Mockito.mock(SinkListener.class);
    // Construct several mock TsBlock(s).
    List<TsBlock> mockTsBlocks = Utils.createMockTsBlocks(numOfMockTsBlock, mockTsBlockSize);
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(mockTsBlockSize);

    // Construct SinkChannel.
    SinkChannel sinkChannel =
        new SinkChannel(
            remoteEndpoint,
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localPlanNodeId,
            localFragmentInstanceId,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockTsBlockSerde,
            mockSinkListener,
            mockClientManager);
    sinkChannel.open();
    Assert.assertTrue(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkChannel.getNumOfBufferedTsBlocks());

    // Send tsblocks.
    sinkChannel.send(mockTsBlocks.get(0));
    Assert.assertTrue(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock + DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
        sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkChannel.getNumOfBufferedTsBlocks());
    //    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(1))
    //        .reserve(
    //            queryId,
    //
    // FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(localFragmentInstanceId),
    //            localPlanNodeId,
    //            mockTsBlockSize * numOfMockTsBlock,
    //            Long.MAX_VALUE);
    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
          .onNewDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                          && remotePlanNodeId.equals(e.getTargetPlanNodeId())
                          && localFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && e.getStartSequenceId() == 0
                          && e.getBlockSizes().size() == numOfMockTsBlock));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Get tsblocks.
    for (int i = 0; i < numOfMockTsBlock; i++) {
      try {
        sinkChannel.getSerializedTsBlock(i);
        sinkChannel.getSerializedTsBlock(i);
        Mockito.verify(mockTsBlockSerde, Mockito.times(1)).serialize(mockTsBlocks.get(i));
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail();
      }
      Assert.assertTrue(sinkChannel.isFull().isDone());
    }
    Assert.assertFalse(sinkChannel.isFinished());

    // Set no-more-tsblocks.
    sinkChannel.setNoMoreTsBlocks();
    Assert.assertTrue(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Mockito.verify(mockSinkListener, Mockito.timeout(10_000).times(1)).onEndOfBlocks(sinkChannel);

    // Ack tsblocks.
    sinkChannel.acknowledgeTsBlock(0, numOfMockTsBlock);
    Assert.assertTrue(sinkChannel.isFull().isDone());
    Assert.assertTrue(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(mockTsBlockSize, sinkChannel.getBufferRetainedSizeInBytes());
    for (int i = 0; i < numOfMockTsBlock; i++) {
      try {
        sinkChannel.getSerializedTsBlock(i);
        Assert.fail("The acknowledged serialized TsBlock should have been released");
      } catch (IllegalStateException expected) {
        // Both the original entry and serialized cache must be removed by acknowledgement.
      } catch (IOException e) {
        Assert.fail(e.getMessage());
      }
    }
    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(1))
        .free(
            queryId,
            FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                localFragmentInstanceId),
            localPlanNodeId,
            numOfMockTsBlock * mockTsBlockSize);
    Mockito.verify(mockSinkListener, Mockito.timeout(10_0000).times(1)).onFinish(sinkChannel);

    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
          .onEndOfDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                          && remotePlanNodeId.equals(e.getTargetPlanNodeId())
                          && localFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && numOfMockTsBlock - 1 == e.getLastSequenceId()));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testMultiTimesBlockedSend() {
    final String queryId = "q0";
    final long mockTsBlockSize = 128 * 1024L;
    final int numOfMockTsBlock = 1;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final String localPlanNodeId = "fragmentSink_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");

    // Construct a mock LocalMemoryManager that returns blocked futures.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool =
        Utils.createMockBlockedMemoryPool(
            queryId,
            FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                localFragmentInstanceId),
            localPlanNodeId,
            numOfMockTsBlock,
            mockTsBlockSize);
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);

    // Construct a mock SinkListener.
    SinkListener mockSinkListener = Mockito.mock(SinkListener.class);
    // Construct several mock TsBlock(s).
    List<TsBlock> mockTsBlocks = Utils.createMockTsBlocks(numOfMockTsBlock, mockTsBlockSize);
    IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> mockClientManager =
        Mockito.mock(IClientManager.class);
    // Construct a mock client.
    SyncDataNodeMPPDataExchangeServiceClient mockClient =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    try {
      Mockito.when(mockClientManager.borrowClient(remoteEndpoint)).thenReturn(mockClient);
      Mockito.doNothing()
          .when(mockClient)
          .onEndOfDataBlockEvent(Mockito.any(TEndOfDataBlockEvent.class));
      Mockito.doNothing()
          .when(mockClient)
          .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));
    } catch (ClientManagerException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Construct SinkChannel.
    SinkChannel sinkChannel =
        new SinkChannel(
            remoteEndpoint,
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localPlanNodeId,
            localFragmentInstanceId,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            Utils.createMockTsBlockSerde(mockTsBlockSize),
            mockSinkListener,
            mockClientManager);
    sinkChannel.open();
    Assert.assertTrue(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkChannel.getNumOfBufferedTsBlocks());

    // Send tsblocks.
    sinkChannel.send(mockTsBlocks.get(0));
    Assert.assertFalse(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock + DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
        sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkChannel.getNumOfBufferedTsBlocks());
    //    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(1))
    //        .reserve(
    //            queryId,
    //
    // FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(localFragmentInstanceId),
    //            localPlanNodeId,
    //            mockTsBlockSize * numOfMockTsBlock,
    //            Long.MAX_VALUE);
    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
          .onNewDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                          && remotePlanNodeId.equals(e.getTargetPlanNodeId())
                          && localFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && e.getStartSequenceId() == 0
                          && e.getBlockSizes().size() == numOfMockTsBlock));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Get tsblocks.
    for (int i = 0; i < numOfMockTsBlock; i++) {
      try {
        sinkChannel.getSerializedTsBlock(i);
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail();
      }
      Assert.assertFalse(sinkChannel.isFull().isDone());
    }
    Assert.assertFalse(sinkChannel.isFinished());

    // Ack tsblocks.
    sinkChannel.acknowledgeTsBlock(0, numOfMockTsBlock);
    Assert.assertTrue(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, sinkChannel.getBufferRetainedSizeInBytes());
    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(1))
        .free(
            queryId,
            FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                localFragmentInstanceId),
            localPlanNodeId,
            numOfMockTsBlock * mockTsBlockSize);

    // Send tsblocks.
    sinkChannel.send(mockTsBlocks.get(0));
    Assert.assertFalse(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock + DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
        sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkChannel.getNumOfBufferedTsBlocks());
    //    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(3))
    //        .reserve(
    //            queryId,
    //
    // FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(localFragmentInstanceId),
    //            localPlanNodeId,
    //            mockTsBlockSize * numOfMockTsBlock,
    //            Long.MAX_VALUE);
    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
          .onNewDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                          && remotePlanNodeId.equals(e.getTargetPlanNodeId())
                          && localFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && e.getStartSequenceId() == numOfMockTsBlock
                          && e.getBlockSizes().size() == numOfMockTsBlock));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Set no-more-tsblocks.
    sinkChannel.setNoMoreTsBlocks();
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Mockito.verify(mockSinkListener, Mockito.timeout(10_000).times(1)).onEndOfBlocks(sinkChannel);

    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
          .onEndOfDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                          && remotePlanNodeId.equals(e.getTargetPlanNodeId())
                          && localFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && numOfMockTsBlock * 2 - 1 == e.getLastSequenceId()));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Get tsblocks after no-more-tsblocks is set.
    for (int i = numOfMockTsBlock; i < numOfMockTsBlock * 2; i++) {
      try {
        sinkChannel.getSerializedTsBlock(i);
      } catch (IOException e) {
        e.printStackTrace();
        Assert.fail();
      }
    }
    Assert.assertFalse(sinkChannel.isFinished());

    // Ack tsblocks.
    sinkChannel.acknowledgeTsBlock(numOfMockTsBlock, numOfMockTsBlock * 2);
    Assert.assertTrue(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, sinkChannel.getBufferRetainedSizeInBytes());
    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(2))
        .free(
            queryId,
            FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                localFragmentInstanceId),
            localPlanNodeId,
            numOfMockTsBlock * mockTsBlockSize);
    Mockito.verify(mockSinkListener, Mockito.timeout(10_0000).times(1)).onFinish(sinkChannel);
  }

  @Test
  public void testFailedSend() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final int numOfMockTsBlock = 1;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final String localPlanNodeId = "fragmentSink_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");

    // Construct a mock LocalMemoryManager that returns blocked futures.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool =
        Utils.createMockBlockedMemoryPool(
            queryId,
            FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                localFragmentInstanceId),
            localPlanNodeId,
            numOfMockTsBlock,
            mockTsBlockSize);
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    // Construct a mock SinkListener.
    SinkListener mockSinkListener = Mockito.mock(SinkListener.class);
    // Construct several mock TsBlock(s).
    List<TsBlock> mockTsBlocks = Utils.createMockTsBlocks(numOfMockTsBlock, mockTsBlockSize);
    IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> mockClientManager =
        Mockito.mock(IClientManager.class);
    // Construct a mock client.
    SyncDataNodeMPPDataExchangeServiceClient mockClient =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    TException mockException = new TException("Mock exception");
    try {
      Mockito.when(mockClientManager.borrowClient(remoteEndpoint)).thenReturn(mockClient);
      Mockito.doThrow(mockException)
          .when(mockClient)
          .onEndOfDataBlockEvent(Mockito.any(TEndOfDataBlockEvent.class));
      Mockito.doThrow(mockException)
          .when(mockClient)
          .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));
    } catch (ClientManagerException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Construct SinkChannel.
    SinkChannel sinkChannel =
        new SinkChannel(
            remoteEndpoint,
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localPlanNodeId,
            localFragmentInstanceId,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            Utils.createMockTsBlockSerde(mockTsBlockSize),
            mockSinkListener,
            mockClientManager);
    sinkChannel.setRetryIntervalInMs(0L);
    sinkChannel.open();
    Assert.assertTrue(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkChannel.getNumOfBufferedTsBlocks());

    // Send tsblocks.
    sinkChannel.send(mockTsBlocks.get(0));
    Assert.assertFalse(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock + DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
        sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkChannel.getNumOfBufferedTsBlocks());
    //    Mockito.verify(mockMemoryPool, Mockito.timeout(10_0000).times(1))
    //        .reserve(
    //            queryId,
    //
    // FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(localFragmentInstanceId),
    //            localPlanNodeId,
    //            mockTsBlockSize * numOfMockTsBlock,
    //            Long.MAX_VALUE);
    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(SinkChannel.MAX_ATTEMPT_TIMES))
          .onNewDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getTargetFragmentInstanceId())
                          && remotePlanNodeId.equals(e.getTargetPlanNodeId())
                          && localFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && e.getStartSequenceId() == 0
                          && e.getBlockSizes().size() == numOfMockTsBlock));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    Mockito.verify(mockSinkListener, Mockito.timeout(10_000).times(1))
        .onFailure(sinkChannel, mockException);

    // Close the SinkChannel.
    sinkChannel.setNoMoreTsBlocks();
    Assert.assertFalse(sinkChannel.isAborted());
    Mockito.verify(mockSinkListener, Mockito.timeout(10_000).times(0)).onEndOfBlocks(sinkChannel);

    // Abort the SinkChannel.
    sinkChannel.abort();
    Assert.assertTrue(sinkChannel.isAborted());
    Mockito.verify(mockSinkListener, Mockito.timeout(10_0000).times(1)).onAborted(sinkChannel);
    Mockito.verify(mockSinkListener, Mockito.timeout(10_0000).times(0)).onFinish(sinkChannel);
  }

  @Test
  public void testAbort() {
    final String queryId = "q0";
    final long mockTsBlockSize = 1024L * 1024L;
    final int numOfMockTsBlock = 1;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final String localPlanNodeId = "fragmentSink_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");

    // Construct a mock LocalMemoryManager that returns blocked futures.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryManager memoryManager =
        Mockito.spy(new MemoryManager(numOfMockTsBlock * mockTsBlockSize));
    MemoryPool spyMemoryPool =
        Mockito.spy(new MemoryPool("test", memoryManager, numOfMockTsBlock * mockTsBlockSize));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);

    // Construct a mock SinkListener.
    SinkListener mockSinkListener = Mockito.mock(SinkListener.class);
    // Construct several mock TsBlock(s).
    List<TsBlock> mockTsBlocks = Utils.createMockTsBlocks(numOfMockTsBlock, mockTsBlockSize);
    IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> mockClientManager =
        Mockito.mock(IClientManager.class);
    // Construct a mock client.
    SyncDataNodeMPPDataExchangeServiceClient mockClient =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    try {
      Mockito.when(mockClientManager.borrowClient(remoteEndpoint)).thenReturn(mockClient);
      Mockito.doNothing()
          .when(mockClient)
          .onEndOfDataBlockEvent(Mockito.any(TEndOfDataBlockEvent.class));
      Mockito.doNothing()
          .when(mockClient)
          .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));
    } catch (ClientManagerException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    // Construct SinkChannel.
    SinkChannel sinkChannel =
        new SinkChannel(
            remoteEndpoint,
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localPlanNodeId,
            localFragmentInstanceId,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            Utils.createMockTsBlockSerde(mockTsBlockSize),
            mockSinkListener,
            mockClientManager);
    sinkChannel.setMaxBytesCanReserve(Long.MAX_VALUE);
    sinkChannel.open();
    Assert.assertTrue(sinkChannel.isFull().isDone());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES, sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkChannel.getNumOfBufferedTsBlocks());

    // Send tsblocks.
    sinkChannel.send(mockTsBlocks.get(0));
    Future<?> blocked = sinkChannel.isFull();
    Assert.assertFalse(blocked.isDone());
    Assert.assertFalse(blocked.isCancelled());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertFalse(sinkChannel.isAborted());
    Assert.assertEquals(
        mockTsBlockSize * numOfMockTsBlock + DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES,
        sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(numOfMockTsBlock, sinkChannel.getNumOfBufferedTsBlocks());

    sinkChannel.abort();
    Assert.assertTrue(blocked.isDone());
    Assert.assertTrue(blocked.isCancelled());
    Assert.assertFalse(sinkChannel.isFinished());
    Assert.assertTrue(sinkChannel.isAborted());
    Assert.assertEquals(0L, sinkChannel.getBufferRetainedSizeInBytes());
    Assert.assertEquals(0, sinkChannel.getNumOfBufferedTsBlocks());
    Mockito.verify(mockSinkListener, Mockito.timeout(10_0000).times(1)).onAborted(sinkChannel);
    Assert.assertEquals(0L, spyMemoryPool.getQueryMemoryReservedBytes(queryId));
  }

  @Test
  public void coordinatedTerminalWaitsForNotificationRpcActualExit() throws Exception {
    for (boolean abort : new boolean[] {false, true}) {
      java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
      java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
      try (CoordinatedSinkFixture f = new CoordinatedSinkFixture()) {
        Mockito.doAnswer(
                call -> {
                  entered.countDown();
                  Assert.assertTrue(release.await(5, java.util.concurrent.TimeUnit.SECONDS));
                  return null;
                })
            .when(f.client)
            .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));
        Mockito.doAnswer(
                call -> {
                  Assert.assertFalse(Thread.holdsLock(f.sink));
                  return null;
                })
            .when(f.listener)
            .onFinish(f.sink);
        Mockito.doAnswer(
                call -> {
                  Assert.assertFalse(Thread.holdsLock(f.sink));
                  return null;
                })
            .when(f.listener)
            .onAborted(f.sink);
        f.sink.open();
        f.sink.send(Utils.createMockTsBlocks(1, CoordinatedSinkFixture.BLOCK_BYTES).get(0));
        Assert.assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
        com.google.common.util.concurrent.ListenableFuture<Void> first =
            abort
                ? f.sink.requestCoordinatedAbort(new RuntimeException("stop"))
                : f.sink.requestCoordinatedClose();
        Assert.assertFalse(first.isDone());
        Assert.assertEquals(0, f.sink.getNumOfBufferedTsBlocks());
        Assert.assertEquals(0, f.pool.getReservedBytes());
        Assert.assertTrue(first.cancel(false));
        com.google.common.util.concurrent.ListenableFuture<Void> second =
            f.sink.requestCoordinatedClose();
        Assert.assertFalse(second.isDone());
        java.util.concurrent.atomic.AtomicBoolean monitorHeld =
            new java.util.concurrent.atomic.AtomicBoolean();
        java.util.concurrent.CountDownLatch observed = new java.util.concurrent.CountDownLatch(1);
        second.addListener(
            () -> {
              monitorHeld.set(Thread.holdsLock(f.sink));
              observed.countDown();
            },
            com.google.common.util.concurrent.MoreExecutors.directExecutor());
        release.countDown();
        second.get(5, java.util.concurrent.TimeUnit.SECONDS);
        Assert.assertTrue(observed.await(5, java.util.concurrent.TimeUnit.SECONDS));
        Assert.assertFalse(monitorHeld.get());
        Mockito.verify(f.listener, Mockito.times(abort ? 1 : 0)).onAborted(f.sink);
        Mockito.verify(f.listener, Mockito.times(abort ? 0 : 1)).onFinish(f.sink);
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  public void coordinatedCloseWaitsForEndNotificationRpcActualExit() throws Exception {
    java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
    try (CoordinatedSinkFixture f = new CoordinatedSinkFixture()) {
      Mockito.doAnswer(
              call -> {
                entered.countDown();
                Assert.assertTrue(release.await(5, java.util.concurrent.TimeUnit.SECONDS));
                return null;
              })
          .when(f.client)
          .onEndOfDataBlockEvent(Mockito.any(TEndOfDataBlockEvent.class));
      f.sink.open();
      f.sink.setNoMoreTsBlocks();
      Assert.assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
      com.google.common.util.concurrent.ListenableFuture<Void> closed =
          f.sink.requestCoordinatedClose();
      Assert.assertTrue(f.sink.isClosed());
      Assert.assertFalse(closed.isDone());
      release.countDown();
      closed.get(5, java.util.concurrent.TimeUnit.SECONDS);
      Mockito.verify(f.listener, Mockito.times(1)).onFinish(f.sink);
      Mockito.verify(f.listener, Mockito.times(1)).onEndOfBlocks(f.sink);
    } finally {
      release.countDown();
    }
  }

  @Test
  public void coordinatedCloseRetainsListenerFailureAfterFreeAndNeverReplays() throws Exception {
    try (CoordinatedSinkFixture f = new CoordinatedSinkFixture()) {
      AssertionError expected = new AssertionError("terminal listener");
      Mockito.doThrow(expected).when(f.listener).onFinish(f.sink);
      f.sink.open();
      f.sink.send(Utils.createMockTsBlocks(1, CoordinatedSinkFixture.BLOCK_BYTES).get(0));
      for (int attempt = 0; attempt < 2; attempt++) {
        java.util.concurrent.ExecutionException failure =
            Assert.assertThrows(
                java.util.concurrent.ExecutionException.class,
                () ->
                    f.sink.requestCoordinatedClose().get(5, java.util.concurrent.TimeUnit.SECONDS));
        Assert.assertSame(expected, failure.getCause());
      }
      Assert.assertEquals(0, f.pool.getReservedBytes());
      Assert.assertEquals(0, f.sink.getNumOfBufferedTsBlocks());
      Mockito.verify(f.listener, Mockito.times(1)).onFinish(f.sink);
    }
  }

  @Test
  public void coordinatedCloseRetainsAmbiguousFreeFailureAndNeverRetries() throws Exception {
    java.util.concurrent.atomic.AtomicInteger frees =
        new java.util.concurrent.atomic.AtomicInteger();
    RuntimeException expected = new RuntimeException("after actual free");
    long unit = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
    MemoryPool pool =
        new MemoryPool("sink-close-failure", new MemoryManager(8 * unit), 5 * unit) {
          @Override
          public void free(String query, String fragment, String node, long bytes) {
            frees.incrementAndGet();
            super.free(query, fragment, node, bytes);
            throw expected;
          }
        };
    try (CoordinatedSinkFixture f =
        new CoordinatedSinkFixture(pool, Executors.newSingleThreadExecutor())) {
      f.sink.open();
      f.sink.send(Utils.createMockTsBlocks(1, CoordinatedSinkFixture.BLOCK_BYTES).get(0));
      for (int attempt = 0; attempt < 2; attempt++) {
        java.util.concurrent.ExecutionException failure =
            Assert.assertThrows(
                java.util.concurrent.ExecutionException.class,
                () ->
                    f.sink.requestCoordinatedClose().get(5, java.util.concurrent.TimeUnit.SECONDS));
        Assert.assertSame(expected, failure.getCause());
      }
      Assert.assertEquals(1, frees.get());
      Assert.assertEquals(0, f.pool.getReservedBytes());
      Assert.assertEquals(0, f.sink.getNumOfBufferedTsBlocks());
    }
  }

  @Test
  public void coordinatedCloseRetainsPublicationRejectionWithoutLeakingOwnership()
      throws Exception {
    java.util.concurrent.ExecutorService executor =
        Mockito.mock(java.util.concurrent.ExecutorService.class);
    java.util.concurrent.RejectedExecutionException expected =
        new java.util.concurrent.RejectedExecutionException("rejected");
    Mockito.doThrow(expected).when(executor).execute(Mockito.any(Runnable.class));
    long unit = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
    try (CoordinatedSinkFixture f =
        new CoordinatedSinkFixture(
            new MemoryPool("sink-rejected", new MemoryManager(8 * unit), 5 * unit), executor)) {
      f.sink.open();
      Assert.assertSame(
          expected,
          Assert.assertThrows(
              java.util.concurrent.RejectedExecutionException.class,
              () ->
                  f.sink.send(
                      Utils.createMockTsBlocks(1, CoordinatedSinkFixture.BLOCK_BYTES).get(0))));
      java.util.concurrent.ExecutionException failure =
          Assert.assertThrows(
              java.util.concurrent.ExecutionException.class,
              () -> f.sink.requestCoordinatedClose().get(5, java.util.concurrent.TimeUnit.SECONDS));
      Assert.assertSame(expected, failure.getCause());
      Assert.assertEquals(0, f.pool.getReservedBytes());
      Assert.assertEquals(0, f.sink.getNumOfBufferedTsBlocks());
    }
  }

  @Test
  public void cancelledRunningRpcCannotAcknowledgeCloseUntilItsActualExit() throws Exception {
    CapturingExecutor executor = new CapturingExecutor();
    java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
    long unit = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
    try (CoordinatedSinkFixture f =
        new CoordinatedSinkFixture(
            new MemoryPool("sink-cancel", new MemoryManager(8 * unit), 5 * unit), executor)) {
      Mockito.doAnswer(
              call -> {
                entered.countDown();
                Assert.assertTrue(
                    com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly(
                        release, 5, java.util.concurrent.TimeUnit.SECONDS));
                return null;
              })
          .when(f.client)
          .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));
      f.sink.open();
      f.sink.send(Utils.createMockTsBlocks(1, CoordinatedSinkFixture.BLOCK_BYTES).get(0));
      Assert.assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
      com.google.common.util.concurrent.ListenableFuture<Void> closed =
          f.sink.requestCoordinatedClose();
      Assert.assertTrue(executor.task.cancel(true));
      Assert.assertTrue(executor.task.isDone());
      Assert.assertFalse(closed.isDone());
      release.countDown();
      java.util.concurrent.ExecutionException failure =
          Assert.assertThrows(
              java.util.concurrent.ExecutionException.class,
              () -> closed.get(5, java.util.concurrent.TimeUnit.SECONDS));
      Assert.assertTrue(failure.getCause() instanceof java.util.concurrent.CancellationException);
      Assert.assertEquals(0, f.pool.getReservedBytes());
    } finally {
      release.countDown();
    }
  }

  @Test
  public void cancelledQueuedRpcSettlesOwnershipWithoutRunningIt() throws Exception {
    java.util.concurrent.ExecutorService executor =
        Mockito.mock(java.util.concurrent.ExecutorService.class);
    java.util.concurrent.atomic.AtomicReference<Future<?>> queued =
        new java.util.concurrent.atomic.AtomicReference<>();
    Mockito.doAnswer(
            call -> {
              queued.set((Future<?>) call.getArgument(0));
              return null;
            })
        .when(executor)
        .execute(Mockito.any(Runnable.class));
    long unit = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
    try (CoordinatedSinkFixture f =
        new CoordinatedSinkFixture(
            new MemoryPool("sink-queued-cancel", new MemoryManager(8 * unit), 5 * unit),
            executor)) {
      f.sink.open();
      f.sink.send(Utils.createMockTsBlocks(1, CoordinatedSinkFixture.BLOCK_BYTES).get(0));
      Assert.assertTrue(queued.get().cancel(false));
      java.util.concurrent.ExecutionException failure =
          Assert.assertThrows(
              java.util.concurrent.ExecutionException.class,
              () -> f.sink.requestCoordinatedClose().get(5, java.util.concurrent.TimeUnit.SECONDS));
      Assert.assertTrue(failure.getCause() instanceof java.util.concurrent.CancellationException);
      Mockito.verify(f.client, Mockito.never())
          .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));
      Assert.assertEquals(0, f.pool.getReservedBytes());
    }
  }

  @Test
  public void pendingMemoryAndTerminalListenersRunOutsideReentrantHandleMonitor() throws Exception {
    long unit = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
    try (CoordinatedSinkFixture f =
        new CoordinatedSinkFixture(
            new MemoryPool("sink-pending", new MemoryManager(unit), unit),
            Executors.newSingleThreadExecutor())) {
      f.sink.open();
      f.sink.send(Utils.createMockTsBlocks(1, CoordinatedSinkFixture.BLOCK_BYTES).get(0));
      com.google.common.util.concurrent.ListenableFuture<?> memory = f.sink.isFull();
      Assert.assertFalse(memory.isDone());
      java.util.concurrent.CountDownLatch observed = new java.util.concurrent.CountDownLatch(1);
      java.util.concurrent.atomic.AtomicBoolean monitorHeld =
          new java.util.concurrent.atomic.AtomicBoolean();
      memory.addListener(
          () -> {
            monitorHeld.set(Thread.holdsLock(f.sink));
            observed.countDown();
          },
          com.google.common.util.concurrent.MoreExecutors.directExecutor());
      Mockito.doAnswer(
              call -> {
                Assert.assertFalse(Thread.holdsLock(f.sink));
                return null;
              })
          .when(f.listener)
          .onFinish(f.sink);
      com.google.common.util.concurrent.ListenableFuture<Void> closed;
      synchronized (f.sink) {
        closed = f.sink.requestCoordinatedClose();
        Assert.assertFalse(closed.isDone());
      }
      closed.get(5, java.util.concurrent.TimeUnit.SECONDS);
      Assert.assertTrue(observed.await(5, java.util.concurrent.TimeUnit.SECONDS));
      Assert.assertFalse(monitorHeld.get());
      Assert.assertEquals(0, f.pool.getReservedBytes());
      Mockito.verify(f.listener, Mockito.times(1)).onFinish(f.sink);
    }
  }

  @Test
  public void callerRunsExecutorDoesNotRunRpcUnderSubmittingHandleMonitor() throws Exception {
    java.util.concurrent.ExecutorService executor =
        Mockito.mock(java.util.concurrent.ExecutorService.class);
    Mockito.doAnswer(
            call -> {
              ((Runnable) call.getArgument(0)).run();
              return null;
            })
        .when(executor)
        .execute(Mockito.any(Runnable.class));
    java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
    long unit = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
    try (CoordinatedSinkFixture f =
        new CoordinatedSinkFixture(
            new MemoryPool("sink-direct", new MemoryManager(8 * unit), 5 * unit), executor)) {
      Mockito.doAnswer(
              call -> {
                Assert.assertFalse(Thread.holdsLock(f.sink));
                entered.countDown();
                Assert.assertTrue(release.await(5, java.util.concurrent.TimeUnit.SECONDS));
                return null;
              })
          .when(f.client)
          .onNewDataBlockEvent(Mockito.any(TNewDataBlockEvent.class));
      f.sink.open();
      f.sink.send(Utils.createMockTsBlocks(1, CoordinatedSinkFixture.BLOCK_BYTES).get(0));
      Assert.assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
      com.google.common.util.concurrent.ListenableFuture<Void> closed =
          f.sink.requestCoordinatedClose();
      Assert.assertFalse(closed.isDone());
      release.countDown();
      closed.get(5, java.util.concurrent.TimeUnit.SECONDS);
    } finally {
      release.countDown();
    }
  }

  @Test
  public void prepaidNativeAckWaitsForResponseScopeBeforeReturningSlotAndClosing()
      throws Exception {
    try (PrepaidSinkFixture f = new PrepaidSinkFixture()) {
      f.sink.open();
      f.sink.send(realFrontierBlock(4));
      Assert.assertFalse(f.sink.isFull().isDone());
      Assert.assertEquals(1, f.reservation.getLeasedCreditCount());
      RpcResponseLeaseScope scope = RpcResponseLeaseScope.enter();
      try {
        java.nio.ByteBuffer encoded = f.sink.getSerializedTsBlock(0);
        Assert.assertEquals(100L, new TsBlockSerde().deserialize(encoded).getColumn(0).getLong(0));
        f.sink.acknowledgeTsBlock(0, 1);
        Assert.assertEquals(0, f.sink.getNumOfBufferedTsBlocks());
        Assert.assertEquals(1, f.reservation.getLeasedCreditCount());
        Assert.assertFalse(f.sink.isFull().isDone());
        com.google.common.util.concurrent.ListenableFuture<Void> closed =
            f.sink.requestCoordinatedClose();
        com.google.common.util.concurrent.ListenableFuture<Void> ownerClosed =
            f.reservation.requestClose();
        Assert.assertFalse(closed.isDone());
        Assert.assertFalse(ownerClosed.isDone());
        Assert.assertEquals(f.prepaidBytes, f.pool.getReservedBytes());
        scope.close();
        closed.get(5, java.util.concurrent.TimeUnit.SECONDS);
        ownerClosed.get(5, java.util.concurrent.TimeUnit.SECONDS);
        Assert.assertEquals(0, f.pool.getReservedBytes());
        Assert.assertEquals(0, f.sink.getRuntimeResponsePinCount());
      } finally {
        scope.close();
      }
    }
  }

  @Test
  public void prepaidNativeAckUnblocksExactlyOneNextBlockWithoutLegacyPoolRequests()
      throws Exception {
    try (PrepaidSinkFixture f = new PrepaidSinkFixture()) {
      f.sink.open();
      f.sink.send(realFrontierBlock(4));
      com.google.common.util.concurrent.ListenableFuture<?> observer = f.sink.isFull();
      Assert.assertTrue(observer.cancel(true));
      f.sink.acknowledgeTsBlock(0, 1);
      Assert.assertTrue(f.sink.isFull().isDone());
      Assert.assertEquals(f.prepaidBytes, f.pool.getReservedBytes());
      f.sink.send(realFrontierBlock(3));
      Assert.assertFalse(f.sink.isFull().isDone());
      Assert.assertEquals(1, f.reservation.getLeasedCreditCount());
      f.sink.acknowledgeTsBlock(0, 1);
      Assert.assertEquals(1, f.reservation.getLeasedCreditCount());
      f.sink.acknowledgeTsBlock(1, 2);
      Assert.assertEquals(0, f.reservation.getLeasedCreditCount());
      Assert.assertEquals(f.prepaidBytes, f.pool.getReservedBytes());
      Assert.assertEquals(0, f.pool.getMemoryReservationSize());
    }
  }

  @Test
  public void prepaidNativeCopyFailureReturnsCreditAndRawResponseBorrowFailsClosed()
      throws Exception {
    try (PrepaidSinkFixture f = new PrepaidSinkFixture()) {
      f.sink.open();
      Assert.assertThrows(RuntimeException.class, () -> f.sink.send(realFrontierBlock(2048)));
      f.reservation.whenWritable(0).get(5, java.util.concurrent.TimeUnit.SECONDS);
      Assert.assertEquals(0, f.reservation.getLeasedCreditCount());
      Assert.assertEquals(0, f.sink.getNumOfBufferedTsBlocks());
      f.sink.send(realFrontierBlock(2));
      Assert.assertThrows(IllegalStateException.class, () -> f.sink.getSerializedTsBlock(0));
      Assert.assertEquals(1, f.reservation.getLeasedCreditCount());
      Assert.assertEquals(f.prepaidBytes, f.pool.getReservedBytes());
    }
  }

  @Test
  public void prepaidNativeDictionaryPartitionIsCopiedIntoIndependentFlatPayload()
      throws Exception {
    try (PrepaidSinkFixture f = new PrepaidSinkFixture()) {
      TsBlock base = realFrontierBlock(4);
      org.apache.tsfile.block.column.Column dictionary =
          org.apache.tsfile.read.common.block.column.DictionaryColumn.create(
              4, base.getColumn(0), new int[] {3, 1, 2, 0});
      TsBlock partition = new TsBlock(4, base.getTimeColumn(), dictionary);
      f.sink.open();
      f.sink.send(partition);
      base.getColumn(0).getLongs()[3] = -999;
      try (RpcResponseLeaseScope scope = RpcResponseLeaseScope.enter()) {
        TsBlock copied = new TsBlockSerde().deserialize(f.sink.getSerializedTsBlock(0));
        Assert.assertEquals(103L, copied.getColumn(0).getLong(0));
        Assert.assertEquals(101L, copied.getColumn(0).getLong(1));
      }
    }
  }

  @Test
  public void prepaidNativeProcessorFlushBarrierRetainsEscapedResponseBacking() throws Exception {
    java.util.concurrent.CountDownLatch flushing = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.ExecutorService rpc = Executors.newSingleThreadExecutor();
    try (PrepaidSinkFixture f = new PrepaidSinkFixture()) {
      f.sink.open();
      f.sink.send(realFrontierBlock(4));
      org.apache.thrift.transport.TTransport transport =
          Mockito.mock(org.apache.thrift.transport.TTransport.class);
      Mockito.doAnswer(
              call -> {
                flushing.countDown();
                Assert.assertTrue(release.await(5, java.util.concurrent.TimeUnit.SECONDS));
                return null;
              })
          .when(transport)
          .flush();
      org.apache.thrift.protocol.TProtocol output =
          Mockito.mock(org.apache.thrift.protocol.TProtocol.class);
      Mockito.when(output.getTransport()).thenReturn(transport);
      org.apache.thrift.TProcessor wrapped =
          RuntimeExchangeResponseProcessor.wrap(
              (input, out) -> {
                try {
                  java.nio.ByteBuffer response = f.sink.getSerializedTsBlock(0);
                  out.writeBinary(response);
                  out.getTransport().flush();
                } catch (java.io.IOException failure) {
                  throw new org.apache.thrift.TException(failure);
                }
              });
      Future<?> response =
          rpc.submit(
              () -> {
                try {
                  wrapped.process(null, output);
                } catch (org.apache.thrift.TException failure) {
                  throw new RuntimeException(failure);
                }
              });
      Assert.assertTrue(flushing.await(5, java.util.concurrent.TimeUnit.SECONDS));
      f.sink.acknowledgeTsBlock(0, 1);
      com.google.common.util.concurrent.ListenableFuture<Void> closed =
          f.sink.requestCoordinatedClose();
      Assert.assertFalse(closed.isDone());
      Assert.assertEquals(1, f.reservation.getLeasedCreditCount());
      Assert.assertEquals(1, f.sink.getRuntimeResponsePinCount());
      release.countDown();
      response.get(5, java.util.concurrent.TimeUnit.SECONDS);
      closed.get(5, java.util.concurrent.TimeUnit.SECONDS);
      Assert.assertEquals(0, f.reservation.getLeasedCreditCount());
    } finally {
      release.countDown();
      rpc.shutdownNow();
    }
  }

  private static TsBlock realFrontierBlock(int positions) {
    long[] times = new long[positions];
    long[] values = new long[positions];
    for (int i = 0; i < positions; i++) {
      times[i] = i;
      values[i] = 100 + i;
    }
    return new TsBlock(
        positions,
        new org.apache.tsfile.read.common.block.column.TimeColumn(positions, times),
        new org.apache.tsfile.read.common.block.column.LongColumn(
            positions, java.util.Optional.empty(), values));
  }

  private static final class PrepaidSinkFixture implements AutoCloseable {
    final RuntimeExchangeBlockCodec.Bounds bounds =
        new RuntimeExchangeBlockCodec.Bounds(128, 1, 4096, 4096, 4096, 8192, 8192)
            .withSchema(List.of(org.apache.tsfile.enums.TSDataType.INT64));
    final long prepaidBytes = bounds.outputEnvelopeBytes();
    final MemoryPool pool =
        new MemoryPool("prepaid-sink", new MemoryManager(4 * prepaidBytes), 2 * prepaidBytes) {
          @Override
          public MemoryReservationResult reserveWithPriority(
              String q, String f, String n, long bytes, long cap, boolean highest) {
            throw new AssertionError("Runtime endpoint attempted a legacy pool reservation");
          }
        };
    final org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation reservation =
        org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation.tryCreate(
                pool,
                new SharedTsBlockQueue.MemoryReservationKey("prepaid_sink", "1.0", "native"),
                org.apache.iotdb.db.queryengine.execution.memory.MemorySide.OUTPUT,
                1,
                prepaidBytes,
                2 * prepaidBytes)
            .get();
    final java.util.concurrent.ExecutorService executor = Executors.newSingleThreadExecutor();
    final SinkChannel sink;

    @SuppressWarnings("unchecked")
    PrepaidSinkFixture() throws Exception {
      LocalMemoryManager memory = Mockito.mock(LocalMemoryManager.class);
      Mockito.when(memory.getQueryPool()).thenReturn(pool);
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clients =
          Mockito.mock(IClientManager.class);
      TEndPoint endpoint = new TEndPoint("prepaid-sink", 12345);
      Mockito.when(clients.borrowClient(endpoint))
          .thenReturn(Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class));
      sink =
          new SinkChannel(
              endpoint,
              new TFragmentInstanceId("prepaid_sink", 0, "0"),
              "remote",
              "native",
              new TFragmentInstanceId("prepaid_sink", 1, "0"),
              memory,
              executor,
              new TsBlockSerde(),
              Mockito.mock(SinkListener.class),
              false,
              clients,
              new org.apache.iotdb.db.queryengine.execution.runtime.RuntimeEndpointFrontier(
                  reservation, 0, bounds));
      sink.setRetryIntervalInMs(0);
    }

    @Override
    public void close() throws Exception {
      try {
        sink.requestCoordinatedAbort().get(5, java.util.concurrent.TimeUnit.SECONDS);
      } finally {
        reservation.requestClose().get(5, java.util.concurrent.TimeUnit.SECONDS);
        executor.shutdownNow();
      }
    }
  }

  private static final class CapturingExecutor
      extends java.util.concurrent.AbstractExecutorService {
    final java.util.concurrent.ExecutorService delegate = Executors.newSingleThreadExecutor();
    volatile Future<?> task;

    @Override
    public void execute(Runnable command) {
      task = (Future<?>) command;
      delegate.execute(command);
    }

    @Override
    public void shutdown() {
      delegate.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
      return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, java.util.concurrent.TimeUnit unit)
        throws InterruptedException {
      return delegate.awaitTermination(timeout, unit);
    }
  }

  private static final class CoordinatedSinkFixture implements AutoCloseable {
    static final long BLOCK_BYTES = 128 * 1024L;
    final MemoryPool pool;
    final java.util.concurrent.ExecutorService executor;
    final SyncDataNodeMPPDataExchangeServiceClient client =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    final SinkListener listener = Mockito.mock(SinkListener.class);
    final SinkChannel sink;

    CoordinatedSinkFixture() throws Exception {
      this(
          new MemoryPool(
              "coordinated-sink",
              new MemoryManager(8L * DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES),
              5L * DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES),
          Executors.newSingleThreadExecutor());
    }

    @SuppressWarnings("unchecked")
    CoordinatedSinkFixture(MemoryPool pool, java.util.concurrent.ExecutorService executor)
        throws Exception {
      this.pool = pool;
      this.executor = executor;
      LocalMemoryManager memory = Mockito.mock(LocalMemoryManager.class);
      Mockito.when(memory.getQueryPool()).thenReturn(pool);
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clients =
          Mockito.mock(IClientManager.class);
      TEndPoint endpoint = new TEndPoint("coordinated-sink", 12345);
      Mockito.when(clients.borrowClient(endpoint)).thenReturn(client);
      sink =
          new SinkChannel(
              endpoint,
              new TFragmentInstanceId("coordinated_sink", 0, "0"),
              "source",
              "sink",
              new TFragmentInstanceId("coordinated_sink", 1, "0"),
              memory,
              executor,
              Utils.createMockTsBlockSerde(BLOCK_BYTES),
              listener,
              clients);
      sink.setMaxBytesCanReserve(5L * DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES);
      sink.setRetryIntervalInMs(0);
    }

    @Override
    public void close() {
      try {
        sink.abort();
      } catch (Throwable ignored) {
        // Tests assert the original terminal receipt.
      } finally {
        executor.shutdownNow();
      }
    }
  }
}
