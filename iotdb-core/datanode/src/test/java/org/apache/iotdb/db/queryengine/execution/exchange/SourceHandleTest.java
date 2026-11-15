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
import org.apache.iotdb.db.exception.runtime.MemoryLeakException;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SourceHandleListener;
import org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.mpp.rpc.thrift.TAcknowledgeDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockRequest;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockResponse;

import org.apache.thrift.TException;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SourceHandleTest {
  private static final long MOCK_TSBLOCK_SIZE = 1024L * 1024L;

  private static long maxBytesPerFI;

  @BeforeClass
  public static void beforeClass() {
    maxBytesPerFI =
        IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance();
    IoTDBDescriptor.getInstance()
        .getMemoryConfig()
        .setMaxBytesPerFragmentInstance(5 * MOCK_TSBLOCK_SIZE);
  }

  @AfterClass
  public static void afterClass() {
    IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(maxBytesPerFI);
  }

  @Test
  public void testPendingHandleCanCloseAfterForcedMemoryDeregistration() throws Exception {
    for (int closeMode = 0; closeMode < 3; closeMode++) {
      MemoryPool memoryPool =
          new MemoryPool("pending", new MemoryManager(MOCK_TSBLOCK_SIZE), MOCK_TSBLOCK_SIZE);
      memoryPool.registerPlanNodeIdToQueryMemoryMap(
          "holder",
          "0.0",
          "holder",
          org.apache.iotdb.db.queryengine.execution.memory.MemorySide.INPUT);
      Assert.assertTrue(
          memoryPool.tryReserve("holder", "0.0", "holder", MOCK_TSBLOCK_SIZE, MOCK_TSBLOCK_SIZE));
      LocalMemoryManager manager = Mockito.mock(LocalMemoryManager.class);
      Mockito.when(manager.getQueryPool()).thenReturn(memoryPool);
      ExecutorService executor = Mockito.mock(ExecutorService.class);
      Mockito.doAnswer(
              invocation -> {
                ((Runnable) invocation.getArgument(0)).run();
                return null;
              })
          .when(executor)
          .execute(Mockito.any(Runnable.class));
      SourceHandleListener listener = Mockito.mock(SourceHandleListener.class);
      SourceHandle source =
          new SourceHandle(
              new TEndPoint("remote", 12345),
              new TFragmentInstanceId("pending", 1, "0"),
              new TFragmentInstanceId("pending", 0, "0"),
              "src",
              0,
              manager,
              executor,
              new TsBlockSerde(),
              listener,
              Mockito.mock(IClientManager.class));
      source.isBlocked();
      source.updatePendingDataBlockInfo(0, List.of(MOCK_TSBLOCK_SIZE));
      Assert.assertEquals(1, memoryPool.getMemoryReservationSize());
      memoryPool.deRegisterFragmentInstanceFromQueryMemoryMap("pending", "0.0", true);
      // Detachment must not schedule an RPC for an unreserved block.
      Mockito.verify(executor, Mockito.never()).execute(Mockito.any(Runnable.class));
      Mockito.verify(executor, Mockito.never()).submit(Mockito.any(Runnable.class));
      memoryPool.free("holder", "0.0", "holder", MOCK_TSBLOCK_SIZE);
      if (closeMode == 0) {
        source.abort();
      } else if (closeMode == 1) {
        source.abort(new RuntimeException("cancel query"));
      } else {
        source.close();
      }
      Assert.assertEquals(0, source.getBufferRetainedSizeInBytes());
      Assert.assertEquals(0, memoryPool.getMemoryReservationSize());
      Assert.assertEquals(0, memoryPool.getReservedBytes());
      if (closeMode < 2) {
        Assert.assertTrue(source.isAborted());
        Mockito.verify(listener).onAborted(source);
      } else {
        Mockito.verify(listener).onFinished(source);
      }
      memoryPool.deRegisterFragmentInstanceFromQueryMemoryMap("holder", "0.0", true);
      Assert.assertEquals(0, memoryPool.getQueryMemoryReservationSize());
    }
  }

  @Test
  public void testCloseStopsFragmentFetchAfterInFlightRpc() throws Exception {
    testCancellationStopsFragmentFetchAfterInFlightRpc(false);
  }

  @Test
  public void testAbortStopsFragmentFetchAfterInFlightRpc() throws Exception {
    testCancellationStopsFragmentFetchAfterInFlightRpc(true);
  }

  @Test
  public void reproduceAbortAfterFragmentInstanceMemoryIsForceDeregistered() throws Exception {
    final String queryId = "q0";
    final String fragmentInstanceId = "0.0";
    final String localPlanNodeId = "exchange_0";
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final MemoryPool memoryPool =
        new MemoryPool("test", new MemoryManager(2 * MOCK_TSBLOCK_SIZE), 2 * MOCK_TSBLOCK_SIZE);
    final LocalMemoryManager localMemoryManager = Mockito.mock(LocalMemoryManager.class);
    Mockito.when(localMemoryManager.getQueryPool()).thenReturn(memoryPool);
    final ExecutorService executorService = Mockito.mock(ExecutorService.class);
    final SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
            0,
            localMemoryManager,
            executorService,
            Utils.createMockTsBlockSerde(MOCK_TSBLOCK_SIZE),
            Mockito.mock(SourceHandleListener.class),
            Mockito.mock(IClientManager.class));

    sourceHandle.isBlocked();
    sourceHandle.updatePendingDataBlockInfo(0, List.of(MOCK_TSBLOCK_SIZE));
    Assert.assertEquals(MOCK_TSBLOCK_SIZE, sourceHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(MOCK_TSBLOCK_SIZE, memoryPool.getReservedBytes());

    final CountDownLatch memoryDeregistered = new CountDownLatch(1);
    final AtomicReference<Throwable> deregisterFailure = new AtomicReference<>();
    final AtomicReference<Throwable> abortFailure = new AtomicReference<>();
    final Thread fragmentInstanceCleanupThread =
        new Thread(
            () -> {
              try {
                memoryPool.deRegisterFragmentInstanceFromQueryMemoryMap(
                    queryId, fragmentInstanceId, true);
              } catch (Throwable t) {
                deregisterFailure.set(t);
              } finally {
                memoryDeregistered.countDown();
              }
            });
    final Thread driverSchedulerCleanupThread =
        new Thread(
            () -> {
              try {
                if (!memoryDeregistered.await(10, TimeUnit.SECONDS)) {
                  abortFailure.set(
                      new AssertionError("Timed out waiting for FI memory deregistration"));
                  return;
                }
                sourceHandle.abort();
              } catch (Throwable t) {
                abortFailure.set(t);
              }
            });

    fragmentInstanceCleanupThread.start();
    driverSchedulerCleanupThread.start();
    fragmentInstanceCleanupThread.join(TimeUnit.SECONDS.toMillis(10));
    driverSchedulerCleanupThread.join(TimeUnit.SECONDS.toMillis(10));

    Assert.assertFalse(fragmentInstanceCleanupThread.isAlive());
    Assert.assertFalse(driverSchedulerCleanupThread.isAlive());
    Assert.assertTrue(deregisterFailure.get() instanceof MemoryLeakException);
    Assert.assertTrue(abortFailure.get() instanceof IllegalArgumentException);
    Assert.assertEquals(
        "RelatedMemoryReserved can't be null when freeing memory", abortFailure.get().getMessage());
    Assert.assertEquals(MOCK_TSBLOCK_SIZE, sourceHandle.getBufferRetainedSizeInBytes());
    Assert.assertEquals(MOCK_TSBLOCK_SIZE, memoryPool.getReservedBytes());
  }

  private void testCancellationStopsFragmentFetchAfterInFlightRpc(boolean abort) throws Exception {
    final String queryId = "q0";
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final LocalMemoryManager localMemoryManager = Mockito.mock(LocalMemoryManager.class);
    final MemoryPool memoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(localMemoryManager.getQueryPool()).thenReturn(memoryPool);
    final SourceHandleListener sourceHandleListener = Mockito.mock(SourceHandleListener.class);
    final IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clientManager =
        Mockito.mock(IClientManager.class);
    final SyncDataNodeMPPDataExchangeServiceClient client =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    final CountDownLatch rpcStarted = new CountDownLatch(1);
    final CountDownLatch returnResponse = new CountDownLatch(1);
    Mockito.when(clientManager.borrowClient(remoteEndpoint)).thenReturn(client);
    Mockito.doAnswer(
            invocation -> {
              rpcStarted.countDown();
              Assert.assertTrue(returnResponse.await(10, TimeUnit.SECONDS));
              return new TGetDataBlockResponse(List.of(ByteBuffer.wrap(new byte[] {1, 2})))
                  .setOffset(2)
                  .setTotalLength(6);
            })
        .when(client)
        .getDataBlock(Mockito.any(TGetDataBlockRequest.class));

    final SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            "exchange_0",
            0,
            localMemoryManager,
            Executors.newSingleThreadExecutor(),
            Utils.createMockTsBlockSerde(MOCK_TSBLOCK_SIZE),
            sourceHandleListener,
            clientManager);
    sourceHandle.isBlocked();
    sourceHandle.updatePendingDataBlockInfo(0, List.of(MOCK_TSBLOCK_SIZE));
    Assert.assertTrue(rpcStarted.await(10, TimeUnit.SECONDS));

    if (abort) {
      sourceHandle.abort();
    } else {
      sourceHandle.close();
    }
    returnResponse.countDown();

    Mockito.verify(client, Mockito.timeout(10_000).times(1))
        .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    Mockito.verify(client, Mockito.after(500).times(1))
        .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    Mockito.verify(sourceHandleListener, Mockito.never())
        .onFailure(Mockito.eq(sourceHandle), Mockito.any());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
  }

  @Test
  public void testFragmentAllocationErrorFailsWithoutRetry() throws Exception {
    final String queryId = "q0";
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final LocalMemoryManager localMemoryManager = Mockito.mock(LocalMemoryManager.class);
    final MemoryPool memoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(localMemoryManager.getQueryPool()).thenReturn(memoryPool);
    final SourceHandleListener sourceHandleListener = Mockito.mock(SourceHandleListener.class);
    final IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clientManager =
        Mockito.mock(IClientManager.class);
    final SyncDataNodeMPPDataExchangeServiceClient client =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    Mockito.when(clientManager.borrowClient(remoteEndpoint)).thenReturn(client);
    Mockito.when(client.getDataBlock(Mockito.any(TGetDataBlockRequest.class)))
        .thenReturn(
            new TGetDataBlockResponse(List.of(ByteBuffer.wrap(new byte[] {1})))
                .setOffset(1)
                .setTotalLength(Integer.MAX_VALUE));

    final SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            "exchange_0",
            0,
            localMemoryManager,
            Executors.newSingleThreadExecutor(),
            Utils.createMockTsBlockSerde(MOCK_TSBLOCK_SIZE),
            sourceHandleListener,
            clientManager);
    sourceHandle.isBlocked();
    sourceHandle.updatePendingDataBlockInfo(0, List.of(MOCK_TSBLOCK_SIZE));

    Mockito.verify(sourceHandleListener, Mockito.timeout(10_000))
        .onFailure(Mockito.eq(sourceHandle), Mockito.any(OutOfMemoryError.class));
    Mockito.verify(client, Mockito.times(1)).getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
    sourceHandle.abort();
  }

  @Test
  public void testFragmentAssemblyFailsWhenTotalLengthDoesNotMatch() {
    final String queryId = "q0";
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");

    LocalMemoryManager localMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool memoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(localMemoryManager.getQueryPool()).thenReturn(memoryPool);
    SourceHandleListener sourceHandleListener = Mockito.mock(SourceHandleListener.class);
    TsBlockSerde serde = Utils.createMockTsBlockSerde(MOCK_TSBLOCK_SIZE);
    IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clientManager =
        Mockito.mock(IClientManager.class);
    SyncDataNodeMPPDataExchangeServiceClient client =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    AtomicInteger rpcCount = new AtomicInteger();
    try {
      Mockito.when(clientManager.borrowClient(remoteEndpoint)).thenReturn(client);
      Mockito.doAnswer(
              invocation -> {
                if (rpcCount.getAndIncrement() == 0) {
                  return new TGetDataBlockResponse(List.of(ByteBuffer.wrap(new byte[] {1, 2})))
                      .setOffset(2)
                      .setTotalLength(5);
                }
                return new TGetDataBlockResponse(List.of(ByteBuffer.wrap(new byte[] {3, 4})));
              })
          .when(client)
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    } catch (ClientManagerException | TException e) {
      Assert.fail(e.getMessage());
    }

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            "exchange_0",
            0,
            localMemoryManager,
            Executors.newSingleThreadExecutor(),
            serde,
            sourceHandleListener,
            clientManager);
    Assert.assertFalse(sourceHandle.isBlocked().isDone());
    sourceHandle.updatePendingDataBlockInfo(0, List.of(MOCK_TSBLOCK_SIZE));
    try {
      Mockito.verify(client, Mockito.timeout(10_000).times(2))
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    } catch (TException e) {
      Assert.fail(e.getMessage());
    }
    Mockito.verify(sourceHandleListener, Mockito.timeout(10_000))
        .onFailure(Mockito.eq(sourceHandle), Mockito.any(IllegalStateException.class));
    Mockito.verify(serde, Mockito.never()).deserialize(Mockito.any(ByteBuffer.class));
    sourceHandle.abort();
  }

  @Test
  public void testNonBlockedOneTimeReceive() {
    final String queryId = "q0";
    final int numOfMockTsBlock = 10;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final String localPlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");

    // Construct a mock LocalMemoryManager that do not block any reservation.
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
      Mockito.doAnswer(
              invocation -> {
                TGetDataBlockRequest req = invocation.getArgument(0);
                List<ByteBuffer> byteBuffers =
                    new ArrayList<>(req.getEndSequenceId() - req.getStartSequenceId());
                for (int i = 0; i < req.getEndSequenceId() - req.getStartSequenceId(); i++) {
                  byteBuffers.add(ByteBuffer.allocate(0));
                }
                return new TGetDataBlockResponse(byteBuffers);
              })
          .when(mockClient)
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    } catch (ClientManagerException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a mock TsBlockSerde that deserializes any bytebuffer into a mock TsBlock.
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(MOCK_TSBLOCK_SIZE);

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
            0,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockTsBlockSerde,
            mockSourceHandleListener,
            mockClientManager);
    Assert.assertFalse(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    // New data blocks event arrived.
    sourceHandle.updatePendingDataBlockInfo(
        0,
        Stream.generate(() -> MOCK_TSBLOCK_SIZE)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
          .getDataBlock(
              Mockito.argThat(
                  req ->
                      remoteFragmentInstanceId.equals(req.getSourceFragmentInstanceId())
                          && 0 == req.getStartSequenceId()
                          && numOfMockTsBlock == req.getEndSequenceId()));
      Mockito.verify(mockClient, Mockito.timeout(10_0000).times(1))
          .onAcknowledgeDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && 0 == e.getStartSequenceId()
                          && numOfMockTsBlock == e.getEndSequenceId()));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(
        numOfMockTsBlock * MOCK_TSBLOCK_SIZE, sourceHandle.getBufferRetainedSizeInBytes());

    // The local fragment instance consumes the data blocks.
    for (int i = 0; i < numOfMockTsBlock; i++) {
      sourceHandle.receive();
      if (i < numOfMockTsBlock - 1) {
        Assert.assertTrue(sourceHandle.isBlocked().isDone());
      } else {
        Assert.assertFalse(sourceHandle.isBlocked().isDone());
      }
      Assert.assertFalse(sourceHandle.isAborted());
      Assert.assertFalse(sourceHandle.isFinished());
      Assert.assertEquals(
          (numOfMockTsBlock - 1 - i) * MOCK_TSBLOCK_SIZE,
          sourceHandle.getBufferRetainedSizeInBytes());
    }

    // Receive EndOfDataBlock event from upstream fragment instance.
    sourceHandle.setNoMoreTsBlocks(numOfMockTsBlock - 1);
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertTrue(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockSourceHandleListener, Mockito.timeout(10_0000).times(1))
        .onFinished(sourceHandle);
  }

  @Test
  public void testBlockedOneTimeReceive() {
    final String queryId = "q0";
    final int numOfMockTsBlock = 10;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final String localPlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");

    // Construct a mock LocalMemoryManager with capacity 5 * MOCK_TSBLOCK_SIZE per query.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryManager memoryManager = Mockito.spy(new MemoryManager(10 * MOCK_TSBLOCK_SIZE));
    MemoryPool spyMemoryPool =
        Mockito.spy(new MemoryPool("test", memoryManager, 5 * MOCK_TSBLOCK_SIZE));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);
    IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> mockClientManager =
        Mockito.mock(IClientManager.class);
    // Construct a mock client.
    SyncDataNodeMPPDataExchangeServiceClient mockClient =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    try {
      Mockito.when(mockClientManager.borrowClient(remoteEndpoint)).thenReturn(mockClient);
      Mockito.doAnswer(
              invocation -> {
                TGetDataBlockRequest req = invocation.getArgument(0);
                List<ByteBuffer> byteBuffers =
                    new ArrayList<>(req.getEndSequenceId() - req.getStartSequenceId());
                for (int i = 0; i < req.getEndSequenceId() - req.getStartSequenceId(); i++) {
                  byteBuffers.add(ByteBuffer.allocate(0));
                }
                return new TGetDataBlockResponse(byteBuffers);
              })
          .when(mockClient)
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    } catch (ClientManagerException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a mock TsBlockSerde that deserializes any bytebuffer into a mock TsBlock.
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(MOCK_TSBLOCK_SIZE);

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
            0,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockTsBlockSerde,
            mockSourceHandleListener,
            mockClientManager);
    long maxBytesCanReserve =
        Math.min(
            5 * MOCK_TSBLOCK_SIZE,
            IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance());
    sourceHandle.setMaxBytesCanReserve(maxBytesCanReserve);
    Assert.assertFalse(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    // New data blocks event arrived.
    sourceHandle.updatePendingDataBlockInfo(
        0,
        Stream.generate(() -> MOCK_TSBLOCK_SIZE)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    try {
      Mockito.verify(spyMemoryPool, Mockito.timeout(10_000).times(6))
          .reserveWithPriority(
              queryId,
              FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                  localFragmentInstanceId),
              localPlanNodeId,
              MOCK_TSBLOCK_SIZE,
              maxBytesCanReserve,
              false);
      Mockito.verify(mockClient, Mockito.timeout(10_0000).times(1))
          .getDataBlock(
              Mockito.argThat(
                  req ->
                      remoteFragmentInstanceId.equals(req.getSourceFragmentInstanceId())
                          && 0 == req.getStartSequenceId()
                          && 5 == req.getEndSequenceId()));
      Mockito.verify(mockClient, Mockito.timeout(10_0000).times(1))
          .onAcknowledgeDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && 0 == e.getStartSequenceId()
                          && 5 == e.getEndSequenceId()));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(6 * MOCK_TSBLOCK_SIZE, sourceHandle.getBufferRetainedSizeInBytes());

    // The local fragment instance consumes the data blocks.
    for (int i = 0; i < numOfMockTsBlock; i++) {
      Mockito.verify(spyMemoryPool, Mockito.timeout(10_0000).times(i))
          .free(
              queryId,
              FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(
                  localFragmentInstanceId),
              localPlanNodeId,
              MOCK_TSBLOCK_SIZE);
      sourceHandle.receive();
      try {
        if (i < 5) {
          Assert.assertEquals(
              i == 4 ? 5 * MOCK_TSBLOCK_SIZE : 6 * MOCK_TSBLOCK_SIZE,
              sourceHandle.getBufferRetainedSizeInBytes());
          final int startSequenceId = 5 + i;
          Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
              .getDataBlock(
                  Mockito.argThat(
                      req ->
                          remoteFragmentInstanceId.equals(req.getSourceFragmentInstanceId())
                              && startSequenceId == req.getStartSequenceId()
                              && startSequenceId + 1 == req.getEndSequenceId()));
          Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
              .onAcknowledgeDataBlockEvent(
                  Mockito.argThat(
                      e ->
                          remoteFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                              && startSequenceId == e.getStartSequenceId()
                              && startSequenceId + 1 == e.getEndSequenceId()));
        } else {
          Assert.assertEquals(
              (numOfMockTsBlock - 1 - i) * MOCK_TSBLOCK_SIZE,
              sourceHandle.getBufferRetainedSizeInBytes());
        }
      } catch (TException e) {
        e.printStackTrace();
        Assert.fail();
      }
      if (i < numOfMockTsBlock - 1) {
        Assert.assertTrue(sourceHandle.isBlocked().isDone());
      } else {
        Assert.assertFalse(sourceHandle.isBlocked().isDone());
      }
      Assert.assertFalse(sourceHandle.isAborted());
      Assert.assertFalse(sourceHandle.isFinished());
    }

    // Receive EndOfDataBlock event from upstream fragment instance.
    sourceHandle.setNoMoreTsBlocks(numOfMockTsBlock - 1);
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertTrue(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockSourceHandleListener, Mockito.timeout(10_0000).times(1))
        .onFinished(sourceHandle);
  }

  @Test
  public void testMultiTimesReceive() {
    final String queryId = "q0";
    final long MOCK_TSBLOCK_SIZE = 1024L * 1024L;
    final int numOfMockTsBlock = 10;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final String localPlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");

    // Construct a mock LocalMemoryManager that returns unblocked futures.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a mock TsBlockSerde that deserializes any bytebuffer into a mock TsBlock.
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(MOCK_TSBLOCK_SIZE);
    IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> mockClientManager =
        Mockito.mock(IClientManager.class);
    // Construct a mock client.
    SyncDataNodeMPPDataExchangeServiceClient mockClient =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    try {
      Mockito.when(mockClientManager.borrowClient(remoteEndpoint)).thenReturn(mockClient);
      Mockito.doAnswer(
              invocation -> {
                TGetDataBlockRequest req = invocation.getArgument(0);
                List<ByteBuffer> byteBuffers =
                    new ArrayList<>(req.getEndSequenceId() - req.getStartSequenceId());
                for (int i = 0; i < req.getEndSequenceId() - req.getStartSequenceId(); i++) {
                  byteBuffers.add(ByteBuffer.allocate(0));
                }
                return new TGetDataBlockResponse(byteBuffers);
              })
          .when(mockClient)
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    } catch (ClientManagerException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
            0,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockTsBlockSerde,
            mockSourceHandleListener,
            mockClientManager);
    Assert.assertFalse(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    // New data blocks event arrived in unordered manner.
    sourceHandle.updatePendingDataBlockInfo(
        numOfMockTsBlock,
        Stream.generate(() -> MOCK_TSBLOCK_SIZE)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(0))
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
      Mockito.verify(mockClient, Mockito.timeout(10_0000).times(0))
          .onAcknowledgeDataBlockEvent(Mockito.any(TAcknowledgeDataBlockEvent.class));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    Assert.assertFalse(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    sourceHandle.updatePendingDataBlockInfo(
        0,
        Stream.generate(() -> MOCK_TSBLOCK_SIZE)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
          .getDataBlock(
              Mockito.argThat(
                  req ->
                      remoteFragmentInstanceId.equals(req.getSourceFragmentInstanceId())
                          && 0 == req.getStartSequenceId()
                          && numOfMockTsBlock * 2 == req.getEndSequenceId()));
      Mockito.verify(mockClient, Mockito.timeout(10_0000).times(1))
          .onAcknowledgeDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && 0 == e.getStartSequenceId()
                          && numOfMockTsBlock * 2 == e.getEndSequenceId()));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(
        numOfMockTsBlock * 2 * MOCK_TSBLOCK_SIZE, sourceHandle.getBufferRetainedSizeInBytes());

    // The local fragment instance consumes the data blocks.
    for (int i = 0; i < 2 * numOfMockTsBlock; i++) {
      sourceHandle.receive();
      if (i < 2 * numOfMockTsBlock - 1) {
        Assert.assertTrue(sourceHandle.isBlocked().isDone());
      } else {
        Assert.assertFalse(sourceHandle.isBlocked().isDone());
      }
      Assert.assertFalse(sourceHandle.isAborted());
      Assert.assertFalse(sourceHandle.isFinished());
      Assert.assertEquals(
          (2 * numOfMockTsBlock - 1 - i) * MOCK_TSBLOCK_SIZE,
          sourceHandle.getBufferRetainedSizeInBytes());
    }

    // New data blocks event arrived.
    sourceHandle.updatePendingDataBlockInfo(
        numOfMockTsBlock * 2,
        Stream.generate(() -> MOCK_TSBLOCK_SIZE)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(1))
          .getDataBlock(
              Mockito.argThat(
                  req ->
                      remoteFragmentInstanceId.equals(req.getSourceFragmentInstanceId())
                          && numOfMockTsBlock * 2 == req.getStartSequenceId()
                          && numOfMockTsBlock * 3 == req.getEndSequenceId()));
      Mockito.verify(mockClient, Mockito.timeout(10_0000).times(1))
          .onAcknowledgeDataBlockEvent(
              Mockito.argThat(
                  e ->
                      remoteFragmentInstanceId.equals(e.getSourceFragmentInstanceId())
                          && numOfMockTsBlock * 2 == e.getStartSequenceId()
                          && numOfMockTsBlock * 3 == e.getEndSequenceId()));
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(
        numOfMockTsBlock * MOCK_TSBLOCK_SIZE, sourceHandle.getBufferRetainedSizeInBytes());

    // The local fragment instance consumes the data blocks.
    for (int i = 0; i < numOfMockTsBlock; i++) {
      sourceHandle.receive();
      if (i < numOfMockTsBlock - 1) {
        Assert.assertTrue(sourceHandle.isBlocked().isDone());
      } else {
        Assert.assertFalse(sourceHandle.isBlocked().isDone());
      }
      Assert.assertFalse(sourceHandle.isAborted());
      Assert.assertFalse(sourceHandle.isFinished());
      Assert.assertEquals(
          (numOfMockTsBlock - 1 - i) * MOCK_TSBLOCK_SIZE,
          sourceHandle.getBufferRetainedSizeInBytes());
    }

    // Receive EndOfDataBlock event from upstream fragment instance.
    sourceHandle.setNoMoreTsBlocks(3 * numOfMockTsBlock - 1);
    Assert.assertTrue(sourceHandle.isBlocked().isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertTrue(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockSourceHandleListener, Mockito.timeout(10_0000).times(1))
        .onFinished(sourceHandle);
  }

  @Test
  public void testFailedReceive() {
    final String queryId = "q0";
    final long MOCK_TSBLOCK_SIZE = 1024L * 1024L;
    final int numOfMockTsBlock = 10;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final String localPlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");

    // Construct a mock LocalMemoryManager that returns unblocked futures.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool mockMemoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a mock TsBlockSerde that deserializes any bytebuffer into a mock TsBlock.
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(MOCK_TSBLOCK_SIZE);
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
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    } catch (ClientManagerException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
            0,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockTsBlockSerde,
            mockSourceHandleListener,
            mockClientManager);
    sourceHandle.setRetryIntervalInMs(0L);
    Future<?> blocked = sourceHandle.isBlocked();
    Assert.assertFalse(blocked.isDone());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    // New data blocks event arrived.
    sourceHandle.updatePendingDataBlockInfo(
        0,
        Stream.generate(() -> MOCK_TSBLOCK_SIZE)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));
    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(SourceHandle.MAX_ATTEMPT_TIMES))
          .getDataBlock(Mockito.any());
    } catch (TException e) {
      e.printStackTrace();
      Assert.fail();
    }

    Mockito.verify(mockSourceHandleListener, Mockito.timeout(10_0000).times(1))
        .onFailure(sourceHandle, mockException);
    Assert.assertFalse(blocked.isDone());

    sourceHandle.abort();
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertTrue(sourceHandle.isAborted());
    Assert.assertTrue(blocked.isDone());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockSourceHandleListener, Mockito.timeout(10_0000).times(1))
        .onAborted(sourceHandle);
  }

  @Test
  public void testShortResponseRetriesAndFails() {
    final String queryId = "q0";
    final int numOfMockTsBlock = 10;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final String localPlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");

    final LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    final MemoryPool mockMemoryPool = Utils.createMockNonBlockedMemoryPool();
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(mockMemoryPool);
    final SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    final TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(MOCK_TSBLOCK_SIZE);
    final IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> mockClientManager =
        Mockito.mock(IClientManager.class);
    final SyncDataNodeMPPDataExchangeServiceClient mockClient =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    try {
      Mockito.when(mockClientManager.borrowClient(remoteEndpoint)).thenReturn(mockClient);
      Mockito.doAnswer(
              invocation -> {
                final TGetDataBlockRequest request = invocation.getArgument(0);
                if (request.getStartSequenceId() > 0) {
                  throw new TException("mock RPC failure");
                }
                final List<ByteBuffer> shortResponse = new ArrayList<>();
                for (int i = 0;
                    i < request.getEndSequenceId() - request.getStartSequenceId() - 1;
                    i++) {
                  shortResponse.add(ByteBuffer.allocate(1));
                }
                return new TGetDataBlockResponse(shortResponse);
              })
          .when(mockClient)
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    } catch (ClientManagerException | TException e) {
      Assert.fail(e.getMessage());
    }

    final SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
            0,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockTsBlockSerde,
            mockSourceHandleListener,
            mockClientManager);
    sourceHandle.setRetryIntervalInMs(0L);
    final Future<?> blocked = sourceHandle.isBlocked();

    sourceHandle.updatePendingDataBlockInfo(
        0,
        Stream.generate(() -> MOCK_TSBLOCK_SIZE)
            .limit(numOfMockTsBlock)
            .collect(Collectors.toList()));

    try {
      Mockito.verify(mockClient, Mockito.timeout(10_000).times(SourceHandle.MAX_ATTEMPT_TIMES + 1))
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    } catch (TException e) {
      Assert.fail(e.getMessage());
    }
    Mockito.verify(mockSourceHandleListener, Mockito.timeout(10_000).times(1))
        .onFailure(Mockito.eq(sourceHandle), Mockito.any(TException.class));
    Assert.assertFalse(blocked.isDone());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    sourceHandle.abort();
    Assert.assertTrue(blocked.isDone());
  }

  @Test
  public void testForceClose() {
    final String queryId = "q0";
    final long MOCK_TSBLOCK_SIZE = 1024L * 1024L;
    final TEndPoint remoteEndpoint =
        new TEndPoint("remote", IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final String localPlanNodeId = "exchange_0";
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");

    // Construct a mock LocalMemoryManager that do not block any reservation.
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
      Mockito.doAnswer(
              invocation -> {
                TGetDataBlockRequest req = invocation.getArgument(0);
                List<ByteBuffer> byteBuffers =
                    new ArrayList<>(req.getEndSequenceId() - req.getStartSequenceId());
                for (int i = 0; i < req.getEndSequenceId() - req.getStartSequenceId(); i++) {
                  byteBuffers.add(ByteBuffer.allocate(0));
                }
                return new TGetDataBlockResponse(byteBuffers);
              })
          .when(mockClient)
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    } catch (ClientManagerException | TException e) {
      e.printStackTrace();
      Assert.fail();
    }
    // Construct a mock SourceHandleListener.
    SourceHandleListener mockSourceHandleListener = Mockito.mock(SourceHandleListener.class);
    // Construct a mock TsBlockSerde that deserializes any bytebuffer into a mock TsBlock.
    TsBlockSerde mockTsBlockSerde = Utils.createMockTsBlockSerde(MOCK_TSBLOCK_SIZE);

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
            0,
            mockLocalMemoryManager,
            Executors.newSingleThreadExecutor(),
            mockTsBlockSerde,
            mockSourceHandleListener,
            mockClientManager);
    Future<?> blocked = sourceHandle.isBlocked();
    Assert.assertFalse(blocked.isDone());
    Assert.assertFalse(blocked.isCancelled());
    Assert.assertFalse(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());

    sourceHandle.abort();
    Assert.assertTrue(blocked.isDone());
    Assert.assertTrue(blocked.isCancelled());
    Assert.assertTrue(sourceHandle.isAborted());
    Assert.assertFalse(sourceHandle.isFinished());
    Assert.assertEquals(0L, sourceHandle.getBufferRetainedSizeInBytes());
    Mockito.verify(mockSourceHandleListener, Mockito.timeout(10_0000).times(1))
        .onAborted(sourceHandle);
  }

  @Test
  public void legacyMemoryGrantDoesNotAcquireHandleOnFutureCompletionThread() throws Exception {
    MemoryPool pool =
        new MemoryPool(
            "legacy-memory-handoff", new MemoryManager(MOCK_TSBLOCK_SIZE), MOCK_TSBLOCK_SIZE);
    pool.registerPlanNodeIdToQueryMemoryMap(
        "holder", "fi", "plan", org.apache.iotdb.db.queryengine.execution.memory.MemorySide.INPUT);
    Assert.assertTrue(
        pool.tryReserve("holder", "fi", "plan", MOCK_TSBLOCK_SIZE, MOCK_TSBLOCK_SIZE));
    ExecutorService grantExecutor = Executors.newSingleThreadExecutor();
    CountDownLatch grantStarted = new CountDownLatch(1);
    try (CoordinatedSourceFixture f =
        new CoordinatedSourceFixture(
            pool, com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService())) {
      f.source.isBlocked();
      f.source.updatePendingDataBlockInfo(0, List.of(MOCK_TSBLOCK_SIZE));
      Assert.assertEquals(1, pool.getMemoryReservationSize());
      Assert.assertEquals(1, f.source.getRuntimeInFlightTaskCount());

      Future<?> grant;
      synchronized (f.source) {
        grant =
            grantExecutor.submit(
                () -> {
                  grantStarted.countDown();
                  pool.free("holder", "fi", "plan", MOCK_TSBLOCK_SIZE);
                });
        Assert.assertTrue(grantStarted.await(5, TimeUnit.SECONDS));
        // The completion thread must return while this thread still holds SourceHandle.
        // A direct endpoint executor must not recreate the future-monitor/handle cycle.
        grant.get(5, TimeUnit.SECONDS);
      }
      f.source.isBlocked().get(5, TimeUnit.SECONDS);
      Assert.assertNotNull(f.source.receive());
      f.source.setNoMoreTsBlocks(0);
      f.source.requestCoordinatedClose().get(5, TimeUnit.SECONDS);
      Assert.assertEquals(0, f.source.getRuntimeInFlightTaskCount());
      Assert.assertEquals(0, pool.getMemoryReservationSize());
      Assert.assertEquals(0, pool.getReservedBytes());
      Mockito.verify(f.client, Mockito.times(1))
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
      Mockito.verify(f.client, Mockito.times(1))
          .onCloseSinkChannelEvent(
              Mockito.any(org.apache.iotdb.mpp.rpc.thrift.TCloseSinkChannelEvent.class));
    } finally {
      grantExecutor.shutdownNow();
      Assert.assertTrue(grantExecutor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void queuedMemoryContinuationRemainsOwnedUntilAbortOrCloseReallySettles()
      throws Exception {
    for (boolean abort : new boolean[] {false, true}) {
      MemoryPool pool =
          new MemoryPool(
              "queued-memory-handoff", new MemoryManager(MOCK_TSBLOCK_SIZE), MOCK_TSBLOCK_SIZE);
      pool.registerPlanNodeIdToQueryMemoryMap(
          "holder", "fi", "plan", org.apache.iotdb.db.queryengine.execution.memory.MemorySide.INPUT);
      Assert.assertTrue(
          pool.tryReserve("holder", "fi", "plan", MOCK_TSBLOCK_SIZE, MOCK_TSBLOCK_SIZE));
      ExecutorService endpointExecutor = Mockito.mock(ExecutorService.class);
      AtomicReference<Runnable> queued = new AtomicReference<>();
      CountDownLatch submitted = new CountDownLatch(1);
      AtomicInteger submissions = new AtomicInteger();
      Mockito.doAnswer(
              call -> {
                Runnable task = call.getArgument(0);
                if (submissions.getAndIncrement() == 0) {
                  queued.set(task);
                  submitted.countDown();
                } else {
                  task.run();
                }
                return null;
              })
          .when(endpointExecutor)
          .execute(Mockito.any(Runnable.class));
      try (CoordinatedSourceFixture f = new CoordinatedSourceFixture(pool, endpointExecutor)) {
        f.source.isBlocked();
        f.source.updatePendingDataBlockInfo(0, List.of(MOCK_TSBLOCK_SIZE));
        Assert.assertEquals(1, f.source.getRuntimeInFlightTaskCount());
        pool.free("holder", "fi", "plan", MOCK_TSBLOCK_SIZE);
        Assert.assertTrue(submitted.await(5, TimeUnit.SECONDS));
        Assert.assertNotNull(queued.get());
        var receipt =
            abort ? f.source.requestCoordinatedAbort() : f.source.requestCoordinatedClose();
        Assert.assertFalse("queued owned continuation must hold the close receipt", receipt.isDone());
        Assert.assertEquals(1, f.source.getRuntimeInFlightTaskCount());
        Assert.assertEquals(0, pool.getReservedBytes());
        Mockito.verify(f.client, Mockito.never())
            .onCloseSinkChannelEvent(
                Mockito.any(org.apache.iotdb.mpp.rpc.thrift.TCloseSinkChannelEvent.class));

        queued.get().run();
        receipt.get(5, TimeUnit.SECONDS);
        Assert.assertEquals(0, f.source.getRuntimeInFlightTaskCount());
        Assert.assertEquals(0, pool.getMemoryReservationSize());
        Assert.assertEquals(0, pool.getReservedBytes());
        Mockito.verify(f.client, Mockito.never()).getDataBlock(Mockito.any(TGetDataBlockRequest.class));
        Mockito.verify(f.client, Mockito.times(1))
            .onCloseSinkChannelEvent(
                Mockito.any(org.apache.iotdb.mpp.rpc.thrift.TCloseSinkChannelEvent.class));
      }
    }
  }

  @Test
  public void rejectedMemoryContinuationSettlesOwnershipAndKeepsFailureReceipt() throws Exception {
    MemoryPool pool =
        new MemoryPool(
            "rejected-memory-handoff", new MemoryManager(MOCK_TSBLOCK_SIZE), MOCK_TSBLOCK_SIZE);
    pool.registerPlanNodeIdToQueryMemoryMap(
        "holder", "fi", "plan", org.apache.iotdb.db.queryengine.execution.memory.MemorySide.INPUT);
    Assert.assertTrue(
        pool.tryReserve("holder", "fi", "plan", MOCK_TSBLOCK_SIZE, MOCK_TSBLOCK_SIZE));
    ExecutorService endpointExecutor = Mockito.mock(ExecutorService.class);
    CountDownLatch rejected = new CountDownLatch(1);
    AtomicInteger submissions = new AtomicInteger();
    Mockito.doAnswer(
            call -> {
              if (submissions.getAndIncrement() == 0) {
                rejected.countDown();
                throw new java.util.concurrent.RejectedExecutionException("reject continuation");
              }
              ((Runnable) call.getArgument(0)).run();
              return null;
            })
        .when(endpointExecutor)
        .execute(Mockito.any(Runnable.class));
    try (CoordinatedSourceFixture f = new CoordinatedSourceFixture(pool, endpointExecutor)) {
      f.source.isBlocked();
      f.source.updatePendingDataBlockInfo(0, List.of(MOCK_TSBLOCK_SIZE));
      pool.free("holder", "fi", "plan", MOCK_TSBLOCK_SIZE);
      Assert.assertTrue(rejected.await(5, TimeUnit.SECONDS));
      try {
        f.source.requestCoordinatedAbort().get(5, TimeUnit.SECONDS);
        Assert.fail("a rejected owned continuation must retain its failure");
      } catch (java.util.concurrent.ExecutionException failure) {
        Assert.assertTrue(
            failure.getCause() instanceof java.util.concurrent.RejectedExecutionException);
      }
      Assert.assertEquals(0, f.source.getRuntimeInFlightTaskCount());
      Assert.assertEquals(0, pool.getMemoryReservationSize());
      Assert.assertEquals(0, pool.getReservedBytes());
      Mockito.verify(f.client, Mockito.never()).getDataBlock(Mockito.any(TGetDataBlockRequest.class));
      Mockito.verify(f.client, Mockito.times(1))
          .onCloseSinkChannelEvent(
              Mockito.any(org.apache.iotdb.mpp.rpc.thrift.TCloseSinkChannelEvent.class));
    }
  }

  @Test
  public void coordinatedTerminalWaitsForFetchExitAndDrainsPayloads() throws Exception {
    for (boolean abort : new boolean[] {false, true}) {
      CountDownLatch entered = new CountDownLatch(1);
      CountDownLatch release = new CountDownLatch(1);
      try (CoordinatedSourceFixture f = new CoordinatedSourceFixture()) {
        Mockito.doAnswer(
                call -> {
                  entered.countDown();
                  Assert.assertTrue(release.await(5, TimeUnit.SECONDS));
                  return new TGetDataBlockResponse(List.of(ByteBuffer.wrap(new byte[] {1, 2})))
                      .setOffset(2)
                      .setTotalLength(6);
                })
            .when(f.client)
            .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
        f.source.isBlocked();
        f.source.updatePendingDataBlockInfo(0, List.of(MOCK_TSBLOCK_SIZE));
        Assert.assertTrue(entered.await(5, TimeUnit.SECONDS));
        com.google.common.util.concurrent.ListenableFuture<Void> first =
            abort
                ? f.source.requestCoordinatedAbort(new RuntimeException("cancel"))
                : f.source.requestCoordinatedClose();
        Assert.assertFalse(first.isDone());
        Assert.assertTrue(first.cancel(false));
        com.google.common.util.concurrent.ListenableFuture<Void> second =
            f.source.requestCoordinatedClose();
        Assert.assertFalse(second.isDone());
        Assert.assertFalse(second.isCancelled());
        Mockito.verify(f.client, Mockito.never())
            .onCloseSinkChannelEvent(
                Mockito.any(org.apache.iotdb.mpp.rpc.thrift.TCloseSinkChannelEvent.class));
        AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
        CountDownLatch observed = new CountDownLatch(1);
        second.addListener(
            () -> {
              if (Thread.holdsLock(f.source)) {
                callbackFailure.set(new AssertionError("close observer under handle monitor"));
              }
              observed.countDown();
            },
            com.google.common.util.concurrent.MoreExecutors.directExecutor());
        release.countDown();
        second.get(5, TimeUnit.SECONDS);
        Assert.assertTrue(observed.await(5, TimeUnit.SECONDS));
        Assert.assertNull(callbackFailure.get());
        Assert.assertEquals(0, retainedPayloads(f.source));
        Assert.assertEquals(0, f.pool.getReservedBytes());
        Mockito.verify(f.client, Mockito.times(1))
            .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
        Mockito.verify(f.client, Mockito.times(1))
            .onCloseSinkChannelEvent(
                Mockito.any(org.apache.iotdb.mpp.rpc.thrift.TCloseSinkChannelEvent.class));
      } finally {
        release.countDown();
      }
    }
  }

  @Test
  public void coordinatedCloseDropsAlreadyBufferedPayloadAndNotifiesOutsideMonitor()
      throws Exception {
    try (CoordinatedSourceFixture f = new CoordinatedSourceFixture()) {
      f.source.isBlocked();
      f.source.updatePendingDataBlockInfo(0, List.of(MOCK_TSBLOCK_SIZE));
      f.source.isBlocked().get(5, TimeUnit.SECONDS);
      Assert.assertEquals(1, retainedPayloads(f.source));
      Mockito.doAnswer(
              call -> {
                Assert.assertFalse(Thread.holdsLock(f.source));
                return null;
              })
          .when(f.listener)
          .onFinished(f.source);
      f.source.requestCoordinatedClose().get(5, TimeUnit.SECONDS);
      f.source.requestCoordinatedClose().get(5, TimeUnit.SECONDS);
      Assert.assertEquals(0, retainedPayloads(f.source));
      Assert.assertEquals(0, f.pool.getReservedBytes());
      Mockito.verify(f.listener, Mockito.times(1)).onFinished(f.source);
    }
  }

  @Test
  public void coordinatedCloseRetainsAmbiguousFreeFailureAndNeverRetries() throws Exception {
    AtomicInteger frees = new AtomicInteger();
    RuntimeException expected = new RuntimeException("after actual free");
    MemoryPool pool =
        new MemoryPool(
            "source-close-failure",
            new MemoryManager(8 * MOCK_TSBLOCK_SIZE),
            5 * MOCK_TSBLOCK_SIZE) {
          @Override
          public void free(String query, String fragment, String node, long bytes) {
            frees.incrementAndGet();
            super.free(query, fragment, node, bytes);
            throw expected;
          }
        };
    try (CoordinatedSourceFixture f =
        new CoordinatedSourceFixture(pool, Executors.newFixedThreadPool(2))) {
      f.source.isBlocked();
      f.source.updatePendingDataBlockInfo(0, List.of(MOCK_TSBLOCK_SIZE));
      f.source.isBlocked().get(5, TimeUnit.SECONDS);
      for (int attempt = 0; attempt < 2; attempt++) {
        java.util.concurrent.ExecutionException failure =
            Assert.assertThrows(
                java.util.concurrent.ExecutionException.class,
                () -> f.source.requestCoordinatedClose().get(5, TimeUnit.SECONDS));
        Assert.assertSame(expected, failure.getCause());
      }
      Assert.assertEquals(1, frees.get());
      Assert.assertEquals(0, retainedPayloads(f.source));
      Assert.assertEquals(0, pool.getReservedBytes());
    }
  }

  @Test
  public void coordinatedCloseWaitsForTerminalControlRpcToReallyExit() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    try (CoordinatedSourceFixture f = new CoordinatedSourceFixture()) {
      Mockito.doAnswer(
              call -> {
                entered.countDown();
                Assert.assertTrue(release.await(5, TimeUnit.SECONDS));
                return null;
              })
          .when(f.client)
          .onCloseSinkChannelEvent(
              Mockito.any(org.apache.iotdb.mpp.rpc.thrift.TCloseSinkChannelEvent.class));
      com.google.common.util.concurrent.ListenableFuture<Void> closed =
          f.source.requestCoordinatedClose();
      Assert.assertTrue(entered.await(5, TimeUnit.SECONDS));
      Assert.assertFalse(closed.isDone());
      Assert.assertTrue(f.source.isClosed());
      release.countDown();
      closed.get(5, TimeUnit.SECONDS);
    } finally {
      release.countDown();
    }
  }

  @Test
  public void coordinatedCloseRetainsListenerFailureAfterPhysicalSettlement() throws Exception {
    try (CoordinatedSourceFixture f = new CoordinatedSourceFixture()) {
      AssertionError expected = new AssertionError("terminal listener failed");
      Mockito.doThrow(expected).when(f.listener).onFinished(f.source);
      for (int attempt = 0; attempt < 2; attempt++) {
        java.util.concurrent.ExecutionException failure =
            Assert.assertThrows(
                java.util.concurrent.ExecutionException.class,
                () -> f.source.requestCoordinatedClose().get(5, TimeUnit.SECONDS));
        Assert.assertSame(expected, failure.getCause());
      }
      Assert.assertEquals(0, retainedPayloads(f.source));
      Assert.assertEquals(0, f.pool.getReservedBytes());
      Mockito.verify(f.listener, Mockito.times(1)).onFinished(f.source);
    }
  }

  @Test
  public void coordinatedCloseRetainsPeerCloseFailureAndDoesNotReplayRpc() throws Exception {
    try (CoordinatedSourceFixture f = new CoordinatedSourceFixture()) {
      TException expected = new TException("peer close failed");
      Mockito.doThrow(expected)
          .when(f.client)
          .onCloseSinkChannelEvent(
              Mockito.any(org.apache.iotdb.mpp.rpc.thrift.TCloseSinkChannelEvent.class));
      for (int attempt = 0; attempt < 2; attempt++) {
        java.util.concurrent.ExecutionException failure =
            Assert.assertThrows(
                java.util.concurrent.ExecutionException.class,
                () -> f.source.requestCoordinatedClose().get(5, TimeUnit.SECONDS));
        Assert.assertSame(expected, failure.getCause());
      }
      Mockito.verify(f.client, Mockito.times(SourceHandle.MAX_ATTEMPT_TIMES))
          .onCloseSinkChannelEvent(
              Mockito.any(org.apache.iotdb.mpp.rpc.thrift.TCloseSinkChannelEvent.class));
    }
  }

  @Test
  public void coordinatedCloseSealsLateMetadataAndNotifiesOutsideReentrantMonitor()
      throws Exception {
    try (CoordinatedSourceFixture f = new CoordinatedSourceFixture()) {
      Mockito.doAnswer(
              call -> {
                Assert.assertFalse(Thread.holdsLock(f.source));
                return null;
              })
          .when(f.listener)
          .onFinished(f.source);
      com.google.common.util.concurrent.ListenableFuture<Void> closed;
      synchronized (f.source) {
        closed = f.source.requestCoordinatedClose();
        Assert.assertFalse(closed.isDone());
        f.source.updatePendingDataBlockInfo(0, List.of(MOCK_TSBLOCK_SIZE));
        f.source.setNoMoreTsBlocks(10);
      }
      closed.get(5, TimeUnit.SECONDS);
      java.lang.reflect.Field sizes =
          SourceHandle.class.getDeclaredField("sequenceIdToDataBlockSize");
      sizes.setAccessible(true);
      synchronized (f.source) {
        Assert.assertTrue(((java.util.Map<?, ?>) sizes.get(f.source)).isEmpty());
      }
      Assert.assertEquals(0, retainedPayloads(f.source));
      Assert.assertEquals(0, f.pool.getReservedBytes());
      Mockito.verify(f.listener, Mockito.times(1)).onFinished(f.source);
      Mockito.verify(f.client, Mockito.never())
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    }
  }

  @Test
  public void prepaidRemoteInputKeepsOneCreditThroughActualDecodeExit() throws Exception {
    CountDownLatch decoding = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    TsBlockSerde serde = Mockito.spy(new TsBlockSerde());
    Mockito.doAnswer(
            call -> {
              decoding.countDown();
              Assert.assertTrue(release.await(5, TimeUnit.SECONDS));
              return new TsBlockSerde().deserialize(call.getArgument(0));
            })
        .when(serde)
        .deserialize(Mockito.any(ByteBuffer.class));
    try (PrepaidSourceFixture f =
        new PrepaidSourceFixture(serde, Executors.newFixedThreadPool(2))) {
      f.source.isBlocked();
      f.source.updatePendingDataBlockInfo(0, List.of(f.logicalBytes, f.logicalBytes));
      f.source.isBlocked().get(5, TimeUnit.SECONDS);
      Assert.assertEquals(f.prepaidBytes, f.pool.getReservedBytes());
      Assert.assertEquals(1, f.reservation.getLeasedCreditCount());
      Future<org.apache.tsfile.read.common.block.TsBlock> received =
          f.executor.submit(f.source::receive);
      Assert.assertTrue(decoding.await(5, TimeUnit.SECONDS));
      Assert.assertEquals(1, f.source.getRuntimeConsumerCount());
      Mockito.verify(f.client, Mockito.times(1))
          .getDataBlock(
              Mockito.argThat(
                  request ->
                      request.getStartSequenceId() == 0
                          && request.getEndSequenceId() == 1
                          && request.isSetOffset()
                          && request.getOffset() == 0));
      com.google.common.util.concurrent.ListenableFuture<Void> stopped =
          f.source.requestCoordinatedAbort();
      com.google.common.util.concurrent.ListenableFuture<Void> ownerClosed =
          f.reservation.requestClose();
      Assert.assertFalse(stopped.isDone());
      Assert.assertFalse(ownerClosed.isDone());
      Assert.assertEquals(f.prepaidBytes, f.pool.getReservedBytes());
      release.countDown();
      Assert.assertEquals(100L, received.get(5, TimeUnit.SECONDS).getColumn(0).getLong(0));
      stopped.get(5, TimeUnit.SECONDS);
      ownerClosed.get(5, TimeUnit.SECONDS);
      Assert.assertEquals(0, f.pool.getReservedBytes());
      Mockito.verify(f.client, Mockito.times(1))
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    } finally {
      release.countDown();
    }
  }

  @Test
  public void prepaidRemoteFragmentsCopyOnlyRemainingFromReadOnlySlices() throws Exception {
    try (PrepaidSourceFixture f = new PrepaidSourceFixture()) {
      ByteBuffer encoded = f.encoded.duplicate();
      int split = encoded.remaining() / 2;
      byte[] firstBacking = new byte[split + 8];
      encoded.get(firstBacking, 3, split);
      ByteBuffer first = ByteBuffer.wrap(firstBacking);
      first.position(3).limit(3 + split);
      ByteBuffer last = encoded.slice().asReadOnlyBuffer();
      Mockito.when(f.client.getDataBlock(Mockito.any(TGetDataBlockRequest.class)))
          .thenReturn(
              new TGetDataBlockResponse(List.of(first.asReadOnlyBuffer()))
                  .setOffset(split)
                  .setTotalLength(f.encoded.remaining()),
              new TGetDataBlockResponse(List.of(last)));
      f.source.isBlocked();
      f.source.updatePendingDataBlockInfo(0, List.of(f.logicalBytes));
      f.source.isBlocked().get(5, TimeUnit.SECONDS);
      org.apache.tsfile.read.common.block.TsBlock decoded = f.source.receive();
      Assert.assertEquals(4, decoded.getPositionCount());
      Assert.assertEquals(103L, decoded.getColumn(0).getLong(3));
      f.reservation.whenWritable(0).get(5, TimeUnit.SECONDS);
      Assert.assertEquals(f.prepaidBytes, f.pool.getReservedBytes());
      Mockito.verify(f.client)
          .getDataBlock(
              Mockito.argThat(
                  request ->
                      request.getStartSequenceId() == 0
                          && request.getEndSequenceId() == 1
                          && request.getOffset() == split));
      Assert.assertEquals(0, retainedPayloads(f.source));
      Assert.assertThrows(UnsupportedOperationException.class, f.source::getSerializedTsBlock);
    }
  }

  @Test
  public void prepaidRemoteOversizedTotalFailsBeforeAssemblyAndReturnsItsCredit() throws Exception {
    try (PrepaidSourceFixture f = new PrepaidSourceFixture()) {
      CountDownLatch failed = new CountDownLatch(1);
      Mockito.doAnswer(
              call -> {
                failed.countDown();
                return null;
              })
          .when(f.listener)
          .onFailure(Mockito.eq(f.source), Mockito.any(Throwable.class));
      Mockito.when(f.client.getDataBlock(Mockito.any(TGetDataBlockRequest.class)))
          .thenReturn(
              new TGetDataBlockResponse(List.of(ByteBuffer.wrap(new byte[] {1})))
                  .setOffset(1)
                  .setTotalLength(f.bounds.maxSerializedBytes() + 1));
      f.source.isBlocked();
      f.source.updatePendingDataBlockInfo(0, List.of(f.logicalBytes));
      Assert.assertTrue(failed.await(5, TimeUnit.SECONDS));
      f.reservation.whenWritable(0).get(5, TimeUnit.SECONDS);
      Assert.assertEquals(0, retainedPayloads(f.source));
      Assert.assertEquals(0, f.reservation.getLeasedCreditCount());
      java.util.concurrent.ExecutionException failure =
          Assert.assertThrows(
              java.util.concurrent.ExecutionException.class,
              () -> f.source.requestCoordinatedAbort().get(5, TimeUnit.SECONDS));
      Assert.assertTrue(failure.getCause() instanceof IllegalArgumentException);
      Assert.assertEquals(f.prepaidBytes, f.pool.getReservedBytes());
    }
  }

  @Test
  public void prepaidRemoteAbortCannotReturnCreditUntilBlockedRpcReallyExits() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    try (PrepaidSourceFixture f = new PrepaidSourceFixture()) {
      Mockito.when(f.client.getDataBlock(Mockito.any(TGetDataBlockRequest.class)))
          .thenAnswer(
              call -> {
                entered.countDown();
                Assert.assertTrue(
                    com.google.common.util.concurrent.Uninterruptibles.awaitUninterruptibly(
                        release, 5, TimeUnit.SECONDS));
                return new TGetDataBlockResponse(List.of(f.encoded.duplicate()));
              });
      f.source.isBlocked();
      f.source.updatePendingDataBlockInfo(0, List.of(f.logicalBytes));
      Assert.assertTrue(entered.await(5, TimeUnit.SECONDS));
      com.google.common.util.concurrent.ListenableFuture<Void> stopped =
          f.source.requestCoordinatedAbort();
      com.google.common.util.concurrent.ListenableFuture<Void> ownerClosed =
          f.reservation.requestClose();
      Assert.assertFalse(stopped.isDone());
      Assert.assertFalse(ownerClosed.isDone());
      Assert.assertEquals(1, f.reservation.getLeasedCreditCount());
      Assert.assertEquals(f.prepaidBytes, f.pool.getReservedBytes());
      release.countDown();
      stopped.get(5, TimeUnit.SECONDS);
      ownerClosed.get(5, TimeUnit.SECONDS);
      Assert.assertEquals(0, f.pool.getReservedBytes());
      Assert.assertEquals(0, retainedPayloads(f.source));
    } finally {
      release.countDown();
    }
  }

  @Test
  public void prepaidRemoteQueuedTaskCancellationReturnsCreditWithoutRunningRpc() throws Exception {
    ExecutorService executor = Mockito.mock(ExecutorService.class);
    java.util.concurrent.atomic.AtomicReference<Runnable> queued =
        new java.util.concurrent.atomic.AtomicReference<>();
    Mockito.doAnswer(
            call -> {
              queued.set(call.getArgument(0));
              return null;
            })
        .when(executor)
        .execute(Mockito.any(Runnable.class));
    try (PrepaidSourceFixture f = new PrepaidSourceFixture(new TsBlockSerde(), executor)) {
      f.source.isBlocked();
      f.source.updatePendingDataBlockInfo(0, List.of(f.logicalBytes));
      Assert.assertTrue(((Future<?>) queued.get()).cancel(false));
      f.reservation.whenWritable(0).get(5, TimeUnit.SECONDS);
      Assert.assertEquals(0, f.reservation.getLeasedCreditCount());
      com.google.common.util.concurrent.ListenableFuture<Void> stopped =
          f.source.requestCoordinatedAbort();
      queued.get().run();
      java.util.concurrent.ExecutionException failure =
          Assert.assertThrows(
              java.util.concurrent.ExecutionException.class,
              () -> stopped.get(5, TimeUnit.SECONDS));
      Assert.assertTrue(failure.getCause() instanceof java.util.concurrent.CancellationException);
      Mockito.verify(f.client, Mockito.never())
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
    }
  }

  @Test
  public void prepaidInputMetadataCannotReuseSlotBeforeDeferredRealCreditReturn() throws Exception {
    try (PrepaidSourceFixture f =
        new PrepaidSourceFixture(new TsBlockSerde(), Executors.newSingleThreadExecutor())) {
      f.source.isBlocked();
      f.source.updatePendingDataBlockInfo(0, List.of(f.logicalBytes));
      f.source.isBlocked().get(5, TimeUnit.SECONDS);
      // FIFO marker proves the fetch and its queued ACK have actually exited.
      f.executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
      Assert.assertEquals(0, f.source.getRuntimeInFlightTaskCount());
      synchronized (f.source) {
        Assert.assertEquals(100L, f.source.receive().getColumn(0).getLong(0));
        Assert.assertEquals(1, f.source.getRuntimePendingCreditReturnCount());
        Assert.assertEquals(1, f.reservation.getLeasedCreditCount());
        // The return action is deferred behind this outer monitor. Metadata must just wait.
        f.source.updatePendingDataBlockInfo(1, List.of(f.logicalBytes));
        Mockito.verify(f.client, Mockito.times(1))
            .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
      }
      f.source.isBlocked().get(5, TimeUnit.SECONDS);
      Assert.assertEquals(100L, f.source.receive().getColumn(0).getLong(0));
      f.executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
      Mockito.verify(f.client, Mockito.times(2))
          .getDataBlock(Mockito.any(TGetDataBlockRequest.class));
      Assert.assertEquals(f.prepaidBytes, f.pool.getReservedBytes());
      Assert.assertEquals(0, f.pool.getMemoryReservationSize());
    }
  }

  private static final class PrepaidSourceFixture implements AutoCloseable {
    final RuntimeExchangeBlockCodec.Bounds bounds =
        new RuntimeExchangeBlockCodec.Bounds(128, 1, 4096, 4096, 4096, 8192, 8192)
            .withSchema(List.of(org.apache.tsfile.enums.TSDataType.INT64));
    final long prepaidBytes = bounds.inputEnvelopeBytes();
    final MemoryPool pool =
        new MemoryPool("prepaid-source", new MemoryManager(4 * prepaidBytes), 2 * prepaidBytes) {
          @Override
          public MemoryReservationResult reserveWithPriority(
              String q, String f, String n, long bytes, long cap, boolean highest) {
            throw new AssertionError("Runtime endpoint attempted a legacy pool reservation");
          }
        };
    final org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation reservation =
        org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation.tryCreate(
                pool,
                new SharedTsBlockQueue.MemoryReservationKey("prepaid_source", "0.0", "remote"),
                org.apache.iotdb.db.queryengine.execution.memory.MemorySide.INPUT,
                1,
                prepaidBytes,
                2 * prepaidBytes)
            .get();
    final ExecutorService executor;
    final SyncDataNodeMPPDataExchangeServiceClient client =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    final SourceHandleListener listener = Mockito.mock(SourceHandleListener.class);
    final SourceHandle source;
    final ByteBuffer encoded;
    final long logicalBytes;

    PrepaidSourceFixture() throws Exception {
      this(new TsBlockSerde(), Executors.newFixedThreadPool(2));
    }

    @SuppressWarnings("unchecked")
    PrepaidSourceFixture(TsBlockSerde serde, ExecutorService executor) throws Exception {
      this.executor = executor;
      org.apache.tsfile.read.common.block.TsBlock block =
          new org.apache.tsfile.read.common.block.TsBlock(
              4,
              new org.apache.tsfile.read.common.block.column.TimeColumn(4, new long[] {0, 1, 2, 3}),
              new org.apache.tsfile.read.common.block.column.LongColumn(
                  4, java.util.Optional.empty(), new long[] {100, 101, 102, 103}));
      encoded = new TsBlockSerde().serialize(block);
      logicalBytes = block.getSizeInBytes();
      LocalMemoryManager memory = Mockito.mock(LocalMemoryManager.class);
      Mockito.when(memory.getQueryPool()).thenReturn(pool);
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clients =
          Mockito.mock(IClientManager.class);
      TEndPoint endpoint = new TEndPoint("prepaid-source", 12345);
      Mockito.when(clients.borrowClient(endpoint)).thenReturn(client);
      Mockito.when(client.getDataBlock(Mockito.any(TGetDataBlockRequest.class)))
          .thenAnswer(call -> new TGetDataBlockResponse(List.of(encoded.duplicate())));
      source =
          new SourceHandle(
              endpoint,
              new TFragmentInstanceId("prepaid_source", 1, "0"),
              new TFragmentInstanceId("prepaid_source", 0, "0"),
              "remote",
              0,
              memory,
              executor,
              serde,
              listener,
              false,
              clients,
              new org.apache.iotdb.db.queryengine.execution.runtime.RuntimeEndpointFrontier(
                  reservation, 0, bounds));
      source.setRetryIntervalInMs(0);
    }

    @Override
    public void close() throws Exception {
      try {
        source.requestCoordinatedAbort().get(5, TimeUnit.SECONDS);
      } catch (java.util.concurrent.ExecutionException ignored) {
        // Failure tests retain and assert the endpoint receipt; close the explicit test owner.
      } finally {
        reservation.requestClose().get(5, TimeUnit.SECONDS);
        executor.shutdownNow();
      }
    }
  }

  private static int retainedPayloads(SourceHandle source) throws Exception {
    java.lang.reflect.Field field = SourceHandle.class.getDeclaredField("sequenceIdToTsBlock");
    field.setAccessible(true);
    synchronized (source) {
      return ((java.util.Map<?, ?>) field.get(source)).size();
    }
  }

  private static final class CoordinatedSourceFixture implements AutoCloseable {
    final MemoryPool pool;
    final ExecutorService executor;
    final SyncDataNodeMPPDataExchangeServiceClient client =
        Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
    final SourceHandleListener listener = Mockito.mock(SourceHandleListener.class);
    final SourceHandle source;

    CoordinatedSourceFixture() throws Exception {
      this(
          new MemoryPool(
              "coordinated-source",
              new MemoryManager(8 * MOCK_TSBLOCK_SIZE),
              5 * MOCK_TSBLOCK_SIZE),
          Executors.newFixedThreadPool(2));
    }

    @SuppressWarnings("unchecked")
    CoordinatedSourceFixture(MemoryPool pool, ExecutorService executor) throws Exception {
      this.pool = pool;
      this.executor = executor;
      LocalMemoryManager memory = Mockito.mock(LocalMemoryManager.class);
      Mockito.when(memory.getQueryPool()).thenReturn(pool);
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clients =
          Mockito.mock(IClientManager.class);
      TEndPoint endpoint = new TEndPoint("coordinated-source", 12345);
      Mockito.when(clients.borrowClient(endpoint)).thenReturn(client);
      Mockito.when(client.getDataBlock(Mockito.any(TGetDataBlockRequest.class)))
          .thenReturn(new TGetDataBlockResponse(List.of(ByteBuffer.wrap(new byte[] {1, 2}))));
      source =
          new SourceHandle(
              endpoint,
              new TFragmentInstanceId("coordinated_source", 1, "0"),
              new TFragmentInstanceId("coordinated_source", 0, "0"),
              "source",
              0,
              memory,
              executor,
              Utils.createMockTsBlockSerde(MOCK_TSBLOCK_SIZE),
              listener,
              clients);
      source.setRetryIntervalInMs(0);
    }

    @Override
    public void close() {
      try {
        source.abort();
      } catch (Throwable ignored) {
        // Tests retain and assert the original terminal receipt.
      } finally {
        executor.shutdownNow();
      }
    }
  }
}
