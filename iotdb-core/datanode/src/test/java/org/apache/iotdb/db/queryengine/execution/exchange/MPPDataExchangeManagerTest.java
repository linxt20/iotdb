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
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceState;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TNewDataBlockEvent;

import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MPPDataExchangeManagerTest {
  @Test
  public void testNewDataBlockMemoryFailureMarksTargetFragmentFailed() throws Exception {
    final String queryId = "q_memory_reservation_failure";
    final String targetPlanNodeId = "root-exchange";
    final long dataBlockSize = 1024L;
    final TFragmentInstanceId targetFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
    final TFragmentInstanceId sourceFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
    final TEndPoint sourceEndpoint = new TEndPoint("remote-exchange", 10740);
    ExecutorService exchangeExecutor = Executors.newSingleThreadExecutor();
    ExecutorService notificationExecutor = Executors.newSingleThreadExecutor();

    try {
      LocalMemoryManager localMemoryManager = Mockito.mock(LocalMemoryManager.class);
      MemoryPool memoryPool = Mockito.mock(MemoryPool.class);
      Mockito.when(localMemoryManager.getQueryPool()).thenReturn(memoryPool);
      IllegalArgumentException expectedFailure =
          new IllegalArgumentException("injected memory reservation failure");
      Mockito.when(
              memoryPool.reserveWithPriority(
                  Mockito.eq(queryId),
                  Mockito.anyString(),
                  Mockito.eq(targetPlanNodeId),
                  Mockito.eq(dataBlockSize),
                  Mockito.anyLong(),
                  Mockito.eq(false)))
          .thenThrow(expectedFailure);

      FragmentInstanceId targetId =
          new FragmentInstanceId(new PlanFragmentId(new QueryId(queryId), 0), "0");
      FragmentInstanceStateMachine stateMachine =
          new FragmentInstanceStateMachine(targetId, notificationExecutor);
      FragmentInstanceContext context =
          FragmentInstanceContext.createFragmentInstanceContext(targetId, stateMachine);
      MPPDataExchangeManager exchangeManager =
          new MPPDataExchangeManager(
              localMemoryManager,
              new TsBlockSerdeFactory(),
              exchangeExecutor,
              Mockito.mock(IClientManager.class));
      ISourceHandle sourceHandle =
          exchangeManager.createSourceHandle(
              targetFragmentInstanceId,
              targetPlanNodeId,
              0,
              sourceEndpoint,
              sourceFragmentInstanceId,
              context::failed);
      sourceHandle.isBlocked();

      TNewDataBlockEvent event =
          new TNewDataBlockEvent(
              targetFragmentInstanceId,
              targetPlanNodeId,
              sourceFragmentInstanceId,
              0,
              Collections.singletonList(dataBlockSize));
      IllegalArgumentException actualFailure =
          Assert.assertThrows(
              IllegalArgumentException.class,
              () ->
                  exchangeManager
                      .getOrCreateMPPDataExchangeServiceImpl()
                      .onNewDataBlockEvent(event));

      Assert.assertSame(expectedFailure, actualFailure);
      Assert.assertEquals(FragmentInstanceState.FAILED, stateMachine.getState());
      Assert.assertSame(expectedFailure, stateMachine.getFailureCauses().peek());
    } finally {
      exchangeExecutor.shutdownNow();
      notificationExecutor.shutdownNow();
    }
  }

  @Test
  public void testCreateLocalSinkHandle() {
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId("q0", 1, "0");
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId("q0", 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final String localPlanNodeId = "shuffleSink_0";
    final FragmentInstanceContext mockFragmentInstanceContext =
        Mockito.mock(FragmentInstanceContext.class);

    // Construct a mock LocalMemoryManager with capacity 5 * mockTsBlockSize per query.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryManager memoryManager = Mockito.spy(new MemoryManager(10240L));
    MemoryPool spyMemoryPool = Mockito.spy(new MemoryPool("test", memoryManager, 5120L));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);

    MPPDataExchangeManager mppDataExchangeManager =
        new MPPDataExchangeManager(
            mockLocalMemoryManager,
            new TsBlockSerdeFactory(),
            Executors.newSingleThreadExecutor(),
            new IClientManager.Factory<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>()
                .createClientManager(
                    new ClientPoolFactory.SyncDataNodeMPPDataExchangeServiceClientPoolFactory()));

    ISinkHandle shuffleSinkHandle =
        mppDataExchangeManager.createShuffleSinkHandle(
            Collections.singletonList(
                new DownStreamChannelLocation(
                    new TEndPoint(
                        IoTDBDescriptor.getInstance().getConfig().getInternalAddress(),
                        IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort()),
                    remoteFragmentInstanceId,
                    remotePlanNodeId)),
            new DownStreamChannelIndex(0),
            ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
            localFragmentInstanceId,
            localPlanNodeId,
            mockFragmentInstanceContext);

    Assert.assertTrue(shuffleSinkHandle instanceof ShuffleSinkHandle);

    ISourceHandle localSourceHandle =
        mppDataExchangeManager.createLocalSourceHandleForFragment(
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localPlanNodeId,
            localFragmentInstanceId,
            0,
            t -> {});

    Assert.assertTrue(localSourceHandle instanceof LocalSourceHandle);

    Assert.assertEquals(
        ((LocalSinkChannel) shuffleSinkHandle.getChannel(0)).getSharedTsBlockQueue(),
        ((LocalSourceHandle) localSourceHandle).getSharedTsBlockQueue());
    assertProducerOwnsQueue(
        spyMemoryPool,
        shuffleSinkHandle,
        localSourceHandle,
        localFragmentInstanceId,
        remoteFragmentInstanceId,
        localPlanNodeId);
  }

  @Test
  public void testCreateLocalSourceHandle() {
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId("q0", 1, "0");
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId("q0", 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final String localPlanNodeId = "shuffleSink_0";
    final FragmentInstanceContext mockFragmentInstanceContext =
        Mockito.mock(FragmentInstanceContext.class);

    // Construct a mock LocalMemoryManager with capacity 5 * mockTsBlockSize per query.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryManager memoryManager = Mockito.spy(new MemoryManager(10240L));
    MemoryPool spyMemoryPool = Mockito.spy(new MemoryPool("test", memoryManager, 5120L));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);

    MPPDataExchangeManager mppDataExchangeManager =
        new MPPDataExchangeManager(
            mockLocalMemoryManager,
            new TsBlockSerdeFactory(),
            Executors.newSingleThreadExecutor(),
            new IClientManager.Factory<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>()
                .createClientManager(
                    new ClientPoolFactory.SyncDataNodeMPPDataExchangeServiceClientPoolFactory()));

    ISourceHandle localSourceHandle =
        mppDataExchangeManager.createLocalSourceHandleForFragment(
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localPlanNodeId,
            localFragmentInstanceId,
            0,
            t -> {});

    Assert.assertTrue(localSourceHandle instanceof LocalSourceHandle);

    ISinkHandle shuffleSinkHandle =
        mppDataExchangeManager.createShuffleSinkHandle(
            Collections.singletonList(
                new DownStreamChannelLocation(
                    new TEndPoint(
                        IoTDBDescriptor.getInstance().getConfig().getInternalAddress(),
                        IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort()),
                    remoteFragmentInstanceId,
                    remotePlanNodeId)),
            new DownStreamChannelIndex(0),
            ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
            localFragmentInstanceId,
            localPlanNodeId,
            mockFragmentInstanceContext);

    Assert.assertTrue(shuffleSinkHandle instanceof ShuffleSinkHandle);

    Assert.assertEquals(
        ((LocalSinkChannel) shuffleSinkHandle.getChannel(0)).getSharedTsBlockQueue(),
        ((LocalSourceHandle) localSourceHandle).getSharedTsBlockQueue());
    assertProducerOwnsQueue(
        spyMemoryPool,
        shuffleSinkHandle,
        localSourceHandle,
        localFragmentInstanceId,
        remoteFragmentInstanceId,
        localPlanNodeId);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void runtimeLoopbackFactoriesUsePaidRpcEndpointsInsteadOfLocalQueues() throws Exception {
    RuntimeExchangeBlockCodec.Bounds bounds =
        new RuntimeExchangeBlockCodec.Bounds(128, 1, 4096, 4096, 4096, 8192, 8192)
            .withSchema(java.util.List.of(org.apache.tsfile.enums.TSDataType.INT64));
    long outputBytes = bounds.outputEnvelopeBytes();
    long inputBytes = bounds.inputEnvelopeBytes();
    MemoryPool pool =
        new MemoryPool(
            "runtime-loopback",
            new MemoryManager(4 * (outputBytes + inputBytes)),
            2 * (outputBytes + inputBytes)) {
          @Override
          public MemoryReservationResult reserveWithPriority(
              String q, String f, String n, long bytes, long cap, boolean highest) {
            throw new AssertionError("Runtime loopback factory used a legacy reservation");
          }
        };
    org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation output =
        org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation.tryCreate(
                pool,
                new SharedTsBlockQueue.MemoryReservationKey("runtime_loopback", "0.0", "paid-out"),
                MemorySide.OUTPUT,
                1,
                outputBytes,
                2 * outputBytes)
            .get();
    org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation input =
        org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation.tryCreate(
                pool,
                new SharedTsBlockQueue.MemoryReservationKey("runtime_loopback", "1.0", "paid-in"),
                MemorySide.INPUT,
                1,
                inputBytes,
                2 * inputBytes)
            .get();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel sink = null;
    org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle source = null;
    try {
      LocalMemoryManager memory = Mockito.mock(LocalMemoryManager.class);
      Mockito.when(memory.getQueryPool()).thenReturn(pool);
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clients =
          Mockito.mock(IClientManager.class);
      Mockito.when(clients.borrowClient(Mockito.any(TEndPoint.class)))
          .thenReturn(Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class));
      MPPDataExchangeManager manager =
          new MPPDataExchangeManager(memory, new TsBlockSerdeFactory(), executor, clients);
      TEndPoint loopback =
          new TEndPoint(
              IoTDBDescriptor.getInstance().getConfig().getInternalAddress(),
              IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
      TFragmentInstanceId producer = new TFragmentInstanceId("runtime_loopback", 0, "0");
      TFragmentInstanceId consumer = new TFragmentInstanceId("runtime_loopback", 1, "0");
      FragmentInstanceContext context = Mockito.mock(FragmentInstanceContext.class);
      Mockito.when(context.getFailureCause()).thenReturn(java.util.Optional.empty());
      ISinkHandle handle =
          manager.createShuffleSinkHandle(
              java.util.List.of(new DownStreamChannelLocation(loopback, consumer, "remote-input")),
              new DownStreamChannelIndex(0),
              ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
              producer,
              "native-output",
              context,
              java.util.List.of(
                  new org.apache.iotdb.db.queryengine.execution.runtime.RuntimeEndpointFrontier(
                      output, 0, bounds)));
      Assert.assertTrue(
          handle.getChannel(0)
              instanceof org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel);
      sink =
          (org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel)
              handle.getChannel(0);
      source =
          (org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle)
              manager.createSourceHandle(
                  consumer,
                  "remote-input",
                  0,
                  loopback,
                  producer,
                  failure -> {
                    throw new AssertionError(failure);
                  },
                  false,
                  new org.apache.iotdb.db.queryengine.execution.runtime.RuntimeEndpointFrontier(
                      input, 0, bounds));
      Assert.assertTrue(sink.isRuntimeFrontierMode());
      Assert.assertTrue(source.isRuntimeFrontierMode());
      Assert.assertEquals(output.getMemoryReservationKey(), sink.getActualAccountKey());
      Assert.assertEquals(input.getMemoryReservationKey(), source.getActualAccountKey());
      sink.open();
      Assert.assertEquals(outputBytes + inputBytes, pool.getReservedBytes());
      Assert.assertEquals(0, pool.getMemoryReservationSize());
      Assert.assertNull(sink.getCoordinatedCloseFuture());
      Assert.assertNull(source.getCoordinatedCloseFuture());
    } finally {
      if (source != null)
        source.requestCoordinatedAbort().get(5, java.util.concurrent.TimeUnit.SECONDS);
      if (sink != null)
        sink.requestCoordinatedAbort().get(5, java.util.concurrent.TimeUnit.SECONDS);
      output.requestClose().get(5, java.util.concurrent.TimeUnit.SECONDS);
      input.requestClose().get(5, java.util.concurrent.TimeUnit.SECONDS);
      executor.shutdownNow();
    }
    Assert.assertEquals(0, pool.getReservedBytes());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void runtimeShuffleFactoryRejectsMissingBindingBeforePublishingAnyChannel() {
    LocalMemoryManager memory = Mockito.mock(LocalMemoryManager.class);
    IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clients =
        Mockito.mock(IClientManager.class);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      MPPDataExchangeManager manager =
          new MPPDataExchangeManager(memory, new TsBlockSerdeFactory(), executor, clients);
      Assert.assertThrows(
          IllegalArgumentException.class,
          () ->
              manager.createShuffleSinkHandle(
                  java.util.List.of(
                      new DownStreamChannelLocation(
                          new TEndPoint("remote", 12345),
                          new TFragmentInstanceId("missing_binding", 1, "0"),
                          "input")),
                  new DownStreamChannelIndex(0),
                  ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
                  new TFragmentInstanceId("missing_binding", 0, "0"),
                  "output",
                  Mockito.mock(FragmentInstanceContext.class),
                  java.util.List.of()));
    } finally {
      executor.shutdownNow();
    }
  }

  private static void assertProducerOwnsQueue(
      MemoryPool pool,
      ISinkHandle sink,
      ISourceHandle source,
      TFragmentInstanceId producer,
      TFragmentInstanceId consumer,
      String planNodeId) {
    SharedTsBlockQueue queue = ((LocalSourceHandle) source).getSharedTsBlockQueue();
    sink.setMaxBytesCanReserve(512);
    source.setMaxBytesCanReserve(128);
    Assert.assertEquals(512, queue.getMaxBytesCanReserve());
    String producerId =
        FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(producer);
    String consumerId =
        FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(consumer);
    TsBlock block = Utils.createMockTsBlock(256);
    synchronized (queue) {
      Assert.assertTrue(queue.add(block).isDone());
      Assert.assertEquals(256, pool.getReservedBytes());
      Assert.assertEquals(
          256, pool.getPlanNodeMemoryReservedBytes(producer.queryId, producerId, planNodeId));
      Assert.assertEquals(
          256,
          pool.getFragmentMemoryReservedBytes(producer.queryId, producerId, MemorySide.OUTPUT));
      Assert.assertEquals(
          0, pool.getFragmentMemoryReservedBytes(producer.queryId, producerId, MemorySide.INPUT));
      Assert.assertEquals(
          0, pool.getFragmentMemoryReservedBytes(consumer.queryId, consumerId, MemorySide.INPUT));
      Assert.assertEquals(
          0, pool.getFragmentMemoryReservedBytes(consumer.queryId, consumerId, MemorySide.OUTPUT));
      Assert.assertSame(block, queue.remove());
      Assert.assertEquals(0, pool.getReservedBytes());
      Assert.assertEquals(
          0, pool.getFragmentMemoryReservedBytes(producer.queryId, producerId, MemorySide.OUTPUT));
    }
  }
}
