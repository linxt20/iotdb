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
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue.MemoryReservationKey;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.RuntimeLocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.RuntimeLocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeEndpointFrontier;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeResourceSnapshot;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionResourceRegistry;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class RuntimeLocalEndpointTest {
  private static final RuntimeExchangeBlockCodec.Bounds BOUNDS =
      new RuntimeExchangeBlockCodec.Bounds(
          16, 1, 4096, 4096, 4096, 8192, 8192, List.of(TSDataType.INT64));

  private static TsBlock block(long value) {
    return new TsBlock(
        1,
        new TimeColumn(1, new long[] {5}),
        new Column[] {new LongColumn(1, Optional.empty(), new long[] {value})});
  }

  private static final class Fixture implements AutoCloseable {
    final FragmentInstanceId producer =
        FragmentInstanceId.fromThrift(new TFragmentInstanceId("local_endpoint", 1, "0"));
    final FragmentInstanceId consumer =
        FragmentInstanceId.fromThrift(new TFragmentInstanceId("local_endpoint", 2, "0"));
    final MemoryPool pool = new MemoryPool("endpoint", new MemoryManager(1L << 22), 1L << 21);
    final RuntimeFrontierReservation out =
        RuntimeFrontierReservation.tryCreate(
                pool,
                new MemoryReservationKey(
                    "local_endpoint", producer.getFragmentInstanceId(), "prepaid-out"),
                MemorySide.OUTPUT,
                1,
                BOUNDS.outputEnvelopeBytes(),
                1L << 21)
            .get();
    final RuntimeFrontierReservation in =
        RuntimeFrontierReservation.tryCreate(
                pool,
                new MemoryReservationKey(
                    "local_endpoint", consumer.getFragmentInstanceId(), "prepaid-in"),
                MemorySide.INPUT,
                1,
                BOUNDS.inputEnvelopeBytes(),
                1L << 21)
            .get();
    final RuntimeEndpointFrontier output = new RuntimeEndpointFrontier(out, 0, BOUNDS);
    final RuntimeEndpointFrontier input = new RuntimeEndpointFrontier(in, 0, BOUNDS);
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    final IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clients =
        mock(IClientManager.class);
    final FragmentInstanceContext context = mock(FragmentInstanceContext.class);
    final TEndPoint local =
        new TEndPoint(
            IoTDBDescriptor.getInstance().getConfig().getInternalAddress(),
            IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final MPPDataExchangeManager manager;
    ISourceHandle source;
    ISinkHandle sink;

    Fixture(boolean enabled) throws Exception {
      LocalMemoryManager memory = mock(LocalMemoryManager.class);
      when(memory.getQueryPool()).thenReturn(pool);
      when(clients.borrowClient(any()))
          .thenReturn(mock(SyncDataNodeMPPDataExchangeServiceClient.class));
      IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
      boolean old = config.isEnableRuntimeLocalExchange();
      try {
        config.setEnableRuntimeLocalExchange(enabled);
        manager = new MPPDataExchangeManager(memory, TsBlockSerde::new, executor, clients);
      } finally {
        config.setEnableRuntimeLocalExchange(old);
      }
      doAnswer(
              invocation -> {
                assertFalse(Thread.holdsLock(manager));
                return null;
              })
          .when(context)
          .finished();
      doAnswer(
              invocation -> {
                assertFalse(Thread.holdsLock(manager));
                return null;
              })
          .when(context)
          .failed(any());
    }

    ISourceHandle source(TEndPoint endpoint) {
      source =
          manager.createSourceHandle(
              consumer.toThrift(),
              "exchange",
              0,
              endpoint,
              producer.toThrift(),
              failure -> assertFalse(Thread.holdsLock(manager)),
              false,
              input);
      return source;
    }

    ISinkHandle sink(TEndPoint endpoint) {
      sink =
          manager.createShuffleSinkHandle(
              List.of(new DownStreamChannelLocation(endpoint, consumer.toThrift(), "exchange")),
              new DownStreamChannelIndex(0),
              ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
              producer.toThrift(),
              "sink",
              context,
              List.of(output));
      return sink;
    }

    @Override
    public void close() throws Exception {
      try {
        if (source instanceof RuntimeExchangeEndpoint) {
          ((RuntimeExchangeEndpoint) source).requestCoordinatedClose().get(5, TimeUnit.SECONDS);
        } else if (source != null) source.close();
        if (sink != null) {
          RuntimeExchangeEndpoint endpoint = (RuntimeExchangeEndpoint) sink.getChannel(0);
          endpoint.requestCoordinatedClose().get(5, TimeUnit.SECONDS);
          sink.close();
        }
        out.requestClose().get(5, TimeUnit.SECONDS);
        in.requestClose().get(5, TimeUnit.SECONDS);
        assertEquals(0, pool.getReservedBytes());
        pool.deRegisterFragmentInstanceFromQueryMemoryMap(
            "local_endpoint", producer.getFragmentInstanceId(), false);
        pool.deRegisterFragmentInstanceFromQueryMemoryMap(
            "local_endpoint", consumer.getFragmentInstanceId(), false);
      } finally {
        executor.shutdownNow();
      }
    }
  }

  @Test
  public void factoryUsesOneLocalLinkInEitherConstructionOrderAndNoClient() throws Exception {
    for (boolean sourceFirst : new boolean[] {true, false}) {
      try (Fixture f = new Fixture(true)) {
        if (sourceFirst) {
          f.source(f.local);
          f.sink(f.local);
        } else {
          f.sink(f.local);
          f.source(f.local);
        }
        assertTrue(f.source instanceof RuntimeLocalSourceHandle);
        assertTrue(f.sink.getChannel(0) instanceof RuntimeLocalSinkChannel);
        RuntimeLocalSourceHandle source = (RuntimeLocalSourceHandle) f.source;
        RuntimeLocalSinkChannel sink = (RuntimeLocalSinkChannel) f.sink.getChannel(0);
        assertSame(source.getLink(), sink.getLink());
        long reserved = f.pool.getReservedBytes();
        for (int value = 0; value < 4; value++) {
          sink.send(block(value));
          assertFalse(sink.isFull().isDone());
          assertEquals(value, source.receive().getColumn(0).getLong(0));
          assertEquals(reserved, f.pool.getReservedBytes());
        }
        f.sink.setNoMoreTsBlocks();
        assertTrue(source.isFinished());
        assertTrue(sink.isFinished());
        assertEquals(0, f.manager.getSourceHandleSize());
        assertEquals(0, f.manager.getShuffleSinkHandleSize());
        verifyZeroInteractions(f.clients);
      }
    }
  }

  @Test
  public void emptyProducerWaitsForLateConsumerToObserveEos() throws Exception {
    try (Fixture f = new Fixture(true)) {
      f.sink(f.local);
      f.sink.setNoMoreTsBlocks();
      assertFalse(f.sink.isFinished());
      verify(f.context, never()).finished();
      f.source(f.local);
      assertTrue(f.source.isFinished());
      assertTrue(f.sink.isFinished());
      verify(f.context, times(1)).finished();
      verifyZeroInteractions(f.clients);
    }
  }

  @Test
  public void disabledManagerRetainsRpcForRuntimeLoopback() throws Exception {
    try (Fixture f = new Fixture(false)) {
      f.source(f.local);
      f.sink(f.local);
      assertTrue(f.source instanceof SourceHandle);
      assertTrue(f.sink.getChannel(0) instanceof SinkChannel);
    }
  }

  @Test
  public void startupChoiceDoesNotChangeWhenConfigObjectChanges() throws Exception {
    try (Fixture f = new Fixture(true)) {
      IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
      boolean old = config.isEnableRuntimeLocalExchange();
      try {
        config.setEnableRuntimeLocalExchange(false);
        f.source(f.local);
        f.sink(f.local);
        assertTrue(f.source instanceof RuntimeLocalSourceHandle);
        assertTrue(f.sink.getChannel(0) instanceof RuntimeLocalSinkChannel);
      } finally {
        config.setEnableRuntimeLocalExchange(old);
      }
    }
  }

  @Test
  public void crossNodeStillUsesRpcWithLocalFlagEnabled() throws Exception {
    try (Fixture f = new Fixture(true)) {
      TEndPoint remote = new TEndPoint("different-datanode", f.local.getPort());
      f.source(remote);
      f.sink(remote);
      assertTrue(f.source instanceof SourceHandle);
      assertTrue(f.sink.getChannel(0) instanceof SinkChannel);
    }
  }

  @Test
  public void nativeFinalResultKeepsNativeInputOnExplicitLocalSerializedTransport()
      throws Exception {
    try (Fixture f = new Fixture(true)) {
      f.source =
          f.manager.createResultSourceHandle(
              f.consumer.toThrift(),
              "exchange",
              0,
              f.local,
              f.producer.toThrift(),
              failure -> {},
              false);
      f.sink(f.local);
      assertTrue(f.source instanceof SourceHandle);
      assertTrue(f.sink.getChannel(0) instanceof SinkChannel);
      assertNull(((SourceHandle) f.source).getRuntimeFrontier());
      assertEquals("LOCAL_SERIALIZED_RESULT", ((SourceHandle) f.source).getTransportMode());
      assertEquals(
          "LOCAL_SERIALIZED_RESULT", ((SinkChannel) f.sink.getChannel(0)).getTransportMode());
    }
  }

  @Test
  public void producerFailurePreservesCauseAtConsumerAndRunsCallbacksOutsideManager()
      throws Exception {
    try (Fixture f = new Fixture(true)) {
      f.source(f.local);
      f.sink(f.local);
      IllegalArgumentException expected = new IllegalArgumentException("late cast failure");
      ((RuntimeExchangeEndpoint) f.sink.getChannel(0))
          .requestCoordinatedAbort(expected)
          .get(5, TimeUnit.SECONDS);
      assertSame(
          expected, assertThrows(IllegalStateException.class, f.source::isBlocked).getCause());
      verify(f.context).failed(same(expected));
      assertTrue(f.source.isAborted());
      assertTrue(f.sink.getChannel(0).isAborted());
    }
  }

  @Test
  public void earlyConsumerCloseReleasesBufferedOutputAndFinishesProducer() throws Exception {
    try (Fixture f = new Fixture(true)) {
      f.source(f.local);
      f.sink(f.local);
      f.sink.getChannel(0).send(block(2));
      assertEquals(1, f.out.getLeasedCreditCount());
      f.source.close();
      assertEquals(0, f.out.getLeasedCreditCount());
      assertEquals(0, f.in.getLeasedCreditCount());
      assertTrue(f.sink.isFinished());
      verify(f.context, times(1)).finished();
    }
  }

  @Test
  public void snapshotUsesIndependentInputOutputAccountsAndOnePhysicalLink() throws Exception {
    try (Fixture f = new Fixture(true)) {
      f.source(f.local);
      f.sink(f.local);
      LocalExecutionResourceRegistry output = new LocalExecutionResourceRegistry();
      output.registerRuntimeFrontier("output", f.out);
      output.registerSinkHandle(f.sink, f.producer, "sink");
      LocalExecutionResourceRegistry input = new LocalExecutionResourceRegistry();
      input.registerRuntimeFrontier("input", f.in);
      input.registerSourceHandle(
          f.source,
          new MemoryReservationKey(
              "local_endpoint", f.consumer.getFragmentInstanceId(), "exchange"));
      RuntimeResourceSnapshot outputView = output.snapshotRuntime(f.pool);
      RuntimeResourceSnapshot inputView = input.snapshotRuntime(f.pool);
      assertTrue(outputView.complete());
      assertTrue(inputView.complete());
      assertEquals(f.out.getTotalReservedBytes(), outputView.actualReservedBytes().longValue());
      assertEquals(f.in.getTotalReservedBytes(), inputView.actualReservedBytes().longValue());
      assertEquals(
          1,
          outputView.resources().stream()
              .filter(r -> r.kind().equals("RUNTIME_LOCAL_LINK"))
              .count());
      output.registerSinkHandle(f.sink, f.producer, "sink");
      assertEquals(
          outputView.resources().size(), output.snapshotRuntime(f.pool).resources().size());
      RuntimeResourceSnapshot.Tracker tracker = new RuntimeResourceSnapshot.Tracker();
      tracker.registerFrontier("output", f.out);
      tracker.registerFrontier("input", f.in);
      RuntimeLocalSinkChannel sink = (RuntimeLocalSinkChannel) f.sink.getChannel(0);
      RuntimeLocalSourceHandle source = (RuntimeLocalSourceHandle) f.source;
      tracker.register(
          sink,
          "SINK_CHANNEL",
          sink::isClosed,
          new MemoryReservationKey("local_endpoint", f.producer.getFragmentInstanceId(), "sink"),
          sink.getActualAccountKey());
      tracker.register(
          source,
          "SOURCE_HANDLE",
          source::isClosed,
          new MemoryReservationKey(
              "local_endpoint", f.consumer.getFragmentInstanceId(), "exchange"),
          source.getActualAccountKey());
      sink.send(block(7));
      RuntimeResourceSnapshot snapshot = tracker.snapshot(f.pool);
      assertTrue(snapshot.complete());
      assertEquals(2, snapshot.accounts().size());
      assertEquals(
          0L,
          snapshot.resources().stream()
              .filter(r -> r.kind().equals("SOURCE_HANDLE"))
              .findFirst()
              .get()
              .bufferedPayloadBytes()
              .longValue());
      assertTrue(
          snapshot.resources().stream()
                  .filter(r -> r.kind().equals("SINK_CHANNEL"))
                  .findFirst()
                  .get()
                  .bufferedPayloadBytes()
              > 0);
      assertEquals(7, source.receive().getColumn(0).getLong(0));
      assertEquals(f.pool.getReservedBytes(), snapshot.actualReservedBytes().longValue());
      assertEquals(
          Set.of("INPUT", "OUTPUT"),
          snapshot.resources().stream()
              .map(RuntimeResourceSnapshot.Resource::memorySide)
              .collect(java.util.stream.Collectors.toSet()));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              tracker.register(
                  source,
                  "SOURCE_HANDLE",
                  source::isClosed,
                  new MemoryReservationKey(
                      "local_endpoint", f.consumer.getFragmentInstanceId(), "exchange"),
                  sink.getActualAccountKey()));
    }
  }

  @Test
  public void coordinatedOwnerClosureWaitsForActualActiveConsumer() throws Exception {
    try (Fixture f = new Fixture(true)) {
      f.source(f.local);
      f.sink(f.local);
      RuntimeLocalExchangeLink link = ((RuntimeLocalSourceHandle) f.source).getLink();
      f.sink.getChannel(0).send(block(4));
      CountDownLatch entered = new CountDownLatch(1), exit = new CountDownLatch(1);
      Future<TsBlock> task =
          f.executor.submit(
              () ->
                  link.consume(
                      b -> {
                        entered.countDown();
                        try {
                          assertTrue(exit.await(5, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                          throw new AssertionError(e);
                        }
                        return b;
                      }));
      try {
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        LocalExecutionResourceRegistry input = new LocalExecutionResourceRegistry();
        input.registerSourceHandle(f.source);
        ListenableFuture<Void> closure =
            input.closeRuntimeOwners(true, new CancellationException());
        assertFalse(closure.isDone());
        assertEquals(1, f.out.getLeasedCreditCount());
        assertEquals(1, f.in.getLeasedCreditCount());
        exit.countDown();
        assertEquals(4, task.get(5, TimeUnit.SECONDS).getColumn(0).getLong(0));
        closure.get(5, TimeUnit.SECONDS);
        assertEquals(0, f.out.getLeasedCreditCount());
        assertEquals(0, f.in.getLeasedCreditCount());
      } finally {
        exit.countDown();
      }
    }
  }

  @Test
  public void pairingTombstonePreventsLateBindingAndRetiresAfterAuthoritativeFiTermination()
      throws Exception {
    try (Fixture f = new Fixture(true)) {
      Set<FragmentInstanceId> terminal = new HashSet<>();
      RuntimeLocalExchangeRegistry registry =
          new RuntimeLocalExchangeRegistry(f.pool, terminal::contains);
      RuntimeLocalExchangeLink.Edge edge =
          new RuntimeLocalExchangeLink.Edge(f.producer, 0, f.consumer, "exchange");
      RuntimeLocalExchangeLink link = registry.get(edge);
      assertSame(link, registry.get(edge));
      AtomicBoolean underLock = new AtomicBoolean();
      link.whenReadable()
          .addListener(
              () -> underLock.set(Thread.holdsLock(registry)), MoreExecutors.directExecutor());
      terminal.add(f.consumer);
      registry.cancel(f.consumer, new CancellationException());
      assertFalse(underLock.get());
      assertThrows(CancellationException.class, () -> registry.get(edge));
      assertThrows(IllegalStateException.class, () -> link.bindOutput(f.output));
      registry.retire(f.consumer);
      assertEquals(0, registry.size());
      assertThrows(CancellationException.class, () -> registry.get(edge));
    }
  }

  @Test
  public void duplicateFactoryPublicationDoesNotRebindEitherFrontier() throws Exception {
    try (Fixture f = new Fixture(true)) {
      f.source(f.local);
      f.sink(f.local);
      assertThrows(IllegalStateException.class, () -> f.source(f.local));
      assertThrows(IllegalStateException.class, () -> f.sink(f.local));
      f.sink.getChannel(0).send(block(8));
      assertEquals(8, f.source.receive().getColumn(0).getLong(0));
      verifyZeroInteractions(f.clients);
    }
  }

  @Test
  public void physicalLinkIsClosedIfPlanningFailsBeforeEndpointRegistration() throws Exception {
    try (Fixture f = new Fixture(true)) {
      RuntimeLocalExchangeLink link =
          new RuntimeLocalExchangeLink(
              new RuntimeLocalExchangeLink.Edge(f.producer, 0, f.consumer, "exchange"), f.pool);
      link.bindOutput(f.output);
      LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
      registry.register(link, "RUNTIME_LOCAL_LINK", f.out.getMemoryReservationKey());
      IllegalStateException expected = new IllegalStateException("planning failed");
      registry.abortUntransferred(expected);
      link.getClosedFuture().get(5, TimeUnit.SECONDS);
      assertSame(expected, link.getFailure());
      assertThrows(IllegalStateException.class, () -> link.bindInput(f.input));
    }
  }
}
