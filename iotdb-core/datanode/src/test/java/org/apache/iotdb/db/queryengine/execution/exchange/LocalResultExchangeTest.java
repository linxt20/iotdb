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
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeEndpointFrontier;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeResourceSnapshot;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockRequest;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockResponse;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class LocalResultExchangeTest {
  private static final AtomicInteger IDS = new AtomicInteger();
  private static final RuntimeExchangeBlockCodec.Bounds BOUNDS =
      new RuntimeExchangeBlockCodec.Bounds(
          16, 1, 4096, 4096, 4096, 8192, 8192, List.of(TSDataType.INT64));

  private static TsBlock block(long value) {
    return new TsBlock(
        1,
        new TimeColumn(1, new long[] {7}),
        new Column[] {new LongColumn(1, Optional.empty(), new long[] {value})});
  }

  private static void await(BooleanSupplier ready) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!ready.getAsBoolean() && System.nanoTime() < deadline) {
      java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
    assertTrue(ready.getAsBoolean());
  }

  private static void waitFor(CountDownLatch latch) {
    try {
      assertTrue(latch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static final class Fixture implements AutoCloseable {
    final String query = "local_result_" + IDS.incrementAndGet();
    final FragmentInstanceId producer =
        FragmentInstanceId.fromThrift(new TFragmentInstanceId(query, 1, "0"));
    final FragmentInstanceId consumer =
        FragmentInstanceId.fromThrift(new TFragmentInstanceId(query, 0, "0"));
    final MemoryPool pool = new MemoryPool("native-result", new MemoryManager(1L << 22), 1L << 21);
    final RuntimeFrontierReservation out =
        RuntimeFrontierReservation.tryCreate(
                pool,
                new MemoryReservationKey(query, producer.getFragmentInstanceId(), "prepaid-output"),
                MemorySide.OUTPUT,
                1,
                BOUNDS.outputEnvelopeBytes(),
                1L << 21)
            .get();
    final RuntimeEndpointFrontier frontier = new RuntimeEndpointFrontier(out, 0, BOUNDS);
    final LocalMemoryManager memory = mock(LocalMemoryManager.class);
    final ExecutorService executor = Executors.newFixedThreadPool(3);
    final IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clients =
        mock(IClientManager.class);
    final FragmentInstanceContext context = mock(FragmentInstanceContext.class);
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final TEndPoint local =
        new TEndPoint(
            IoTDBDescriptor.getInstance().getConfig().getInternalAddress(),
            IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort());
    final MPPDataExchangeManager manager;
    final SourceHandle source;
    final ISinkHandle handle;
    final SinkChannel sink;
    LocalResultExchange exchange;

    Fixture(Consumer<TGetDataBlockResponse> copy) throws Exception {
      this(copy, false);
    }

    Fixture(Consumer<TGetDataBlockResponse> copy, boolean delayProducer) throws Exception {
      when(memory.getQueryPool()).thenReturn(pool);
      IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
      boolean old = config.isEnableRuntimeLocalExchange();
      try {
        config.setEnableRuntimeLocalExchange(true);
        manager =
            new MPPDataExchangeManager(memory, TsBlockSerde::new, executor, clients) {
              @Override
              protected LocalResultExchange createLocalResultExchange(
                  RuntimeLocalExchangeLink.Edge edge) {
                MPPDataExchangeServiceImpl service = getOrCreateMPPDataExchangeServiceImpl();
                exchange =
                    new LocalResultExchange(edge, service, service::getLocalResultDataBlock, copy);
                return exchange;
              }
            };
      } finally {
        config.setEnableRuntimeLocalExchange(old);
      }
      source =
          (SourceHandle)
              manager.createResultSourceHandle(
                  consumer.toThrift(),
                  "result",
                  0,
                  local,
                  producer.toThrift(),
                  t -> {
                    assertFalse(Thread.holdsLock(manager));
                    failure.set(t);
                  },
                  false);
      if (delayProducer) {
        handle = null;
        sink = null;
      } else {
        handle =
            manager.createShuffleSinkHandle(
                List.of(new DownStreamChannelLocation(local, consumer.toThrift(), "result")),
                new DownStreamChannelIndex(0),
                ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
                producer.toThrift(),
                "sink",
                context,
                List.of(frontier));
        sink = (SinkChannel) handle.getChannel(0);
      }
      source.setRetryIntervalInMs(0);
      if (sink != null) {
        sink.setRetryIntervalInMs(0);
        sink.open();
      }
    }

    long inputBytes() {
      return pool.getFragmentMemoryReservedBytes(
          query, consumer.getFragmentInstanceId(), MemorySide.INPUT);
    }

    void send(long value) throws Exception {
      await(() -> sink.isFull().isDone());
      sink.send(block(value));
    }

    long receive() throws Exception {
      source.isBlocked().get(5, TimeUnit.SECONDS);
      return new TsBlockSerde().deserialize(source.getSerializedTsBlock()).getColumn(0).getLong(0);
    }

    @Override
    public void close() throws Exception {
      try {
        try {
          source.requestCoordinatedClose().get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
          assertNotNull(failure.get());
        }
        if (sink != null) sink.requestCoordinatedClose().get(5, TimeUnit.SECONDS);
        if (handle != null) handle.close();
        out.requestClose().get(5, TimeUnit.SECONDS);
        assertEquals(0, pool.getReservedBytes());
        assertEquals(0, pool.getMemoryReservationSize());
        manager.deRegisterFragmentInstanceFromMemoryPool(
            query, producer.getFragmentInstanceId(), false);
        manager.deRegisterFragmentInstanceFromMemoryPool(
            query, consumer.getFragmentInstanceId(), false);
        assertEquals(0, pool.getQueryMemoryReservationSize());
        verifyZeroInteractions(clients);
      } finally {
        executor.shutdownNow();
      }
    }
  }

  @Test
  public void realFactoryUsesNativeInputAndDetachedSerializedResultsWithoutRpcClients()
      throws Exception {
    try (Fixture f = new Fixture(response -> {})) {
      assertEquals("LOCAL_SERIALIZED_RESULT", f.source.getTransportMode());
      assertEquals("LOCAL_SERIALIZED_RESULT", f.sink.getTransportMode());
      assertNull(f.source.getRuntimeFrontier());
      assertEquals(0, f.inputBytes());
      for (long value : new long[] {3, 8, 42}) {
        f.send(value);
        assertEquals(value, f.receive());
      }
      f.sink.setNoMoreTsBlocks();
      await(f.source::isFinished);
      assertNull(RemoteShufflePayloadByteTracker.getInstance().snapshot(f.query));
    }
  }

  @Test
  public void fragmentOffsetsPreserveAllSerializedBytes() throws Exception {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    int old = config.getMppDataExchangeMaxPayloadSizeInBytes();
    try {
      config.setMppDataExchangeMaxPayloadSizeInBytes(7);
      AtomicInteger fragments = new AtomicInteger();
      try (Fixture f = new Fixture(response -> fragments.incrementAndGet())) {
        f.send(123456789);
        assertEquals(123456789, f.receive());
        assertTrue(fragments.get() > 1);
      }
    } finally {
      config.setMppDataExchangeMaxPayloadSizeInBytes(old);
    }
  }

  @Test
  public void copyPinsProducerAndRetainsNativeInputUntilActualExitOnCancellation()
      throws Exception {
    CountDownLatch entered = new CountDownLatch(1), exit = new CountDownLatch(1);
    try (Fixture f =
        new Fixture(
            response -> {
              entered.countDown();
              waitFor(exit);
            })) {
      try {
        f.source.isBlocked();
        f.send(9);
        waitFor(entered);
        assertEquals(1, f.sink.getRuntimeResponsePinCount());
        assertTrue(f.inputBytes() > 0);
        com.google.common.util.concurrent.ListenableFuture<Void> inputClose =
            f.source.requestCoordinatedAbort(new RuntimeException("cancel"));
        com.google.common.util.concurrent.ListenableFuture<Void> outputClose =
            f.sink.requestCoordinatedClose();
        com.google.common.util.concurrent.ListenableFuture<Void> physicalClose =
            f.out.requestClose();
        assertFalse(inputClose.isDone());
        assertFalse(outputClose.isDone());
        assertFalse(physicalClose.isDone());
        assertTrue(f.inputBytes() > 0);
        assertEquals(1, f.out.getLeasedCreditCount());
        exit.countDown();
        inputClose.get(5, TimeUnit.SECONDS);
        outputClose.get(5, TimeUnit.SECONDS);
        physicalClose.get(5, TimeUnit.SECONDS);
        assertEquals(0, f.inputBytes());
        assertEquals(0, f.pool.getReservedBytes());
      } finally {
        exit.countDown();
      }
    }
  }

  @Test
  public void copyFailurePreservesCauseAndSettlesNativeInputAfterTaskExit() throws Exception {
    RuntimeException expected = new RuntimeException("copy failure");
    try (Fixture f =
        new Fixture(
            response -> {
              throw expected;
            })) {
      f.source.isBlocked();
      f.send(4);
      await(() -> f.failure.get() != null && f.source.getRuntimeInFlightTaskCount() == 0);
      assertSame(expected, f.failure.get());
      assertEquals(0, f.inputBytes());
      ExecutionException failure =
          assertThrows(
              ExecutionException.class,
              () -> f.source.requestCoordinatedAbort(expected).get(5, TimeUnit.SECONDS));
      assertSame(expected, failure.getCause());
      await(() -> f.out.getLeasedCreditCount() == 0);
    }
  }

  @Test
  public void pendingNativeReservationCancelsBeforeWaitingForItsContinuation() throws Exception {
    try (Fixture f = new Fixture(response -> {})) {
      long bytes = block(1).getSizeInBytes();
      f.source.setMaxBytesCanReserve(bytes);
      f.source.isBlocked();
      f.send(1);
      await(() -> f.inputBytes() > 0 && f.sink.isFull().isDone());
      f.send(2);
      await(() -> f.pool.getMemoryReservationSize() > 0);
      f.source
          .requestCoordinatedAbort(new RuntimeException("stop pending input"))
          .get(5, TimeUnit.SECONDS);
      assertEquals(0, f.pool.getMemoryReservationSize());
      assertEquals(0, f.inputBytes());
      await(() -> f.out.getLeasedCreditCount() == 0);
    }
  }

  @Test
  public void edgeValidationRejectsWrongProducerChannelAndTargetWithoutDelegation()
      throws Exception {
    try (Fixture f = new Fixture(response -> {})) {
      assertThrows(
          IllegalArgumentException.class,
          () -> f.exchange.getDataBlock(new TGetDataBlockRequest(f.producer.toThrift(), 0, 1, 7)));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              f.exchange.getDataBlock(
                  new TGetDataBlockRequest(
                      new TFragmentInstanceId("other_query", 1, "0"), 0, 1, 0)));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              f.exchange.onNewDataBlockEvent(
                  new org.apache.iotdb.mpp.rpc.thrift.TNewDataBlockEvent(
                      f.consumer.toThrift(),
                      "wrong-plan",
                      f.producer.toThrift(),
                      0,
                      List.of(16L))));
      assertEquals(0, f.source.getRuntimeInFlightTaskCount());
      assertEquals(0, f.inputBytes());
    }
  }

  @Test
  public void detachedCopyNeverAliasesProducerResponseBacking() throws Exception {
    AtomicReference<ByteBuffer> original = new AtomicReference<>();
    try (Fixture f = new Fixture(response -> original.set(response.getTsBlocks().get(0)))) {
      f.source.isBlocked();
      f.send(55);
      f.source.isBlocked().get(5, TimeUnit.SECONDS);
      ByteBuffer result = f.source.getSerializedTsBlock();
      assertNotSame(original.get().array(), result.array());
      original.get().put(original.get().position(), (byte) 127);
      assertEquals(55, new TsBlockSerde().deserialize(result).getColumn(0).getLong(0));
    }
  }

  @Test
  public void snapshotReportsRealNativeInputAccountTasksAndLocalTransport() throws Exception {
    CountDownLatch entered = new CountDownLatch(1), exit = new CountDownLatch(1);
    try (Fixture f =
        new Fixture(
            response -> {
              entered.countDown();
              waitFor(exit);
            })) {
      try {
        RuntimeResourceSnapshot.Tracker tracker = new RuntimeResourceSnapshot.Tracker();
        tracker.registerFrontier("out", f.out);
        tracker.register(
            f.source,
            "SOURCE_HANDLE",
            f.source::isClosed,
            f.source.getActualAccountKey(),
            f.source.getActualAccountKey());
        tracker.register(
            f.sink,
            "SINK_CHANNEL",
            f.sink::isClosed,
            new MemoryReservationKey(f.query, f.producer.getFragmentInstanceId(), "sink"),
            f.sink.getActualAccountKey());
        f.source.isBlocked();
        f.send(6);
        waitFor(entered);
        RuntimeResourceSnapshot snapshot = tracker.snapshot(f.pool);
        assertTrue(snapshot.complete());
        RuntimeResourceSnapshot.Resource input =
            snapshot.resources().stream()
                .filter(r -> r.kind().equals("SOURCE_HANDLE"))
                .findFirst()
                .get();
        assertEquals("LEGACY", input.accountingMode());
        assertEquals("LOCAL_SERIALIZED_RESULT", input.transportMode());
        assertTrue(input.inFlightTaskCount() > 0);
        assertEquals(f.pool.getReservedBytes(), snapshot.actualReservedBytes().longValue());
        assertTrue(f.inputBytes() > 0);
        exit.countDown();
        assertEquals(6, f.receive());
      } finally {
        exit.countDown();
      }
    }
  }

  @Test
  public void retiredConsumerKeepsTombstoneUntilLateProducerIsRejectedAndRetired()
      throws Exception {
    try (Fixture f = new Fixture(response -> {}, true)) {
      f.source.requestCoordinatedClose().get(5, TimeUnit.SECONDS);
      f.manager.deRegisterFragmentInstanceFromMemoryPool(
          f.query, f.consumer.getFragmentInstanceId(), false);
      assertThrows(
          java.util.concurrent.CancellationException.class,
          () ->
              f.manager.createShuffleSinkHandle(
                  List.of(new DownStreamChannelLocation(f.local, f.consumer.toThrift(), "result")),
                  new DownStreamChannelIndex(0),
                  ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
                  f.producer.toThrift(),
                  "sink",
                  f.context,
                  List.of(f.frontier)));
      assertEquals(0, f.manager.getShuffleSinkHandleSize());
      assertEquals(0, f.out.getLeasedCreditCount());
    }
  }

  @Test
  public void producerPublicationAndConsumerClosureCannotLoseTheClose() throws Exception {
    CountDownLatch publishing = new CountDownLatch(1), finishPublication = new CountDownLatch(1);
    try (Fixture f = new Fixture(response -> {}, true)) {
      f.exchange.bindProducer(f.frontier, f.producer.toThrift());
      AtomicInteger closed = new AtomicInteger();
      java.util.concurrent.atomic.AtomicBoolean published =
          new java.util.concurrent.atomic.AtomicBoolean();
      try {
        java.util.concurrent.Future<?> publication =
            f.executor.submit(
                () ->
                    f.exchange.publishProducer(
                        () -> {
                          publishing.countDown();
                          waitFor(finishPublication);
                          published.set(true);
                        },
                        () -> {
                          assertFalse(Thread.holdsLock(f.exchange));
                          assertTrue(published.get());
                          closed.incrementAndGet();
                        }));
        waitFor(publishing);
        java.util.concurrent.Future<?> closure = f.executor.submit(f.exchange::markConsumerClosed);
        finishPublication.countDown();
        publication.get(5, TimeUnit.SECONDS);
        closure.get(5, TimeUnit.SECONDS);
        assertEquals(1, closed.get());
      } finally {
        finishPublication.countDown();
      }
    }
  }

  @Test
  public void closureBeforePublicationCannotPublishAnyProducer() throws Exception {
    try (Fixture f = new Fixture(response -> {}, true)) {
      f.exchange.bindProducer(f.frontier, f.producer.toThrift());
      f.exchange.markConsumerClosed();
      AtomicInteger published = new AtomicInteger();
      assertThrows(
          java.util.concurrent.CancellationException.class,
          () -> f.exchange.publishProducer(published::incrementAndGet, () -> {}));
      assertEquals(0, published.get());
    }
  }

  @Test
  public void reentrantResultCloseDoesNotInvokeProducerCallbacksUnderSourceMonitor()
      throws Exception {
    try (Fixture f = new Fixture(response -> {})) {
      java.util.concurrent.atomic.AtomicBoolean underSource =
          new java.util.concurrent.atomic.AtomicBoolean();
      org.mockito.Mockito.doAnswer(
              call -> {
                underSource.set(Thread.holdsLock(f.source));
                return null;
              })
          .when(f.context)
          .finished();
      AtomicReference<com.google.common.util.concurrent.ListenableFuture<Void>> closing =
          new AtomicReference<>();
      f.source
          .isBlocked()
          .addListener(
              () -> closing.set(f.source.requestCoordinatedClose()),
              com.google.common.util.concurrent.MoreExecutors.directExecutor());
      f.send(8);
      await(() -> closing.get() != null);
      closing.get().get(5, TimeUnit.SECONDS);
      assertFalse(underSource.get());
      org.mockito.Mockito.verify(f.context).finished();
    }
  }
}
