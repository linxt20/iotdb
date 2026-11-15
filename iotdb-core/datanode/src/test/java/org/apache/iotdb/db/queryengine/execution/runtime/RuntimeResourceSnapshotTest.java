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
package org.apache.iotdb.db.queryengine.execution.runtime;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SinkListener;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SourceHandleListener;
import org.apache.iotdb.db.queryengine.execution.exchange.RpcResponseLeaseScope;
import org.apache.iotdb.db.queryengine.execution.exchange.RuntimeExchangeBlockCodec;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue.MemoryReservationKey;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeResourceSnapshot.AccountState;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class RuntimeResourceSnapshotTest {
  private static MemoryPool pool() {
    return new MemoryPool("runtime-snapshot", new MemoryManager(1024), 512);
  }

  private static MemoryReservationKey key(String node) {
    return new MemoryReservationKey("q", "q.0.0", node);
  }

  private static RuntimeFrontierReservation reserve(MemoryPool pool, String node) {
    return RuntimeFrontierReservation.tryCreate(pool, key(node), 2, 32, 512).get();
  }

  @Test
  public void sharedPhysicalAccountIsCountedOnceAndWrappersKeepIndependentTerminals() {
    MemoryPool pool = pool();
    RuntimeFrontierReservation owner = reserve(pool, "group");
    RuntimeResourceSnapshot.Tracker tracker = new RuntimeResourceSnapshot.Tracker();
    AtomicBoolean sinkClosed = new AtomicBoolean();
    AtomicBoolean sourceClosed = new AtomicBoolean();
    Object sink = new Object(), source = new Object();
    try {
      tracker.registerFrontier("scan-group", owner);
      tracker.registerFrontier("scan-group", owner);
      tracker.register(sink, "SINK", sinkClosed::get, key("lane"), key("group"));
      tracker.register(source, "SOURCE_HANDLE", sourceClosed::get, key("lane"), key("group"));
      tracker.register(sink, "SINK_CHANNEL", sinkClosed::get, key("lane"), key("group"));
      RuntimeResourceSnapshot view = tracker.snapshot(pool);
      assertEquals(3, view.schemaVersion());
      assertTrue(view.complete());
      assertEquals(2, view.resources().size());
      assertEquals(1, view.accounts().size());
      assertEquals(1, view.groups().size());
      assertEquals(Long.valueOf(64), view.actualReservedBytes());
      assertEquals(64, view.groups().get(0).physicalReservedBytes());
      assertNotEquals(view.resources().get(0).token(), view.resources().get(1).token());
      sinkClosed.set(true);
      assertTrue(tracker.snapshot(pool).resources().get(0).terminal());
      assertFalse(tracker.snapshot(pool).resources().get(1).terminal());
      assertFalse(view.resources().get(0).terminal());
      assertThrows(UnsupportedOperationException.class, () -> view.resources().clear());
      assertThrows(UnsupportedOperationException.class, () -> view.accounts().clear());
      assertThrows(UnsupportedOperationException.class, () -> view.groups().clear());
    } finally {
      owner.close();
    }
  }

  @Test
  public void conflictingMappingsAndFrontierOwnersDoNotReplaceTheOriginal() {
    MemoryPool pool = pool();
    RuntimeFrontierReservation first = reserve(pool, "group");
    RuntimeFrontierReservation second = reserve(pool, "other");
    RuntimeFrontierReservation duplicateAccount = reserve(pool, "group");
    RuntimeResourceSnapshot.Tracker tracker = new RuntimeResourceSnapshot.Tracker();
    Object identity = new Object();
    try {
      tracker.register(identity, "SOURCE", () -> false, key("lane"), key("group"));
      assertThrows(
          IllegalArgumentException.class,
          () -> tracker.register(identity, "SOURCE", () -> true, key("lane"), key("other")));
      assertThrows(
          IllegalArgumentException.class,
          () -> tracker.register(identity, "SOURCE", () -> true, key("different"), key("group")));
      assertEquals(key("group"), tracker.snapshot(pool).resources().get(0).actualAccountKey());
      tracker.registerFrontier("first", first);
      assertThrows(IllegalArgumentException.class, () -> tracker.registerFrontier("first", second));
      assertThrows(
          IllegalArgumentException.class, () -> tracker.registerFrontier("renamed", first));
      assertThrows(
          IllegalArgumentException.class,
          () -> tracker.registerFrontier("duplicate", duplicateAccount));
      assertEquals(1, tracker.snapshot(pool).groups().size());
      tracker.registerFrontier("second", second);
      assertEquals(2, tracker.snapshot(pool).groups().size());
    } finally {
      duplicateAccount.close();
      second.close();
      first.close();
    }
  }

  @Test
  public void wrongFirstQueueMappingIsRejectedWithoutPoisoningRegistration() {
    MemoryPool pool = pool();
    RuntimeFrontierReservation owner = reserve(pool, "group");
    RuntimeResourceSnapshot.Tracker tracker = new RuntimeResourceSnapshot.Tracker();
    SharedTsBlockQueue queue = Mockito.mock(SharedTsBlockQueue.class);
    Mockito.when(queue.getQueueIdentityKey()).thenReturn(key("lane"));
    Mockito.when(queue.getActualAccountKey()).thenReturn(key("group"));
    Mockito.when(queue.isFrontierMode()).thenReturn(true);
    try {
      assertThrows(
          IllegalArgumentException.class,
          () -> tracker.register(queue, "LOCAL_QUEUE", () -> false, key("lane"), key("lane")));
      tracker.registerFrontier("group", owner);
      tracker.register(queue, "LOCAL_QUEUE", () -> false, key("lane"), key("group"));
      RuntimeResourceSnapshot view = tracker.snapshot(pool);
      assertEquals(1, view.resources().size());
      assertEquals("PREPAID_FRONTIER", view.resources().get(0).accountingMode());
      assertEquals("group", view.resources().get(0).groupId());
      assertEquals(Long.valueOf(64), view.actualReservedBytes());
    } finally {
      owner.close();
    }
  }

  @Test
  public void missingRegisteredZeroAndDeregisteredAccountsAreDifferent() {
    MemoryPool pool = pool();
    RuntimeResourceSnapshot.Tracker tracker = new RuntimeResourceSnapshot.Tracker();
    tracker.register(new Object(), "REMOTE_SOURCE", () -> false, key("legacy"), key("legacy"));
    RuntimeResourceSnapshot missing = tracker.snapshot(pool);
    assertEquals(AccountState.NOT_OBSERVED, missing.accounts().get(0).state());
    assertFalse(missing.accounts().get(0).fragmentPresent());
    assertNull(missing.accounts().get(0).reservedBytes());
    assertNull(missing.actualReservedBytes());
    assertFalse(missing.complete());

    pool.registerPlanNodeIdToQueryMemoryMap("q", "q.0.0", "other", MemorySide.INPUT);
    RuntimeResourceSnapshot absentNode = tracker.snapshot(pool);
    assertTrue(absentNode.accounts().get(0).fragmentPresent());
    assertEquals(AccountState.NOT_OBSERVED, absentNode.accounts().get(0).state());
    assertNull(absentNode.accounts().get(0).reservedBytes());
    pool.registerPlanNodeIdToQueryMemoryMap("q", "q.0.0", "legacy", MemorySide.INPUT);
    RuntimeResourceSnapshot zero = tracker.snapshot(pool);
    assertEquals(AccountState.PRESENT, zero.accounts().get(0).state());
    assertEquals(Long.valueOf(0), zero.actualReservedBytes());
    pool.deRegisterFragmentInstanceFromQueryMemoryMap("q", "q.0.0", true);
    RuntimeResourceSnapshot removed = tracker.snapshot(pool);
    assertEquals(AccountState.DEREGISTERED, removed.accounts().get(0).state());
    assertNull(removed.accounts().get(0).reservedBytes());
    assertNull(removed.actualReservedBytes());
  }

  @Test
  public void ownerPoolMismatchIsRejectedAndPoolIdentityIsNotAStringId() {
    MemoryPool firstPool = pool(), secondPool = pool();
    RuntimeFrontierReservation owner = reserve(firstPool, "group");
    RuntimeResourceSnapshot.Tracker tracker = new RuntimeResourceSnapshot.Tracker();
    try {
      tracker.registerFrontier("group", owner);
      assertSame(firstPool, owner.getMemoryPool());
      assertThrows(IllegalArgumentException.class, () -> tracker.snapshot(secondPool));
      assertEquals(Long.valueOf(64), tracker.snapshot(firstPool).actualReservedBytes());
    } finally {
      owner.close();
    }

    RuntimeResourceSnapshot.Tracker legacy = new RuntimeResourceSnapshot.Tracker();
    legacy.register(new Object(), "SOURCE", () -> true, key("same"), key("same"));
    firstPool.registerPlanNodeIdToQueryMemoryMap("q", "q.0.0", "same", MemorySide.INPUT);
    secondPool.registerPlanNodeIdToQueryMemoryMap("q", "q.0.0", "same", MemorySide.INPUT);
    long firstToken = legacy.snapshot(firstPool).accounts().get(0).poolIdentityToken();
    long secondToken = legacy.snapshot(secondPool).accounts().get(0).poolIdentityToken();
    assertNotEquals(firstToken, secondToken);
  }

  @Test
  public void groupShowsCreditsAndClosureWithoutControllingTheirLifetime() throws Exception {
    MemoryPool pool = pool();
    RuntimeFrontierReservation owner = reserve(pool, "group");
    RuntimeResourceSnapshot.Tracker tracker = new RuntimeResourceSnapshot.Tracker();
    tracker.registerFrontier("group", owner);
    RuntimeFrontierReservation.BlockCredit credit = owner.acquire(0, 16);
    owner.requestClose();
    RuntimeResourceSnapshot.Group pending = tracker.snapshot(pool).groups().get(0);
    assertEquals(16, pending.internalUsedBytes());
    assertEquals(1, pending.leasedCreditCount());
    assertTrue(pending.closeRequested());
    assertEquals("PENDING", pending.closeState());
    assertEquals(64, pool.getReservedBytes());
    owner.release(credit);
    RuntimeResourceSnapshot.Group closed = tracker.snapshot(pool).groups().get(0);
    assertEquals("CLOSED", closed.closeState());
    assertEquals(0, closed.physicalReservedBytes());
    assertEquals(Long.valueOf(0), tracker.snapshot(pool).actualReservedBytes());
    pool.deRegisterFragmentInstanceFromQueryMemoryMap("q", "q.0.0", true);
    assertEquals(AccountState.DEREGISTERED, tracker.snapshot(pool).accounts().get(0).state());
  }

  @Test
  public void unmappedMemoryResourcesCannotClaimZeroActualBytes() {
    RuntimeResourceSnapshot.Tracker tracker = new RuntimeResourceSnapshot.Tracker();
    tracker.register(new Object(), "SOURCE_HANDLE", () -> false, null, null);
    RuntimeResourceSnapshot value = tracker.snapshot(pool());
    assertFalse(value.complete());
    assertNull(value.actualReservedBytes());
    assertEquals("UNMAPPED", value.resources().get(0).accountingMode());
    RuntimeResourceSnapshot.Tracker factories = new RuntimeResourceSnapshot.Tracker();
    factories.register(new Object(), "FACTORY", () -> false, null, null);
    assertEquals(Long.valueOf(0), factories.snapshot(pool()).actualReservedBytes());
    assertEquals("NO_ACCOUNT", factories.snapshot(pool()).resources().get(0).accountingMode());
  }

  @Test
  public void ambiguousFreeFailureKeepsOwnerReceiptSeparateFromActualAccount() {
    RuntimeException expected = new RuntimeException("callback failed after accounting");
    MemoryPool pool =
        new MemoryPool("ambiguous-free", new MemoryManager(1024), 512) {
          @Override
          public void free(String query, String fragment, String node, long bytes) {
            super.free(query, fragment, node, bytes);
            throw expected;
          }
        };
    RuntimeFrontierReservation owner = reserve(pool, "group");
    RuntimeResourceSnapshot.Tracker tracker = new RuntimeResourceSnapshot.Tracker();
    tracker.registerFrontier("group", owner);
    owner.requestClose();
    RuntimeResourceSnapshot value = tracker.snapshot(pool);
    assertEquals("FAILED", value.groups().get(0).closeState());
    assertEquals(RuntimeException.class.getName(), value.groups().get(0).closeFailure());
    assertEquals(64, value.groups().get(0).physicalReservedBytes());
    assertEquals(Long.valueOf(0), value.accounts().get(0).reservedBytes());
    assertEquals(Long.valueOf(0), value.actualReservedBytes());
    owner.close();
    assertEquals(0, pool.getReservedBytes());
  }

  @Test
  public void concurrentConflictingRegistrationHasOneWinner() throws Exception {
    RuntimeResourceSnapshot.Tracker tracker = new RuntimeResourceSnapshot.Tracker();
    Object identity = new Object();
    java.util.concurrent.CyclicBarrier start = new java.util.concurrent.CyclicBarrier(2);
    java.util.concurrent.ExecutorService executor =
        java.util.concurrent.Executors.newFixedThreadPool(2);
    try {
      java.util.List<java.util.concurrent.Future<MemoryReservationKey>> tasks =
          new java.util.ArrayList<>();
      for (String node : new String[] {"first", "second"}) {
        MemoryReservationKey mapping = key(node);
        tasks.add(
            executor.submit(
                () -> {
                  start.await(5, java.util.concurrent.TimeUnit.SECONDS);
                  try {
                    tracker.register(identity, "REMOTE", () -> false, mapping, mapping);
                    return mapping;
                  } catch (IllegalArgumentException conflict) {
                    return null;
                  }
                }));
      }
      MemoryReservationKey first = tasks.get(0).get(5, java.util.concurrent.TimeUnit.SECONDS);
      MemoryReservationKey second = tasks.get(1).get(5, java.util.concurrent.TimeUnit.SECONDS);
      assertTrue((first == null) != (second == null));
      RuntimeResourceSnapshot value = tracker.snapshot(pool());
      assertEquals(1, value.resources().size());
      assertEquals(first == null ? second : first, value.resources().get(0).actualAccountKey());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void nativeInputOutputAccountsAndRepeatedWrappersHaveOnePhysicalTotal() throws Exception {
    try (EndpointFixture f = new EndpointFixture()) {
      assertTrue(f.sink.isRuntimeFrontierMode());
      assertTrue(f.source.isRuntimeFrontierMode());
      f.registerEndpoints();
      f.tracker.register(f.sink, "SINK_CHANNEL", f.sink::isClosed, f.sinkLogical, f.outputKey);
      f.tracker.register(f.source, "SOURCE_ALIAS", f.source::isClosed, f.sourceLogical, f.inputKey);
      RuntimeResourceSnapshot view = f.tracker.snapshot(f.pool);
      assertEquals(2, view.resources().size());
      assertEquals(2, view.accounts().size());
      assertEquals(2, view.groups().size());
      assertTrue(view.complete());
      assertEquals(Long.valueOf(f.totalPrepaid), view.actualReservedBytes());
      assertEquals(f.totalPrepaid, f.pool.getReservedBytes());
      assertEquals(
          Set.of(f.outputKey, f.inputKey),
          view.accounts().stream()
              .map(RuntimeResourceSnapshot.Account::actualAccountKey)
              .collect(Collectors.toSet()));
      for (RuntimeResourceSnapshot.Account account : view.accounts()) {
        assertEquals(AccountState.PRESENT, account.state());
        assertEquals(
            account.actualAccountKey().equals(f.inputKey) ? "INPUT" : "OUTPUT",
            account.memorySide());
      }
      for (RuntimeResourceSnapshot.Resource resource : view.resources()) {
        assertEquals("PREPAID_FRONTIER", resource.accountingMode());
        assertEquals("ENVELOPE_UPPER_BOUND", resource.creditAccounting());
        assertEquals(Long.valueOf(0), resource.leasedCapacityBytes());
        assertEquals(Long.valueOf(0), resource.bufferedPayloadBytes());
        assertNull(resource.bufferRetainedBytes());
        assertEquals(Integer.valueOf(0), resource.inFlightTaskCount());
        assertEquals(Integer.valueOf(0), resource.consumerCount());
        assertEquals(Integer.valueOf(0), resource.responsePinCount());
        assertEquals(Integer.valueOf(0), resource.pendingCreditReturnCount());
        assertEquals("NOT_REQUESTED", resource.endpointCloseState());
        assertEquals(0, resource.laneIndex());
      }
      assertEquals("UNSUPPORTED_OUTSIDE_EXCHANGE_POOL", view.transportMemoryAccounting());
      f.sink.requestCoordinatedClose().get(5, TimeUnit.SECONDS);
      RuntimeResourceSnapshot afterSinkClose = f.tracker.snapshot(f.pool);
      assertTrue(resource(afterSinkClose, f.outputKey).terminal());
      assertFalse(resource(afterSinkClose, f.inputKey).terminal());
      assertEquals("CLOSED", resource(afterSinkClose, f.outputKey).endpointCloseState());
      // Endpoint closure and zero payload do not return the prepaid owner reservation.
      assertEquals(Long.valueOf(f.totalPrepaid), afterSinkClose.actualReservedBytes());
      assertFalse(resource(view, f.outputKey).terminal());
    }
  }

  @Test
  public void responsePinsAndEnvelopeCreditsAreObservedWithoutAddingPhysicalBytes()
      throws Exception {
    try (EndpointFixture f = new EndpointFixture()) {
      f.registerEndpoints();
      f.sink.open();
      f.sink.send(
          new TsBlock(
              new TimeColumn(2, new long[] {1, 2}),
              new LongColumn(2, Optional.empty(), new long[] {10, 20})));
      // A real executor FIFO receipt settles the metadata task; the client itself is a mock.
      f.executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
      RuntimeResourceSnapshot buffered = f.tracker.snapshot(f.pool);
      RuntimeResourceSnapshot.Resource output = resource(buffered, f.outputKey);
      assertEquals(Long.valueOf(f.bounds.outputEnvelopeBytes()), output.leasedCapacityBytes());
      assertTrue(output.bufferedPayloadBytes() > 0);
      assertEquals(Integer.valueOf(0), output.responsePinCount());
      assertEquals(Long.valueOf(f.totalPrepaid), buffered.actualReservedBytes());
      ListenableFuture<Void> endpointClosed;
      try (RpcResponseLeaseScope scope = RpcResponseLeaseScope.enter()) {
        ByteBuffer bytes = f.sink.borrowSerializedTsBlock(0, scope);
        RuntimeResourceSnapshot borrowed = f.tracker.snapshot(f.pool);
        assertEquals(Integer.valueOf(1), resource(borrowed, f.outputKey).responsePinCount());
        assertEquals(
            Long.valueOf(bytes.remaining()),
            resource(borrowed, f.outputKey).bufferedPayloadBytes());
        f.sink.acknowledgeTsBlock(0, 1);
        endpointClosed = f.sink.requestCoordinatedClose();
        f.output.requestClose();
        RuntimeResourceSnapshot pinned = f.tracker.snapshot(f.pool);
        output = resource(pinned, f.outputKey);
        assertTrue(output.terminal());
        assertEquals("PENDING", output.endpointCloseState());
        assertEquals(Integer.valueOf(1), output.responsePinCount());
        assertEquals(Long.valueOf(0), output.bufferedPayloadBytes());
        assertEquals(Long.valueOf(f.bounds.outputEnvelopeBytes()), output.leasedCapacityBytes());
        assertEquals(Long.valueOf(f.totalPrepaid), pinned.actualReservedBytes());
        assertFalse(endpointClosed.isDone());
        assertFalse(f.output.getClosedFuture().isDone());
      }
      endpointClosed.get(5, TimeUnit.SECONDS);
      f.output.getClosedFuture().get(5, TimeUnit.SECONDS);
      RuntimeResourceSnapshot returned = f.tracker.snapshot(f.pool);
      output = resource(returned, f.outputKey);
      assertEquals(Integer.valueOf(0), output.responsePinCount());
      assertEquals(Long.valueOf(0), output.leasedCapacityBytes());
      assertEquals("CLOSED", output.endpointCloseState());
      assertEquals(Long.valueOf(f.bounds.inputEnvelopeBytes()), returned.actualReservedBytes());
      assertEquals(f.bounds.inputEnvelopeBytes(), f.pool.getReservedBytes());
    }
  }

  @Test
  public void nativeEndpointDualKeyErrorsCannotPoisonLaterCorrectRegistration() throws Exception {
    try (EndpointFixture f = new EndpointFixture()) {
      assertThrows(
          IllegalArgumentException.class,
          () -> f.tracker.register(f.sink, "SINK", f.sink::isClosed, f.sinkLogical, f.sinkLogical));
      assertThrows(
          IllegalArgumentException.class,
          () -> f.tracker.register(f.sink, "SINK", f.sink::isClosed, f.sourceLogical, f.outputKey));
      assertThrows(
          IllegalArgumentException.class,
          () ->
              f.tracker.register(
                  f.source, "SOURCE", f.source::isClosed, f.sourceLogical, f.outputKey));
      assertThrows(
          IllegalArgumentException.class,
          () -> f.tracker.register(f.source, "SOURCE", f.source::isClosed, f.inputKey, f.inputKey));
      f.registerEndpoints();
      RuntimeResourceSnapshot view = f.tracker.snapshot(f.pool);
      assertEquals(2, view.resources().size());
      assertEquals(2, view.accounts().size());
      assertEquals(f.sinkLogical, resource(view, f.outputKey).queueIdentityKey());
      assertEquals(f.sourceLogical, resource(view, f.inputKey).queueIdentityKey());
      assertEquals("OUTPUT", resource(view, f.outputKey).memorySide());
      assertEquals("INPUT", resource(view, f.inputKey).memorySide());
      assertEquals(Long.valueOf(f.totalPrepaid), view.actualReservedBytes());
      assertThrows(
          IllegalArgumentException.class,
          () -> f.tracker.register(f.sink, "SINK", () -> true, f.sinkLogical, f.inputKey));
      assertEquals(Long.valueOf(f.totalPrepaid), f.tracker.snapshot(f.pool).actualReservedBytes());
    }
  }

  private static RuntimeResourceSnapshot.Resource resource(
      RuntimeResourceSnapshot snapshot, MemoryReservationKey actualKey) {
    return snapshot.resources().stream()
        .filter(resource -> actualKey.equals(resource.actualAccountKey()))
        .findFirst()
        .orElseThrow();
  }

  private static final class EndpointFixture implements AutoCloseable {
    final RuntimeExchangeBlockCodec.Bounds bounds =
        new RuntimeExchangeBlockCodec.Bounds(128, 1, 4096, 4096, 4096, 8192, 8192)
            .withSchema(List.of(TSDataType.INT64));
    final long totalPrepaid =
        Math.addExact(bounds.outputEnvelopeBytes(), bounds.inputEnvelopeBytes());
    final MemoryPool pool =
        new MemoryPool(
            "endpoint-v3",
            new MemoryManager(Math.multiplyExact(4, totalPrepaid)),
            Math.multiplyExact(2, totalPrepaid));
    final MemoryReservationKey outputKey =
        new MemoryReservationKey("endpoint_v3", "0.0", "paid_native_output");
    final MemoryReservationKey inputKey =
        new MemoryReservationKey("endpoint_v3", "0.0", "paid_remote_input");
    final MemoryReservationKey sinkLogical =
        new MemoryReservationKey("endpoint_v3", "0.0", "logical_sink");
    final MemoryReservationKey sourceLogical =
        new MemoryReservationKey("endpoint_v3", "0.0", "logical_source");
    final RuntimeFrontierReservation output =
        RuntimeFrontierReservation.tryCreate(
                pool,
                outputKey,
                MemorySide.OUTPUT,
                1,
                bounds.outputEnvelopeBytes(),
                Math.multiplyExact(2, bounds.outputEnvelopeBytes()))
            .orElseThrow();
    final RuntimeFrontierReservation input =
        RuntimeFrontierReservation.tryCreate(
                pool,
                inputKey,
                MemorySide.INPUT,
                1,
                bounds.inputEnvelopeBytes(),
                Math.multiplyExact(2, bounds.inputEnvelopeBytes()))
            .orElseThrow();
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final RuntimeResourceSnapshot.Tracker tracker = new RuntimeResourceSnapshot.Tracker();
    final SinkChannel sink;
    final SourceHandle source;

    @SuppressWarnings("unchecked")
    EndpointFixture() throws Exception {
      LocalMemoryManager memory = Mockito.mock(LocalMemoryManager.class);
      Mockito.when(memory.getQueryPool()).thenReturn(pool);
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clients =
          Mockito.mock(IClientManager.class);
      TEndPoint endpoint = new TEndPoint("endpoint-v3-peer", 10740);
      SyncDataNodeMPPDataExchangeServiceClient client =
          Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
      Mockito.when(clients.borrowClient(endpoint)).thenReturn(client);
      TFragmentInstanceId local = new TFragmentInstanceId("endpoint_v3", 0, "0");
      TFragmentInstanceId peer = new TFragmentInstanceId("endpoint_v3", 1, "0");
      // Exercise the convenience frontier constructor too: it must never select legacy mode.
      sink =
          new SinkChannel(
              endpoint,
              peer,
              "remote_exchange",
              "logical_sink",
              local,
              memory,
              executor,
              new TsBlockSerde(),
              Mockito.mock(SinkListener.class),
              clients,
              new RuntimeEndpointFrontier(output, 0, bounds));
      source =
          new SourceHandle(
              endpoint,
              peer,
              local,
              "logical_source",
              0,
              memory,
              executor,
              new TsBlockSerde(),
              Mockito.mock(SourceHandleListener.class),
              false,
              clients,
              new RuntimeEndpointFrontier(input, 0, bounds));
      sink.setRetryIntervalInMs(0);
      source.setRetryIntervalInMs(0);
    }

    void registerEndpoints() {
      tracker.registerFrontier("native-output", output);
      tracker.registerFrontier("remote-input", input);
      tracker.register(sink, "SINK_CHANNEL", sink::isClosed, sinkLogical, outputKey);
      tracker.register(source, "SOURCE_HANDLE", source::isClosed, sourceLogical, inputKey);
    }

    @Override
    public void close() throws Exception {
      try {
        sink.requestCoordinatedAbort().get(5, TimeUnit.SECONDS);
      } finally {
        try {
          source.requestCoordinatedAbort().get(5, TimeUnit.SECONDS);
        } finally {
          try {
            output.requestClose().get(5, TimeUnit.SECONDS);
            input.requestClose().get(5, TimeUnit.SECONDS);
            assertEquals(0, pool.getReservedBytes());
          } finally {
            executor.shutdownNow();
          }
        }
      }
    }
  }
}
