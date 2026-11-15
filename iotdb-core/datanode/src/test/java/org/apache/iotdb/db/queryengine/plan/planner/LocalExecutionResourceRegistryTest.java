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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.OptionalLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class LocalExecutionResourceRegistryTest {

  @Test
  public void queueRegistrationIsPhysicalAndIdempotent() {
    SharedTsBlockQueue.MemoryReservationKey key =
        new SharedTsBlockQueue.MemoryReservationKey("q", "q.0.i", "scan");
    SharedTsBlockQueue queue = Mockito.mock(SharedTsBlockQueue.class);
    Mockito.when(queue.getMemoryReservationKey()).thenReturn(key);
    Mockito.when(queue.getQueueIdentityKey()).thenReturn(key);
    Mockito.when(queue.getActualAccountKey()).thenReturn(key);

    LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
    registry.registerQueue(queue);
    registry.registerQueue(queue);
    registry.register(new Object(), "REMOTE_CHANNEL", key);

    LocalExecutionResourceRegistry.Snapshot snapshot = registry.snapshot(4_022_509L, 4_022_509L);
    assertEquals(2, snapshot.objectCount());
    assertEquals(1, snapshot.uniqueMemoryKeyCount());
    assertEquals(OptionalLong.of(2_011_254L), snapshot.nominalShareBytes());
    assertEquals(4_022_509L, snapshot.effectiveRequestBytes());
    assertEquals(
        LocalExecutionResourceRegistry.QuotaPolicy.LEGACY_COMPATIBILITY, snapshot.quotaPolicy());
  }

  @Test
  public void emptyRegistryHasNoNominalShareAndSnapshotIsImmutable() {
    LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
    LocalExecutionResourceRegistry.Snapshot snapshot = registry.snapshot(1L, 1L);

    assertEquals(0, snapshot.objectCount());
    assertFalse(snapshot.nominalShareBytes().isPresent());
    assertTrue(snapshot.entries().isEmpty());
    try {
      snapshot.entries().add(new LocalExecutionResourceRegistry.ResourceEntry("x", null));
    } catch (UnsupportedOperationException expected) {
      return;
    }
    throw new AssertionError("snapshot entries must be immutable");
  }

  @Test(expected = IllegalArgumentException.class)
  public void nonPositiveBudgetIsRejected() {
    new LocalExecutionResourceRegistry().snapshot(0L, 1L);
  }

  @Test(expected = IllegalStateException.class)
  public void freezeIsOneShot() {
    LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
    registry.freeze(10L, 10L);
    registry.freeze(10L, 10L);
  }

  @Test
  public void thirtyTwoChannelsHaveDiagnosticShareButLegacyEffectiveQuota() throws Exception {
    try (BQuotaCompatibilityContractTest.RealShuffle real =
        new BQuotaCompatibilityContractTest.RealShuffle(32)) {
      LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
      registry.registerSinkHandle(real.sink, real.id, "shared_sink");
      registry.recordLegacySinkQuota(real.sink, 4_022_509L);
      LocalExecutionResourceRegistry.Snapshot snapshot = registry.freeze(4_022_509L, 4_022_509L);
      assertEquals(32, snapshot.objectCount());
      assertEquals(1, snapshot.uniqueMemoryKeyCount());
      assertEquals(OptionalLong.of(125_703L), snapshot.nominalShareBytes());
      assertEquals(4_022_509L, snapshot.effectiveRequestBytes());
      org.apache.tsfile.read.common.block.TsBlock payload =
          BQuotaCompatibilityContractTest.block(10_240);
      assertTrue(payload.getSizeInBytes() > 125_703L);
      assertTrue(payload.getSizeInBytes() < 4_022_509L);
      System.out.println("B32 actual TsBlock bytes=" + payload.getSizeInBytes());
      assertTrue(real.sink.isFull().isDone());
      real.sink.send(payload);
      assertEquals(1, real.channels.get(0).getNumOfBufferedTsBlocks());
      assertTrue(
          real.pool.snapshotFragmentReservations("quota_contract", "0.0").fragmentReservedBytes()
              >= payload.getSizeInBytes());
      assertEquals(
          1, real.pool.snapshotFragmentReservations("quota_contract", "0.0").byPlanNode().size());
      real.sink.close();
      assertEquals(
          0L,
          real.pool.snapshotFragmentReservations("quota_contract", "0.0").fragmentReservedBytes());
      assertEquals(8_045_018L, real.pool.getRemainingBytes());
      for (org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkChannel channel :
          real.channels) assertTrue(channel.isClosed());
    }
  }

  @Test(expected = IllegalStateException.class)
  public void registrationAfterFreezeIsRejected() {
    LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
    registry.freeze(100L, 100L);
    registry.register(new Object(), "SOURCE_HANDLE", null);
  }

  @Test
  public void adoptingParentTransfersChildCloseResponsibility() throws Exception {
    LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
    org.apache.iotdb.calc.execution.operator.Operator child =
        Mockito.mock(org.apache.iotdb.calc.execution.operator.Operator.class);
    org.apache.iotdb.calc.execution.operator.Operator parent =
        Mockito.mock(org.apache.iotdb.calc.execution.operator.Operator.class);
    registry.registerOperatorRoot(child);
    registry.adoptOperatorRoot(parent, java.util.Collections.singletonList(child));
    registry.abortUntransferred(new Exception("planning failed"));
    registry.abortUntransferred(new Exception("duplicate cancellation"));
    Mockito.verify(parent, Mockito.times(1)).close();
    Mockito.verify(child, Mockito.never()).close();
  }

  @Test
  public void abortNotifiesBothEndpointsAndPreservesCleanupFailure() throws Exception {
    LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
    org.apache.iotdb.calc.execution.operator.Operator root =
        Mockito.mock(org.apache.iotdb.calc.execution.operator.Operator.class);
    org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle source =
        Mockito.mock(org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle.class);
    org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink sink =
        Mockito.mock(org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink.class);
    Exception primary = new Exception("planning failure"), cleanup = new Exception("close failure");
    Mockito.doThrow(cleanup).when(root).close();
    registry.registerOperatorRoot(root);
    registry.registerSourceHandle(source);
    registry.registerSink(sink);
    registry.abortUntransferred(primary);
    registry.abortUntransferred(primary);
    Mockito.verify(source).abort(primary);
    Mockito.verify(sink).abort();
    assertEquals(cleanup, primary.getSuppressed()[0]);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void mixedLocalAndRemoteChannelsCountBuffersAndRetainAllAuditedChannels()
      throws Exception {
    org.apache.iotdb.db.queryengine.execution.exchange.ResourceAuditManager audit =
        org.apache.iotdb.db.queryengine.execution.exchange.ResourceAuditManager.getInstance();
    String window =
        audit.openWindow(30_000L, BQuotaCompatibilityContractTest.LEGACY_CAP * 2L, 2048L);
    try (BQuotaCompatibilityContractTest.RealShuffle real =
        new BQuotaCompatibilityContractTest.RealShuffle(32, 16)) {
      LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
      registry.bindResourceAudit(real.id);
      org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle consumer =
          new org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle(
              real.localQueues.get(0),
              Mockito.mock(
                  org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager
                      .SourceHandleListener.class),
              "mixed-local-consumer");
      try {
        registry.registerSinkHandle(real.sink, real.id, "shared_sink");
        registry.registerSinkHandle(real.sink, real.id, "shared_sink");
        registry.registerSourceHandle(consumer);
        registry.recordLegacySinkQuota(real.sink, BQuotaCompatibilityContractTest.LEGACY_CAP);
        LocalExecutionResourceRegistry.Snapshot snapshot =
            registry.freeze(
                BQuotaCompatibilityContractTest.LEGACY_CAP,
                BQuotaCompatibilityContractTest.LEGACY_CAP);
        assertEquals(16, real.localQueues.size());
        assertEquals(32, real.channels.size());
        assertEquals(32, snapshot.objectCount());
        assertEquals(1, snapshot.uniqueMemoryKeyCount());
        assertEquals(OptionalLong.of(125_703L), snapshot.nominalShareBytes());
        assertEquals(
            16L,
            snapshot.entries().stream()
                .filter(entry -> "LOCAL_QUEUE".equals(entry.kind()))
                .count());
        assertEquals(
            16L,
            snapshot.entries().stream()
                .filter(entry -> "SINK_CHANNEL".equals(entry.kind()))
                .count());
        for (LocalExecutionResourceRegistry.ResourceEntry entry : snapshot.entries())
          assertEquals(real.localQueues.get(0).getMemoryReservationKey(), entry.memoryAccountKey());
        java.lang.reflect.Field remoteCap =
            org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel.class
                .getDeclaredField("maxBytesCanReserve");
        remoteCap.setAccessible(true);
        for (org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkChannel channel :
            real.channels) {
          long cap =
              channel
                      instanceof
                      org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel
                  ? ((org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel)
                          channel)
                      .getSharedTsBlockQueue()
                      .getMaxBytesCanReserve()
                  : remoteCap.getLong(channel);
          assertEquals(BQuotaCompatibilityContractTest.LEGACY_CAP, cap);
        }
        assertEquals(
            BQuotaCompatibilityContractTest.LEGACY_CAP,
            real.sink.freezeLegacyChannelLimits().constructorCapBytes());
        assertEquals(
            BQuotaCompatibilityContractTest.LEGACY_CAP,
            real.sink.freezeLegacyChannelLimits().publishedCapBytes());
        org.apache.tsfile.read.common.block.TsBlock payload =
            BQuotaCompatibilityContractTest.block(10_240);
        assertTrue(payload.getSizeInBytes() > 125_703L);
        assertTrue(payload.getSizeInBytes() < BQuotaCompatibilityContractTest.LEGACY_CAP);
        assertFalse(consumer.isBlocked().isDone());
        assertTrue(real.sink.isFull().isDone());
        real.sink.send(payload);
        assertEquals(
            payload.getSizeInBytes(), real.localQueues.get(0).getBufferRetainedSizeInBytes());
        assertSame(payload, consumer.receive());
        assertEquals(0L, real.localQueues.get(0).getBufferRetainedSizeInBytes());
        assertEquals(
            BQuotaCompatibilityContractTest.LEGACY_CAP * 2L, real.pool.getRemainingBytes());
        java.util.Map<String, Object> observed =
            audit.snapshot(
                window, real.id.getQueryId().getId(), real.id.getFullId(), real.pool, 2048L);
        java.util.List<java.util.Map<String, Object>> objects =
            (java.util.List<java.util.Map<String, Object>>) observed.get("objects");
        java.util.Set<Object> channelTokens = new java.util.HashSet<>();
        long queues = 0;
        for (java.util.Map<String, Object> object : objects) {
          if ("SINK_CHANNEL".equals(object.get("kind"))) channelTokens.add(object.get("token"));
          if ("LOCAL_QUEUE".equals(object.get("kind"))) queues++;
        }
        assertEquals(32, channelTokens.size());
        assertEquals(16L, queues);
      } finally {
        registry.abortUntransferred(new RuntimeException("mixed contract cleanup"));
        assertTrue(consumer.isAborted());
        assertTrue(real.sink.isAborted());
        for (org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkChannel channel :
            real.channels) assertTrue(channel.isClosed() || channel.isAborted());
        for (SharedTsBlockQueue queue : real.localQueues) assertTrue(queue.isClosed());
      }
      java.util.Map<String, Object> terminal =
          audit.snapshot(
              window, real.id.getQueryId().getId(), real.id.getFullId(), real.pool, 2048L);
      assertEquals(0, terminal.get("liveQueues"));
      assertEquals(0, terminal.get("liveEndpoints"));
      assertEquals(0, terminal.get("pendingFutures"));
    } finally {
      audit.closeWindow(window);
    }
  }

  @Test
  public void partialSinkRegistrationFailureAbortsParentAndContinuesAfterEndpointFailure()
      throws Exception {
    try (BQuotaCompatibilityContractTest.RealShuffle real =
        new BQuotaCompatibilityContractTest.RealShuffle(3, 3)) {
      org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle parent =
          Mockito.spy(real.sink);
      RuntimeException planning = new RuntimeException("second channel registration failed");
      RuntimeException cleanup = new RuntimeException("source termination listener failed");
      Mockito.doThrow(planning).when(parent).getChannel(1);
      org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SourceHandleListener
          listener =
              Mockito.mock(
                  org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager
                      .SourceHandleListener.class);
      Mockito.doThrow(cleanup).when(listener).onAborted(Mockito.any());
      org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle source =
          new org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle(
              real.localQueues.get(0), listener, "partial-registration-consumer");
      LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
      registry.registerSourceHandle(source);
      org.apache.tsfile.read.common.block.TsBlock payload =
          BQuotaCompatibilityContractTest.block(10_240);
      assertTrue(parent.isFull().isDone());
      parent.send(payload);
      assertEquals(
          payload.getSizeInBytes(), real.localQueues.get(0).getBufferRetainedSizeInBytes());
      assertTrue(real.pool.getRemainingBytes() < BQuotaCompatibilityContractTest.LEGACY_CAP * 2L);
      assertSame(
          planning,
          assertThrows(
              RuntimeException.class,
              () -> registry.registerSinkHandle(parent, real.id, "shared_sink")));
      registry.abortUntransferred(planning);
      registry.abortUntransferred(planning);
      assertTrue(parent.isAborted());
      assertFalse(parent.abort());
      assertTrue(source.isAborted());
      assertEquals(1, planning.getSuppressed().length);
      assertSame(cleanup, planning.getSuppressed()[0]);
      for (org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkChannel channel :
          real.channels) {
        assertTrue(channel.isAborted());
        assertFalse(channel.abort());
      }
      for (SharedTsBlockQueue queue : real.localQueues) {
        assertTrue(queue.isClosed());
        assertEquals(0L, queue.getBufferRetainedSizeInBytes());
      }
      assertEquals(BQuotaCompatibilityContractTest.LEGACY_CAP * 2L, real.pool.getRemainingBytes());
      assertEquals(
          0L,
          real.pool.snapshotFragmentReservations("quota_contract", "0.0").fragmentReservedBytes());
    }
  }

  @Test
  public void channelsTerminateBeforeParentAndRootsWithoutLosingAnyCleanupFailure()
      throws Exception {
    try (BQuotaCompatibilityContractTest.RealShuffle real =
        new BQuotaCompatibilityContractTest.RealShuffle(3, 3)) {
      java.util.List<String> events = new java.util.ArrayList<>();
      RuntimeException childFailure = new RuntimeException("real local sink listener failed");
      RuntimeException parentFailure = new RuntimeException("real parent listener failed");
      RuntimeException rootFailure = new RuntimeException("root close failed");
      for (int i = 0; i < real.channelListeners.size(); i++) {
        final int index = i;
        Mockito.doAnswer(
                invocation -> {
                  events.add("child" + index);
                  if (index == 0) throw childFailure;
                  return java.util.Optional.empty();
                })
            .when(real.channelListeners.get(i))
            .onAborted(Mockito.any());
      }
      Mockito.doAnswer(
              invocation -> {
                events.add("parent");
                throw parentFailure;
              })
          .when(real.sinkListener)
          .onAborted(Mockito.any());
      org.apache.iotdb.calc.execution.operator.Operator root =
          Mockito.mock(org.apache.iotdb.calc.execution.operator.Operator.class);
      Mockito.doAnswer(
              invocation -> {
                events.add("root");
                assertFalse(real.sink.close());
                throw rootFailure;
              })
          .when(root)
          .close();
      LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
      registry.registerOperatorRoot(root);
      registry.registerSinkHandle(real.sink, real.id, "shared_sink");
      org.apache.tsfile.read.common.block.TsBlock payload =
          BQuotaCompatibilityContractTest.block(10_240);
      assertTrue(real.sink.isFull().isDone());
      real.sink.send(payload);
      assertTrue(real.pool.getRemainingBytes() < BQuotaCompatibilityContractTest.LEGACY_CAP * 2L);
      RuntimeException primary = new RuntimeException("planning failed");
      registry.abortUntransferred(primary);
      registry.abortUntransferred(primary);
      assertTrue(real.sink.isAborted());
      assertEquals(3, primary.getSuppressed().length);
      java.util.Set<Throwable> failures =
          java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
      java.util.Collections.addAll(failures, primary.getSuppressed());
      assertTrue(failures.contains(childFailure));
      assertTrue(failures.contains(parentFailure));
      assertTrue(failures.contains(rootFailure));
      assertEquals(5, events.size());
      for (int i = 0; i < 3; i++)
        assertTrue(events.indexOf("child" + i) < events.indexOf("parent"));
      assertTrue(events.indexOf("parent") < events.indexOf("root"));
      for (org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkChannel channel :
          real.channels) {
        assertTrue(channel.isAborted());
        assertFalse(channel.abort());
      }
      for (SharedTsBlockQueue queue : real.localQueues) {
        assertTrue(queue.isClosed());
        assertEquals(0L, queue.getBufferRetainedSizeInBytes());
      }
      assertEquals(BQuotaCompatibilityContractTest.LEGACY_CAP * 2L, real.pool.getRemainingBytes());
      assertEquals(
          0L,
          real.pool.snapshotFragmentReservations("quota_contract", "0.0").fragmentReservedBytes());
    }
  }

  @Test
  public void partialRegistrationRetainsUnregisteredLocalChannelAndQueueDespiteListenerFailure()
      throws Exception {
    try (BQuotaCompatibilityContractTest.RealShuffle real =
        new BQuotaCompatibilityContractTest.RealShuffle(3, 3)) {
      java.util.List<org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkChannel>
          children = real.sink.getChannelsForLifecycle();
      assertThrows(UnsupportedOperationException.class, children::clear);
      org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkChannel last =
          real.channels.remove(2);
      try {
        assertEquals(3, children.size());
      } finally {
        real.channels.add(last);
      }
      org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle parent =
          Mockito.spy(real.sink);
      RuntimeException planning = new RuntimeException("second channel registration failed");
      RuntimeException cleanup = new RuntimeException("unregistered child sink listener failed");
      Mockito.doThrow(planning).when(parent).getChannel(1);
      Mockito.doThrow(cleanup).when(real.channelListeners.get(1)).onAborted(Mockito.any());
      org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle pendingSource =
          new org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle(
              real.localQueues.get(2),
              Mockito.mock(
                  org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager
                      .SourceHandleListener.class),
              "pending-unregistered-queue-consumer");
      LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
      registry.registerSourceHandle(pendingSource);
      com.google.common.util.concurrent.ListenableFuture<?> pending = pendingSource.isBlocked();
      assertFalse(pending.isDone());
      org.apache.tsfile.read.common.block.TsBlock payload =
          BQuotaCompatibilityContractTest.block(10_240);
      parent.sendToChannel(1, payload);
      assertEquals(
          payload.getSizeInBytes(), real.localQueues.get(1).getBufferRetainedSizeInBytes());
      assertTrue(real.pool.getRemainingBytes() < BQuotaCompatibilityContractTest.LEGACY_CAP * 2L);
      assertSame(
          planning,
          assertThrows(
              RuntimeException.class,
              () -> registry.registerSinkHandle(parent, real.id, "shared_sink")));
      registry.abortUntransferred(planning);
      registry.abortUntransferred(planning);
      assertTrue(parent.isAborted());
      assertFalse(parent.abort());
      assertTrue(pendingSource.isAborted());
      assertTrue(pending.isDone());
      assertEquals(1, planning.getSuppressed().length);
      assertSame(cleanup, planning.getSuppressed()[0]);
      for (org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkChannel channel :
          real.channels) {
        assertTrue(channel.isAborted());
        assertFalse(channel.abort());
      }
      for (SharedTsBlockQueue queue : real.localQueues) {
        assertTrue(queue.isClosed());
        assertEquals(0L, queue.getBufferRetainedSizeInBytes());
      }
      assertEquals(BQuotaCompatibilityContractTest.LEGACY_CAP * 2L, real.pool.getRemainingBytes());
      assertEquals(
          0L,
          real.pool.snapshotFragmentReservations("quota_contract", "0.0").fragmentReservedBytes());
    }
  }

  @Test
  public void localSourceSinkAndPipelineAliasesShareOneActualQueueAccount() throws Exception {
    try (BQuotaCompatibilityContractTest.RealShuffle real =
        new BQuotaCompatibilityContractTest.RealShuffle(1, 1)) {
      SharedTsBlockQueue queue = real.localQueues.get(0);
      org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle plain =
          new org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle(
              queue,
              Mockito.mock(
                  org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager
                      .SourceHandleListener.class),
              "nullable-plain-source");
      org.apache.iotdb.db.queryengine.execution.exchange.source.PipelineSourceHandle pipeline =
          new org.apache.iotdb.db.queryengine.execution.exchange.source.PipelineSourceHandle(
              queue,
              Mockito.mock(
                  org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager
                      .SourceHandleListener.class),
              "nullable-pipeline-source");
      LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
      try {
        registry.registerQueue(queue);
        registry.registerSinkHandle(real.sink, real.id, "wrong-owner-hint");
        registry.registerSink(real.channels.get(0));
        registry.registerSourceHandle(
            plain, new SharedTsBlockQueue.MemoryReservationKey("wrong", "9.9", "consumer"));
        registry.registerSourceHandle(pipeline);
        assertEquals(1, registry.getObjectCount());
        assertEquals("LOCAL_QUEUE", registry.snapshotEntries().get(0).kind());
        assertEquals(
            queue.getMemoryReservationKey(), registry.snapshotEntries().get(0).memoryAccountKey());
        registry.recordLegacySinkQuota(
            real.channels.get(0), BQuotaCompatibilityContractTest.LEGACY_CAP);
        registry.recordLegacySourceQuota(plain, 1L);
        registry.recordLegacySourceQuota(pipeline, 2_000_000L);
        LocalExecutionResourceRegistry.Snapshot frozen =
            registry.freeze(
                BQuotaCompatibilityContractTest.LEGACY_CAP,
                BQuotaCompatibilityContractTest.LEGACY_CAP);
        assertEquals(1, frozen.objectCount());
        assertEquals(1, frozen.uniqueMemoryKeyCount());
        assertEquals(
            OptionalLong.of(BQuotaCompatibilityContractTest.LEGACY_CAP),
            frozen.nominalShareBytes());
        assertEquals(2_000_000L, queue.getMaxBytesCanReserve());
        assertEquals(BQuotaCompatibilityContractTest.LEGACY_CAP, frozen.effectiveRequestBytes());
        for (java.util.Map<String, Object> publication : registry.getQuotaPublication()) {
          java.util.List<?> accounts = (java.util.List<?>) publication.get("memoryAccounts");
          assertEquals(1, accounts.size());
          assertEquals("shared_sink", ((java.util.Map<?, ?>) accounts.get(0)).get("planNodeId"));
          assertEquals("quota_contract", ((java.util.Map<?, ?>) accounts.get(0)).get("queryId"));
        }
      } finally {
        registry.abortUntransferred(new RuntimeException("alias cleanup"));
        assertTrue(plain.isAborted());
        assertTrue(pipeline.isAborted());
        assertTrue(real.channels.get(0).isAborted());
        assertTrue(queue.isClosed());
      }
    }
  }

  @Test
  public void distinctQueuesWithOneMemoryKeyRemainDistinctPhysicalResources() throws Exception {
    try (BQuotaCompatibilityContractTest.RealShuffle real =
        new BQuotaCompatibilityContractTest.RealShuffle(2, 2)) {
      assertNotSame(real.localQueues.get(0), real.localQueues.get(1));
      assertEquals(
          real.localQueues.get(0).getMemoryReservationKey(),
          real.localQueues.get(1).getMemoryReservationKey());
      LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
      try {
        registry.registerSinkHandle(real.sink, real.id, "shared_sink");
        LocalExecutionResourceRegistry.Snapshot snapshot =
            registry.snapshot(
                BQuotaCompatibilityContractTest.LEGACY_CAP,
                BQuotaCompatibilityContractTest.LEGACY_CAP);
        assertEquals(2, snapshot.objectCount());
        assertEquals(1, snapshot.uniqueMemoryKeyCount());
        assertEquals(OptionalLong.of(2_011_254L), snapshot.nominalShareBytes());
        assertTrue(
            snapshot.entries().stream().allMatch(entry -> "LOCAL_QUEUE".equals(entry.kind())));
      } finally {
        registry.abortUntransferred(new RuntimeException("distinct queue cleanup"));
      }
    }
  }

  @Test
  public void planningFailureAbortsEveryLocalWrapperAsWellAsItsQueue() throws Exception {
    assertLocalAliasAbort(new RuntimeException("planning failure"));
  }

  @Test
  public void cancellationAbortsEveryLocalWrapperAsWellAsItsQueue() throws Exception {
    assertLocalAliasAbort(new java.util.concurrent.CancellationException("planning cancellation"));
  }

  private void assertLocalAliasAbort(Throwable primary) throws Exception {
    try (BQuotaCompatibilityContractTest.RealShuffle real =
        new BQuotaCompatibilityContractTest.RealShuffle(1, 1)) {
      SharedTsBlockQueue queue = real.localQueues.get(0);
      org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle source =
          new org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle(
              queue,
              Mockito.mock(
                  org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager
                      .SourceHandleListener.class),
              "aborted-source");
      org.apache.iotdb.db.queryengine.execution.exchange.source.PipelineSourceHandle pipeline =
          new org.apache.iotdb.db.queryengine.execution.exchange.source.PipelineSourceHandle(
              queue,
              Mockito.mock(
                  org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager
                      .SourceHandleListener.class),
              "aborted-pipeline");
      LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
      registry.registerSink(real.channels.get(0));
      registry.registerSourceHandle(source);
      registry.registerSourceHandle(pipeline);
      com.google.common.util.concurrent.ListenableFuture<?> pending = source.isBlocked();
      assertFalse(pending.isDone());
      registry.abortUntransferred(primary);
      registry.abortUntransferred(primary);
      assertTrue(source.isAborted());
      assertTrue(pipeline.isAborted());
      assertTrue(real.channels.get(0).isAborted());
      assertTrue(queue.isClosed());
      assertTrue(pending.isDone());
      assertEquals(BQuotaCompatibilityContractTest.LEGACY_CAP * 2L, real.pool.getRemainingBytes());
      assertThrows(IllegalStateException.class, () -> registry.registerSink(real.channels.get(0)));
      assertThrows(IllegalStateException.class, () -> registry.registerSourceHandle(source));
      assertThrows(IllegalStateException.class, () -> registry.freeze(1L, 1L));
    }
  }

  @Test
  public void freezeRejectsEveryLocalAliasRegistrationEntrypointWithoutMutation() throws Exception {
    try (BQuotaCompatibilityContractTest.RealShuffle real =
        new BQuotaCompatibilityContractTest.RealShuffle(1, 1)) {
      SharedTsBlockQueue queue = real.localQueues.get(0);
      org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle source =
          new org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle(
              queue,
              Mockito.mock(
                  org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager
                      .SourceHandleListener.class),
              "frozen-source");
      LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
      try {
        registry.registerSinkHandle(real.sink, real.id, "shared_sink");
        registry.registerSourceHandle(source);
        LocalExecutionResourceRegistry.Snapshot frozen = registry.freeze(4_022_509L, 4_022_509L);
        assertEquals(1, frozen.objectCount());
        assertThrows(IllegalStateException.class, () -> registry.registerQueue(queue));
        assertThrows(
            IllegalStateException.class,
            () -> registry.registerSinkHandle(real.sink, real.id, "shared_sink"));
        assertThrows(
            IllegalStateException.class, () -> registry.registerSink(real.channels.get(0)));
        assertThrows(IllegalStateException.class, () -> registry.registerSourceHandle(source));
        assertThrows(
            IllegalStateException.class,
            () -> registry.recordLegacySinkQuota(real.channels.get(0), 1L));
        assertEquals(1, registry.getObjectCount());
        assertSame(frozen, registry.getFrozenSnapshot());
      } finally {
        registry.abortUntransferred(new RuntimeException("frozen cleanup"));
      }
    }
  }

  @Test
  public void boundRuntimeRegistryUsesCanonicalPoolFragmentIdentity() {
    org.apache.iotdb.db.queryengine.common.FragmentInstanceId owner =
        new org.apache.iotdb.db.queryengine.common.FragmentInstanceId(
            new org.apache.iotdb.db.queryengine.common.PlanFragmentId(
                new org.apache.iotdb.db.queryengine.common.QueryId("canonical_runtime"), 0),
            "instance");
    org.apache.iotdb.db.queryengine.execution.memory.MemoryPool pool =
        new org.apache.iotdb.db.queryengine.execution.memory.MemoryPool(
            "canonical-runtime", new org.apache.iotdb.commons.memory.MemoryManager(1024), 512);
    SharedTsBlockQueue.MemoryReservationKey key =
        new SharedTsBlockQueue.MemoryReservationKey(
            owner.getQueryId().getId(), owner.getFragmentInstanceId(), "group");
    org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation reservation =
        org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation.tryCreate(
                pool, key, 1, 32, 512)
            .get();
    try {
      LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
      registry.bindResourceAudit(owner);
      registry.registerRuntimeFrontier("canonical-scan", reservation);
      assertEquals(1, registry.snapshotRuntime(pool).groups().size());
    } finally {
      reservation.close();
      pool.deRegisterFragmentInstanceFromQueryMemoryMap(
          owner.getQueryId().getId(), owner.getFragmentInstanceId(), true);
    }
  }

  @Test
  public void runtimeDualKeysAndGroupAccountDoNotChangeTheFrozenV2Snapshot() {
    org.apache.iotdb.db.queryengine.execution.memory.MemoryPool pool =
        new org.apache.iotdb.db.queryengine.execution.memory.MemoryPool(
            "registry-runtime", new org.apache.iotdb.commons.memory.MemoryManager(1024), 512);
    SharedTsBlockQueue.MemoryReservationKey account =
        new SharedTsBlockQueue.MemoryReservationKey("q", "q.0.0", "group");
    org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation reservation =
        org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation.tryCreate(
                pool, account, 2, 32, 512)
            .get();
    LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
    java.util.concurrent.atomic.AtomicBoolean sourceClosed =
        new java.util.concurrent.atomic.AtomicBoolean();
    try {
      registry.registerRuntimeFrontier("scan", reservation);
      SharedTsBlockQueue first = runtimeQueue("lane-a", account, 0);
      SharedTsBlockQueue second = runtimeQueue("lane-b", account, 1);
      org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel sink =
          Mockito.mock(
              org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel.class);
      Mockito.when(sink.getSharedTsBlockQueue()).thenReturn(first);
      org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle source =
          Mockito.mock(
              org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle.class);
      Mockito.when(source.getSharedTsBlockQueue()).thenReturn(first);
      Mockito.when(source.isClosed()).thenAnswer(call -> sourceClosed.get());
      registry.registerQueue(first);
      registry.registerQueue(second);
      registry.registerSink(sink);
      registry.registerSourceHandle(source);
      LocalExecutionResourceRegistry.Snapshot before = registry.snapshot(512, 128);
      LocalExecutionResourceRegistry.Snapshot frozen = registry.freeze(512, 128);
      assertEquals(before, frozen);
      assertEquals(2, frozen.objectCount());
      assertEquals(2, frozen.uniqueMemoryKeyCount());
      assertEquals(OptionalLong.of(256), frozen.nominalShareBytes());
      assertEquals(128, frozen.effectiveRequestBytes());

      org.apache.iotdb.db.queryengine.execution.runtime.RuntimeResourceSnapshot runtime =
          registry.snapshotRuntime(pool);
      assertEquals(3, runtime.schemaVersion());
      assertEquals(4, runtime.resources().size());
      assertEquals(1, runtime.accounts().size());
      assertEquals(1, runtime.groups().size());
      assertEquals(Long.valueOf(64), runtime.actualReservedBytes());
      assertTrue(
          runtime.resources().stream()
              .allMatch(resource -> account.equals(resource.actualAccountKey())));
      sourceClosed.set(true);
      assertTrue(
          registry.snapshotRuntime(pool).resources().stream()
              .anyMatch(
                  resource -> resource.kind().equals("SOURCE_HANDLE") && resource.terminal()));
      assertFalse(
          registry.snapshotRuntime(pool).resources().stream()
              .filter(resource -> resource.kind().equals("SINK"))
              .findFirst()
              .get()
              .terminal());
      assertSame(frozen, registry.getFrozenSnapshot());
      assertEquals(before, registry.snapshot(512, 128));
      org.apache.iotdb.db.queryengine.execution.memory.MemoryPool other =
          new org.apache.iotdb.db.queryengine.execution.memory.MemoryPool(
              "registry-runtime", new org.apache.iotdb.commons.memory.MemoryManager(1024), 512);
      assertThrows(IllegalArgumentException.class, () -> registry.snapshotRuntime(other));
    } finally {
      reservation.close();
    }
  }

  @Test
  public void conflictingRuntimeMappingCannotReplaceTheFirstV2Entry() {
    LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
    Object identity = new Object();
    SharedTsBlockQueue.MemoryReservationKey first =
        new SharedTsBlockQueue.MemoryReservationKey("q", "q.0.0", "first");
    SharedTsBlockQueue.MemoryReservationKey other =
        new SharedTsBlockQueue.MemoryReservationKey("q", "q.0.0", "other");
    registry.register(identity, "REMOTE", first);
    assertThrows(
        IllegalArgumentException.class, () -> registry.register(identity, "REMOTE", other));
    assertEquals(1, registry.getObjectCount());
    assertEquals(first, registry.snapshotEntries().get(0).memoryAccountKey());
  }

  @Test
  public void runtimeClosureWaitsForBothRemoteSourceAndSinkReceipts() throws Exception {
    for (boolean abort : new boolean[] {false, true}) {
      RemoteClosureFixture f = new RemoteClosureFixture();
      Throwable cause = new RuntimeException("FI stop");
      com.google.common.util.concurrent.ListenableFuture<Void> closed =
          f.registry.closeRuntimeOwners(abort, cause);
      assertFalse(closed.isDone());
      java.util.concurrent.atomic.AtomicBoolean monitorHeld =
          new java.util.concurrent.atomic.AtomicBoolean();
      java.util.concurrent.CountDownLatch observed = new java.util.concurrent.CountDownLatch(1);
      closed.addListener(
          () -> {
            monitorHeld.set(
                Thread.holdsLock(f.registry)
                    || Thread.holdsLock(f.source)
                    || Thread.holdsLock(f.sink));
            observed.countDown();
          },
          com.google.common.util.concurrent.MoreExecutors.directExecutor());
      if (abort) {
        Mockito.verify(f.source).requestCoordinatedAbort(cause);
        Mockito.verify(f.sink).requestCoordinatedAbort(cause);
      } else {
        Mockito.verify(f.source).requestCoordinatedClose();
        Mockito.verify(f.sink).requestCoordinatedClose();
      }
      f.sourceReceipt.set(null);
      assertFalse(closed.isDone());
      f.sinkReceipt.set(null);
      closed.get(5, java.util.concurrent.TimeUnit.SECONDS);
      assertTrue(observed.await(5, java.util.concurrent.TimeUnit.SECONDS));
      assertFalse(monitorHeld.get());
    }
  }

  @Test
  public void oneEndpointRequestFailureStillStopsOtherOwnersAndWaitsActualReceipt()
      throws Exception {
    RemoteClosureFixture f = new RemoteClosureFixture();
    RuntimeException expected = new RuntimeException("source request failed");
    Mockito.when(f.source.requestCoordinatedClose()).thenThrow(expected);
    org.apache.iotdb.calc.execution.operator.Operator root =
        Mockito.mock(org.apache.iotdb.calc.execution.operator.Operator.class);
    f.registry.registerOperatorRoot(root);
    com.google.common.util.concurrent.ListenableFuture<Void> closed =
        f.registry.closeRuntimeOwners(false, null);
    Mockito.verify(f.source).requestCoordinatedClose();
    Mockito.verify(f.sink).requestCoordinatedClose();
    Mockito.verify(root).close();
    assertFalse(closed.isDone());
    f.sinkReceipt.set(null);
    java.util.concurrent.ExecutionException failure =
        assertThrows(
            java.util.concurrent.ExecutionException.class,
            () -> closed.get(5, java.util.concurrent.TimeUnit.SECONDS));
    assertSame(expected, failure.getCause());
  }

  @Test
  public void runtimeClosureObserverCancellationDoesNotCancelEndpointSettlement() throws Exception {
    RemoteClosureFixture f = new RemoteClosureFixture();
    com.google.common.util.concurrent.ListenableFuture<Void> first =
        f.registry.closeRuntimeOwners(false, null);
    assertTrue(first.cancel(true));
    assertFalse(f.sourceReceipt.isCancelled());
    assertFalse(f.sinkReceipt.isCancelled());
    com.google.common.util.concurrent.ListenableFuture<Void> second =
        f.registry.closeRuntimeOwners(true, new RuntimeException("later cancellation"));
    assertFalse(second.isDone());
    assertFalse(second.isCancelled());
    f.sourceReceipt.set(null);
    assertFalse(second.isDone());
    f.sinkReceipt.set(null);
    second.get(5, java.util.concurrent.TimeUnit.SECONDS);
    Mockito.verify(f.source, Mockito.times(1)).requestCoordinatedClose();
    Mockito.verify(f.sink, Mockito.times(1)).requestCoordinatedClose();
    Mockito.verify(f.source, Mockito.never()).requestCoordinatedAbort(Mockito.<Throwable>any());
    Mockito.verify(f.sink, Mockito.never()).requestCoordinatedAbort(Mockito.<Throwable>any());
  }

  @Test
  public void repeatedRuntimeClosureRetainsSameFailureReceiptWithoutReplayingOwners()
      throws Exception {
    RemoteClosureFixture f = new RemoteClosureFixture();
    RuntimeException expected = new RuntimeException("asynchronous source failure");
    org.apache.iotdb.calc.execution.operator.Operator root =
        Mockito.mock(org.apache.iotdb.calc.execution.operator.Operator.class);
    f.registry.registerOperatorRoot(root);
    f.sourceReceipt.setException(expected);
    com.google.common.util.concurrent.ListenableFuture<Void> first =
        f.registry.closeRuntimeOwners(false, null);
    assertFalse(first.isDone());
    f.sinkReceipt.set(null);
    for (int attempt = 0; attempt < 2; attempt++) {
      com.google.common.util.concurrent.ListenableFuture<Void> observer =
          attempt == 0
              ? first
              : f.registry.closeRuntimeOwners(true, new RuntimeException("repeat"));
      java.util.concurrent.ExecutionException failure =
          assertThrows(
              java.util.concurrent.ExecutionException.class,
              () -> observer.get(5, java.util.concurrent.TimeUnit.SECONDS));
      assertSame(expected, failure.getCause());
    }
    Mockito.verify(f.source, Mockito.times(1)).requestCoordinatedClose();
    Mockito.verify(f.sink, Mockito.times(1)).requestCoordinatedClose();
    Mockito.verify(root, Mockito.times(1)).close();
    Mockito.verify(f.source, Mockito.never()).requestCoordinatedAbort(Mockito.<Throwable>any());
    Mockito.verify(f.sink, Mockito.never()).requestCoordinatedAbort(Mockito.<Throwable>any());
  }

  @Test
  public void rootCloseFailureStillWaitsForRemoteEndpointsToReallyStop() throws Exception {
    RemoteClosureFixture f = new RemoteClosureFixture();
    RuntimeException expected = new RuntimeException("root close failed");
    org.apache.iotdb.calc.execution.operator.Operator root =
        Mockito.mock(org.apache.iotdb.calc.execution.operator.Operator.class);
    Mockito.doAnswer(
            call -> {
              assertFalse(Thread.holdsLock(f.registry));
              throw expected;
            })
        .when(root)
        .close();
    f.registry.registerOperatorRoot(root);
    com.google.common.util.concurrent.ListenableFuture<Void> closed =
        f.registry.closeRuntimeOwners(false, null);
    Mockito.verify(f.source).requestCoordinatedClose();
    Mockito.verify(f.sink).requestCoordinatedClose();
    assertFalse(closed.isDone());
    f.sourceReceipt.set(null);
    assertFalse(closed.isDone());
    f.sinkReceipt.set(null);
    java.util.concurrent.ExecutionException failure =
        assertThrows(
            java.util.concurrent.ExecutionException.class,
            () -> closed.get(5, java.util.concurrent.TimeUnit.SECONDS));
    assertSame(expected, failure.getCause());
    Mockito.verify(root, Mockito.times(1)).close();
  }

  @Test
  public void priorLegacyAbortCannotTurnClearedOwnerSetIntoSuccessfulRuntimeReceipt()
      throws Exception {
    RemoteClosureFixture f = new RemoteClosureFixture();
    RuntimeException original = new RuntimeException("legacy planning abort");
    f.registry.abortUntransferred(original);
    Mockito.verify(f.source).abort(original);
    Mockito.verify(f.sink).abort();
    com.google.common.util.concurrent.ListenableFuture<Void> first =
        f.registry.closeRuntimeOwners(true, original);
    java.util.concurrent.ExecutionException failure =
        assertThrows(
            java.util.concurrent.ExecutionException.class,
            () -> first.get(5, java.util.concurrent.TimeUnit.SECONDS));
    java.util.concurrent.ExecutionException repeated =
        assertThrows(
            java.util.concurrent.ExecutionException.class,
            () ->
                f.registry
                    .closeRuntimeOwners(false, null)
                    .get(5, java.util.concurrent.TimeUnit.SECONDS));
    assertSame(failure.getCause(), repeated.getCause());
    Mockito.verify(f.source, Mockito.never()).requestCoordinatedClose();
    Mockito.verify(f.sink, Mockito.never()).requestCoordinatedClose();
    Mockito.verify(f.source, Mockito.never()).requestCoordinatedAbort(Mockito.<Throwable>any());
    Mockito.verify(f.sink, Mockito.never()).requestCoordinatedAbort(Mockito.<Throwable>any());
    Mockito.verify(f.source, Mockito.times(1)).abort(original);
    Mockito.verify(f.sink, Mockito.times(1)).abort();
  }

  private static final class RemoteClosureFixture {
    final LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
    final org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle source =
        Mockito.mock(org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle.class);
    final org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel sink =
        Mockito.mock(org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel.class);
    final com.google.common.util.concurrent.SettableFuture<Void> sourceReceipt =
        com.google.common.util.concurrent.SettableFuture.create();
    final com.google.common.util.concurrent.SettableFuture<Void> sinkReceipt =
        com.google.common.util.concurrent.SettableFuture.create();

    RemoteClosureFixture() {
      Mockito.when(source.requestCoordinatedClose())
          .thenAnswer(
              call -> {
                assertFalse(Thread.holdsLock(registry));
                return sourceReceipt;
              });
      Mockito.when(sink.requestCoordinatedClose())
          .thenAnswer(
              call -> {
                assertFalse(Thread.holdsLock(registry));
                return sinkReceipt;
              });
      Mockito.when(source.requestCoordinatedAbort(Mockito.<Throwable>any()))
          .thenAnswer(
              call -> {
                assertFalse(Thread.holdsLock(registry));
                return sourceReceipt;
              });
      Mockito.when(sink.requestCoordinatedAbort(Mockito.<Throwable>any()))
          .thenAnswer(
              call -> {
                assertFalse(Thread.holdsLock(registry));
                return sinkReceipt;
              });
      registry.register(
          source,
          "SOURCE_HANDLE",
          new SharedTsBlockQueue.MemoryReservationKey("runtime_close", "0.0", "source"));
      registry.register(
          sink,
          "SINK",
          new SharedTsBlockQueue.MemoryReservationKey("runtime_close", "0.0", "sink"));
    }
  }

  private static SharedTsBlockQueue runtimeQueue(
      String node, SharedTsBlockQueue.MemoryReservationKey account, int lane) {
    SharedTsBlockQueue queue = Mockito.mock(SharedTsBlockQueue.class);
    SharedTsBlockQueue.MemoryReservationKey identity =
        new SharedTsBlockQueue.MemoryReservationKey("q", "q.0.0", node);
    Mockito.when(queue.getMemoryReservationKey()).thenReturn(identity);
    Mockito.when(queue.getQueueIdentityKey()).thenReturn(identity);
    Mockito.when(queue.getActualAccountKey()).thenReturn(account);
    Mockito.when(queue.isFrontierMode()).thenReturn(true);
    Mockito.when(queue.getFrontierLaneIndex()).thenReturn(lane);
    return queue;
  }

  @Test
  public void cancellationWithoutFailureCauseSettlesRealQueuesAndPendingConsumers()
      throws Exception {
    try (BQuotaCompatibilityContractTest.RealShuffle real =
        new BQuotaCompatibilityContractTest.RealShuffle(2, 2)) {
      LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
      java.util.List<org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle>
          sources = new java.util.ArrayList<>();
      for (SharedTsBlockQueue queue : real.localQueues) {
        org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle source =
            new org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle(
                queue,
                Mockito.mock(
                    org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager
                        .SourceHandleListener.class),
                "cancel-without-failure");
        sources.add(source);
        registry.registerSourceHandle(source);
        registry.registerQueue(queue);
      }
      com.google.common.util.concurrent.ListenableFuture<?> waiting = sources.get(1).isBlocked();
      assertFalse(waiting.isDone());
      real.sink.sendToChannel(0, BQuotaCompatibilityContractTest.block(1024));
      assertTrue(real.localQueues.get(0).getBufferRetainedSizeInBytes() > 0);
      assertTrue(real.pool.getRemainingBytes() < BQuotaCompatibilityContractTest.LEGACY_CAP * 2L);

      registry.closeRuntimeOwners(true, null).get(5, java.util.concurrent.TimeUnit.SECONDS);
      registry.closeRuntimeOwners(true, null).get(5, java.util.concurrent.TimeUnit.SECONDS);
      for (org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle source :
          sources) assertTrue(source.isAborted());
      for (SharedTsBlockQueue queue : real.localQueues) {
        assertTrue(queue.isClosed());
        assertEquals(0L, queue.getBufferRetainedSizeInBytes());
      }
      assertTrue(waiting.isDone());
      assertEquals(BQuotaCompatibilityContractTest.LEGACY_CAP * 2L, real.pool.getRemainingBytes());
      assertEquals(
          0L,
          real.pool.snapshotFragmentReservations("quota_contract", "0.0").fragmentReservedBytes());
    }
  }

  @Test
  public void missingCancellationCauseIsSharedAndDoesNotReplaceARealFailure() throws Exception {
    for (Throwable original : new Throwable[] {null, new IllegalStateException("actual failure")}) {
      LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
      org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle first =
          Mockito.mock(
              org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle.class);
      org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle second =
          Mockito.mock(
              org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle.class);
      registry.registerSourceHandle(first);
      registry.registerSourceHandle(second);
      registry.closeRuntimeOwners(true, original).get(5, java.util.concurrent.TimeUnit.SECONDS);
      org.mockito.ArgumentCaptor<Throwable> firstCause =
          org.mockito.ArgumentCaptor.forClass(Throwable.class);
      org.mockito.ArgumentCaptor<Throwable> secondCause =
          org.mockito.ArgumentCaptor.forClass(Throwable.class);
      Mockito.verify(first).abort(firstCause.capture());
      Mockito.verify(second).abort(secondCause.capture());
      assertSame(firstCause.getValue(), secondCause.getValue());
      if (original == null)
        assertTrue(firstCause.getValue() instanceof java.util.concurrent.CancellationException);
      else assertSame(original, firstCause.getValue());
    }
  }
}
