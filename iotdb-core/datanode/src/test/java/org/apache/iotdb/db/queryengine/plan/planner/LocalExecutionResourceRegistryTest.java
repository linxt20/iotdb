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
}
