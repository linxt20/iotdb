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
}
