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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class LegacyQuotaPublicationTest {
  @Test
  public void settersPublishOnlyAtFreezeAndThenRejectNewWrites() {
    LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
    ISourceHandle source = Mockito.mock(ISourceHandle.class);
    registry.recordLegacySourceQuota(source, 128L);
    Mockito.verify(source, Mockito.never()).setMaxBytesCanReserve(Mockito.anyLong());
    assertThrows(IllegalArgumentException.class, () -> registry.freeze(0L, 128L));
    Mockito.verify(source, Mockito.never()).setMaxBytesCanReserve(Mockito.anyLong());
    registry.freeze(1024L, 128L);
    Mockito.verify(source, Mockito.times(1)).setMaxBytesCanReserve(128L);
    assertThrows(IllegalStateException.class, () -> registry.recordLegacySourceQuota(source, 64L));
    assertThrows(IllegalStateException.class, () -> registry.freeze(1024L, 128L));
  }

  @Test
  public void roundRobinKeepsConstructorThresholdAfterPublishedCapChanges() {
    long initial = IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance();
    List<ISinkChannel> channels = new ArrayList<>();
    for (int i = 0; i < 4; i++) channels.add(Mockito.mock(ISinkChannel.class));
    ShuffleSinkHandle sink =
        new ShuffleSinkHandle(
            new TFragmentInstanceId("quota", 0, "0"),
            channels,
            new DownStreamChannelIndex(0),
            ShuffleSinkHandle.ShuffleStrategyEnum.SIMPLE_ROUND_ROBIN,
            Mockito.mock(MPPDataExchangeManager.SinkListener.class));
    LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
    registry.registerSinkHandle(
        sink, new FragmentInstanceId(new PlanFragmentId(new QueryId("quota"), 0), "0"), "sink");
    registry.recordLegacySinkQuota(sink, initial / 2L);
    for (ISinkChannel channel : channels)
      Mockito.verify(channel, Mockito.never()).setMaxBytesCanReserve(Mockito.anyLong());
    registry.freeze(initial, initial / 2L);
    for (ISinkChannel channel : channels)
      Mockito.verify(channel).setMaxBytesCanReserve(initial / 2L);
    ShuffleSinkHandle.LegacyChannelLimits limits = sink.freezeLegacyChannelLimits();
    assertEquals(initial, limits.constructorCapBytes());
    assertEquals(initial / 2L, limits.publishedCapBytes());
    assertEquals(Long.valueOf(initial / 4L * 3L), limits.roundRobinThresholdBytes());
    assertEquals(4, limits.channelCount());
    assertFalse(limits.explicitRouting());
    sink.setMaxBytesCanReserve(initial / 4L);
    assertSame(limits, sink.freezeLegacyChannelLimits());
  }

  @Test
  public void plainLocalSourceRetainsNoopSetterAndUsesProducerQueueAccount() {
    TFragmentInstanceId producer = new TFragmentInstanceId("quota", 0, "0");
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            producer,
            "producer-scan",
            Mockito.mock(LocalMemoryManager.class),
            MoreExecutors.newDirectExecutorService());
    LocalSourceHandle source =
        new LocalSourceHandle(
            new TFragmentInstanceId("quota", 1, "0"),
            "consumer-exchange",
            queue,
            Mockito.mock(MPPDataExchangeManager.SourceHandleListener.class));
    LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
    registry.registerSourceHandle(
        source,
        new SharedTsBlockQueue.MemoryReservationKey("quota", "quota.1.0", "consumer-exchange"));
    assertEquals(1, registry.getObjectCount());
    for (LocalExecutionResourceRegistry.ResourceEntry entry : registry.snapshotEntries())
      assertEquals(queue.getMemoryReservationKey(), entry.memoryAccountKey());
    long before = queue.getMaxBytesCanReserve();
    registry.recordLegacySourceQuota(source, 1L);
    registry.freeze(before, 1L);
    assertEquals(before, queue.getMaxBytesCanReserve());
    assertEquals("PLAIN_LOCAL_SOURCE_NOOP", registry.getQuotaPublication().get(0).get("kind"));
    java.util.List<?> accounts =
        (java.util.List<?>) registry.getQuotaPublication().get(0).get("memoryAccounts");
    assertEquals(1, accounts.size());
    assertEquals("producer-scan", ((java.util.Map<?, ?>) accounts.get(0)).get("planNodeId"));
  }
}
