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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.MoreExecutors;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.*;

/** Executable compatibility examples from the approved B4 resource contract. */
public class BQuotaCompatibilityContractTest {
  static final long LEGACY_CAP = 4_022_509L;

  static TsBlock block(int rows) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT64));
    for (int i = 0; i < rows; i++) {
      builder.getTimeColumnBuilder().writeLong(i);
      builder.getColumnBuilder(0).writeLong(i * 17L + 3L);
      builder.declarePosition();
    }
    return builder.build();
  }

  static final class RealShuffle implements AutoCloseable {
    final long originalCap =
        IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance();
    final ExecutorService executor = MoreExecutors.newDirectExecutorService();
    final MemoryPool pool =
        new MemoryPool("b-contract", new MemoryManager(LEGACY_CAP * 2L), LEGACY_CAP);
    final LocalMemoryManager memory = Mockito.mock(LocalMemoryManager.class);
    final FragmentInstanceId id =
        new FragmentInstanceId(new PlanFragmentId(new QueryId("quota_contract"), 0), "0");
    final List<ISinkChannel> channels = new ArrayList<>();
    final List<SharedTsBlockQueue> localQueues = new ArrayList<>();
    final List<MPPDataExchangeManager.SinkListener> channelListeners = new ArrayList<>();
    final MPPDataExchangeManager.SinkListener sinkListener =
        Mockito.mock(MPPDataExchangeManager.SinkListener.class);
    final ShuffleSinkHandle sink;

    RealShuffle(int count) throws Exception {
      this(count, 0);
    }

    @SuppressWarnings("unchecked")
    RealShuffle(int count, int localCount) throws Exception {
      if (localCount < 0 || localCount > count)
        throw new IllegalArgumentException("local channel count");
      IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(LEGACY_CAP);
      Mockito.when(memory.getQueryPool()).thenReturn(pool);
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clients =
          Mockito.mock(IClientManager.class);
      SyncDataNodeMPPDataExchangeServiceClient client =
          Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class);
      Mockito.when(clients.borrowClient(Mockito.any(TEndPoint.class))).thenReturn(client);
      for (int i = 0; i < count; i++) {
        MPPDataExchangeManager.SinkListener channelListener =
            Mockito.mock(MPPDataExchangeManager.SinkListener.class);
        channelListeners.add(channelListener);
        if (i < localCount) {
          SharedTsBlockQueue queue =
              new SharedTsBlockQueue(id.toThrift(), "shared_sink", memory, executor);
          queue.allowAddingTsBlock();
          localQueues.add(queue);
          channels.add(new LocalSinkChannel(id.toThrift(), queue, channelListener));
          continue;
        }
        channels.add(
            new SinkChannel(
                new TEndPoint("127.0.0.1", 65000),
                new TFragmentInstanceId("quota_contract", i + 1, "0"),
                "exchange_" + i,
                "shared_sink",
                id.toThrift(),
                memory,
                executor,
                new TsBlockSerde(),
                channelListener,
                clients));
      }
      sink =
          new ShuffleSinkHandle(
              id.toThrift(),
              channels,
              new DownStreamChannelIndex(0),
              ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
              sinkListener);
    }

    @Override
    public void close() {
      try {
        for (ISinkChannel channel : channels) channel.abort();
      } finally {
        executor.shutdownNow();
        IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(originalCap);
      }
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void remoteEndpointsUseTheirActualAccountsInsteadOfCallerHints() throws Exception {
    try (RealShuffle real = new RealShuffle(1)) {
      org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle source =
          new org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle(
              new TEndPoint("127.0.0.1", 65000),
              new TFragmentInstanceId("quota_contract", 7, "0"),
              real.id.toThrift(),
              "consumer_exchange",
              0,
              real.memory,
              real.executor,
              new TsBlockSerde(),
              Mockito.mock(MPPDataExchangeManager.SourceHandleListener.class),
              Mockito.mock(IClientManager.class));
      try {
        LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
        registry.registerSourceHandle(
            source,
            new SharedTsBlockQueue.MemoryReservationKey("wrong_query", "9.9", "upstream_sink"));
        registry.registerSink(real.channels.get(0));
        assertEquals(2, registry.getObjectCount());
        assertTrue(
            registry.snapshotEntries().stream()
                .anyMatch(
                    entry ->
                        entry
                            .memoryAccountKey()
                            .equals(
                                new SharedTsBlockQueue.MemoryReservationKey(
                                    "quota_contract", "0.0", "consumer_exchange"))));
        assertTrue(
            registry.snapshotEntries().stream()
                .anyMatch(
                    entry ->
                        entry
                            .memoryAccountKey()
                            .equals(
                                new SharedTsBlockQueue.MemoryReservationKey(
                                    "quota_contract", "0.0", "shared_sink"))));
        LocalExecutionResourceRegistry handleRegistry = new LocalExecutionResourceRegistry();
        FragmentInstanceId wrongOwner =
            new FragmentInstanceId(new PlanFragmentId(new QueryId("wrong_query"), 9), "9");
        handleRegistry.registerSinkHandle(real.sink, wrongOwner, "upstream_hint");
        assertEquals(
            new SharedTsBlockQueue.MemoryReservationKey("quota_contract", "0.0", "shared_sink"),
            handleRegistry.snapshotEntries().get(0).memoryAccountKey());
      } finally {
        source.abort();
      }
    }
  }

  @Test
  public void fourScanPipelinesAndPlainSinkKeepFiveLegacyExchanges() throws Exception {
    try (RealShuffle real = new RealShuffle(1)) {
      FragmentInstanceContext fragment =
          FragmentInstanceContext.createFragmentInstanceContext(
              real.id, new FragmentInstanceStateMachine(real.id, real.executor));
      LocalExecutionPlanContext context =
          new LocalExecutionPlanContext(new TypeProvider(), fragment, new DataNodeQueryContext(1));
      context.setISink(real.sink);
      context.addExchangeSumNum(1);
      try {
        List<org.apache.iotdb.calc.execution.operator.Operator> exchanges =
            LocalScanPipelineBuilder.build(
                context,
                4,
                (index, sub) -> {
                  PlanNode node = Mockito.mock(PlanNode.class);
                  Mockito.when(node.getPlanNodeId()).thenReturn(new PlanNodeId("scan_" + index));
                  Mockito.when(node.getType()).thenThrow(new UnsupportedOperationException());
                  org.apache.iotdb.calc.execution.operator.Operator operator =
                      Mockito.mock(org.apache.iotdb.calc.execution.operator.Operator.class);
                  Mockito.when(operator.calculateMaxReturnSize()).thenReturn(1024L);
                  return new LocalScanPipelineBuilder.PipelineSpec(
                      operator, node, new PlanNodeId("collect"), node.getPlanNodeId());
                });
        assertEquals(4, exchanges.size());
        assertEquals(4, context.getPipelineDriverFactories().size());
        assertEquals(5, context.getExchangeSumNum());
        assertEquals(804_501L, context.getMaxBytesOneHandleCanReserve());
        for (PipelineDriverFactory factory : context.getPipelineDriverFactories())
          context.recordLegacySinkQuota(factory.getDriverContext().getSink(), LEGACY_CAP);
        context.recordLegacySinkQuota(real.sink, context.getMaxBytesOneHandleCanReserve());
        context.setMaxBytesOneHandleCanReserve();
        LocalExecutionResourceRegistry.Snapshot frozen = context.freezeResourceQuota();
        assertEquals(5, frozen.objectCount());
        assertEquals(
            4L,
            frozen.entries().stream().filter(entry -> "LOCAL_QUEUE".equals(entry.kind())).count());
        assertEquals(
            1L,
            frozen.entries().stream().filter(entry -> "SINK_CHANNEL".equals(entry.kind())).count());
        assertEquals(5, frozen.uniqueMemoryKeyCount());
        assertEquals(804_501L, frozen.effectiveRequestBytes());
        for (PipelineDriverFactory factory : context.getPipelineDriverFactories()) {
          SharedTsBlockQueue queue =
              ((LocalSinkChannel) factory.getDriverContext().getSink()).getSharedTsBlockQueue();
          assertEquals(804_501L, queue.getMaxBytesCanReserve());
        }
        assertEquals(804_501L, real.sink.freezeLegacyChannelLimits().publishedCapBytes());
      } finally {
        context
            .getResourceRegistry()
            .abortUntransferred(new RuntimeException("contract test cleanup"));
      }
    }
  }

  @Test
  public void frozenRoundRobinMatchesBaselineTracesForFourAndThirtyTwoChannels() {
    long original =
        IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance();
    IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(LEGACY_CAP);
    try {
      assertRoundRobinTrace(4, 3_016_881L, new int[] {1, 3, 3, 0, 1, 3});
      assertRoundRobinTrace(32, 377_109L, new int[] {1, 3, 3, 4, 5, 7});
    } finally {
      IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(original);
    }
  }

  private void assertRoundRobinTrace(int count, long threshold, int[] expectedAfter) {
    long[] bytes = new long[count];
    int[] blocks = new int[count], sent = new int[count], baselineSent = new int[count];
    boolean[] closed = new boolean[count], ended = new boolean[count];
    List<ISinkChannel> channels = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      final int index = i;
      ISinkChannel channel = Mockito.mock(ISinkChannel.class);
      Mockito.when(channel.getBufferRetainedSizeInBytes()).thenAnswer(ignored -> bytes[index]);
      Mockito.when(channel.getNumOfBufferedTsBlocks()).thenAnswer(ignored -> blocks[index]);
      Mockito.when(channel.isClosed()).thenAnswer(ignored -> closed[index]);
      Mockito.when(channel.isNoMoreTsBlocks()).thenAnswer(ignored -> ended[index]);
      Mockito.doAnswer(
              ignored -> {
                sent[index]++;
                return null;
              })
          .when(channel)
          .send(Mockito.any(TsBlock.class));
      channels.add(channel);
    }
    DownStreamChannelIndex selected = new DownStreamChannelIndex(0);
    ShuffleSinkHandle sink =
        new ShuffleSinkHandle(
            new TFragmentInstanceId("trace_" + count, 0, "0"),
            channels,
            selected,
            ShuffleSinkHandle.ShuffleStrategyEnum.SIMPLE_ROUND_ROBIN,
            Mockito.mock(MPPDataExchangeManager.SinkListener.class));
    LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
    registry.recordLegacySinkQuota(sink, LEGACY_CAP / 5L);
    registry.freeze(LEGACY_CAP, LEGACY_CAP / 5L);
    assertEquals(
        Long.valueOf(threshold), sink.freezeLegacyChannelLimits().roundRobinThresholdBytes());
    int baseline = 0;
    for (int step = 0; step < expectedAfter.length; step++) {
      Arrays.fill(bytes, 0L);
      Arrays.fill(blocks, 0);
      Arrays.fill(closed, false);
      Arrays.fill(ended, false);
      if (step == 1) bytes[2] = threshold + 1L;
      if (step == 2) {
        for (int i = 0; i < count; i++) {
          if (i == baseline) continue;
          if (i % 4 == 0) ended[i] = true;
          else if (i % 4 == 1) closed[i] = true;
          else if (i % 4 == 2) blocks[i] = 3;
          else bytes[i] = threshold + 1L;
        }
      }
      if (step == 4) {
        bytes[(baseline + 1) % count] = threshold;
        blocks[(baseline + 1) % count] = 2;
      }
      if (step == 5) {
        Arrays.fill(bytes, threshold + 1L);
        int target = (baseline + 2) % count;
        bytes[target] = threshold;
        blocks[target] = 2;
      }
      assertEquals(baseline, selected.getCurrentIndex());
      baselineSent[baseline]++;
      int next = baseline;
      for (int offset = 1; offset < count; offset++) {
        int candidate = (baseline + offset) % count;
        if (!closed[candidate]
            && !ended[candidate]
            && blocks[candidate] <= 2
            && bytes[candidate] <= threshold) {
          next = candidate;
          break;
        }
      }
      sink.send(block(1));
      assertEquals(
          "step " + step + " channels " + count, expectedAfter[step], selected.getCurrentIndex());
      assertEquals(next, selected.getCurrentIndex());
      assertArrayEquals(baselineSent, sent);
      baseline = next;
    }
  }
}
