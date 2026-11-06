/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.Gson;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

/** Independent carriers for B6 queue omission and diagnostic-share-as-cap mutations. */
public class B6NegativeResourceContractTest {
  private static final long BUDGET = 4_022_509L;
  private static final long BASELINE_CAP = 804_501L;

  @Test
  public void omittedRealQueueKeepsCapsButFailsPhysicalObjectCount() throws Exception {
    try (Graph graph = Graph.mixedSeven()) {
      LocalExecutionResourceRegistry.Snapshot snapshot =
          graph.registry.freeze(BUDGET, BASELINE_CAP);
      graph.assertActualCaps(BASELINE_CAP);
      observation("seven-object-count", graph, snapshot);
      assertEquals("B6_REAL_QUEUE_OBJECT_COUNT_AFTER_CAP_VERIFIED", 7, snapshot.objectCount());
      assertEquals("B6_DISTINCT_QUEUES_SAME_KEY_REMAIN_PHYSICAL", 4, graph.distinctQueueObjects());
    }
  }

  @Test
  public void omittedRealQueueKeepsCapsButFailsIndependentNominalShare() throws Exception {
    try (Graph graph = Graph.mixedSeven()) {
      LocalExecutionResourceRegistry.Snapshot snapshot =
          graph.registry.freeze(BUDGET, BASELINE_CAP);
      graph.assertActualCaps(BASELINE_CAP);
      observation("seven-object-nominal-share", graph, snapshot);
      assertTrue("B6_NOMINAL_SHARE_MUST_BE_PRESENT", snapshot.nominalShareBytes().isPresent());
      assertEquals(
          "B6_REAL_QUEUE_NOMINAL_SHARE_AFTER_CAP_VERIFIED",
          BUDGET / 7L,
          snapshot.nominalShareBytes().getAsLong());
    }
  }

  @Test
  public void thirtyTwoRealLocalChannelsAcceptMeasuredBlockAboveDiagnosticShare() throws Exception {
    try (Graph graph = Graph.localThirtyTwo()) {
      LocalExecutionResourceRegistry.Snapshot snapshot = graph.registry.freeze(BUDGET, BUDGET);
      assertEquals("B6_CANONICAL_LOCAL_CHANNEL_COUNT", 32, snapshot.objectCount());
      assertEquals("B6_SAME_ACCOUNT_DOES_NOT_MERGE_QUEUES", 1, snapshot.uniqueMemoryKeyCount());
      TsBlock block = block(10_240);
      long bytes = block.getSizeInBytes();
      assertTrue("B6_MEASURED_BLOCK_ABOVE_DIAGNOSTIC_SHARE", bytes > BUDGET / 32L);
      assertTrue("B6_MEASURED_BLOCK_BELOW_LEGACY_CAP", bytes < BUDGET);
      Map<String, Object> before = new LinkedHashMap<>();
      before.put("actualBlockBytes", bytes);
      before.put("actualQueueCapBytes", graph.queues.get(0).getMaxBytesCanReserve());
      before.put("nominalShareBytes", snapshot.nominalShareBytes().getAsLong());
      before.put("actualPendingPoolFutures", graph.pendingFutures());
      System.out.println("B6_MEASURED_REAL_LOCAL_BLOCK " + new Gson().toJson(before));
      try {
        // LocalSinkChannel directly reserves the measured data block, without a remote open
        // reservation. The exception's request bytes must therefore equal this actual block.
        graph.local.get(0).send(block);
      } catch (IllegalArgumentException actualPoolFailure) {
        String message = actualPoolFailure.getMessage();
        assertTrue(
            "B6_POOL_ERROR_MUST_NAME_MEASURED_BLOCK",
            message.contains("bytesToReserve: " + bytes + "B"));
        throw new AssertionError(
            "B6_NOMINAL_AS_CAP_REAL_BLOCK_REJECTED actualBlockBytes="
                + bytes
                + " actualCapBytes="
                + graph.queues.get(0).getMaxBytesCanReserve()
                + " realPoolFailure="
                + message,
            actualPoolFailure);
      }
      assertEquals(
          "B6_REAL_RESERVATION_EQUALS_THE_SENT_BLOCK",
          bytes,
          graph.pool.snapshotFragmentReservations(graph.query, "0.0").fragmentReservedBytes());
      assertEquals(
          "B6_ONLY_ONE_SHARED_LEAF_ACCOUNT",
          1,
          graph.pool.snapshotFragmentReservations(graph.query, "0.0").byPlanNode().size());
      graph.assertActualCaps(BUDGET);
      observation("real-local-block-accepted", graph, snapshot);
    }
  }

  private static TsBlock block(int rows) {
    TsBlockBuilder builder = new TsBlockBuilder(Collections.singletonList(TSDataType.INT64));
    for (int index = 0; index < rows; index++) {
      builder.getTimeColumnBuilder().writeLong(index);
      builder.getColumnBuilder(0).writeLong(17L * index + 3L);
      builder.declarePosition();
    }
    return builder.build();
  }

  private static long actualCap(Object endpoint) throws Exception {
    Field field = endpoint.getClass().getDeclaredField("maxBytesCanReserve");
    field.setAccessible(true);
    return ((Number) field.get(endpoint)).longValue();
  }

  private static void observation(
      String shape, Graph graph, LocalExecutionResourceRegistry.Snapshot snapshot)
      throws Exception {
    Map<String, Object> value = new LinkedHashMap<>();
    value.put("shape", shape);
    value.put("snapshot", snapshot);
    value.put("actualDistinctQueues", graph.distinctQueueObjects());
    value.put(
        "actualQueueCaps",
        graph.queues.stream().map(SharedTsBlockQueue::getMaxBytesCanReserve).toArray());
    value.put(
        "actualReservedBytes",
        graph.pool.snapshotFragmentReservations(graph.query, "0.0").fragmentReservedBytes());
    value.put("actualPendingPoolFutures", graph.pendingFutures());
    System.out.println("B6_REAL_RESOURCE_OBSERVATION " + new Gson().toJson(value));
  }

  private static final class Graph implements AutoCloseable {
    final long originalCap =
        IoTDBDescriptor.getInstance().getMemoryConfig().getMaxBytesPerFragmentInstance();
    final String query;
    final FragmentInstanceId id;
    final ExecutorService executor = MoreExecutors.newDirectExecutorService();
    final MemoryPool pool =
        new MemoryPool("b6-negative-real", new MemoryManager(BUDGET * 2L), BUDGET);
    final LocalMemoryManager memory = Mockito.mock(LocalMemoryManager.class);
    final LocalExecutionResourceRegistry registry = new LocalExecutionResourceRegistry();
    final List<SharedTsBlockQueue> queues = new ArrayList<>();
    final List<LocalSinkChannel> local = new ArrayList<>();
    final List<SinkChannel> remote = new ArrayList<>();
    SourceHandle input;

    Graph(String query) {
      this.query = query;
      id = new FragmentInstanceId(new PlanFragmentId(new QueryId(query), 0), "0");
      IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(BUDGET);
      Mockito.when(memory.getQueryPool()).thenReturn(pool);
    }

    static Graph mixedSeven() throws Exception {
      Graph graph = new Graph("b6_negative_seven");
      for (int index = 0; index < 4; index++)
        graph.addLocal("same_source_account", BASELINE_CAP, false);
      graph.input =
          new SourceHandle(
              new TEndPoint("127.0.0.1", 65000),
              new TFragmentInstanceId(graph.query, 7, "0"),
              graph.id.toThrift(),
              "remote_input",
              0,
              graph.memory,
              graph.executor,
              new TsBlockSerde(),
              Mockito.mock(MPPDataExchangeManager.SourceHandleListener.class),
              Mockito.mock(IClientManager.class));
      graph.registry.registerSourceHandle(graph.input);
      graph.registry.recordLegacySourceQuota(graph.input, BASELINE_CAP);
      for (int index = 0; index < 2; index++) graph.addRemote(index);
      return graph;
    }

    static Graph localThirtyTwo() {
      Graph graph = new Graph("b6_negative_nominal");
      for (int index = 0; index < 32; index++) graph.addLocal("shared_sink", BUDGET, true);
      return graph;
    }

    private void addLocal(String planNodeId, long cap, boolean registerViaSink) {
      SharedTsBlockQueue queue =
          new SharedTsBlockQueue(id.toThrift(), planNodeId, memory, executor);
      LocalSinkChannel sink =
          new LocalSinkChannel(
              id.toThrift(), queue, Mockito.mock(MPPDataExchangeManager.SinkListener.class));
      queue.allowAddingTsBlock();
      queues.add(queue);
      local.add(sink);
      if (registerViaSink) registry.registerSink(sink);
      else registry.registerQueue(queue);
      registry.recordLegacySinkQuota(sink, cap);
    }

    @SuppressWarnings("unchecked")
    private void addRemote(int index) throws Exception {
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clients =
          Mockito.mock(IClientManager.class);
      Mockito.when(clients.borrowClient(Mockito.any(TEndPoint.class)))
          .thenReturn(Mockito.mock(SyncDataNodeMPPDataExchangeServiceClient.class));
      SinkChannel channel =
          new SinkChannel(
              new TEndPoint("127.0.0.1", 65000),
              new TFragmentInstanceId(query, 8 + index, "0"),
              "receiver_" + index,
              "remote_output_" + index,
              id.toThrift(),
              memory,
              executor,
              new TsBlockSerde(),
              Mockito.mock(MPPDataExchangeManager.SinkListener.class),
              clients);
      remote.add(channel);
      registry.registerSink(channel);
      registry.recordLegacySinkQuota(channel, BASELINE_CAP);
    }

    int distinctQueueObjects() {
      Set<SharedTsBlockQueue> identities = Collections.newSetFromMap(new IdentityHashMap<>());
      identities.addAll(queues);
      return identities.size();
    }

    int pendingFutures() throws Exception {
      Field field = MemoryPool.class.getDeclaredField("memoryReservationFutures");
      field.setAccessible(true);
      int pending = 0;
      for (Object value : ((Map<?, ?>) field.get(pool)).values())
        if (!((Future<?>) value).isDone()) pending++;
      return pending;
    }

    void assertActualCaps(long cap) throws Exception {
      for (SharedTsBlockQueue queue : queues)
        assertEquals(
            "B6_ACTUAL_QUEUE_CAP_UNCHANGED_BEFORE_ACCOUNTING", cap, queue.getMaxBytesCanReserve());
      if (input != null)
        assertEquals("B6_ACTUAL_INPUT_CAP_UNCHANGED_BEFORE_ACCOUNTING", cap, actualCap(input));
      for (SinkChannel channel : remote)
        assertEquals("B6_ACTUAL_OUTPUT_CAP_UNCHANGED_BEFORE_ACCOUNTING", cap, actualCap(channel));
    }

    @Override
    public void close() throws Exception {
      try {
        for (LocalSinkChannel channel : local) channel.abort();
        if (input != null) input.abort();
        for (SinkChannel channel : remote) channel.abort();
        assertEquals(
            "B6_FINAL_REAL_RESERVED_BYTES",
            0L,
            pool.snapshotFragmentReservations(query, "0.0").fragmentReservedBytes());
        assertEquals("B6_FINAL_REAL_PENDING_FUTURES", 0, pendingFutures());
        assertEquals("B6_FINAL_REAL_POOL_RESTORED", BUDGET * 2L, pool.getRemainingBytes());
        MemoryPool.FragmentReservationSnapshot before =
            pool.snapshotFragmentReservations(query, "0.0");
        pool.deRegisterFragmentInstanceFromQueryMemoryMap(query, "0.0", true);
        MemoryPool.FragmentReservationSnapshot after =
            pool.snapshotFragmentReservations(query, "0.0");
        pool.recordDeregistrationReceipt(
            query, "0.0", before.fragmentReservedBytes(), after, true, null);
        assertFalse("B6_FINAL_REAL_ACCOUNT_REMOVED", after.present());
        assertTrue(
            "B6_FINAL_REAL_DEREGISTRATION_RECEIPT",
            pool.getDeregistrationReceipt(query, "0.0").success());
        assertTrue(
            "B6_FINAL_ALL_REAL_QUEUES_CLOSED",
            queues.stream().allMatch(SharedTsBlockQueue::isClosed));
        System.out.println(
            "B6_REAL_RESOURCE_CLEANUP "
                + new Gson()
                    .toJson(
                        Map.of(
                            "reservedBytes",
                            after.fragmentReservedBytes(),
                            "accountPresent",
                            after.present(),
                            "poolRemaining",
                            pool.getRemainingBytes(),
                            "pendingFutures",
                            pendingFutures(),
                            "deregistration",
                            pool.getDeregistrationReceipt(query, "0.0"))));
      } finally {
        executor.shutdownNow();
        IoTDBDescriptor.getInstance().getMemoryConfig().setMaxBytesPerFragmentInstance(originalCap);
      }
    }
  }
}
