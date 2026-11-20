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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeEndpointFrontier;
import org.apache.iotdb.mpp.rpc.thrift.MPPDataExchangeService;
import org.apache.iotdb.mpp.rpc.thrift.TAcknowledgeDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TCloseSinkChannelEvent;
import org.apache.iotdb.mpp.rpc.thrift.TEndOfDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockRequest;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockResponse;
import org.apache.iotdb.mpp.rpc.thrift.TNewDataBlockEvent;

import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Serialized same-JVM result transport. Native INPUT admission remains in SourceHandle; OUTPUT
 * credit and response pins remain in SinkChannel. This object owns neither account.
 */
public final class LocalResultExchange {
  @FunctionalInterface
  public interface BlockFetcher {
    TGetDataBlockResponse fetch(TGetDataBlockRequest request) throws TException;
  }

  private final RuntimeLocalExchangeLink.Edge edge;
  private final MPPDataExchangeService.Iface controls;
  private final BlockFetcher fetcher;
  private final Consumer<TGetDataBlockResponse> beforeCopy;
  private final AtomicReference<RuntimeEndpointFrontier> producer = new AtomicReference<>();
  private final java.util.concurrent.atomic.AtomicBoolean consumerClosed =
      new java.util.concurrent.atomic.AtomicBoolean();
  private volatile boolean producerRetired;
  private volatile boolean consumerRetired;
  private Runnable closePublishedProducer;

  public LocalResultExchange(
      RuntimeLocalExchangeLink.Edge edge,
      MPPDataExchangeService.Iface controls,
      BlockFetcher fetcher) {
    this(edge, controls, fetcher, response -> {});
  }

  @TestOnly
  LocalResultExchange(
      RuntimeLocalExchangeLink.Edge edge,
      MPPDataExchangeService.Iface controls,
      BlockFetcher fetcher,
      Consumer<TGetDataBlockResponse> beforeCopy) {
    this.edge = Objects.requireNonNull(edge);
    this.controls = Objects.requireNonNull(controls);
    this.fetcher = Objects.requireNonNull(fetcher);
    this.beforeCopy = Objects.requireNonNull(beforeCopy);
  }

  public RuntimeLocalExchangeLink.Edge getEdge() {
    return edge;
  }

  public void bindProducer(RuntimeEndpointFrontier frontier, TFragmentInstanceId owner) {
    require(edge.producer(), owner);
    requireConsumerOpen();
    if (frontier == null || frontier.reservation().getMemorySide() != MemorySide.OUTPUT) {
      throw new IllegalArgumentException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_LOCAL_RESULT_PRODUCER_REQUIRES_A_PREPAID_OUTPUT_FRONTIER_85C8209D);
    }
    if (!producer.compareAndSet(null, frontier)) {
      throw new IllegalStateException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_LOCAL_RESULT_PRODUCER_IS_ALREADY_BOUND_72894B56);
    }
  }

  private RuntimeEndpointFrontier producer() {
    RuntimeEndpointFrontier frontier = producer.get();
    if (frontier == null)
      throw new IllegalStateException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_LOCAL_RESULT_PRODUCER_IS_NOT_BOUND_F6BDAFD9);
    return frontier;
  }

  public TGetDataBlockResponse getDataBlock(TGetDataBlockRequest request) throws TException {
    requireRequest(request.getSourceFragmentInstanceId(), request.getIndex());
    requireConsumerOpen();
    RuntimeEndpointFrontier frontier = producer();
    // The actual copy exits before response pins are released, including on cancellation/error.
    try (RpcResponseLeaseScope ignored = RpcResponseLeaseScope.enter()) {
      TGetDataBlockResponse original = Objects.requireNonNull(fetcher.fetch(request));
      beforeCopy.accept(original);
      TGetDataBlockResponse copy = new TGetDataBlockResponse(new ArrayList<>());
      if (original.isSetOffset()) copy.setOffset(original.getOffset());
      if (original.isSetTotalLength()) copy.setTotalLength(original.getTotalLength());
      for (ByteBuffer payload : original.getTsBlocks()) {
        ByteBuffer source = payload.duplicate();
        if (source.remaining() <= 0
            || source.remaining() > frontier.bounds().maxSerializedBytes()) {
          throw new IllegalArgumentException(
              org.apache.iotdb.db.i18n.DataNodeQueryMessages
                  .EXCEPTION_LOCAL_RESULT_PAYLOAD_EXCEEDS_ITS_ADMITTED_BOUND_ABBF3BCD);
        }
        byte[] detached = new byte[source.remaining()];
        source.get(detached);
        copy.addToTsBlocks(ByteBuffer.wrap(detached));
      }
      return copy;
    }
  }

  public void onNewDataBlockEvent(TNewDataBlockEvent event) throws TException {
    require(edge.producer(), event.getSourceFragmentInstanceId());
    requireTarget(event.getTargetFragmentInstanceId(), event.getTargetPlanNodeId());
    producer();
    controls.onNewDataBlockEvent(event);
  }

  public void onEndOfDataBlockEvent(TEndOfDataBlockEvent event) throws TException {
    require(edge.producer(), event.getSourceFragmentInstanceId());
    requireTarget(event.getTargetFragmentInstanceId(), event.getTargetPlanNodeId());
    controls.onEndOfDataBlockEvent(event);
  }

  public void onAcknowledgeDataBlockEvent(TAcknowledgeDataBlockEvent event) throws TException {
    requireRequest(event.getSourceFragmentInstanceId(), event.getIndex());
    controls.onAcknowledgeDataBlockEvent(event);
  }

  public void onCloseSinkChannelEvent(TCloseSinkChannelEvent event) throws TException {
    requireRequest(event.getSourceFragmentInstanceId(), event.getIndex());
    controls.onCloseSinkChannelEvent(event);
  }

  private void requireRequest(TFragmentInstanceId id, int channel) {
    require(edge.producer(), id);
    if (channel != edge.channel())
      throw new IllegalArgumentException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_LOCAL_RESULT_EXCHANGE_BELONGS_TO_ANOTHER_EDGE_5EF70208);
  }

  private void requireTarget(TFragmentInstanceId id, String node) {
    require(edge.consumer(), id);
    if (!edge.consumerPlanNode().equals(node))
      throw new IllegalArgumentException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_LOCAL_RESULT_EXCHANGE_BELONGS_TO_ANOTHER_EDGE_5EF70208);
  }

  private static void require(FragmentInstanceId expected, TFragmentInstanceId id) {
    if (id == null || !expected.equals(FragmentInstanceId.fromThrift(id))) {
      throw new IllegalArgumentException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_LOCAL_RESULT_EXCHANGE_BELONGS_TO_ANOTHER_EDGE_5EF70208);
    }
  }

  public void markConsumerClosed() {
    Runnable close;
    synchronized (this) {
      if (consumerClosed.getAndSet(true)) return;
      close = closePublishedProducer;
    }
    // A close event may precede publication. Pair publication and closure in metadata first,
    // then terminate the actual producer outside this and the source/manager monitors.
    if (close != null) close.run();
  }

  void publishProducer(Runnable publish, Runnable closeProducer) {
    synchronized (this) {
      requireConsumerOpen();
      producer();
      if (closePublishedProducer != null)
        throw new IllegalStateException(
            org.apache.iotdb.db.i18n.DataNodeQueryMessages
                .EXCEPTION_LOCAL_RESULT_PRODUCER_IS_ALREADY_BOUND_72894B56);
      publish.run();
      closePublishedProducer = closeProducer;
    }
  }

  private void requireConsumerOpen() {
    if (consumerClosed.get())
      throw new java.util.concurrent.CancellationException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .DRIVER_TASK_ABORTED_BY_ALREADY_BEING_CANCELLED);
  }

  public void retireAccount(String query, String fragment) {
    if (!edge.producer().getQueryId().getId().equals(query)) return;
    if (edge.producer().getFragmentInstanceId().equals(fragment)) producerRetired = true;
    if (edge.consumer().getFragmentInstanceId().equals(fragment)) consumerRetired = true;
  }

  public boolean isRetired() {
    return producerRetired && consumerRetired;
  }
}
