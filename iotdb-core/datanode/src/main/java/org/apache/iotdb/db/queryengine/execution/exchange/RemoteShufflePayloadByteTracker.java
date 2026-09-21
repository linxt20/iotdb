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

import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Query-scoped accounting for remote TsBlock payloads.
 *
 * <p>This intentionally measures only bytes in the {@code tsBlocks} Thrift field. It excludes
 * Thrift framing, RPC headers, TLS/compression overhead, local queues, and retained memory. A
 * retry is a separate payload transfer attempt, so it is deliberately counted again. Therefore a
 * source node's sent value and a target node's received value are useful independent audit
 * observations, not a value which is silently deduplicated by sequence ID.
 *
 * <p>The structured logger is the durable hand-off for the benchmark collector. The in-process
 * snapshots are a short-lived diagnostic/read API only; idle query entries are bounded by a TTL.
 */
public class RemoteShufflePayloadByteTracker {

  public static final String AUDIT_MARKER = "REMOTE_SHUFFLE_PAYLOAD_BYTES";

  private static final Logger AUDIT_LOGGER =
      LoggerFactory.getLogger(RemoteShufflePayloadByteTracker.class.getName() + ".audit");
  private static final long RETENTION_NANOS = TimeUnit.HOURS.toNanos(1);
  private static final int PURGE_INTERVAL = 1024;
  private static final RemoteShufflePayloadByteTracker INSTANCE =
      new RemoteShufflePayloadByteTracker();

  private final ConcurrentMap<String, QueryCounters> countersByQuery = new ConcurrentHashMap<>();
  private final AtomicLong updatesSinceLastPurge = new AtomicLong();

  public static RemoteShufflePayloadByteTracker getInstance() {
    return INSTANCE;
  }

  /** Record one response payload emitted by the remote exchange Thrift service. */
  public void recordSent(TFragmentInstanceId producer, int channelIndex, List<ByteBuffer> payloads) {
    Payload payload = payloadOf(payloads);
    if (payload.bytes == 0) {
      return;
    }
    QueryCounters queryCounters = countersFor(producer.queryId);
    queryCounters.sent.add(payload.bytes, payload.blocks);
    queryCounters.fragmentCounters(producer).sent.add(payload.bytes, payload.blocks);
    audit("sent", producer.queryId, producer, null, channelIndex, payload);
    purgeExpiredEntries();
  }

  /**
   * Record one response payload received by a remote SourceHandle.
   *
   * <p>Both fragment IDs must belong to the same query. Refusing a mismatch prevents a malformed
   * or stale RPC response from contaminating an unrelated query's benchmark record.
   */
  public void recordReceived(
      TFragmentInstanceId consumer,
      TFragmentInstanceId producer,
      int channelIndex,
      List<ByteBuffer> payloads) {
    Payload payload = payloadOf(payloads);
    if (payload.bytes == 0) {
      return;
    }
    if (!consumer.queryId.equals(producer.queryId)) {
      AUDIT_LOGGER.warn(
          "{} version=1 direction=rejected_query_mismatch consumer_query_id={} producer_query_id={} consumer_fragment={} producer_fragment={} channel={}",
          AUDIT_MARKER,
          safe(consumer.queryId),
          safe(producer.queryId),
          fragmentId(consumer),
          fragmentId(producer),
          channelIndex);
      return;
    }
    QueryCounters queryCounters = countersFor(consumer.queryId);
    queryCounters.received.add(payload.bytes, payload.blocks);
    queryCounters.fragmentCounters(consumer).received.add(payload.bytes, payload.blocks);
    audit("received", consumer.queryId, producer, consumer, channelIndex, payload);
    purgeExpiredEntries();
  }

  /** Return an immutable live snapshot for a single query, or {@code null} if it has no payload. */
  public QuerySnapshot snapshot(String queryId) {
    QueryCounters counters = countersByQuery.get(queryId);
    return counters == null ? null : counters.snapshot(queryId);
  }

  /**
   * Remove a completed query's live diagnostic entry after its structured audit evidence is
   * archived. This never affects already written audit events.
   */
  public void remove(String queryId) {
    countersByQuery.remove(queryId);
  }

  /** Test-only reset. Production retention is automatic and never resets another live query. */
  public void clearForTest() {
    countersByQuery.clear();
    updatesSinceLastPurge.set(0);
  }

  private QueryCounters countersFor(String queryId) {
    return countersByQuery.computeIfAbsent(queryId, ignored -> new QueryCounters());
  }

  private void purgeExpiredEntries() {
    if (updatesSinceLastPurge.incrementAndGet() % PURGE_INTERVAL != 0) {
      return;
    }
    long now = System.nanoTime();
    countersByQuery.entrySet().removeIf(entry -> now - entry.getValue().lastUpdatedNanos > RETENTION_NANOS);
  }

  private static Payload payloadOf(List<ByteBuffer> payloads) {
    long bytes = 0;
    int blocks = 0;
    for (ByteBuffer payload : payloads) {
      if (payload != null && payload.hasRemaining()) {
        bytes += payload.remaining();
        blocks++;
      }
    }
    return new Payload(bytes, blocks);
  }

  private static void audit(
      String direction,
      String queryId,
      TFragmentInstanceId producer,
      TFragmentInstanceId consumer,
      int channelIndex,
      Payload payload) {
    AUDIT_LOGGER.info(
        "{} version=1 direction={} query_id={} producer_fragment={} consumer_fragment={} channel={} payload_bytes={} payload_blocks={}",
        AUDIT_MARKER,
        direction,
        safe(queryId),
        fragmentId(producer),
        consumer == null ? "-" : fragmentId(consumer),
        channelIndex,
        payload.bytes,
        payload.blocks);
  }

  private static String fragmentId(TFragmentInstanceId fragmentInstanceId) {
    return safe(fragmentInstanceId.queryId)
        + ":"
        + fragmentInstanceId.fragmentId
        + ":"
        + fragmentInstanceId.instanceId;
  }

  // Logfmt delimiters must not let a query ID create a new field in the durable audit stream.
  private static String safe(String value) {
    return value.replaceAll("[^A-Za-z0-9_.:-]", "_");
  }

  private static class Payload {
    private final long bytes;
    private final int blocks;

    private Payload(long bytes, int blocks) {
      this.bytes = bytes;
      this.blocks = blocks;
    }
  }

  private static class Counter {
    private final LongAdder bytes = new LongAdder();
    private final LongAdder blocks = new LongAdder();

    private void add(long payloadBytes, int payloadBlocks) {
      bytes.add(payloadBytes);
      blocks.add(payloadBlocks);
    }

    private CounterSnapshot snapshot() {
      return new CounterSnapshot(bytes.sum(), blocks.sum());
    }
  }

  private static class FragmentCounters {
    private final String fragmentInstanceId;
    private final Counter sent = new Counter();
    private final Counter received = new Counter();

    private FragmentCounters(String fragmentInstanceId) {
      this.fragmentInstanceId = fragmentInstanceId;
    }

    private FragmentSnapshot snapshot() {
      return new FragmentSnapshot(fragmentInstanceId, sent.snapshot(), received.snapshot());
    }
  }

  private static class QueryCounters {
    private final Counter sent = new Counter();
    private final Counter received = new Counter();
    private final ConcurrentMap<String, FragmentCounters> byFragment = new ConcurrentHashMap<>();
    private volatile long lastUpdatedNanos = System.nanoTime();

    private FragmentCounters fragmentCounters(TFragmentInstanceId fragmentInstanceId) {
      lastUpdatedNanos = System.nanoTime();
      String id = fragmentId(fragmentInstanceId);
      return byFragment.computeIfAbsent(id, FragmentCounters::new);
    }

    private QuerySnapshot snapshot(String queryId) {
      List<FragmentSnapshot> fragments = new ArrayList<>();
      for (Map.Entry<String, FragmentCounters> entry : byFragment.entrySet()) {
        fragments.add(entry.getValue().snapshot());
      }
      fragments.sort(Comparator.comparing(FragmentSnapshot::getFragmentInstanceId));
      return new QuerySnapshot(queryId, sent.snapshot(), received.snapshot(), fragments);
    }
  }

  public static class CounterSnapshot {
    private final long payloadBytes;
    private final long payloadBlocks;

    private CounterSnapshot(long payloadBytes, long payloadBlocks) {
      this.payloadBytes = payloadBytes;
      this.payloadBlocks = payloadBlocks;
    }

    public long getPayloadBytes() {
      return payloadBytes;
    }

    public long getPayloadBlocks() {
      return payloadBlocks;
    }
  }

  public static class FragmentSnapshot {
    private final String fragmentInstanceId;
    private final CounterSnapshot sent;
    private final CounterSnapshot received;

    private FragmentSnapshot(
        String fragmentInstanceId, CounterSnapshot sent, CounterSnapshot received) {
      this.fragmentInstanceId = fragmentInstanceId;
      this.sent = sent;
      this.received = received;
    }

    public String getFragmentInstanceId() {
      return fragmentInstanceId;
    }

    public CounterSnapshot getSent() {
      return sent;
    }

    public CounterSnapshot getReceived() {
      return received;
    }
  }

  public static class QuerySnapshot {
    private final String queryId;
    private final CounterSnapshot sent;
    private final CounterSnapshot received;
    private final List<FragmentSnapshot> fragments;

    private QuerySnapshot(
        String queryId,
        CounterSnapshot sent,
        CounterSnapshot received,
        List<FragmentSnapshot> fragments) {
      this.queryId = queryId;
      this.sent = sent;
      this.received = received;
      this.fragments = Collections.unmodifiableList(new ArrayList<>(fragments));
    }

    public String getQueryId() {
      return queryId;
    }

    public CounterSnapshot getSent() {
      return sent;
    }

    public CounterSnapshot getReceived() {
      return received;
    }

    public List<FragmentSnapshot> getFragments() {
      return fragments;
    }
  }
}
