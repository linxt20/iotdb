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

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.IChannelRoutingSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Per-fragment registry of physical exchange resources created during local planning.
 *
 * <p>The registry deliberately separates physical object counting from the legacy exchange counter.
 * Registering the same queue through its source and sink wrappers is idempotent by object identity,
 * while distinct channels may remain distinct entries even when they share one memory key.
 */
public final class LocalExecutionResourceRegistry {

  public enum QuotaPolicy {
    LEGACY_COMPATIBILITY
  }

  public record ResourceEntry(
      String kind, SharedTsBlockQueue.MemoryReservationKey memoryAccountKey) {}

  public record Snapshot(
      List<ResourceEntry> entries,
      int objectCount,
      int uniqueMemoryKeyCount,
      OptionalLong nominalShareBytes,
      long legacyCapBytes,
      long effectiveRequestBytes,
      QuotaPolicy quotaPolicy) {}

  private final Map<Object, ResourceEntry> entries = new IdentityHashMap<>();
  // Quota aliases share one physical buffer; endpoint termination state remains per wrapper.
  private final Set<Object> ownedEndpoints = Collections.newSetFromMap(new IdentityHashMap<>());
  private Snapshot frozenSnapshot;
  private final Set<Operator> ownedRoots = Collections.newSetFromMap(new IdentityHashMap<>());
  private boolean aborted;

  public synchronized void registerOperatorRoot(Operator root) {
    if (aborted)
      throw new IllegalStateException(
          DataNodeQueryMessages
              .EXCEPTION_PLANNING_RESOURCE_OWNERSHIP_HAS_ALREADY_BEEN_ABORTED_432B8254);
    ownedRoots.add(Objects.requireNonNull(root));
  }

  public synchronized void adoptOperatorRoot(
      Operator root, java.util.Collection<? extends Operator> children) {
    if (children != null) ownedRoots.removeAll(children);
    registerOperatorRoot(root);
  }

  public synchronized void releaseOperatorRoot(Operator root) {
    ownedRoots.remove(root);
  }

  /** Abort planning resources whose ownership has not passed to a driver. */
  public void abortUntransferred(Throwable primary) {
    List<Operator> roots;
    List<Object> endpoints;
    synchronized (this) {
      if (aborted) return;
      aborted = true;
      roots = new ArrayList<>(ownedRoots);
      ownedRoots.clear();
      endpoints = new ArrayList<>(ownedEndpoints);
      ownedEndpoints.clear();
    }
    // Parents and operator roots can otherwise swallow a channel cleanup exception. Finish every
    // real child/queue first so its failure reaches the original planning or cancellation cause.
    endpoints.sort(
        java.util.Comparator.comparingInt(endpoint -> endpoint instanceof ISinkHandle ? 1 : 0));
    for (Object endpoint : endpoints) {
      try {
        if (endpoint instanceof ISourceHandle) ((ISourceHandle) endpoint).abort(primary);
        else if (endpoint instanceof ISink) ((ISink) endpoint).abort();
        else if (endpoint instanceof SharedTsBlockQueue)
          ((SharedTsBlockQueue) endpoint).abort(primary);
      } catch (Throwable failure) {
        if (failure != primary) primary.addSuppressed(failure);
      }
    }
    for (Operator root : roots) {
      try {
        root.close();
      } catch (Throwable failure) {
        if (failure != primary) primary.addSuppressed(failure);
      }
    }
  }

  private org.apache.iotdb.db.queryengine.common.FragmentInstanceId auditOwner;

  public synchronized void bindResourceAudit(
      org.apache.iotdb.db.queryengine.common.FragmentInstanceId owner) {
    this.auditOwner = owner;
    if (owner == null) return;
    org.apache.iotdb.db.queryengine.execution.exchange.ResourceAuditManager.getInstance()
        .beginFragment(owner.getQueryId().getId(), owner.getFullId());
  }

  public synchronized void registerQueue(SharedTsBlockQueue queue) {
    Objects.requireNonNull(
        queue,
        () ->
            String.format(
                DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918, "queue"));
    ensureQuotaMutable();
    register(queue, "LOCAL_QUEUE", queue.getMemoryReservationKey());
  }

  public synchronized void registerSinkHandle(
      ISinkHandle sinkHandle, FragmentInstanceId fragmentInstanceId, String planNodeId) {
    Objects.requireNonNull(
        sinkHandle,
        () ->
            String.format(
                DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918,
                "sinkHandle"));
    Objects.requireNonNull(
        fragmentInstanceId,
        () ->
            String.format(
                DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918,
                "fragmentInstanceId"));
    Objects.requireNonNull(
        planNodeId,
        () ->
            String.format(
                DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918,
                "planNodeId"));
    ensureQuotaMutable();
    ownedEndpoints.add(sinkHandle);
    if (sinkHandle
        instanceof org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle) {
      for (org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkChannel channel :
          ((org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle) sinkHandle)
              .getChannelsForLifecycle()) {
        ownedEndpoints.add(channel);
        ownedEndpoints.add(quotaIdentity(channel));
      }
    }
    SharedTsBlockQueue.MemoryReservationKey key = memoryKey(fragmentInstanceId, planNodeId);
    int channelCount =
        sinkHandle instanceof IChannelRoutingSinkHandle
            ? ((IChannelRoutingSinkHandle) sinkHandle).getChannelCount()
            : 1;
    for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
      org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkChannel channel =
          sinkHandle.getChannel(channelIndex);
      SharedTsBlockQueue.MemoryReservationKey actualKey = key;
      if (channel
          instanceof org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel) {
        SharedTsBlockQueue queue =
            ((org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel) channel)
                .getSharedTsBlockQueue();
        if (queue != null) {
          registerQueue(queue);
          actualKey = queue.getMemoryReservationKey();
        }
      } else if (channel
          instanceof org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel) {
        String actualNode =
            ((org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel) channel)
                .getLocalPlanNodeId();
        if (actualNode != null)
          actualKey =
              actualMemoryKey(
                  ((org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel) channel)
                      .getLocalFragmentInstanceId(),
                  actualNode);
      }
      register(channel, "SINK_CHANNEL", actualKey);
    }
  }

  public synchronized void registerSourceHandle(ISourceHandle sourceHandle) {
    registerSourceHandle(sourceHandle, null);
  }

  public synchronized void registerSourceHandle(
      ISourceHandle sourceHandle, SharedTsBlockQueue.MemoryReservationKey memoryKey) {
    Objects.requireNonNull(
        sourceHandle,
        () ->
            String.format(
                DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918,
                "sourceHandle"));
    ensureQuotaMutable();
    if (sourceHandle
        instanceof org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle) {
      SharedTsBlockQueue queue =
          ((org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle)
                  sourceHandle)
              .getSharedTsBlockQueue();
      if (queue != null) {
        registerQueue(queue);
        memoryKey = queue.getMemoryReservationKey();
      }
    }
    if (sourceHandle
        instanceof org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle) {
      org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle remote =
          (org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle) sourceHandle;
      memoryKey = actualMemoryKey(remote.getLocalFragmentInstanceId(), remote.getLocalPlanNodeId());
    }
    register(sourceHandle, "SOURCE_HANDLE", memoryKey);
  }

  public synchronized void registerSink(ISink sink) {
    Objects.requireNonNull(
        sink,
        () ->
            String.format(
                DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918, "sink"));
    ensureQuotaMutable();
    SharedTsBlockQueue.MemoryReservationKey key = null;
    if (sink instanceof org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel) {
      SharedTsBlockQueue queue =
          ((org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel) sink)
              .getSharedTsBlockQueue();
      if (queue != null) {
        registerQueue(queue);
        key = queue.getMemoryReservationKey();
      }
    }
    if (sink instanceof org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel) {
      org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel remote =
          (org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel) sink;
      key = actualMemoryKey(remote.getLocalFragmentInstanceId(), remote.getLocalPlanNodeId());
    }
    register(sink, "SINK", key);
  }

  private static SharedTsBlockQueue.MemoryReservationKey actualMemoryKey(
      org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId fragment, String planNodeId) {
    return new SharedTsBlockQueue.MemoryReservationKey(
        fragment.getQueryId(),
        FragmentInstanceId.createFragmentInstanceIdFromTFragmentInstanceId(fragment),
        planNodeId);
  }

  public static SharedTsBlockQueue.MemoryReservationKey memoryKey(
      FragmentInstanceId fragmentInstanceId, String planNodeId) {
    Objects.requireNonNull(
        fragmentInstanceId,
        () ->
            String.format(
                DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918,
                "fragmentInstanceId"));
    Objects.requireNonNull(
        planNodeId,
        () ->
            String.format(
                DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918,
                "planNodeId"));
    return new SharedTsBlockQueue.MemoryReservationKey(
        fragmentInstanceId.getQueryId().getId(),
        fragmentInstanceId.getFragmentInstanceId(),
        planNodeId);
  }

  private static Object quotaIdentity(Object resource) {
    SharedTsBlockQueue queue = null;
    if (resource
        instanceof org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel) {
      queue =
          ((org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel) resource)
              .getSharedTsBlockQueue();
    } else if (resource
        instanceof org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle) {
      queue =
          ((org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle) resource)
              .getSharedTsBlockQueue();
    }
    return queue == null ? resource : queue;
  }

  public synchronized void register(
      Object identity, String kind, SharedTsBlockQueue.MemoryReservationKey memoryAccountKey) {
    Objects.requireNonNull(
        identity,
        () ->
            String.format(
                DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918,
                "identity"));
    Objects.requireNonNull(
        kind,
        () ->
            String.format(
                DataNodeQueryMessages.EXCEPTION_ARGUMENT_MUST_NOT_BE_NULL_ARG_A1C0C918, "kind"));
    ensureQuotaMutable();
    Object physicalResource = quotaIdentity(identity);
    SharedTsBlockQueue.MemoryReservationKey physicalMemoryKey = memoryAccountKey;
    String physicalKind = kind;
    if (physicalResource instanceof SharedTsBlockQueue) {
      physicalMemoryKey = ((SharedTsBlockQueue) physicalResource).getMemoryReservationKey();
      physicalKind = "LOCAL_QUEUE";
    }
    entries.putIfAbsent(physicalResource, new ResourceEntry(physicalKind, physicalMemoryKey));
    ownedEndpoints.add(identity);
    ownedEndpoints.add(physicalResource);
    if (auditOwner != null) {
      java.util.function.BooleanSupplier terminal =
          () -> {
            if (identity instanceof SharedTsBlockQueue)
              return ((SharedTsBlockQueue) identity).isClosed();
            if (identity instanceof ISourceHandle) {
              ISourceHandle source = (ISourceHandle) identity;
              return source.isClosed() || source.isAborted();
            }
            if (identity instanceof ISink)
              return ((ISink) identity).isClosed() || ((ISink) identity).isAborted();
            return false;
          };
      org.apache.iotdb.db.queryengine.execution.exchange.ResourceAuditManager.getInstance()
          .registerObject(
              auditOwner.getQueryId().getId(),
              auditOwner.getFullId(),
              identity,
              kind,
              terminal,
              memoryAccountKey == null ? null : memoryAccountKey.queryId(),
              memoryAccountKey == null ? null : memoryAccountKey.fragmentInstanceId(),
              memoryAccountKey == null ? null : memoryAccountKey.planNodeId());
    }
  }

  public synchronized int getObjectCount() {
    return entries.size();
  }

  public synchronized List<ResourceEntry> snapshotEntries() {
    return Collections.unmodifiableList(new ArrayList<>(entries.values()));
  }

  public synchronized Snapshot snapshot(long fragmentBudgetBytes, long legacyCapBytes) {
    if (fragmentBudgetBytes <= 0 || legacyCapBytes <= 0) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_RESOURCE_BUDGETS_MUST_BE_POSITIVE_F3EA50E8);
    }
    List<ResourceEntry> snapshotEntries = new ArrayList<>(entries.values());
    snapshotEntries.sort(
        java.util.Comparator.comparing(
                (ResourceEntry entry) -> entry.kind() == null ? "" : entry.kind())
            .thenComparing(
                entry ->
                    entry.memoryAccountKey() == null ? "" : entry.memoryAccountKey().toString()));
    Set<SharedTsBlockQueue.MemoryReservationKey> keys = new LinkedHashSet<>();
    for (ResourceEntry entry : snapshotEntries) {
      if (entry.memoryAccountKey() != null) {
        keys.add(entry.memoryAccountKey());
      }
    }
    OptionalLong nominalShareBytes =
        snapshotEntries.isEmpty()
            ? OptionalLong.empty()
            : OptionalLong.of(fragmentBudgetBytes / snapshotEntries.size());
    return new Snapshot(
        Collections.unmodifiableList(snapshotEntries),
        snapshotEntries.size(),
        keys.size(),
        nominalShareBytes,
        legacyCapBytes,
        legacyCapBytes,
        QuotaPolicy.LEGACY_COMPATIBILITY);
  }

  private final List<LegacyQuotaWrite> legacyQuotaWrites = new ArrayList<>();
  private boolean quotaPublicationStarted;
  private List<java.util.Map<String, Object>> quotaPublication = java.util.Collections.emptyList();

  private record LegacyQuotaWrite(Object target, String kind, long cap, boolean published) {}

  public synchronized void recordLegacySinkQuota(ISink sink, long cap) {
    ensureQuotaMutable();
    legacyQuotaWrites.add(new LegacyQuotaWrite(sink, "SINK", cap, false));
  }

  public synchronized void recordLegacySourceQuota(ISourceHandle source, long cap) {
    ensureQuotaMutable();
    String kind =
        source.getClass()
                == org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle.class
            ? "PLAIN_LOCAL_SOURCE_NOOP"
            : "SOURCE";
    legacyQuotaWrites.add(new LegacyQuotaWrite(source, kind, cap, false));
  }

  /** Retain the effective native policy without replaying superseded legacy writes at freeze. */
  public synchronized void observePublishedQuota(Object target, String kind, long cap) {
    ensureQuotaMutable();
    Objects.requireNonNull(target);
    legacyQuotaWrites.removeIf(write -> write.target() == target);
    legacyQuotaWrites.add(new LegacyQuotaWrite(target, kind, cap, true));
  }

  private void ensureQuotaMutable() {
    if (aborted)
      throw new IllegalStateException(
          DataNodeQueryMessages
              .EXCEPTION_PLANNING_RESOURCE_OWNERSHIP_HAS_ALREADY_BEEN_ABORTED_432B8254);
    if (quotaPublicationStarted || frozenSnapshot != null)
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_CANNOT_REGISTER_RESOURCES_AFTER_QUOTA_FREEZE_416641B9);
  }

  public synchronized List<java.util.Map<String, Object>> getQuotaPublication() {
    return quotaPublication;
  }

  private void collectPublicationTarget(
      Object target,
      long cap,
      java.util.Map<SharedTsBlockQueue, Long> queues,
      java.util.Map<ISink, Long> sinks,
      java.util.Map<ISourceHandle, Long> sources,
      java.util.Map<org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle, Long>
          parents) {
    if (target
        instanceof org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle) {
      org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle handle =
          (org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle) target;
      parents.put(handle, cap);
      for (int i = 0; i < handle.getChannelCount(); i++)
        collectPublicationTarget(handle.getChannel(i), cap, queues, sinks, sources, parents);
    } else if (target
        instanceof org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel) {
      SharedTsBlockQueue queue =
          ((org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel) target)
              .getSharedTsBlockQueue();
      if (queue != null) queues.merge(queue, cap, Math::min);
      else sinks.merge((ISink) target, cap, Math::min);
    } else if (target
        instanceof org.apache.iotdb.db.queryengine.execution.exchange.source.PipelineSourceHandle) {
      queues.merge(
          ((org.apache.iotdb.db.queryengine.execution.exchange.source.PipelineSourceHandle) target)
              .getSharedTsBlockQueue(),
          cap,
          Math::min);
    } else if (target instanceof ISourceHandle)
      sources.merge((ISourceHandle) target, cap, Math::min);
    else sinks.merge((ISink) target, cap, Math::min);
  }

  private List<java.util.Map<String, String>> quotaAccounts(Object target) {
    java.util.Set<SharedTsBlockQueue.MemoryReservationKey> keys = new java.util.LinkedHashSet<>();
    if (target instanceof ISinkHandle) {
      ISinkHandle handle = (ISinkHandle) target;
      int count =
          handle instanceof IChannelRoutingSinkHandle
              ? ((IChannelRoutingSinkHandle) handle).getChannelCount()
              : 1;
      for (int i = 0; i < count; i++) {
        ResourceEntry entry = entries.get(quotaIdentity(handle.getChannel(i)));
        if (entry != null && entry.memoryAccountKey() != null) keys.add(entry.memoryAccountKey());
      }
    } else {
      ResourceEntry entry = entries.get(quotaIdentity(target));
      if (entry != null && entry.memoryAccountKey() != null) keys.add(entry.memoryAccountKey());
    }
    List<java.util.Map<String, String>> result = new ArrayList<>();
    for (SharedTsBlockQueue.MemoryReservationKey key : keys)
      result.add(
          java.util.Map.of(
              "queryId",
              key.queryId(),
              "fragmentInstanceId",
              key.fragmentInstanceId(),
              "planNodeId",
              key.planNodeId()));
    return java.util.Collections.unmodifiableList(result);
  }

  public synchronized Snapshot freeze(long fragmentBudgetBytes, long legacyCapBytes) {
    if (aborted)
      throw new IllegalStateException(
          DataNodeQueryMessages
              .EXCEPTION_PLANNING_RESOURCE_OWNERSHIP_HAS_ALREADY_BEEN_ABORTED_432B8254);
    if (frozenSnapshot != null || quotaPublicationStarted)
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_RESOURCE_QUOTA_IS_ALREADY_FROZEN_33314DF3);
    Snapshot validated = snapshot(fragmentBudgetBytes, legacyCapBytes);
    quotaPublicationStarted = true;
    java.util.Map<SharedTsBlockQueue, Long> queues = new java.util.IdentityHashMap<>();
    java.util.Map<ISink, Long> sinks = new java.util.IdentityHashMap<>();
    java.util.Map<ISourceHandle, Long> sources = new java.util.IdentityHashMap<>();
    java.util.Map<org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle, Long>
        parents = new java.util.LinkedHashMap<>();
    List<java.util.Map<String, Object>> records = new ArrayList<>();
    for (LegacyQuotaWrite write : legacyQuotaWrites) {
      if (!write.published()) {
        collectPublicationTarget(write.target(), write.cap(), queues, sinks, sources, parents);
      } else if (write.target()
          instanceof org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle) {
        parents.put(
            (org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle)
                write.target(),
            write.cap());
      }
      java.util.Map<String, Object> record = new java.util.LinkedHashMap<>();
      record.put("kind", write.kind());
      record.put("legacyCapBytes", write.cap());
      record.put("memoryAccounts", quotaAccounts(write.target()));
      records.add(java.util.Collections.unmodifiableMap(record));
    }
    // One physical target receives one publication, including aliases from both pipeline ends.
    queues.forEach(SharedTsBlockQueue::setMaxBytesCanReserve);
    sinks.forEach(ISink::setMaxBytesCanReserve);
    sources.forEach(ISourceHandle::setMaxBytesCanReserve);
    parents.forEach(
        org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle
            ::publishLegacyHandleCap);
    for (org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle sink :
        parents.keySet()) {
      org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle.LegacyChannelLimits
          limits = sink.freezeLegacyChannelLimits();
      java.util.Map<String, Object> record = new java.util.LinkedHashMap<>();
      record.put("kind", "SHUFFLE_CONSTRUCTOR_LIMITS");
      record.put("constructorCapBytes", limits.constructorCapBytes());
      record.put("publishedCapBytes", limits.publishedCapBytes());
      record.put("roundRobinThresholdBytes", limits.roundRobinThresholdBytes());
      record.put("channelCount", limits.channelCount());
      record.put("explicitRouting", limits.explicitRouting());
      record.put("memoryAccounts", quotaAccounts(sink));
      records.add(java.util.Collections.unmodifiableMap(record));
    }
    quotaPublication = java.util.Collections.unmodifiableList(records);
    legacyQuotaWrites.clear();
    frozenSnapshot = validated;
    if (auditOwner != null)
      org.apache.iotdb.db.queryengine.execution.exchange.ResourceAuditManager.getInstance()
          .recordQuotaPublication(
              auditOwner.getQueryId().getId(), auditOwner.getFullId(), quotaPublication);
    return frozenSnapshot;
  }

  public synchronized Snapshot getFrozenSnapshot() {
    return frozenSnapshot;
  }
}
