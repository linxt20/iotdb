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

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFrontierReservation;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Execution ownership which is independent of optional, bounded feedback metadata. */
public final class RuntimeScanGroupDescriptor {
  private final String groupId;
  private final String sourcePlanNodeId;
  private final RuntimeScanLayout layout;
  private final RuntimeFrontierReservation reservation;
  private final List<Lane> lanes;

  private RuntimeScanGroupDescriptor(Builder builder, List<Lane> lanes) {
    this.groupId = builder.groupId;
    this.sourcePlanNodeId = builder.sourcePlanNodeId;
    this.layout = builder.layout;
    this.reservation = builder.reservation;
    this.lanes = Collections.unmodifiableList(new ArrayList<>(lanes));
  }

  public String getGroupId() {
    return groupId;
  }

  public String getSourcePlanNodeId() {
    return sourcePlanNodeId;
  }

  public RuntimeScanLayout getLayout() {
    return layout;
  }

  public RuntimeFrontierReservation getReservation() {
    return reservation;
  }

  public List<Lane> getLanes() {
    return lanes;
  }

  public static final class Lane {
    private final int laneIndex;
    private final int originalFactoryIndex;
    private final int originalPipelineId;
    private final PipelineDriverFactory factory;
    private final ExchangeOperator ownExchange;
    private final LocalSinkChannel sink;
    private final ISourceHandle source;
    private final ListenableFuture<?> ownConsumerClose;
    private final ListenableFuture<?> startDependency;

    private Lane(LaneBuild build, int index, ListenableFuture<?> startDependency) {
      this.laneIndex = build.index;
      this.originalFactoryIndex = index;
      this.originalPipelineId = build.factory.getDriverContext().getPipelineId();
      this.factory = build.factory;
      this.ownExchange = build.ownExchange;
      this.sink = build.sink;
      this.source = build.source;
      this.ownConsumerClose = build.ownConsumerClose;
      this.startDependency = startDependency;
    }

    public int getLaneIndex() {
      return laneIndex;
    }

    public int getOriginalFactoryIndex() {
      return originalFactoryIndex;
    }

    public int getOriginalPipelineId() {
      return originalPipelineId;
    }

    public PipelineDriverFactory getFactory() {
      return factory;
    }

    public ExchangeOperator getOwnExchange() {
      return ownExchange;
    }

    public LocalSinkChannel getSink() {
      return sink;
    }

    public ISourceHandle getSource() {
      return source;
    }

    public ListenableFuture<?> getOwnConsumerClose() {
      return Futures.nonCancellationPropagating(ownConsumerClose);
    }

    public ListenableFuture<?> getStartDependency() {
      return Futures.nonCancellationPropagating(startDependency);
    }
  }

  private static final class LaneBuild {
    private final int index;
    private final PipelineDriverFactory factory;
    private final ExchangeOperator ownExchange;
    private final LocalSinkChannel sink;
    private final ISourceHandle source;
    private final ListenableFuture<?> ownConsumerClose;

    private LaneBuild(
        int index,
        PipelineDriverFactory factory,
        ExchangeOperator ownExchange,
        LocalSinkChannel sink,
        ISourceHandle source) {
      this.index = index;
      this.factory = factory;
      this.ownExchange = ownExchange;
      this.sink = sink;
      this.source = source;
      // Capture before any consumer can close; the legacy getter is lazily initialized.
      this.ownConsumerClose = ownExchange.getBlockedDependencyDriver();
    }
  }

  /** Context-owned partial construction; even an incomplete group retains its reservation. */
  public static final class Builder {
    private final String groupId;
    private final String sourcePlanNodeId;
    private final RuntimeScanLayout layout;
    private final RuntimeFrontierReservation reservation;
    private final List<SharedTsBlockQueue> queues;
    private final List<LaneBuild> lanes;
    private boolean completed;
    private boolean failed;
    private boolean frozen;

    Builder(
        String groupId,
        String sourcePlanNodeId,
        RuntimeScanLayout layout,
        RuntimeFrontierReservation reservation) {
      this.groupId = Objects.requireNonNull(groupId);
      this.sourcePlanNodeId = Objects.requireNonNull(sourcePlanNodeId);
      this.layout = Objects.requireNonNull(layout);
      this.reservation = Objects.requireNonNull(reservation);
      this.queues = new ArrayList<>(Collections.nCopies(layout.getLaneCount(), null));
      this.lanes = new ArrayList<>(Collections.nCopies(layout.getLaneCount(), null));
    }

    public RuntimeFrontierReservation getReservation() {
      return reservation;
    }

    public RuntimeScanLayout getLayout() {
      return layout;
    }

    public void captureQueue(int lane, SharedTsBlockQueue queue) {
      requireMutable();
      Objects.requireNonNull(queue);
      if (lane < 0
          || lane >= queues.size()
          || !queue.isFrontierMode()
          || queue.getFrontierLaneIndex() != lane
          || !reservation.getMemoryReservationKey().equals(queue.getActualAccountKey())
          || (queues.get(lane) != null && queues.get(lane) != queue)) {
        throw stateError();
      }
      queues.set(lane, queue);
    }

    void registerLane(
        int lane,
        PipelineDriverFactory factory,
        ExchangeOperator exchange,
        LocalSinkChannel sink,
        ISourceHandle source) {
      requireMutable();
      Objects.requireNonNull(factory);
      Objects.requireNonNull(exchange);
      Objects.requireNonNull(sink);
      Objects.requireNonNull(source);
      if (lane < 0
          || lane >= lanes.size()
          || lanes.get(lane) != null
          || queues.get(lane) != sink.getSharedTsBlockQueue()
          || !(source instanceof LocalSourceHandle)
          || ((LocalSourceHandle) source).getSharedTsBlockQueue() != queues.get(lane)
          || exchange.getSourceHandle() != source
          || factory.getDriverContext().getDownstreamOperator() != exchange) {
        throw stateError();
      }
      lanes.set(lane, new LaneBuild(lane, factory, exchange, sink, source));
    }

    public void completeConstruction() {
      requireMutable();
      if (lanes.contains(null) || queues.contains(null)) throw stateError();
      completed = true;
    }

    RuntimeScanGroupDescriptor freeze(
        Map<PipelineDriverFactory, Integer> indices, List<ListenableFuture<?>> dependencies) {
      if (!completed || failed || frozen) throw stateError();
      List<Lane> result = new ArrayList<>(lanes.size());
      java.util.Set<PipelineDriverFactory> distinct =
          Collections.newSetFromMap(new java.util.IdentityHashMap<>());
      for (LaneBuild lane : lanes) {
        Integer index = indices.get(lane.factory);
        if (index == null || !distinct.add(lane.factory)) throw stateError();
        result.add(new Lane(lane, index, dependencies.get(index)));
      }
      frozen = true;
      return new RuntimeScanGroupDescriptor(this, result);
    }

    /** Roots/factories remain owned by the existing resource registry; avoid closing them twice. */
    public void abortBuild(Throwable cause) {
      if (failed) return;
      failed = true;
      for (SharedTsBlockQueue queue : queues) {
        if (queue == null) continue;
        try {
          queue.abort(cause);
        } catch (Throwable cleanup) {
          if (cleanup != cause) cause.addSuppressed(cleanup);
        }
      }
      try {
        reservation.requestClose();
      } catch (Throwable cleanup) {
        if (cleanup != cause) cause.addSuppressed(cleanup);
      }
    }

    private void requireMutable() {
      if (completed || failed || frozen) throw stateError();
    }
  }

  private static IllegalStateException stateError() {
    return new IllegalStateException(DataNodeQueryMessages.EXCEPTION_RUNTIME_SCAN_DESCRIPTOR_STATE);
  }
}
