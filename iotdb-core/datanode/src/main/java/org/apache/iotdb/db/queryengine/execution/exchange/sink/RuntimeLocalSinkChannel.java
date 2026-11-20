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

package org.apache.iotdb.db.queryengine.execution.exchange.sink;

import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SinkListener;
import org.apache.iotdb.db.queryengine.execution.exchange.RuntimeLocalEndpoint;
import org.apache.iotdb.db.queryengine.execution.exchange.RuntimeLocalExchangeLink;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeEndpointFrontier;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;

/** A local OUTPUT whose buffer and close barrier are owned by its matched runtime link. */
public final class RuntimeLocalSinkChannel extends RuntimeLocalEndpoint implements ISinkChannel {
  private final SinkListener listener;
  private final AtomicBoolean ended = new AtomicBoolean();

  public RuntimeLocalSinkChannel(
      RuntimeLocalExchangeLink link,
      RuntimeEndpointFrontier frontier,
      String planNode,
      SinkListener listener) {
    super(link, frontier, link.getEdge().producer(), planNode);
    this.listener = Objects.requireNonNull(listener);
    link.bindOutput(frontier);
    observeClosure();
    link.getDrainedFuture()
        .addListener(
            () -> {
              if (link.getFailure() == null && terminalNotified.compareAndSet(false, true)) {
                listener.onFinish(this);
              }
            },
            MoreExecutors.directExecutor());
  }

  @Override
  protected void notifyTerminated(Throwable cause) {
    if (!terminalNotified.compareAndSet(false, true)) return;
    if (cause == null) listener.onFinish(this);
    else {
      try {
        if (!(cause instanceof CancellationException)) listener.onFailure(this, cause);
      } finally {
        listener.onAborted(this);
      }
    }
  }

  @Override
  public void open() {}

  @Override
  public ListenableFuture<?> isFull() {
    checkFailure();
    return link.isCloseRequested() ? Futures.immediateVoidFuture() : link.whenWritable();
  }

  @Override
  public void send(TsBlock block) {
    checkFailure();
    if (!link.isCloseRequested()) link.send(block);
  }

  @Override
  public void setNoMoreTsBlocks() {
    if (link.isCloseRequested() || !ended.compareAndSet(false, true)) return;
    listener.onEndOfBlocks(this);
    link.setNoMore();
  }

  @Override
  public boolean isNoMoreTsBlocks() {
    return ended.get();
  }

  @Override
  public int getNumOfBufferedTsBlocks() {
    return link.getBufferedBlockCount();
  }

  @Override
  public boolean isFinished() {
    return link.isCloseRequested() || link.getDrainedFuture().isDone();
  }

  @Override
  public boolean abort() {
    boolean first = !link.isCloseRequested();
    requestCoordinatedAbort(new CancellationException());
    return first;
  }

  @Override
  public boolean close() {
    boolean first = !link.isCloseRequested();
    requestCoordinatedClose();
    return first;
  }

  @Override
  public void checkState() {
    checkFailure();
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.shallowSizeOfInstance(RuntimeLocalSinkChannel.class);
  }
}
