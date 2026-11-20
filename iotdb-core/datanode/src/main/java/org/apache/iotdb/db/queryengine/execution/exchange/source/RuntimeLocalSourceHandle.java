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

package org.apache.iotdb.db.queryengine.execution.exchange.source;

import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager.SourceHandleListener;
import org.apache.iotdb.db.queryengine.execution.exchange.RuntimeLocalEndpoint;
import org.apache.iotdb.db.queryengine.execution.exchange.RuntimeLocalExchangeLink;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeEndpointFrontier;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.function.Function;

/** Ordinary FI INPUT. Final serialized results retain a separate native-account adapter. */
public final class RuntimeLocalSourceHandle extends RuntimeLocalEndpoint implements ISourceHandle {
  private final SourceHandleListener listener;

  public RuntimeLocalSourceHandle(
      RuntimeLocalExchangeLink link,
      RuntimeEndpointFrontier frontier,
      SourceHandleListener listener) {
    super(link, frontier, link.getEdge().consumer(), link.getEdge().consumerPlanNode());
    this.listener = Objects.requireNonNull(listener);
    link.bindInput(frontier);
    observeClosure();
  }

  @Override
  protected void notifyTerminated(Throwable cause) {
    if (!terminalNotified.compareAndSet(false, true)) return;
    if (cause == null) listener.onFinished(this);
    else {
      try {
        if (!(cause instanceof CancellationException)) listener.onFailure(this, cause);
      } finally {
        listener.onAborted(this);
      }
    }
  }

  @Override
  public TsBlock receive() {
    checkFailure();
    if (link.isCloseRequested()) return null;
    TsBlock result = link.consume(Function.identity());
    checkFinished();
    return result;
  }

  private boolean checkFinished() {
    checkFailure();
    boolean finished = link.isFinished() || link.isCloseRequested();
    if (finished) {
      link.acknowledgeFinished();
      if (terminalNotified.compareAndSet(false, true)) listener.onFinished(this);
    }
    return finished;
  }

  @Override
  public ByteBuffer getSerializedTsBlock() {
    throw new UnsupportedOperationException(
        org.apache.iotdb.db.i18n.DataNodeQueryMessages
            .EXCEPTION_RUNTIME_LOCAL_INPUT_REQUIRES_TYPED_RECEIVE_OWNERSHIP_425D3F5A);
  }

  @Override
  public boolean isFinished() {
    return checkFinished();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    checkFailure();
    return link.whenReadable();
  }

  @Override
  public void abort() {
    requestCoordinatedAbort(new CancellationException());
  }

  @Override
  public void abort(Throwable cause) {
    requestCoordinatedAbort(cause);
  }

  @Override
  public void close() {
    requestCoordinatedClose();
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.shallowSizeOfInstance(RuntimeLocalSourceHandle.class);
  }
}
