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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Protects original payload backing through the synchronous Thrift processor's actual exit. */
public final class RpcResponseLeaseScope implements AutoCloseable {
  private static final ThreadLocal<RpcResponseLeaseScope> CURRENT = new ThreadLocal<>();
  private final RpcResponseLeaseScope previous;
  private final Thread thread;
  private final List<AutoCloseable> pins = new ArrayList<>();
  private boolean closed;

  private RpcResponseLeaseScope() {
    previous = CURRENT.get();
    thread = Thread.currentThread();
    CURRENT.set(this);
  }

  public static RpcResponseLeaseScope enter() {
    return new RpcResponseLeaseScope();
  }

  public static RpcResponseLeaseScope requireCurrent() {
    RpcResponseLeaseScope scope = CURRENT.get();
    if (scope == null || scope.closed) {
      throw new IllegalStateException("Runtime payload requires a synchronous response scope");
    }
    return scope;
  }

  public void retain(AutoCloseable pin) {
    if (closed || thread != Thread.currentThread() || CURRENT.get() != this) {
      throw new IllegalStateException("Response scope is not active on this thread");
    }
    pins.add(Objects.requireNonNull(pin));
  }

  @Override
  public void close() {
    if (closed) return;
    if (thread != Thread.currentThread() || CURRENT.get() != this) {
      throw new IllegalStateException("Response scopes must close in stack order");
    }
    closed = true;
    Throwable first = null;
    try {
      for (AutoCloseable pin : pins) {
        try {
          pin.close();
        } catch (Throwable failure) {
          if (first == null) first = failure;
          else if (first != failure) first.addSuppressed(failure);
        }
      }
    } finally {
      pins.clear();
      if (previous == null) CURRENT.remove();
      else CURRENT.set(previous);
    }
    if (first instanceof Error) throw (Error) first;
    if (first instanceof RuntimeException) throw (RuntimeException) first;
    if (first != null) throw new IllegalStateException("Response pin settlement failed", first);
  }
}
