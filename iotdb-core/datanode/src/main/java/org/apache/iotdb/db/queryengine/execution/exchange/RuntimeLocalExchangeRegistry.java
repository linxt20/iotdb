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

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.function.Predicate;

/** Lifecycle-scoped pairing. Cancellation is published by FragmentInstanceManager first. */
public final class RuntimeLocalExchangeRegistry {
  private MemoryPool pool;
  private final java.util.function.Supplier<MemoryPool> poolSupplier;
  private final Predicate<FragmentInstanceId> terminal;
  private final Map<RuntimeLocalExchangeLink.Edge, RuntimeLocalExchangeLink> links =
      new HashMap<>();

  public RuntimeLocalExchangeRegistry(MemoryPool pool, Predicate<FragmentInstanceId> terminal) {
    this(() -> pool, terminal);
    Objects.requireNonNull(pool);
  }

  public RuntimeLocalExchangeRegistry(
      java.util.function.Supplier<MemoryPool> poolSupplier,
      Predicate<FragmentInstanceId> terminal) {
    this.poolSupplier = Objects.requireNonNull(poolSupplier);
    this.terminal = Objects.requireNonNull(terminal);
  }

  public synchronized RuntimeLocalExchangeLink get(RuntimeLocalExchangeLink.Edge edge) {
    if (terminal.test(edge.producer()) || terminal.test(edge.consumer())) {
      throw new CancellationException(
          DataNodeQueryMessages.DRIVER_TASK_ABORTED_BY_ALREADY_BEING_CANCELLED);
    }
    if (pool == null) pool = Objects.requireNonNull(poolSupplier.get());
    return links.computeIfAbsent(edge, key -> new RuntimeLocalExchangeLink(key, pool));
  }

  public void cancel(FragmentInstanceId fragment, Throwable cause) {
    List<RuntimeLocalExchangeLink> affected = new ArrayList<>();
    synchronized (this) {
      for (Map.Entry<RuntimeLocalExchangeLink.Edge, RuntimeLocalExchangeLink> entry :
          links.entrySet()) {
        if (touches(entry.getKey(), fragment)) affected.add(entry.getValue());
      }
    }
    Throwable first = null;
    for (RuntimeLocalExchangeLink link : affected) {
      try {
        link.requestClose(cause);
      } catch (Throwable failure) {
        if (first == null) first = failure;
        else if (first != failure) first.addSuppressed(failure);
      }
    }
    if (first instanceof Error) throw (Error) first;
    if (first instanceof RuntimeException) throw (RuntimeException) first;
  }

  /** Remove tombstones only after authoritative FI termination and actual link closure. */
  public synchronized void retire(FragmentInstanceId fragment) {
    if (!terminal.test(fragment)) return;
    links
        .entrySet()
        .removeIf(
            entry ->
                touches(entry.getKey(), fragment)
                    && entry.getValue().isCloseRequested()
                    && entry.getValue().getClosedFuture().isDone());
  }

  private static boolean touches(RuntimeLocalExchangeLink.Edge edge, FragmentInstanceId fragment) {
    return edge.producer().equals(fragment) || edge.consumer().equals(fragment);
  }

  public synchronized int size() {
    return links.size();
  }

  public synchronized void retireAccount(String queryId, String shortFragment) {
    List<FragmentInstanceId> ids = new ArrayList<>();
    for (RuntimeLocalExchangeLink.Edge edge : links.keySet()) {
      for (FragmentInstanceId id : List.of(edge.producer(), edge.consumer())) {
        if (id.getQueryId().getId().equals(queryId)
            && id.getFragmentInstanceId().equals(shortFragment)) ids.add(id);
      }
    }
    for (FragmentInstanceId id : ids) retire(id);
  }
}
