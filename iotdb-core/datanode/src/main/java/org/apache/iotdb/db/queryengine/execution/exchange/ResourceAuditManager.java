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
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/** Bounded, explicitly enabled observations of physical query-resource lifecycles. */
public final class ResourceAuditManager {
  private static final java.util.concurrent.ScheduledThreadPoolExecutor EXPIRY = expiryExecutor();

  private static java.util.concurrent.ScheduledThreadPoolExecutor expiryExecutor() {
    java.util.concurrent.ScheduledThreadPoolExecutor executor =
        new java.util.concurrent.ScheduledThreadPoolExecutor(
            1,
            task -> {
              Thread thread = new Thread(task, "ResourceAuditWindowExpiry");
              thread.setDaemon(true);
              return thread;
            });
    executor.setRemoveOnCancelPolicy(true);
    return executor;
  }

  private static final ResourceAuditManager INSTANCE =
      new ResourceAuditManager(System::currentTimeMillis, 4, 4096, 16384, 32768, true);
  private final LongSupplier clock;
  private final boolean autoExpire;
  private final int maxWindows;
  private final int maxFragments;
  private final int maxObjects;
  private final int maxFutures;
  private final Map<String, Window> windows = new ConcurrentHashMap<>();

  ResourceAuditManager(
      LongSupplier clock, int maxWindows, int maxFragments, int maxObjects, int maxFutures) {
    this(clock, maxWindows, maxFragments, maxObjects, maxFutures, false);
  }

  private ResourceAuditManager(
      LongSupplier clock,
      int maxWindows,
      int maxFragments,
      int maxObjects,
      int maxFutures,
      boolean autoExpire) {
    this.clock = clock;
    this.autoExpire = autoExpire;
    this.maxWindows = maxWindows;
    this.maxFragments = maxFragments;
    this.maxObjects = maxObjects;
    this.maxFutures = maxFutures;
  }

  public static ResourceAuditManager getInstance() {
    return INSTANCE;
  }

  public synchronized String openWindow(long durationMs, long exchangeBytes, long operatorBytes) {
    expire();
    if (durationMs <= 0 || durationMs > 600_000L) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_DURATIONMS_MUST_BE_IN_0_600000_25F6A63F);
    }
    if (windows.size() >= maxWindows) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_TOO_MANY_RESOURCE_AUDIT_WINDOWS_E2E7CD0B);
    }
    String id = UUID.randomUUID().toString();
    Window window = new Window(clock.getAsLong() + durationMs, exchangeBytes, operatorBytes);
    windows.put(id, window);
    if (autoExpire)
      synchronized (window) {
        if (!window.closed)
          window.expiryTask =
              EXPIRY.schedule(() -> closeWindow(id), durationMs, TimeUnit.MILLISECONDS);
      }
    return id;
  }

  public void closeWindow(String id) {
    Window window = windows.remove(id);
    if (window != null)
      synchronized (window) {
        window.closed = true;
        if (window.expiryTask != null) window.expiryTask.cancel(false);
        window.records.clear();
        window.entities.clear();
      }
  }

  public boolean isEnabled() {
    if (windows.isEmpty()) return false;
    expire();
    return !windows.isEmpty();
  }

  private void expire() {
    long now = clock.getAsLong();
    for (Map.Entry<String, Window> entry : windows.entrySet()) {
      if (entry.getValue().deadline <= now) closeWindow(entry.getKey());
    }
  }

  private Window requireWindow(String id) {
    expire();
    Window window = windows.get(id);
    if (window == null)
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_RESOURCE_AUDIT_WINDOW_IS_MISSING_OR_EXPIRED_D052202A);
    return window;
  }

  public void beginFragment(String query, String fragment) {
    if (windows.isEmpty()) return;
    expire();
    for (Window window : windows.values())
      synchronized (window) {
        Record record = window.record(query, fragment);
        if (record != null) {
          if (!record.terminalEvidence.isEmpty()) record.incomplete = true;
          record.began = true;
        }
      }
  }

  /** Published only after an actual pre-dispatch cancellation and successful resource cleanup. */
  public void recordCancellationBeforeDispatch(
      String query, String fragment, String origin, String state) {
    if (windows.isEmpty()) return;
    expire();
    for (Window window : windows.values())
      synchronized (window) {
        Record record = window.record(query, fragment);
        if (record == null) continue;
        if (record.began
            || !record.drivers.isEmpty()
            || record.entities.values().stream()
                .anyMatch(entity -> entity.kind.equals("FACTORY"))) {
          record.incomplete = true;
          continue;
        }
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("kind", "CANCEL_BEFORE_DISPATCH");
        evidence.put("origin", origin);
        evidence.put("queryId", query);
        evidence.put("fragmentInstanceId", fullFragmentId(query, fragment));
        evidence.put("state", state);
        evidence.put("beforeDispatch", true);
        evidence.put("exchangeCleanupComplete", true);
        evidence.put("observedAtEpochMillis", clock.getAsLong());
        record.terminalEvidence = Collections.unmodifiableMap(evidence);
      }
  }

  public void registerObject(
      String query,
      String fragment,
      Object identity,
      String kind,
      BooleanSupplier terminal,
      String memoryQuery,
      String memoryFragment,
      String memoryPlanNode) {
    if (windows.isEmpty()) return;
    expire();
    for (Window window : windows.values())
      synchronized (window) {
        Record record = window.record(query, fragment);
        if (record == null) continue;
        if (kind.equals("FACTORY") && !record.terminalEvidence.isEmpty()) record.incomplete = true;
        Entity entity = window.entities.get(identity);
        if (entity == null) {
          if (window.entities.size() >= maxObjects) {
            window.overflow = true;
            continue;
          }
          entity =
              new Entity(
                  window.entities.size() + 1L,
                  kind,
                  terminal,
                  memoryQuery,
                  memoryFragment,
                  memoryPlanNode);
          window.entities.put(identity, entity);
        }
        record.entities.put(identity, entity);
      }
  }

  public void driverCreated(String query, String fragment, Object driver) {
    String taskId =
        driver instanceof org.apache.iotdb.db.queryengine.execution.driver.IDriver
            ? ((org.apache.iotdb.db.queryengine.execution.driver.IDriver) driver)
                .getDriverTaskId()
                .toString()
            : null;
    driverCreated(query, fragment, driver, taskId);
  }

  public void driverCreated(String query, String fragment, Object driver, String taskId) {
    if (windows.isEmpty()) return;
    expire();
    for (Window window : windows.values())
      synchronized (window) {
        Record record = window.record(query, fragment);
        if (record == null) continue;
        if (!record.terminalEvidence.isEmpty()) record.incomplete = true;
        if (window.driverCount >= maxObjects && !record.drivers.containsKey(driver)) {
          window.overflow = true;
          continue;
        }
        if (taskId == null || !taskId.startsWith(record.fragment + ".")) record.incomplete = true;
        if (!record.drivers.containsKey(driver)) {
          if (!record.driverTaskIds.add(taskId)) record.incomplete = true;
          record.drivers.put(driver, false);
          record.driverIds.put(driver, taskId);
          window.driverCount++;
        } else if (!java.util.Objects.equals(record.driverIds.get(driver), taskId))
          record.incomplete = true;
      }
  }

  public void driverClosed(String query, String fragment, Object driver) {
    if (windows.isEmpty()) return;
    expire();
    for (Window window : windows.values())
      synchronized (window) {
        Record record = window.record(query, fragment);
        if (record == null) continue;
        if (!record.drivers.containsKey(driver)) record.incomplete = true;
        else record.drivers.put(driver, true);
      }
  }

  public void trackFuture(String query, String fragment, Future<?> future) {
    if (windows.isEmpty() || future.isDone()) return;
    expire();
    for (Window window : windows.values())
      synchronized (window) {
        Record record = window.record(query, fragment);
        if (record == null) continue;
        int previousCount = record.futures.size();
        record.futures.keySet().removeIf(Future::isDone);
        window.futureCount -= previousCount - record.futures.size();
        if (window.futureCount >= maxFutures && !record.futures.containsKey(future)) {
          window.overflow = true;
          continue;
        }
        if (!record.futures.containsKey(future)) {
          record.futures.put(future, true);
          window.futureCount++;
        }
      }
  }

  public void recordDeregistration(
      String query, String fragment, MemoryPool.DeregistrationReceipt receipt) {
    if (windows.isEmpty()) return;
    expire();
    for (Window window : windows.values())
      synchronized (window) {
        Record record = window.record(query, fragment);
        if (record == null) continue;
        record.receipt = receipt;
        if (!receipt.success()
            || (!receipt.presentAfter()
                && (receipt.bytesBefore() != 0 || receipt.bytesAfter() != 0))) {
          record.deregisterFailures++;
        }
      }
  }

  public void recordQuotaPublication(
      String query, String fragment, List<Map<String, Object>> publication) {
    if (windows.isEmpty()) return;
    expire();
    for (Window window : windows.values())
      synchronized (window) {
        Record record = window.record(query, fragment);
        if (record != null) record.quotaPublication = publication;
      }
  }

  public List<Map<String, String>> listFragments(String id, String query) {
    Window window = requireWindow(id);
    List<Map<String, String>> result = new ArrayList<>();
    synchronized (window) {
      for (Record record : window.records.values()) {
        if (query.isEmpty() || query.equals(record.query)) {
          result.add(Map.of("queryId", record.query, "fragmentInstanceId", record.fragment));
        }
      }
    }
    result.sort(java.util.Comparator.comparing(item -> item.get("fragmentInstanceId")));
    return Collections.unmodifiableList(result);
  }

  public Map<String, Object> snapshot(
      String id, String query, String fragment, MemoryPool pool, long operatorsFreeBytes) {
    Window window = requireWindow(id);
    List<Entity> entities;
    List<Future<?>> futures;
    int created = 0, closed = 0, failures = 0;
    List<Map<String, Object>> drivers = new ArrayList<>();
    boolean complete;
    boolean planningBegan = false;
    Map<String, Object> terminalEvidence = Collections.emptyMap();
    MemoryPool.DeregistrationReceipt receipt = null;
    List<Map<String, Object>> publication = Collections.emptyList();
    synchronized (window) {
      Record record = window.records.get(key(query, fragment));
      complete =
          !window.closed
              && !window.overflow
              && record != null
              && (record.began || !record.terminalEvidence.isEmpty())
              && !record.incomplete;
      if (record == null) {
        entities = Collections.emptyList();
        futures = Collections.emptyList();
      } else {
        entities = new ArrayList<>(record.entities.values());
        futures = new ArrayList<>(record.futures.keySet());
        planningBegan = record.began;
        terminalEvidence = record.terminalEvidence;
        created = record.drivers.size();
        for (Map.Entry<Object, Boolean> entry : record.drivers.entrySet()) {
          if (entry.getValue()) closed++;
          Map<String, Object> driver = new LinkedHashMap<>();
          driver.put("driverTaskId", record.driverIds.get(entry.getKey()));
          driver.put("closed", entry.getValue());
          drivers.add(Collections.unmodifiableMap(driver));
        }
        publication = record.quotaPublication;
        receipt = record.receipt;
        failures = record.deregisterFailures;
      }
    }
    // Query resource locks are never acquired while holding an audit-window monitor.
    int queues = 0, endpoints = 0, factories = 0, pending = 0;
    List<Map<String, Object>> objects = new ArrayList<>();
    for (Entity entity : entities) {
      boolean terminal;
      try {
        terminal = entity.terminal.getAsBoolean();
      } catch (RuntimeException exception) {
        terminal = false;
        complete = false;
      }
      if (!terminal) {
        if (entity.kind.equals("LOCAL_QUEUE")) queues++;
        else if (entity.kind.equals("FACTORY")) factories++;
        else endpoints++;
      }
      Map<String, Object> object = new LinkedHashMap<>();
      object.put("token", entity.token);
      object.put("kind", entity.kind);
      object.put("terminal", terminal);
      object.put("memoryQueryId", entity.memoryQuery);
      object.put("memoryFragmentInstanceId", entity.memoryFragment);
      object.put("memoryPlanNodeId", entity.memoryPlanNode);
      objects.add(Collections.unmodifiableMap(object));
    }
    objects.sort(java.util.Comparator.comparingLong(item -> (long) item.get("token")));
    for (Future<?> future : futures) if (!future.isDone()) pending++;
    MemoryPool.FragmentReservationSnapshot memory =
        pool.snapshotFragmentReservations(query, memoryFragmentId(query, fragment));
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("windowId", id);
    result.put("queryId", query);
    result.put("fragmentInstanceId", fullFragmentId(query, fragment));
    result.put("memoryFragmentInstanceId", memoryFragmentId(query, fragment));
    drivers.sort(java.util.Comparator.comparing(item -> String.valueOf(item.get("driverTaskId"))));
    result.put("drivers", Collections.unmodifiableList(drivers));
    result.put("complete", complete);
    result.put("planningBegan", planningBegan);
    result.put("terminalEvidence", terminalEvidence);
    result.put("createdDrivers", created);
    result.put("closedDrivers", closed);
    result.put("liveDrivers", created - closed);
    result.put("liveQueues", queues);
    result.put("unconsumedFactories", factories);
    result.put("liveEndpoints", endpoints);
    result.put("pendingFutures", pending);
    result.put("objects", Collections.unmodifiableList(objects));
    result.put("quotaPublication", publication);
    result.put("fragmentMapPresent", memory.present());
    result.put("fragmentReservedBytes", memory.fragmentReservedBytes());
    result.put("byPlanNode", memory.byPlanNode());
    result.put("deregisterReceipt", receipt);
    result.put("deregisterFailures", failures);
    result.put("queryReservedBytes", pool.getQueryMemoryReservedBytes(query));
    result.put("exchangeRemainingBytes", pool.getRemainingBytes());
    result.put("operatorsFreeBytes", operatorsFreeBytes);
    result.put("baselineExchangeRemainingBytes", window.exchangeBytes);
    result.put("baselineOperatorsFreeBytes", window.operatorBytes);
    result.put(
        "released",
        complete
            && created == closed
            && queues == 0
            && endpoints == 0
            && factories == 0
            && pending == 0
            && !memory.present()
            && memory.fragmentReservedBytes() == 0
            && receipt != null
            && receipt.success()
            && !receipt.presentAfter()
            && receipt.bytesBefore() == 0
            && receipt.bytesAfter() == 0
            && failures == 0);
    return Collections.unmodifiableMap(result);
  }

  // Observation IDs include the query; the existing MemoryPool uses a separate query key.
  private static String fullFragmentId(String query, String fragment) {
    return fragment.startsWith(query + ".") ? fragment : query + "." + fragment;
  }

  private static String memoryFragmentId(String query, String fragment) {
    return fragment.startsWith(query + ".") ? fragment.substring(query.length() + 1) : fragment;
  }

  private static String key(String query, String fragment) {
    return query + "\0" + fullFragmentId(query, fragment);
  }

  private final class Window {
    final long deadline, exchangeBytes, operatorBytes;
    final Map<String, Record> records = new LinkedHashMap<>();
    final IdentityHashMap<Object, Entity> entities = new IdentityHashMap<>();
    boolean overflow, closed;
    java.util.concurrent.ScheduledFuture<?> expiryTask;
    int driverCount, futureCount;

    Window(long deadline, long exchangeBytes, long operatorBytes) {
      this.deadline = deadline;
      this.exchangeBytes = exchangeBytes;
      this.operatorBytes = operatorBytes;
    }

    Record record(String query, String fragment) {
      if (closed) return null;
      String key = key(query, fragment);
      Record record = records.get(key);
      if (record == null) {
        if (records.size() >= maxFragments) {
          overflow = true;
          return null;
        }
        record = new Record(query, fullFragmentId(query, fragment));
        records.put(key, record);
      }
      return record;
    }
  }

  private static final class Record {
    final String query, fragment;
    boolean began, incomplete;
    Map<String, Object> terminalEvidence = Collections.emptyMap();
    final IdentityHashMap<Object, Entity> entities = new IdentityHashMap<>();
    final IdentityHashMap<Object, Boolean> drivers = new IdentityHashMap<>();
    final IdentityHashMap<Object, String> driverIds = new IdentityHashMap<>();
    final java.util.Set<String> driverTaskIds = new java.util.HashSet<>();
    final IdentityHashMap<Future<?>, Boolean> futures = new IdentityHashMap<>();
    MemoryPool.DeregistrationReceipt receipt;
    List<Map<String, Object>> quotaPublication = Collections.emptyList();
    int deregisterFailures;

    Record(String query, String fragment) {
      this.query = query;
      this.fragment = fragment;
    }
  }

  private static final class Entity {
    final long token;
    final String kind, memoryQuery, memoryFragment, memoryPlanNode;
    final BooleanSupplier terminal;

    Entity(
        long token,
        String kind,
        BooleanSupplier terminal,
        String memoryQuery,
        String memoryFragment,
        String memoryPlanNode) {
      this.token = token;
      this.kind = kind;
      this.terminal = terminal;
      this.memoryQuery = memoryQuery;
      this.memoryFragment = memoryFragment;
      this.memoryPlanNode = memoryPlanNode;
    }
  }
}
