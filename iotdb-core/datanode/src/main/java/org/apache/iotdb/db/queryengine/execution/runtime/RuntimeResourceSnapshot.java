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
package org.apache.iotdb.db.queryengine.execution.runtime;

import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue;
import org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue.MemoryReservationKey;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.execution.memory.MemorySide;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;

/** Independent V3 observation. It never changes quotas, ownership, or resource lifetimes. */
public record RuntimeResourceSnapshot(
    int schemaVersion,
    boolean complete,
    List<Resource> resources,
    List<Account> accounts,
    List<Group> groups,
    Long actualReservedBytes,
    String transportMemoryAccounting) {
  public RuntimeResourceSnapshot(
      int schemaVersion,
      boolean complete,
      List<Resource> resources,
      List<Account> accounts,
      List<Group> groups,
      Long actualReservedBytes) {
    this(
        schemaVersion,
        complete,
        resources,
        accounts,
        groups,
        actualReservedBytes,
        "UNSUPPORTED_OUTSIDE_EXCHANGE_POOL");
  }

  public RuntimeResourceSnapshot {
    if (schemaVersion != 3) {
      throw new IllegalArgumentException();
    }
    resources = List.copyOf(resources);
    accounts = List.copyOf(accounts);
    groups = List.copyOf(groups);
  }

  public enum AccountState {
    PRESENT,
    NOT_OBSERVED,
    DEREGISTERED
  }

  public record Resource(
      long token,
      String kind,
      MemoryReservationKey queueIdentityKey,
      MemoryReservationKey actualAccountKey,
      String accountingMode,
      String groupId,
      int laneIndex,
      boolean terminal,
      Long bufferRetainedBytes,
      String memorySide,
      String creditAccounting,
      Long leasedCapacityBytes,
      Long bufferedPayloadBytes,
      Integer inFlightTaskCount,
      Integer consumerCount,
      Integer responsePinCount,
      Integer pendingCreditReturnCount,
      String endpointCloseState,
      String transportMode) {}

  /** Missing accounts have null bytes. A present, registered zero-byte account has bytes=0. */
  public record Account(
      long poolIdentityToken,
      MemoryReservationKey actualAccountKey,
      AccountState state,
      boolean fragmentPresent,
      Long reservedBytes,
      String memorySide) {}

  public record Group(
      String groupId,
      long poolIdentityToken,
      MemoryReservationKey actualAccountKey,
      int laneCount,
      long bytesPerLane,
      long physicalReservedBytes,
      long internalUsedBytes,
      int leasedCreditCount,
      boolean closeRequested,
      String closeState,
      String closeFailure,
      String memorySide,
      String creditAccounting) {}

  /**
   * Non-evicting ownership/provenance ledger. Observation-window budgets must not replace it.
   * Tokens and pool identities are local to this tracker; keys retain exact MemoryPool spelling.
   */
  public static final class Tracker {
    private record Registration(
        long token,
        Object identity,
        String kind,
        BooleanSupplier terminal,
        MemoryReservationKey queueKey,
        MemoryReservationKey actualKey) {}

    private record Frontier(String groupId, RuntimeFrontierReservation reservation) {}

    private final Map<Object, Registration> resources = new IdentityHashMap<>();
    private final Map<String, RuntimeFrontierReservation> groupIds = new HashMap<>();
    private final Map<RuntimeFrontierReservation, Frontier> groups = new IdentityHashMap<>();
    private final Map<MemoryPool, Map<MemoryReservationKey, RuntimeFrontierReservation>> owners =
        new IdentityHashMap<>();
    private final Map<MemoryPool, Set<MemoryReservationKey>> observedAccounts =
        new IdentityHashMap<>();
    private final Map<MemoryPool, Long> poolTokens = new IdentityHashMap<>();
    private long nextResourceToken;

    public synchronized void register(
        Object identity,
        String kind,
        BooleanSupplier terminal,
        MemoryReservationKey queueKey,
        MemoryReservationKey actualKey) {
      register(nextResourceToken + 1, identity, kind, terminal, queueKey, actualKey);
    }

    /** Explicit tokens preserve an audit window's existing cross-fragment object identity. */
    public synchronized void register(
        long token,
        Object identity,
        String kind,
        BooleanSupplier terminal,
        MemoryReservationKey queueKey,
        MemoryReservationKey actualKey) {
      Objects.requireNonNull(identity);
      Objects.requireNonNull(kind);
      Objects.requireNonNull(terminal);
      validateMapping(identity, queueKey, actualKey);
      Registration previous = resources.get(identity);
      if (previous != null) {
        if (!Objects.equals(previous.queueKey, queueKey)
            || !Objects.equals(previous.actualKey, actualKey)) {
          throw new IllegalArgumentException("Conflicting resource account mapping");
        }
        return;
      }
      resources.put(
          identity, new Registration(token, identity, kind, terminal, queueKey, actualKey));
      nextResourceToken = Math.max(nextResourceToken, token);
    }

    public synchronized void registerFrontier(
        String groupId, RuntimeFrontierReservation reservation) {
      Objects.requireNonNull(reservation);
      if (groupId == null || groupId.isBlank()) {
        throw new IllegalArgumentException();
      }
      MemoryPool pool = reservation.getMemoryPool();
      MemoryReservationKey key = reservation.getMemoryReservationKey();
      RuntimeFrontierReservation byId = groupIds.get(groupId);
      Frontier byIdentity = groups.get(reservation);
      Map<MemoryReservationKey, RuntimeFrontierReservation> poolOwners = owners.get(pool);
      RuntimeFrontierReservation byAccount = poolOwners == null ? null : poolOwners.get(key);
      if ((byId != null && byId != reservation)
          || (byIdentity != null && !byIdentity.groupId.equals(groupId))
          || (byAccount != null && byAccount != reservation)) {
        throw new IllegalArgumentException("Conflicting frontier ownership");
      }
      groupIds.put(groupId, reservation);
      groups.put(reservation, new Frontier(groupId, reservation));
      owners.computeIfAbsent(pool, ignored -> new HashMap<>()).put(key, reservation);
      observedAccounts.computeIfAbsent(pool, ignored -> new HashSet<>()).add(key);
    }

    public RuntimeResourceSnapshot snapshot(MemoryPool pool) {
      Objects.requireNonNull(pool);
      List<Registration> registrations;
      List<Frontier> frontiers;
      Set<MemoryReservationKey> previouslyObserved;
      long poolToken;
      synchronized (this) {
        registrations = new ArrayList<>(resources.values());
        frontiers = new ArrayList<>(groups.values());
        previouslyObserved = new HashSet<>(observedAccounts.getOrDefault(pool, Set.of()));
        poolToken = poolTokens.computeIfAbsent(pool, ignored -> (long) poolTokens.size() + 1);
      }
      Map<MemoryReservationKey, String> groupNames = new HashMap<>();
      Map<MemoryReservationKey, Account> accounts = new LinkedHashMap<>();
      List<Group> groupViews = new ArrayList<>();
      for (Frontier frontier : frontiers) {
        RuntimeFrontierReservation reservation = frontier.reservation;
        if (!reservation.isBackedBy(pool)) {
          throw new IllegalArgumentException("Reservation belongs to a different memory pool");
        }
        MemoryReservationKey key = reservation.getMemoryReservationKey();
        groupNames.put(key, frontier.groupId);
        accounts.put(
            key,
            readAccount(
                pool, poolToken, key, previouslyObserved, reservation.getMemorySide().name()));
        ListenableFuture<Void> closed = reservation.getClosedFuture();
        String closeState = "PENDING";
        String failure = null;
        if (closed.isDone()) {
          try {
            Futures.getDone(closed);
            closeState = "CLOSED";
          } catch (ExecutionException e) {
            closeState = "FAILED";
            failure = (e.getCause() == null ? e : e.getCause()).getClass().getName();
          } catch (CancellationException e) {
            closeState = "FAILED";
            failure = e.getClass().getName();
          }
        }
        groupViews.add(
            new Group(
                frontier.groupId,
                poolToken,
                key,
                reservation.getLaneCount(),
                reservation.getBytesPerLane(),
                reservation.getTotalReservedBytes(),
                reservation.getInternalUsedBytes(),
                reservation.getLeasedCreditCount(),
                reservation.isCloseRequested(),
                closeState,
                failure,
                reservation.getMemorySide().name(),
                "OWNER_CREDIT_CHARGE"));
      }
      boolean complete = true;
      boolean unknownMapping = false;
      List<Resource> resourceViews = new ArrayList<>();
      for (Registration registration : registrations) {
        boolean terminal;
        try {
          terminal = registration.terminal.getAsBoolean();
        } catch (RuntimeException failure) {
          terminal = false;
          complete = false;
        }
        SharedTsBlockQueue queue = queueOf(registration.identity);
        RuntimeEndpointFrontier endpoint = endpointFrontierOf(registration.identity);
        boolean frontier = (queue != null && queue.isFrontierMode()) || endpoint != null;
        String groupId = groupNames.get(registration.actualKey);
        if (frontier && groupId == null) {
          complete = false;
        }
        String mode =
            frontier || groupId != null
                ? "PREPAID_FRONTIER"
                : registration.actualKey != null
                    ? "LEGACY"
                    : registration.kind.equals("FACTORY") ? "NO_ACCOUNT" : "UNMAPPED";
        if (mode.equals("UNMAPPED")) {
          unknownMapping = true;
          complete = false;
        }
        Long payload = null;
        Integer tasks = null, consumers = null, pins = null, returning = null;
        String endpointClose = null;
        if (registration.identity
            instanceof org.apache.iotdb.db.queryengine.execution.exchange.RuntimeExchangeEndpoint) {
          org.apache.iotdb.db.queryengine.execution.exchange.RuntimeExchangeEndpoint view =
              (org.apache.iotdb.db.queryengine.execution.exchange.RuntimeExchangeEndpoint)
                  registration.identity;
          payload = view.getRuntimeBufferedPayloadBytes();
          tasks = view.getRuntimeInFlightTaskCount();
          consumers = view.getRuntimeConsumerCount();
          pins = view.getRuntimeResponsePinCount();
          returning = view.getRuntimePendingCreditReturnCount();
          endpointClose = closeState(view.getCoordinatedCloseFuture());
        }
        resourceViews.add(
            new Resource(
                registration.token,
                registration.kind,
                registration.queueKey,
                registration.actualKey,
                mode,
                groupId,
                endpoint != null
                    ? endpoint.slot()
                    : queue != null && queue.isFrontierMode() ? queue.getFrontierLaneIndex() : -1,
                terminal,
                queue == null ? null : queue.getBufferRetainedSizeInBytes(),
                endpoint != null
                    ? endpoint.reservation().getMemorySide().name()
                    : queue != null ? MemorySide.OUTPUT.name() : null,
                endpoint != null
                    ? "ENVELOPE_UPPER_BOUND"
                    : queue != null && queue.isFrontierMode() ? "RETAINED_BLOCK_CHARGE" : null,
                endpoint == null ? null : endpoint.reservation().getInternalUsedBytes(),
                payload,
                tasks,
                consumers,
                pins,
                returning,
                endpointClose,
                registration.identity
                        instanceof
                        org.apache.iotdb.db.queryengine.execution.exchange.RuntimeExchangeEndpoint
                    ? ((org.apache.iotdb.db.queryengine.execution.exchange.RuntimeExchangeEndpoint)
                            registration.identity)
                        .getTransportMode()
                    : queue != null ? "LOCAL_QUEUE" : null));
        if (registration.actualKey != null) {
          accounts.computeIfAbsent(
              registration.actualKey, key -> readAccount(pool, poolToken, key, previouslyObserved));
        }
      }
      Long total = unknownMapping ? null : 0L;
      for (Account account : accounts.values()) {
        if (account.reservedBytes == null) {
          total = null;
          complete = false;
        } else if (total != null) {
          total = Math.addExact(total, account.reservedBytes);
        }
      }
      synchronized (this) {
        Set<MemoryReservationKey> seen =
            observedAccounts.computeIfAbsent(pool, ignored -> new HashSet<>());
        for (Account account : accounts.values()) {
          if (account.state == AccountState.PRESENT) {
            seen.add(account.actualAccountKey);
          }
        }
      }
      resourceViews.sort(Comparator.comparingLong(Resource::token));
      List<Account> accountViews = new ArrayList<>(accounts.values());
      accountViews.sort(Comparator.comparing(account -> account.actualAccountKey.toString()));
      groupViews.sort(Comparator.comparing(Group::groupId));
      return new RuntimeResourceSnapshot(
          3, complete, resourceViews, accountViews, groupViews, total);
    }

    private static Account readAccount(
        MemoryPool pool, long token, MemoryReservationKey key, Set<MemoryReservationKey> seen) {
      return readAccount(pool, token, key, seen, null);
    }

    private static Account readAccount(
        MemoryPool pool,
        long token,
        MemoryReservationKey key,
        Set<MemoryReservationKey> seen,
        String memorySide) {
      MemoryPool.FragmentReservationSnapshot fragment =
          pool.snapshotFragmentReservations(key.queryId(), key.fragmentInstanceId());
      Long bytes = fragment.byPlanNode().get(key.planNodeId());
      AccountState state =
          bytes != null
              ? AccountState.PRESENT
              : seen.contains(key) ? AccountState.DEREGISTERED : AccountState.NOT_OBSERVED;
      return new Account(token, key, state, fragment.present(), bytes, memorySide);
    }
  }

  /** Validate before first registration so a failed mapping cannot poison later observations. */
  public static void validateMapping(
      Object identity, MemoryReservationKey queueKey, MemoryReservationKey actualKey) {
    if ((queueKey == null) != (actualKey == null)) {
      throw new IllegalArgumentException();
    }
    RuntimeEndpointFrontier endpoint = endpointFrontierOf(identity);
    if (endpoint != null) {
      MemorySide expectedSide =
          identity
                  instanceof org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle
              ? MemorySide.INPUT
              : MemorySide.OUTPUT;
      if (!endpoint.reservation().getMemoryReservationKey().equals(actualKey)
          || endpoint.reservation().getMemorySide() != expectedSide) {
        throw new IllegalArgumentException("Mapping disagrees with the endpoint owner");
      }
      org.apache.iotdb.db.queryengine.execution.exchange.RuntimeExchangeEndpoint view =
          (org.apache.iotdb.db.queryengine.execution.exchange.RuntimeExchangeEndpoint) identity;
      org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId fragment =
          view.getLocalFragmentInstanceId();
      String node = view.getLocalPlanNodeId();
      MemoryReservationKey logical =
          new MemoryReservationKey(
              fragment.getQueryId(),
              org.apache.iotdb.db.queryengine.common.FragmentInstanceId
                  .createFragmentInstanceIdFromTFragmentInstanceId(fragment),
              node);
      if (!logical.equals(queueKey)) {
        throw new IllegalArgumentException("Mapping disagrees with the endpoint identity");
      }
    }
    SharedTsBlockQueue queue = queueOf(identity);
    if (queue != null
        && (queueKey == null
            || !Objects.equals(queue.getQueueIdentityKey(), queueKey)
            || !Objects.equals(queue.getActualAccountKey(), actualKey))) {
      throw new IllegalArgumentException("Mapping disagrees with the queue owner");
    }
  }

  private static String closeState(ListenableFuture<Void> future) {
    if (future == null) return "NOT_REQUESTED";
    if (!future.isDone()) return "PENDING";
    try {
      Futures.getDone(future);
      return "CLOSED";
    } catch (ExecutionException | CancellationException failure) {
      return "FAILED";
    }
  }

  public static RuntimeEndpointFrontier endpointFrontierOf(Object identity) {
    if (identity
        instanceof org.apache.iotdb.db.queryengine.execution.exchange.RuntimeExchangeEndpoint) {
      return ((org.apache.iotdb.db.queryengine.execution.exchange.RuntimeExchangeEndpoint) identity)
          .getRuntimeFrontier();
    }
    return null;
  }

  public static SharedTsBlockQueue queueOf(Object identity) {
    if (identity instanceof SharedTsBlockQueue) {
      return (SharedTsBlockQueue) identity;
    }
    if (identity instanceof LocalSinkChannel) {
      return ((LocalSinkChannel) identity).getSharedTsBlockQueue();
    }
    if (identity instanceof LocalSourceHandle) {
      return ((LocalSourceHandle) identity).getSharedTsBlockQueue();
    }
    return null;
  }
}
