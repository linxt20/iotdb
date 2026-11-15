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
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.rescon.quotas;

import org.apache.iotdb.common.rpc.thrift.TSetThrottleQuotaReq;

import java.util.HashMap;
import java.util.Map;

public class ThrottleQuotaLimit {
  private Map<String, QuotaLimiter> userQuotaLimiter;
  private final Map<String, Long> memLimit;
  private final Map<String, Integer> cpuLimit;
  private final Map<String, ReservedResources> reservedResources;

  public ThrottleQuotaLimit() {
    userQuotaLimiter = new HashMap<>();
    memLimit = new HashMap<>();
    cpuLimit = new HashMap<>();
    reservedResources = new HashMap<>();
  }

  public synchronized void setQuotas(TSetThrottleQuotaReq req) {
    if (!req.getThrottleQuota().getThrottleLimit().isEmpty()) {
      userQuotaLimiter.put(
          req.getUserName(), QuotaLimiter.fromThrottle(req.getThrottleQuota().getThrottleLimit()));
    }
    memLimit.put(req.getUserName(), req.getThrottleQuota().getMemLimit());
    cpuLimit.put(req.getUserName(), req.getThrottleQuota().cpuLimit);
  }

  public synchronized Map<String, QuotaLimiter> getUserQuotaLimiter() {
    return userQuotaLimiter;
  }

  public synchronized void setUserQuotaLimiter(Map<String, QuotaLimiter> userQuotaLimiter) {
    this.userQuotaLimiter = userQuotaLimiter;
  }

  public synchronized QuotaLimiter getUserLimiter(String userName) {
    return userQuotaLimiter.get(userName);
  }

  public synchronized boolean checkCpu(String userName, int cpuNum) {
    if (cpuLimit.get(userName) == null
        || cpuLimit.get(userName) == 0
        || cpuLimit.get(userName) > cpuNum) {
      return true;
    }
    return false;
  }

  public synchronized boolean checkMemory(String userName, long estimatedMemory) {
    if (memLimit.get(userName) == null
        || memLimit.get(userName) == 0
        || memLimit.get(userName) > estimatedMemory) {
      return true;
    }
    return false;
  }

  /**
   * Atomically reserves the whole batch against this user's running and pending reservations.
   *
   * <p>Callers must reserve before publishing work and retain ownership until that work releases
   * its resources. All task paths, including default and legacy execution, must use this same
   * ledger to obtain a shared bound; the legacy check methods do not reserve resources.
   *
   * <p>A missing or zero limit is unlimited. Equality with a positive limit is accepted. Lowering a
   * quota preserves existing ownership but can prevent further reservations. A zero-resource ticket
   * always succeeds for a valid user. Rejected, invalid, or overflowing requests return null
   * without changing either resource total.
   */
  public synchronized AdmissionTicket tryReserve(
      String userName, long cpuCount, long incrementalMemoryBytes) {
    if (userName == null
        || userName.trim().isEmpty()
        || cpuCount < 0
        || incrementalMemoryBytes < 0) {
      return null;
    }
    if (cpuCount == 0 && incrementalMemoryBytes == 0) {
      return new AdmissionTicket(this, userName, 0, 0);
    }

    ReservedResources current = reservedResources.get(userName);
    long totalCpu;
    long totalMemory;
    try {
      totalCpu = Math.addExact(current == null ? 0 : current.cpuCount, cpuCount);
      totalMemory =
          Math.addExact(
              current == null ? 0 : current.incrementalMemoryBytes, incrementalMemoryBytes);
    } catch (ArithmeticException e) {
      return null;
    }

    Integer configuredCpu = cpuLimit.get(userName);
    Long configuredMemory = memLimit.get(userName);
    if ((configuredCpu != null && configuredCpu != 0 && totalCpu > configuredCpu)
        || (configuredMemory != null && configuredMemory != 0 && totalMemory > configuredMemory)) {
      return null;
    }

    if (current == null) {
      current = new ReservedResources();
      reservedResources.put(userName, current);
    }
    current.cpuCount = totalCpu;
    current.incrementalMemoryBytes = totalMemory;
    return new AdmissionTicket(this, userName, cpuCount, incrementalMemoryBytes);
  }

  public synchronized long getReservedCpu(String userName) {
    ReservedResources current = reservedResources.get(userName);
    return current == null ? 0 : current.cpuCount;
  }

  public synchronized long getReservedMemory(String userName) {
    ReservedResources current = reservedResources.get(userName);
    return current == null ? 0 : current.incrementalMemoryBytes;
  }

  private void release(String userName, long cpuCount, long incrementalMemoryBytes) {
    if (cpuCount == 0 && incrementalMemoryBytes == 0) {
      return;
    }
    ReservedResources current = reservedResources.get(userName);
    current.cpuCount -= cpuCount;
    current.incrementalMemoryBytes -= incrementalMemoryBytes;
    if (current.cpuCount == 0 && current.incrementalMemoryBytes == 0) {
      reservedResources.remove(userName);
    }
  }

  private static final class ReservedResources {
    private long cpuCount;
    private long incrementalMemoryBytes;
  }

  /**
   * Ownership of a reservation. Pending-to-running transitions do not change its accounting.
   *
   * <p>Splitting transfers ownership without changing the user's totals. A caller can split worker
   * CPU tickets with zero memory and a fragment-instance base-memory ticket with zero CPU, then
   * close each at its own lifetime boundary. Closing a parent releases only its remaining
   * ownership, not its children.
   */
  public static final class AdmissionTicket implements AutoCloseable {
    private final ThrottleQuotaLimit owner;
    private final String userName;
    private long cpuCount;
    private long incrementalMemoryBytes;
    private boolean closed;

    private AdmissionTicket(
        ThrottleQuotaLimit owner, String userName, long cpuCount, long incrementalMemoryBytes) {
      this.owner = owner;
      this.userName = userName;
      this.cpuCount = cpuCount;
      this.incrementalMemoryBytes = incrementalMemoryBytes;
    }

    /** Returns null for an invalid, excessive, or already-closed ownership transfer. */
    public AdmissionTicket split(long childCpuCount, long childMemoryBytes) {
      synchronized (owner) {
        if (closed
            || childCpuCount < 0
            || childMemoryBytes < 0
            || childCpuCount > cpuCount
            || childMemoryBytes > incrementalMemoryBytes) {
          return null;
        }
        cpuCount -= childCpuCount;
        incrementalMemoryBytes -= childMemoryBytes;
        return new AdmissionTicket(owner, userName, childCpuCount, childMemoryBytes);
      }
    }

    public long getCpuCount() {
      synchronized (owner) {
        return cpuCount;
      }
    }

    public long getMemoryBytes() {
      synchronized (owner) {
        return incrementalMemoryBytes;
      }
    }

    @Override
    public void close() {
      synchronized (owner) {
        if (!closed) {
          closed = true;
          owner.release(userName, cpuCount, incrementalMemoryBytes);
          cpuCount = 0;
          incrementalMemoryBytes = 0;
        }
      }
    }
  }
}
