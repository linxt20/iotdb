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

/** Counts only the existing fragment-admission account, never general pool activity. */
public final class AdmissionMemoryAccount {
  private long checks, allocationAttempts, reservations, reservedBytes, releases, releasedBytes;

  public static final class Snapshot {
    private final long checks,
        allocationAttempts,
        reservations,
        reservedBytes,
        releases,
        releasedBytes;

    private Snapshot(
        long checks,
        long attempts,
        long reservations,
        long reserved,
        long releases,
        long released) {
      this.checks = checks;
      allocationAttempts = attempts;
      this.reservations = reservations;
      reservedBytes = reserved;
      this.releases = releases;
      releasedBytes = released;
    }

    public long getChecks() {
      return checks;
    }

    public long getAllocationAttempts() {
      return allocationAttempts;
    }

    public long getReservations() {
      return reservations;
    }

    public long getReservedBytes() {
      return reservedBytes;
    }

    public long getReleases() {
      return releases;
    }

    public long getReleasedBytes() {
      return releasedBytes;
    }
  }

  synchronized void checked() {
    checks = Math.addExact(checks, 1);
  }

  synchronized void attempted() {
    allocationAttempts = Math.addExact(allocationAttempts, 1);
  }

  synchronized void reserved(long bytes) {
    reservations = Math.addExact(reservations, 1);
    reservedBytes = Math.addExact(reservedBytes, bytes);
  }

  synchronized void released(long bytes) {
    releases = Math.addExact(releases, 1);
    releasedBytes = Math.addExact(releasedBytes, bytes);
  }

  public synchronized Snapshot snapshot() {
    return new Snapshot(
        checks, allocationAttempts, reservations, reservedBytes, releases, releasedBytes);
  }
}
