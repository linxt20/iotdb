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

package org.apache.iotdb.db.queryengine.execution.runtime;

/**
 * Counts resolved devices in each concrete scan's fixed partition assignment where the existing
 * scan metrics have no live completion counter. A pruned unit is resolved before a full scan;
 * closing early adds no progress.
 */
public final class RuntimeScanProgress {
  private final long total;
  private long completed;
  private long pruned;

  public RuntimeScanProgress(long total) {
    if (total < 0) throw new IllegalArgumentException();
    this.total = total;
  }

  public synchronized void resolve(long units, boolean werePruned) {
    if (units < 0 || units > total - completed) throw new IllegalArgumentException();
    completed += units;
    if (werePruned) pruned += units;
  }

  public synchronized void pruneRemaining() {
    pruned += total - completed;
    completed = total;
  }

  public synchronized Snapshot snapshot() {
    return new Snapshot(total, completed, pruned);
  }

  public record Snapshot(long total, long completed, long pruned) {
    public Snapshot {
      if (total < 0 || completed < 0 || completed > total || pruned < 0 || pruned > completed)
        throw new IllegalArgumentException();
    }
  }
}
