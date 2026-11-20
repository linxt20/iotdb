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

import java.util.Objects;

/** A fixed logical layout and an independently selected initial worker count. */
public final class RuntimeScanLayout {
  private final ScanParallelPlan lanePlan;
  private final int initialWorkers;
  private final boolean workerAdjustmentEnabled;

  public RuntimeScanLayout(ScanParallelPlan lanePlan, int initialWorkers) {
    this(lanePlan, initialWorkers, true);
  }

  private RuntimeScanLayout(
      ScanParallelPlan lanePlan, int initialWorkers, boolean workerAdjustmentEnabled) {
    this.lanePlan = Objects.requireNonNull(lanePlan);
    if (lanePlan.getPipelineCount() < 1
        || initialWorkers < 1
        || initialWorkers > lanePlan.getPipelineCount()) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_INVALID_RUNTIME_SCAN_LAYOUT);
    }
    this.initialWorkers = initialWorkers;
    this.workerAdjustmentEnabled = workerAdjustmentEnabled;
  }

  /** Retains the exact lane plan; insufficient work caps the effective fixed width. */
  public RuntimeScanLayout withFixedWorkers(int requestedWorkers) {
    if (requestedWorkers < 1) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_INVALID_RUNTIME_SCAN_LAYOUT);
    }
    return new RuntimeScanLayout(lanePlan, Math.min(requestedWorkers, getLaneCount()), false);
  }

  /** Final weighting may replace lane work, but cannot reset the worker policy. */
  RuntimeScanLayout withLanePlan(ScanParallelPlan finalPlan) {
    return new RuntimeScanLayout(
        finalPlan, Math.min(initialWorkers, finalPlan.getPipelineCount()), workerAdjustmentEnabled);
  }

  public boolean isWorkerAdjustmentEnabled() {
    return workerAdjustmentEnabled;
  }

  public ScanParallelPlan getLanePlan() {
    return lanePlan;
  }

  public int getInitialWorkers() {
    return initialWorkers;
  }

  public int getLaneCount() {
    return lanePlan.getPipelineCount();
  }
}
