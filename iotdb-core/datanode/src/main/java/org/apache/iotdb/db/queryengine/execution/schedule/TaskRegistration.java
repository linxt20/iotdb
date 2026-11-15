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

package org.apache.iotdb.db.queryengine.execution.schedule;

import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTask;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskId;
import org.apache.iotdb.db.storageengine.rescon.quotas.ThrottleQuotaLimit.AdmissionTicket;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/** Separate proofs of cessation and native mapping removal; neither substitutes for the other. */
public final class TaskRegistration {
  final RuntimeSchedulingGroup group;
  final PreparedDriverBatch batch;
  final DriverSubmission submission;
  final DriverTask task;
  final SettableFuture<Void> removed = SettableFuture.create();
  final SettableFuture<Void> stopped = SettableFuture.create();
  final SettableFuture<Void> quotaSettled = SettableFuture.create();
  final SettableFuture<Void> failure = SettableFuture.create();
  boolean admissionComplete;
  boolean registered;
  boolean stoppedAccounted;
  boolean driverStopped;
  int schedulerActions;
  volatile boolean dependencyReady;
  volatile boolean everStarted;
  boolean removing;
  boolean quotaReleased;
  AdmissionTicket cpuTicket;
  AdmissionTicket memoryTicket;

  TaskRegistration(
      RuntimeSchedulingGroup group,
      PreparedDriverBatch batch,
      DriverSubmission submission,
      DriverTask task) {
    this.group = group;
    this.batch = batch;
    this.submission = submission;
    this.task = task;
  }

  public IDriver getDriver() {
    return submission.driver();
  }

  public DriverTaskId getDriverTaskId() {
    return task.getDriverTaskId();
  }

  public ListenableFuture<Void> getFailureFuture() {
    return Futures.nonCancellationPropagating(failure);
  }

  public ListenableFuture<Void> getRemovedFuture() {
    return Futures.nonCancellationPropagating(removed);
  }

  public ListenableFuture<Void> getExecutionStoppedFuture() {
    return Futures.nonCancellationPropagating(stopped);
  }

  public ListenableFuture<Void> getQuotaSettledFuture() {
    return Futures.nonCancellationPropagating(quotaSettled);
  }
}
