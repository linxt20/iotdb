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

import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.schedule.queue.multilevelqueue.DriverTaskHandle;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskId;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/** Opaque, scheduler-owned lifetime identity for one FI generation. */
public final class RuntimeSchedulingGroup {
  final DriverScheduler owner;
  final FragmentInstanceId id;
  final FragmentInstanceContext context;
  final long deadline;
  final SessionInfo session;
  final String quotaUser;
  final DriverTaskHandle handle;
  final Runnable stopAndWake;
  final Consumer<Throwable> failFragment;
  final boolean runtime;
  final Map<DriverTaskId, TaskRegistration> members = new LinkedHashMap<>();
  final SettableFuture<Void> removed = SettableFuture.create();
  final SettableFuture<Void> stoppedAndRemoved = SettableFuture.create();
  volatile boolean stopped;
  boolean baseMemoryClaimed;
  int outstandingExecutionProofs;
  int inFlightAdmissions;
  boolean aborted;
  boolean legacyDeregisterClaimed;
  // Direct IDs belong to the finite initial native graph. Retired worker IDs are represented
  // only by a monotonic high-water mark, so repeated resize does not retain historical objects.
  final java.util.Set<DriverTaskId> initialDirectIds = new java.util.HashSet<>();
  boolean initialDirectIdsSealed;
  int workerIdHighWater = -1;

  RuntimeSchedulingGroup(
      DriverScheduler owner,
      FragmentInstanceId id,
      FragmentInstanceContext context,
      long deadline,
      SessionInfo session,
      DriverTaskHandle handle,
      Runnable stopAndWake,
      Consumer<Throwable> failFragment,
      boolean runtime) {
    this.owner = owner;
    this.id = id;
    this.context = context;
    this.deadline = deadline;
    this.session = session;
    this.quotaUser = session == null ? null : session.getUserName();
    this.handle = handle;
    this.stopAndWake = stopAndWake;
    this.failFragment = failFragment;
    this.runtime = runtime;
  }

  public FragmentInstanceId getFragmentInstanceId() {
    return id;
  }

  public long getOriginalAbsoluteDeadlineMillis() {
    return deadline;
  }

  public java.util.List<TaskRegistration> getActiveRegistrations() {
    return owner.getRuntimeRegistrations(this);
  }

  public ListenableFuture<Void> getStoppedAndRemovedFuture() {
    return Futures.nonCancellationPropagating(stoppedAndRemoved);
  }

  public ListenableFuture<Void> getRemovedFuture() {
    return Futures.nonCancellationPropagating(removed);
  }
}
