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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * An admitted inert batch, or exact quota denial with independently verifiable candidate rollback.
 * A denied result never exposes an activatable batch. Its receipt must succeed before callers
 * continue existing work; stop, deadline expiry or any missing settlement proof fails that receipt.
 */
public final class RuntimeGrowthPreparation {
  private final PreparedDriverBatch admittedBatch;
  private final List<TaskRegistration> candidates;
  private final ListenableFuture<Void> rejectionSettlement;

  RuntimeGrowthPreparation(
      PreparedDriverBatch admittedBatch,
      List<TaskRegistration> candidates,
      ListenableFuture<Void> rejectionSettlement) {
    this.admittedBatch = admittedBatch;
    this.candidates = List.copyOf(candidates);
    this.rejectionSettlement = Objects.requireNonNull(rejectionSettlement);
  }

  public boolean isQuotaDenied() {
    return admittedBatch == null;
  }

  public Optional<PreparedDriverBatch> getAdmittedBatch() {
    return Optional.ofNullable(admittedBatch);
  }

  public List<TaskRegistration> getCandidates() {
    return candidates;
  }

  public ListenableFuture<Void> getRejectionSettlementFuture() {
    return Futures.nonCancellationPropagating(rejectionSettlement);
  }
}
