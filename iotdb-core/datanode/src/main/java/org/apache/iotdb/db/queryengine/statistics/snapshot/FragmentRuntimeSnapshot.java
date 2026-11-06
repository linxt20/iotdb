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

package org.apache.iotdb.db.queryengine.statistics.snapshot;

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Immutable fragment-level runtime snapshot. */
public final class FragmentRuntimeSnapshot {

  private final Integer schemaVersion;
  private final String queryId;
  private final String fragmentInstanceId;
  private final long generation;
  private final long capturedAtNanos;
  private final String sourceNodeId;
  private final String sourceEndpoint;
  private final String state;
  private final boolean terminal;
  private final List<DriverRuntimeSnapshot> drivers;
  private final List<OperatorRuntimeSnapshot> operators;
  private final RuntimeSnapshotValiditySet validity;
  private final String checksum;
  private final StaticParallelPlanIdentity planIdentity;

  public FragmentRuntimeSnapshot(
      String queryId,
      String fragmentInstanceId,
      long generation,
      long capturedAtNanos,
      String sourceNodeId,
      String sourceEndpoint,
      String state,
      boolean terminal,
      List<DriverRuntimeSnapshot> drivers,
      List<OperatorRuntimeSnapshot> operators,
      RuntimeSnapshotValiditySet validity,
      String checksum) {
    this(
        queryId,
        fragmentInstanceId,
        generation,
        capturedAtNanos,
        sourceNodeId,
        sourceEndpoint,
        state,
        terminal,
        drivers,
        operators,
        validity,
        checksum,
        null);
  }

  public FragmentRuntimeSnapshot(
      String queryId,
      String fragmentInstanceId,
      long generation,
      long capturedAtNanos,
      String sourceNodeId,
      String sourceEndpoint,
      String state,
      boolean terminal,
      List<DriverRuntimeSnapshot> drivers,
      List<OperatorRuntimeSnapshot> operators,
      RuntimeSnapshotValiditySet validity,
      String checksum,
      StaticParallelPlanIdentity planIdentity) {
    this(
        queryId,
        fragmentInstanceId,
        generation,
        capturedAtNanos,
        sourceNodeId,
        sourceEndpoint,
        state,
        terminal,
        drivers,
        operators,
        validity,
        checksum,
        planIdentity,
        1);
  }

  public FragmentRuntimeSnapshot(
      String queryId,
      String fragmentInstanceId,
      long generation,
      long capturedAtNanos,
      String sourceNodeId,
      String sourceEndpoint,
      String state,
      boolean terminal,
      List<DriverRuntimeSnapshot> drivers,
      List<OperatorRuntimeSnapshot> operators,
      RuntimeSnapshotValiditySet validity,
      String checksum,
      StaticParallelPlanIdentity planIdentity,
      Integer schemaVersion) {
    this.schemaVersion = schemaVersion;
    this.queryId = requireText(queryId, "queryId");
    this.fragmentInstanceId = requireText(fragmentInstanceId, "fragmentInstanceId");
    this.generation = nonNegative(generation, "generation");
    this.capturedAtNanos = nonNegative(capturedAtNanos, "capturedAtNanos");
    this.sourceNodeId = sourceNodeId;
    this.sourceEndpoint = sourceEndpoint;
    this.state = state == null ? "UNKNOWN" : state;
    this.terminal = terminal;
    this.drivers = immutableCopy(drivers);
    this.operators = immutableCopy(operators);
    this.validity = validity == null ? RuntimeSnapshotValiditySet.empty() : validity;
    this.checksum = checksum;
    this.planIdentity = planIdentity;
  }

  public Integer getSchemaVersion() {
    return schemaVersion;
  }

  public String getQueryId() {
    return queryId;
  }

  public String getFragmentInstanceId() {
    return fragmentInstanceId;
  }

  public long getGeneration() {
    return generation;
  }

  public long getCapturedAtNanos() {
    return capturedAtNanos;
  }

  public String getSourceNodeId() {
    return sourceNodeId;
  }

  public String getSourceEndpoint() {
    return sourceEndpoint;
  }

  public String getState() {
    return state;
  }

  public boolean isTerminal() {
    return terminal;
  }

  public List<DriverRuntimeSnapshot> getDrivers() {
    return drivers;
  }

  public List<OperatorRuntimeSnapshot> getOperators() {
    return operators;
  }

  public RuntimeSnapshotValiditySet getValidity() {
    return validity;
  }

  public String getChecksum() {
    return checksum;
  }

  public StaticParallelPlanIdentity getPlanIdentity() {
    return planIdentity;
  }

  public FragmentRuntimeSnapshot withChecksum(String value) {
    return new FragmentRuntimeSnapshot(
        queryId,
        fragmentInstanceId,
        generation,
        capturedAtNanos,
        sourceNodeId,
        sourceEndpoint,
        state,
        terminal,
        drivers,
        operators,
        validity,
        value,
        planIdentity,
        schemaVersion);
  }

  private static <T> List<T> immutableCopy(List<T> values) {
    if (values == null || values.isEmpty()) {
      return Collections.emptyList();
    }
    return Collections.unmodifiableList(new ArrayList<>(values));
  }

  private static String requireText(String value, String name) {
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(DataNodeQueryMessages.EXCEPTION_ARG_MUST_NOT_BE_EMPTY_313AFCE2, name));
    }
    return value;
  }

  private static long nonNegative(long value, String name) {
    if (value < 0) {
      throw new IllegalArgumentException(
          String.format(DataNodeQueryMessages.EXCEPTION_ARG_MUST_BE_NON_NEGATIVE_F31E7CAD, name));
    }
    return value;
  }
}
