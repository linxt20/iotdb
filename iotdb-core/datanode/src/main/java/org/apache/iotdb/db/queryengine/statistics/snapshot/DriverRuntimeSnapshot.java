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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Immutable driver-level runtime values. Nullable measurements mean unsupported or missing. */
public final class DriverRuntimeSnapshot {

  private final String fragmentInstanceId;
  private final String driverPipelineId;
  private final String driverTaskId;
  private final String driverRole;
  private final long sampleGeneration;
  private final Long firstProcessNanos;
  private final Long lastProcessNanos;
  private final Long cpuNanos;
  private final Long wallNanos;
  private final Long readyNanos;
  private final Long blockedNanos;
  private final Long outputRows;
  private final Long outputBytes;
  private final Long tsBlocks;
  private final RuntimeSnapshotValiditySet validity;
  private final Map<String, String> fieldValidityReasons;

  private DriverRuntimeSnapshot(Builder builder) {
    fragmentInstanceId = requireId(builder.fragmentInstanceId, "fragmentInstanceId");
    driverPipelineId = requireId(builder.driverPipelineId, "driverPipelineId");
    driverTaskId = requireId(builder.driverTaskId, "driverTaskId");
    driverRole = builder.driverRole == null ? "unknown" : builder.driverRole;
    sampleGeneration = requireNonNegative(builder.sampleGeneration, "sampleGeneration");
    firstProcessNanos = nonNegativeOrNull(builder.firstProcessNanos, "firstProcessNanos");
    lastProcessNanos = nonNegativeOrNull(builder.lastProcessNanos, "lastProcessNanos");
    cpuNanos = nonNegativeOrNull(builder.cpuNanos, "cpuNanos");
    wallNanos = nonNegativeOrNull(builder.wallNanos, "wallNanos");
    readyNanos = nonNegativeOrNull(builder.readyNanos, "readyNanos");
    blockedNanos = nonNegativeOrNull(builder.blockedNanos, "blockedNanos");
    outputRows = nonNegativeOrNull(builder.outputRows, "outputRows");
    outputBytes = nonNegativeOrNull(builder.outputBytes, "outputBytes");
    tsBlocks = nonNegativeOrNull(builder.tsBlocks, "tsBlocks");
    validity = builder.validity == null ? RuntimeSnapshotValiditySet.empty() : builder.validity;
    fieldValidityReasons =
        Collections.unmodifiableMap(new LinkedHashMap<>(builder.fieldValidityReasons));
  }

  public static Builder builder(
      String fragmentInstanceId, String driverPipelineId, String driverTaskId) {
    return new Builder(fragmentInstanceId, driverPipelineId, driverTaskId);
  }

  public String getFragmentInstanceId() {
    return fragmentInstanceId;
  }

  public String getDriverPipelineId() {
    return driverPipelineId;
  }

  public String getDriverTaskId() {
    return driverTaskId;
  }

  public String getDriverRole() {
    return driverRole;
  }

  public long getSampleGeneration() {
    return sampleGeneration;
  }

  public Long getFirstProcessNanos() {
    return firstProcessNanos;
  }

  public Long getLastProcessNanos() {
    return lastProcessNanos;
  }

  public Long getCpuNanos() {
    return cpuNanos;
  }

  public Long getWallNanos() {
    return wallNanos;
  }

  public Long getReadyNanos() {
    return readyNanos;
  }

  public Long getBlockedNanos() {
    return blockedNanos;
  }

  public Long getOutputRows() {
    return outputRows;
  }

  public Long getOutputBytes() {
    return outputBytes;
  }

  public Long getTsBlocks() {
    return tsBlocks;
  }

  public RuntimeSnapshotValiditySet getValidity() {
    return validity;
  }

  public Map<String, String> getFieldValidityReasons() {
    return fieldValidityReasons;
  }

  public String getFieldValidityReason(String fieldName) {
    return fieldValidityReasons.get(fieldName);
  }

  public boolean isFieldSupported(String fieldName) {
    return !fieldValidityReasons.containsKey(fieldName);
  }

  private static String requireId(String value, String name) {
    if (value == null || value.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(DataNodeQueryMessages.EXCEPTION_ARG_MUST_NOT_BE_EMPTY_313AFCE2, name));
    }
    return value;
  }

  private static long requireNonNegative(long value, String name) {
    if (value < 0) {
      throw new IllegalArgumentException(
          String.format(DataNodeQueryMessages.EXCEPTION_ARG_MUST_BE_NON_NEGATIVE_F31E7CAD, name));
    }
    return value;
  }

  private static Long nonNegativeOrNull(Long value, String name) {
    if (value != null && value < 0) {
      throw new IllegalArgumentException(
          String.format(DataNodeQueryMessages.EXCEPTION_ARG_MUST_BE_NON_NEGATIVE_F31E7CAD, name));
    }
    return value;
  }

  public static final class Builder {
    private final String fragmentInstanceId;
    private final String driverPipelineId;
    private final String driverTaskId;
    private String driverRole;
    private long sampleGeneration;
    private Long firstProcessNanos;
    private Long lastProcessNanos;
    private Long cpuNanos;
    private Long wallNanos;
    private Long readyNanos;
    private Long blockedNanos;
    private Long outputRows;
    private Long outputBytes;
    private Long tsBlocks;
    private RuntimeSnapshotValiditySet validity;
    private final Map<String, String> fieldValidityReasons = new LinkedHashMap<>();

    private Builder(String fragmentInstanceId, String driverPipelineId, String driverTaskId) {
      this.fragmentInstanceId = fragmentInstanceId;
      this.driverPipelineId = driverPipelineId;
      this.driverTaskId = driverTaskId;
    }

    public Builder role(String value) {
      driverRole = value;
      return this;
    }

    public Builder sampleGeneration(long value) {
      sampleGeneration = value;
      return this;
    }

    public Builder firstProcessNanos(Long value) {
      firstProcessNanos = value;
      return this;
    }

    public Builder lastProcessNanos(Long value) {
      lastProcessNanos = value;
      return this;
    }

    public Builder cpuNanos(Long value) {
      cpuNanos = value;
      return this;
    }

    public Builder wallNanos(Long value) {
      wallNanos = value;
      return this;
    }

    public Builder readyNanos(Long value) {
      readyNanos = value;
      return this;
    }

    public Builder blockedNanos(Long value) {
      blockedNanos = value;
      return this;
    }

    public Builder outputRows(Long value) {
      outputRows = value;
      return this;
    }

    public Builder outputBytes(Long value) {
      outputBytes = value;
      return this;
    }

    public Builder tsBlocks(Long value) {
      tsBlocks = value;
      return this;
    }

    public Builder validity(RuntimeSnapshotValiditySet value) {
      validity = value;
      return this;
    }

    public Builder fieldValidityReason(String fieldName, String reason) {
      fieldValidityReasons.put(fieldName, reason);
      return this;
    }

    public DriverRuntimeSnapshot build() {
      return new DriverRuntimeSnapshot(this);
    }
  }
}
