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
import org.apache.iotdb.mpp.rpc.thrift.TDriverRuntimeSnapshot;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.mpp.rpc.thrift.TOperatorStatistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Converts the optional thrift C snapshot into the immutable Java D scheduler input. */
public final class RuntimeSnapshotAdapters {
  private RuntimeSnapshotAdapters() {}

  public static FragmentRuntimeSnapshot fromResponse(
      TFetchFragmentInstanceStatisticsResp response) {
    if (response == null || !response.isSetFragmentInstanceId()) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_SNAPSHOT_RESPONSE_HAS_NO_FRAGMENT_INSTANCE_ID_5099B351);
    }
    String queryId = response.getFragmentInstanceId().getQueryId();
    String fragmentInstanceId =
        response.getFragmentInstanceId().getFragmentId()
            + "."
            + response.getFragmentInstanceId().getInstanceId();
    List<DriverRuntimeSnapshot> drivers =
        fromDrivers(response.isSetDriverSnapshots() ? response.getDriverSnapshots() : null);
    List<OperatorRuntimeSnapshot> operators =
        fromOperatorStatistics(
            response.isSetOperatorStatisticsMap()
                ? response.getOperatorStatisticsMap()
                : Collections.emptyMap());
    java.util.Set<String> driverKeys = new java.util.HashSet<>();
    for (DriverRuntimeSnapshot driver : drivers) {
      if (!fragmentInstanceId.equals(driver.getFragmentInstanceId())
          && !driver.getFragmentInstanceId().equals(queryId + "." + fragmentInstanceId)) {
        throw new IllegalArgumentException(
            DataNodeQueryMessages.EXCEPTION_DRIVER_BELONGS_TO_ANOTHER_FRAGMENT_9BDC9524);
      }
      String key = driver.getDriverPipelineId() + "|" + driver.getDriverTaskId();
      if (!driverKeys.add(key))
        throw new IllegalArgumentException(
            String.format(
                DataNodeQueryMessages.EXCEPTION_DUPLICATE_DRIVER_IDENTITY_ARG_9439336C, key));
    }
    RuntimeSnapshotValiditySet validity = RuntimeSnapshotValiditySet.empty();
    if (!response.isSetSnapshotGeneration() || !response.isSetCapturedAtNanos()) {
      validity = validity.plus(RuntimeSnapshotValidity.LEGACY_UNVERSIONED);
    }
    // Legacy fields that have no source remain explicitly unsupported; operator fields
    // are adapted below from the existing Explain Analyze map.

    if (response.isSetValidityReasons() && !response.getValidityReasons().isEmpty()) {
      validity = validity.plus(RuntimeSnapshotValidity.PARTIAL);
    }
    StaticParallelPlanIdentity identity = null;
    if (response.isSetPlanIdentityChecksum()) {
      boolean completeIdentity =
          response.isSetPlanGeneration()
              && response.isSetPlannedDop()
              && response.isSetActualPipelineCount()
              && response.isSetWorkUnitIds()
              && response.isSetBucketIds();
      if (completeIdentity) {
        identity =
            new StaticParallelPlanIdentity(
                response.getPlanIdentityChecksum(),
                queryId,
                fragmentInstanceId,
                response.getPlanGeneration(),
                response.getPlannedDop(),
                response.getActualPipelineCount(),
                response.getWorkUnitIds(),
                response.getBucketIds(),
                response.isSetExchangeEdgeIds()
                    ? response.getExchangeEdgeIds()
                    : Collections.emptyList(),
                response.isSetSplitReason() ? response.getSplitReason() : null);
      } else {
        validity = validity.plus(RuntimeSnapshotValidity.PARTIAL);
      }
    }
    FragmentRuntimeSnapshot snapshot =
        RuntimeSnapshotBuilder.buildFragment(
            queryId,
            fragmentInstanceId,
            response.isSetSnapshotGeneration() ? response.getSnapshotGeneration() : 0L,
            response.isSetCapturedAtNanos() ? response.getCapturedAtNanos() : 0L,
            response.isSetSourceNodeId() ? response.getSourceNodeId() : null,
            response.isSetSourceEndpoint() ? response.getSourceEndpoint() : null,
            response.isSetState() ? response.getState() : "UNKNOWN",
            response.isSetIsTerminal() && response.isIsTerminal(),
            drivers,
            operators,
            validity,
            identity);
    if (response.isSetSnapshotChecksum()
        && !response.getSnapshotChecksum().equals(snapshot.getChecksum())) {
      RuntimeSnapshotValiditySet mismatch =
          validity.plus(RuntimeSnapshotValidity.CHECKSUM_MISMATCH);
      snapshot =
          RuntimeSnapshotBuilder.buildFragment(
              queryId,
              fragmentInstanceId,
              response.isSetSnapshotGeneration() ? response.getSnapshotGeneration() : 0L,
              response.isSetCapturedAtNanos() ? response.getCapturedAtNanos() : 0L,
              response.isSetSourceNodeId() ? response.getSourceNodeId() : null,
              response.isSetSourceEndpoint() ? response.getSourceEndpoint() : null,
              response.isSetState() ? response.getState() : "UNKNOWN",
              response.isSetIsTerminal() && response.isIsTerminal(),
              drivers,
              operators,
              mismatch,
              identity);
    }
    return snapshot;
  }

  public static List<OperatorRuntimeSnapshot> fromOperatorStatistics(
      java.util.Map<String, TOperatorStatistics> statistics) {
    if (statistics == null || statistics.isEmpty()) return Collections.emptyList();
    List<OperatorRuntimeSnapshot> result = new ArrayList<>();
    statistics.entrySet().stream()
        .sorted(java.util.Map.Entry.comparingByKey())
        .forEach(entry -> result.add(OperatorRuntimeSnapshot.fromThrift(entry.getValue())));
    return Collections.unmodifiableList(result);
  }

  public static List<DriverRuntimeSnapshot> fromDrivers(
      List<TDriverRuntimeSnapshot> thriftSnapshots) {
    if (thriftSnapshots == null || thriftSnapshots.isEmpty()) {
      return Collections.emptyList();
    }
    List<DriverRuntimeSnapshot> result = new ArrayList<>(thriftSnapshots.size());
    for (TDriverRuntimeSnapshot thrift : thriftSnapshots) {
      DriverRuntimeSnapshot.Builder builder =
          DriverRuntimeSnapshot.builder(
                  thrift.getFragmentInstanceId(),
                  thrift.getDriverPipelineId(),
                  thrift.getDriverTaskId())
              .sampleGeneration(thrift.getDriverSampleGeneration());
      if (thrift.isSetDriverRole()) {
        builder.role(thrift.getDriverRole());
      }
      if (thrift.isSetDriverFirstProcessNanos()) {
        builder.firstProcessNanos(thrift.getDriverFirstProcessNanos());
      }
      if (thrift.isSetDriverLastProcessNanos()) {
        builder.lastProcessNanos(thrift.getDriverLastProcessNanos());
      }
      setOrUnsupported(
          thrift.isSetDriverCpuNanos(),
          thrift.isSetDriverCpuNanos() ? thrift.getDriverCpuNanos() : null,
          "driverCpuNanos",
          builder);
      setOrUnsupported(
          thrift.isSetDriverWallNanos(),
          thrift.isSetDriverWallNanos() ? thrift.getDriverWallNanos() : null,
          "driverWallNanos",
          builder);
      setOrUnsupported(
          thrift.isSetDriverReadyNanos(),
          thrift.isSetDriverReadyNanos() ? thrift.getDriverReadyNanos() : null,
          "driverReadyNanos",
          builder);
      setOrUnsupported(
          thrift.isSetDriverBlockedNanos(),
          thrift.isSetDriverBlockedNanos() ? thrift.getDriverBlockedNanos() : null,
          "driverBlockedNanos",
          builder);
      if (thrift.isSetDriverOutputRows()) {
        builder.outputRows(thrift.getDriverOutputRows());
      }
      setOrUnsupported(
          thrift.isSetDriverOutputBytes(),
          thrift.isSetDriverOutputBytes() ? thrift.getDriverOutputBytes() : null,
          "driverOutputBytes",
          builder);
      if (thrift.isSetDriverTsBlockCount()) {
        builder.tsBlocks(thrift.getDriverTsBlockCount());
      }
      if (thrift.isSetValidityReasons()) {
        for (String reason : thrift.getValidityReasons()) {
          int separator = reason.indexOf(':');
          if (separator > 0 && separator + 1 < reason.length()) {
            builder.fieldValidityReason(reason.substring(separator + 1), reason);
          }
        }
      }
      result.add(builder.build());
    }
    result.sort(RuntimeSnapshotDerivation.stableDriverOrder());
    return Collections.unmodifiableList(result);
  }

  private static void setOrUnsupported(
      boolean supported, Long value, String field, DriverRuntimeSnapshot.Builder builder) {
    if (!supported) {
      builder.fieldValidityReason(field, "UNSUPPORTED_FIELD:" + field);
      return;
    }
    switch (field) {
      case "driverCpuNanos":
        builder.cpuNanos(value);
        break;
      case "driverWallNanos":
        builder.wallNanos(value);
        break;
      case "driverReadyNanos":
        builder.readyNanos(value);
        break;
      case "driverBlockedNanos":
        builder.blockedNanos(value);
        break;
      case "driverOutputBytes":
        builder.outputBytes(value);
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                DataNodeQueryMessages.EXCEPTION_UNKNOWN_DRIVER_FIELD_ARG_AC56F319, field));
    }
  }
}
