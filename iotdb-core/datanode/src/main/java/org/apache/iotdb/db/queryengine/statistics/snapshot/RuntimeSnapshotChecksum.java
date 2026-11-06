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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/** Stable SHA-256 checksum for snapshot fields in an explicit order. */
public final class RuntimeSnapshotChecksum {

  private RuntimeSnapshotChecksum() {
    // utility class
  }

  public static String sha256(List<String> fields) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      for (String field : fields) {
        byte[] bytes = String.valueOf(field).getBytes(StandardCharsets.UTF_8);
        digest.update((byte) (bytes.length >>> 24));
        digest.update((byte) (bytes.length >>> 16));
        digest.update((byte) (bytes.length >>> 8));
        digest.update((byte) bytes.length);
        digest.update(bytes);
      }
      StringBuilder result = new StringBuilder(64);
      for (byte value : digest.digest()) {
        result.append(String.format("%02x", value & 0xff));
      }
      return result.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_SHA_256_IS_UNAVAILABLE_6140A4E9, e);
    }
  }

  public static String forFragment(FragmentRuntimeSnapshot snapshot) {
    List<String> fields =
        new java.util.ArrayList<>(
            java.util.Arrays.asList(
                snapshot.getQueryId(),
                snapshot.getFragmentInstanceId(),
                Long.toString(snapshot.getGeneration()),
                Long.toString(snapshot.getCapturedAtNanos()),
                String.valueOf(snapshot.getSourceNodeId()),
                String.valueOf(snapshot.getSourceEndpoint()),
                snapshot.getState(),
                Boolean.toString(snapshot.isTerminal())));
    for (DriverRuntimeSnapshot driver : snapshot.getDrivers()) {
      fields.add(driver.getFragmentInstanceId());
      fields.add(driver.getDriverPipelineId());
      fields.add(driver.getDriverTaskId());
      fields.add(driver.getDriverRole());
      fields.add(Long.toString(driver.getSampleGeneration()));
      fields.add(String.valueOf(driver.getFirstProcessNanos()));
      fields.add(String.valueOf(driver.getLastProcessNanos()));
      fields.add(String.valueOf(driver.getCpuNanos()));
      fields.add(String.valueOf(driver.getWallNanos()));
      fields.add(String.valueOf(driver.getReadyNanos()));
      fields.add(String.valueOf(driver.getBlockedNanos()));
      fields.add(String.valueOf(driver.getOutputRows()));
      fields.add(String.valueOf(driver.getOutputBytes()));
      fields.add(String.valueOf(driver.getTsBlocks()));
      driver.getFieldValidityReasons().entrySet().stream()
          .sorted(java.util.Map.Entry.comparingByKey())
          .forEach(
              entry -> {
                fields.add(entry.getKey());
                fields.add(entry.getValue());
              });
    }
    for (OperatorRuntimeSnapshot operator : snapshot.getOperators()) {
      fields.add(operator.getPlanNodeId());
      fields.add(operator.getOperatorType());
      fields.add(Long.toString(operator.getExecutionNanos()));
      fields.add(Long.toString(operator.getCalls()));
      fields.add(Long.toString(operator.getOutputRows()));
      fields.add(Long.toString(operator.getTsBlocks()));
      fields.add(String.valueOf(operator.getEstimatedMemory()));
      fields.add(String.valueOf(operator.getExchangeBytes()));
    }
    StaticParallelPlanIdentity plan = snapshot.getPlanIdentity();
    if (plan != null) {
      fields.add(plan.getPlanIdentityChecksum());
      fields.add(plan.getQueryId());
      fields.add(plan.getFragmentInstanceId());
      fields.add(Long.toString(plan.getPlanGeneration()));
      fields.add(Integer.toString(plan.getPlannedDop()));
      fields.add(Integer.toString(plan.getActualPipelineCount()));
      fields.addAll(plan.getWorkUnitIds());
      for (Integer id : plan.getBucketIds()) fields.add(String.valueOf(id));
      fields.addAll(plan.getExchangeEdgeIds());
      fields.add(String.valueOf(plan.getSplitReason()));
    }
    return sha256(fields);
  }
}
