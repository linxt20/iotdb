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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/** Stable checksum for static parallel plans. */
public final class StaticParallelPlanChecksum {

  private StaticParallelPlanChecksum() {}

  public static String compute(ScanParallelPlan plan) {
    StringBuilder canonical = new StringBuilder();
    canonical
        .append(plan.getMode())
        .append('|')
        .append(plan.getFallbackReason())
        .append('|')
        .append(plan.getInputDop())
        .append('|')
        .append(plan.getTargetParallelism());
    for (ScanParallelPlan.WorkUnit workUnit : plan.getWorkUnits()) {
      canonical.append('|').append(workUnit.getSourceId());
      canonical.append('|').append(workUnit.getEstimatedCost());
      canonical.append('|').append(workUnit.isEstimateSupported());
      canonical.append('|').append(workUnit.getDevices());
      canonical.append('|').append(workUnit.getTimePartitions());
    }
    for (ScanParallelPlan.Bucket bucket : plan.getBuckets()) {
      append(canonical, Integer.toString(bucket.getBucketId()));
      append(canonical, Long.toString(bucket.getEstimatedCost()));
      for (ScanParallelPlan.WorkUnit workUnit : bucket.getWorkUnits()) {
        append(canonical, workUnit.getSourceId());
      }
    }
    return sha256(canonical.toString());
  }

  /**
   * Adds the immutable planning generation to a planner checksum. The generation is deliberately
   * kept outside {@link ScanParallelPlan}: the pure planner is deterministic for an input while a
   * fragment can be replanned more than once during its lifetime.
   */
  public static String withGeneration(String planChecksum, long planGeneration) {
    if (planChecksum == null || planChecksum.isEmpty()) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_PLANCHECKSUM_MUST_NOT_BE_EMPTY_7470325F);
    }
    if (planGeneration < 0) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_PLANGENERATION_MUST_NOT_BE_NEGATIVE_B8792414);
    }
    return sha256("generation|" + planGeneration + "|checksum|" + planChecksum);
  }

  /** Stable identity for a serial/fallback plan that did not go through the split planner. */
  public static String computeFallback(
      String queryId,
      String fragmentId,
      String planNodeId,
      String mode,
      String rawReason,
      int inputDop,
      int targetParallelism,
      List<String> workUnitIds,
      List<Integer> bucketIds,
      long planGeneration) {
    if (queryId == null
        || fragmentId == null
        || planNodeId == null
        || mode == null
        || rawReason == null
        || workUnitIds == null
        || bucketIds == null) {
      throw new NullPointerException(
          DataNodeQueryMessages.EXCEPTION_FALLBACK_PLAN_IDENTITY_FIELD_4136E13B);
    }
    StringBuilder fallback = new StringBuilder();
    append(fallback, queryId);
    append(fallback, fragmentId);
    append(fallback, planNodeId);
    append(fallback, mode);
    append(fallback, rawReason);
    append(fallback, Long.toString(planGeneration));
    append(fallback, Integer.toString(inputDop));
    append(fallback, Integer.toString(targetParallelism));
    for (String workUnitId : workUnitIds) {
      append(fallback, String.valueOf(workUnitId));
    }
    for (Integer bucketId : bucketIds) {
      append(fallback, String.valueOf(bucketId));
    }
    return sha256(fallback.toString());
  }

  private static void append(StringBuilder builder, String value) {
    builder.append(value.length()).append(':').append(value).append('|');
  }

  public static String sha256(String value) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
      StringBuilder result = new StringBuilder(bytes.length * 2);
      for (byte current : bytes) {
        result.append(String.format("%02x", current & 0xff));
      }
      return result.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_SHA_256_IS_UNAVAILABLE_6140A4E9, e);
    }
  }
}
