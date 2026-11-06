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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Separate E identity. The D plan checksum deliberately retains its original encoding. */
public final class DynamicScanGroupingChecksum {
  private DynamicScanGroupingChecksum() {}

  public static String compute(DynamicScanGrouping grouping, ScanParallelPlan plan) {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bytes);
      text(out, "IOTDB_DYNAMIC_SCAN_GROUPING");
      out.writeInt(1);
      text(out, grouping.getMode().name());
      out.writeInt(plan.getWorkUnits().size());
      for (int i = 0; i < plan.getWorkUnits().size(); i++) {
        ScanParallelPlan.WorkUnit unit = plan.getWorkUnits().get(i);
        text(out, unit.getSourceId());
        out.writeInt(unit.getDevices().size());
        for (String device : unit.getDevices()) {
          text(out, device);
        }
        out.writeInt(unit.getTimePartitions().size());
        for (Long partition : unit.getTimePartitions()) {
          out.writeLong(partition);
        }
        out.writeLong(unit.getEstimatedCost());
        out.writeBoolean(unit.isEstimateSupported());
        text(out, grouping.getCostSources().get(i).name());
      }
      out.writeInt(plan.getBuckets().size());
      for (ScanParallelPlan.Bucket bucket : plan.getBuckets()) {
        out.writeInt(bucket.getBucketId());
        out.writeInt(bucket.getWorkUnits().size());
        for (ScanParallelPlan.WorkUnit unit : bucket.getWorkUnits()) {
          text(out, unit.getSourceId());
        }
      }
      out.flush();
      byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray());
      StringBuilder hex = new StringBuilder();
      for (byte b : digest) {
        hex.append(String.format("%02x", b & 255));
      }
      return hex.toString();
    } catch (IOException | NoSuchAlgorithmException e) {
      throw new IllegalStateException(
          DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SCAN_GROUPING_8A0E09F6, e);
    }
  }

  private static void text(DataOutputStream out, String value) throws IOException {
    if (value == null) {
      out.writeByte(0);
      return;
    }
    out.writeByte(1);
    byte[] b = value.getBytes(StandardCharsets.UTF_8);
    out.writeInt(b.length);
    out.write(b);
  }
}
