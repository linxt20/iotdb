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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Immutable scan semantics. A stable source position distinguishes same-table aliases. */
public final class DynamicQueryShape {
  private final String canonical, checksum;
  private final ScanParallelPlan.Mode mode;
  private final String regionId, sourceSemanticKey;
  private final List<String> devices;

  public DynamicQueryShape(
      String database,
      String table,
      String regionId,
      String sourceSemanticKey,
      List<String> devices,
      String timePredicate,
      String filterExpression,
      Map<String, String> projectionTypes,
      String ordering,
      ScanParallelPlan.Mode mode) {
    if (database == null
        || table == null
        || regionId == null
        || sourceSemanticKey == null
        || sourceSemanticKey.isEmpty()
        || devices == null
        || projectionTypes == null
        || mode == null) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SCAN_GROUPING_8A0E09F6);
    }
    for (String device : devices) {
      if (device == null) {
        throw new IllegalArgumentException(
            DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SCAN_GROUPING_8A0E09F6);
      }
    }
    for (Map.Entry<String, String> e : projectionTypes.entrySet()) {
      if (e.getKey() == null || e.getValue() == null) {
        throw new IllegalArgumentException(
            DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SCAN_GROUPING_8A0E09F6);
      }
    }
    this.mode = mode;
    this.regionId = regionId;
    this.sourceSemanticKey = sourceSemanticKey;
    this.devices = Collections.unmodifiableList(new ArrayList<>(devices));
    StringBuilder b = new StringBuilder();
    DynamicIdentityEncoding.field(b, "IOTDB_DYNAMIC_QUERY_SHAPE_V1");
    for (String value :
        new String[] {
          database,
          table,
          regionId,
          sourceSemanticKey,
          timePredicate,
          filterExpression,
          ordering,
          mode.name()
        }) {
      DynamicIdentityEncoding.field(b, value);
    }
    DynamicIdentityEncoding.strings(b, this.devices);
    b.append("M").append(projectionTypes.size()).append('|');
    for (Map.Entry<String, String> e : new TreeMap<>(projectionTypes).entrySet()) {
      DynamicIdentityEncoding.field(b, e.getKey());
      DynamicIdentityEncoding.field(b, e.getValue());
    }
    canonical = b.toString();
    checksum = StaticParallelPlanChecksum.sha256(canonical);
  }

  public String getRegionId() {
    return regionId;
  }

  public String getSourceSemanticKey() {
    return sourceSemanticKey;
  }

  public List<String> getDevices() {
    return devices;
  }

  public long estimatedRetainedBytes() {
    long bytes =
        384L
            + 2L * canonical.length()
            + 8L * devices.size()
            + 2L * regionId.length()
            + 2L * sourceSemanticKey.length();
    for (String device : devices) {
      bytes = Math.addExact(bytes, 40L + 2L * device.length());
    }
    return bytes;
  }

  public ScanParallelPlan.Mode getMode() {
    return mode;
  }

  public String getChecksum() {
    return checksum;
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof DynamicQueryShape
        && canonical.equals(((DynamicQueryShape) other).canonical);
  }

  @Override
  public int hashCode() {
    return canonical.hashCode();
  }
}
