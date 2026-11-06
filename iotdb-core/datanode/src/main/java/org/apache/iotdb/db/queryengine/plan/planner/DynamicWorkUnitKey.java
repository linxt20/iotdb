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

/** Coverage identity for matching independent completed runs, never for runtime counter deltas. */
public final class DynamicWorkUnitKey implements Comparable<DynamicWorkUnitKey> {
  private final String canonical;
  private final List<String> devices;
  private final List<Long> partitions;

  public DynamicWorkUnitKey(
      String regionId, String sourceSemanticKey, List<String> devices, List<Long> partitions) {
    if (regionId == null || sourceSemanticKey == null || devices == null || partitions == null) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SCAN_GROUPING_8A0E09F6);
    }
    for (String v : devices) {
      if (v == null) {
        throw new IllegalArgumentException(
            DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SCAN_GROUPING_8A0E09F6);
      }
    }
    for (Long v : partitions) {
      if (v == null) {
        throw new IllegalArgumentException(
            DataNodeQueryMessages.EXCEPTION_INVALID_DYNAMIC_SCAN_GROUPING_8A0E09F6);
      }
    }
    this.devices = Collections.unmodifiableList(new ArrayList<>(devices));
    this.partitions = Collections.unmodifiableList(new ArrayList<>(partitions));
    StringBuilder b = new StringBuilder();
    DynamicIdentityEncoding.field(b, "IOTDB_DYNAMIC_WORK_UNIT_V1");
    DynamicIdentityEncoding.field(b, regionId);
    DynamicIdentityEncoding.field(b, sourceSemanticKey);
    DynamicIdentityEncoding.strings(b, this.devices);
    b.append("T").append(this.partitions.size()).append('|');
    for (long value : this.partitions) {
      b.append(value).append('|');
    }
    canonical = b.toString();
  }

  public List<String> getDevices() {
    return devices;
  }

  public List<Long> getTimePartitions() {
    return partitions;
  }

  public String canonicalId() {
    return canonical;
  }

  @Override
  public int compareTo(DynamicWorkUnitKey other) {
    return canonical.compareTo(other.canonical);
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof DynamicWorkUnitKey
        && canonical.equals(((DynamicWorkUnitKey) other).canonical);
  }

  @Override
  public int hashCode() {
    return canonical.hashCode();
  }
}
