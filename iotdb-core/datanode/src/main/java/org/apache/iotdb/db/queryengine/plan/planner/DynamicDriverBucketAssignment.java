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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** Immutable source-local bucket ownership; actual driver identity is its enclosing snapshot. */
public final class DynamicDriverBucketAssignment {
  private final String sourcePlanNodeId;
  private final int bucketId;
  private final List<String> workUnitIds;

  public DynamicDriverBucketAssignment(
      String sourcePlanNodeId, int bucketId, List<String> workUnitIds) {
    if (sourcePlanNodeId == null
        || sourcePlanNodeId.isEmpty()
        || bucketId < 0
        || workUnitIds == null) {
      throw new IllegalArgumentException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_INVALID_DYNAMIC_SCAN_GROUPING_8A0E09F6);
    }
    for (String id : workUnitIds) {
      if (id == null || id.isEmpty()) {
        throw new IllegalArgumentException(
            org.apache.iotdb.db.i18n.DataNodeQueryMessages
                .EXCEPTION_INVALID_DYNAMIC_SCAN_GROUPING_8A0E09F6);
      }
    }
    if (new HashSet<>(workUnitIds).size() != workUnitIds.size()) {
      throw new IllegalArgumentException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_INVALID_DYNAMIC_SCAN_GROUPING_8A0E09F6);
    }
    this.sourcePlanNodeId = sourcePlanNodeId;
    this.bucketId = bucketId;
    this.workUnitIds = Collections.unmodifiableList(new ArrayList<>(workUnitIds));
  }

  public String getSourcePlanNodeId() {
    return sourcePlanNodeId;
  }

  public int getBucketId() {
    return bucketId;
  }

  public List<String> getWorkUnitIds() {
    return workUnitIds;
  }

  @Override
  public boolean equals(Object value) {
    if (!(value instanceof DynamicDriverBucketAssignment)) {
      return false;
    }
    DynamicDriverBucketAssignment v = (DynamicDriverBucketAssignment) value;
    return bucketId == v.bucketId
        && sourcePlanNodeId.equals(v.sourcePlanNodeId)
        && workUnitIds.equals(v.workUnitIds);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourcePlanNodeId, bucketId, workUnitIds);
  }
}
