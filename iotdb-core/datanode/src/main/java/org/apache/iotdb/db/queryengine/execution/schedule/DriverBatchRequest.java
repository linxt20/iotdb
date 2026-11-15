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

import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.common.QueryId;

import java.util.List;
import java.util.Objects;

/** Explicit dependency requests; the deadline is already absolute. */
public record DriverBatchRequest(
    QueryId queryId,
    List<DriverSubmission> submissions,
    long originalAbsoluteDeadlineMillis,
    SessionInfo sessionInfo) {
  public DriverBatchRequest {
    Objects.requireNonNull(queryId);
    submissions = List.copyOf(submissions);
  }
}
