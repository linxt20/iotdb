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

import java.util.Objects;

/** A driver task label is only unique within its enclosing query/fragment/pipeline. */
public final class RuntimeSnapshotDriverKey {
  private final String queryId, fragmentInstanceId, pipelineId, taskId;

  public RuntimeSnapshotDriverKey(
      String queryId, String fragmentInstanceId, String pipelineId, String taskId) {
    this.queryId = Objects.requireNonNull(queryId);
    this.fragmentInstanceId = Objects.requireNonNull(fragmentInstanceId);
    this.pipelineId = Objects.requireNonNull(pipelineId);
    this.taskId = Objects.requireNonNull(taskId);
  }

  public String getQueryId() {
    return queryId;
  }

  public String getFragmentInstanceId() {
    return fragmentInstanceId;
  }

  public String getPipelineId() {
    return pipelineId;
  }

  public String getTaskId() {
    return taskId;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof RuntimeSnapshotDriverKey)) return false;
    RuntimeSnapshotDriverKey that = (RuntimeSnapshotDriverKey) other;
    return queryId.equals(that.queryId)
        && fragmentInstanceId.equals(that.fragmentInstanceId)
        && pipelineId.equals(that.pipelineId)
        && taskId.equals(that.taskId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(queryId, fragmentInstanceId, pipelineId, taskId);
  }
}
