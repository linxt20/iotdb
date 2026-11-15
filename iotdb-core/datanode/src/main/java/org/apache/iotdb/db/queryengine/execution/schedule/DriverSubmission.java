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

import org.apache.iotdb.db.queryengine.execution.driver.IDriver;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Objects;

/** A native member and the original Exchange dependency captured by its caller. */
public record DriverSubmission(
    IDriver driver,
    ListenableFuture<?> originalDependency,
    Role role,
    long memberMemoryQuotaBytes) {
  public enum Role {
    DIRECT_NATIVE,
    SCAN_WORKER,
    LEGACY_NATIVE
  }

  public DriverSubmission {
    Objects.requireNonNull(driver);
    Objects.requireNonNull(originalDependency);
    Objects.requireNonNull(role);
    if (memberMemoryQuotaBytes < 0 || (role != Role.SCAN_WORKER && memberMemoryQuotaBytes != 0)) {
      throw new IllegalArgumentException();
    }
  }
}
