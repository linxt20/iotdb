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

package org.apache.iotdb.db.queryengine.execution.runtime;

import org.apache.iotdb.db.queryengine.execution.driver.IDriver;

/** Exclusive permission for one worker to invoke one logical lane for a single quantum. */
public final class RuntimeLaneLease {
  private final Object owner;
  private final IDriver driver;
  private final int laneIndex;
  private final long workerId;
  private final long generation;

  RuntimeLaneLease(Object owner, IDriver driver, int laneIndex, long workerId, long generation) {
    this.owner = owner;
    this.driver = driver;
    this.laneIndex = laneIndex;
    this.workerId = workerId;
    this.generation = generation;
  }

  boolean belongsTo(Object expectedOwner) {
    return owner == expectedOwner;
  }

  public IDriver getDriver() {
    return driver;
  }

  public int getLaneIndex() {
    return laneIndex;
  }

  public long getWorkerId() {
    return workerId;
  }

  public long getGeneration() {
    return generation;
  }
}
