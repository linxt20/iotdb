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

/** A pure choice. It does not own execution resources or reserve memory. */
public final class DynamicParallelScheduleDecision {
  public enum Action {
    KEEP_STATIC,
    REBALANCE_NEXT_FRAGMENT,
    REDUCE_DOP,
    SERIAL_FALLBACK
  }

  private final Action action;
  private final int targetDop;
  private final DynamicParallelDecisionReason reason;

  DynamicParallelScheduleDecision(Action action, int target, DynamicParallelDecisionReason reason) {
    this.action = action;
    this.targetDop = target;
    this.reason = reason;
  }

  public Action getAction() {
    return action;
  }

  public int getTargetDop() {
    return targetDop;
  }

  public DynamicParallelDecisionReason getReason() {
    return reason;
  }
}
