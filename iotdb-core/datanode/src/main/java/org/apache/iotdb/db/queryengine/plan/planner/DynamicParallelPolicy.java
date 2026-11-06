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

/** Immutable state held by one bounded history-key slot, never by a separate global cache. */
public final class DynamicParallelPolicy {
  private DynamicParallelPolicy() {}

  public static final int REQUIRED_OBSERVATIONS = 2;
  public static final int COOLDOWN_CHECKS = 3;

  public static final class State {
    final long checkSequence, lastObservation;
    final String keyA, keyB;
    final int sign, streak, cooldown;

    State(
        long checkSequence,
        long lastObservation,
        String keyA,
        String keyB,
        int sign,
        int streak,
        int cooldown) {
      this.checkSequence = checkSequence;
      this.lastObservation = lastObservation;
      this.keyA = keyA;
      this.keyB = keyB;
      this.sign = sign;
      this.streak = streak;
      this.cooldown = cooldown;
    }

    public static State empty() {
      return new State(0, 0, null, null, 0, 0, 0);
    }

    public State withCheckSequence(long sequence) {
      if (sequence < checkSequence) {
        throw new IllegalArgumentException(
            org.apache.iotdb.db.i18n.DataNodeQueryMessages
                .EXCEPTION_INVALID_DYNAMIC_PARALLEL_POLICY_INPUT_FAD0AEB0);
      }
      return new State(sequence, lastObservation, keyA, keyB, sign, streak, cooldown);
    }

    public long getCheckSequence() {
      return checkSequence;
    }

    public long getLastObservation() {
      return lastObservation;
    }

    public int getConsecutiveObservations() {
      return streak;
    }

    public int getCooldownRemaining() {
      return cooldown;
    }
  }
}
