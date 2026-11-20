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

import org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler.ControlPressure;
import org.apache.iotdb.db.queryengine.execution.schedule.IDriverScheduler.RuntimePressure;

import java.util.concurrent.TimeUnit;

/** An experimental, node-local hint. It neither owns worker slots nor changes the lane plan. */
public final class RuntimeSharedPressureFeedback {
  private static final long MAX_AGE = TimeUnit.MILLISECONDS.toNanos(750);
  private static final long MAX_CAPTURE_TIME = TimeUnit.MILLISECONDS.toNanos(100);

  public enum Phase {
    CONTENDED,
    RECOVERING
  }

  /** Absolute local clock and attributed counts are retained for the live experimental trace. */
  public record Advice(Phase phase, int width, RuntimePressure source) {}

  private boolean seen;
  private long lastSample;
  private int highWindows;
  private int calmWindows;
  private int cap;
  private int prePressureWidth;

  Advice observe(ControlPressure signal, long now, int current) {
    RuntimePressure source = signal == null ? null : signal.sample();
    if (source == null
        || !source.complete()
        || !source.ownQuerySeen()
        || source.own() == null
        || source.other() == null
        || signal.runtimeWorkerFragments() != 1
        || source.configuredQueryThreads() < 2
        || current < 1
        || now - source.captureFinishedNanos() < 0
        || now - source.captureFinishedNanos() > MAX_AGE
        || source.captureFinishedNanos() - source.captureStartedNanos() < 0
        || source.captureFinishedNanos() - source.captureStartedNanos() > MAX_CAPTURE_TIME) {
      reset();
      return null;
    }
    long stamp = source.captureFinishedNanos();
    if (seen && (stamp - lastSample < 0 || stamp - lastSample > MAX_AGE)) {
      reset();
    }
    if (!seen || stamp != lastSample) {
      seen = true;
      lastSample = stamp;
      int slots = source.configuredQueryThreads();
      long outside = source.other().running() + source.other().runnableReady();
      boolean high =
          source.other().runnableReady() > 0
              && source.own().running() + source.other().running() >= slots;
      highWindows = high ? Math.min(2, highWindows + 1) : 0;
      calmWindows = outside == 0 ? Math.min(3, calmWindows + 1) : 0;
      if (highWindows == 2) {
        int reserve = (int) Math.min(Math.max(1, slots / 2), outside);
        int nextCap = Math.max(1, slots - reserve);
        if (cap > 0 || nextCap < current) {
          if (cap == 0) prePressureWidth = Math.max(prePressureWidth, current);
          cap = nextCap;
        }
      } else if (calmWindows == 3) {
        cap = 0;
      }
    }
    if (cap > 0) return new Advice(Phase.CONTENDED, cap, source);
    if (prePressureWidth > 0 && calmWindows == 3 && current < prePressureWidth) {
      return new Advice(Phase.RECOVERING, prePressureWidth, source);
    }
    if (calmWindows == 3 && current >= prePressureWidth) prePressureWidth = 0;
    return null;
  }

  private void reset() {
    seen = false;
    highWindows = 0;
    calmWindows = 0;
    cap = 0;
    prePressureWidth = 0;
  }
}
