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
package org.apache.iotdb.db.queryengine.plan.planner;

import org.junit.Test;

import java.util.Arrays;
import java.util.OptionalLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class StaticCostCalibratorTest {
  @Test
  public void testFiltersKeyEndpointAndValidity() {
    StaticCostCalibrator calibrator = new StaticCostCalibrator();
    OptionalLong result =
        calibrator.medianCost(
            Arrays.asList(
                new StaticCostCalibrator.HistoricalSample(
                    "s", "DEVICE", "shape", "n:1", 30, true, true),
                new StaticCostCalibrator.HistoricalSample(
                    "s", "DEVICE", "shape", "n:1", 10, true, true),
                new StaticCostCalibrator.HistoricalSample(
                    "s", "DEVICE", "shape", "n:1", 999, false, true),
                new StaticCostCalibrator.HistoricalSample(
                    "s", "DEVICE", "other", "n:1", 1, true, true)),
            "s",
            "DEVICE",
            "shape",
            "n:1");
    assertEquals(10L, result.getAsLong());
    assertFalse(
        calibrator
            .medianCost(
                Arrays.asList(
                    new StaticCostCalibrator.HistoricalSample(
                        "s", "DEVICE", "shape", "n:2", 1, true, true)),
                "s",
                "DEVICE",
                "shape",
                "n:1")
            .isPresent());
  }
}
