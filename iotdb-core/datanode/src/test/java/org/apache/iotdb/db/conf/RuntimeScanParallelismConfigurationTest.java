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

package org.apache.iotdb.db.conf;

import org.junit.Test;

import static org.junit.Assert.*;

public class RuntimeScanParallelismConfigurationTest {
  @Test
  public void defaultPreservesAdaptiveWorkerSelection() {
    assertEquals(0, new IoTDBConfig().getRuntimeScanFixedWorkers());
  }

  @Test
  public void sharedFeedbackDefaultsOffAndInitialWidthDoesNotChangeFixedWidth() {
    IoTDBConfig config = new IoTDBConfig();
    assertFalse(config.isEnableRuntimeScanSharedPressureFeedback());
    assertEquals(0, config.getRuntimeScanInitialWorkers());
    config.setRuntimeScanInitialWorkers(8);
    config.setRuntimeScanFixedWorkers(4);
    config.setEnableRuntimeScanSharedPressureFeedback(true);
    assertTrue(config.isEnableRuntimeScanSharedPressureFeedback());
    assertEquals(8, config.getRuntimeScanInitialWorkers());
    assertEquals(4, config.getRuntimeScanFixedWorkers());
    assertThrows(IllegalArgumentException.class, () -> config.setRuntimeScanInitialWorkers(1025));
    assertEquals(8, config.getRuntimeScanInitialWorkers());
  }

  @Test
  public void ablationModesRejectAmbiguousOverrides() {
    IoTDBConfig config = new IoTDBConfig();
    assertEquals(IoTDBConfig.RuntimeScanAblationMode.STANDARD, config.getRuntimeScanAblationMode());
    config.setEnableRuntimeScanParallelism(true);
    assertThrows(
        IllegalArgumentException.class, () -> config.setRuntimeScanAblationMode("runtime_only"));
    config.setRuntimeScanInitialWorkers(4);
    config.setRuntimeScanAblationMode("runtime_only");
    assertEquals(
        IoTDBConfig.RuntimeScanAblationMode.RUNTIME_ONLY, config.getRuntimeScanAblationMode());
    assertThrows(
        IllegalArgumentException.class, () -> config.setRuntimeScanAblationMode("static_only"));
    config.setRuntimeScanInitialWorkers(0);
    config.setRuntimeScanFixedWorkers(2);
    assertThrows(
        IllegalArgumentException.class, () -> config.setRuntimeScanAblationMode("static_only"));
    config.setRuntimeScanFixedWorkers(0);
    config.setRuntimeScanAblationMode("static_only");
    assertEquals(
        IoTDBConfig.RuntimeScanAblationMode.STATIC_ONLY, config.getRuntimeScanAblationMode());
  }

  @Test
  public void configuredFixedWidthsIncludeBothSupportedBounds() {
    IoTDBConfig config = new IoTDBConfig();
    for (int width : new int[] {1, 2, 4, 8, 1024, 0}) {
      config.setRuntimeScanFixedWorkers(width);
      assertEquals(width, config.getRuntimeScanFixedWorkers());
    }
  }

  @Test
  public void invalidWidthIsRejectedWithoutChangingThePreviousValue() {
    IoTDBConfig config = new IoTDBConfig();
    config.setRuntimeScanFixedWorkers(4);
    assertThrows(IllegalArgumentException.class, () -> config.setRuntimeScanFixedWorkers(-1));
    assertThrows(IllegalArgumentException.class, () -> config.setRuntimeScanFixedWorkers(1025));
    assertEquals(4, config.getRuntimeScanFixedWorkers());
  }
}
