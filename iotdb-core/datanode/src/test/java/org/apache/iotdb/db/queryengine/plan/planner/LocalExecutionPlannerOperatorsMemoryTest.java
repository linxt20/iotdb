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

import org.apache.iotdb.calc.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.memory.NotThreadSafeMemoryReservationManager;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class LocalExecutionPlannerOperatorsMemoryTest {

  private static final LocalExecutionPlanner PLANNER = LocalExecutionPlanner.getInstance();

  private long bytesHeldByTest = 0L;

  @After
  public void tearDown() {
    if (bytesHeldByTest > 0) {
      PLANNER.releaseToFreeMemoryForOperators(bytesHeldByTest);
      bytesHeldByTest = 0L;
    }
  }

  @Test
  public void testAllocateOperatorsMemoryFailsWhenInsufficient() {
    long free = PLANNER.getFreeMemoryForOperators();
    Assert.assertEquals(-1L, PLANNER.allocateOperatorsMemoryForTest(free + 1024L, false));
  }

  @Test
  public void testAllocateOperatorsMemorySucceedsWhenAvailable() {
    long request = Math.min(1024L, PLANNER.getFreeMemoryForOperators());
    long reserved = PLANNER.allocateOperatorsMemoryForTest(request, false);
    Assert.assertEquals(request, reserved);
    bytesHeldByTest = reserved;
  }

  @Test
  public void testHighestPriorityAllocatesWhenPoolHasRoom() {
    long request = Math.min(1024L, PLANNER.getFreeMemoryForOperators());
    if (request <= 0) {
      return;
    }
    long freeBefore = PLANNER.getFreeMemoryForOperators();

    long reserved = PLANNER.allocateOperatorsMemoryForTest(request, true);
    Assert.assertEquals(request, reserved);
    Assert.assertEquals(freeBefore - request, PLANNER.getFreeMemoryForOperators());
    bytesHeldByTest = reserved;
  }

  @Test
  public void testHighestPriorityFallbackWhenPoolInsufficient() {
    long freeBefore = PLANNER.getFreeMemoryForOperators();
    long request = freeBefore + 1024L;

    Assert.assertEquals(0L, PLANNER.allocateOperatorsMemoryForTest(request, true));
    Assert.assertEquals(freeBefore, PLANNER.getFreeMemoryForOperators());
  }

  @Test
  public void testMemoryReservationManagerHighestPriorityAllocatesWhenPoolHasRoom() {
    long request = Math.min(1024L, PLANNER.getFreeMemoryForOperators());
    if (request <= 0) {
      return;
    }

    NotThreadSafeMemoryReservationManager manager =
        new NotThreadSafeMemoryReservationManager(new QueryId("show_queries"), "test");
    manager.setHighestPriority(true);
    long freeBefore = PLANNER.getFreeMemoryForOperators();

    manager.reserveMemoryImmediately(request);
    Assert.assertEquals(request, manager.getReservedBytesInTotalForTest());
    Assert.assertEquals(freeBefore - request, PLANNER.getFreeMemoryForOperators());

    manager.releaseAllReservedMemory();
    Assert.assertEquals(0L, manager.getReservedBytesInTotalForTest());
    Assert.assertEquals(freeBefore, PLANNER.getFreeMemoryForOperators());
  }

  @Test
  public void testMemoryReservationManagerHighestPriorityFallbackWhenPoolInsufficient() {
    long freeBefore = PLANNER.getFreeMemoryForOperators();
    long request = freeBefore + 1024L;

    NotThreadSafeMemoryReservationManager manager =
        new NotThreadSafeMemoryReservationManager(new QueryId("show_queries"), "test");
    manager.setHighestPriority(true);

    manager.reserveMemoryImmediately(request);
    Assert.assertEquals(0L, manager.getReservedBytesInTotalForTest());
    Assert.assertEquals(request, manager.getFallbackBytesInTotalForTest());
    Assert.assertEquals(freeBefore, PLANNER.getFreeMemoryForOperators());

    manager.releaseMemoryCumulatively(request);
    Assert.assertEquals(0L, manager.getFallbackBytesInTotalForTest());
    Assert.assertEquals(freeBefore, PLANNER.getFreeMemoryForOperators());

    manager.releaseAllReservedMemory();
    Assert.assertEquals(0L, manager.getFallbackBytesInTotalForTest());
    Assert.assertEquals(freeBefore, PLANNER.getFreeMemoryForOperators());
  }

  @Test
  public void testMemoryReservationManagerHighestPriorityFallbackReleaseViaBatchThreshold() {
    long freeBefore = PLANNER.getFreeMemoryForOperators();
    long request = freeBefore + MEMORY_BATCH_THRESHOLD;

    NotThreadSafeMemoryReservationManager manager =
        new NotThreadSafeMemoryReservationManager(new QueryId("show_queries"), "test");
    manager.setHighestPriority(true);

    manager.reserveMemoryImmediately(request);
    Assert.assertEquals(0L, manager.getReservedBytesInTotalForTest());
    Assert.assertEquals(request, manager.getFallbackBytesInTotalForTest());

    manager.releaseMemoryCumulatively(request);
    Assert.assertEquals(0L, manager.getFallbackBytesInTotalForTest());
    Assert.assertEquals(0L, manager.getReservedBytesInTotalForTest());
    Assert.assertEquals(freeBefore, PLANNER.getFreeMemoryForOperators());
  }

  private static final long MEMORY_BATCH_THRESHOLD = 1024L * 1024L;

  @Test
  public void testMemoryReservationManagerNormalPriorityReserveAndRelease() {
    long request = Math.min(1024L, PLANNER.getFreeMemoryForOperators());
    if (request <= 0) {
      return;
    }

    NotThreadSafeMemoryReservationManager manager =
        new NotThreadSafeMemoryReservationManager(new QueryId("normal_query"), "test");
    long freeBefore = PLANNER.getFreeMemoryForOperators();

    manager.reserveMemoryImmediately(request);
    Assert.assertEquals(request, manager.getReservedBytesInTotalForTest());
    Assert.assertEquals(freeBefore - request, PLANNER.getFreeMemoryForOperators());

    manager.releaseAllReservedMemory();
    Assert.assertEquals(0L, manager.getReservedBytesInTotalForTest());
    Assert.assertEquals(freeBefore, PLANNER.getFreeMemoryForOperators());
  }

  @Test
  public void testImmediateReservationRollback() {
    long request = Math.min(1024L, PLANNER.getFreeMemoryForOperators());
    if (request <= 0) {
      return;
    }

    NotThreadSafeMemoryReservationManager manager =
        new NotThreadSafeMemoryReservationManager(new QueryId("normal_query"), "test");
    long freeBefore = PLANNER.getFreeMemoryForOperators();

    manager.reserveMemoryCumulatively(request);
    manager.releaseMemoryImmediately(request);
    manager.reserveMemoryImmediately();

    Assert.assertEquals(0L, manager.getReservedBytesInTotalForTest());
    Assert.assertEquals(freeBefore, PLANNER.getFreeMemoryForOperators());

    manager.reserveMemoryImmediately(request);
    manager.releaseMemoryImmediately(request);

    Assert.assertEquals(0L, manager.getReservedBytesInTotalForTest());
    Assert.assertEquals(freeBefore, PLANNER.getFreeMemoryForOperators());
  }

  @Test
  public void testFailedCumulativeReservationDoesNotRemainPending() {
    long freeBefore = PLANNER.getFreeMemoryForOperators();
    long request = freeBefore + MEMORY_BATCH_THRESHOLD;
    NotThreadSafeMemoryReservationManager manager =
        new NotThreadSafeMemoryReservationManager(new QueryId("normal_query"), "test");

    try {
      manager.reserveMemoryCumulatively(request);
      Assert.fail("Expected insufficient query memory");
    } catch (MemoryNotEnoughException expected) {
      // expected
    }

    // A stale pending reservation would make this retry fail again.
    manager.reserveMemoryImmediately();
    Assert.assertEquals(0L, manager.getReservedBytesInTotalForTest());
    Assert.assertEquals(freeBefore, PLANNER.getFreeMemoryForOperators());
  }

  @Test
  public void memoryChecksDisabledRecordAnExplicitReason() throws Exception {
    org.apache.iotdb.db.conf.IoTDBDescriptor descriptor =
        org.apache.iotdb.db.conf.IoTDBDescriptor.getInstance();
    boolean estimate = descriptor.getMemoryConfig().isEnableQueryMemoryEstimation();
    boolean quota = descriptor.getConfig().isQuotaEnable();
    org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext context =
        org.mockito.Mockito.mock(
            org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.class);
    try {
      descriptor.getMemoryConfig().setEnableQueryMemoryEstimation(false);
      descriptor.getConfig().setQuotaEnable(false);
      Assert.assertEquals(0L, invokeCheckMemory(context, 1024));
      org.mockito.Mockito.verify(context).setAdmissionAccepted("CHECK_DISABLED");
    } finally {
      descriptor.getMemoryConfig().setEnableQueryMemoryEstimation(estimate);
      descriptor.getConfig().setQuotaEnable(quota);
    }
  }

  @Test
  public void highestPriorityZeroReservationIsRecordedAndDoesNotConsumeMemory() throws Exception {
    org.apache.iotdb.db.conf.IoTDBDescriptor descriptor =
        org.apache.iotdb.db.conf.IoTDBDescriptor.getInstance();
    boolean estimate = descriptor.getMemoryConfig().isEnableQueryMemoryEstimation();
    int dataNodeId = descriptor.getConfig().getDataNodeId();
    org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext context =
        org.mockito.Mockito.mock(
            org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.class);
    org.mockito.Mockito.when(context.isHighestPriority()).thenReturn(true);
    long free = PLANNER.getFreeMemoryForOperators();
    try {
      descriptor.getConfig().setDataNodeId(0);
      descriptor.getMemoryConfig().setEnableQueryMemoryEstimation(true);
      Assert.assertEquals(0L, invokeCheckMemory(context, free + 1024));
      org.mockito.Mockito.verify(context).setAdmissionAccepted("HIGHEST_PRIORITY_ZERO_RESERVATION");
      Assert.assertEquals(free, PLANNER.getFreeMemoryForOperators());
    } finally {
      descriptor.getMemoryConfig().setEnableQueryMemoryEstimation(estimate);
      descriptor.getConfig().setDataNodeId(dataNodeId);
    }
  }

  private long invokeCheckMemory(
      org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext context,
      long estimate)
      throws Exception {
    org.apache.iotdb.calc.plan.planner.memory.PipelineMemoryEstimator estimator =
        org.mockito.Mockito.mock(
            org.apache.iotdb.calc.plan.planner.memory.PipelineMemoryEstimator.class);
    org.mockito.Mockito.when(estimator.getEstimatedMemoryUsageInBytes()).thenReturn(estimate);
    java.lang.reflect.Method method =
        LocalExecutionPlanner.class.getDeclaredMethod(
            "checkMemory",
            org.apache.iotdb.calc.plan.planner.memory.PipelineMemoryEstimator.class,
            org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.class);
    method.setAccessible(true);
    return (Long) method.invoke(PLANNER, estimator, context);
  }
}
