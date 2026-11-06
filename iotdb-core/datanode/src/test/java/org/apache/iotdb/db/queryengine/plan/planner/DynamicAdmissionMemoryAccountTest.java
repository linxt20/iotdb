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

import org.apache.iotdb.calc.plan.planner.memory.PipelineMemoryEstimator;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.*;
import org.apache.iotdb.db.queryengine.execution.fragment.*;

import org.junit.*;

import java.lang.reflect.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DynamicAdmissionMemoryAccountTest {
  private boolean estimation, quota;
  private int nodeId;

  @Before
  public void setup() {
    nodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    if (nodeId < 0) IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    estimation = IoTDBDescriptor.getInstance().getMemoryConfig().isEnableQueryMemoryEstimation();
    quota = IoTDBDescriptor.getInstance().getConfig().isQuotaEnable();
    IoTDBDescriptor.getInstance().getMemoryConfig().setEnableQueryMemoryEstimation(true);
    IoTDBDescriptor.getInstance().getConfig().setQuotaEnable(false);
  }

  @After
  public void restore() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(nodeId);
    IoTDBDescriptor.getInstance().getMemoryConfig().setEnableQueryMemoryEstimation(estimation);
    IoTDBDescriptor.getInstance().getConfig().setQuotaEnable(quota);
  }

  private FragmentInstanceContext context(String name) {
    FragmentInstanceId id = new FragmentInstanceId(new PlanFragmentId(new QueryId(name), 0), "0");
    FragmentInstanceContext c =
        FragmentInstanceContext.createFragmentInstanceContext(
            id, new FragmentInstanceStateMachine(id, Runnable::run), null, false, false);
    c.setDynamicScanAssignments(new DynamicScanAssignmentRegistry(true));
    return c;
  }

  private long admit(FragmentInstanceContext context, long bytes) throws Exception {
    PipelineMemoryEstimator estimator = mock(PipelineMemoryEstimator.class);
    when(estimator.getEstimatedMemoryUsageInBytes()).thenReturn(bytes);
    Method method =
        LocalExecutionPlanner.class.getDeclaredMethod(
            "checkMemory", PipelineMemoryEstimator.class, FragmentInstanceContext.class);
    method.setAccessible(true);
    return (long) method.invoke(LocalExecutionPlanner.getInstance(), estimator, context);
  }

  @Test
  public void acceptedReservationAndReleaseUseOnlyTheAdmissionAccount() throws Exception {
    FragmentInstanceContext c = context("admission_account_success");
    long before = LocalExecutionPlanner.getInstance().getFreeMemoryForOperators();
    long bytes = 1024 * 1024;
    assertEquals(bytes, admit(c, bytes));
    try {
      AdmissionMemoryAccount.Snapshot accepted = c.getAdmissionMemoryAccount().snapshot();
      assertEquals(1, accepted.getChecks());
      assertEquals(1, accepted.getAllocationAttempts());
      assertEquals(1, accepted.getReservations());
      assertEquals(bytes, accepted.getReservedBytes());
      assertEquals(0, accepted.getReleases());
      long unrelated =
          LocalExecutionPlanner.getInstance().allocateOperatorsMemoryForTest(512L, false);
      try {
        assertEquals(512L, unrelated);
        assertEquals(1, c.getAdmissionMemoryAccount().snapshot().getReservations());
        assertEquals(bytes, c.getAdmissionMemoryAccount().snapshot().getReservedBytes());
      } finally {
        if (unrelated > 0)
          LocalExecutionPlanner.getInstance().releaseToFreeMemoryForOperators(unrelated);
      }

      c.getStateMachine().finished();
      AdmissionMemoryAccount.Snapshot terminal = c.getAdmissionMemoryAccount().snapshot();
      assertEquals(1, terminal.getReleases());
      assertEquals(bytes, terminal.getReleasedBytes());
      assertEquals(before, LocalExecutionPlanner.getInstance().getFreeMemoryForOperators());
      c.setDynamicScanAssignments(null);
      assertEquals(1, c.getAdmissionMemoryAccount().snapshot().getReleases());
    } finally {
      c.getStateMachine().finished();
    }
  }

  @Test
  public void rejectionAndPriorityBypassDoNotInventReservationsOrReleases() throws Exception {
    FragmentInstanceContext rejected = context("admission_account_rejected");
    InvocationTargetException failure =
        assertThrows(InvocationTargetException.class, () -> admit(rejected, Long.MAX_VALUE));
    assertTrue(
        failure.getCause() instanceof org.apache.iotdb.calc.exception.MemoryNotEnoughException);
    AdmissionMemoryAccount.Snapshot rejectedAccount =
        rejected.getAdmissionMemoryAccount().snapshot();
    assertEquals(1, rejectedAccount.getChecks());
    assertEquals(1, rejectedAccount.getAllocationAttempts());
    assertEquals(0, rejectedAccount.getReservations());
    assertEquals(0, rejectedAccount.getReleases());
    FragmentInstanceContext priority = context("admission_account_priority");
    priority.setHighestPriority(true);
    assertEquals(0, admit(priority, Long.MAX_VALUE));
    priority.getStateMachine().finished();
    assertEquals("HIGHEST_PRIORITY_ZERO_RESERVATION", priority.getAdmissionReason());
    assertEquals(0, priority.getAdmissionMemoryAccount().snapshot().getReservedBytes());
    assertEquals(0, priority.getAdmissionMemoryAccount().snapshot().getReleases());
  }

  @Test
  public void disabledCheckIsCountedWithoutAllocating() throws Exception {
    IoTDBDescriptor.getInstance().getMemoryConfig().setEnableQueryMemoryEstimation(false);
    FragmentInstanceContext c = context("admission_account_disabled");
    assertEquals(0, admit(c, Long.MAX_VALUE));
    AdmissionMemoryAccount.Snapshot snapshot = c.getAdmissionMemoryAccount().snapshot();
    assertEquals("CHECK_DISABLED", c.getAdmissionReason());
    assertEquals(1, snapshot.getChecks());
    assertEquals(0, snapshot.getAllocationAttempts());
    assertEquals(0, snapshot.getReservations());
  }
}
