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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskId;
import org.apache.iotdb.db.queryengine.statistics.snapshot.FragmentRuntimeSnapshot;
import org.apache.iotdb.db.queryengine.statistics.snapshot.RuntimeSnapshotAdapters;
import org.apache.iotdb.db.queryengine.statistics.snapshot.RuntimeSnapshotValidity;
import org.apache.iotdb.mpp.rpc.thrift.TDriverRuntimeSnapshot;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;

import org.junit.Test;
import org.mockito.InOrder;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FragmentDriverTimingSnapshotTest {
  @Test
  public void wallUsesTheCapturedEndpointsRatherThanALaterWall() throws Exception {
    for (boolean terminal : new boolean[] {false, true}) {
      IDriver driver = driver();
      when(driver.getFirstProcessNanos()).thenReturn(100L);
      when(driver.getLastProcessNanos()).thenReturn(160L);
      when(driver.getWallNanos()).thenReturn(90L);
      TFetchFragmentInstanceStatisticsResp response = capture(execution(driver), terminal);
      TDriverRuntimeSnapshot timing = response.getDriverSnapshots().get(0);
      assertEquals(100L, timing.getDriverFirstProcessNanos());
      assertEquals(160L, timing.getDriverLastProcessNanos());
      assertEquals(60L, timing.getDriverWallNanos());
      verify(driver, times(1)).getLastProcessNanos();
      verify(driver, times(1)).getFirstProcessNanos();
      verify(driver, never()).getWallNanos();
      assertAdapted(response, terminal);
    }
  }

  @Test
  public void concurrentCompletionDoesNotFillMissingLastFromNewerWall() throws Exception {
    for (boolean terminal : new boolean[] {false, true}) {
      IDriver driver = driver();
      when(driver.getFirstProcessNanos()).thenReturn(100L);
      when(driver.getLastProcessNanos()).thenReturn(-1L, 180L);
      when(driver.getWallNanos()).thenReturn(80L);
      TFetchFragmentInstanceStatisticsResp response = capture(execution(driver), terminal);
      TDriverRuntimeSnapshot timing = response.getDriverSnapshots().get(0);
      assertEquals(100L, timing.getDriverFirstProcessNanos());
      assertFalse(timing.isSetDriverLastProcessNanos());
      assertFalse(timing.isSetDriverWallNanos());
      assertTrue(timing.getValidityReasons().contains("UNSUPPORTED_FIELD:driverLastProcessNanos"));
      assertTrue(timing.getValidityReasons().contains("UNSUPPORTED_FIELD:driverWallNanos"));
      FragmentRuntimeSnapshot adapted = assertAdapted(response, terminal);
      assertNull(adapted.getDrivers().get(0).getLastProcessNanos());
      assertNull(adapted.getDrivers().get(0).getWallNanos());
      verify(driver, times(1)).getLastProcessNanos();
      verify(driver, never()).getWallNanos();
    }
  }

  @Test
  public void lastIsReadBeforeFirstDuringInitialPublication() throws Exception {
    IDriver driver = driver();
    AtomicBoolean lastObserved = new AtomicBoolean();
    when(driver.getLastProcessNanos())
        .thenAnswer(
            call -> {
              lastObserved.set(true);
              return 180L;
            });
    when(driver.getFirstProcessNanos()).thenAnswer(call -> lastObserved.get() ? 100L : -1L);
    TDriverRuntimeSnapshot timing = capture(execution(driver), false).getDriverSnapshots().get(0);
    assertEquals(100L, timing.getDriverFirstProcessNanos());
    assertEquals(180L, timing.getDriverLastProcessNanos());
    assertEquals(80L, timing.getDriverWallNanos());
    InOrder order = inOrder(driver);
    order.verify(driver).getLastProcessNanos();
    order.verify(driver).getFirstProcessNanos();
    verify(driver, never()).getWallNanos();
  }

  @Test
  public void unstartedDriverHasExplicitMissingTimingReasons() throws Exception {
    for (boolean terminal : new boolean[] {false, true}) {
      IDriver driver = driver();
      when(driver.getFirstProcessNanos()).thenReturn(-1L);
      when(driver.getLastProcessNanos()).thenReturn(-1L);
      when(driver.getWallNanos()).thenReturn(-1L);
      TFetchFragmentInstanceStatisticsResp response = capture(execution(driver), terminal);
      TDriverRuntimeSnapshot timing = response.getDriverSnapshots().get(0);
      assertFalse(timing.isSetDriverFirstProcessNanos());
      assertFalse(timing.isSetDriverLastProcessNanos());
      assertFalse(timing.isSetDriverWallNanos());
      for (String field :
          new String[] {"driverFirstProcessNanos", "driverLastProcessNanos", "driverWallNanos"}) {
        assertTrue(timing.getValidityReasons().contains("UNSUPPORTED_FIELD:" + field));
      }
      assertAdapted(response, terminal);
    }
  }

  @Test
  public void reverseClockIsReportedWithoutFabricatingAZeroInterval() throws Exception {
    IDriver driver = driver();
    when(driver.getFirstProcessNanos()).thenReturn(180L);
    when(driver.getLastProcessNanos()).thenReturn(100L);
    when(driver.getWallNanos()).thenReturn(0L);
    TFetchFragmentInstanceStatisticsResp response = capture(execution(driver), true);
    TDriverRuntimeSnapshot timing = response.getDriverSnapshots().get(0);
    assertEquals(180L, timing.getDriverFirstProcessNanos());
    assertEquals(100L, timing.getDriverLastProcessNanos());
    assertFalse(timing.isSetDriverWallNanos());
    assertTrue(timing.getValidityReasons().contains("CLOCK_INVALID:driverWallNanos"));
    assertAdapted(response, true);
  }

  @Test
  public void subsequentSamplesKeepTheirOwnIntervalAndChecksum() throws Exception {
    IDriver driver = driver();
    when(driver.getFirstProcessNanos()).thenReturn(100L);
    when(driver.getLastProcessNanos()).thenReturn(160L, 210L);
    when(driver.getWallNanos()).thenReturn(110L);
    FragmentInstanceExecution execution = execution(driver);
    TFetchFragmentInstanceStatisticsResp first = capture(execution, false);
    TFetchFragmentInstanceStatisticsResp second = capture(execution, true);
    assertEquals(60L, first.getDriverSnapshots().get(0).getDriverWallNanos());
    assertEquals(110L, second.getDriverSnapshots().get(0).getDriverWallNanos());
    assertTrue(second.getSnapshotGeneration() > first.getSnapshotGeneration());
    assertAdapted(first, false);
    assertAdapted(second, true);
    verify(driver, times(2)).getLastProcessNanos();
    verify(driver, times(2)).getFirstProcessNanos();
    verify(driver, never()).getWallNanos();
  }

  private static IDriver driver() {
    IDriver driver = mock(IDriver.class);
    FragmentInstanceId id =
        new FragmentInstanceId(new PlanFragmentId(new QueryId("timing"), 2), "0");
    when(driver.getDriverTaskId()).thenReturn(new DriverTaskId(id, 1));
    when(driver.getDriverContext()).thenReturn(new DriverContext());
    return driver;
  }

  private static FragmentInstanceExecution execution(IDriver driver) throws Exception {
    FragmentInstanceId id =
        new FragmentInstanceId(new PlanFragmentId(new QueryId("timing"), 2), "0");
    FragmentInstanceContext context = mock(FragmentInstanceContext.class);
    when(context.getId()).thenReturn(id);
    when(context.getQueryStatistics()).thenReturn(new QueryStatistics());
    FragmentInstanceStateMachine state = mock(FragmentInstanceStateMachine.class);
    when(state.getState()).thenReturn(FragmentInstanceState.FINISHED);
    Constructor<FragmentInstanceExecution> constructor =
        FragmentInstanceExecution.class.getDeclaredConstructor(
            FragmentInstanceId.class,
            FragmentInstanceContext.class,
            List.class,
            ISink.class,
            FragmentInstanceStateMachine.class,
            long.class,
            MPPDataExchangeManager.class);
    constructor.setAccessible(true);
    FragmentInstanceExecution execution =
        constructor.newInstance(
            id,
            context,
            Collections.singletonList(driver),
            mock(ISink.class),
            state,
            1000L,
            mock(MPPDataExchangeManager.class));
    execution.shouldIgnoreForStatistics = true;
    return execution;
  }

  private static TFetchFragmentInstanceStatisticsResp capture(
      FragmentInstanceExecution execution, boolean terminal) throws Exception {
    if (!terminal) {
      return execution.buildStatistics();
    }
    Field field = FragmentInstanceExecution.class.getDeclaredField("statisticsLock");
    field.setAccessible(true);
    ReentrantReadWriteLock lock = (ReentrantReadWriteLock) field.get(execution);
    lock.writeLock().lock();
    try {
      return execution.buildStatistics();
    } finally {
      lock.writeLock().unlock();
    }
  }

  private static FragmentRuntimeSnapshot assertAdapted(
      TFetchFragmentInstanceStatisticsResp response, boolean terminal) {
    FragmentRuntimeSnapshot adapted = RuntimeSnapshotAdapters.fromResponse(response);
    assertEquals(terminal, response.isIsTerminal());
    assertEquals(terminal, adapted.isTerminal());
    assertFalse(adapted.getValidity().contains(RuntimeSnapshotValidity.CHECKSUM_MISMATCH));
    return adapted;
  }
}
