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
package org.apache.iotdb.db.queryengine.execution.driver;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;

import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DriverTerminalTimingPublicationTest {
  private static final Duration ONE_SLICE = new Duration(0, TimeUnit.NANOSECONDS);

  @Test
  public void workerFailurePublishesTimingBeforeFailureListener() throws Exception {
    DriverContext context = context(DriverContext.class);
    Operator root = root(context);
    RuntimeException expected = new RuntimeException("worker failure");
    when(root.hasNextWithTimer()).thenReturn(true);
    when(root.nextWithTimer()).thenThrow(expected);
    Driver driver = new TestDriver(root, context);
    doAnswer(
            call -> {
              assertSame(expected, call.getArgument(0));
              assertTiming(driver);
              return null;
            })
        .when(context)
        .failed(any(Throwable.class));
    try {
      driver.processFor(ONE_SLICE);
      fail("worker failure must propagate");
    } catch (RuntimeException actual) {
      assertSame(expected, actual);
      assertTiming(driver);
    } finally {
      driver.close();
    }
  }

  @Test
  public void dataInitFailurePublishesTimingBeforeFailureListener() throws Exception {
    DataDriverContext context = context(DataDriverContext.class);
    Operator root = root(context);
    RuntimeException expected = new RuntimeException("data initialization failure");
    when(context.getSourceOperators()).thenThrow(expected);
    Driver driver = new DataDriver(root, context, 0);
    doAnswer(
            call -> {
              assertSame(expected, call.getArgument(0));
              assertTiming(driver);
              return null;
            })
        .when(context)
        .failed(any(Throwable.class));
    try {
      driver.processFor(ONE_SLICE);
      fail("data initialization failure must propagate");
    } catch (RuntimeException actual) {
      assertSame(expected, actual);
      assertTiming(driver);
    } finally {
      driver.close();
    }
  }

  @Test
  public void thrownInitializationStillRecordsProcessSlice() {
    DriverContext context = context(DriverContext.class);
    RuntimeException expected = new RuntimeException("direct initialization failure");
    Driver driver =
        new TestDriver(root(context), context) {
          @Override
          protected boolean init(SettableFuture<?> blockedFuture) {
            throw expected;
          }
        };
    try {
      driver.processFor(ONE_SLICE);
      fail("initialization failure must propagate");
    } catch (RuntimeException actual) {
      assertSame(expected, actual);
      assertTiming(driver);
    } finally {
      driver.close();
    }
  }

  @Test
  public void successfulFinishPublishesTimingBeforeRootClose() throws Exception {
    DriverContext context = context(DriverContext.class);
    Operator root = root(context);
    when(root.isFinished()).thenReturn(true);
    Driver driver = new TestDriver(root, context);
    doAnswer(
            call -> {
              assertTiming(driver);
              return null;
            })
        .when(root)
        .close();
    driver.processFor(new Duration(1, TimeUnit.SECONDS));
    assertTiming(driver);
    driver.close();
  }

  @Test
  public void lockTimeoutDoesNotInventProcessCompletion() throws Exception {
    DriverContext context = context(DriverContext.class);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Driver driver =
        new TestDriver(root(context), context) {
          @Override
          protected boolean init(SettableFuture<?> blockedFuture) {
            entered.countDown();
            try {
              assertTrue(release.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
              throw new AssertionError(e);
            }
            return true;
          }
        };
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> owner = executor.submit(() -> driver.processFor(ONE_SLICE));
    try {
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      assertTrue(driver.getFirstProcessNanos() >= 0);
      assertEquals(-1, driver.getLastProcessNanos());
      driver.processFor(ONE_SLICE);
      assertEquals(
          "a failed lock acquisition is not a processed slice", -1, driver.getLastProcessNanos());
      assertEquals(-1, driver.getWallNanos());
    } finally {
      release.countDown();
      owner.get(5, TimeUnit.SECONDS);
      executor.shutdownNow();
      driver.close();
    }
  }

  @Test
  public void repeatedFailureKeepsFirstTerminalEndpoint() {
    DriverContext context = context(DriverContext.class);
    RuntimeException expected = new RuntimeException("failed within initialization");
    AtomicReference<long[]> frozen = new AtomicReference<>();
    Driver failingDriver =
        new TestDriver(root(context), context) {
          @Override
          protected boolean init(SettableFuture<?> blockedFuture) {
            failed(expected);
            throw expected;
          }
        };
    // The listener observes the actual failing driver, rather than a different started driver.
    doAnswer(
            call -> {
              assertTiming(failingDriver);
              frozen.compareAndSet(
                  null,
                  new long[] {failingDriver.getLastProcessNanos(), failingDriver.getWallNanos()});
              return null;
            })
        .when(context)
        .failed(any(Throwable.class));
    try {
      failingDriver.processFor(ONE_SLICE);
      fail("failure must propagate");
    } catch (RuntimeException actual) {
      assertSame(expected, actual);
    }
    long[] first = frozen.get();
    assertEquals(first[0], failingDriver.getLastProcessNanos());
    assertEquals(first[1], failingDriver.getWallNanos());
    failingDriver.failed(expected);
    assertEquals(first[0], failingDriver.getLastProcessNanos());
    assertEquals(first[1], failingDriver.getWallNanos());
    failingDriver.close();
  }

  @Test
  public void unstartedCancellationDoesNotInventTiming() {
    DriverContext context = context(DriverContext.class);
    Driver driver = new TestDriver(root(context), context);
    driver.close();
    assertEquals(-1, driver.getFirstProcessNanos());
    assertEquals(-1, driver.getLastProcessNanos());
    assertEquals(-1, driver.getWallNanos());
  }

  @Test
  public void cancellationAfterBlockedSliceDoesNotExtendLastProcessingTime() {
    DriverContext context = context(DriverContext.class);
    Operator root = root(context);
    SettableFuture<?> blocked = SettableFuture.create();
    doReturn(blocked).when(root).isBlocked();
    Driver driver = new TestDriver(root, context);
    driver.processFor(ONE_SLICE);
    assertTiming(driver);
    long last = driver.getLastProcessNanos();
    long wall = driver.getWallNanos();
    driver.close();
    assertEquals(last, driver.getLastProcessNanos());
    assertEquals(wall, driver.getWallNanos());
  }

  @Test
  public void retryableInitializationRecordsItsActualSlice() {
    DriverContext context = context(DriverContext.class);
    Driver driver =
        new TestDriver(root(context), context) {
          @Override
          protected boolean init(SettableFuture<?> blockedFuture) {
            return false;
          }
        };
    driver.processFor(ONE_SLICE);
    assertTiming(driver);
    driver.close();
  }

  private static void assertTiming(Driver driver) {
    assertTrue(driver.getFirstProcessNanos() >= 0);
    assertTrue(
        "timing must be published before the terminal observer",
        driver.getLastProcessNanos() >= driver.getFirstProcessNanos());
    assertEquals(
        driver.getLastProcessNanos() - driver.getFirstProcessNanos(), driver.getWallNanos());
  }

  private static <T extends DriverContext> T context(Class<T> type) {
    T context = mock(type);
    ISink sink = mock(ISink.class);
    when(context.getSink()).thenReturn(sink);
    doReturn(Operator.NOT_BLOCKED).when(sink).isFull();
    when(context.getOperatorContexts()).thenReturn(Collections.emptyList());
    when(context.getFragmentInstanceContext()).thenReturn(mock(FragmentInstanceContext.class));
    return context;
  }

  private static Operator root(DriverContext context) {
    Operator root = mock(Operator.class);
    doReturn(Operator.NOT_BLOCKED).when(root).isBlocked();
    when(root.getOperatorContext())
        .thenReturn(new OperatorContext(0, new PlanNodeId("timing-root"), "TimingRoot", context));
    return root;
  }

  private static class TestDriver extends Driver {
    private TestDriver(Operator root, DriverContext context) {
      super(root, context);
    }

    @Override
    protected boolean init(SettableFuture<?> blockedFuture) {
      return true;
    }

    @Override
    protected void releaseResource() {}

    @Override
    boolean isInit() {
      return true;
    }
  }
}
