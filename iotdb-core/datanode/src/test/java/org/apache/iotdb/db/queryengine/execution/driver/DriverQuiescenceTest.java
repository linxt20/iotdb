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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DriverQuiescenceTest {
  private static final Duration SLICE = new Duration(0, TimeUnit.NANOSECONDS);

  @Test
  public void unstartedDriverStopsWithoutClosingOrInventingStatistics() throws Exception {
    Fixture f = new Fixture();
    assertTrue(f.driver.supportsQuiescence());
    f.driver.requestQuiescence().get(5, TimeUnit.SECONDS);
    f.driver.processFor(SLICE);
    assertTrue(f.driver.isFinished());
    assertEquals(-1, f.driver.getFirstProcessNanos());
    assertEquals(0, f.initializations);
    verify(f.root, never()).close();
    verify(f.context, never()).finished();
    f.driver.close();
    verify(f.root).close();
  }

  @Test
  public void activeQuantumMustExitBeforeAcknowledgement() throws Exception {
    Fixture f = new Fixture();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    f.init =
        () -> {
          entered.countDown();
          await(release);
        };
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> task = executor.submit(() -> f.driver.processFor(SLICE));
    try {
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      ListenableFuture<Void> stopped = f.driver.requestQuiescence();
      assertFalse(stopped.isDone());
      verify(f.root, never()).close();
      release.countDown();
      task.get(5, TimeUnit.SECONDS);
      stopped.get(5, TimeUnit.SECONDS);
      assertTrue(f.driver.getLastProcessNanos() >= f.driver.getFirstProcessNanos());
      verify(f.root, never()).hasNextWithTimer();
      long last = f.driver.getLastProcessNanos();
      f.driver.processFor(SLICE);
      assertEquals(last, f.driver.getLastProcessNanos());
    } finally {
      release.countDown();
      executor.shutdownNow();
      f.driver.close();
    }
  }

  @Test
  public void selfRequestNotifiesOnlyOutsideDriverLock() throws Exception {
    Fixture f = new Fixture();
    AtomicReference<ListenableFuture<Void>> stopped = new AtomicReference<>();
    AtomicBoolean callbackRan = new AtomicBoolean();
    AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
    f.init =
        () -> {
          ListenableFuture<Void> future = f.driver.requestQuiescence();
          stopped.set(future);
          assertFalse(future.isDone());
          future.addListener(
              () -> {
                try {
                  f.driver.close();
                  callbackRan.set(true);
                } catch (Throwable t) {
                  callbackFailure.set(t);
                }
              },
              MoreExecutors.directExecutor());
        };
    f.driver.processFor(SLICE);
    stopped.get().get(5, TimeUnit.SECONDS);
    assertNull(callbackFailure.get());
    assertTrue(callbackRan.get());
    verify(f.root).close();
  }

  @Test
  public void blockedDriverWakesWithoutCancellingSource() throws Exception {
    Fixture f = new Fixture();
    SettableFuture<Void> source = SettableFuture.create();
    doReturn(source).when(f.root).isBlocked();
    ListenableFuture<?> nativeBlocked = f.driver.processFor(SLICE);
    assertFalse(nativeBlocked.isDone());
    f.driver.requestQuiescence().get(5, TimeUnit.SECONDS);
    assertTrue(nativeBlocked.isDone());
    assertFalse(source.isDone());
    verify(f.root, never()).close();
    f.driver.close();
  }

  @Test
  public void cancellationOfObserverDoesNotCancelSharedAcknowledgement() throws Exception {
    Fixture f = new Fixture();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    f.init =
        () -> {
          entered.countDown();
          await(release);
        };
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> task = executor.submit(() -> f.driver.processFor(SLICE));
    try {
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      ListenableFuture<Void> observer = f.driver.requestQuiescence();
      assertTrue(observer.cancel(false));
      ListenableFuture<Void> secondObserver = f.driver.requestQuiescence();
      assertFalse(secondObserver.isDone());
      assertFalse(secondObserver.isCancelled());
      release.countDown();
      task.get(5, TimeUnit.SECONDS);
      secondObserver.get(5, TimeUnit.SECONDS);
    } finally {
      release.countDown();
      executor.shutdownNow();
      f.driver.close();
    }
  }

  @Test
  public void processingFailureStillAcknowledgesAfterUnwind() throws Exception {
    Fixture f = new Fixture();
    RuntimeException expected = new RuntimeException("quantum failure");
    AtomicReference<ListenableFuture<Void>> stopped = new AtomicReference<>();
    f.init =
        () -> {
          stopped.set(f.driver.requestQuiescence());
          throw expected;
        };
    try {
      f.driver.processFor(SLICE);
      fail("the failure must remain observable");
    } catch (RuntimeException actual) {
      assertSame(expected, actual);
    }
    stopped.get().get(5, TimeUnit.SECONDS);
    verify(f.root, never()).close();
    f.driver.close();
  }

  @Test
  public void closeAlreadyInProgressFinishesBeforeAcknowledgement() throws Exception {
    Fixture f = new Fixture();
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    doAnswer(
            call -> {
              entered.countDown();
              await(release);
              return null;
            })
        .when(f.root)
        .close();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> task = executor.submit(f.driver::close);
    try {
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      ListenableFuture<Void> stopped = f.driver.requestQuiescence();
      assertFalse(stopped.isDone());
      release.countDown();
      task.get(5, TimeUnit.SECONDS);
      stopped.get(5, TimeUnit.SECONDS);
      verify(f.context).finished();
    } finally {
      release.countDown();
      executor.shutdownNow();
      f.driver.close();
    }
  }

  @Test
  public void pendingCloseFailureCompletesAllObserversExceptionally() throws Exception {
    Fixture f = new Fixture();
    AssertionError expected = new AssertionError("cleanup failed");
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    doAnswer(
            call -> {
              entered.countDown();
              await(release);
              throw expected;
            })
        .when(f.root)
        .close();
    // This is the valid state between close's CAS and its attempt to obtain the driver lock.
    f.driver.state.set(Driver.State.NEED_DESTRUCTION);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<ListenableFuture<Void>> first = executor.submit(f.driver::requestQuiescence);
    try {
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      ListenableFuture<Void> second = f.driver.requestQuiescence();
      assertFalse(second.isDone());
      release.countDown();
      ListenableFuture<Void> firstObserver = first.get(5, TimeUnit.SECONDS);
      for (ListenableFuture<Void> observer : java.util.Arrays.asList(firstObserver, second)) {
        try {
          observer.get(5, TimeUnit.SECONDS);
          fail("cleanup failure must complete the acknowledgement exceptionally");
        } catch (java.util.concurrent.ExecutionException e) {
          assertSame(expected, e.getCause());
        }
      }
      verify(f.root).close();
    } finally {
      release.countDown();
      executor.shutdownNow();
      f.driver.close();
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static class Fixture {
    final DriverContext context = mock(DriverContext.class);
    final ISink sink = mock(ISink.class);
    final Operator root = mock(Operator.class);
    final Driver driver;
    Runnable init = () -> {};
    int initializations;

    Fixture() {
      when(context.getSink()).thenReturn(sink);
      doReturn(Operator.NOT_BLOCKED).when(sink).isFull();
      doReturn(Operator.NOT_BLOCKED).when(root).isBlocked();
      when(context.getOperatorContexts()).thenReturn(Collections.emptyList());
      when(context.getFragmentInstanceContext()).thenReturn(mock(FragmentInstanceContext.class));
      when(root.getOperatorContext())
          .thenReturn(
              new OperatorContext(0, new PlanNodeId("quiescence"), "QuiescenceRoot", context));
      driver =
          new Driver(root, context) {
            @Override
            protected boolean init(SettableFuture<?> blocked) {
              initializations++;
              init.run();
              return true;
            }

            @Override
            protected void releaseResource() {}

            @Override
            boolean isInit() {
              return true;
            }
          };
    }
  }
}
