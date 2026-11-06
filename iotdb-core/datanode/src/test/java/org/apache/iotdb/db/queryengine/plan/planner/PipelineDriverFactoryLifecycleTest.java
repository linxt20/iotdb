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

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PipelineDriverFactoryLifecycleTest {

  @Test
  public void closeUncreatedIsIdempotent() throws Exception {
    Operator operation = mock(Operator.class);
    PipelineDriverFactory factory =
        new PipelineDriverFactory(operation, mock(DriverContext.class), 0);

    assertTrue(factory.closeUncreated());
    assertFalse(factory.closeUncreated());
    verify(operation, times(1)).close();
  }

  @Test(expected = IllegalStateException.class)
  public void closedFactoryCannotCreateADriver() throws Exception {
    PipelineDriverFactory factory =
        new PipelineDriverFactory(mock(Operator.class), mock(DriverContext.class), 0);
    factory.closeUncreated();
    factory.createDriver();
  }

  @Test
  public void closeWaitsForConcurrentDriverConstruction() throws Exception {
    org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext context =
        mock(org.apache.iotdb.db.queryengine.execution.driver.DataDriverContext.class);
    org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink sink =
        mock(org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink.class);
    Operator operation = mock(Operator.class);
    java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
    org.mockito.Mockito.when(context.getSink())
        .thenAnswer(
            ignored -> {
              entered.countDown();
              if (!release.await(10, java.util.concurrent.TimeUnit.SECONDS))
                throw new AssertionError("construction timeout");
              return sink;
            });
    PipelineDriverFactory factory = new PipelineDriverFactory(operation, context, 0);
    java.util.concurrent.ExecutorService pool =
        java.util.concurrent.Executors.newFixedThreadPool(2);
    try {
      java.util.concurrent.Future<?> creating = pool.submit(factory::createDriver);
      assertTrue(entered.await(10, java.util.concurrent.TimeUnit.SECONDS));
      java.util.concurrent.CountDownLatch closeStarted = new java.util.concurrent.CountDownLatch(1);
      java.util.concurrent.Future<Boolean> closing =
          pool.submit(
              () -> {
                closeStarted.countDown();
                return factory.closeUncreated();
              });
      assertTrue(closeStarted.await(10, java.util.concurrent.TimeUnit.SECONDS));
      try {
        closing.get(200, java.util.concurrent.TimeUnit.MILLISECONDS);
        org.junit.Assert.fail("close crossed an in-progress driver construction");
      } catch (java.util.concurrent.TimeoutException expected) {
        // Construction still owns the factory. Closing must remain blocked.
      }
      release.countDown();
      creating.get(10, java.util.concurrent.TimeUnit.SECONDS);
      assertFalse(closing.get(10, java.util.concurrent.TimeUnit.SECONDS));
      verify(operation, times(0)).close();
    } finally {
      release.countDown();
      pool.shutdownNow();
    }
  }
}
