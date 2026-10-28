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

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.execution.exchange.StubSink;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;

import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Covers the driver half of the morsel completion statistic.
 *
 * <p>The statistic exists so that EXPLAIN ANALYZE can show a measured per-driver completion tail next
 * to the static TsFile-size estimate the planner used to schedule the morsels. The planner marks the
 * scan context at planning time with {@code MORSEL_SCHEDULING}, and the driver supplies the
 * measurement once it has run.
 *
 * <p>What these tests pin down is the driver's side of the contract only: when the value is written,
 * when it is not, and that it is written once. The ordering requirement that makes the write visible
 * at all - it has to happen before the fragment instance converts an operator context's specifiedInfo
 * into its frozen statistics snapshot - is a property of {@code FragmentInstanceExecution} and is
 * covered by the end-to-end EXPLAIN ANALYZE run rather than here.
 */
public class DriverMorselCompletionStatisticsTest {

  private static final String MORSEL_SCHEDULING = "MORSEL_SCHEDULING";
  private static final String MORSEL_WALL_TIME = "MORSEL_DRIVER_WALL_TIME_MS";

  /** One iteration of the process loop, which is enough to stamp the first-process timestamp. */
  private static final Duration ONE_SLICE = new Duration(0, TimeUnit.MILLISECONDS);

  @Test
  public void aMorselMarkedScanContextReceivesTheWallTime() {
    DriverContext driverContext = newDriverContext();
    OperatorContext scan = addContext(driverContext, "scan", "TableScan");
    markAsMorsel(scan, "LPT_TSFILE_BYTES");
    Driver driver = newDriver(driverContext);

    driver.processFor(ONE_SLICE);
    driver.recordMorselCompletionStatistics();

    assertTrue(
        "a morsel-marked scan context must receive the wall time once the driver has processed",
        hasWallTime(scan));
    assertTrue(
        "the wall time is an elapsed duration, so it cannot be negative",
        wallTimeOf(scan) >= 0);
  }

  @Test
  public void theWallTimeIsWrittenOnce() throws InterruptedException {
    DriverContext driverContext = newDriverContext();
    OperatorContext scan = addContext(driverContext, "scan", "TableScan");
    markAsMorsel(scan, "EQUAL_PARTITION_COUNT");
    Driver driver = newDriver(driverContext);

    driver.processFor(ONE_SLICE);
    driver.recordMorselCompletionStatistics();
    long first = wallTimeOf(scan);

    // The interval being measured starts at the driver's first processing slice and does not move,
    // so a second recording of the same driver must publish the same number. Without the guard the
    // second call measures from the same origin to a later now and reports a larger tail.
    Thread.sleep(10);
    driver.recordMorselCompletionStatistics();

    assertEquals(first, wallTimeOf(scan));
  }

  @Test
  public void aDriverThatNeverProcessedWritesNothing() {
    DriverContext driverContext = newDriverContext();
    OperatorContext scan = addContext(driverContext, "scan", "TableScan");
    markAsMorsel(scan, "LPT_TSFILE_BYTES");
    Driver driver = newDriver(driverContext);

    // No processFor call, so the driver has no first-processing timestamp to measure from. Closing
    // the driver is the other call site that records, and it must not invent a duration either.
    driver.recordMorselCompletionStatistics();
    driver.close();

    assertFalse(hasWallTime(scan));
  }

  @Test
  public void aContextThatIsNotMorselMarkedWritesNothing() {
    DriverContext driverContext = newDriverContext();
    OperatorContext aggregation = addContext(driverContext, "aggregation", "Aggregation");
    Driver driver = newDriver(driverContext);

    driver.processFor(ONE_SLICE);
    driver.recordMorselCompletionStatistics();
    driver.close();

    assertFalse(
        "only a scan the planner marked as a morsel carries the statistic",
        hasWallTime(aggregation));
  }

  @Test
  public void onlyTheMorselMarkedContextReceivesTheWallTime() {
    DriverContext driverContext = newDriverContext();
    OperatorContext scan = addContext(driverContext, "scan", "TableScan");
    markAsMorsel(scan, "LPT_TSFILE_BYTES");
    OperatorContext filter = addContext(driverContext, "filter", "Filter");
    Driver driver = newDriver(driverContext);

    driver.processFor(ONE_SLICE);
    driver.recordMorselCompletionStatistics();

    assertTrue(hasWallTime(scan));
    assertFalse(hasWallTime(filter));
  }

  // ------------------------------------------------------------------ helpers

  private DriverContext newDriverContext() {
    DriverContext driverContext = new DriverContext();
    driverContext.setSink(new StubSink(Mockito.mock(FragmentInstanceContext.class)));
    return driverContext;
  }

  private OperatorContext addContext(
      DriverContext driverContext, String planNodeId, String operatorType) {
    return driverContext.addOperatorContext(0, new PlanNodeId(planNodeId), operatorType);
  }

  private static void markAsMorsel(OperatorContext context, String scheduling) {
    context.recordSpecifiedInfo(MORSEL_SCHEDULING, scheduling);
  }

  private Driver newDriver(DriverContext driverContext) {
    CommonOperatorContext rootContext =
        new OperatorContext(0, new PlanNodeId("root"), "Operator", new DriverContext());
    return new NeverFinishingDriver(new NeverFinishingOperator(rootContext), driverContext);
  }

  private static boolean hasWallTime(OperatorContext context) {
    return context.getSpecifiedInfo().containsKey(MORSEL_WALL_TIME);
  }

  private static long wallTimeOf(OperatorContext context) {
    Object value = context.getSpecifiedInfo().get(MORSEL_WALL_TIME);
    if (!(value instanceof String)) {
      fail("the wall time must be published as a decimal string, found " + value);
    }
    return Long.parseLong((String) value);
  }

  /**
   * A driver whose root never finishes, so {@code processFor} cannot reach the self-destruct path
   * that closes the driver and records on its own. That keeps each test's writer explicit.
   */
  private static final class NeverFinishingDriver extends Driver {
    private NeverFinishingDriver(Operator root, DriverContext driverContext) {
      super(root, driverContext);
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

  /** Produces no rows and never reports itself finished, so one process slice returns at once. */
  private static final class NeverFinishingOperator implements Operator {
    private final CommonOperatorContext operatorContext;

    private NeverFinishingOperator(CommonOperatorContext operatorContext) {
      this.operatorContext = operatorContext;
    }

    @Override
    public CommonOperatorContext getOperatorContext() {
      return operatorContext;
    }

    @Override
    public TsBlock next() {
      return null;
    }

    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public boolean isFinished() {
      return false;
    }

    @Override
    public void close() {}

    @Override
    public long calculateMaxPeekMemory() {
      return 0;
    }

    @Override
    public long calculateMaxReturnSize() {
      return 0;
    }

    @Override
    public long calculateRetainedSizeAfterCallingNext() {
      return 0;
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }
  }
}
