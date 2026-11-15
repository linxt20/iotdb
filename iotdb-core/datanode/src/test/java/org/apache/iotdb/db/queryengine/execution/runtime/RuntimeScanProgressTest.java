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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RuntimeScanProgressTest {
  @Test
  public void resolvedAndPrunedUnitsShareOneMonotonicCompletionTotal() {
    RuntimeScanProgress progress = new RuntimeScanProgress(8);
    assertEquals(new RuntimeScanProgress.Snapshot(8, 0, 0), progress.snapshot());
    progress.resolve(3, false);
    RuntimeScanProgress.Snapshot beforePruning = progress.snapshot();
    progress.resolve(4, true);
    assertEquals(new RuntimeScanProgress.Snapshot(8, 7, 4), progress.snapshot());
    progress.resolve(1, false);
    assertEquals(new RuntimeScanProgress.Snapshot(8, 8, 4), progress.snapshot());
    // A captured sample is immutable across later batches and pruning.
    assertEquals(new RuntimeScanProgress.Snapshot(8, 3, 0), beforePruning);
  }

  @Test
  public void emptyScanAndZeroDeltaDoNotInventProgress() {
    RuntimeScanProgress empty = new RuntimeScanProgress(0);
    empty.resolve(0, false);
    empty.resolve(0, true);
    assertEquals(new RuntimeScanProgress.Snapshot(0, 0, 0), empty.snapshot());
    assertThrows(IllegalArgumentException.class, () -> empty.resolve(1, false));
    assertEquals(new RuntimeScanProgress.Snapshot(0, 0, 0), empty.snapshot());
  }

  @Test
  public void rejectedUpdatesPreserveThePreviousSample() {
    RuntimeScanProgress progress = new RuntimeScanProgress(3);
    progress.resolve(2, true);
    RuntimeScanProgress.Snapshot before = progress.snapshot();
    assertThrows(IllegalArgumentException.class, () -> progress.resolve(-1, false));
    assertEquals(before, progress.snapshot());
    assertThrows(IllegalArgumentException.class, () -> progress.resolve(2, true));
    assertEquals(before, progress.snapshot());
    progress.resolve(1, false);
    assertThrows(IllegalArgumentException.class, () -> progress.resolve(1, false));
    assertEquals(new RuntimeScanProgress.Snapshot(3, 3, 2), progress.snapshot());
  }

  @Test
  public void longBoundaryCannotWrapCompletionOrPruning() {
    RuntimeScanProgress progress = new RuntimeScanProgress(Long.MAX_VALUE);
    progress.resolve(Long.MAX_VALUE - 1, true);
    progress.resolve(1, false);
    assertEquals(
        new RuntimeScanProgress.Snapshot(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE - 1),
        progress.snapshot());
    assertThrows(IllegalArgumentException.class, () -> progress.resolve(Long.MAX_VALUE, true));
    assertEquals(Long.MAX_VALUE, progress.snapshot().completed());
  }

  @Test
  public void invalidTotalsAndSnapshotsCannotEnterControlSampling() {
    assertThrows(IllegalArgumentException.class, () -> new RuntimeScanProgress(-1));
    long[][] invalid = {{-1, 0, 0}, {1, -1, 0}, {1, 2, 0}, {1, 1, -1}, {2, 1, 2}};
    for (long[] value : invalid) {
      assertThrows(
          IllegalArgumentException.class,
          () -> new RuntimeScanProgress.Snapshot(value[0], value[1], value[2]));
    }
  }

  @Test(timeout = 10000)
  public void concurrentBatchCompletionAndSamplesKeepExactTotals() throws Exception {
    int workers = 4;
    int unitsPerWorker = 256;
    RuntimeScanProgress progress = new RuntimeScanProgress(workers * unitsPerWorker);
    ExecutorService executor = Executors.newFixedThreadPool(workers);
    CountDownLatch ready = new CountDownLatch(workers);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<?>> futures = new ArrayList<>();
    try {
      for (int worker = 0; worker < workers; worker++) {
        final boolean pruned = worker % 2 == 0;
        futures.add(
            executor.submit(
                () -> {
                  ready.countDown();
                  assertTrue(start.await(5, TimeUnit.SECONDS));
                  for (int unit = 0; unit < unitsPerWorker; unit++) {
                    progress.resolve(1, pruned);
                    RuntimeScanProgress.Snapshot sample = progress.snapshot();
                    assertTrue(sample.pruned() <= sample.completed());
                    assertTrue(sample.completed() <= sample.total());
                  }
                  return null;
                }));
      }
      assertTrue(ready.await(5, TimeUnit.SECONDS));
      start.countDown();
      for (Future<?> future : futures) future.get(5, TimeUnit.SECONDS);
      assertEquals(
          new RuntimeScanProgress.Snapshot(
              workers * unitsPerWorker, workers * unitsPerWorker, 2 * unitsPerWorker),
          progress.snapshot());
    } finally {
      start.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test(timeout = 10000)
  public void concurrentOversubscriptionResolvesEachAvailableUnitOnlyOnce() throws Exception {
    RuntimeScanProgress progress = new RuntimeScanProgress(2);
    ExecutorService executor = Executors.newFixedThreadPool(4);
    CountDownLatch ready = new CountDownLatch(4);
    CountDownLatch start = new CountDownLatch(1);
    AtomicInteger accepted = new AtomicInteger();
    AtomicInteger rejected = new AtomicInteger();
    List<Future<?>> futures = new ArrayList<>();
    try {
      for (int worker = 0; worker < 4; worker++) {
        futures.add(
            executor.submit(
                () -> {
                  ready.countDown();
                  assertTrue(start.await(5, TimeUnit.SECONDS));
                  try {
                    progress.resolve(1, true);
                    accepted.incrementAndGet();
                  } catch (IllegalArgumentException expected) {
                    rejected.incrementAndGet();
                  }
                  return null;
                }));
      }
      assertTrue(ready.await(5, TimeUnit.SECONDS));
      start.countDown();
      for (Future<?> future : futures) future.get(5, TimeUnit.SECONDS);
      assertEquals(2, accepted.get());
      assertEquals(2, rejected.get());
      assertEquals(new RuntimeScanProgress.Snapshot(2, 2, 2), progress.snapshot());
    } finally {
      start.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void pruningRemainingAssignmentsIsIdempotentAndKeepsScannedUnitsDistinct() {
    RuntimeScanProgress progress = new RuntimeScanProgress(8);
    progress.resolve(2, false);
    progress.resolve(1, true);
    RuntimeScanProgress.Snapshot before = progress.snapshot();
    progress.pruneRemaining();
    progress.pruneRemaining();
    assertEquals(new RuntimeScanProgress.Snapshot(8, 8, 6), progress.snapshot());
    assertEquals(new RuntimeScanProgress.Snapshot(8, 3, 1), before);
    progress.resolve(0, false);
    assertThrows(IllegalArgumentException.class, () -> progress.resolve(1, false));
    assertEquals(new RuntimeScanProgress.Snapshot(8, 8, 6), progress.snapshot());
  }

  @Test
  public void pruningAtEmptyAndLongBoundaryDoesNotWrapOrInventAssignments() {
    RuntimeScanProgress empty = new RuntimeScanProgress(0);
    empty.pruneRemaining();
    empty.pruneRemaining();
    assertEquals(new RuntimeScanProgress.Snapshot(0, 0, 0), empty.snapshot());
    RuntimeScanProgress large = new RuntimeScanProgress(Long.MAX_VALUE);
    large.resolve(Long.MAX_VALUE - 1, true);
    large.pruneRemaining();
    large.pruneRemaining();
    assertEquals(
        new RuntimeScanProgress.Snapshot(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE),
        large.snapshot());
  }
}
