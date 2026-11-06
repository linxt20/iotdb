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

package org.apache.iotdb.db.queryengine.execution.exchange;

import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class SharedQueueQuotaAtomicityTest {
  private SharedTsBlockQueue queue() {
    return new SharedTsBlockQueue(
        new TFragmentInstanceId("quota", 0, "0"),
        "scan",
        Mockito.mock(LocalMemoryManager.class),
        MoreExecutors.newDirectExecutorService());
  }

  @Test
  public void laterLargerQuotaCannotUndoTightening() {
    SharedTsBlockQueue queue = queue();
    queue.setMaxBytesCanReserve(4L);
    queue.setMaxBytesCanReserve(8L);
    assertEquals(4L, queue.getMaxBytesCanReserve());
  }

  @Test
  public void concurrentTighteningPublishesTheMinimum() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      for (int round = 0; round < 200; round++) {
        SharedTsBlockQueue queue = queue();
        CountDownLatch start = new CountDownLatch(1);
        Future<?> a =
            executor.submit(
                () -> {
                  try {
                    start.await();
                    queue.setMaxBytesCanReserve(8L);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                  }
                });
        Future<?> b =
            executor.submit(
                () -> {
                  try {
                    start.await();
                    queue.setMaxBytesCanReserve(4L);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                  }
                });
        start.countDown();
        a.get(5, TimeUnit.SECONDS);
        b.get(5, TimeUnit.SECONDS);
        assertEquals(4L, queue.getMaxBytesCanReserve());
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void pendingReservationUsesItsOriginalConditionAfterTightening() {
    org.apache.iotdb.db.queryengine.execution.memory.MemoryPool pool =
        new org.apache.iotdb.db.queryengine.execution.memory.MemoryPool(
            "quota", new org.apache.iotdb.commons.memory.MemoryManager(1024L), 512L);
    LocalMemoryManager memory = Mockito.mock(LocalMemoryManager.class);
    Mockito.when(memory.getQueryPool()).thenReturn(pool);
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            new TFragmentInstanceId("quota_pending", 0, "0"),
            "scan",
            memory,
            MoreExecutors.newDirectExecutorService());
    queue.setMaxBytesCanReserve(512L);
    queue.allowAddingTsBlock();
    try {
      for (int i = 0; i < 4; i++)
        org.junit.Assert.assertTrue(queue.add(Utils.createMockTsBlock(128L)).isDone());
      com.google.common.util.concurrent.ListenableFuture<?> waiting =
          queue.add(Utils.createMockTsBlock(128L));
      org.junit.Assert.assertFalse(waiting.isDone());
      queue.setMaxBytesCanReserve(128L);
      queue.remove();
      org.junit.Assert.assertTrue(waiting.isDone());
      assertEquals(128L, queue.getMaxBytesCanReserve());
      int remaining = 0;
      while (!queue.isEmpty()) {
        queue.remove();
        remaining++;
      }
      assertEquals(4, remaining);
      assertEquals(0L, pool.getReservedBytes());
    } finally {
      queue.close();
    }
  }
}
