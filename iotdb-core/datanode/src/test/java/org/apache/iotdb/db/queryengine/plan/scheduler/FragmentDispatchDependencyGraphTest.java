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

package org.apache.iotdb.db.queryengine.plan.scheduler;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.MultiChildrenSinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableHashPartitioningShuffleSinkNode;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class FragmentDispatchDependencyGraphTest {
  @Test
  public void sharedSourcesWaitForEveryBucketAndDispatchOnce() {
    List<FragmentInstance> graph = diamond();
    List<List<FragmentInstance>> layers = FragmentDispatchDependencyGraph.layers(graph);
    assertTrue(FragmentDispatchDependencyGraph.required(graph));
    assertEquals(3, layers.size());
    assertEquals(List.of(0), ids(layers.get(0)));
    assertEquals(32, layers.get(1).size());
    assertEquals(List.of(34, 33), ids(layers.get(2)));
    assertEquals(
        35,
        layers.stream()
            .flatMap(Collection::stream)
            .map(FragmentInstance::getId)
            .distinct()
            .count());
  }

  @Test
  public void producerCannotStartBeforeLastConsumerAcknowledges() throws Exception {
    ExecutorService workers = Executors.newFixedThreadPool(4);
    ExecutorService caller = Executors.newSingleThreadExecutor();
    CountDownLatch lastEntered = new CountDownLatch(1), release = new CountDownLatch(1);
    AtomicInteger consumers = new AtomicInteger(), producers = new AtomicInteger();
    try {
      Future<FragInstanceDispatchResult> result =
          caller.submit(
              () ->
                  FragmentDispatchDependencyGraph.dispatch(
                      diamond(),
                      workers,
                      fi -> {
                        int id = fi.getId().getFragmentId().getId();
                        if (id == 32) {
                          lastEntered.countDown();
                          try {
                            if (!release.await(10, TimeUnit.SECONDS))
                              throw new AssertionError("consumer timeout");
                          } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                          }
                        }
                        if (id > 0 && id <= 32) consumers.incrementAndGet();
                        if (id >= 33) {
                          assertEquals(32, consumers.get());
                          producers.incrementAndGet();
                        }
                        return new FragInstanceDispatchResult(true);
                      }));
      assertTrue(lastEntered.await(10, TimeUnit.SECONDS));
      assertEquals(0, producers.get());
      try {
        result.get(200, TimeUnit.MILLISECONDS);
        fail("dispatch crossed the unacknowledged consumer");
      } catch (TimeoutException expected) {
        assertEquals(0, producers.get());
      }
      release.countDown();
      assertTrue(result.get(10, TimeUnit.SECONDS).isSuccessful());
      assertEquals(2, producers.get());
    } finally {
      release.countDown();
      caller.shutdownNow();
      workers.shutdownNow();
    }
  }

  @Test
  public void consumerFailureJoinsItsPeersAndNeverStartsSources() throws Exception {
    ExecutorService workers = Executors.newFixedThreadPool(4);
    AtomicInteger consumers = new AtomicInteger(), producers = new AtomicInteger();
    TSStatus failure = new TSStatus(500);
    try {
      FragInstanceDispatchResult result =
          FragmentDispatchDependencyGraph.dispatch(
              diamond(),
              workers,
              fi -> {
                int id = fi.getId().getFragmentId().getId();
                if (id > 0 && id <= 32) consumers.incrementAndGet();
                if (id >= 33) producers.incrementAndGet();
                return id == 1
                    ? new FragInstanceDispatchResult(failure)
                    : new FragInstanceDispatchResult(true);
              });
      assertFalse(result.isSuccessful());
      assertSame(failure, result.getFailureStatus());
      assertEquals(32, consumers.get());
      assertEquals(0, producers.get());
    } finally {
      workers.shutdownNow();
    }
  }

  @Test
  public void throwingConsumerAlsoPreventsProducerDispatch() throws Exception {
    ExecutorService workers = Executors.newFixedThreadPool(4);
    AtomicInteger sources = new AtomicInteger();
    try {
      try {
        FragmentDispatchDependencyGraph.dispatch(
            diamond(),
            workers,
            fi -> {
              int id = fi.getId().getFragmentId().getId();
              if (id == 1) throw new IllegalStateException("injected dispatch failure");
              if (id >= 33) sources.incrementAndGet();
              return new FragInstanceDispatchResult(true);
            });
        fail("dispatch failure was hidden");
      } catch (ExecutionException expected) {
        assertTrue(expected.getCause() instanceof IllegalStateException);
      }
      assertEquals(0, sources.get());
    } finally {
      workers.shutdownNow();
    }
  }

  @Test
  public void interruptedDispatchCancelsItsConsumersAndDoesNotStartSources() throws Exception {
    ExecutorService workers = Executors.newFixedThreadPool(2);
    ExecutorService caller = Executors.newSingleThreadExecutor();
    CountDownLatch entered = new CountDownLatch(1), stopped = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger sources = new AtomicInteger();
    try {
      Future<?> task =
          caller.submit(
              () -> {
                try {
                  FragmentDispatchDependencyGraph.dispatch(
                      diamond(),
                      workers,
                      fi -> {
                        int id = fi.getId().getFragmentId().getId();
                        if (id >= 33) sources.incrementAndGet();
                        if (id > 0 && id <= 32) {
                          entered.countDown();
                          try {
                            release.await();
                          } catch (InterruptedException e) {
                            stopped.countDown();
                            Thread.currentThread().interrupt();
                          }
                        }
                        return new FragInstanceDispatchResult(true);
                      });
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                } catch (ExecutionException e) {
                  throw new AssertionError(e);
                }
              });
      assertTrue(entered.await(10, TimeUnit.SECONDS));
      task.cancel(true);
      assertTrue(stopped.await(10, TimeUnit.SECONDS));
      assertEquals(0, sources.get());
    } finally {
      release.countDown();
      caller.shutdownNow();
      workers.shutdownNow();
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void duplicateInstanceIsRejected() {
    FragmentInstance f = fragment(1, true, false);
    FragmentDispatchDependencyGraph.layers(List.of(f, f));
  }

  @Test(expected = IllegalArgumentException.class)
  public void missingConsumerIsRejected() {
    FragmentDispatchDependencyGraph.layers(List.of(fragment(1, false, false, 99)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void cyclicDependencyIsRejected() {
    FragmentDispatchDependencyGraph.layers(
        List.of(fragment(1, false, false, 2), fragment(2, false, false, 1)));
  }

  @Test
  public void externalResultIsAllowedOnlyForRootAndRepeatedChannelsCountOnce() {
    List<FragmentInstance> graph =
        List.of(fragment(2, false, true, 1, 1), fragment(1, true, false, 0));
    assertEquals(List.of(1), ids(FragmentDispatchDependencyGraph.layers(graph).get(0)));
    assertEquals(List.of(2), ids(FragmentDispatchDependencyGraph.layers(graph).get(1)));
    assertFalse(FragmentDispatchDependencyGraph.required(List.of(fragment(1, true, false))));
  }

  @Test(expected = IllegalArgumentException.class)
  public void unboundChannelIsRejected() {
    FragmentInstance f = fragment(1, true, false);
    ((MultiChildrenSinkNode) f.getFragment().getPlanNodeTree())
        .getDownStreamChannelLocationList()
        .add(new DownStreamChannelLocation("unbound"));
    FragmentDispatchDependencyGraph.layers(List.of(f));
  }

  private static List<Integer> ids(List<FragmentInstance> instances) {
    return instances.stream()
        .map(f -> f.getId().getFragmentId().getId())
        .collect(Collectors.toList());
  }

  private static List<FragmentInstance> diamond() {
    List<FragmentInstance> list = new ArrayList<>();
    int[] targets = java.util.stream.IntStream.rangeClosed(1, 32).toArray();
    list.add(fragment(34, false, true, targets));
    list.add(fragment(33, false, true, targets));
    for (int i = 32; i >= 1; i--) list.add(fragment(i, false, false, 0));
    list.add(fragment(0, true, false));
    return list;
  }

  private static FragmentInstance fragment(int id, boolean root, boolean hash, int... downstream) {
    PlanFragmentId fragmentId = new PlanFragmentId(new QueryId("dispatch_graph"), id);
    List<DownStreamChannelLocation> locations = new ArrayList<>();
    for (int target : downstream)
      locations.add(
          new DownStreamChannelLocation(
              new TEndPoint("127.0.0.1", 1),
              new FragmentInstanceId(new PlanFragmentId(new QueryId("dispatch_graph"), target), "0")
                  .toThrift(),
              "exchange" + target));
    MultiChildrenSinkNode sink;
    if (hash) {
      sink = Mockito.mock(TableHashPartitioningShuffleSinkNode.class);
      Mockito.when(sink.getDownStreamChannelLocationList()).thenReturn(locations);
    } else sink = new IdentitySinkNode(new PlanNodeId("sink" + id), locations);
    PlanFragment plan = new PlanFragment(fragmentId, sink);
    plan.setRoot(root);
    return new FragmentInstance(
        plan,
        new FragmentInstanceId(fragmentId, "0"),
        null,
        QueryType.READ,
        60000,
        null,
        false,
        false);
  }
}
