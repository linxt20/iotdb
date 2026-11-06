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

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.Driver;
import org.apache.iotdb.db.queryengine.execution.exchange.ResourceAuditManager;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.memory.MemoryPool;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.schemaengine.schemaregion.ISchemaRegion;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.junit.Assert.*;

public class ResourceAuditFactoryProductionTest {
  private LocalExecutionPlanContext context(boolean data, String query, ExecutorService executor) {
    FragmentInstanceId id = new FragmentInstanceId(new PlanFragmentId(new QueryId(query), 0), "0");
    FragmentInstanceContext fragment =
        FragmentInstanceContext.createFragmentInstanceContext(
            id, new FragmentInstanceStateMachine(id, executor));
    fragment.initializeNumOfDrivers(1);
    return data
        ? new LocalExecutionPlanContext(new TypeProvider(), fragment, new DataNodeQueryContext(1))
        : new LocalExecutionPlanContext(
            new TypeProvider(), fragment, Mockito.mock(ISchemaRegion.class));
  }

  @Test
  public void dataAndSchemaRecordActualConstructionAndDestruction() throws Exception {
    ResourceAuditManager audit = ResourceAuditManager.getInstance();
    for (boolean data : new boolean[] {true, false}) {
      String query = "factory_audit_" + (data ? "data" : "schema");
      MemoryPool pool = new MemoryPool("audit", new MemoryManager(1024L), 512L);
      String window = audit.openWindow(30_000L, 1024L, 2048L);
      ExecutorService executor = MoreExecutors.newDirectExecutorService();
      try {
        LocalExecutionPlanContext context = context(data, query, executor);
        context.getDriverContext().setSink(Mockito.mock(ISink.class));
        Operator operator = Mockito.mock(Operator.class);
        context.addPipelineDriverFactory(operator, context.getDriverContext(), 0L);
        PipelineDriverFactory factory = context.getPipelineDriverFactories().get(0);
        Map<String, Object> before = audit.snapshot(window, query, query + ".0.0", pool, 2048L);
        assertEquals(1, before.get("unconsumedFactories"));
        assertEquals(0, before.get("createdDrivers"));
        Driver driver = factory.createDriver();
        Map<String, Object> running = audit.snapshot(window, query, query + ".0.0", pool, 2048L);
        assertEquals(1, running.get("createdDrivers"));
        assertEquals(1, running.get("liveDrivers"));
        assertEquals(0, running.get("unconsumedFactories"));
        assertTrue(running.get("drivers").toString().contains(driver.getDriverTaskId().toString()));
        driver.close();
        driver.close();
        Map<String, Object> closed = audit.snapshot(window, query, query + ".0.0", pool, 2048L);
        assertEquals(true, closed.get("complete"));
        assertEquals(1, closed.get("closedDrivers"));
        assertEquals(0, closed.get("liveDrivers"));
        Mockito.verify(operator, Mockito.times(1)).close();
      } finally {
        audit.closeWindow(window);
        executor.shutdownNow();
      }
    }
  }

  @Test
  public void directPipelineSinkRegistersBothEndpointAndQueue() {
    ExecutorService executor = MoreExecutors.newDirectExecutorService();
    ResourceAuditManager audit = ResourceAuditManager.getInstance();
    String window = audit.openWindow(30_000L, 1024L, 2048L);
    try {
      LocalExecutionPlanContext context = context(true, "direct_sink_audit", executor);
      org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue queue =
          Mockito.mock(org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue.class);
      Mockito.when(queue.getMemoryReservationKey())
          .thenReturn(
              LocalExecutionResourceRegistry.memoryKey(
                  context.getFragmentInstanceId(), "pipeline"));
      org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel sink =
          Mockito.mock(
              org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel.class);
      Mockito.when(sink.getSharedTsBlockQueue()).thenReturn(queue);
      context.setISink(sink);
      assertEquals(1, context.getResourceRegistry().getObjectCount());
      assertEquals("LOCAL_QUEUE", context.getResourceRegistry().snapshotEntries().get(0).kind());
      Map<String, Object> observed =
          audit.snapshot(
              window,
              "direct_sink_audit",
              context.getFragmentInstanceId().getFullId(),
              new MemoryPool("direct-sink-audit", new MemoryManager(1024L), 512L),
              2048L);
      java.util.List<?> objects = (java.util.List<?>) observed.get("objects");
      assertEquals(2, objects.size());
      java.util.Set<Object> kinds = new java.util.HashSet<>();
      for (Object object : objects) kinds.add(((Map<?, ?>) object).get("kind"));
      assertEquals(new java.util.HashSet<>(java.util.Arrays.asList("LOCAL_QUEUE", "SINK")), kinds);
      assertTrue(
          context.getResourceRegistry().snapshotEntries().stream()
              .allMatch(entry -> entry.memoryAccountKey().planNodeId().equals("pipeline")));
    } finally {
      audit.closeWindow(window);
      executor.shutdownNow();
    }
  }

  @Test
  public void failedConstructorCreatesNoPhantomDriver() throws Exception {
    ResourceAuditManager audit = ResourceAuditManager.getInstance();
    for (boolean data : new boolean[] {true, false}) {
      String query = "failed_factory_" + (data ? "data" : "schema");
      String window = audit.openWindow(30_000L, 1024L, 2048L);
      ExecutorService executor = MoreExecutors.newDirectExecutorService();
      try {
        LocalExecutionPlanContext context = context(data, query, executor);
        Operator operator = Mockito.mock(Operator.class);
        context.addPipelineDriverFactory(operator, context.getDriverContext(), 0L);
        PipelineDriverFactory factory = context.getPipelineDriverFactories().get(0);
        assertThrows(NullPointerException.class, factory::createDriver);
        MemoryPool pool = new MemoryPool("audit", new MemoryManager(1024L), 512L);
        Map<String, Object> snapshot = audit.snapshot(window, query, query + ".0.0", pool, 2048L);
        assertEquals(0, snapshot.get("createdDrivers"));
        assertEquals(0, snapshot.get("unconsumedFactories"));
        Mockito.verify(operator, Mockito.times(1)).close();
      } finally {
        audit.closeWindow(window);
        executor.shutdownNow();
      }
    }
  }
}
