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
package org.apache.iotdb.db.queryengine.plan.execution;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.partition.ExecutorType;
import org.apache.iotdb.commons.partition.QueryExecutor;
import org.apache.iotdb.commons.partition.StorageExecutor;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.IPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.scheduler.IScheduler;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.nio.file.NoSuchFileException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryExecutionRetryTest {

  private ExecutorService stateMachineExecutor;

  @Before
  public void setUp() {
    stateMachineExecutor = Executors.newSingleThreadExecutor();
  }

  @After
  public void tearDown() {
    stateMachineExecutor.shutdownNow();
  }

  @Test
  public void testAnalysisAttemptIsRolledBackOnlyWhenRetryStarts() {
    RecordingPlanner planner = new RecordingPlanner(false);
    QueryExecution execution =
        new QueryExecution(planner, createQueryContext("retry_succeeds"), stateMachineExecutor);

    assertEquals(Arrays.asList("begin", "analyze"), planner.getAnalysisEvents());

    execution.start();

    assertEquals(1, planner.getScheduleAttempts());
    // PENDING_RETRY must keep the journal alive until the plans from this attempt have been
    // stopped. In particular, returning from start() must not commit or roll back the attempt.
    assertEquals(Arrays.asList("begin", "analyze"), planner.getAnalysisEvents());

    ExecutionResult result = execution.getStatus();

    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), result.status.code);
    assertEquals(2, planner.getScheduleAttempts());
    assertEquals(
        Arrays.asList("begin", "analyze", "rollback", "begin", "analyze", "commit"),
        planner.getAnalysisEvents());
  }

  @Test
  public void testAnalysisAttemptIsRolledBackWhenRetryLimitIsReached() throws Exception {
    RecordingPlanner planner = new RecordingPlanner(true);
    QueryExecution execution =
        new QueryExecution(planner, createQueryContext("retry_exhausted"), stateMachineExecutor);

    execution.start();
    assertEquals(Arrays.asList("begin", "analyze"), planner.getAnalysisEvents());

    setRetryCount(execution, Integer.MAX_VALUE);
    ExecutionResult result = execution.getStatus();

    assertEquals(TSStatusCode.DISPATCH_ERROR.getStatusCode(), result.status.code);
    assertEquals(Arrays.asList("begin", "analyze", "rollback"), planner.getAnalysisEvents());
    assertFalse(planner.getAnalysisEvents().contains("commit"));
  }

  @Test
  public void testPlanningFailureIsNotSwallowedWhileNotDone() {
    RecordingPlanner planner =
        new RecordingPlanner(false) {
          @Override
          public LogicalQueryPlan doLogicalPlan(IAnalysis analysis, MPPQueryContext context) {
            throw new IllegalStateException("planning failed");
          }
        };
    QueryExecution execution =
        new QueryExecution(planner, createQueryContext("plan_fails"), stateMachineExecutor);

    IllegalStateException thrown = assertThrows(IllegalStateException.class, execution::start);
    assertEquals("planning failed", thrown.getMessage());
  }

  @Test
  public void testCleanupFailureAfterQueryCanceledIsIgnoredDuringStart() {
    QueryExecution[] self = new QueryExecution[1];
    RecordingPlanner planner =
        new RecordingPlanner(false) {
          @Override
          public LogicalQueryPlan doLogicalPlan(IAnalysis analysis, MPPQueryContext context) {
            // Simulate KILL QUERY arriving while the planner is still fetching device schema. The
            // cancel releases the spill directory, so the next segment write fails with
            // NoSuchFileException, which must not override the recorded QUERY_WAS_KILLED status.
            self[0].cancel();
            throw new UncheckedIOException(
                new NoSuchFileException(
                    "/tmp/device-entry/cancel_during_start/1/raw/segment-000002.tmp"));
          }
        };
    QueryExecution execution =
        new QueryExecution(
            planner, createQueryContext("cancel_during_start"), stateMachineExecutor);
    self[0] = execution;

    execution.start();

    assertEquals(Arrays.asList("begin", "analyze", "rollback"), planner.getAnalysisEvents());
    ExecutionResult result = execution.getStatus();
    assertEquals(TSStatusCode.QUERY_WAS_KILLED.getStatusCode(), result.status.code);
  }

  @Test
  public void runtimeTableDataRegionResultUsesRpcForItsActualProducer() {
    FragmentInstance data =
        resultProducer("result_data", storageExecutor(TConsensusGroupType.DataRegion));
    data.setRuntimeExecutionEnabled(true);
    assertTrue(
        QueryExecution.usesRuntimeResultExchange(
            true,
            SqlDialect.TABLE,
            data.getId(),
            new DistributedQueryPlan(null, Collections.singletonList(data))));
  }

  @Test
  public void runtimeTableSchemaRegionResultRetainsLegacyQueueForItsActualProducer() {
    FragmentInstance schema =
        resultProducer("result_schema", storageExecutor(TConsensusGroupType.SchemaRegion));
    FragmentInstance data =
        resultProducer("other_data", storageExecutor(TConsensusGroupType.DataRegion));
    // A single-region SHOW DEVICE root executes in the schema context, with no runtime frontier.
    assertFalse(
        QueryExecution.usesRuntimeResultExchange(
            true,
            SqlDialect.TABLE,
            schema.getId(),
            new DistributedQueryPlan(null, Arrays.asList(data, schema))));
  }

  @Test
  public void runtimeTableQueryExecutorResultUsesRpcWithoutAStorageRegion() {
    FragmentInstance producer =
        resultProducer("result_data_merge", new QueryExecutor(new TDataNodeLocation()));
    producer.setRuntimeExecutionEnabled(true);
    assertTrue(
        QueryExecution.usesRuntimeResultExchange(
            true,
            SqlDialect.TABLE,
            producer.getId(),
            new DistributedQueryPlan(null, Collections.singletonList(producer))));
  }

  @Test
  public void disabledRuntimeAndTreeResultsKeepLegacyRoutingWithoutProducerLookup() {
    assertFalse(QueryExecution.usesRuntimeResultExchange(false, SqlDialect.TABLE, null, null));
    assertFalse(QueryExecution.usesRuntimeResultExchange(true, SqlDialect.TREE, null, null));
  }

  @Test
  public void schemaMergeQueryExecutorRootUsesItsExplicitLegacyProtocol() {
    FragmentInstance root =
        resultProducer("schema_merge", new QueryExecutor(new TDataNodeLocation()));
    FragmentInstance schema =
        resultProducer("schema_source", storageExecutor(TConsensusGroupType.SchemaRegion));
    // The whole-graph planner selected false for both, despite the root being a QueryExecutor.
    root.setRuntimeExecutionEnabled(false);
    schema.setRuntimeExecutionEnabled(false);
    assertFalse(
        QueryExecution.usesRuntimeResultExchange(
            true,
            SqlDialect.TABLE,
            root.getId(),
            new DistributedQueryPlan(null, Arrays.asList(root, schema))));
  }

  @Test
  public void dataRegionResultCannotInferRuntimeFromItsExecutorType() {
    FragmentInstance data =
        resultProducer("data_legacy", storageExecutor(TConsensusGroupType.DataRegion));
    data.setRuntimeExecutionEnabled(false);
    assertFalse(
        QueryExecution.usesRuntimeResultExchange(
            true,
            SqlDialect.TABLE,
            data.getId(),
            new DistributedQueryPlan(null, Collections.singletonList(data))));
  }

  private static StorageExecutor storageExecutor(TConsensusGroupType type) {
    return new StorageExecutor(
        new TRegionReplicaSet(
            new TConsensusGroupId(type, 1), Collections.singletonList(new TDataNodeLocation())));
  }

  private static FragmentInstance resultProducer(String queryId, ExecutorType executor) {
    PlanFragmentId fragmentId = new PlanFragmentId(queryId, 0);
    FragmentInstance producer =
        new FragmentInstance(
            new PlanFragment(fragmentId, null),
            fragmentId.genFragmentInstanceId(),
            null,
            QueryType.READ,
            1000,
            null,
            false,
            false);
    producer.setExecutorAndHost(executor);
    return producer;
  }

  private static MPPQueryContext createQueryContext(String queryId) {
    MPPQueryContext context =
        new MPPQueryContext(
            "",
            new QueryId(queryId),
            new SessionInfo(0, "test", ZoneId.systemDefault()),
            new TEndPoint(),
            new TEndPoint());
    // Keeping this as a non-query avoids constructing a result exchange handle. The fake scheduler
    // still drives the same QueryStateMachine transitions used by a retryable query dispatch.
    context.setQueryType(QueryType.OTHER);
    return context;
  }

  private static void setRetryCount(QueryExecution execution, int retryCount) throws Exception {
    Field retryCountField = QueryExecution.class.getDeclaredField("retryCount");
    retryCountField.setAccessible(true);
    retryCountField.setInt(execution, retryCount);
  }

  private static class RecordingPlanner implements IPlanner {

    private final IAnalysis analysis = mock(IAnalysis.class);
    private final IScheduler scheduler = mock(IScheduler.class);
    private final LogicalQueryPlan logicalPlan = mock(LogicalQueryPlan.class);
    private final DistributedQueryPlan distributedPlan = mock(DistributedQueryPlan.class);
    private final List<String> analysisEvents = new ArrayList<>();
    private final boolean alwaysFailDispatch;

    private int scheduleAttempts;

    private RecordingPlanner(boolean alwaysFailDispatch) {
      this.alwaysFailDispatch = alwaysFailDispatch;
      when(distributedPlan.getInstances()).thenReturn(Collections.emptyList());
    }

    @Override
    public void beginAnalysisAttempt() {
      analysisEvents.add("begin");
    }

    @Override
    public IAnalysis analyze(MPPQueryContext context) {
      analysisEvents.add("analyze");
      return analysis;
    }

    @Override
    public void rollbackAnalysisAttempt() {
      analysisEvents.add("rollback");
    }

    @Override
    public void commitAnalysisAttempt() {
      analysisEvents.add("commit");
    }

    @Override
    public LogicalQueryPlan doLogicalPlan(IAnalysis analysis, MPPQueryContext context) {
      return logicalPlan;
    }

    @Override
    public DistributedQueryPlan doDistributionPlan(
        IAnalysis analysis, LogicalQueryPlan logicalPlan, MPPQueryContext context) {
      return distributedPlan;
    }

    @Override
    public IScheduler doSchedule(
        IAnalysis analysis,
        DistributedQueryPlan distributedPlan,
        MPPQueryContext context,
        QueryStateMachine stateMachine) {
      scheduleAttempts++;
      stateMachine.transitionToDispatching();
      if (alwaysFailDispatch || scheduleAttempts == 1) {
        TSStatus dispatchFailure = RpcUtils.getStatus(TSStatusCode.DISPATCH_ERROR);
        stateMachine.transitionToPendingRetry(dispatchFailure);
      } else {
        stateMachine.transitionToRunning();
      }
      return scheduler;
    }

    @Override
    public void invalidatePartitionCache() {
      // No cache is used by this fake planner.
    }

    @Override
    public ScheduledExecutorService getScheduledExecutorService() {
      return null;
    }

    @Override
    public void setRedirectInfo(IAnalysis analysis, TEndPoint localEndPoint, TSStatus status) {
      // Redirect information is irrelevant to the retry journal lifecycle.
    }

    private List<String> getAnalysisEvents() {
      return analysisEvents;
    }

    private int getScheduleAttempts() {
      return scheduleAttempts;
    }
  }
}
