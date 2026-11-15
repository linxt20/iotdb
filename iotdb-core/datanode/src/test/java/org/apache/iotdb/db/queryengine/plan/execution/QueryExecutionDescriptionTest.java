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
import org.apache.iotdb.commons.partition.QueryExecutor;
import org.apache.iotdb.commons.partition.StorageExecutor;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.QueryStateMachine;
import org.apache.iotdb.db.queryengine.execution.exchange.ResourceAuditManager;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.IPlanner;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class QueryExecutionDescriptionTest {
  private static final AtomicLong IDS = new AtomicLong(Long.MAX_VALUE - 100_000);
  private ConcurrentHashMap<Long, IQueryExecution> executions;
  private ExecutorService notifications;
  private long rpcId;
  private long otherRpcId;
  private String sql;
  private String auditWindow;
  private boolean previousRuntimeEnabled;
  private boolean runtimeConfigurationSaved;

  @BeforeClass
  public static void initializeDataNodeIdentity() {
    // Coordinator constructs its QueryIdGenerator during static initialization.
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
  }

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    Field field = Coordinator.class.getDeclaredField("queryExecutionMap");
    field.setAccessible(true);
    executions = (ConcurrentHashMap<Long, IQueryExecution>) field.get(Coordinator.getInstance());
    rpcId = IDS.getAndIncrement();
    otherRpcId = IDS.getAndIncrement();
    assertFalse(executions.containsKey(rpcId));
    assertFalse(executions.containsKey(otherRpcId));
    notifications = MoreExecutors.newDirectExecutorService();
    sql = "SELECT time, device_id FROM telemetry /* roster-" + rpcId + "-\u03bc */";
    previousRuntimeEnabled =
        IoTDBDescriptor.getInstance().getConfig().isEnableRuntimeScanParallelism();
    runtimeConfigurationSaved = true;
    IoTDBDescriptor.getInstance().getConfig().setEnableRuntimeScanParallelism(false);
    auditWindow = ResourceAuditManager.getInstance().openWindow(60_000, 0, 0);
  }

  @After
  public void tearDown() {
    closeAuditWindow();
    if (runtimeConfigurationSaved) {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setEnableRuntimeScanParallelism(previousRuntimeEnabled);
    }
    if (executions != null) {
      executions.remove(rpcId);
      executions.remove(otherRpcId);
    }
    if (notifications != null) {
      notifications.shutdownNow();
    }
  }

  @Test
  public void missingAndInvalidLookupsNeverInventAnInternalQueryId() throws Exception {
    Map<String, Object> missing = QueryExecutionDescription.describe(rpcId);
    assertEquals("MISSING", missing.get("status"));
    assertEquals(rpcId, missing.get("actualRpcQueryId"));
    assertNull(missing.get("queryId"));
    assertEquals(false, missing.get("rosterReady"));
    Map<String, Object> byHash = QueryExecutionDescription.describeBySqlHash(hash(sql));
    assertEquals("NOT_FOUND", byHash.get("status"));
    assertNull(byHash.get("actualRpcQueryId"));
    assertNull(byHash.get("queryId"));
    assertEquals("NOT_AVAILABLE", byHash.get("rpcQueryIdStatus"));
    assertEquals(
        "INVALID_SQL_HASH", QueryExecutionDescription.describeBySqlHash("not-a-sha").get("status"));
  }

  @Test
  public void unplannedRealExecutionHasExplicitNotReadyStateAndExactSqlHash() throws Exception {
    Fixture fixture = new Fixture("unplanned_" + rpcId);
    executions.put(rpcId, fixture.execution);
    Map<String, Object> result = QueryExecutionDescription.describe(rpcId);
    assertEquals("PLAN_UNAVAILABLE", result.get("status"));
    assertEquals("QUEUED", result.get("state"));
    assertEquals(fixture.queryId.getId(), result.get("queryId"));
    assertEquals(hash(sql), result.get("sqlSha256"));
    assertEquals(false, result.get("rosterReady"));
    assertEquals(List.of(), result.get("fragmentInstances"));
    assertFalse(result.containsKey("sql"));
    Map<String, Object> byHash = QueryExecutionDescription.describeBySqlHash(hash(sql));
    assertEquals(fixture.queryId.getId(), byHash.get("queryId"));
    assertEquals("PLAN_UNAVAILABLE", byHash.get("status"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void actualCoordinatorBindingProducesDetachedImmutableRosterAndEdges() throws Exception {
    Fixture fixture = new Fixture("actual_" + rpcId);
    fixture.publishRunningPlan();
    executions.put(rpcId, fixture.execution);
    Map<String, Object> result = QueryExecutionDescription.describe(rpcId);
    assertEquals("READY", result.get("status"));
    assertEquals(true, result.get("rosterReady"));
    assertEquals("RUNNING", result.get("state"));
    assertEquals(fixture.queryId.getId(), result.get("queryId"));
    assertEquals(2, result.get("fragmentInstanceCount"));
    List<Map<String, Object>> fragments =
        (List<Map<String, Object>>) result.get("fragmentInstances");
    Map<String, Object> source =
        fragments.stream()
            .filter(value -> fixture.sourceId.getFullId().equals(value.get("fragmentInstanceId")))
            .findFirst()
            .orElseThrow();
    assertEquals(11, source.get("dataNodeId"));
    Map<String, Object> endpoint = (Map<String, Object>) source.get("internalEndpoint");
    assertEquals("127.0.0.1", endpoint.get("ip"));
    assertEquals(27130, endpoint.get("port"));
    assertEquals(9, ((Map<String, Object>) source.get("region")).get("id"));
    List<Map<String, Object>> edges = (List<Map<String, Object>>) result.get("edges");
    assertEquals(2, edges.size());
    assertTrue(
        edges.stream()
            .allMatch(
                edge ->
                    fixture.sourceId.getFullId().equals(edge.get("sourceFragmentInstanceId"))
                        && fixture.rootId.getFullId().equals(edge.get("targetFragmentInstanceId"))
                        && Integer.valueOf(0).equals(edge.get("channelIndex"))));
    Map<String, Object> byHash = QueryExecutionDescription.describeBySqlHash(hash(sql));
    assertEquals("READY", byHash.get("status"));
    assertNull(byHash.get("actualRpcQueryId"));
    assertEquals(result.get("fragmentInstances"), byHash.get("fragmentInstances"));
    assertThrows(UnsupportedOperationException.class, () -> result.put("state", "FAILED"));
    assertThrows(UnsupportedOperationException.class, () -> fragments.add(Map.of()));
    assertThrows(UnsupportedOperationException.class, () -> endpoint.put("ip", "changed"));
    assertThrows(UnsupportedOperationException.class, () -> edges.get(0).put("channelIndex", 7));
    String frozenJson = new GsonBuilder().serializeNulls().create().toJson(result);
    fixture.sourceHost.getInternalEndPoint().setIp("192.0.2.1");
    fixture.channels.clear();
    fixture.plan.getInstances().clear();
    assertEquals(frozenJson, new GsonBuilder().serializeNulls().create().toJson(result));
    assertEquals(
        fixture.queryId.getId(),
        JsonParser.parseString(frozenJson).getAsJsonObject().get("queryId").getAsString());
    // Dispatcher tree release and later mutations do not invalidate the already published record.
    assertEquals("READY", QueryExecutionDescription.describe(rpcId).get("status"));
  }

  @Test
  public void duplicateSqlHashesAndRetryPlansCannotMasqueradeAsAUniqueReadyQuery()
      throws Exception {
    Fixture first = new Fixture("first_" + rpcId);
    Fixture second = new Fixture("second_" + rpcId);
    first.publishDispatchingPlan();
    second.publishRunningPlan();
    executions.put(rpcId, first.execution);
    executions.put(otherRpcId, second.execution);
    Map<String, Object> ambiguous = QueryExecutionDescription.describeBySqlHash(hash(sql));
    assertEquals("AMBIGUOUS", ambiguous.get("status"));
    assertEquals(2, ambiguous.get("matchCount"));
    assertEquals(false, ambiguous.get("rosterReady"));
    assertNull(ambiguous.get("queryId"));
    assertEquals(first.queryId.getId(), QueryExecutionDescription.describe(rpcId).get("queryId"));
    executions.remove(otherRpcId);
    assertEquals("READY", QueryExecutionDescription.describeBySqlHash(hash(sql)).get("status"));
    stateMachine(first.execution).transitionToPendingRetry(new TSStatus(503));
    Map<String, Object> retrying = QueryExecutionDescription.describe(rpcId);
    assertEquals("PLANNING", retrying.get("status"));
    assertEquals("PENDING_RETRY", retrying.get("state"));
    assertEquals(false, retrying.get("rosterReady"));
    assertEquals(List.of(), retrying.get("fragmentInstances"));
  }

  @Test
  public void ordinaryDispatchTreeReleasePreservesCompletePublishedRoster() throws Exception {
    Fixture fixture = new Fixture("released_" + rpcId);
    fixture.publishRunningPlan();
    executions.put(rpcId, fixture.execution);
    Map<String, Object> before = QueryExecutionDescription.describe(rpcId);
    assertEquals("READY", before.get("status"));
    fixture.plan.getInstances().forEach(instance -> instance.getFragment().clearUselessField());
    assertTrue(
        fixture.plan.getInstances().stream()
            .allMatch(instance -> instance.getFragment().getPlanNodeTree() == null));
    Map<String, Object> after = QueryExecutionDescription.describe(rpcId);
    assertEquals("READY", after.get("status"));
    assertEquals(before.get("fragmentInstances"), after.get("fragmentInstances"));
    assertEquals(before.get("edges"), after.get("edges"));
    assertEquals(2, after.get("fragmentInstanceCount"));
    assertEquals("READY", QueryExecutionDescription.describeBySqlHash(hash(sql)).get("status"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void eachDistributionPublicationReplacesItsOwnPlanIdentityAndMetadata() throws Exception {
    Fixture fixture = new Fixture("replanned_" + rpcId);
    Fixture replacement = new Fixture(fixture.queryId.getId());
    replacement.sourceHost.getInternalEndPoint().setPort(29130);
    fixture.publishDispatchingPlan();
    executions.put(rpcId, fixture.execution);
    Map<String, Object> first = QueryExecutionDescription.describe(rpcId);
    assertEquals("READY", first.get("status"));
    when(fixture.planner.doDistributionPlan(fixture.analysis, null, fixture.context))
        .thenReturn(replacement.plan);
    fixture.execution.doDistributedPlan();
    assertNull(fixture.execution.getPublishedPlanDescription(fixture.plan));
    Map<String, Object> second = QueryExecutionDescription.describe(rpcId);
    assertEquals("READY", second.get("status"));
    List<Map<String, Object>> sources = (List<Map<String, Object>>) second.get("fragmentInstances");
    Map<String, Object> source =
        sources.stream()
            .filter(value -> fixture.sourceId.getFullId().equals(value.get("fragmentInstanceId")))
            .findFirst()
            .orElseThrow();
    assertEquals(29130, ((Map<String, Object>) source.get("internalEndpoint")).get("port"));
    assertFalse(first.get("fragmentInstances").equals(second.get("fragmentInstances")));
    replacement.plan.getInstances().forEach(instance -> instance.getFragment().clearUselessField());
    assertEquals(
        second.get("fragmentInstances"),
        QueryExecutionDescription.describe(rpcId).get("fragmentInstances"));
    stateMachine(fixture.execution).transitionToPendingRetry(new TSStatus(503));
    assertEquals("PLANNING", QueryExecutionDescription.describe(rpcId).get("status"));
  }

  @Test
  public void missingTreesAtPublicationNeverBecomeAnInventedReadyRoster() throws Exception {
    Fixture fixture = new Fixture("missing_at_publication_" + rpcId);
    fixture.plan.getInstances().get(0).getFragment().clearUselessField();
    fixture.publishRunningPlan();
    executions.put(rpcId, fixture.execution);
    Map<String, Object> description = QueryExecutionDescription.describe(rpcId);
    assertEquals("INCOMPLETE_ROSTER", description.get("status"));
    assertEquals(false, description.get("rosterReady"));
    assertEquals(List.of(), description.get("fragmentInstances"));
    assertEquals(List.of(), description.get("edges"));
  }

  @Test
  public void disabledRuntimeWithoutAuditNeverTraversesOrRetainsPlanMetadata() throws Exception {
    closeAuditWindow();
    assertFalse(ResourceAuditManager.getInstance().isEnabled());
    Fixture fixture = new Fixture("disabled_" + rpcId, SqlDialect.TABLE);
    DistributedQueryPlan forbidden = traversalForbiddenPlan(fixture);
    fixture.publishRunningPlan();
    executions.put(rpcId, fixture.execution);
    verify(forbidden, never()).getInstances();
    assertNull(fixture.execution.getPublishedPlanDescription(forbidden));
    assertEquals(
        "PLAN_DESCRIPTION_UNAVAILABLE", QueryExecutionDescription.describe(rpcId).get("status"));
  }

  @Test
  public void explicitAuditWindowCapturesRuntimeDisabledControlMetadata() throws Exception {
    assertTrue(ResourceAuditManager.getInstance().isEnabled());
    assertFalse(IoTDBDescriptor.getInstance().getConfig().isEnableRuntimeScanParallelism());
    Fixture fixture = new Fixture("audited_control_" + rpcId, SqlDialect.TABLE);
    fixture.publishRunningPlan();
    executions.put(rpcId, fixture.execution);
    assertEquals("READY", QueryExecutionDescription.describe(rpcId).get("status"));
    assertTrue(fixture.execution.getPublishedPlanDescription(fixture.plan) != null);
  }

  @Test
  public void explicitAuditAlsoCapturesTreeDialectMetadata() throws Exception {
    assertTrue(ResourceAuditManager.getInstance().isEnabled());
    Fixture fixture = new Fixture("audited_tree_" + rpcId, SqlDialect.TREE);
    fixture.publishRunningPlan();
    executions.put(rpcId, fixture.execution);
    assertEquals("READY", QueryExecutionDescription.describe(rpcId).get("status"));
  }

  @Test
  public void closingAuditBeforeReplanningClearsTheOldAttemptBinding() throws Exception {
    Fixture fixture = new Fixture("closed_window_" + rpcId, SqlDialect.TABLE);
    fixture.publishRunningPlan();
    executions.put(rpcId, fixture.execution);
    assertEquals("READY", QueryExecutionDescription.describe(rpcId).get("status"));
    closeAuditWindow();
    assertFalse(ResourceAuditManager.getInstance().isEnabled());
    DistributedQueryPlan forbidden = traversalForbiddenPlan(fixture);
    fixture.execution.doDistributedPlan();
    verify(forbidden, never()).getInstances();
    assertNull(fixture.execution.getPublishedPlanDescription(fixture.plan));
    assertNull(fixture.execution.getPublishedPlanDescription(forbidden));
    Map<String, Object> result = QueryExecutionDescription.describe(rpcId);
    assertEquals("PLAN_DESCRIPTION_UNAVAILABLE", result.get("status"));
    assertEquals(false, result.get("rosterReady"));
    assertEquals(List.of(), result.get("fragmentInstances"));
  }

  @Test
  public void treeDialectDoesNotCaptureForTableRuntimeFlagAlone() throws Exception {
    closeAuditWindow();
    IoTDBDescriptor.getInstance().getConfig().setEnableRuntimeScanParallelism(true);
    Fixture fixture = new Fixture("tree_off_path_" + rpcId, SqlDialect.TREE);
    DistributedQueryPlan forbidden = traversalForbiddenPlan(fixture);
    fixture.publishRunningPlan();
    executions.put(rpcId, fixture.execution);
    verify(forbidden, never()).getInstances();
    assertNull(fixture.execution.getPublishedPlanDescription(forbidden));
    assertEquals(
        "PLAN_DESCRIPTION_UNAVAILABLE", QueryExecutionDescription.describe(rpcId).get("status"));
  }

  @Test
  public void tableRuntimeFlagCapturesWithoutAnAuditWindow() throws Exception {
    closeAuditWindow();
    IoTDBDescriptor.getInstance().getConfig().setEnableRuntimeScanParallelism(true);
    Fixture fixture = new Fixture("table_runtime_" + rpcId, SqlDialect.TABLE);
    fixture.publishRunningPlan();
    executions.put(rpcId, fixture.execution);
    assertEquals("READY", QueryExecutionDescription.describe(rpcId).get("status"));
  }

  private DistributedQueryPlan traversalForbiddenPlan(Fixture fixture) {
    DistributedQueryPlan forbidden = mock(DistributedQueryPlan.class);
    when(forbidden.getInstances())
        .thenThrow(new AssertionError("Disabled metadata observation must not traverse the plan"));
    when(fixture.planner.doDistributionPlan(fixture.analysis, null, fixture.context))
        .thenReturn(forbidden);
    return forbidden;
  }

  private void closeAuditWindow() {
    if (auditWindow != null) {
      ResourceAuditManager.getInstance().closeWindow(auditWindow);
      auditWindow = null;
    }
  }

  private final class Fixture {
    private final QueryId queryId;
    private final QueryExecution execution;
    private final IPlanner planner;
    private final IAnalysis analysis;
    private final MPPQueryContext context;
    private final DistributedQueryPlan plan;
    private final TDataNodeLocation sourceHost;
    private final FragmentInstanceId sourceId;
    private final FragmentInstanceId rootId;
    private final List<DownStreamChannelLocation> channels = new ArrayList<>();

    private Fixture(String id) {
      this(id, SqlDialect.TABLE);
    }

    private Fixture(String id, SqlDialect dialect) {
      queryId = new QueryId(id);
      sourceHost = host(11, 27130, 27140, 27667);
      TDataNodeLocation rootHost = host(12, 28130, 28140, 28667);
      PlanFragmentId sourceFragment = new PlanFragmentId(queryId, 0);
      PlanFragmentId rootFragment = new PlanFragmentId(queryId, 1);
      sourceId = new FragmentInstanceId(sourceFragment, "0");
      rootId = new FragmentInstanceId(rootFragment, "0");
      PlanNodeId sourceSinkId = new PlanNodeId("source-sink");
      PlanNodeId receiveId = new PlanNodeId("receive");
      channels.add(
          new DownStreamChannelLocation(
              rootHost.getMPPDataExchangeEndPoint(), rootId.toThrift(), receiveId.getId()));
      IdentitySinkNode sink = new IdentitySinkNode(sourceSinkId, channels);
      ExchangeNode receive = new ExchangeNode(receiveId);
      receive.setUpstream(sourceHost.getMPPDataExchangeEndPoint(), sourceId, sourceSinkId);
      receive.setIndexOfUpstreamSinkHandle(0);
      PlanFragment sourcePlan = new PlanFragment(sourceFragment, sink);
      PlanFragment rootPlan = new PlanFragment(rootFragment, receive);
      rootPlan.setRoot(true);
      FragmentInstance source =
          new FragmentInstance(
              sourcePlan, sourceId, QueryType.READ, 30_000, null, false, false, false, false);
      source.setExecutorAndHost(
          new StorageExecutor(
              new TRegionReplicaSet(
                  new TConsensusGroupId(TConsensusGroupType.DataRegion, 9), List.of(sourceHost))));
      FragmentInstance root =
          new FragmentInstance(
              rootPlan, rootId, QueryType.READ, 30_000, null, false, false, true, false);
      root.setExecutorAndHost(new QueryExecutor(rootHost));
      plan = new DistributedQueryPlan(null, new ArrayList<>(List.of(source, root)));
      context =
          new MPPQueryContext(
              sql,
              queryId,
              new SessionInfo(0, "test", ZoneId.of("UTC"), null, dialect),
              new TEndPoint(),
              new TEndPoint());
      context.setQueryType(QueryType.OTHER);
      planner = mock(IPlanner.class);
      analysis = mock(IAnalysis.class);
      when(planner.analyze(context)).thenReturn(analysis);
      when(planner.doDistributionPlan(analysis, null, context)).thenReturn(plan);
      execution = new QueryExecution(planner, context, notifications);
    }

    private void publishDispatchingPlan() throws Exception {
      execution.doDistributedPlan();
      QueryStateMachine state = stateMachine(execution);
      state.transitionToPlanned();
      state.transitionToDispatching();
    }

    private void publishRunningPlan() throws Exception {
      publishDispatchingPlan();
      stateMachine(execution).transitionToRunning();
    }
  }

  private static TDataNodeLocation host(int id, int internalPort, int exchangePort, int rpcPort) {
    return new TDataNodeLocation()
        .setDataNodeId(id)
        .setInternalEndPoint(new TEndPoint("127.0.0.1", internalPort))
        .setMPPDataExchangeEndPoint(new TEndPoint("127.0.0.1", exchangePort))
        .setClientRpcEndPoint(new TEndPoint("127.0.0.1", rpcPort));
  }

  private static QueryStateMachine stateMachine(QueryExecution execution) throws Exception {
    Field field = QueryExecution.class.getDeclaredField("stateMachine");
    field.setAccessible(true);
    return (QueryStateMachine) field.get(execution);
  }

  private static String hash(String text) throws Exception {
    return HexFormat.of()
        .formatHex(
            MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));
  }
}
