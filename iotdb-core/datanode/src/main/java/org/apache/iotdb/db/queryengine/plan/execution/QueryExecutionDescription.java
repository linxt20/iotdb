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

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.execution.QueryState;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.MultiChildrenSinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableHashPartitioningShuffleSinkNode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A read-only observation of the coordinator's actual RPC-id binding and published planned
 * fragment-instance roster. Routing metadata is frozen before dispatch; no execution, plan,
 * endpoint, driver, callback or throwable is retained in the result.
 */
public final class QueryExecutionDescription {
  private QueryExecutionDescription() {}

  public static Map<String, Object> describe(long actualRpcQueryId) {
    Coordinator coordinator = Coordinator.getInstance();
    return describe(
        coordinator, actualRpcQueryId, null, coordinator.getQueryExecution(actualRpcQueryId));
  }

  public static Map<String, Object> describeBySqlHash(String exactUtf8Sha256) {
    if (exactUtf8Sha256 == null || !exactUtf8Sha256.matches("[0-9a-fA-F]{64}")) {
      return finish(empty(null, null), "INVALID_SQL_HASH");
    }
    String hash = exactUtf8Sha256.toLowerCase(java.util.Locale.ROOT);
    Coordinator coordinator = Coordinator.getInstance();
    List<QueryExecution> matches = matches(coordinator, hash);
    if (matches.size() != 1) {
      Map<String, Object> result = empty(null, hash);
      result.put("matchCount", matches.size());
      return finish(result, matches.isEmpty() ? "NOT_FOUND" : "AMBIGUOUS");
    }
    return describe(coordinator, null, hash, matches.get(0));
  }

  private static List<QueryExecution> matches(Coordinator coordinator, String hash) {
    List<QueryExecution> matches = new ArrayList<>();
    for (IQueryExecution candidate : coordinator.getAllQueryExecutions()) {
      if (candidate instanceof QueryExecution
          && candidate
              .getExecuteSQL()
              .map(QueryExecutionDescription::sha256)
              .filter(hash::equals)
              .isPresent()) {
        matches.add((QueryExecution) candidate);
      }
    }
    return matches;
  }

  private static Map<String, Object> empty(Long actualRpcQueryId, String sqlHash) {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("schemaVersion", 1);
    result.put("actualRpcQueryId", actualRpcQueryId);
    result.put("rpcQueryIdStatus", actualRpcQueryId == null ? "NOT_AVAILABLE" : "SUPPLIED");
    result.put("requestedSqlSha256", sqlHash);
    result.put("queryId", null);
    result.put("sqlSha256", null);
    result.put("state", null);
    result.put("rosterReady", false);
    result.put("fragmentInstanceCount", 0);
    result.put("fragmentInstances", List.of());
    result.put("edges", List.of());
    result.put("hostObservation", "PLANNED_HOST_AT_SNAPSHOT");
    return result;
  }

  private static Map<String, Object> describe(
      Coordinator coordinator, Long actualRpcQueryId, String sqlHash, IQueryExecution execution) {
    Map<String, Object> result = empty(actualRpcQueryId, sqlHash);
    if (execution == null) {
      return finish(result, actualRpcQueryId == null ? "NOT_FOUND" : "MISSING");
    }
    result.put("queryId", execution.getQueryId());
    result.put(
        "sqlSha256", execution.getExecuteSQL().map(QueryExecutionDescription::sha256).orElse(null));
    if (!(execution instanceof QueryExecution)) {
      return finish(result, "UNSUPPORTED_EXECUTION");
    }
    QueryExecution query = (QueryExecution) execution;
    QueryState state = query.getQueryState();
    result.put("state", state.name());
    DistributedQueryPlan plan = query.getDistributedPlan();
    if (plan == null) {
      return finish(result, "PLAN_UNAVAILABLE");
    }
    // A retry can leave the preceding attempt's plan assigned until the new distribution completes.
    // Wait for dispatch/running instead of presenting that stale plan as this attempt's roster.
    if (state == QueryState.QUEUED
        || state == QueryState.PLANNED
        || state == QueryState.PENDING_RETRY) {
      return finish(result, "PLANNING");
    }
    Map<String, Object> description = query.getPublishedPlanDescription(plan);
    if (description == null) {
      return finish(result, "PLAN_DESCRIPTION_UNAVAILABLE");
    }
    // The plan is published through a volatile reference after distribution has built the roster.
    // Reject a concurrent retry/removal/state transition rather than joining two query attempts.
    boolean sameBinding;
    if (actualRpcQueryId != null) {
      sameBinding = coordinator.getQueryExecution(actualRpcQueryId) == execution;
    } else {
      List<QueryExecution> current = matches(coordinator, sqlHash);
      sameBinding = current.size() == 1 && current.get(0) == execution;
    }
    if (!sameBinding
        || query.getDistributedPlan() != plan
        || query.getQueryState() != state
        || query.getPublishedPlanDescription(plan) != description) {
      return finish(result, "CHANGING");
    }
    if (!"READY".equals(description.get("status"))) {
      if (description.containsKey("copyFailureClass")) {
        result.put("copyFailureClass", description.get("copyFailureClass"));
      }
      return finish(result, (String) description.get("status"));
    }
    result.put("fragmentInstanceCount", description.get("fragmentInstanceCount"));
    result.put("fragmentInstances", description.get("fragmentInstances"));
    result.put("edges", description.get("edges"));
    result.put("rosterReady", true);
    return finish(result, "READY");
  }

  /**
   * Copies the completed routing graph before dispatch can release its plan trees. The returned
   * record retains only immutable scalar/list/map values; it never retains the plan or instances.
   * QueryExecution binds this record to one exact distribution attempt, replacing it on retry.
   */
  static Map<String, Object> capturePlan(DistributedQueryPlan plan, String queryId) {
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("fragmentInstanceCount", 0);
    result.put("fragmentInstances", List.of());
    result.put("edges", List.of());
    if (plan == null) {
      return finish(result, "PLAN_UNAVAILABLE");
    }
    try {
      List<FragmentInstance> instances = new ArrayList<>(plan.getInstances());
      List<Map<String, Object>> fragments = new ArrayList<>();
      List<Map<String, Object>> edges = new ArrayList<>();
      Set<String> ids = new HashSet<>();
      boolean complete = !instances.isEmpty();
      for (FragmentInstance instance : instances) {
        if (instance == null
            || instance.getFragment() == null
            || instance.getFragment().getPlanNodeTree() == null
            || !ids.add(instance.getId().getFullId())
            || !queryId.equals(instance.getId().getQueryId().getId())) {
          return finish(result, "INCOMPLETE_ROSTER");
        }
        Map<String, Object> fragment = fragment(instance, edges);
        complete &= Boolean.TRUE.equals(fragment.get("hostAssigned"));
        fragments.add(fragment);
      }
      fragments.sort(Comparator.comparing(item -> (String) item.get("fragmentInstanceId")));
      edges.sort(Comparator.comparing(QueryExecutionDescription::edgeKey));
      result.put("fragmentInstanceCount", fragments.size());
      result.put("fragmentInstances", List.copyOf(fragments));
      result.put("edges", List.copyOf(edges));
      return finish(result, complete ? "READY" : "INCOMPLETE_ROSTER");
    } catch (RuntimeException incompleteCopy) {
      // A capture failure is explicitly not ready; it never turns a missing routing graph into a
      // successful roster and does not change execution semantics for an unsupported statement.
      result.put("copyFailureClass", incompleteCopy.getClass().getName());
      return finish(result, "COPY_UNAVAILABLE");
    }
  }

  private static Map<String, Object> fragment(
      FragmentInstance instance, List<Map<String, Object>> edges) {
    Map<String, Object> result = new LinkedHashMap<>();
    String id = instance.getId().getFullId();
    result.put("fragmentInstanceId", id);
    result.put("fragmentId", instance.getId().getFragmentId().getId());
    result.put("instanceId", instance.getId().getInstanceId());
    result.put("root", instance.isRoot());
    result.put("runtimeExecutionEnabled", instance.isRuntimeExecutionEnabled());
    TDataNodeLocation host = instance.getHostDataNode();
    result.put(
        "hostAssigned",
        host != null
            && host.getInternalEndPoint() != null
            && host.getInternalEndPoint().getIp() != null
            && host.getInternalEndPoint().getPort() > 0);
    result.put("dataNodeId", host == null ? null : host.getDataNodeId());
    result.put("internalEndpoint", endpoint(host == null ? null : host.getInternalEndPoint()));
    result.put(
        "exchangeEndpoint", endpoint(host == null ? null : host.getMPPDataExchangeEndPoint()));
    result.put("rpcEndpoint", endpoint(host == null ? null : host.getClientRpcEndPoint()));
    result.put(
        "executorType",
        instance.getExecutorType() == null
            ? null
            : instance.getExecutorType().getClass().getSimpleName());
    TRegionReplicaSet region =
        instance.getExecutorType() == null || !instance.getExecutorType().isStorageExecutor()
            ? null
            : instance.getRegionReplicaSet();
    Map<String, Object> regionValue = new LinkedHashMap<>();
    if (region != null && region.getRegionId() != null) {
      regionValue.put("type", region.getRegionId().getType().name());
      regionValue.put("id", region.getRegionId().getId());
      List<Integer> replicas = new ArrayList<>();
      if (region.getDataNodeLocations() != null) {
        for (TDataNodeLocation replica : region.getDataNodeLocations()) {
          replicas.add(replica.getDataNodeId());
        }
      }
      regionValue.put("replicaDataNodeIds", List.copyOf(replicas));
    }
    result.put("region", immutable(regionValue));
    List<Map<String, Object>> nodes = new ArrayList<>();
    Set<PlanNode> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    ArrayDeque<PlanNode> pending = new ArrayDeque<>();
    pending.add(instance.getFragment().getPlanNodeTree());
    boolean scan = false;
    boolean hashSink = false;
    boolean exchange = false;
    while (!pending.isEmpty()) {
      PlanNode node = pending.removeFirst();
      if (!visited.add(node)) continue;
      Map<String, Object> value = new LinkedHashMap<>();
      String nodeId = node.getPlanNodeId().getId();
      value.put("planNodeId", nodeId);
      value.put("type", node.getClass().getSimpleName());
      List<String> children = new ArrayList<>();
      for (PlanNode child : node.getChildren()) {
        children.add(child.getPlanNodeId().getId());
        pending.addLast(child);
      }
      value.put("children", List.copyOf(children));
      scan |= node instanceof DeviceTableScanNode;
      if (node instanceof TableHashPartitioningShuffleSinkNode) {
        hashSink = true;
        TableHashPartitioningShuffleSinkNode sink = (TableHashPartitioningShuffleSinkNode) node;
        value.put("hashPartitionCount", sink.getPartitioningDescriptor().getPartitionCount());
        value.put("hashVersion", sink.getPartitioningDescriptor().getHashVersion().name());
        value.put("nullRouting", sink.getPartitioningDescriptor().getNullRouting().name());
        value.put(
            "partitioningSymbols",
            sink.getPartitioningDescriptor().getPartitioningSymbols().stream()
                .map(Object::toString)
                .collect(java.util.stream.Collectors.toUnmodifiableList()));
      }
      if (node
          instanceof
          org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode) {
        value.put(
            "aggregationStep",
            ((org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode)
                    node)
                .getStep()
                .name());
      }
      if (node instanceof MultiChildrenSinkNode) {
        List<DownStreamChannelLocation> channels =
            new ArrayList<>(((MultiChildrenSinkNode) node).getDownStreamChannelLocationList());
        for (int index = 0; index < channels.size(); index++) {
          DownStreamChannelLocation channel = channels.get(index);
          Map<String, Object> edge = new LinkedHashMap<>();
          edge.put("type", "SINK_CHANNEL");
          edge.put("sourceFragmentInstanceId", id);
          edge.put("sourcePlanNodeId", nodeId);
          edge.put("channelIndex", index);
          edge.put(
              "targetFragmentInstanceId",
              channel.getRemoteFragmentInstanceId() == null
                  ? null
                  : FragmentInstanceId.fromThrift(channel.getRemoteFragmentInstanceId())
                      .getFullId());
          edge.put("targetPlanNodeId", channel.getRemotePlanNodeId());
          edge.put("endpoint", endpoint(channel.getRemoteEndpoint()));
          edges.add(immutable(edge));
        }
      }
      if (node instanceof ExchangeNode) {
        exchange = true;
        ExchangeNode receive = (ExchangeNode) node;
        Map<String, Object> edge = new LinkedHashMap<>();
        edge.put("type", "EXCHANGE_INPUT");
        edge.put(
            "sourceFragmentInstanceId",
            receive.getUpstreamInstanceId() == null
                ? null
                : receive.getUpstreamInstanceId().getFullId());
        edge.put(
            "sourcePlanNodeId",
            receive.getUpstreamPlanNodeId() == null
                ? null
                : receive.getUpstreamPlanNodeId().getId());
        edge.put("channelIndex", receive.getIndexOfUpstreamSinkHandle());
        edge.put("targetFragmentInstanceId", id);
        edge.put("targetPlanNodeId", nodeId);
        edge.put("endpoint", endpoint(receive.getUpstreamEndpoint()));
        edges.add(immutable(edge));
      }
      nodes.add(immutable(value));
    }
    nodes.sort(Comparator.comparing(item -> (String) item.get("planNodeId")));
    result.put("nodes", List.copyOf(nodes));
    result.put("containsDeviceScan", scan);
    result.put("containsHashSink", hashSink);
    result.put("containsExchange", exchange);
    return immutable(result);
  }

  private static String edgeKey(Map<String, Object> edge) {
    return edge.get("type")
        + "/"
        + edge.get("sourceFragmentInstanceId")
        + "/"
        + edge.get("sourcePlanNodeId")
        + "/"
        + edge.get("channelIndex")
        + "/"
        + edge.get("targetFragmentInstanceId")
        + "/"
        + edge.get("targetPlanNodeId");
  }

  private static Map<String, Object> endpoint(TEndPoint value) {
    if (value == null) return Map.of();
    Map<String, Object> result = new LinkedHashMap<>();
    result.put("ip", value.getIp());
    result.put("port", value.getPort());
    return immutable(result);
  }

  private static Map<String, Object> finish(Map<String, Object> result, String status) {
    result.put("status", status);
    return immutable(result);
  }

  private static Map<String, Object> immutable(Map<String, Object> values) {
    return Collections.unmodifiableMap(new LinkedHashMap<>(values));
  }

  private static String sha256(String sql) {
    try {
      return HexFormat.of()
          .formatHex(
              MessageDigest.getInstance("SHA-256").digest(sql.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException unavailable) {
      throw new IllegalStateException(unavailable);
    }
  }
}
