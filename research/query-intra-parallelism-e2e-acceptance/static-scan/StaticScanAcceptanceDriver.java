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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.partition.QueryExecutor;
import org.apache.iotdb.commons.partition.StorageExecutor;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.TsTableInternalRPCUtil;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.DistributedQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.IdentitySinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.planner.PlannerContext;
import org.apache.iotdb.db.queryengine.plan.relational.planner.TableModelPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.HashPartitioningDescriptor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableHashPartitioningShuffleSinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.DistributedOptimizeFactory;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.LogicalOptimizeFactory;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.rewrite.StatementRewriteFactory;
import org.apache.iotdb.db.queryengine.plan.scheduler.FragInstanceDispatchResult;
import org.apache.iotdb.db.queryengine.plan.scheduler.FragmentInstanceDispatcherImpl;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.mpp.rpc.thrift.TCancelQueryReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.mpp.rpc.thrift.TOperatorStatistics;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.tsfile.read.common.block.TsBlock;

/**
 * Standalone acceptance coordinator; every physical operator executes in the two real DataNodes.
 */
public final class StaticScanAcceptanceDriver {
  private static final Gson JSON = new GsonBuilder().serializeNulls().setPrettyPrinting().create();
  private static final Gson ROW_JSON = new GsonBuilder().serializeNulls().create();
  private static final int BUCKETS = 32;
  private static final long TIMEOUT_MS = 180000;

  public record FrozenCase(
      MPPQueryContext context,
      List<FragmentInstance> instances,
      FragmentInstance source,
      FragmentInstance root,
      List<ExchangeNode> bucketExchanges,
      String sinkId,
      List<Symbol> symbols,
      JsonObject topology) {}

  private static void require(boolean value, String message) {
    if (!value) throw new IllegalStateException(message);
  }

  private static Object field(Object receiver, String name) throws Exception {
    return receiver.getClass().getMethod(name).invoke(receiver);
  }

  private static void write(Path path, Object value) throws Exception {
    require(!Files.exists(path), "Evidence already exists: " + path);
    Files.writeString(path, JSON.toJson(value));
  }

  private static String sha(byte[] bytes) throws Exception {
    return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static byte[] bytes(ByteBuffer original) {
    ByteBuffer copy = original.duplicate();
    byte[] bytes = new byte[copy.remaining()];
    copy.get(bytes);
    return bytes;
  }

  private static List<DeviceTableScanNode> scans(PlanNode root) {
    List<DeviceTableScanNode> found = new ArrayList<>();
    if (root instanceof DeviceTableScanNode) found.add((DeviceTableScanNode) root);
    for (PlanNode child : root.getChildren()) found.addAll(scans(child));
    return found;
  }

  private static FragmentInstance instance(
      MPPQueryContext context,
      PlanNode root,
      TDataNodeLocation node,
      TypeProvider types,
      boolean isRoot) {
    PlanFragment fragment = new PlanFragment(context.getQueryId().genPlanFragmentId(), root);
    fragment.setTypeProvider(TypeProvider.copyOf(types.allTableModelTypes()));
    fragment.setRoot(isRoot);
    FragmentInstance instance =
        new FragmentInstance(
            fragment,
            new FragmentInstanceId(fragment.getId(), "0"),
            QueryType.READ,
            TIMEOUT_MS,
            context.getSession(),
            true,
            false,
            isRoot,
            true);
    instance.setExecutorAndHost(new QueryExecutor(node));
    return instance;
  }

  /**
   * Receives the actual analyzer's input scan and types, then freezes the acceptance-only graph.
   */
  public static FrozenCase buildSingleSource32BucketCase(
      MPPQueryContext context,
      FragmentInstance analyzedSource,
      TypeProvider analyzedTypes,
      List<TDataNodeLocation> nodes,
      Path output)
      throws Exception {
    require(nodes.size() == 2, "Exactly two actual DataNodes are required");
    require(
        analyzedSource.getExecutorType().isStorageExecutor(),
        "Input must be a real storage fragment");
    PlanNode input = analyzedSource.getFragment().getPlanNodeTree();
    require(
        input instanceof IdentitySinkNode && input.getChildren().size() == 1,
        "Analyzed source must have one ordinary terminal sink");
    input = input.getChildren().get(0);
    require(scans(input).size() == 1, "Exactly one analyzed DeviceTableScanNode is required");
    require(
        input.getOutputSymbols().size() == 2, "Fixture requires precisely s1,s2 output symbols");
    List<Symbol> symbols = List.copyOf(input.getOutputSymbols());
    List<FragmentInstance> instances = new ArrayList<>();
    List<ExchangeNode> bucketExchanges = new ArrayList<>();
    List<DownStreamChannelLocation> sourceChannels = new ArrayList<>();
    List<PlanNode> rootExchanges = new ArrayList<>();
    PlanNodeId sinkId = context.getQueryId().genPlanNodeId();
    HashPartitioningDescriptor descriptor =
        new HashPartitioningDescriptor(
            List.of(symbols.get(0)),
            HashPartitioningDescriptor.HashVersion.TABLE_HASH_V1,
            HashPartitioningDescriptor.NullRouting.HASH_NULL,
            BUCKETS);
    TableHashPartitioningShuffleSinkNode sourceSink =
        new TableHashPartitioningShuffleSinkNode(sinkId, descriptor);
    sourceSink.addChild(input);
    FragmentInstance source =
        instance(context, sourceSink, analyzedSource.getHostDataNode(), analyzedTypes, false);
    source.setExecutorAndHost(analyzedSource.getExecutorType());
    source.setHostDataNode(analyzedSource.getHostDataNode());
    CollectNode collect = new CollectNode(context.getQueryId().genPlanNodeId(), symbols);
    IdentitySinkNode rootSink =
        new IdentitySinkNode(
            context.getQueryId().genPlanNodeId(),
            List.of(collect),
            List.of(
                new DownStreamChannelLocation(
                    context.getLocalDataBlockEndpoint(),
                    context.getResultNodeContext().getVirtualFragmentInstanceId().toThrift(),
                    context.getResultNodeContext().getVirtualResultNodeId().getId())));
    FragmentInstance root = instance(context, rootSink, nodes.get(0), analyzedTypes, true);
    JsonArray buckets = new JsonArray();
    for (int bucket = 0; bucket < BUCKETS; bucket++) {
      TDataNodeLocation owner = nodes.get(bucket % nodes.size());
      ExchangeNode receive = new ExchangeNode(context.getQueryId().genPlanNodeId());
      receive.setOutputSymbols(symbols);
      receive.setUpstream(
          source.getHostDataNode().getMPPDataExchangeEndPoint(), source.getId(), sinkId);
      receive.setIndexOfUpstreamSinkHandle(bucket);
      ExchangeNode gather = new ExchangeNode(context.getQueryId().genPlanNodeId());
      gather.setOutputSymbols(symbols);
      IdentitySinkNode bucketSink =
          new IdentitySinkNode(
              context.getQueryId().genPlanNodeId(),
              List.of(receive),
              List.of(
                  new DownStreamChannelLocation(
                      root.getHostDataNode().getMPPDataExchangeEndPoint(),
                      root.getId().toThrift(),
                      gather.getPlanNodeId().getId())));
      FragmentInstance receiver = instance(context, bucketSink, owner, analyzedTypes, false);
      sourceChannels.add(
          new DownStreamChannelLocation(
              owner.getMPPDataExchangeEndPoint(),
              receiver.getId().toThrift(),
              receive.getPlanNodeId().getId()));
      gather.setUpstream(
          owner.getMPPDataExchangeEndPoint(), receiver.getId(), bucketSink.getPlanNodeId());
      gather.setIndexOfUpstreamSinkHandle(0);
      collect.addChild(gather);
      instances.add(receiver);
      rootExchanges.add(gather);
      bucketExchanges.add(receive);
      JsonObject identity = new JsonObject();
      identity.addProperty("bucket", bucket);
      identity.addProperty("dataNodeId", owner.getDataNodeId());
      identity.addProperty("fragmentInstanceId", receiver.getId().getFullId());
      identity.addProperty("sourceExchangeId", receive.getPlanNodeId().getId());
      identity.addProperty("gatherExchangeId", gather.getPlanNodeId().getId());
      identity.addProperty("bucketSinkId", bucketSink.getPlanNodeId().getId());
      identity.addProperty("upstreamSinkChannelIndex", bucket);
      identity.addProperty(
          "sourceMemoryKey", source.getId().getFragmentInstanceId() + "/" + sinkId.getId());
      buckets.add(identity);
    }
    sourceSink.getDownStreamChannelLocationList().addAll(sourceChannels);
    instances.add(source);
    instances.add(root);
    Map<Integer, Integer> counts = new HashMap<>();
    for (FragmentInstance fi : instances)
      counts.merge(fi.getHostDataNode().getDataNodeId(), 1, Integer::sum);
    Set<String> ids = new HashSet<>();
    JsonArray frozen = new JsonArray();
    for (FragmentInstance fi : instances) {
      require(ids.add(fi.getId().getFullId()), "Duplicate fragment instance");
      fi.setDataNodeFINum(counts.get(fi.getHostDataNode().getDataNodeId()));
      byte[] plan = bytes(fi.serializeToByteBuffer());
      Path path = output.resolve(fi.getId().getFragmentInstanceId() + ".fi.bin");
      Files.write(path, plan);
      FragmentInstance roundTrip = FragmentInstance.deserializeFrom(ByteBuffer.wrap(plan));
      require(roundTrip.getId().equals(fi.getId()), "Frozen fragment round-trip identity differs");
      JsonObject item = new JsonObject();
      item.addProperty("path", path.toString());
      item.addProperty("sha256", sha(plan));
      item.addProperty("fragmentInstanceId", fi.getId().getFullId());
      item.addProperty("dataNodeId", fi.getHostDataNode().getDataNodeId());
      item.addProperty("internalEndpoint", fi.getHostDataNode().getInternalEndPoint().toString());
      require(nodes.contains(fi.getHostDataNode()), "Frozen host is not a discovered real DataNode");
      require(fi.isRoot() == fi.getFragment().isRoot(), "Construction root markers disagree");
      item.addProperty("isRoot", fi.isRoot());
      boolean storage = fi.getExecutorType().isStorageExecutor();
      item.addProperty("executorKind", storage ? "STORAGE" : "QUERY");
      byte[] hostBytes =
          new TSerializer(new TBinaryProtocol.Factory()).serialize(fi.getHostDataNode());
      Path hostPath = output.resolve(fi.getId().getFragmentInstanceId() + ".host.thrift.bin");
      Files.write(hostPath, hostBytes);
      JsonObject hostBinding = new JsonObject();
      hostBinding.addProperty("path", hostPath.toString());
      hostBinding.addProperty("sha256", sha(hostBytes));
      item.add("hostBinding", hostBinding);
      if (storage) {
        TRegionReplicaSet region = fi.getRegionReplicaSet();
        require(
            region != null && region.getDataNodeLocations().contains(fi.getHostDataNode()),
            "Storage source host is outside its actual analyzed replica set");
        byte[] regionBytes = new TSerializer(new TBinaryProtocol.Factory()).serialize(region);
        Path regionPath =
            output.resolve(fi.getId().getFragmentInstanceId() + ".region.thrift.bin");
        Files.write(regionPath, regionBytes);
        JsonObject regionBinding = new JsonObject();
        regionBinding.addProperty("path", regionPath.toString());
        regionBinding.addProperty("sha256", sha(regionBytes));
        regionBinding.addProperty("regionId", region.getRegionId().getId());
        regionBinding.addProperty("regionType", region.getRegionId().getType().name());
        regionBinding.addProperty("selectedHostDataNodeId", fi.getHostDataNode().getDataNodeId());
        item.add("storageRegionBinding", regionBinding);
      }
      frozen.add(item);
    }
    context
        .getResultNodeContext()
        .setUpStream(
            root.getHostDataNode().getMPPDataExchangeEndPoint(),
            root.getId(),
            rootSink.getPlanNodeId());
    JsonObject topology = new JsonObject();
    topology.addProperty("queryId", context.getQueryId().getId());
    topology.addProperty("sourceFragmentInstanceId", source.getId().getFullId());
    topology.addProperty("sourceSinkId", sinkId.getId());
    topology.addProperty("sourceDataNodeId", source.getHostDataNode().getDataNodeId());
    topology.addProperty("sourceScanNodeId", scans(input).get(0).getPlanNodeId().getId());
    topology.addProperty(
        "sourceMemoryKey", source.getId().getFragmentInstanceId() + "/" + sinkId.getId());
    topology.addProperty("sourceCount", 1);
    topology.addProperty("bucketCount", BUCKETS);
    topology.addProperty("hashChannelCount", BUCKETS);
    topology.addProperty("fragmentInstanceCount", instances.size());
    topology.addProperty("scanDop", 1);
    topology.add(
        "outputTypes",
        JSON.toJsonTree(
            symbols.stream()
                .map(symbol -> analyzedTypes.getTableModelType(symbol).getTypeEnum().name())
                .toList()));
    topology.addProperty("rootFragmentInstanceId", root.getId().getFullId());
    topology.add("buckets", buckets);
    topology.add("frozenFragments", frozen);
    write(output.resolve("frozen-plan.json"), topology);
    return new FrozenCase(
        context, instances, source, root, bucketExchanges, sinkId.getId(), symbols, topology);
  }

  private static JsonObject stableNodeIdentity(PlanNode node) {
    JsonObject identity = new JsonObject();
    identity.addProperty("id", node.getPlanNodeId().getId());
    identity.addProperty("class", node.getClass().getName());
    if (node instanceof org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.MultiChildrenSinkNode sink) {
      JsonArray channels = new JsonArray();
      for (DownStreamChannelLocation channel : sink.getDownStreamChannelLocationList()) {
        JsonObject edge = new JsonObject();
        edge.addProperty("endpoint", String.valueOf(channel.getRemoteEndpoint()));
        edge.addProperty("fragment", String.valueOf(channel.getRemoteFragmentInstanceId()));
        edge.addProperty("planNode", channel.getRemotePlanNodeId());
        channels.add(edge);
      }
      identity.add("downstreamChannels", channels);
    } else {
      identity.add("outputs", JSON.toJsonTree(node.getOutputSymbols().stream().map(Symbol::getName).toList()));
    }
    if (node instanceof ExchangeNode exchange) {
      identity.addProperty("upstreamEndpoint", String.valueOf(exchange.getUpstreamEndpoint()));
      identity.addProperty("upstreamFragment", exchange.getUpstreamInstanceId().getFullId());
      identity.addProperty("upstreamPlanNode", exchange.getUpstreamPlanNodeId().getId());
      identity.addProperty("upstreamBucket", exchange.getIndexOfUpstreamSinkHandle());
    }
    if (node instanceof TableHashPartitioningShuffleSinkNode hash) {
      HashPartitioningDescriptor descriptor = hash.getPartitioningDescriptor();
      identity.addProperty("partitionCount", descriptor.getPartitionCount());
      identity.addProperty("hashVersion", descriptor.getHashVersion().name());
      identity.addProperty("nullRouting", descriptor.getNullRouting().name());
      identity.add("partitionSymbols", JSON.toJsonTree(descriptor.getPartitioningSymbols().stream().map(Symbol::getName).toList()));
    }
    if (node instanceof DeviceTableScanNode scan) {
      identity.add("devices", JSON.toJsonTree(scan.getDeviceEntries().stream().map(device -> device.getDeviceID().toString()).toList()));
      identity.addProperty("timePredicate", String.valueOf(scan.getTimePredicate()));
      identity.addProperty("pushDownPredicate", String.valueOf(scan.getPushDownPredicate()));
    }
    JsonArray children = new JsonArray();
    for (PlanNode child : node.getChildren()) children.add(stableNodeIdentity(child));
    identity.add("children", children);
    return identity;
  }

  private static JsonObject stableWireIdentity(FragmentInstance instance) {
    JsonObject identity = new JsonObject();
    identity.addProperty("fragmentInstanceId", instance.getId().getFullId());
    identity.addProperty("queryType", instance.getType().name());
    identity.addProperty("host", String.valueOf(instance.getHostDataNode()));
    identity.addProperty("globalTimePredicate", String.valueOf(instance.getGlobalTimePredicate()));
    identity.addProperty("explainAnalyze", instance.isExplainAnalyze());
    identity.addProperty("dataNodeFINum", instance.getDataNodeFINum());
    Map<String, String> types = new TreeMap<>();
    instance.getFragment().getTypeProvider().allTableModelTypes()
        .forEach((symbol, type) -> types.put(symbol.getName(), type.getTypeEnum().name()));
    identity.add("types", JSON.toJsonTree(types));
    identity.add("plan", stableNodeIdentity(instance.getFragment().getPlanNodeTree()));
    return identity;
  }

  /** Uses the unchanged production dispatcher, source handle, RPC snapshot and cleanup paths. */
  public static JsonObject executeFrozenCase(
      FrozenCase plan,
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> sync,
      IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> async,
      Path output,
      boolean requireNewMetrics)
      throws Exception {
    AtomicReference<Throwable> receiveFailure = new AtomicReference<>();
    var resultContext = plan.context().getResultNodeContext();
    ISourceHandle result =
        MPPDataExchangeService.getInstance()
            .getMPPDataExchangeManager()
            .createSourceHandle(
                resultContext.getVirtualFragmentInstanceId().toThrift(),
                resultContext.getVirtualResultNodeId().getId(),
                0,
                resultContext.getUpStreamEndpoint(),
                resultContext.getUpStreamFragmentInstanceId().toThrift(),
                receiveFailure::set);
    JsonArray rows = new JsonArray();
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MS);
    Throwable executionFailure = null;
    try {
      // The bytes frozen above, rather than the mutable construction objects, are dispatched.
      Path frozenPlanPath = output.resolve("frozen-plan.json");
      byte[] frozenPlanBytes = Files.readAllBytes(frozenPlanPath);
      JsonObject frozenPlan =
          JsonParser.parseString(new String(frozenPlanBytes, StandardCharsets.UTF_8))
              .getAsJsonObject();
      require(frozenPlan.equals(plan.topology()), "Frozen topology metadata changed");
      List<FragmentInstance> actual = new ArrayList<>();
      JsonArray dispatchBindings = new JsonArray();
      int roots = 0;
      int storageSources = 0;
      for (JsonElement e : frozenPlan.getAsJsonArray("frozenFragments")) {
        JsonObject f = e.getAsJsonObject();
        byte[] data = Files.readAllBytes(Path.of(f.get("path").getAsString()));
        require(sha(data).equals(f.get("sha256").getAsString()), "Frozen plan changed");
        FragmentInstance decoded = FragmentInstance.deserializeFrom(ByteBuffer.wrap(data));
        byte[] decodedWire = bytes(decoded.serializeToByteBuffer());
        JsonObject decodedIdentity = stableWireIdentity(decoded);
        Path decodedPath = output.resolve(decoded.getId().getFragmentInstanceId() + ".decoded.fi.bin");
        Files.write(decodedPath, decodedWire);
        require(
            decoded.getId().getFullId().equals(f.get("fragmentInstanceId").getAsString()),
            "Frozen instance identity differs");
        JsonObject hostBinding = f.getAsJsonObject("hostBinding");
        byte[] hostBytes =
            Files.readAllBytes(Path.of(hostBinding.get("path").getAsString()));
        require(sha(hostBytes).equals(hostBinding.get("sha256").getAsString()), "Frozen host changed");
        TDataNodeLocation host = new TDataNodeLocation();
        new TDeserializer(new TBinaryProtocol.Factory()).deserialize(host, hostBytes);
        require(
            host.equals(decoded.getHostDataNode())
                && host.getDataNodeId() == f.get("dataNodeId").getAsInt()
                && host.getInternalEndPoint().toString().equals(f.get("internalEndpoint").getAsString()),
            "Wire host, frozen route and actual node identity differ");
        boolean isRoot = f.get("isRoot").getAsBoolean();
        require(
            isRoot
                == decoded
                    .getId()
                    .getFullId()
                    .equals(frozenPlan.get("rootFragmentInstanceId").getAsString()),
            "Frozen root identity differs");
        if (isRoot) roots++;
        // FI and PlanFragment wire omit these external coordinator/root metadata fields.
        // Keep every executable plan node and wire field from the frozen bytes.
        decoded.getFragment().setRoot(isRoot);
        FragmentInstance restored =
            new FragmentInstance(
                decoded.getFragment(),
                decoded.getId(),
                decoded.getGlobalTimePredicate(),
                decoded.getType(),
                decoded.getTimeOut(),
                decoded.getSessionInfo(),
                decoded.isExplainAnalyze(),
                decoded.isDebug(),
                isRoot,
                decoded.isVerbose());
        restored.setDataNodeFINum(decoded.getDataNodeFINum());
        restored.setHighestPriority(decoded.isHighestPriority());
        String kind = f.get("executorKind").getAsString();
        if ("STORAGE".equals(kind)) {
          storageSources++;
          require(
              restored.getId().getFullId()
                  .equals(frozenPlan.get("sourceFragmentInstanceId").getAsString()),
              "Storage routing belongs to a different frozen source");
          JsonObject regionBinding = f.getAsJsonObject("storageRegionBinding");
          byte[] regionBytes =
              Files.readAllBytes(Path.of(regionBinding.get("path").getAsString()));
          require(
              sha(regionBytes).equals(regionBinding.get("sha256").getAsString()),
              "Frozen storage region changed");
          TRegionReplicaSet region = new TRegionReplicaSet();
          new TDeserializer(new TBinaryProtocol.Factory()).deserialize(region, regionBytes);
          require(
              region.getDataNodeLocations().contains(host)
                  && region.getRegionId().getId() == regionBinding.get("regionId").getAsInt()
                  && region.getRegionId().getType().name()
                      .equals(regionBinding.get("regionType").getAsString())
                  && host.getDataNodeId() == regionBinding.get("selectedHostDataNodeId").getAsInt(),
              "Actual analyzed storage replica and selected host bindings differ");
          restored.setExecutorAndHost(new StorageExecutor(region));
          restored.setHostDataNode(host);
        } else {
          require("QUERY".equals(kind), "Unknown frozen executor kind");
          require(!f.has("storageRegionBinding"), "Query executor carries storage routing");
          restored.setExecutorAndHost(new QueryExecutor(host));
        }
        require(restored.isExplainAnalyze(), "Frozen statistics instrumentation was lost");
        byte[] restoredWire = bytes(restored.serializeToByteBuffer());
        require(
            java.util.Arrays.equals(decodedWire, restoredWire),
            "Restoring external metadata changed the unmodified decoded executable wire");
        JsonObject restoredIdentity = stableWireIdentity(restored);
        require(
            decodedIdentity.equals(restoredIdentity),
            "Restoring external metadata changed FI/node/edge/bucket/type/device/predicate identity");
        Path dispatchPath = output.resolve(decoded.getId().getFragmentInstanceId() + ".dispatch.fi.bin");
        Files.write(dispatchPath, restoredWire);
        JsonObject binding = f.deepCopy();
        binding.addProperty("decodedWirePath", decodedPath.toString());
        binding.addProperty("decodedWireSha256", sha(decodedWire));
        binding.addProperty("dispatchWirePath", dispatchPath.toString());
        binding.addProperty("originalWireEqualsNormalRoundTrip",
            java.util.Arrays.equals(data, decodedWire));
        binding.addProperty("restoredWireEqualsUnmodifiedDecodedWire", true);
        binding.add("stableWireIdentity", decodedIdentity);
        binding.addProperty("stableWireIdentitySha256",
            sha(JSON.toJson(decodedIdentity).getBytes(StandardCharsets.UTF_8)));
        binding.addProperty("restoredInstanceIsRoot", restored.isRoot());
        binding.addProperty("restoredFragmentIsRoot", restored.getFragment().isRoot());
        binding.addProperty("restoredExecutorKind",
            restored.getExecutorType().isStorageExecutor() ? "STORAGE" : "QUERY");
        binding.addProperty("restoredWireSha256", sha(bytes(restored.serializeToByteBuffer())));
        dispatchBindings.add(binding);
        actual.add(restored);
      }
      require(actual.size() == 34 && roots == 1 && storageSources == 1,
          "Frozen dispatch routing/root cardinality differs");
      write(
          output.resolve("dispatch-frozen-bindings.json"),
          Map.of("frozenPlanPath", frozenPlanPath.toString(),
              "frozenPlanSha256", sha(frozenPlanBytes), "bindings", dispatchBindings));
      FragInstanceDispatchResult dispatched =
          new FragmentInstanceDispatcherImpl(QueryType.READ, plan.context(), sync, async)
              .dispatch(null, actual)
              .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
      require(
          dispatched.isSuccessful(),
          "Production dispatch rejected: " + dispatched.getFailureStatus());
      while (!result.isFinished()) {
        require(System.nanoTime() < deadline, "Complete result timed out");
        require(
            receiveFailure.get() == null,
            "Production result exchange failed: " + receiveFailure.get());
        result
            .isBlocked()
            .get(
                Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())),
                TimeUnit.MILLISECONDS);
        if (result.isFinished()) break;
        TsBlock block = result.receive();
        if (block == null) continue;
        require(block.getValueColumnCount() == 2, "Result changed the analyzed output schema");
        for (int row = 0; row < block.getPositionCount(); row++) {
          JsonArray values = new JsonArray();
          if (block.getColumn(0).isNull(row)) values.add(com.google.gson.JsonNull.INSTANCE);
          else values.add(block.getColumn(0).getLong(row));
          if (block.getColumn(1).isNull(row)) values.add(com.google.gson.JsonNull.INSTANCE);
          else values.add(block.getColumn(1).getDouble(row));
          rows.add(values);
        }
      }
      write(output.resolve("result.json"), rows);
      List<String> canonical = new ArrayList<>();
      rows.forEach(r -> canonical.add(ROW_JSON.toJson(r)));
      canonical.sort(Comparator.naturalOrder());
      String content = String.join("\n", canonical) + "\n";
      // Exact compact JSON rows sorted lexically; empty input is one final newline.
      Files.writeString(output.resolve("result.canonical.jsonl"), content);
      Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> responses =
          new LinkedHashMap<>();
      while (true) {
        responses.clear();
        boolean ready = true;
        for (FragmentInstance fi : plan.instances()) {
          try (var client = sync.borrowClient(fi.getHostDataNode().getInternalEndPoint())) {
            var response =
                client.fetchFragmentInstanceStatistics(
                    new TFetchFragmentInstanceStatisticsReq(fi.getId().toThrift()));
            ready &=
                response.getStatus().getCode() == 200 && "FINISHED".equals(response.getState());
            require(
                !List.of("FAILED", "ABORTED", "CANCELLED").contains(response.getState()),
                "Actual fragment failed: " + fi.getId() + " " + response);
            responses.put(fi.getId(), response);
          }
        }
        if (ready && responses.size() == 34) break;
        require(System.nanoTime() < deadline, "Not all 34 real fragments reached FINISHED");
        Thread.sleep(50);
      }
      JsonArray snapshots = new JsonArray();
      JsonArray actualBuckets = new JsonArray();
      Set<String> seen = new HashSet<>();
      for (var e : responses.entrySet()) {
        TFetchFragmentInstanceStatisticsResp s = e.getValue();
        require(
            "FINISHED".equals(s.getState()),
            "Fragment did not finish: " + e.getKey() + " " + s.getState());
        JsonObject record = new JsonObject();
        record.addProperty("fragmentInstanceId", e.getKey().getFullId());
        record.addProperty("endpoint", s.getIp());
        require(seen.add(e.getKey().getFullId()), "Duplicate runtime fragment identity");
        if (requireNewMetrics) {
          require(
              ((Number) field(s, "getResourceObjectCount")).longValue() > 0
                  && ((Number) field(s, "getResourceUniqueMemoryKeyCount")).longValue() > 0
                  && ((Number) field(s, "getResourceNominalShareBytes")).longValue() > 0,
              "All-zero or missing real resource accounting");
          require(Boolean.TRUE.equals(field(s, "isIsTerminal")), "Snapshot is not frozen terminal");
          require(
              field(s, "getSnapshotChecksum") != null
                  && field(s, "getPlanIdentityChecksum") != null,
              "Missing terminal or plan checksum");
          require(
              field(s, "getDriverSnapshots") instanceof List
                  && !((List<?>) field(s, "getDriverSnapshots")).isEmpty(),
              "Missing actual drivers");
          if (e.getKey().equals(plan.source().getId())) {
            require(
                ((Number) field(s, "getPlannedDop")).intValue() == 1,
                "Source actual planned scan DOP is not one");
            require(
                ((Number) field(s, "getActualPipelineCount")).intValue() == 1,
                "Pure source created extra pipelines");
          }
          record.addProperty("dataNodeId", String.valueOf(field(s, "getSourceNodeId")));
          record.addProperty("snapshotChecksum", String.valueOf(field(s, "getSnapshotChecksum")));
          record.addProperty("plannedDop", (Number) field(s, "getPlannedDop"));
          record.addProperty("actualPipelineCount", (Number) field(s, "getActualPipelineCount"));
        }
        record.add(
            "rawThrift",
            JsonParser.parseString(
                new String(
                    new TSerializer(new TJSONProtocol.Factory()).serialize(s),
                    StandardCharsets.UTF_8)));
        JsonArray actualOperators = new JsonArray();
        for (TOperatorStatistics operator : s.getOperatorStatisticsMap().values()) {
          JsonObject item = new JsonObject();
          item.addProperty("planNodeId", operator.getPlanNodeId());
          item.addProperty("operatorType", operator.getOperatorType());
          item.addProperty("outputRows", operator.getOutputRows());
          item.add("specifiedInfo", JSON.toJsonTree(operator.getSpecifiedInfo()));
          actualOperators.add(item);
        }
        record.add("actualOperators", actualOperators);
        snapshots.add(record);
        for (int bucket = 0; bucket < BUCKETS; bucket++) {
          JsonObject owner =
              plan.topology().getAsJsonArray("buckets").get(bucket).getAsJsonObject();
          if (!e.getKey().getFullId().equals(owner.get("fragmentInstanceId").getAsString()))
            continue;
          String exchange = owner.get("sourceExchangeId").getAsString();
          List<TOperatorStatistics> operators =
              s.getOperatorStatisticsMap().values().stream()
                  .filter(op -> exchange.equals(op.getPlanNodeId()))
                  .toList();
          require(operators.size() == 1, "Missing/duplicate actual bucket exchange operator");
          JsonObject observed = owner.deepCopy();
          observed.addProperty("state", s.getState());
          observed.addProperty("outputRows", operators.get(0).getOutputRows());
          actualBuckets.add(observed);
        }
      }
      require(actualBuckets.size() == BUCKETS, "Missing completed receive bucket");
      write(output.resolve("terminal-snapshots.json"), snapshots);
      write(output.resolve("bucket-results.json"), actualBuckets);
      JsonObject receipt = new JsonObject();
      receipt.addProperty("status", "EXECUTED");
      receipt.addProperty("rows", rows.size());
      receipt.addProperty("resultSha256", sha(content.getBytes(StandardCharsets.UTF_8)));
      receipt.addProperty("sourceCount", 1);
      receipt.addProperty("hashChannels", BUCKETS);
      receipt.addProperty("completedBuckets", actualBuckets.size());
      receipt.addProperty("actualFragmentInstances", seen.size());
      receipt.addProperty("scanDop", 1);
      return receipt;
    } catch (Exception | Error failure) {
      executionFailure = failure;
      throw failure;
    } finally {
      try {
        List<String> failures = new ArrayList<>();
        try {
          result.close();
        } catch (Exception failure) {
          failures.add("result close: " + failure);
        }
        Map<TEndPoint, List<org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId>> byNode =
            new LinkedHashMap<>();
        for (FragmentInstance fi : plan.instances())
          byNode
              .computeIfAbsent(fi.getHostDataNode().getInternalEndPoint(), k -> new ArrayList<>())
              .add(fi.getId().toThrift());
        for (var e : byNode.entrySet()) {
          try (var client = sync.borrowClient(e.getKey())) {
            var response =
                client.cancelQuery(
                    new TCancelQueryReq(plan.context().getQueryId().getId(), e.getValue(), false));
            if (!response.isCancelled()) failures.add(e.getKey() + ": " + response);
          } catch (Exception failure) {
            failures.add(e.getKey() + ": " + failure);
          }
        }
        write(
            output.resolve("query-cleanup.json"),
            Map.of("failures", failures, "allPlannedFragments", 34));
        require(failures.isEmpty(), "Query cleanup failed: " + failures);
      } catch (Exception | Error cleanupFailure) {
        if (executionFailure != null) executionFailure.addSuppressed(cleanupFailure);
        else throw cleanupFailure;
      }
    }
  }

  private static final class Audit implements AutoCloseable {
    final JMXConnector connector;
    final MBeanServerConnection server;
    final ObjectName object;
    final String window;
    final int nodeId;

    Audit(int nodeId, int port) throws Exception {
      this.nodeId = nodeId;
      connector =
          JMXConnectorFactory.connect(
              new JMXServiceURL("service:jmx:rmi:///jndi/rmi://127.0.0.1:" + port + "/jmxrmi"),
              Map.of("jmx.remote.x.request.waiting.timeout", 15000L));
      server = connector.getMBeanServerConnection();
      Set<ObjectName> matches = new HashSet<>();
      for (ObjectName name : server.queryNames(null, null)) {
        for (MBeanOperationInfo op : server.getMBeanInfo(name).getOperations()) {
          if (op.getName().equals("openResourceAuditWindow")
              && op.getSignature().length == 1
              && op.getSignature()[0].getType().equals("long")) matches.add(name);
        }
      }
      require(matches.size() == 1, "Expected one unique resource audit MBean with long duration");
      object = matches.iterator().next();
      window = (String) call("openResourceAuditWindow", new Object[] {600000L}, "long");
    }

    Object call(String op, Object[] args, String... types) throws Exception {
      return server.invoke(object, op, args, types);
    }

    JsonObject read(String query, String fragment) throws Exception {
      return JsonParser.parseString(
              (String)
                  call(
                      "readResourceAudit",
                      new Object[] {window, query, fragment},
                      "java.lang.String",
                      "java.lang.String",
                      "java.lang.String"))
          .getAsJsonObject();
    }

    @Override
    public void close() throws Exception {
      Throwable windowFailure = null;
      try {
        call("closeResourceAuditWindow", new Object[] {window}, "java.lang.String");
      } catch (Exception | Error failure) {
        windowFailure = failure;
        throw failure;
      } finally {
        try {
          connector.close();
        } catch (Exception | Error failure) {
          if (windowFailure != null) windowFailure.addSuppressed(failure);
          else throw failure;
        }
      }
    }
  }

  private static JsonObject captureRawAudits(
      List<Audit> audits, String queryId, boolean protocolAvailable, Throwable primaryFailure) {
    JsonObject evidence = new JsonObject();
    evidence.addProperty("queryId", queryId);
    evidence.addProperty("protocolAvailable", protocolAvailable);
    evidence.addProperty("capturedBeforeWindowClose", true);
    evidence.addProperty(
        "primaryFailureClass", primaryFailure == null ? null : primaryFailure.getClass().getName());
    evidence.addProperty(
        "primaryFailure", primaryFailure == null ? null : primaryFailure.toString());
    JsonArray nodes = new JsonArray();
    JsonArray errors = new JsonArray();
    evidence.addProperty("openedAuditWindows", audits.size());
    if (protocolAvailable && audits.size() != 2)
      errors.add("Expected two open candidate audit windows; found " + audits.size());
    for (Audit audit : audits) {
      JsonObject node = new JsonObject();
      node.addProperty("dataNodeId", audit.nodeId);
      node.addProperty("windowId", audit.window);
      JsonArray snapshots = new JsonArray();
      try {
        JsonArray records =
            JsonParser.parseString(
                    (String)
                        audit.call(
                            "listResourceAuditFragments",
                            new Object[] {audit.window, ""},
                            "java.lang.String",
                            "java.lang.String"))
                .getAsJsonArray();
        node.add("rawFragmentList", records);
        for (JsonElement element : records) {
          JsonObject record = element.getAsJsonObject();
          JsonObject item = new JsonObject();
          item.add("identity", record.deepCopy());
          try {
            item.add(
                "rawSnapshot",
                audit.read(
                    record.get("queryId").getAsString(),
                    record.get("fragmentInstanceId").getAsString()));
          } catch (Exception | Error failure) {
            item.addProperty("readFailure", failure.toString());
            errors.add("node=" + audit.nodeId + " fragment=" + record + " " + failure);
          }
          snapshots.add(item);
        }
      } catch (Exception | Error failure) {
        node.addProperty("listFailure", failure.toString());
        errors.add("node=" + audit.nodeId + " list " + failure);
      }
      node.add("rawSnapshots", snapshots);
      try {
        node.add("rawPool", audit.read("__idle__", "0.0"));
      } catch (Exception | Error failure) {
        node.addProperty("poolReadFailure", failure.toString());
        errors.add("node=" + audit.nodeId + " pool " + failure);
      }
      nodes.add(node);
    }
    evidence.add("nodes", nodes);
    evidence.add("captureErrors", errors);
    evidence.addProperty(
        "status",
        !protocolAvailable
            ? "UNAVAILABLE_IN_FROZEN_A"
            : errors.size() == 0 ? "RAW_CAPTURED" : "PARTIAL");
    return evidence;
  }

  private static JsonArray resourceReceipts(FrozenCase plan, List<Audit> audits, Path output)
      throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    JsonArray finalState;
    JsonArray poolState = new JsonArray();
    JsonArray baselinePools =
        JsonParser.parseString(Files.readString(output.resolve("resource-idle-before.json")))
            .getAsJsonArray();
    while (true) {
      finalState = new JsonArray();
      boolean complete = true;
      for (FragmentInstance fi : plan.instances()) {
        Audit audit =
            audits.stream()
                .filter(a -> a.nodeId == fi.getHostDataNode().getDataNodeId())
                .findFirst()
                .orElseThrow();
        JsonObject s = audit.read(plan.context().getQueryId().getId(), fi.getId().getFullId());
        s.addProperty("dataNodeId", audit.nodeId);
        finalState.add(s);
        for (String key :
            List.of(
                "liveDrivers",
                "liveQueues",
                "unconsumedFactories",
                "liveEndpoints",
                "pendingFutures",
                "fragmentReservedBytes",
                "queryReservedBytes",
                "deregisterFailures")) complete &= s.has(key) && s.get(key).getAsLong() == 0;
        complete &=
            s.has("complete")
                && s.get("complete").getAsBoolean()
                && s.has("released")
                && s.get("released").getAsBoolean()
                && s.has("fragmentMapPresent")
                && !s.get("fragmentMapPresent").getAsBoolean();
        complete &=
            s.has("deregisterReceipt")
                && s.get("deregisterReceipt").isJsonObject()
                && s.getAsJsonObject("deregisterReceipt").get("success").getAsBoolean();
      }
      poolState = new JsonArray();
      for (Audit audit : audits) {
        JsonObject pool = audit.read("__idle__", "0.0");
        pool.addProperty("dataNodeId", audit.nodeId);
        poolState.add(pool);
        JsonObject before = null;
        for (JsonElement e : baselinePools)
          if (e.getAsJsonObject().get("dataNodeId").getAsInt() == audit.nodeId)
            before = e.getAsJsonObject();
        require(before != null, "Missing independent node pool baseline");
        for (String key : List.of("exchangeRemainingBytes", "operatorsFreeBytes"))
          complete &= pool.has(key) && pool.get(key).getAsLong() == before.get(key).getAsLong();
      }
      if (complete) break;
      if (System.nanoTime() >= deadline) {
        write(output.resolve("resource-incomplete.json"), finalState);
        throw new IllegalStateException(
            "Fragment resources did not return independent zero receipts");
      }
      Thread.sleep(50);
    }
    JsonObject source = null;
    for (JsonElement e : finalState) {
      JsonObject s = e.getAsJsonObject();
      if (plan.source().getId().getFullId().equals(s.get("fragmentInstanceId").getAsString()))
        source = s;
    }
    require(source != null, "Missing producer resource receipt");
    Set<String> tokens = new HashSet<>(), keys = new HashSet<>();
    for (JsonElement e : source.getAsJsonArray("objects")) {
      JsonObject o = e.getAsJsonObject();
      if (!"SINK_CHANNEL".equals(o.get("kind").getAsString())) continue;
      require(tokens.add(o.get("token").getAsString()), "Duplicate physical producer channel");
      keys.add(
          o.get("memoryQueryId").getAsString()
              + "/"
              + o.get("memoryFragmentInstanceId").getAsString()
              + "/"
              + o.get("memoryPlanNodeId").getAsString());
    }
    require(
        tokens.size() == 32 && keys.size() == 1,
        "Producer must own 32 distinct channels with one memory account");
    require(
        keys.equals(
            Set.of(
                plan.context().getQueryId().getId()
                    + "/"
                    + plan.source().getId().getFragmentInstanceId()
                    + "/"
                    + plan.sinkId())),
        "Observed producer account differs from frozen source identity");
    require(
        source.get("createdDrivers").getAsLong() == 1
            && source.get("closedDrivers").getAsLong() == 1,
        "Pure producer must create/close exactly one real driver");
    boolean constructor = false;
    for (JsonElement e : source.getAsJsonArray("quotaPublication")) {
      JsonObject q = e.getAsJsonObject();
      if ("SHUFFLE_CONSTRUCTOR_LIMITS".equals(q.get("kind").getAsString())) {
        require(
            q.get("channelCount").getAsInt() == 32 && q.get("explicitRouting").getAsBoolean(),
            "Actual hash sink did not create 32 explicitly routed channels");
        constructor = true;
      }
    }
    require(constructor, "Missing production constructor/quota observation");
    write(output.resolve("resource-terminal.json"), finalState);
    write(output.resolve("resource-idle-after.json"), poolState);
    return finalState;
  }

  private static void run(JsonObject manifest, Path output) throws Exception {
    require(!Files.exists(output), "Run output must be new");
    Files.createDirectories(output);
    write(output.resolve("driver-input.json"), manifest);
    Map<String, String> loaded = new TreeMap<>();
    for (Class<?> type :
        List.of(
            StaticScanAcceptanceDriver.class,
            TableModelPlanner.class,
            FragmentInstanceDispatcherImpl.class,
            DeviceTableScanNode.class,
            TableHashPartitioningShuffleSinkNode.class,
            MPPDataExchangeService.class))
      loaded.put(
          type.getName(), type.getProtectionDomain().getCodeSource().getLocation().toString());
    write(output.resolve("actual-loaded-code.json"), loaded);
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    // Coordinator initializes a QueryIdGenerator even though this process only plans and dispatches.
    // Use the separately bound helper endpoint as its private ID; it never registers as a DataNode.
    config.setDataNodeId(manifest.get("helperInternalPort").getAsInt());
    require(config.getDegreeOfParallelism() == 1, "Helper scan DOP configuration must be one");
    int collectorPort = manifest.get("collectorPort").getAsInt();
    require(
        config.getMppDataExchangePort() == collectorPort, "Collector port/config binding differs");
    require(
        config.getInternalPort() != manifest.get("firstDataNodeInternalPort").getAsInt(),
        "Standalone coordinator must dispatch remotely to real nodes");
    List<TEndPoint> configs = new ArrayList<>();
    for (JsonElement e : manifest.getAsJsonArray("configNodes")) {
      JsonObject n = e.getAsJsonObject();
      configs.add(new TEndPoint(n.get("host").getAsString(), n.get("port").getAsInt()));
    }
    require(
        ConfigNodeInfo.getInstance().updateConfigNodeList(configs),
        "Cannot bind isolated ConfigNode list");
    List<TDataNodeLocation> nodes = new ArrayList<>();
    String database = manifest.get("database").getAsString();
    String table = manifest.get("table").getAsString();
    try (ConfigNodeClient client =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      int beforeSlots = config.getSeriesPartitionSlotNum();
      String beforeExecutor = config.getSeriesPartitionExecutorClass();
      var systemConfiguration = client.getSystemConfiguration();
      require(
          systemConfiguration.getStatus().getCode() == 200
              && systemConfiguration.getGlobalConfig() != null,
          "Cannot fetch actual isolated cluster global configuration");
      var globalConfig = systemConfiguration.getGlobalConfig();
      // Real DataNodes apply this ConfigNode response before constructing partition fetchers.
      // The standalone planner must use the same hash slots and time-partition parameters.
      IoTDBDescriptor.getInstance().loadGlobalConfig(globalConfig);
      CommonDescriptor.getInstance().loadGlobalConfig(globalConfig);
      require(
          config.getSeriesPartitionSlotNum() == globalConfig.getSeriesPartitionSlotNum()
              && config.getSeriesPartitionExecutorClass()
                  .equals(globalConfig.getSeriesPartitionExecutorClass()),
          "Standalone partition configuration differs from actual isolated cluster");
      JsonObject globalIdentity = new JsonObject();
      globalIdentity.addProperty("beforeSeriesPartitionSlotNum", beforeSlots);
      globalIdentity.addProperty("beforeSeriesPartitionExecutor", beforeExecutor);
      globalIdentity.addProperty("loadedSeriesPartitionSlotNum", config.getSeriesPartitionSlotNum());
      globalIdentity.addProperty("loadedSeriesPartitionExecutor", config.getSeriesPartitionExecutorClass());
      globalIdentity.add(
          "actualClusterGlobalConfig",
          JsonParser.parseString(
              new String(
                  new TSerializer(new TJSONProtocol.Factory()).serialize(globalConfig),
                  StandardCharsets.UTF_8)));
      write(output.resolve("standalone-global-config.json"), globalIdentity);
      var response = client.getDataNodeConfiguration(-1);
      require(response.getStatus().getCode() == 200, "Cannot discover actual DataNodes");
      response.getDataNodeConfigurationMap().values().forEach(c -> nodes.add(c.getLocation()));
      require(
          nodes.size() == 2
              && nodes.stream().map(TDataNodeLocation::getDataNodeId).distinct().count() == 2,
          "Expected exactly two unique actual DataNodes");
      var schema = client.fetchTables(Map.of(database, Set.of(table)), (byte) 1);
      require(schema.getStatus().getCode() == 200, "Cannot fetch actual table schema");
      var tables = TsTableInternalRPCUtil.deserializeTsTableFetchResult(schema.getTableInfoMap());
      Map<String, List<TsTable>> using = new HashMap<>();
      tables.forEach((db, map) -> using.put(db, new ArrayList<>(map.values())));
      DataNodeTableCache.getInstance()
          .init(TsTableInternalRPCUtil.serializeTableInitializationInfo(using, Map.of()));
      JsonArray schemaDto = new JsonArray();
      tables.forEach(
          (db, tableMap) ->
              tableMap.forEach(
                  (name, schemaTable) -> {
                    JsonObject tableDto = new JsonObject();
                    tableDto.addProperty("database", db);
                    tableDto.addProperty("table", schemaTable.getTableName());
                    JsonArray columnsDto = new JsonArray();
                    schemaTable
                        .getColumnList()
                        .forEach(
                            column -> {
                              JsonObject columnDto = new JsonObject();
                              columnDto.addProperty("name", column.getColumnName());
                              columnDto.addProperty("type", column.getDataType().name());
                              columnDto.addProperty("category", column.getColumnCategory().name());
                              columnsDto.add(columnDto);
                            });
                    tableDto.add("columns", columnsDto);
                    schemaDto.add(tableDto);
                  }));
      write(output.resolve("actual-table-schema.json"), schemaDto);
    }
    nodes.sort(Comparator.comparingInt(TDataNodeLocation::getDataNodeId));
    require(
        nodes.stream().noneMatch(node -> node.getDataNodeId() == config.getDataNodeId()),
        "Standalone coordinator ID overlaps a real registered DataNode");
    write(
        output.resolve("standalone-coordinator-identity.json"),
        Map.of(
            "privateCoordinatorId", config.getDataNodeId(),
            "registeredAsDataNode", false,
            "internalPort", config.getInternalPort(),
            "collectorPort", collectorPort));
    write(output.resolve("actual-datanodes.json"), nodes);
    String password = System.getenv(manifest.get("passwordEnv").getAsString());
    require(password != null && !password.isEmpty(), "Missing JDBC password environment");
    String url = manifest.get("jdbcUrl").getAsString();
    Set<Integer> running = new HashSet<>();
    try (Connection connection =
            DriverManager.getConnection(url, manifest.get("username").getAsString(), password);
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SHOW DATANODES")) {
      ResultSetMetaData columns = rs.getMetaData();
      int id = -1, status = -1;
      for (int i = 1; i <= columns.getColumnCount(); i++) {
        String label = columns.getColumnLabel(i).toLowerCase().replace("_", "").replace(" ", "");
        if (label.equals("nodeid") || label.equals("datanodeid")) id = i;
        if (label.equals("status")) status = i;
      }
      require(id > 0 && status > 0, "SHOW DATANODES lacks identity/status");
      while (rs.next()) {
        require("Running".equalsIgnoreCase(rs.getString(status)), "DataNode is not running");
        require(running.add(rs.getInt(id)), "Duplicate registered DataNode ID");
      }
    }
    require(
        running.equals(
            new HashSet<>(nodes.stream().map(TDataNodeLocation::getDataNodeId).toList())),
        "Registered running identities differ from ConfigNode-discovered identities");
    List<Audit> audits = new ArrayList<>();
    var sync =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
    var async =
        new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.AsyncDataNodeInternalServiceClientPoolFactory(1));
    ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(2);
    MPPDataExchangeService exchange = MPPDataExchangeService.getInstance();
    Throwable primaryFailure = null;
    try {
      boolean requireNewMetrics = manifest.get("requireNewMetrics").getAsBoolean();
      if (requireNewMetrics) {
        JsonArray baseline = new JsonArray();
        for (TDataNodeLocation node : nodes) {
          Audit audit =
              new Audit(
                  node.getDataNodeId(),
                  node.getInternalEndPoint().getPort() + manifest.get("jmxPortOffset").getAsInt());
          audits.add(audit);
          JsonObject pool = audit.read("__idle__", "0.0");
          pool.addProperty("dataNodeId", node.getDataNodeId());
          baseline.add(pool);
        }
        write(output.resolve("resource-idle-before.json"), baseline);
      }
      exchange.start();
      SessionInfo session =
          new SessionInfo(1L, "root", ZoneId.of("UTC"), database, SqlDialect.TABLE);
      String sql = "SELECT s1,s2 FROM " + database + "." + table;
      QueryId query = new QueryId(manifest.get("queryId").getAsString());
      MPPQueryContext context =
          new MPPQueryContext(
              sql,
              query,
              session,
              new TEndPoint("127.0.0.1", collectorPort),
              new TEndPoint("127.0.0.1", config.getInternalPort()));
      context.setStartTime(System.currentTimeMillis());
      context.setTimeOut(TIMEOUT_MS);
      // Keep the ordinary SELECT output header while analyzing its two typed columns.
      context.setVerbose(true);
      var metadata = new TableMetadataImpl();
      var typeManager = new InternalTypeManager();
      var plannerContext = new PlannerContext(metadata, typeManager);
      var parser = new SqlParser();
      var planner =
          new TableModelPlanner(
              parser.createStatement(sql, ZoneId.of("UTC"), null),
              parser,
              metadata,
              scheduled,
              sync,
              async,
              new StatementRewriteFactory().getStatementRewrite(),
              new LogicalOptimizeFactory(plannerContext).getPlanOptimizers(),
              new DistributedOptimizeFactory(plannerContext).getPlanOptimizers(),
              AuthorityChecker.getAccessControl(),
              ignored -> nodes,
              List.of(),
              Map.of(),
              typeManager);
      var analysis = planner.analyze(context);
      var logical = planner.doLogicalPlan(analysis, context);
      DistributedQueryPlan distributed = planner.doDistributionPlan(analysis, logical, context);
      List<FragmentInstance> sources =
          distributed.getInstances().stream()
              .filter(f -> !scans(f.getFragment().getPlanNodeTree()).isEmpty())
              .toList();
      JsonObject analyzedDiagnostic = new JsonObject();
      Object partitionInfo = field(analysis, "getDataPartitionInfo");
      analyzedDiagnostic.addProperty("dataPartitionPresent", partitionInfo != null);
      if (partitionInfo != null) {
        analyzedDiagnostic.addProperty(
            "dataPartitionMap", String.valueOf(field(partitionInfo, "getDataPartitionMap")));
      }
      write(output.resolve("analysis-partition-diagnostic.json"), analyzedDiagnostic);
      JsonArray analyzedInstances = new JsonArray();
      StringBuilder analyzedPlanText = new StringBuilder();
      for (FragmentInstance analyzed : distributed.getInstances()) {
        JsonObject diagnostic = new JsonObject();
        diagnostic.addProperty("fragmentInstanceId", analyzed.getId().getFullId());
        diagnostic.addProperty("executorKind", analyzed.getExecutorType().getClass().getName());
        diagnostic.addProperty("storageExecutor", analyzed.getExecutorType().isStorageExecutor());
        diagnostic.addProperty("hostDataNodeId", analyzed.getHostDataNode().getDataNodeId());
        JsonArray scanDiagnostics = new JsonArray();
        for (DeviceTableScanNode scan : scans(analyzed.getFragment().getPlanNodeTree())) {
          JsonObject scanDiagnostic = new JsonObject();
          scanDiagnostic.addProperty("planNodeId", scan.getPlanNodeId().getId());
          scanDiagnostic.addProperty("deviceEntryCount", scan.getDeviceEntries().size());
          JsonArray devices = new JsonArray();
          scan.getDeviceEntries().forEach(device -> devices.add(device.getDeviceID().toString()));
          scanDiagnostic.add("devices", devices);
          scanDiagnostic.addProperty(
              "regionReplicaSetText", String.valueOf(scan.getRegionReplicaSet()));
          if (scan.getRegionReplicaSet() != null
              && scan.getRegionReplicaSet().getRegionId() != null
              && scan.getRegionReplicaSet().getDataNodeLocations() != null) {
            scanDiagnostic.add(
                "regionReplicaSet",
                JsonParser.parseString(
                    new String(
                        new TSerializer(new TJSONProtocol.Factory())
                            .serialize(scan.getRegionReplicaSet()),
                        StandardCharsets.UTF_8)));
          }
          scanDiagnostics.add(scanDiagnostic);
        }
        diagnostic.add("scans", scanDiagnostics);
        analyzedInstances.add(diagnostic);
        analyzedPlanText.append(analyzed).append("\n");
      }
      analyzedDiagnostic.add("instances", analyzedInstances);
      write(output.resolve("analyzed-distribution-diagnostic.json"), analyzedDiagnostic);
      Files.writeString(output.resolve("analyzed-distribution-plan.txt"), analyzedPlanText);
      require(sources.size() == 1, "SQL fixture did not produce exactly one real storage source");
      // The acceptance graph enables actual ExplainAnalyze statistics on every frozen instance.
      context.setExplainType(MPPQueryContext.ExplainType.EXPLAIN_ANALYZE);
      FrozenCase plan =
          buildSingleSource32BucketCase(
              context, sources.get(0), context.getTypeProvider(), nodes, output);
      JsonObject receipt = executeFrozenCase(plan, sync, async, output, requireNewMetrics);
      if (requireNewMetrics) resourceReceipts(plan, audits, output);
      else
        write(
            output.resolve("resource-protocol-scope.json"),
            Map.of(
                "status",
                "UNAVAILABLE_IN_FROZEN_A",
                "claim",
                "Baseline complete results and 34 FINISHED fragments only; no B resource cleanup"
                    + " PASS"));
      receipt.addProperty(
          "resourceProtocol",
          requireNewMetrics ? "B_REQUIRED_AND_CHECKED" : "UNAVAILABLE_IN_FROZEN_A");
      receipt.addProperty("status", "PASS");
      receipt.addProperty(
          "claim",
          requireNewMetrics
              ? "One real source, 32 physical hash channels, 32 terminal receive buckets, full result"
                  + " and independently checked resource cleanup"
              : "One real source, 32 physical hash channels, 32 terminal receive buckets and full"
                  + " result; resource cleanup observation unavailable in frozen A");
      write(output.resolve("receipt.json"), receipt);
    } catch (Exception | Error failure) {
      primaryFailure = failure;
      throw failure;
    } finally {
      List<Throwable> cleanupFailures = new ArrayList<>();
      try {
        JsonObject rawAudit =
            captureRawAudits(
                audits,
                manifest.get("queryId").getAsString(),
                manifest.get("requireNewMetrics").getAsBoolean(),
                primaryFailure);
        write(output.resolve("resource-finally-audit.json"), rawAudit);
        if (rawAudit.getAsJsonArray("captureErrors").size() > 0)
          cleanupFailures.add(
              new IllegalStateException(
                  "Final raw audit is incomplete; inspect resource-finally-audit.json"));
      } catch (Exception | Error failure) {
        cleanupFailures.add(failure);
      }
      for (Audit audit : audits) {
        try {
          audit.close();
        } catch (Exception | Error failure) {
          cleanupFailures.add(failure);
        }
      }
      try {
        exchange.stop();
      } catch (Exception | Error failure) {
        cleanupFailures.add(failure);
      }
      try {
        scheduled.shutdownNow();
      } catch (Exception | Error failure) {
        cleanupFailures.add(failure);
      }
      try {
        async.close();
      } catch (Exception | Error failure) {
        cleanupFailures.add(failure);
      }
      try {
        sync.close();
      } catch (Exception | Error failure) {
        cleanupFailures.add(failure);
      }
      if (primaryFailure != null) {
        for (Throwable failure : cleanupFailures)
          if (failure != primaryFailure) primaryFailure.addSuppressed(failure);
      } else if (!cleanupFailures.isEmpty()) {
        Throwable failure = cleanupFailures.get(0);
        for (int index = 1; index < cleanupFailures.size(); index++)
          if (failure != cleanupFailures.get(index))
            failure.addSuppressed(cleanupFailures.get(index));
        if (failure instanceof Exception) throw (Exception) failure;
        throw (Error) failure;
      }
    }
  }

  private static void loadFixture(JsonObject manifest, Path sql, Path output) throws Exception {
    require(!Files.exists(output), "Fixture output must be new");
    String password = System.getenv(manifest.get("passwordEnv").getAsString());
    require(password != null && !password.isEmpty(), "Missing JDBC password environment");
    try (Connection connection =
            DriverManager.getConnection(
                manifest.get("jdbcUrl").getAsString(),
                manifest.get("username").getAsString(),
                password);
        Statement statement = connection.createStatement()) {
      // This loader serves only the bound deterministic fixture, whose literals contain no
      // semicolons. Skip full-line comments so the license header is never executed as SQL.
      String statements =
          String.join(
              "\n",
              Files.readAllLines(sql).stream()
                  .filter(line -> !line.stripLeading().startsWith("--"))
                  .toList());
      for (String query : statements.split(";")) {
        if (!query.isBlank()) statement.execute(query);
      }
    }
    write(output, Map.of("status", "LOADED", "fixtureSha256", sha(Files.readAllBytes(sql))));
  }

  public static void main(String[] args) {
    int code = 0;
    try {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
      if (args.length == 4 && "--fixture".equals(args[0])) {
        loadFixture(
            JsonParser.parseString(Files.readString(Path.of(args[1]))).getAsJsonObject(),
            Path.of(args[2]),
            Path.of(args[3]));
      } else {
        require(
            args.length == 2,
            "Usage: StaticScanAcceptanceDriver SIDE_MANIFEST NEW_OUTPUT_DIRECTORY");
        run(
            JsonParser.parseString(Files.readString(Path.of(args[0]))).getAsJsonObject(),
            Path.of(args[1]));
      }
    } catch (Throwable failure) {
      failure.printStackTrace();
      code = 1;
    }
    // Production coordinators own background executors; the standalone tool has a process boundary.
    System.exit(code);
  }
}
