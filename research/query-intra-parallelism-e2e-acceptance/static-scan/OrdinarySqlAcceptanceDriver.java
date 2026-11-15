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
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.statistics.snapshot.RuntimeSnapshotAdapters;
import org.apache.iotdb.db.queryengine.statistics.snapshot.RuntimeSnapshotValidity;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.jdbc.IoTDBStatement;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceInfoReq;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceInfoResp;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TOperatorStatistics;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.thrift.TBase;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import java.util.zip.GZIPInputStream;

/** Ordinary client SQL and fixture loading. This class never builds or dispatches a query graph. */
public final class OrdinarySqlAcceptanceDriver {
  private static final Gson JSON = new GsonBuilder().serializeNulls().setPrettyPrinting().create();
  private static final Gson ROW = new GsonBuilder().serializeNulls().disableHtmlEscaping().create();
  private static final Set<String> TERMINAL = Set.of("FINISHED", "FAILED", "CANCELLED", "ABORTED");
  private static final List<String> FIXTURE_NAMES = List.of("time", "device_id", "g", "s1", "s2");
  private static final List<String> FIXTURE_TYPES =
      List.of("TIMESTAMP", "STRING", "INT64", "INT64", "INT64");

  private OrdinarySqlAcceptanceDriver() {}

  private static void require(boolean condition, String message) {
    if (!condition) throw new IllegalStateException(message);
  }

  private static String text(JsonObject object, String key) {
    require(object.has(key) && !object.get(key).isJsonNull(), "Missing JSON field: " + key);
    return object.get(key).getAsString();
  }

  private static String optional(JsonObject object, String key, String fallback) {
    return object.has(key) && !object.get(key).isJsonNull() ? object.get(key).getAsString() : fallback;
  }

  private static int integer(JsonObject object, String key, int fallback) {
    return object.has(key) ? object.get(key).getAsInt() : fallback;
  }

  private static boolean flag(JsonObject object, String key, boolean fallback) {
    return object.has(key) ? object.get(key).getAsBoolean() : fallback;
  }

  private static Path resolve(Path base, String value) {
    Path path = Path.of(value);
    return (path.isAbsolute() ? path : base.resolve(path)).toAbsolutePath().normalize();
  }

  private static JsonObject read(Path path) throws Exception {
    return JsonParser.parseString(Files.readString(path, StandardCharsets.UTF_8)).getAsJsonObject();
  }

  private static void write(Path path, Object value) throws Exception {
    Files.writeString(path, JSON.toJson(value) + "\n", StandardCharsets.UTF_8,
        StandardOpenOption.CREATE_NEW);
  }

  private static BufferedWriter log(Path path) throws Exception {
    return Files.newBufferedWriter(path, StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
  }

  private static void line(BufferedWriter writer, Object value) throws Exception {
    synchronized (writer) {
      writer.write(ROW.toJson(value));
      writer.newLine();
      writer.flush();
    }
  }

  private static String sha(byte[] bytes) throws Exception {
    return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static String sha(Path path) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (InputStream stream = Files.newInputStream(path)) {
      byte[] buffer = new byte[1024 * 1024];
      for (int count; (count = stream.read(buffer)) >= 0; ) digest.update(buffer, 0, count);
    }
    return HexFormat.of().formatHex(digest.digest());
  }

  private static JsonElement thrift(TBase<?, ?> object) throws Exception {
    return JsonParser.parseString(new String(
        new TSerializer(new TJSONProtocol.Factory()).serialize(object), StandardCharsets.UTF_8));
  }

  private static TEndPoint endpoint(JsonObject object) {
    return new TEndPoint(text(object, "ip"), integer(object, "port", -1));
  }

  private static String address(TEndPoint point) {
    return point.getIp() + ":" + point.getPort();
  }

  private static JsonObject endpoint(TEndPoint point) {
    JsonObject result = new JsonObject();
    result.addProperty("ip", point.getIp());
    result.addProperty("port", point.getPort());
    return result;
  }

  private static JsonObject redact(JsonObject object) {
    JsonObject result = object.deepCopy();
    if (result.has("password")) result.addProperty("password", "<redacted>");
    return result;
  }

  private static String password(JsonObject params, JsonObject deployment) {
    String environment = optional(params, "passwordEnv", optional(deployment, "passwordEnv", ""));
    if (!environment.isEmpty()) {
      String value = System.getenv(environment);
      require(value != null && !value.isEmpty(), "Missing password environment: " + environment);
      return value;
    }
    String value = optional(params, "password", optional(deployment, "password", ""));
    require(!value.isEmpty(), "Specify passwordEnv or password explicitly");
    return value;
  }

  private static String username(JsonObject params, JsonObject deployment) {
    return optional(params, "username", optional(deployment, "username", "root"));
  }

  static void run(String mode, Path parameterFile) throws Exception {
    parameterFile = parameterFile.toAbsolutePath().normalize();
    JsonObject params = read(parameterFile);
    require(integer(params, "schemaVersion", -1) == 1, "Unsupported parameter schemaVersion");
    require(mode.equals(text(params, "mode")), "CLI mode differs from parameter mode");
    Path base = parameterFile.getParent();
    Path manifest = resolve(base, text(params, "manifest"));
    Path output = resolve(base, text(params, "out"));
    require(!Files.exists(output), "Output directory must be fresh: " + output);
    Files.createDirectories(output.getParent());
    Files.createDirectory(output);
    JsonObject identity = new JsonObject();
    identity.addProperty("mode", mode);
    identity.addProperty("parametersPath", parameterFile.toString());
    identity.addProperty("parametersSha256", sha(parameterFile));
    identity.add("parameters", redact(params));
    identity.addProperty("manifestPath", manifest.toString());
    identity.addProperty("manifestSha256", sha(manifest));
    identity.addProperty("startedAt", Instant.now().toString());
    identity.addProperty("javaVersion", System.getProperty("java.version"));
    identity.addProperty("javaHome", System.getProperty("java.home"));
    identity.addProperty("classpath", System.getProperty("java.class.path"));
    identity.addProperty("driverCodeSource", OrdinarySqlAcceptanceDriver.class
        .getProtectionDomain().getCodeSource().getLocation().toString());
    identity.addProperty("pid", ProcessHandle.current().pid());
    write(output.resolve("execution-identity.json"), identity);
    Throwable primary = null;
    try {
      if ("load-runtime-fixture".equals(mode)) load(params, base, manifest, output);
      else if ("ordinary-sql".equals(mode)) ordinary(params, base, manifest, output);
      else throw new IllegalArgumentException("Unsupported ordinary driver mode: " + mode);
    } catch (Exception | Error failure) {
      primary = failure;
      JsonObject record = new JsonObject();
      record.addProperty("status", "FAILED");
      record.addProperty("failureClass", failure.getClass().getName());
      record.addProperty("failedAt", Instant.now().toString());
      record.add("failures", failures(failure));
      primary = evidence(output.resolve("failure.json"), record, primary);
    } finally {
      identity.addProperty("finishedAt", Instant.now().toString());
      identity.addProperty("status", primary == null ? "COMPLETED" : "FAILED");
      primary = evidence(output.resolve("execution-exit.json"), identity, primary);
    }
    if (primary != null) rethrow(primary);
  }

  private record Node(TDataNodeLocation actual, JsonObject configured) {
    int id() { return actual.getDataNodeId(); }
  }

  private static List<Node> discover(JsonObject deployment, Path output) throws Exception {
    List<TEndPoint> configs = new ArrayList<>();
    for (JsonElement element : deployment.getAsJsonArray("configNodes")) {
      JsonObject node = element.getAsJsonObject();
      configs.add(node.has("internalEndpoint")
          ? endpoint(node.getAsJsonObject("internalEndpoint"))
          : new TEndPoint(text(node, "host"), integer(node, "port", -1)));
    }
    require(!configs.isEmpty()
        && ConfigNodeInfo.getInstance().updateConfigNodeList(configs),
        "Cannot bind declared ConfigNode endpoints");
    List<TDataNodeLocation> actual = new ArrayList<>();
    Set<Integer> running = new HashSet<>();
    try (ConfigNodeClient client =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      var show = client.showDataNodes();
      write(output.resolve("config-node-show-datanodes.thrift.json"), thrift(show));
      require(show.getStatus().getCode() == 200 && show.isSetDataNodesInfoList(),
          "ConfigNode SHOW DATANODES failed");
      for (var info : show.getDataNodesInfoList()) {
        require("Running".equalsIgnoreCase(info.getStatus()), "A discovered DataNode is not Running");
        require(running.add(info.getDataNodeId()), "Duplicate actual DataNode ID");
      }
      var configuration = client.getDataNodeConfiguration(-1);
      write(output.resolve("config-node-datanode-configurations.thrift.json"), thrift(configuration));
      require(configuration.getStatus().getCode() == 200
          && configuration.isSetDataNodeConfigurationMap(), "ConfigNode node discovery failed");
      configuration.getDataNodeConfigurationMap().values()
          .forEach(value -> actual.add(value.getLocation()));
    }
    int expected = integer(deployment, "expectedDataNodeCount", 2);
    require(expected == 2 && actual.size() == expected && running.size() == expected,
        "Acceptance requires two actual running DataNodes");
    require(running.equals(new HashSet<>(actual.stream().map(TDataNodeLocation::getDataNodeId).toList())),
        "ConfigNode status IDs differ from actual configuration IDs");
    Map<String, JsonObject> configured = new HashMap<>();
    for (JsonElement element : deployment.getAsJsonArray("nodes")) {
      JsonObject value = element.getAsJsonObject();
      require(configured.put(address(endpoint(value.getAsJsonObject("internalEndpoint"))), value) == null,
          "Duplicate configured internal endpoint");
    }
    require(configured.size() == expected, "Deployment endpoint roster is incomplete");
    List<Node> nodes = new ArrayList<>();
    JsonArray normalized = new JsonArray();
    actual.sort(Comparator.comparingInt(TDataNodeLocation::getDataNodeId));
    for (TDataNodeLocation location : actual) {
      JsonObject value = configured.remove(address(location.getInternalEndPoint()));
      require(value != null, "Actual DataNode is absent from frozen deployment endpoints");
      require(location.getClientRpcEndPoint().equals(endpoint(value.getAsJsonObject("rpcEndpoint"))),
          "Actual DataNode RPC endpoint differs from deployment");
      JsonObject row = new JsonObject();
      row.addProperty("dataNodeId", location.getDataNodeId());
      row.addProperty("status", "Running");
      row.add("internalEndpoint", endpoint(location.getInternalEndPoint()));
      row.add("rpcEndpoint", endpoint(location.getClientRpcEndPoint()));
      row.add("exchangeEndpoint", endpoint(location.getMPPDataExchangeEndPoint()));
      row.addProperty("jmxHost", optional(value, "jmxHost",
          optional(deployment, "jmxHost", "127.0.0.1")));
      row.addProperty("jmxPort", integer(value, "jmxPort", -1));
      normalized.add(row);
      nodes.add(new Node(location, row));
    }
    require(configured.isEmpty(), "Declared endpoint does not belong to actual cluster");
    write(output.resolve("actual-datanodes.json"), normalized);
    return nodes;
  }

  private static void load(JsonObject params, Path base, Path fixturePath, Path output)
      throws Exception {
    JsonObject fixture = read(fixturePath);
    require("runtime_sql_fixture_v1".equals(text(fixture, "fixtureVersion"))
        && "gzip-tsv-v1".equals(text(fixture, "format"))
        && "\\N".equals(text(fixture, "nullMarker")), "Unexpected fixture format/version");
    JsonArray columns = fixture.getAsJsonArray("columns");
    require(columns.size() == 5, "Fixture schema changed");
    for (int index = 0; index < 5; index++) {
      require(FIXTURE_NAMES.get(index).equals(text(columns.get(index).getAsJsonObject(), "name"))
          && FIXTURE_TYPES.get(index).equals(text(columns.get(index).getAsJsonObject(), "type")),
          "Fixture column/type differs from admitted loader schema");
      require((index == 0 ? "TIME" : index == 1 ? "TAG" : "FIELD")
          .equals(text(columns.get(index).getAsJsonObject(), "category")),
          "Fixture column category differs from admitted loader schema");
    }
    Path deploymentPath = resolve(base, text(params, "deploymentManifest"));
    JsonObject deployment = read(deploymentPath);
    write(output.resolve("deployment-input.json"), Map.of(
        "path", deploymentPath.toString(), "sha256", sha(deploymentPath),
        "manifest", redact(deployment)));
    discover(deployment, output);
    try (Connection connection = DriverManager.getConnection(text(deployment, "jdbcUrl"),
        username(params, deployment), password(params, deployment))) {
      require(connection instanceof IoTDBConnection
          && ((IoTDBConnection) connection).getTimeFactor() == 1000,
          "Fixture loader requires actual millisecond cluster time precision");
      write(output.resolve("actual-time-precision.json"),
          Map.of("timeFactor", ((IoTDBConnection) connection).getTimeFactor(),
              "precision", "ms", "provenance", "actual JDBC open-session response"));
    }
    String database = text(fixture, "database"), table = text(fixture, "table");
    require(database.matches("[A-Za-z][A-Za-z0-9_]*") && table.matches("[A-Za-z][A-Za-z0-9_]*"),
        "Fixture database/table identifiers must be unquoted simple names");
    JsonArray partitions = fixture.getAsJsonArray("partitions");
    require(partitions.size() == 4, "Expected exactly four fixture partitions");
    JsonArray preflight = new JsonArray();
    for (int index = 0; index < partitions.size(); index++) {
      JsonObject partition = partitions.get(index).getAsJsonObject();
      Path path = resolve(fixturePath.getParent(), text(partition, "path"));
      String compressedSha = sha(path);
      require(integer(partition, "partitionIndex", -1) == index
          && Files.size(path) == partition.get("bytes").getAsLong()
          && compressedSha.equals(text(partition, "sha256")), "Compressed fixture identity mismatch");
      JsonObject row = new JsonObject();
      row.addProperty("partitionIndex", index);
      row.addProperty("path", path.toString());
      row.addProperty("sha256", compressedSha);
      preflight.add(row);
    }
    write(output.resolve("fixture-preflight.json"), preflight);
    int batchSize = integer(params, "tabletBatchRows", 4096);
    require(batchSize > 0 && batchSize <= 65536, "Invalid tablet batch size");
    List<String> urls = new ArrayList<>();
    if (deployment.has("tableSessionNodeUrls"))
      deployment.getAsJsonArray("tableSessionNodeUrls").forEach(value -> urls.add(value.getAsString()));
    else deployment.getAsJsonArray("nodes").forEach(value ->
        urls.add(address(endpoint(value.getAsJsonObject().getAsJsonObject("rpcEndpoint")))));
    long loaded = 0, batches = 0;
    try (ITableSession session = new TableSessionBuilder().nodeUrls(urls)
            .username(username(params, deployment)).password(password(params, deployment))
            .zoneId(ZoneId.of("UTC")).queryTimeoutInMs(180000).build();
        BufferedWriter batchLog = log(output.resolve("tablet-batches.jsonl"));
        BufferedWriter partitionLog = log(output.resolve("partitions.jsonl"))) {
      // Frozen generator setup creates a fresh database; existing fixtures must fail.
      JsonArray setup = fixture.getAsJsonArray("setupSql");
      require(setup != null && setup.size() == 3
          && !setup.get(0).getAsString().toUpperCase(java.util.Locale.ROOT).contains("IF NOT EXISTS"),
          "Fixture must declare exactly three fresh schema setup statements");
      for (JsonElement sql : setup) session.executeNonQueryStatement(sql.getAsString());
      write(output.resolve("schema-created.json"), Map.of(
          "setupSql", setup, "table", table, "columns", columns,
          "writerClass", session.getClass().getName(), "tabletClass", Tablet.class.getName(),
          "batchRows", batchSize, "crossDeviceBatches", false, "timePrecision", "ms"));
      Tablet tablet = new Tablet(table, List.of("device_id", "g", "s1", "s2"),
          List.of(TSDataType.STRING, TSDataType.INT64, TSDataType.INT64, TSDataType.INT64),
          List.of(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD),
          batchSize);
      for (JsonElement element : partitions) {
        JsonObject partition = element.getAsJsonObject();
        int partitionIndex = partition.get("partitionIndex").getAsInt();
        Path path = resolve(fixturePath.getParent(), text(partition, "path"));
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        long partitionRows = 0, partitionBatches = 0, cellRows = 0;
        long batchFirstTime = 0, batchLastTime = 0;
        String currentDevice = null;
        int cellIndex = 0;
        JsonArray cells = partition.getAsJsonArray("cells");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
            new DigestInputStream(new GZIPInputStream(Files.newInputStream(path)), digest),
            StandardCharsets.UTF_8))) {
          require("time\tdevice_id\tg\ts1\ts2".equals(reader.readLine()), "Unexpected fixture header");
          for (String raw; (raw = reader.readLine()) != null; ) {
            String[] values = raw.split("\t", -1);
            require(values.length == 5, "Malformed TSV row");
            long time = Long.parseLong(values[0]);
            String device = values[1];
            if (currentDevice != null && !currentDevice.equals(device)) {
              if (tablet.getRowSize() != 0) {
                session.insert(tablet);
                batches++; partitionBatches++;
                line(batchLog, Map.of("partitionIndex", partitionIndex, "deviceId", currentDevice,
                    "batchIndex", batches, "rows", tablet.getRowSize(),
                    "minTime", batchFirstTime, "maxTime", batchLastTime, "status", "RPC_SUCCEEDED"));
                tablet.reset();
              }
              require(cellRows == cells.get(cellIndex).getAsJsonObject().get("rows").getAsLong(),
                  "Fixture device cell row count changed");
              cellRows = 0; cellIndex++;
            }
            require(cellIndex < cells.size(), "Unexpected extra fixture cell");
            JsonObject cell = cells.get(cellIndex).getAsJsonObject();
            require(device.equals(text(cell, "deviceId"))
                && time == cell.get("minTime").getAsLong() + cellRows
                && time <= cell.get("maxTime").getAsLong(), "Fixture cell order/time changed");
            currentDevice = device;
            int row = tablet.getRowSize();
            if (row == 0) batchFirstTime = time;
            batchLastTime = time;
            tablet.addTimestamp(row, time);
            tablet.addValue("device_id", row, device);
            tablet.addValue("g", row, Long.valueOf(values[2]));
            tablet.addValue("s1", row, Long.valueOf(values[3]));
            tablet.addValue("s2", row, "\\N".equals(values[4]) ? null : Long.valueOf(values[4]));
            cellRows++; partitionRows++;
            if (tablet.getRowSize() == tablet.getMaxRowNumber()) {
              session.insert(tablet);
              batches++; partitionBatches++;
              line(batchLog, Map.of("partitionIndex", partitionIndex, "deviceId", currentDevice,
                  "batchIndex", batches, "rows", tablet.getRowSize(),
                  "minTime", batchFirstTime, "maxTime", batchLastTime, "status", "RPC_SUCCEEDED"));
              tablet.reset();
            }
          }
        }
        if (tablet.getRowSize() != 0) {
          session.insert(tablet);
          batches++; partitionBatches++;
          line(batchLog, Map.of("partitionIndex", partitionIndex, "deviceId", currentDevice,
              "batchIndex", batches, "rows", tablet.getRowSize(),
              "minTime", batchFirstTime, "maxTime", batchLastTime, "status", "RPC_SUCCEEDED"));
          tablet.reset();
        }
        require(cellIndex == cells.size() - 1
            && cellRows == cells.get(cellIndex).getAsJsonObject().get("rows").getAsLong()
            && partitionRows == partition.get("rows").getAsLong(), "Partition rows/cells incomplete");
        String uncompressed = HexFormat.of().formatHex(digest.digest());
        require(uncompressed.equals(text(partition, "uncompressedSha256")),
            "Uncompressed fixture identity mismatch");
        session.executeNonQueryStatement("FLUSH");
        loaded += partitionRows;
        line(partitionLog, Map.of("partitionIndex", partitionIndex,
            "compressedSha256", text(partition, "sha256"), "uncompressedSha256", uncompressed,
            "rows", partitionRows, "batches", partitionBatches, "flush", "RPC_SUCCEEDED"));
      }
    }
    require(loaded == fixture.get("totalRows").getAsLong(), "Loaded total differs from fixture");
    write(output.resolve("load-receipt.json"), Map.of(
        "status", "LOADED_AND_FLUSHED", "fixtureSha256", sha(fixturePath),
        "database", database, "table", table, "rows", loaded, "batches", batches,
        "partitionsFlushed", 4, "tableFilePlacementVerified", false,
        "tableRowCountVerified", false));
  }


  private static final class Audit implements AutoCloseable {
    final Node node;
    final JMXConnector connector;
    final MBeanServerConnection server;
    final ObjectName object;
    final String window;

    Audit(Node node, long duration) throws Exception {
      this.node = node;
      connector = JMXConnectorFactory.connect(new JMXServiceURL(
          "service:jmx:rmi:///jndi/rmi://" + text(node.configured(), "jmxHost") + ":"
              + integer(node.configured(), "jmxPort", -1) + "/jmxrmi"),
          Map.of("jmx.remote.x.request.waiting.timeout", 10000L));
      try {
        server = connector.getMBeanServerConnection();
        Set<ObjectName> names = new HashSet<>();
        for (ObjectName name : server.queryNames(null, null)) {
          for (MBeanOperationInfo operation : server.getMBeanInfo(name).getOperations()) {
            if ("openResourceAuditWindow".equals(operation.getName())
                && operation.getSignature().length == 1
                && "long".equals(operation.getSignature()[0].getType())) names.add(name);
          }
        }
        require(names.size() == 1, "Expected one actual resource audit MBean per DataNode");
        object = names.iterator().next();
        window = (String) call("openResourceAuditWindow", new Object[] {duration}, "long");
      } catch (Exception | Error failure) {
        try { connector.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
        throw failure;
      }
    }

    Object call(String operation, Object[] arguments, String... types) throws Exception {
      return server.invoke(object, operation, arguments, types);
    }

    JsonObject describe(String hash) throws Exception {
      return JsonParser.parseString((String) call("describeQueryExecutionBySqlHash",
          new Object[] {hash}, "java.lang.String")).getAsJsonObject();
    }

    JsonObject read(String queryId, String fragmentId) throws Exception {
      return JsonParser.parseString((String) call("readResourceAudit",
          new Object[] {window, queryId, fragmentId},
          "java.lang.String", "java.lang.String", "java.lang.String")).getAsJsonObject();
    }

    @Override
    public void close() throws Exception {
      Throwable primary = null;
      try { call("closeResourceAuditWindow", new Object[] {window}, "java.lang.String"); }
      catch (Exception | Error failure) { primary = failure; }
      try { connector.close(); }
      catch (Exception | Error failure) { primary = mergeFailure(primary, failure); }
      if (primary != null) rethrow(primary);
    }
  }

  private static final class Probe implements AutoCloseable {
    final List<Audit> audits;
    final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clients;
    final String sqlHash;
    final int period;
    final BufferedWriter rosterLog;
    final BufferedWriter statisticsLog;
    final BufferedWriter controlLog;
    final Map<String, String> layouts = new LinkedHashMap<>();
    final Map<String, java.util.TreeMap<Long, JsonObject>> events = new LinkedHashMap<>();
    final Map<String, JsonObject> groups = new LinkedHashMap<>();
    final Map<String, Long> deadlines = new LinkedHashMap<>();
    final Thread thread;
    volatile boolean stopping;
    volatile JsonObject roster;
    volatile Audit coordinator;
    volatile Throwable failure;
    private String rosterIdentity;

    Probe(List<Audit> audits,
        IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clients,
        String sqlHash, int period, Path output) throws Exception {
      this.audits = audits;
      this.clients = clients;
      this.sqlHash = sqlHash;
      this.period = period;
      BufferedWriter rosters = null, statistics = null, control = null;
      try {
        rosters = log(output.resolve("roster-polls.jsonl"));
        statistics = log(output.resolve("live-statistics.jsonl"));
        control = log(output.resolve("runtime-control.jsonl"));
      } catch (Exception | Error failure) {
        for (BufferedWriter writer : new BufferedWriter[] {rosters, statistics, control}) {
          if (writer == null) continue;
          try { writer.close(); }
          catch (Exception | Error closeFailure) { mergeFailure(failure, closeFailure); }
        }
        throw failure;
      }
      rosterLog = rosters;
      statisticsLog = statistics;
      controlLog = control;
      thread = new Thread(this::poll, "ordinary-sql-read-only-evidence");
      thread.setDaemon(true);
    }

    private String rosterIdentity(JsonObject value) {
      return text(value, "queryId") + "\n"
          + ROW.toJson(value.get("fragmentInstances")) + "\n" + ROW.toJson(value.get("edges"));
    }

    private void poll() {
      try {
        while (!stopping) {
          for (Audit audit : audits) {
            if (stopping) break;
            JsonObject value = audit.describe(sqlHash);
            line(rosterLog, Map.of("at", Instant.now().toString(), "dataNodeId", audit.node.id(),
                "response", value));
            String status = text(value, "status");
            require(!Set.of("AMBIGUOUS", "INVALID_SQL_HASH").contains(status),
                "SQL identity is ambiguous or invalid: " + value);
            if (flag(value, "rosterReady", false)) {
              require("READY".equals(status) && sqlHash.equals(text(value, "sqlSha256")),
                  "Ready roster is not bound to the exact executed SQL");
              String identity = rosterIdentity(value);
              if (rosterIdentity == null) {
                rosterIdentity = identity;
                coordinator = audit;
                roster = value.deepCopy();
              } else require(rosterIdentity.equals(identity), "Ready production roster changed");
            }
          }
          if (stopping) break;
          JsonObject captured = roster;
          if (captured != null) {
            for (JsonElement element : captured.getAsJsonArray("fragmentInstances")) {
              if (stopping) break;
              JsonObject fi = element.getAsJsonObject();
              try {
                TFetchFragmentInstanceStatisticsResp response = fetch(clients, captured, fi);
                line(statisticsLog, Map.of("at", Instant.now().toString(),
                    "fragmentInstanceId", text(fi, "fragmentInstanceId"), "rawThrift", thrift(response)));
                captureControls(captured, fi, response, "LIVE");
              } catch (IllegalStateException invariant) {
                throw invariant;
              } catch (Exception error) {
                line(statisticsLog, Map.of("at", Instant.now().toString(),
                    "fragmentInstanceId", text(fi, "fragmentInstanceId"),
                    "fetchFailure", error.toString()));
              }
            }
          }
          if (!stopping) Thread.sleep(period);
        }
      } catch (Exception | Error error) {
        failure = error;
      }
    }

    JsonObject awaitRoster(long timeoutMs) throws Exception {
      long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
      while (roster == null) {
        if (failure != null) rethrow(failure);
        require(System.nanoTime() < deadline, "Actual SQL roster was not captured before result drain");
        Thread.sleep(10);
      }
      if (failure != null) rethrow(failure);
      return roster.deepCopy();
    }

    void captureControls(JsonObject roster, JsonObject fi,
        TFetchFragmentInstanceStatisticsResp response, String phase) throws Exception {
      if (!response.isSetOperatorStatisticsMap()) return;
      for (TOperatorStatistics operator : response.getOperatorStatisticsMap().values()) {
        if (!operator.isSetSpecifiedInfo()) continue;
        String raw = operator.getSpecifiedInfo().get("__runtime_dop_control");
        if (raw == null) continue;
        JsonObject control = JsonParser.parseString(raw).getAsJsonObject();
        require(text(roster, "queryId").equals(text(control, "queryId"))
            && text(fi, "fragmentInstanceId").equals(text(control, "fragmentInstanceId")),
            "Runtime control evidence belongs to another SQL/FI");
        long deadline = control.get("originalDeadlineMillis").getAsLong();
        require(deadline > 0, "Runtime control omitted the original external deadline");
        Long previousDeadline = deadlines.putIfAbsent(text(fi, "fragmentInstanceId"), deadline);
        require(previousDeadline == null || previousDeadline == deadline,
            "A fragment changed its original external query deadline");
        line(controlLog, Map.of("at", Instant.now().toString(), "phase", phase,
            "fragmentInstanceId", text(fi, "fragmentInstanceId"), "snapshot", control));
        for (Map.Entry<String, JsonElement> entry : control.getAsJsonObject("groups").entrySet()) {
          JsonObject group = entry.getValue().getAsJsonObject();
          String key = text(fi, "fragmentInstanceId") + "/" + entry.getKey();
          JsonObject layout = new JsonObject();
          for (String field : List.of("sourcePlanNodeId", "lanePlanChecksum",
              "lanePlanIdentityChecksum", "layoutIdentitySupported", "logicalLaneCount",
              "laneIdentities")) layout.add(field, group.get(field));
          String frozen = ROW.toJson(layout);
          String prior = layouts.putIfAbsent(key, frozen);
          require(prior == null || prior.equals(frozen), "Runtime logical lane identities changed");
          groups.put(key, group.deepCopy());
          java.util.TreeMap<Long, JsonObject> sequence =
              events.computeIfAbsent(key, ignored -> new java.util.TreeMap<>());
          for (JsonElement event : group.getAsJsonArray("events")) {
            JsonObject value = event.getAsJsonObject();
            long number = value.get("sequence").getAsLong();
            JsonObject previous = sequence.putIfAbsent(number, value.deepCopy());
            require(previous == null || previous.equals(value), "Runtime event sequence changed");
          }
        }
      }
    }

    JsonArray observedActions(boolean requireGrowShrink, boolean requireOrderedOne)
        throws Exception {
      JsonArray proof = new JsonArray();
      boolean anyGrowShrink = false;
      for (Map.Entry<String, JsonObject> entry : groups.entrySet()) {
        String key = entry.getKey();
        JsonObject group = entry.getValue();
        boolean initial = false, grew = false, shrank = false;
        int previous = -1;
        for (JsonObject event : events.get(key).values()) {
          String outcome = text(event, "outcome");
          int observed = event.get("observedWorkers").getAsInt();
          if ("INITIAL_ACTIVATED".equals(outcome)) {
            initial = observed == 1;
            previous = observed;
          } else if ("APPLIED".equals(outcome)) {
            int requested = event.get("requestedWorkers").getAsInt();
            // Runtime membership settlement must match a positive requested live worker count.
            // A stop/retirement race returning zero cannot certify a running-query SHRINK.
            if (requested >= 1 && observed >= 1 && observed == requested) {
              if (initial && previous >= 0 && observed > previous) grew = true;
              if (grew && previous >= 0 && observed < previous) shrank = true;
              previous = observed;
            }
          }
        }
        boolean layout = flag(group, "layoutIdentitySupported", false);
        anyGrowShrink |= layout && initial && grew && shrank;
        if (requireOrderedOne) require(layout
            && group.get("logicalLaneCount").getAsInt() > 1
            && group.get("initialActivatedWorkers").getAsInt() == 1
            && group.get("maxAppliedWorkers").getAsInt() == 1,
            "Ordered query did not preserve real multi-lane initial-one membership");
        JsonObject item = group.deepCopy();
        item.addProperty("fragmentAndGroup", key);
        item.addProperty("initialOneEventObserved", initial);
        item.addProperty("appliedGrowObserved", grew);
        item.addProperty("laterAppliedShrinkObserved", shrank);
        item.add("allObservedEvents", ROW.toJsonTree(events.get(key).values()));
        proof.add(item);
      }
      require(!requireGrowShrink || anyGrowShrink,
          "Same SQL/FI/group INITIAL1 -> APPLIED grow -> APPLIED shrink was not exercised");
      require(!requireOrderedOne || !groups.isEmpty(), "No real ordered runtime group was observed");
      return proof;
    }

    @Override
    public void close() throws Exception {
      stopping = true;
      Throwable primary = null;
      try {
        thread.join(15000);
        if (thread.isAlive()) {
          thread.interrupt();
          thread.join(5000);
          if (thread.isAlive()) primary =
              new IllegalStateException("Read-only evidence polling did not exit");
        }
      } catch (InterruptedException interrupted) {
        stopping = true;
        thread.interrupt();
        Thread.currentThread().interrupt();
        primary = interrupted;
      }
      primary = mergeFailure(primary, failure);
      try { rosterLog.close(); }
      catch (Exception | Error closeFailure) { primary = mergeFailure(primary, closeFailure); }
      try { statisticsLog.close(); }
      catch (Exception | Error closeFailure) { primary = mergeFailure(primary, closeFailure); }
      // The caller retains controlLog for the sequential final frozen-snapshot collection.
      if (primary != null) rethrow(primary);
    }
  }

  private static void rethrow(Throwable failure) throws Exception {
    if (failure instanceof Exception) throw (Exception) failure;
    if (failure instanceof Error) throw (Error) failure;
    throw new RuntimeException(failure);
  }

  private static TFragmentInstanceId fragment(JsonObject roster, JsonObject fi) {
    return new TFragmentInstanceId(text(roster, "queryId"), integer(fi, "fragmentId", -1),
        text(fi, "instanceId"));
  }

  private static TFetchFragmentInstanceStatisticsResp fetch(
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clients,
      JsonObject roster, JsonObject fi) throws Exception {
    try (SyncDataNodeInternalServiceClient client =
        clients.borrowClient(endpoint(fi.getAsJsonObject("internalEndpoint")))) {
      client.setTimeout(5000);
      return client.fetchFragmentInstanceStatistics(
          new TFetchFragmentInstanceStatisticsReq(fragment(roster, fi)));
    }
  }

  private static TFragmentInstanceInfoResp fetchInfo(
      IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clients,
      JsonObject roster, JsonObject fi) throws Exception {
    try (SyncDataNodeInternalServiceClient client =
        clients.borrowClient(endpoint(fi.getAsJsonObject("internalEndpoint")))) {
      client.setTimeout(5000);
      return client.fetchFragmentInstanceInfo(
          new TFetchFragmentInstanceInfoReq(fragment(roster, fi)));
    }
  }

  private static boolean actualStatisticsNotRetained(
      JsonObject roster, JsonObject fi, TFetchFragmentInstanceStatisticsResp response) {
    return response.isSetStatus()
        && response.getStatus().getCode()
            == org.apache.iotdb.rpc.TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()
        && response.isSetFragmentInstanceId()
        && fragment(roster, fi).equals(response.getFragmentInstanceId())
        && response.isSetValidityReasons() && response.getValidityReasons().contains("NOT_FOUND");
  }

  private static JsonElement typed(ResultSet rows, int index, String type) throws Exception {
    JsonElement value;
    switch (type.toUpperCase(java.util.Locale.ROOT)) {
      case "TIMESTAMP", "INT64", "BIGINT" -> value = ROW.toJsonTree(rows.getLong(index));
      case "INT32", "INTEGER" -> value = ROW.toJsonTree(rows.getInt(index));
      case "FLOAT", "DOUBLE" -> {
        double number = rows.getDouble(index);
        require(rows.wasNull() || Double.isFinite(number), "Nonfinite numeric result is not canonical");
        value = ROW.toJsonTree(number);
      }
      case "BOOLEAN" -> value = ROW.toJsonTree(rows.getBoolean(index));
      case "TEXT", "STRING", "VARCHAR" -> value = ROW.toJsonTree(rows.getString(index));
      case "DATE" -> {
        java.sql.Date date = rows.getDate(index);
        value = date == null ? JsonNull.INSTANCE : ROW.toJsonTree(date.toLocalDate().toString());
      }
      case "BLOB", "BINARY" -> {
        byte[] bytes = rows.getBytes(index);
        value = bytes == null ? JsonNull.INSTANCE
            : ROW.toJsonTree(Base64.getEncoder().encodeToString(bytes));
      }
      default -> throw new IllegalStateException("Unsupported exact result type: " + type);
    }
    return rows.wasNull() ? JsonNull.INSTANCE : value;
  }

  private enum StatisticsPolicy {
    REQUIRE_FROZEN, ALLOW_UNRETAINED_CONTROL
  }

  private static StatisticsPolicy statisticsPolicy(JsonObject params, JsonObject deployment,
      ExpectedOutcome expected) throws Exception {
    StatisticsPolicy policy = StatisticsPolicy.valueOf(
        optional(params, "statisticsPolicy", "REQUIRE_FROZEN")
            .toUpperCase(java.util.Locale.ROOT));
    if (policy == StatisticsPolicy.REQUIRE_FROZEN) return policy;
    require(expected == ExpectedOutcome.SUCCESS
            && !flag(params, "requireGrowShrink", false)
            && !flag(params, "requireOrderedInitialOne", false)
            && !flag(params, "requireHashRepartition", false),
        "Unretained statistics policy cannot relax a negative or runtime/topology proof");
    require("control".equals(optional(deployment, "deployment", ""))
            && "fixed-one".equals(optional(deployment, "profile", ""))
            && deployment.has("runtimeProfile"),
        "Unretained statistics policy is restricted to explicit fixed-one control");
    JsonObject profile = deployment.getAsJsonObject("runtimeProfile");
    require(profile.has("runtimeEnabled") && !profile.get("runtimeEnabled").getAsBoolean()
            && integer(profile, "logicalLaneCeiling", -1) == 1,
        "Control manifest does not declare disabled runtime and fixed-one lanes");
    require(deployment.has("nodes") && !deployment.getAsJsonArray("nodes").isEmpty(),
        "Control manifest has no actual configuration bindings");
    for (JsonElement element : deployment.getAsJsonArray("nodes")) {
      JsonObject node = element.getAsJsonObject();
      Path path = Path.of(text(node, "configPath")).toAbsolutePath().normalize();
      require(sha(path).equals(text(node, "configSha256")),
          "Control configuration differs from its launcher manifest");
      java.util.Properties properties = new java.util.Properties();
      try (var reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
        properties.load(reader);
      }
      require("false".equals(properties.getProperty("enable_runtime_scan_parallelism"))
              && "false".equals(properties.getProperty("enable_dynamic_scan_parallelism")),
          "Permissive control policy requires both runtime and legacy feedback explicitly disabled");
    }
    return policy;
  }

  private enum ExpectedOutcome {
    SUCCESS, CANCELLED, DEADLINE, EXCEPTION
  }

  private static Throwable mergeFailure(Throwable primary, Throwable next) {
    if (next == null) return primary;
    if (primary == null) return next;
    if (primary != next) primary.addSuppressed(next);
    return primary;
  }

  private static Throwable evidence(Path path, Object value, Throwable primary) {
    try {
      write(path, value);
    } catch (Exception | Error failedWrite) {
      primary = mergeFailure(primary, failedWrite);
    }
    return primary;
  }

  private static JsonArray failures(Throwable failure) {
    JsonArray result = new JsonArray();
    Set<Throwable> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    java.util.ArrayDeque<Throwable> pending = new java.util.ArrayDeque<>();
    if (failure != null) pending.add(failure);
    while (!pending.isEmpty()) {
      Throwable current = pending.removeFirst();
      if (!seen.add(current)) continue;
      JsonObject value = new JsonObject();
      value.addProperty("class", current.getClass().getName());
      value.addProperty("message", current.getMessage());
      if (current instanceof java.sql.SQLException sql) {
        value.addProperty("sqlErrorCode", sql.getErrorCode());
        value.addProperty("sqlState", sql.getSQLState());
        value.add("sqlCodeObservation", sqlCodeObservation(sql));
        if (sql.getNextException() != null) pending.add(sql.getNextException());
      }
      result.add(value);
      if (current.getCause() != null) pending.add(current.getCause());
      for (Throwable suppressed : current.getSuppressed()) pending.add(suppressed);
    }
    return result;
  }

  private static final java.util.regex.Pattern LEGACY_SQL_STATUS =
      java.util.regex.Pattern.compile("\\A(715|720|751): [\\s\\S]+\\z");

  private static JsonObject sqlCodeObservation(java.sql.SQLException sql) {
    JsonObject observed = new JsonObject();
    int raw = sql.getErrorCode();
    observed.addProperty("class", sql.getClass().getName());
    observed.addProperty("rawSqlErrorCode", raw);
    observed.addProperty("sqlState", sql.getSQLState());
    observed.addProperty("message", sql.getMessage());
    observed.add("parsedSqlErrorCode", JsonNull.INSTANCE);
    if (raw != 0) {
      observed.addProperty("effectiveSqlErrorCode", raw);
      observed.addProperty("provenance", "JDBC_VENDOR_CODE");
      return observed;
    }
    StackTraceElement[] trace = sql.getStackTrace();
    boolean legacyOrigin = sql.getClass() == java.sql.SQLException.class
        && sql.getCause() == null && sql.getSQLState() == null && trace.length > 0
        && "org.apache.iotdb.jdbc.IoTDBJDBCResultSet".equals(trace[0].getClassName())
        && "next".equals(trace[0].getMethodName());
    observed.addProperty("verifiedLegacyResultSetNextOrigin", legacyOrigin);
    if (trace.length > 0) observed.addProperty("originFrame", trace[0].toString());
    java.util.regex.Matcher match = LEGACY_SQL_STATUS.matcher(
        sql.getMessage() == null ? "" : sql.getMessage());
    if (legacyOrigin && match.matches()) {
      int parsed = Integer.parseInt(match.group(1));
      observed.addProperty("parsedSqlErrorCode", parsed);
      observed.addProperty("effectiveSqlErrorCode", parsed);
      observed.addProperty("provenance", "IOTDB_LEGACY_RESULTSET_NEXT_MESSAGE_PREFIX");
    } else {
      observed.add("effectiveSqlErrorCode", JsonNull.INSTANCE);
      observed.addProperty("provenance", "UNAVAILABLE");
    }
    return observed;
  }

  private static JsonObject sqlCodeEvidence(Throwable failure, int code) {
    JsonArray observations = new JsonArray();
    Set<Throwable> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    java.util.ArrayDeque<Throwable> pending = new java.util.ArrayDeque<>();
    if (failure != null) pending.add(failure);
    boolean vendorPresent = false, vendorMatch = false, vendorConflict = false;
    boolean parsedMatch = false, parsedConflict = false;
    while (!pending.isEmpty()) {
      Throwable current = pending.removeFirst();
      if (!seen.add(current)) continue;
      if (current instanceof java.sql.SQLException sql) {
        JsonObject observed = sqlCodeObservation(sql);
        observations.add(observed);
        if (sql.getErrorCode() != 0) {
          vendorPresent = true;
          vendorMatch |= sql.getErrorCode() == code;
          vendorConflict |= sql.getErrorCode() != code;
        } else if (!observed.get("parsedSqlErrorCode").isJsonNull()) {
          int parsed = observed.get("parsedSqlErrorCode").getAsInt();
          parsedMatch |= parsed == code;
          parsedConflict |= parsed != code;
        }
        if (sql.getNextException() != null) pending.add(sql.getNextException());
      }
      if (current.getCause() != null) pending.add(current.getCause());
    }
    JsonObject result = new JsonObject();
    result.addProperty("expectedSqlErrorCode", code);
    result.addProperty("vendorCodePresent", vendorPresent);
    result.addProperty("conflictingSqlCodes", vendorConflict || parsedConflict);
    result.addProperty("matched", code > 0 && !vendorConflict && !parsedConflict
        && (vendorPresent ? vendorMatch : parsedMatch));
    result.addProperty("authority", vendorPresent ? "JDBC_VENDOR_CODE"
        : parsedMatch && !parsedConflict ? "VERIFIED_LEGACY_RESULTSET_NEXT_PREFIX" : "UNAVAILABLE");
    result.add("observations", observations);
    return result;
  }

  private static boolean hasSqlCode(Throwable failure, int code) {
    return sqlCodeEvidence(failure, code).get("matched").getAsBoolean();
  }

  /**
   * Offline regression through the actual JDBC ResultSet.next and RPC status validator,
   * followed by strict boundary/precedence checks. No socket, server or query is opened.
   */
  static void selfTestSqlErrors() throws Exception {
    int checks = 0;
    JsonArray wrapped = new JsonArray();
    java.sql.SQLException actualLegacy = null;
    for (int status : new int[] {715, 720, 751}) {
      java.util.concurrent.atomic.AtomicInteger fetches = new java.util.concurrent.atomic.AtomicInteger();
      org.apache.iotdb.service.rpc.thrift.IClientRPCService.Iface client =
          (org.apache.iotdb.service.rpc.thrift.IClientRPCService.Iface)
              java.lang.reflect.Proxy.newProxyInstance(
                  OrdinarySqlAcceptanceDriver.class.getClassLoader(),
                  new Class<?>[] {org.apache.iotdb.service.rpc.thrift.IClientRPCService.Iface.class},
                  (proxy, method, arguments) -> {
                    if ("fetchResultsV2".equals(method.getName())) {
                      fetches.incrementAndGet();
                      var response = new org.apache.iotdb.service.rpc.thrift.TSFetchResultsResp();
                      response.setStatus(new org.apache.iotdb.common.rpc.thrift.TSStatus(status)
                          .setMessage("offline synthetic fetch failure"));
                      return response;
                    }
                    if ("closeOperation".equals(method.getName())) {
                      return new org.apache.iotdb.common.rpc.thrift.TSStatus(200);
                    }
                    throw new UnsupportedOperationException("Unexpected offline RPC: " + method);
                  });
      IoTDBConnection connection = new IoTDBConnection() {
        @Override public boolean isClosed() { return false; }
      };
      var constructor = IoTDBStatement.class.getDeclaredConstructor(
          IoTDBConnection.class, org.apache.iotdb.service.rpc.thrift.IClientRPCService.Iface.class,
          long.class, ZoneId.class, int.class, long.class);
      constructor.setAccessible(true);
      IoTDBStatement statement =
          constructor.newInstance(connection, client, 1L, ZoneId.of("UTC"), 1, 2L);
      try (ResultSet rows = new org.apache.iotdb.jdbc.IoTDBJDBCResultSet(
          statement, List.of("v"), List.of("INT64"), Map.of("v", 0), true, client,
          "OFFLINE_SELF_TEST_NO_SERVER_QUERY", 3L, 1L, List.of(), null, 1000L,
          true, ZoneId.of("UTC"), StandardCharsets.UTF_8, true, List.of(0))) {
        try {
          rows.next();
          throw new AssertionError("Expected actual JDBC next to wrap the RPC error");
        } catch (java.sql.SQLException failure) {
          require(fetches.get() == 1 && failure.getErrorCode() == 0 && failure.getCause() == null,
              "Selected JDBC does not reproduce the pinned legacy next wrapper");
          require(hasSqlCode(failure, status), "Actual legacy JDBC fetch code was not recognized");
          for (int other : new int[] {715, 720, 751})
            if (other != status) require(!hasSqlCode(failure, other), "Wrong status was accepted");
          wrapped.add(sqlCodeEvidence(failure, status));
          if (status == 715) actualLegacy = failure;
          checks += 3;
        }
      }
    }
    require(actualLegacy != null, "Missing real legacy exception regression fixture");
    StackTraceElement[] legacyStack = actualLegacy.getStackTrace();
    // Authoritative nonzero fields win over any diagnostic text or fallback chain.
    java.sql.SQLException structured = new org.apache.iotdb.jdbc.IoTDBSQLException(
        "720: misleading diagnostic", new org.apache.iotdb.common.rpc.thrift.TSStatus(715)
            .setMessage("structured server status"));
    require(hasSqlCode(structured, 715) && !hasSqlCode(structured, 720),
        "Structured vendor code did not take precedence over message");
    require("JDBC_VENDOR_CODE".equals(
        sqlCodeObservation(structured).get("provenance").getAsString()),
        "Structured status provenance was lost");
    checks++;
    java.sql.SQLException mismatch = new java.sql.SQLException("715: misleading", null, 720);
    mismatch.setStackTrace(legacyStack);
    mismatch.setNextException(actualLegacy);
    require(!hasSqlCode(mismatch, 715), "Fallback overrode a conflicting nonzero vendor field");
    checks++;
    java.sql.SQLException conflict = new java.sql.SQLException("explicit", null, 751);
    structured.setNextException(conflict);
    require(!hasSqlCode(structured, 715), "Conflicting chained vendor fields were accepted");
    checks++;
    for (String message : Arrays.asList(
        null, "", "715: ", "715:no space", " 715: not at start", "prefix 715: code",
        "7150: extra digit", "999: payload includes 715: code", "720: wrong expected code")) {
      java.sql.SQLException invalid = new java.sql.SQLException(message);
      invalid.setStackTrace(legacyStack);
      require(!hasSqlCode(invalid, 715), "Unrecognized legacy prefix was accepted");
      checks++;
    }
    require(!hasSqlCode(new java.sql.SQLException("715: generic client error"), 715),
        "A generic SQLException message was accepted without actual JDBC origin");
    checks++;
    java.sql.SQLException subclass = new java.sql.SQLException("715: subclass") {};
    subclass.setStackTrace(legacyStack);
    require(!hasSqlCode(subclass, 715), "An arbitrary SQLException subclass used legacy fallback");
    checks++;
    require(!hasSqlCode(new RuntimeException("715: arbitrary string"), 715),
        "A non-SQL diagnostic string was accepted");
    checks++;
    require(hasSqlCode(new RuntimeException("outer", actualLegacy), 715),
        "A real legacy exception in the cause chain was lost");
    checks++;
    JsonObject result = new JsonObject();
    result.addProperty("status", "PASS_OFFLINE_ACTUAL_JDBC_ERROR_CONTRACT");
    result.addProperty("checks", checks);
    result.addProperty("jdbcClass", org.apache.iotdb.jdbc.IoTDBJDBCResultSet.class.getName());
    result.addProperty("jdbcCodeSource", org.apache.iotdb.jdbc.IoTDBJDBCResultSet.class
        .getProtectionDomain().getCodeSource().getLocation().toString());
    result.add("actualLegacyRpcWrapping", wrapped);
    result.addProperty("scope", "Offline real JDBC/RPC-wrapper regression only; no SQL/runtime acceptance.");
    System.out.println(JSON.toJson(result));
  }

  private static java.util.Properties jdbcProperties(
      JsonObject params, JsonObject deployment, int timeout) {
    java.util.Properties properties = new java.util.Properties();
    properties.setProperty("user", username(params, deployment));
    properties.setProperty("password", password(params, deployment));
    properties.setProperty("sql_dialect", "table");
    properties.setProperty("time_zone", "UTC");
    properties.setProperty("network_timeout", Integer.toString((timeout + 15) * 1000));
    return properties;
  }

  /** Uses only existing ordinary SQL cancellation, after observing this query's real FI activity. */
  private static final class CancelAction implements AutoCloseable {
    final JsonObject params;
    final JsonObject deployment;
    final Probe probe;
    final Thread thread;
    volatile boolean stopping;
    volatile boolean applied;
    volatile Throwable failure;
    volatile JsonObject receipt = new JsonObject();

    CancelAction(JsonObject params, JsonObject deployment, Probe probe) {
      this.params = params;
      this.deployment = deployment;
      this.probe = probe;
      thread = new Thread(this::run, "ordinary-sql-targeted-kill");
      thread.setDaemon(true);
    }

    private void run() {
      JsonObject observation = new JsonObject();
      observation.addProperty("action", "KILL_QUERY");
      observation.addProperty("sqlSha256", probe.sqlHash);
      observation.addProperty("startedAt", Instant.now().toString());
      try {
        int limit = integer(params, "interventionTimeoutSeconds", 30);
        int delay = integer(params, "cancelAfterActivityMs", 500);
        require(limit > 0 && limit <= 120 && delay >= 0 && delay <= 30000,
            "Invalid targeted cancellation timing");
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(limit);
        JsonObject captured = probe.awaitRoster(TimeUnit.SECONDS.toMillis(limit));
        String queryId = text(captured, "queryId");
        require(queryId.matches("[A-Za-z0-9_.-]+"), "Unsafe actual internal query ID");
        observation.addProperty("queryId", queryId);
        JsonArray witnesses = new JsonArray();
        boolean active = false;
        while (!stopping) {
          if (probe.failure != null) rethrow(probe.failure);
          witnesses = new JsonArray();
          boolean everyExecuted = true;
          boolean liveSource = false;
          for (JsonElement element : captured.getAsJsonArray("fragmentInstances")) {
            if (stopping) break;
            JsonObject fi = element.getAsJsonObject();
            TFetchFragmentInstanceStatisticsResp response;
            try {
              response = fetch(probe.clients, captured, fi);
            } catch (Exception unavailable) {
              everyExecuted = false;
              JsonObject missing = new JsonObject();
              missing.addProperty("fragmentInstanceId", text(fi, "fragmentInstanceId"));
              missing.addProperty("fetchFailure", unavailable.toString());
              witnesses.add(missing);
              continue;
            }
            boolean identity = response.isSetFragmentInstanceId()
                && fragment(captured, fi).equals(response.getFragmentInstanceId())
                && response.isSetSourceNodeId()
                && Integer.toString(fi.get("dataNodeId").getAsInt()).equals(response.getSourceNodeId());
            boolean positive = identity && response.isSetStatus() && response.getStatus().getCode() == 200
                && response.isSetDriverSnapshots() && !response.getDriverSnapshots().isEmpty()
                && response.getResourceObjectCount() > 0
                && response.getResourceUniqueMemoryKeyCount() > 0;
            everyExecuted &= positive;
            liveSource |= positive && flag(fi, "containsDeviceScan", false)
                && response.isSetState() && !TERMINAL.contains(response.getState());
            witnesses.add(MapToJson(Map.of("fragmentInstanceId", text(fi, "fragmentInstanceId"),
                "positiveExecutionEvidence", positive, "rawThrift", thrift(response))));
          }
          if (everyExecuted && liveSource && !stopping) { active = true; break; }
          require(System.nanoTime() < deadline,
              "Target query did not expose real source activity before cancellation");
          Thread.sleep(25);
        }
        require(active && !stopping, "Targeted cancellation was not exercised");
        observation.add("preCancelActualFiWitnesses", witnesses);
        observation.addProperty("activityObservedAt", Instant.now().toString());
        long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(delay);
        while (!stopping && System.nanoTime() < until) Thread.sleep(10);
        require(!stopping && System.nanoTime() < deadline, "Cancellation deadline expired");
        JsonObject current = probe.coordinator.describe(probe.sqlHash);
        require(flag(current, "rosterReady", false)
                && queryId.equals(text(current, "queryId"))
                && probe.sqlHash.equals(text(current, "sqlSha256"))
                && Set.of("RUNNING", "DISPATCHING").contains(text(current, "state")),
            "Target query was no longer the same live ordinary SQL before KILL QUERY");
        String killSql = "KILL QUERY '" + queryId + "'";
        observation.addProperty("killSql", killSql);
        observation.addProperty("killStartedAt", Instant.now().toString());
        try (Connection connection = DriverManager.getConnection(
                text(deployment, "jdbcUrl"), jdbcProperties(params, deployment, 15));
            Statement statement = connection.createStatement()) {
          statement.setQueryTimeout(15);
          statement.execute(killSql);
          applied = true;
          observation.addProperty("killAcknowledged", true);
          observation.addProperty("acknowledgmentProvenance", "existing ordinary SQL execute completed");
          observation.addProperty("killAcknowledgedAt", Instant.now().toString());
        }
        observation.addProperty("status", "TARGETED_KILL_ACKNOWLEDGED");
      } catch (Exception | Error error) {
        failure = error;
        observation.addProperty("status", applied ? "KILL_ACKNOWLEDGED_WITH_CLIENT_FAILURE"
            : "TARGETED_KILL_FAILED_OR_NOT_EXERCISED");
        observation.add("failures", failures(error));
      } finally {
        observation.addProperty("finishedAt", Instant.now().toString());
        receipt = observation;
      }
    }

    @Override
    public void close() throws Exception {
      Throwable primary = null;
      try {
        thread.join(15000);
        if (thread.isAlive()) {
          stopping = true;
          thread.interrupt();
          thread.join(5000);
          if (thread.isAlive()) primary = new IllegalStateException("Targeted KILL worker did not exit");
        }
      } catch (InterruptedException interrupted) {
        stopping = true;
        thread.interrupt();
        Thread.currentThread().interrupt();
        primary = interrupted;
      }
      primary = mergeFailure(primary, failure);
      if (!applied) primary = mergeFailure(primary,
          new IllegalStateException("Targeted KILL QUERY was not acknowledged"));
      if (primary != null) rethrow(primary);
    }
  }

  private static JsonObject execute(JsonObject params, JsonObject deployment,
      Path sqlFile, Path output, Probe probe) throws Exception {
    byte[] input = Files.readAllBytes(sqlFile);
    require(input.length > 0 && input.length <= 1024 * 1024, "SQL file must be nonempty and bounded");
    String original = new String(input, StandardCharsets.UTF_8);
    String sql = original + "\n/* runtime_sql_acceptance_run="
        + text(params, "_runMarker") + " */";
    require(sha(sql.getBytes(StandardCharsets.UTF_8)).equals(probe.sqlHash), "Exact SQL binding changed");
    Files.write(output.resolve("input.sql"), input, StandardOpenOption.CREATE_NEW);
    Files.writeString(output.resolve("executed.sql"), sql, StandardCharsets.UTF_8,
        StandardOpenOption.CREATE_NEW);
    ExpectedOutcome expected = ExpectedOutcome.valueOf(
        optional(params, "expectedOutcome", "SUCCESS").toUpperCase(java.util.Locale.ROOT));
    int timeout = integer(params, "queryTimeoutSeconds", expected == ExpectedOutcome.DEADLINE ? 1 : 180);
    int fetchSize = integer(params, "fetchSize", 1024);
    require(timeout > 0 && timeout <= 600 && fetchSize > 0 && fetchSize <= 65536,
        "Invalid query timeout/fetch size");
    java.util.Properties properties = jdbcProperties(params, deployment, timeout);
    long rowCount = 0;
    long started = System.nanoTime();
    MessageDigest resultDigest = MessageDigest.getInstance("SHA-256");
    JsonObject statementIdentity = new JsonObject();
    try (Connection connection = DriverManager.getConnection(text(deployment, "jdbcUrl"), properties);
        Statement statement = connection.createStatement();
        var writer = new java.io.BufferedOutputStream(
            Files.newOutputStream(output.resolve("result.rows.jsonl"), StandardOpenOption.CREATE_NEW),
            1024 * 1024)) {
      require(connection instanceof IoTDBConnection, "Expected actual IoTDB JDBC connection");
      require(((IoTDBConnection) connection).getTimeFactor() == 1000,
          "Fixture acceptance requires actual millisecond time precision");
      statement.setFetchSize(fetchSize);
      statement.setQueryTimeout(timeout);
      statementIdentity.addProperty("jdbcUrl", text(deployment, "jdbcUrl"));
      statementIdentity.addProperty("connectionClass", connection.getClass().getName());
      statementIdentity.addProperty("statementClass", statement.getClass().getName());
      statementIdentity.addProperty("queryTimeoutSeconds", timeout);
      statementIdentity.addProperty("fetchSize", fetchSize);
      statementIdentity.addProperty("inputSqlSha256", sha(input));
      statementIdentity.addProperty("executedSqlSha256", probe.sqlHash);
      try (ResultSet rows = statement.executeQuery(sql)) {
        statementIdentity.addProperty("executeResponseAt", Instant.now().toString());
        JsonObject captured = probe.awaitRoster(integer(params, "rosterTimeoutMs", 15000));
        write(output.resolve("roster.json"), captured);
        statementIdentity.addProperty("actualInternalQueryId", text(captured, "queryId"));
        if (statement instanceof IoTDBStatement actual) {
          statementIdentity.addProperty("actualSessionId", actual.getSessionId());
          statementIdentity.addProperty("actualStatementId", actual.getStmtId());
          Field field = IoTDBStatement.class.getDeclaredField("queryId");
          field.setAccessible(true);
          long numeric = field.getLong(actual);
          statementIdentity.addProperty("actualRpcQueryId", numeric);
          statementIdentity.addProperty("rpcQueryIdProvenance", "actual IoTDBStatement.queryId field");
          for (Audit audit : List.of(probe.coordinator)) {
            JsonObject mapping = JsonParser.parseString((String) audit.call(
                "describeQueryExecution", new Object[] {numeric}, "long")).getAsJsonObject();
            line(probe.rosterLog, Map.of("at", Instant.now().toString(),
                "dataNodeId", audit.node.id(), "numericQueryLookup", mapping));
            if (flag(mapping, "rosterReady", false)) require(
                text(captured, "queryId").equals(text(mapping, "queryId"))
                    && probe.sqlHash.equals(text(mapping, "sqlSha256")),
                "Numeric RPC ID and SQL hash resolve to different actual queries");
          }
        }
        write(output.resolve("statement-identity.json"), statementIdentity);
        ResultSetMetaData metadata = rows.getMetaData();
        List<String> types = new ArrayList<>();
        JsonArray schema = new JsonArray();
        for (int index = 1; index <= metadata.getColumnCount(); index++) {
          JsonObject column = new JsonObject();
          String type = metadata.getColumnTypeName(index);
          types.add(type);
          column.addProperty("index", index);
          column.addProperty("name", metadata.getColumnName(index));
          column.addProperty("label", metadata.getColumnLabel(index));
          column.addProperty("type", type);
          column.addProperty("jdbcType", metadata.getColumnType(index));
          column.addProperty("nullable", metadata.isNullable(index));
          schema.add(column);
        }
        write(output.resolve("result.schema.json"), Map.of(
            "columns", schema, "encoding", "typed-json-array-lf",
            "timestampRepresentation", "server-millisecond-integer",
            "rowOrder", "actual JDBC result sequence", "sqlSha256", probe.sqlHash));
        while (rows.next()) {
          JsonArray values = new JsonArray();
          for (int index = 1; index <= types.size(); index++)
            values.add(typed(rows, index, types.get(index - 1)));
          byte[] encoded = (ROW.toJson(values) + "\n").getBytes(StandardCharsets.UTF_8);
          writer.write(encoded);
          resultDigest.update(encoded);
          rowCount++;
          if (rowCount == 1) {
            int defaultPause = expected == ExpectedOutcome.DEADLINE ? 1500
                : expected == ExpectedOutcome.CANCELLED
                    ? Math.min(30000, Math.max(2000,
                        integer(params, "cancelAfterActivityMs", 500) + 1500)) : 0;
            int pause = integer(params, "pauseAfterFirstRowMs", defaultPause);
            require(pause >= 0 && pause <= 30000, "Invalid deliberate first-row pause");
            if (pause > 0) Thread.sleep(pause);
          }
          require(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(timeout + 20L),
              "Client full-result drain exceeded its wall bound");
        }
      }
    } catch (Exception | Error queryFailure) {
      JsonObject partial = new JsonObject();
      partial.addProperty("status", "PARTIAL_TYPED_RESULT_OR_EXECUTION_FAILED");
      partial.addProperty("rows", rowCount);
      partial.addProperty("sha256", HexFormat.of().formatHex(resultDigest.digest()));
      partial.addProperty("sha256Kind", "ACTUAL_TYPED_JDBC_SEQUENCE_PREFIX");
      partial.addProperty("fullResultDrained", false);
      partial.addProperty("oracleCompared", false);
      partial.addProperty("multisetCompared", false);
      partial.addProperty("sqlSha256", probe.sqlHash);
      partial.addProperty("elapsedNanos", System.nanoTime() - started);
      partial.add("failures", failures(queryFailure));
      Throwable combined = evidence(output.resolve("result-partial-receipt.json"), partial, queryFailure);
      combined = evidence(output.resolve("statement-execution-failure.json"),
          Map.of("identity", statementIdentity, "failures", failures(queryFailure)), combined);
      rethrow(combined);
      throw new AssertionError();
    }
    JsonObject result = new JsonObject();
    result.addProperty("status", "FULL_TYPED_RESULT_DRAINED");
    result.addProperty("rows", rowCount);
    result.addProperty("sha256", HexFormat.of().formatHex(resultDigest.digest()));
    result.addProperty("sequencePreserved", true);
    result.addProperty("sha256Kind", "ACTUAL_TYPED_JDBC_SEQUENCE");
    result.addProperty("fullResultDrained", true);
    result.addProperty("oracleCompared", false);
    result.addProperty("multisetCompared", false);
    result.addProperty("sqlSha256", probe.sqlHash);
    result.addProperty("elapsedNanos", System.nanoTime() - started);
    write(output.resolve("result-receipt.json"), result);
    return result;
  }


  private static String hashSourceKey(JsonObject edge) {
    return ROW.toJson(List.of(text(edge, "sourceFragmentInstanceId"),
        text(edge, "sourcePlanNodeId")));
  }

  private static String connectionKey(JsonObject edge) {
    return ROW.toJson(List.of(text(edge, "sourceFragmentInstanceId"),
        text(edge, "sourcePlanNodeId"), edge.get("channelIndex").getAsInt(),
        text(edge, "targetFragmentInstanceId"), text(edge, "targetPlanNodeId")));
  }

  private static JsonArray actualAggregations(JsonObject fi) {
    JsonArray result = new JsonArray();
    for (JsonElement value : fi.getAsJsonArray("nodes")) {
      JsonObject node = value.getAsJsonObject();
      if (node.has("aggregationStep") && !node.get("aggregationStep").isJsonNull()) {
        result.add(node.deepCopy());
      }
    }
    return result;
  }

  private static JsonObject hashRepartition(JsonObject roster, Path output) throws Exception {
    JsonObject receipt = new JsonObject();
    receipt.addProperty("queryId", text(roster, "queryId"));
    receipt.addProperty("sqlSha256", text(roster, "sqlSha256"));
    receipt.addProperty("provenance", "same ordinary SQL immutable actual FI/plan/channel roster");
    receipt.addProperty("status", "COLLECTING");
    JsonArray sourceProofs = new JsonArray(), bucketProofs = new JsonArray();
    JsonArray connections = new JsonArray();
    receipt.add("sources", sourceProofs);
    receipt.add("buckets", bucketProofs);
    receipt.add("connections", connections);
    receipt.add("actualRosterEdges", roster.getAsJsonArray("edges").deepCopy());
    Throwable primary = null;
    try {
      Map<String, JsonObject> instances = new LinkedHashMap<>();
      Map<String, Map<String, JsonObject>> planNodes = new LinkedHashMap<>();
      Map<String, JsonObject> sources = new LinkedHashMap<>();
      Map<String, String> sourceFis = new LinkedHashMap<>();
      Set<String> sourceFiIds = new HashSet<>();
      int partitions = -1;
      String descriptor = null;
      for (JsonElement element : roster.getAsJsonArray("fragmentInstances")) {
        JsonObject fi = element.getAsJsonObject();
        String fiId = text(fi, "fragmentInstanceId");
        require(instances.putIfAbsent(fiId, fi) == null, "Duplicate actual hash FI ID");
        require(flag(fi, "hostAssigned", false), "Actual hash FI has no host");
        Map<String, JsonObject> indexed = new LinkedHashMap<>();
        planNodes.put(fiId, indexed);
        for (JsonElement value : fi.getAsJsonArray("nodes")) {
          JsonObject node = value.getAsJsonObject();
          String nodeId = text(node, "planNodeId");
          require(indexed.putIfAbsent(nodeId, node) == null,
              "Duplicate actual plan node ID within one FI");
          if (!"TableHashPartitioningShuffleSinkNode".equals(text(node, "type"))) continue;
          String key = ROW.toJson(List.of(fiId, nodeId));
          JsonObject source = node.deepCopy();
          source.addProperty("sourceKey", key);
          source.addProperty("fragmentInstanceId", fiId);
          source.addProperty("dataNodeId", fi.get("dataNodeId").getAsInt());
          source.add("actualAggregationNodes", actualAggregations(fi));
          sourceProofs.add(source);
          require(flag(fi, "containsDeviceScan", false),
              "Hash source FI has no actual device scan");
          require(sources.putIfAbsent(key, node) == null && sourceFiIds.add(fiId),
              "Hash source FI/sink identity is duplicated or ambiguous");
          sourceFis.put(key, fiId);
          int count = node.get("hashPartitionCount").getAsInt();
          require(count > 0, "Actual hash partition count is not positive");
          JsonObject routing = new JsonObject();
          for (String field : List.of("hashVersion", "nullRouting", "partitioningSymbols")) {
            require(node.has(field) && !node.get(field).isJsonNull(),
                "Actual hash routing descriptor is incomplete");
            routing.add(field, node.get(field));
          }
          String actualDescriptor = ROW.toJson(routing);
          if (partitions < 0) { partitions = count; descriptor = actualDescriptor; }
          require(partitions == count && descriptor.equals(actualDescriptor),
              "Hash sources disagree on actual partition count or routing descriptor");
        }
      }
      require(sources.size() >= 2, "Ordinary SQL did not produce multiple actual hash sources");
      receipt.addProperty("actualSourceCount", sources.size());
      receipt.addProperty("actualPartitionCount", partitions);
      Map<String, JsonObject> sinkEdges = new LinkedHashMap<>();
      Map<String, JsonObject> inputEdges = new LinkedHashMap<>();
      Map<String, Set<Integer>> sourceChannels = new LinkedHashMap<>();
      Map<Integer, String> bucketOwners = new java.util.TreeMap<>();
      Map<Integer, Set<String>> bucketSources = new java.util.TreeMap<>();
      for (String key : sources.keySet()) sourceChannels.put(key, new HashSet<>());
      for (JsonElement value : roster.getAsJsonArray("edges")) {
        JsonObject edge = value.getAsJsonObject();
        String type = text(edge, "type");
        if (!Set.of("SINK_CHANNEL", "EXCHANGE_INPUT").contains(type)) continue;
        String sourceKey = hashSourceKey(edge);
        if (!sources.containsKey(sourceKey)) continue;
        int channel = edge.get("channelIndex").getAsInt();
        require(channel >= 0 && channel < partitions, "Hash channel index is outside actual P");
        String key = connectionKey(edge);
        Map<String, JsonObject> indexed = "SINK_CHANNEL".equals(type) ? sinkEdges : inputEdges;
        require(indexed.putIfAbsent(key, edge) == null, "Duplicate actual hash connection");
        if (!"SINK_CHANNEL".equals(type)) continue;
        require(sourceChannels.get(sourceKey).add(channel),
            "One actual hash source has duplicate bucket channel index");
        String targetId = text(edge, "targetFragmentInstanceId");
        JsonObject target = instances.get(targetId);
        require(target != null && flag(target, "hostAssigned", false),
            "Hash channel targets an unknown or unhosted FI");
        JsonObject exchange = planNodes.get(targetId).get(text(edge, "targetPlanNodeId"));
        require(exchange != null && "ExchangeNode".equals(text(exchange, "type")),
            "Hash channel does not target an actual ExchangeNode");
        require(!actualAggregations(target).isEmpty(),
            "Hash bucket owner has no actual aggregation node metadata");
        require(endpoint(edge.getAsJsonObject("endpoint"))
                .equals(endpoint(target.getAsJsonObject("exchangeEndpoint"))),
            "Sink channel endpoint differs from the actual bucket owner's exchange endpoint");
        String previous = bucketOwners.putIfAbsent(channel, targetId);
        require(previous == null || previous.equals(targetId),
            "Different sources disagree on an actual bucket owner");
        require(bucketSources.computeIfAbsent(channel, ignored -> new HashSet<>()).add(sourceKey),
            "Duplicate actual source-to-bucket connection");
      }
      require(sinkEdges.size() == Math.multiplyExact(sources.size(), partitions),
          "Actual hash source/channel roster is not complete N x P");
      require(sinkEdges.keySet().equals(inputEdges.keySet()),
          "Hash channels do not have exactly reciprocal actual Exchange inputs");
      require(bucketOwners.size() == partitions
              && new HashSet<>(bucketOwners.values()).size() == partitions,
          "Actual hash buckets do not have P distinct owner FI identities");
      for (Map.Entry<String, Set<Integer>> source : sourceChannels.entrySet()) {
        require(source.getValue().size() == partitions, "Actual hash source is missing a bucket");
      }
      for (Map.Entry<String, JsonObject> item : sinkEdges.entrySet()) {
        JsonObject sink = item.getValue(), input = inputEdges.get(item.getKey());
        JsonObject source = instances.get(text(sink, "sourceFragmentInstanceId"));
        require(endpoint(input.getAsJsonObject("endpoint"))
                .equals(endpoint(source.getAsJsonObject("exchangeEndpoint"))),
            "Exchange input endpoint differs from the actual source's exchange endpoint");
        JsonObject connection = new JsonObject();
        connection.addProperty("connectionKey", item.getKey());
        connection.addProperty("sourceKey", hashSourceKey(sink));
        connection.addProperty("bucketIndex", sink.get("channelIndex").getAsInt());
        connection.add("sinkChannel", sink.deepCopy());
        connection.add("exchangeInput", input.deepCopy());
        connections.add(connection);
      }
      for (Map.Entry<Integer, String> bucket : bucketOwners.entrySet()) {
        require(bucketSources.get(bucket.getKey()).size() == sources.size(),
            "Actual bucket does not receive every actual source");
        JsonObject owner = instances.get(bucket.getValue());
        JsonObject proof = new JsonObject();
        proof.addProperty("bucketIndex", bucket.getKey());
        proof.addProperty("fragmentInstanceId", bucket.getValue());
        proof.addProperty("dataNodeId", owner.get("dataNodeId").getAsInt());
        proof.add("actualAggregationNodes", actualAggregations(owner));
        proof.add("sourceKeys", ROW.toJsonTree(new java.util.TreeSet<>(
            bucketSources.get(bucket.getKey()))));
        bucketProofs.add(proof);
      }
      receipt.addProperty("actualConnectionCount", connections.size());
      receipt.addProperty("aggregationStepInterpretation", "recorded actual metadata; no guessed enum step");
      receipt.addProperty("status", "ACTUAL_SAME_SQL_N_BY_P_TOPOLOGY_VERIFIED");
    } catch (Exception | Error failure) {
      primary = failure;
      receipt.addProperty("status", "REJECTED_OR_INCOMPLETE");
      receipt.add("failures", failures(failure));
    } finally {
      primary = evidence(output.resolve("hash-repartition-roster.json"), receipt, primary);
    }
    if (primary != null) rethrow(primary);
    return receipt;
  }

  private static JsonObject verifyRoster(JsonObject roster, List<Node> nodes, int requiredSources)
      throws Exception {
    Map<Integer, Node> known = new HashMap<>();
    nodes.forEach(node -> known.put(node.id(), node));
    Set<String> identities = new HashSet<>();
    Set<Integer> hosts = new HashSet<>(), sources = new HashSet<>();
    Set<Integer> fragmentIds = new HashSet<>();
    for (JsonElement element : roster.getAsJsonArray("fragmentInstances")) {
      JsonObject fi = element.getAsJsonObject();
      require(identities.add(text(fi, "fragmentInstanceId")), "Duplicate actual FI identity");
      require(flag(fi, "hostAssigned", false), "A planned FI has no actual host assignment");
      int id = fi.get("dataNodeId").getAsInt();
      Node node = known.get(id);
      require(node != null
          && node.actual().getInternalEndPoint().equals(endpoint(fi.getAsJsonObject("internalEndpoint")))
          && node.actual().getClientRpcEndPoint().equals(endpoint(fi.getAsJsonObject("rpcEndpoint")))
          && node.actual().getMPPDataExchangeEndPoint().equals(
              endpoint(fi.getAsJsonObject("exchangeEndpoint"))),
          "Actual FI host is not a ConfigNode-discovered DataNode endpoint");
      TFragmentInstanceId thriftId = fragment(roster, fi);
      fragmentIds.add(thriftId.getFragmentId());
      require((thriftId.getQueryId() + "." + thriftId.getFragmentId() + "." + thriftId.getInstanceId())
              .equals(text(fi, "fragmentInstanceId")), "FI full identity differs from its actual components");
      hosts.add(id);
      if (flag(fi, "containsDeviceScan", false)) sources.add(id);
    }
    require(!identities.isEmpty(), "Production SQL has no actual FI roster");
    JsonObject result = new JsonObject();
    require(roster.get("fragmentInstanceCount").getAsInt() == identities.size(),
        "Advertised FI count differs from actual unique FI identities");
    result.addProperty("actualFragmentInstanceCount", identities.size());
    result.addProperty("actualFragmentCount", fragmentIds.size());
    result.add("actualFragmentIds", ROW.toJsonTree(new java.util.TreeSet<>(fragmentIds)));
    result.add("actualFiHostDataNodeIds", ROW.toJsonTree(new java.util.TreeSet<>(hosts)));
    result.add("actualDeviceSourceDataNodeIds", ROW.toJsonTree(new java.util.TreeSet<>(sources)));
    result.add("configNodeDiscoveredDataNodeIds",
        ROW.toJsonTree(new java.util.TreeSet<>(known.keySet())));
    result.addProperty("requiredActualSourceNodeCount", requiredSources);
    result.addProperty("actualTwoSourceNodesProved", sources.size() == 2);
    require(requiredSources >= 0 && requiredSources <= 2, "Invalid source placement requirement");
    require(sources.size() >= requiredSources,
        "Actual SQL source FIs do not execute on the required distinct discovered DataNodes");
    return result;
  }

  private static JsonArray terminal(JsonObject params, JsonObject roster, Probe probe,
      Path output, StatisticsPolicy statisticsPolicy) throws Exception {
    Path directory = output.resolve("statistics");
    Files.createDirectory(directory);
    long deadline = System.nanoTime()
        + TimeUnit.SECONDS.toNanos(integer(params, "terminalTimeoutSeconds", 45));
    Map<String, TFetchFragmentInstanceStatisticsResp> settled = new LinkedHashMap<>();
    Map<String, JsonObject> unretained = new LinkedHashMap<>();
    Map<String, Throwable> rejected = new LinkedHashMap<>();
    Throwable primary = null;
    int count = roster.getAsJsonArray("fragmentInstances").size();
    try (BufferedWriter attempts = log(output.resolve("terminal-statistics-polls.jsonl"))) {
      while (settled.size() + unretained.size() + rejected.size() < count) {
        for (JsonElement element : roster.getAsJsonArray("fragmentInstances")) {
          JsonObject fi = element.getAsJsonObject();
          String id = text(fi, "fragmentInstanceId");
          if (settled.containsKey(id) || unretained.containsKey(id) || rejected.containsKey(id)) continue;
          TFetchFragmentInstanceStatisticsResp response;
          try {
            response = fetch(probe.clients, roster, fi);
            line(attempts, Map.of("at", Instant.now().toString(), "fragmentInstanceId", id,
                "rawThrift", thrift(response)));
          } catch (Exception unavailable) {
            line(attempts, Map.of("at", Instant.now().toString(), "fragmentInstanceId", id,
                "fetchFailure", unavailable.toString()));
            continue;
          }
          if (statisticsPolicy == StatisticsPolicy.ALLOW_UNRETAINED_CONTROL
              && actualStatisticsNotRetained(roster, fi, response)) {
            TFragmentInstanceInfoResp info;
            try {
              info = fetchInfo(probe.clients, roster, fi);
            } catch (Exception unavailable) {
              line(attempts, Map.of("at", Instant.now().toString(), "fragmentInstanceId", id,
                  "legacyInfoFetchFailure", unavailable.toString()));
              continue;
            }
            try {
              line(attempts, Map.of("at", Instant.now().toString(), "fragmentInstanceId", id,
                  "actualLegacyInfoRpc", thrift(info)));
              require(info.isSetState(), "Legacy info RPC omitted its actual state");
              String actualState = info.getState();
              if (!TERMINAL.contains(actualState) && !"NO_SUCH_INSTANCE".equals(actualState)) {
                continue;
              }
              require("FINISHED".equals(actualState) || "NO_SUCH_INSTANCE".equals(actualState),
                  "Fixed-one control has an actually unsuccessful terminal FI: " + actualState);
              require(id.matches("[A-Za-z0-9_.-]+"), "Unsafe control FI evidence filename");
              Files.write(directory.resolve(id + ".statistics-not-retained.thrift.bin"),
                  new TSerializer(new TBinaryProtocol.Factory()).serialize(response),
                  StandardOpenOption.CREATE_NEW);
              write(directory.resolve(id + ".statistics-not-retained.thrift.json"), thrift(response));
              write(directory.resolve(id + ".legacy-info.thrift.json"), thrift(info));
              JsonObject row = new JsonObject();
              row.addProperty("fragmentInstanceId", id);
              row.addProperty("statisticsObservation", "NOT_RETAINED");
              row.addProperty("statisticsProvenance",
                  "actual statistics RPC NOT_FOUND, correct FI ID, EXECUTE_STATEMENT_ERROR");
              row.addProperty("frozenSnapshotVerified", false);
              row.addProperty("checksumRecomputedAndVerified", false);
              row.addProperty("driverResourceMetricsVerifiedFromStatistics", false);
              row.addProperty("terminalStateObserved", TERMINAL.contains(actualState));
              row.addProperty("infoRpcState", actualState);
              row.addProperty("stateObservation", TERMINAL.contains(actualState)
                  ? "ACTUAL_LEGACY_INFO_RPC" : "UNAVAILABLE_ACTUAL_INFO_RPC_NO_SUCH_INSTANCE");
              if (TERMINAL.contains(actualState)) row.addProperty("state", actualState);
              else row.add("state", JsonNull.INSTANCE);
              if (info.isSetEndTime()) row.addProperty("actualInfoEndTime", info.getEndTime());
              row.addProperty("actualHostDataNodeId", fi.get("dataNodeId").getAsInt());
              row.addProperty("notRetainedJsonSha256",
                  sha(directory.resolve(id + ".statistics-not-retained.thrift.json")));
              row.addProperty("legacyInfoJsonSha256",
                  sha(directory.resolve(id + ".legacy-info.thrift.json")));
              row.add("checksNotExecuted", ROW.toJsonTree(List.of(
                  "frozen V2 snapshot/checksum/plan checksum/structured validity",
                  "driver/resource creation metrics from frozen statistics",
                  "runtime group activation/actions/layout from frozen statistics")));
              unretained.put(id, row);
            } catch (Exception | Error invalid) {
              rejected.put(id, invalid);
              primary = mergeFailure(primary, invalid);
            }
            continue;
          }
          if (response.getStatus().getCode() != 200 || !response.isSetState()
              || !TERMINAL.contains(response.getState())
              || !response.isSetIsTerminal() || !response.isIsTerminal()) continue;
          try {
            // Archive each actual response once, even when it is rejected by later invariants.
            require(id.matches("[A-Za-z0-9_.-]+"), "Unsafe FI evidence filename");
            byte[] binary = new TSerializer(new TBinaryProtocol.Factory()).serialize(response);
            Files.write(directory.resolve(id + ".thrift.bin"), binary, StandardOpenOption.CREATE_NEW);
            write(directory.resolve(id + ".thrift.json"), thrift(response));
            require(response.isSetFragmentInstanceId()
                    && fragment(roster, fi).equals(response.getFragmentInstanceId()),
                "Terminal RPC statistics returned another FI identity");
            require(response.isSetSourceNodeId()
                    && Integer.toString(fi.get("dataNodeId").getAsInt()).equals(response.getSourceNodeId()),
                "Terminal statistics source node differs from actual planned FI host");
            require(response.isSetSnapshotChecksum() && !response.getSnapshotChecksum().isEmpty()
                    && response.isSetPlanIdentityChecksum()
                    && !response.getPlanIdentityChecksum().isEmpty(),
                "Terminal statistics lacks actual snapshot/plan identity");
            var adapted = RuntimeSnapshotAdapters.fromResponse(response);
            require(adapted.getChecksum().equals(response.getSnapshotChecksum())
                && adapted.getValidity().asSet().stream().allMatch(reason ->
                    reason == RuntimeSnapshotValidity.VALID
                        || reason == RuntimeSnapshotValidity.UNSUPPORTED_FIELD),
                "Frozen statistics checksum or structured snapshot validity failed: "
                    + adapted.getValidity());
            require(response.isSetDriverSnapshots() && !response.getDriverSnapshots().isEmpty()
                    && response.getResourceObjectCount() > 0
                    && response.getResourceUniqueMemoryKeyCount() > 0
                    && response.getResourceNominalShareBytes() > 0,
                "All-zero or missing actual FI driver/resource creation evidence");
            probe.captureControls(roster, fi, response, "FROZEN_TERMINAL");
            if (flag(params, "requireOrderedInitialOne", false)
                && probe.groups.keySet().stream().anyMatch(key -> key.startsWith(id + "/"))) {
              require(response.isSetSplitMode() && "ORDERED".equals(response.getSplitMode()),
                  "Runtime group FI did not execute the actual ORDERED split path");
              require(response.isSetActualPipelineCount() && response.getActualPipelineCount() > 1,
                  "Actual ORDERED FI did not preserve multiple planned pipeline lanes");
            }
            settled.put(id, response);
          } catch (Exception | Error invalid) {
            rejected.put(id, invalid);
            primary = mergeFailure(primary, invalid);
          }
        }
        if (settled.size() + unretained.size() + rejected.size() == count) break;
        if (System.nanoTime() >= deadline) {
          primary = mergeFailure(primary, new IllegalStateException(
              "Not every actual FI produced a frozen terminal snapshot"));
          break;
        }
        Thread.sleep(50);
      }
    } catch (Exception | Error collectionFailure) {
      primary = mergeFailure(primary, collectionFailure);
    }
    JsonArray receipt = new JsonArray();
    for (Map.Entry<String, TFetchFragmentInstanceStatisticsResp> entry : settled.entrySet()) {
      TFetchFragmentInstanceStatisticsResp response = entry.getValue();
      JsonObject row = new JsonObject();
      row.addProperty("fragmentInstanceId", entry.getKey());
      row.addProperty("statisticsObservation", "FROZEN_TERMINAL_VERIFIED");
      row.addProperty("frozenSnapshotVerified", true);
      row.addProperty("terminalStateObserved", true);
      row.addProperty("state", response.getState());
      row.addProperty("sourceNodeId", response.getSourceNodeId());
      row.addProperty("snapshotChecksum", response.getSnapshotChecksum());
      row.addProperty("checksumRecomputedAndVerified", true);
      row.addProperty("planIdentityChecksum", response.getPlanIdentityChecksum());
      row.addProperty("actualSplitMode", response.getSplitMode());
      row.addProperty("actualPipelineCount", response.getActualPipelineCount());
      row.addProperty("binarySha256", sha(directory.resolve(entry.getKey() + ".thrift.bin")));
      row.addProperty("jsonSha256", sha(directory.resolve(entry.getKey() + ".thrift.json")));
      receipt.add(row);
    }
    for (JsonObject row : unretained.values()) receipt.add(row);
    primary = evidence(output.resolve("terminal-statistics-receipt.json"), receipt, primary);
    if (primary != null) {
      JsonObject incomplete = new JsonObject();
      incomplete.addProperty("status", "TERMINAL_COLLECTION_INCOMPLETE");
      incomplete.addProperty("requiredFragmentCount", count);
      incomplete.addProperty("verifiedFragmentCount", settled.size());
      incomplete.addProperty("explicitlyUnretainedControlCount", unretained.size());
      incomplete.add("rejectedFragmentIds", ROW.toJsonTree(rejected.keySet()));
      incomplete.add("failures", failures(primary));
      primary = evidence(output.resolve("terminal-statistics-incomplete.json"), incomplete, primary);
      rethrow(primary);
    }
    return receipt;
  }

  private static boolean zero(JsonObject value, String name) {
    return value.has(name) && !value.get(name).isJsonNull() && value.get(name).getAsLong() == 0;
  }

  private static boolean settledAudit(JsonObject value, boolean requireDrivers) {
    boolean complete = flag(value, "complete", false) && flag(value, "released", false)
        && value.has("fragmentMapPresent") && !value.get("fragmentMapPresent").getAsBoolean();
    for (String key : List.of("liveDrivers", "liveQueues", "unconsumedFactories", "liveEndpoints",
        "pendingFutures", "fragmentReservedBytes", "queryReservedBytes", "deregisterFailures"))
      complete &= zero(value, key);
    if (requireDrivers) {
      complete &= value.has("createdDrivers") && value.has("closedDrivers")
          && value.get("createdDrivers").getAsLong() > 0
          && value.get("createdDrivers").getAsLong() == value.get("closedDrivers").getAsLong()
          && value.has("objects") && !value.getAsJsonArray("objects").isEmpty()
          && value.has("quotaPublication") && !value.getAsJsonArray("quotaPublication").isEmpty();
    }
    if (!value.has("deregisterReceipt") || !value.get("deregisterReceipt").isJsonObject()) return false;
    JsonObject receipt = value.getAsJsonObject("deregisterReceipt");
    return complete && flag(receipt, "success", false)
        && receipt.has("presentAfter") && !receipt.get("presentAfter").getAsBoolean()
        && zero(receipt, "bytesAfter");
  }

  private static JsonArray resources(JsonObject params, JsonObject roster,
      List<Audit> audits, Map<Integer, JsonObject> baselines, Path output) throws Exception {
    Map<Integer, Audit> byId = new HashMap<>();
    audits.forEach(audit -> byId.put(audit.node.id(), audit));
    long deadline = System.nanoTime()
        + TimeUnit.SECONDS.toNanos(integer(params, "terminalTimeoutSeconds", 45));
    JsonArray finalState = new JsonArray(), finalPools = new JsonArray();
    Map<String, Throwable> lastFailures = new LinkedHashMap<>();
    Throwable primary = null;
    boolean complete = false;
    try (BufferedWriter polls = log(output.resolve("resource-audit-polls.jsonl"))) {
      do {
        complete = true;
        finalState = new JsonArray();
        lastFailures.clear();
        for (JsonElement element : roster.getAsJsonArray("fragmentInstances")) {
          JsonObject fi = element.getAsJsonObject();
          String id = text(fi, "fragmentInstanceId");
          Audit audit = byId.get(fi.get("dataNodeId").getAsInt());
          try {
            require(audit != null, "No actual DataNode audit for FI " + id);
            JsonObject value = audit.read(text(roster, "queryId"), id);
            value.addProperty("actualDataNodeId", audit.node.id());
            value.addProperty("windowId", audit.window);
            finalState.add(value);
            complete &= settledAudit(value, true);
          } catch (Exception | Error unavailable) {
            complete = false;
            lastFailures.put(id, unavailable);
            finalState.add(MapToJson(Map.of("fragmentInstanceId", id,
                "status", "AUDIT_READ_FAILED", "failures", failures(unavailable))));
          }
        }
        finalPools = new JsonArray();
        for (Audit audit : audits) {
          try {
            JsonObject pool = audit.read("__idle__", "0.0");
            pool.addProperty("actualDataNodeId", audit.node.id());
            finalPools.add(pool);
            JsonObject before = baselines.get(audit.node.id());
            for (String key : List.of("exchangeRemainingBytes", "operatorsFreeBytes"))
              complete &= pool.has(key) && before != null && before.has(key)
                  && pool.get(key).getAsLong() == before.get(key).getAsLong();
          } catch (Exception | Error unavailable) {
            complete = false;
            lastFailures.put("pool/" + audit.node.id(), unavailable);
            finalPools.add(MapToJson(Map.of("actualDataNodeId", audit.node.id(),
                "status", "POOL_READ_FAILED", "failures", failures(unavailable))));
          }
        }
        line(polls, Map.of("at", Instant.now().toString(), "complete", complete,
            "fragments", finalState, "pools", finalPools));
        if (complete) break;
        if (System.nanoTime() >= deadline) {
          primary = new IllegalStateException("Real FI owners/receipts/pool baselines did not settle");
          for (Throwable failure : lastFailures.values()) primary = mergeFailure(primary, failure);
          break;
        }
        Thread.sleep(50);
      } while (true);
    } catch (Exception | Error failure) {
      primary = mergeFailure(primary, failure);
    }
    primary = evidence(output.resolve(complete ? "resource-audit-final.json"
        : "resource-audit-incomplete.json"), finalState, primary);
    primary = evidence(output.resolve(complete ? "resource-pools-final.json"
        : "resource-pools-incomplete.json"), finalPools, primary);
    JsonArray raw = new JsonArray();
    for (Audit audit : audits) {
      JsonObject node = new JsonObject();
      node.addProperty("actualDataNodeId", audit.node.id());
      node.addProperty("windowId", audit.window);
      JsonArray snapshots = new JsonArray();
      node.add("snapshots", snapshots);
      try {
        JsonArray fragments = JsonParser.parseString((String) audit.call(
            "listResourceAuditFragments", new Object[] {audit.window, text(roster, "queryId")},
            "java.lang.String", "java.lang.String")).getAsJsonArray();
        node.add("fragmentIdentities", fragments);
        for (JsonElement element : fragments) {
          JsonObject identity = element.getAsJsonObject();
          try {
            snapshots.add(audit.read(text(identity, "queryId"), text(identity, "fragmentInstanceId")));
          } catch (Exception | Error failure) {
            primary = mergeFailure(primary, failure);
            snapshots.add(MapToJson(Map.of("identity", identity, "failures", failures(failure))));
          }
        }
      } catch (Exception | Error failure) {
        primary = mergeFailure(primary, failure);
        node.add("failures", failures(failure));
      }
      raw.add(node);
    }
    primary = evidence(output.resolve("all-query-audit-records.json"), raw, primary);
    if (!complete && primary == null) primary =
        new IllegalStateException("Actual resource-settlement proof is incomplete");
    if (primary != null) rethrow(primary);
    return finalState;
  }

  private static void ordinary(JsonObject params, Path base, Path deploymentPath, Path output)
      throws Exception {
    JsonObject deployment = read(deploymentPath);
    write(output.resolve("deployment-input.json"), redact(deployment));
    ExpectedOutcome expected = ExpectedOutcome.valueOf(
        optional(params, "expectedOutcome", "SUCCESS").toUpperCase(java.util.Locale.ROOT));
    int expectedCode = switch (expected) {
      case SUCCESS -> 0;
      case CANCELLED -> org.apache.iotdb.rpc.TSStatusCode.QUERY_WAS_KILLED.getStatusCode();
      case DEADLINE -> org.apache.iotdb.rpc.TSStatusCode.QUERY_TIMEOUT.getStatusCode();
      case EXCEPTION -> integer(params, "expectedSqlErrorCode", -1);
    };
    require(expected != ExpectedOutcome.EXCEPTION || expectedCode > 0,
        "EXCEPTION requires an explicit actual server SQL error code");
    StatisticsPolicy statisticsPolicy = statisticsPolicy(params, deployment, expected);
    write(output.resolve("statistics-observability-policy.json"), Map.of(
        "statisticsPolicy", statisticsPolicy.name(), "defaultStrict", true,
        "controlConfigurationBindingsVerified",
            statisticsPolicy == StatisticsPolicy.ALLOW_UNRETAINED_CONTROL,
        "scope", "Candidate/negative/runtime proofs always REQUIRE_FROZEN; explicit fixed-one SUCCESS control only may lack retained statistics."));
    Path sqlFile = resolve(base, text(params, "sqlFile"));
    byte[] originalBytes = Files.readAllBytes(sqlFile);
    String runMarker = UUID.randomUUID().toString();
    params.addProperty("_runMarker", runMarker);
    String sql = new String(originalBytes, StandardCharsets.UTF_8)
        + "\n/* runtime_sql_acceptance_run=" + runMarker + " */";
    String sqlHash = sha(sql.getBytes(StandardCharsets.UTF_8));
    require(sql.stripLeading().matches("(?is)(SELECT|WITH)\\b.*"),
        "ordinary-sql mode requires one ordinary SELECT/WITH statement");
    List<Node> nodes = discover(deployment, output);
    long auditDuration = integer(params, "auditDurationMs", 300000);
    int pollMs = integer(params, "rosterPollMs", 25);
    require(auditDuration > 0 && auditDuration <= 600000 && pollMs >= 5 && pollMs <= 1000,
        "Invalid audit duration or roster polling period");
    List<Audit> audits = new ArrayList<>();
    Map<Integer, JsonObject> baseline = new LinkedHashMap<>();
    IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clients =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
    Throwable primary = null;
    Throwable sqlFailure = null;
    Probe probe = null;
    CancelAction cancel = null;
    boolean probeClosed = false, cancelClosed = false;
    JsonObject proof = null;
    try {
      JsonArray windows = new JsonArray(), pools = new JsonArray();
      for (Node node : nodes) {
        Audit audit = new Audit(node, auditDuration);
        audits.add(audit);
        windows.add(MapToJson(Map.of("dataNodeId", node.id(), "windowId", audit.window,
            "objectName", audit.object.toString())));
        JsonObject pool = audit.read("__idle__", "0.0");
        baseline.put(node.id(), pool.deepCopy());
        pool.addProperty("actualDataNodeId", node.id());
        pools.add(pool);
      }
      write(output.resolve("audit-windows.json"), windows);
      write(output.resolve("resource-pools-before.json"), pools);
      write(output.resolve("sql-binding.json"), Map.of(
          "sqlFile", sqlFile.toString(), "inputSqlSha256", sha(originalBytes),
          "runMarker", runMarker, "executedSqlSha256", sqlHash, "expectedOutcome", expected.name()));
      probe = new Probe(audits, clients, sqlHash, pollMs, output);
      probe.thread.start();
      if (expected == ExpectedOutcome.CANCELLED) {
        cancel = new CancelAction(params, deployment, probe);
        cancel.thread.start();
      }
      JsonObject result = null;
      try {
        result = execute(params, deployment, sqlFile, output, probe);
      } catch (Exception | Error queryFailure) {
        sqlFailure = queryFailure;
        if (expected == ExpectedOutcome.SUCCESS || !hasSqlCode(queryFailure, expectedCode)) {
          primary = mergeFailure(primary, queryFailure);
        }
      } finally {
        if (cancel != null) {
          try { cancel.close(); }
          catch (Exception | Error closeFailure) { primary = mergeFailure(primary, closeFailure); }
          finally { cancelClosed = true; }
          primary = evidence(output.resolve("targeted-kill-receipt.json"), cancel.receipt, primary);
        }
        try { probe.close(); }
        catch (Exception | Error closeFailure) { primary = mergeFailure(primary, closeFailure); }
        finally { probeClosed = true; }
      }
      JsonObject roster = probe.roster;
      JsonArray snapshots = null, auditReceipts = null, actions = null;
      if (roster == null) {
        primary = mergeFailure(primary,
            new IllegalStateException("No real coordinator roster was captured for ordinary SQL"));
      } else {
        try {
          if (!Files.exists(output.resolve("roster.json"))) write(output.resolve("roster.json"), roster);
          write(output.resolve("actual-source-placement.json"), verifyRoster(roster, nodes,
              integer(params, "requireActualSourceNodeCount", 2)));
          if (flag(params, "requireHashRepartition", false)) hashRepartition(roster, output);
        } catch (Exception | Error invalidRoster) {
          primary = mergeFailure(primary, invalidRoster);
        }
        // These are independent evidence owners. A terminal statistics failure must not skip
        // actual resource-settlement collection, and vice versa.
        try { snapshots = terminal(params, roster, probe, output, statisticsPolicy); }
        catch (Exception | Error terminalFailure) { primary = mergeFailure(primary, terminalFailure); }
        try { auditReceipts = resources(params, roster, audits, baseline, output); }
        catch (Exception | Error resourceFailure) { primary = mergeFailure(primary, resourceFailure); }
        try {
          write(output.resolve("runtime-actions-observed.json"), probe.observedActions(false, false));
          actions = probe.observedActions(flag(params, "requireGrowShrink", false),
              flag(params, "requireOrderedInitialOne", false));
        } catch (Exception | Error actionFailure) {
          primary = mergeFailure(primary, actionFailure);
        }
        try {
          require(snapshots != null && auditReceipts != null && actions != null,
              "Terminal/resource/runtime evidence collection is incomplete");
          require(snapshots.size() == roster.getAsJsonArray("fragmentInstances").size()
                  && auditReceipts.size() == snapshots.size(),
              "Not every actually planned FI supplied terminal and resource proofs");
          boolean nonSuccess = false;
          for (JsonElement value : snapshots) {
            JsonObject snapshot = value.getAsJsonObject();
            if ("NOT_RETAINED".equals(optional(snapshot, "statisticsObservation", ""))) {
              require(statisticsPolicy == StatisticsPolicy.ALLOW_UNRETAINED_CONTROL
                      && expected == ExpectedOutcome.SUCCESS,
                  "Unretained statistics cannot satisfy a strict or negative case");
              if (!flag(snapshot, "terminalStateObserved", false)) {
                require("NO_SUCH_INSTANCE".equals(text(snapshot, "infoRpcState")),
                    "Missing terminal state is not an actual unavailable legacy info observation");
                continue;
              }
            }
            String state = text(snapshot, "state");
            require(TERMINAL.contains(state), "A target FI has no real terminal state");
            nonSuccess |= !"FINISHED".equals(state);
            if (expected == ExpectedOutcome.SUCCESS) require("FINISHED".equals(state),
                "A successful target SQL has a non-successful FI");
          }
          if (expected == ExpectedOutcome.SUCCESS) {
            if (sqlFailure != null) rethrow(sqlFailure);
            require(result != null && flag(result, "fullResultDrained", false),
                "Successful SQL did not drain its complete typed result");
          } else {
            require(sqlFailure != null && hasSqlCode(sqlFailure, expectedCode),
                "Expected server SQL termination was not exercised with its exact error code");
            require(sqlFailure.getSuppressed().length == 0,
                "Expected SQL error was accompanied by additional client/close/evidence failures");
            require(nonSuccess, "Negative case has no actually unsuccessful terminal FI");
            require(probe.groups.values().stream().anyMatch(group ->
                flag(group, "layoutIdentitySupported", false)
                    && integer(group, "initialActivatedWorkers", 0) > 0),
                "Negative case did not execute a real runtime scan group");
            if (expected == ExpectedOutcome.CANCELLED) require(
                cancel != null && cancel.applied && cancel.failure == null,
                "Negative cancellation lacks an acknowledged targeted KILL QUERY");
            if (expected == ExpectedOutcome.DEADLINE) {
              require(!probe.deadlines.isEmpty()
                  && probe.deadlines.values().stream().allMatch(value ->
                      value > 0 && System.currentTimeMillis() >= value),
                  "Deadline case did not preserve and reach its actual external deadline");
            }
            write(output.resolve("expected-sql-termination.json"), Map.of(
                "expectedOutcome", expected.name(), "expectedSqlErrorCode", expectedCode,
                "actualFailure", failures(sqlFailure), "sqlCodeEvidence", sqlCodeEvidence(sqlFailure, expectedCode),
                "queryId", text(roster, "queryId"),
                "runtimeOriginalDeadlines", probe.deadlines,
                "proof", "actual server SQL error plus real FI terminal and resource settlement"));
          }
          proof = new JsonObject();
          long unretainedCount = java.util.stream.StreamSupport.stream(
              snapshots.spliterator(), false).filter(value -> "NOT_RETAINED".equals(optional(
                  value.getAsJsonObject(), "statisticsObservation", ""))).count();
          long terminalStateCount = java.util.stream.StreamSupport.stream(
              snapshots.spliterator(), false).filter(value ->
                  flag(value.getAsJsonObject(), "terminalStateObserved", false)).count();
          proof.addProperty("statisticsPolicy", statisticsPolicy.name());
          proof.addProperty("frozenStatisticsVerifiedFragments", snapshots.size() - unretainedCount);
          proof.addProperty("unretainedControlStatisticsFragments", unretainedCount);
          proof.addProperty("actualTerminalStateObservedFragments", terminalStateCount);
          proof.addProperty("terminalStateUnavailableFragments", snapshots.size() - terminalStateCount);
          proof.addProperty("realResourceAuditCreationAndSettlementRequired", true);
          proof.add("checksNotExecuted", ROW.toJsonTree(unretainedCount == 0 ? List.of() : List.of(
              "unretained FI frozen V2 snapshot/checksum/plan identity/structured validity",
              "unretained FI frozen driver/resource statistics",
              "unretained FI runtime group activation/actions/layout")));
          proof.addProperty("status", unretainedCount > 0
              ? "FULL_CONTROL_RESULT_AND_REAL_RESOURCE_PROOF_WITH_UNRETAINED_STATISTICS"
              : expected == ExpectedOutcome.SUCCESS
                  ? "FULL_RESULT_AND_REAL_TERMINAL_PROOF_CAPTURED"
                  : "EXPECTED_QUERY_TERMINATION_AND_REAL_CLEANUP_PROOF_CAPTURED");
          proof.addProperty("expectedOutcome", expected.name());
          proof.addProperty("queryId", text(roster, "queryId"));
          proof.addProperty("sqlSha256", sqlHash);
          proof.addProperty("fragments", snapshots.size());
          proof.addProperty("resourceReceipts", auditReceipts.size());
          proof.addProperty("runtimeGroups", actions.size());
          proof.addProperty("requireHashRepartition", flag(params, "requireHashRepartition", false));
          proof.addProperty("actualHashTopologyVerified", Files.exists(
              output.resolve("hash-repartition-roster.json"))
              && "ACTUAL_SAME_SQL_N_BY_P_TOPOLOGY_VERIFIED".equals(optional(
                  read(output.resolve("hash-repartition-roster.json")), "status", "")));
          proof.addProperty("fullResultDrained", result != null);
          proof.addProperty("oracleCompared", false);
          proof.addProperty("multisetCompared", false);
          if (result != null) {
            proof.addProperty("rows", result.get("rows").getAsLong());
            proof.addProperty("resultSha256", text(result, "sha256"));
            proof.addProperty("resultSha256Kind", "ACTUAL_TYPED_JDBC_SEQUENCE");
          } else if (Files.exists(output.resolve("result-partial-receipt.json"))) {
            proof.add("typedPrefix", read(output.resolve("result-partial-receipt.json")));
          }
        } catch (Exception | Error invalidCase) {
          primary = mergeFailure(primary, invalidCase);
          primary = mergeFailure(primary, sqlFailure);
        }
      }
    } catch (Exception | Error failure) {
      primary = mergeFailure(primary, failure);
    } finally {
      if (cancel != null && !cancelClosed) {
        try { cancel.close(); }
        catch (Exception | Error failure) { primary = mergeFailure(primary, failure); }
      }
      if (probe != null && !probeClosed) {
        try { probe.close(); }
        catch (Exception | Error failure) { primary = mergeFailure(primary, failure); }
      }
      if (probe != null) {
        try { probe.controlLog.close(); }
        catch (Exception | Error failure) { primary = mergeFailure(primary, failure); }
      }
      JsonArray closed = new JsonArray();
      for (Audit audit : audits) {
        JsonObject closure = new JsonObject();
        closure.addProperty("dataNodeId", audit.node.id());
        closure.addProperty("windowId", audit.window);
        try {
          audit.close();
          closure.addProperty("status", "CLOSED");
        } catch (Exception | Error failure) {
          closure.addProperty("status", "CLOSE_FAILED");
          closure.add("failures", failures(failure));
          primary = mergeFailure(primary, failure);
        }
        closed.add(closure);
      }
      primary = evidence(output.resolve("audit-window-close.json"), closed, primary);
      try { clients.close(); }
      catch (Exception | Error failure) { primary = mergeFailure(primary, failure); }
      // A result/terminal capture is only a candidate until every independent close has succeeded.
      if (primary == null && proof != null) {
        proof.addProperty("auditWindowsClosed", true);
        proof.addProperty("clientManagerClosed", true);
        primary = evidence(output.resolve("ordinary-sql-receipt.json"), proof, null);
      }
      if (primary == null && proof == null) {
        primary = new IllegalStateException("No complete ordinary SQL case proof was produced");
      }
      if (primary != null) {
        primary = mergeFailure(primary, sqlFailure);
        primary = evidence(output.resolve("ordinary-sql-incomplete.json"), Map.of(
            "status", "FAILED_OR_INCOMPLETE", "expectedOutcome", expected.name(),
            "sqlSha256", sqlHash, "failures", failures(primary)), primary);
      }
    }
    if (primary != null) rethrow(primary);
  }

  private static JsonElement MapToJson(Map<String, ?> value) {
    return ROW.toJsonTree(value);
  }
}
