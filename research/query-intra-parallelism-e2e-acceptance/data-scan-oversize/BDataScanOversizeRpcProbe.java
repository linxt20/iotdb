/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file to You under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 * See the License for the specific language governing permissions and limitations.
 */
import com.google.gson.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsReq;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;

/** Ordinary production statistics RPC only; no production writes or query dispatch. */
public final class BDataScanOversizeRpcProbe {
  private static final Gson JSON = new GsonBuilder().serializeNulls().setPrettyPrinting().create();

  public static void main(String[] args) throws Exception {
    if (args.length != 2) throw new IllegalArgumentException("JMX_OBSERVATION NEW_OUTPUT");
    Path output = Path.of(args[1]);
    if (Files.exists(output)) throw new IllegalArgumentException("Preserve existing output");
    JsonArray observation = JsonParser.parseString(Files.readString(Path.of(args[0]))).getAsJsonArray();
    JsonArray responses = new JsonArray();
    var manager = new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
        .createClientManager(new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
    try {
      for (JsonElement nodeElement : observation) {
        JsonObject node = nodeElement.getAsJsonObject();
        int internal = node.get("port").getAsInt() - 6000;
        for (JsonElement item : node.getAsJsonArray("snapshots")) {
          JsonObject snapshot = item.getAsJsonObject();
          String query = snapshot.get("queryId").getAsString();
          String full = snapshot.get("fragmentInstanceId").getAsString();
          if (!full.startsWith(query + ".")) throw new AssertionError("Query/FI identity differs");
          String[] suffix = full.substring(query.length() + 1).split("\\.", 2);
          JsonObject response = new JsonObject();
          response.addProperty("queryId", query);
          response.addProperty("fragmentInstanceId", full);
          response.addProperty("internalPort", internal);
          try (var client = manager.borrowClient(new TEndPoint("127.0.0.1", internal))) {
            TFetchFragmentInstanceStatisticsResp raw = client.fetchFragmentInstanceStatistics(
                new TFetchFragmentInstanceStatisticsReq(
                    new TFragmentInstanceId(query, Integer.parseInt(suffix[0]), suffix[1])));
            response.addProperty("statusCode", raw.getStatus().getCode());
            response.addProperty("state", raw.getState());
            response.addProperty("dataRegion", raw.getDataRegion());
            response.addProperty("isTerminalSet", raw.isSetIsTerminal());
            response.addProperty("isTerminal", raw.isIsTerminal());
            response.addProperty("snapshotChecksum", raw.getSnapshotChecksum());
            response.addProperty("planIdentityChecksum", raw.getPlanIdentityChecksum());
            response.addProperty("sourceNodeId", raw.getSourceNodeId());
            response.addProperty("plannedDop", raw.getPlannedDop());
            response.addProperty("actualPipelineCount", raw.getActualPipelineCount());
            JsonArray drivers = new JsonArray();
            if (raw.isSetDriverSnapshots()) for (var driver : raw.getDriverSnapshots()) {
              JsonObject d = new JsonObject();
              d.addProperty("fragmentInstanceId", driver.getFragmentInstanceId());
              d.addProperty("driverTaskId", driver.getDriverTaskId());
              d.addProperty("driverPipelineId", driver.getDriverPipelineId());
              d.addProperty("driverRole", driver.getDriverRole());
              d.addProperty("firstProcessPresent", driver.isSetDriverFirstProcessNanos());
              d.addProperty("firstProcessNanos", driver.getDriverFirstProcessNanos());
              d.addProperty("lastProcessPresent", driver.isSetDriverLastProcessNanos());
              d.addProperty("lastProcessNanos", driver.getDriverLastProcessNanos());
              d.addProperty("wallPresent", driver.isSetDriverWallNanos());
              d.addProperty("wallNanos", driver.getDriverWallNanos());
              d.addProperty("outputRowsPresent", driver.isSetDriverOutputRows());
              d.addProperty("outputRows", driver.getDriverOutputRows());
              d.addProperty("tsBlockCountPresent", driver.isSetDriverTsBlockCount());
              d.addProperty("tsBlockCount", driver.getDriverTsBlockCount());
              drivers.add(d);
            }
            response.add("drivers", drivers);
            JsonArray operators = new JsonArray();
            if (raw.isSetOperatorStatisticsMap()) for (var operator : raw.getOperatorStatisticsMap().values()) {
              JsonObject op = new JsonObject();
              op.addProperty("planNodeId", operator.getPlanNodeId());
              op.addProperty("operatorType", operator.getOperatorType());
              op.addProperty("outputRows", operator.getOutputRows());
              op.addProperty("nextCalledCount", operator.getNextCalledCount());
              op.add("specifiedInfo", JSON.toJsonTree(operator.getSpecifiedInfo()));
              operators.add(op);
            }
            response.add("operators", operators);
            response.add("rawThrift", JsonParser.parseString(new String(
                new TSerializer(new TJSONProtocol.Factory()).serialize(raw), StandardCharsets.UTF_8)));
          } catch (Exception failure) {
            response.addProperty("rpcFailure", failure.toString());
          }
          responses.add(response);
        }
      }
      JsonArray loaded = new JsonArray();
      for (Class<?> type : List.of(BDataScanOversizeRpcProbe.class,
          SyncDataNodeInternalServiceClient.class, TFetchFragmentInstanceStatisticsResp.class)) {
        JsonObject value = new JsonObject();
        value.addProperty("class", type.getName());
        value.addProperty("codeSource", type.getProtectionDomain().getCodeSource().getLocation().toString());
        try (var stream = type.getResourceAsStream("/" + type.getName().replace('.', '/') + ".class")) {
          value.addProperty("classSha256", HexFormat.of().formatHex(
              MessageDigest.getInstance("SHA-256").digest(stream.readAllBytes())));
        }
        loaded.add(value);
      }
      JsonObject result = new JsonObject();
      result.add("responses", responses);
      result.add("actualLoadedCode", loaded);
      Files.writeString(output, JSON.toJson(result), StandardOpenOption.CREATE_NEW);
    } finally {
      manager.close();
    }
    System.exit(0);
  }
}
