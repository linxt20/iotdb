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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import org.apache.iotdb.jdbc.IoTDBConnection;

/** Streaming full-source validation, deliberately outside any performance sample timer. */
public final class SourceFingerprintValidationDriver {
  private static final Gson JSON = new GsonBuilder().serializeNulls().disableHtmlEscaping().create();

  private static void require(boolean condition, String reason) {
    if (!condition) throw new IllegalStateException(reason);
  }

  private static String sha(byte[] bytes) throws Exception {
    return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static void write(Path file, JsonObject value) throws Exception {
    Files.writeString(file, JSON.toJson(value) + "\n", StandardOpenOption.CREATE_NEW);
  }

  public static void main(String[] args) throws Exception {
    require(args.length == 1, "Specify parameter JSON");
    Path parameter = Path.of(args[0]).toAbsolutePath();
    JsonObject plan = JsonParser.parseString(Files.readString(parameter)).getAsJsonObject();
    Path oraclePath = Path.of(plan.get("oracleManifest").getAsString());
    byte[] oracleBytes = Files.readAllBytes(oraclePath);
    require(sha(oracleBytes).equals(plan.get("oracleManifestSha256").getAsString()), "Oracle identity mismatch");
    JsonObject oracle = JsonParser.parseString(new String(oracleBytes, StandardCharsets.UTF_8)).getAsJsonObject();
    require(oracle.get("sourceArchiveSha256").getAsString().equals(plan.get("sourceArchiveSha256").getAsString()), "Original source identity mismatch");
    require(oracle.get("noGeneratedCsvRead").getAsBoolean(), "Independent original-source oracle required");
    String database = plan.get("database").getAsString();
    String table = plan.get("table").getAsString();
    require(database.matches("[a-z][a-z0-9_]*") && table.matches("[a-z][a-z0-9_]*"), "Invalid table identity");
    Path out = Path.of(plan.get("output").getAsString());
    Files.createDirectory(out);
    String password = System.getenv(plan.get("passwordEnv").getAsString());
    require(password != null && !password.isEmpty(), "Missing database password environment");
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    JsonObject identity = new JsonObject();
    identity.addProperty("parameter", parameter.toString());
    identity.addProperty("parameterSha256", sha(Files.readAllBytes(parameter)));
    identity.addProperty("oracleManifestSha256", sha(oracleBytes));
    identity.addProperty("entryCodeSource", SourceFingerprintValidationDriver.class.getProtectionDomain().getCodeSource().getLocation().toString());
    identity.addProperty("jdbcCodeSource", Class.forName("org.apache.iotdb.jdbc.IoTDBDriver").getProtectionDomain().getCodeSource().getLocation().toString());
    identity.addProperty("classpath", System.getProperty("java.class.path"));
    identity.addProperty("javaVersion", System.getProperty("java.version"));
    identity.addProperty("scope", "Complete ordered row fingerprints; not a latency or throughput benchmark");
    write(out.resolve("loaded-identity.json"), identity);
    Properties properties = new Properties();
    properties.setProperty("user", "root");
    properties.setProperty("password", password);
    properties.setProperty("sql_dialect", "table");
    properties.setProperty("time_zone", "UTC");
    long allRows = 0;
    int clients = 0;
    try (Connection connection = DriverManager.getConnection(plan.get("jdbcUrl").getAsString(), properties);
        BufferedWriter log = Files.newBufferedWriter(out.resolve("client-receipts.jsonl"), StandardOpenOption.CREATE_NEW)) {
      require(connection instanceof IoTDBConnection && ((IoTDBConnection) connection).getTimeFactor() == 1000,
          "Actual millisecond IoTDB JDBC connection required");
      JsonObject scale = oracle.getAsJsonObject("scales").getAsJsonObject("four-years");
      for (JsonElement element : scale.getAsJsonArray("clients")) {
        JsonObject expected = element.getAsJsonObject();
        String client = expected.get("client").getAsString();
        require(client.matches("MT_[0-9]{3}"), "Invalid source client");
        String sql = "SELECT time,client_id,power_kw,source_row_no,record_present FROM "
            + database + "." + table + " WHERE client_id='" + client + "' ORDER BY time";
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        long rows = 0;
        long previousTime = Long.MIN_VALUE;
        long zeroRows = 0;
        try (Statement statement = connection.createStatement()) {
          statement.setQueryTimeout(600);
          statement.setFetchSize(4096);
          try (ResultSet result = statement.executeQuery(sql)) {
            ResultSetMetaData meta = result.getMetaData();
            List<String> types = List.of("TIMESTAMP", "STRING", "DOUBLE", "INT64", "BOOLEAN");
            require(meta.getColumnCount() == types.size(), "Column count mismatch");
            for (int i = 0; i < types.size(); i++) {
              require(types.get(i).equals(meta.getColumnTypeName(i + 1)), "Type mismatch at column " + (i + 1));
            }
            while (result.next()) {
              long time = result.getLong(1);
              require(!result.wasNull(), "NULL time");
              String actualClient = result.getString(2);
              require(!result.wasNull() && client.equals(actualClient), "Wrong client identity");
              double power = result.getDouble(3);
              require(!result.wasNull() && Double.isFinite(power), "NULL/nonfinite power");
              long ordinal = result.getLong(4);
              require(!result.wasNull(), "NULL source row ordinal");
              boolean present = result.getBoolean(5);
              require(!result.wasNull() && present, "Missing record-present flag");
              rows++;
              require(ordinal == rows, "Missing, duplicated or permuted source row");
              require(rows == 1 || time - previousTime == 900000, "Time step/order mismatch");
              previousTime = time;
              String bits = HexFormat.of().toHexDigits(Double.doubleToRawLongBits(power));
              String encoded = time + "\t" + client + "\t" + bits + "\t" + ordinal + "\ttrue\n";
              digest.update(encoded.getBytes(StandardCharsets.UTF_8));
              if (power == 0) zeroRows++;
            }
          }
        }
        String contentSha = HexFormat.of().formatHex(digest.digest());
        JsonObject receipt = new JsonObject();
        receipt.addProperty("client", client);
        receipt.addProperty("rows", rows);
        receipt.addProperty("zeroRows", zeroRows);
        receipt.addProperty("contentSha256", contentSha);
        receipt.addProperty("expectedContentSha256", expected.get("contentSha256").getAsString());
        receipt.addProperty("sqlSha256", sha(sql.getBytes(StandardCharsets.UTF_8)));
        boolean match = rows == expected.get("rows").getAsLong()
            && zeroRows == expected.get("zeroRows").getAsLong()
            && contentSha.equals(expected.get("contentSha256").getAsString());
        receipt.addProperty("status", match ? "PASS_ALL_ORDERED_TYPED_SOURCE_FIELDS" : "FAIL_SOURCE_CONTENT");
        log.write(JSON.toJson(receipt) + "\n");
        log.flush();
        require(match, "Full source fingerprint mismatch for " + client);
        allRows += rows;
        clients++;
        if (clients % 10 == 0) System.out.println("VERIFIED " + clients + " clients " + allRows + " rows");
      }
    }
    require(clients == 370 && allRows == oracle.get("rows").getAsLong(), "Incomplete source validation");
    JsonObject receipt = new JsonObject();
    receipt.addProperty("status", "PASS_COMPLETE_SOURCE_ALL_TYPED_ROW_FINGERPRINTS");
    receipt.addProperty("clients", clients);
    receipt.addProperty("rows", allRows);
    receipt.addProperty("oracleManifestSha256", sha(oracleBytes));
    receipt.addProperty("clientReceiptsSha256", sha(Files.readAllBytes(out.resolve("client-receipts.jsonl"))));
    receipt.addProperty("performanceClaim", false);
    write(out.resolve("summary.json"), receipt);
    System.out.println(JSON.toJson(receipt));
  }
}
