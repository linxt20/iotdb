/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.HexFormat;
import java.util.Properties;

/** Capture complete public JDBC metadata and plans; this client adds no server instrumentation. */
public final class SqlPlanCaptureDriver {
  private static final Gson JSON =
      new GsonBuilder().serializeNulls().disableHtmlEscaping().setPrettyPrinting().create();
  private static String sha(Path path) throws Exception {
    return HexFormat.of().formatHex(
        MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)));
  }
  private static JsonElement typed(ResultSet rows, int c, String type) throws Exception {
    JsonElement value = switch (type) {
      case "TIMESTAMP", "INT64", "BIGINT" -> JSON.toJsonTree(rows.getLong(c));
      case "INT32", "INTEGER" -> JSON.toJsonTree(rows.getInt(c));
      case "DOUBLE", "FLOAT" -> {
        double number = rows.getDouble(c);
        if (rows.wasNull()) yield JsonNull.INSTANCE;
        if (Double.isFinite(number)) yield JSON.toJsonTree(number);
        JsonObject tagged = new JsonObject();
        tagged.addProperty("nonFiniteDouble", Double.toString(number));
        yield tagged;
      }
      case "BOOLEAN" -> JSON.toJsonTree(rows.getBoolean(c));
      case "STRING", "TEXT", "VARCHAR" -> JSON.toJsonTree(rows.getString(c));
      default -> throw new IllegalStateException("Unsupported JDBC capture type: " + type);
    };
    return rows.wasNull() ? JsonNull.INSTANCE : value;
  }
  public static void main(String[] args) throws Exception {
    if (args.length != 1) throw new IllegalArgumentException("Specify parameter JSON");
    Path parameter = Path.of(args[0]).toAbsolutePath();
    JsonObject plan = JsonParser.parseString(Files.readString(parameter)).getAsJsonObject();
    Path out = Path.of(plan.get("output").getAsString());
    Files.createDirectory(out);
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    JsonObject identity = new JsonObject();
    identity.addProperty("parameterPath", parameter.toString());
    identity.addProperty("parameterSha256", sha(parameter));
    identity.addProperty("javaHome", System.getProperty("java.home"));
    identity.addProperty("javaVersion", System.getProperty("java.version"));
    identity.addProperty("entryCodeSource",
        SqlPlanCaptureDriver.class.getProtectionDomain().getCodeSource().getLocation().toString());
    identity.addProperty("jdbcCodeSource",
        Class.forName("org.apache.iotdb.jdbc.IoTDBDriver")
            .getProtectionDomain().getCodeSource().getLocation().toString());
    identity.addProperty("classpath", System.getProperty("java.class.path"));
    identity.addProperty("scope", "Public JDBC capture; not a performance measurement");
    Files.writeString(out.resolve("loaded-identity.json"), JSON.toJson(identity) + System.lineSeparator());
    Properties properties = new Properties();
    properties.setProperty("user", "root");
    String password = System.getenv(plan.get("passwordEnv").getAsString());
    if (password == null || password.isEmpty()) throw new IllegalArgumentException("Missing DB password");
    properties.setProperty("password", password);
    properties.setProperty("sql_dialect", "table");
    properties.setProperty("time_zone", "UTC");
    try (Connection connection = DriverManager.getConnection(
        plan.get("jdbcUrl").getAsString(), properties)) {
      for (JsonElement query : plan.getAsJsonArray("queries")) {
        JsonObject spec = query.getAsJsonObject();
        String name = spec.get("name").getAsString();
        if (!name.matches("[A-Za-z0-9-]+")) throw new IllegalArgumentException("Unsafe query label");
        JsonObject receipt = new JsonObject();
        receipt.addProperty("sql", spec.get("sql").getAsString());
        receipt.addProperty("startedAtUtc", java.time.Instant.now().toString());
        try (Statement statement = connection.createStatement()) {
          statement.setFetchSize(1024);
          statement.setQueryTimeout(180);
          boolean hasRows = statement.execute(spec.get("sql").getAsString());
          receipt.addProperty("hasResultSet", hasRows);
          if (hasRows) {
            JsonArray data = new JsonArray();
            JsonArray names = new JsonArray();
            JsonArray types = new JsonArray();
            try (ResultSet rows = statement.getResultSet()) {
              ResultSetMetaData meta = rows.getMetaData();
              for (int c = 1; c <= meta.getColumnCount(); c++) {
                names.add(meta.getColumnLabel(c));
                types.add(meta.getColumnTypeName(c));
              }
              while (rows.next()) {
                JsonArray row = new JsonArray();
                for (int c = 1; c <= meta.getColumnCount(); c++) {
                  row.add(typed(rows, c, types.get(c-1).getAsString()));
                }
                data.add(row);
                if (data.size() > 100000) throw new IllegalStateException("Capture row bound exceeded");
              }
            }
            receipt.add("columnNames", names);
            receipt.add("columnTypes", types);
            receipt.add("rows", data);
            receipt.addProperty("completeRowCount", data.size());
          }
          receipt.addProperty("status", "COMPLETE_PUBLIC_JDBC_RESULT");
        } catch (Exception failure) {
          receipt.addProperty("status", "FAILED");
          receipt.addProperty("error", failure.toString());
          Files.writeString(out.resolve(name+".json"), JSON.toJson(receipt)+System.lineSeparator());
          throw failure;
        }
        receipt.addProperty("finishedAtUtc", java.time.Instant.now().toString());
        Files.writeString(out.resolve(name+".json"), JSON.toJson(receipt)+System.lineSeparator());
      }
    }
  }
}
