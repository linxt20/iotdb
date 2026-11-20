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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/** Warm-connection paired SQL pilot. No JMX polling or intentional row pauses. */
public final class PairedSqlBenchmarkDriver {
  private static final Gson JSON = new GsonBuilder().serializeNulls().disableHtmlEscaping().create();

  private static void require(boolean condition, String message) {
    if (!condition) throw new IllegalStateException(message);
  }

  private static String sha(byte[] value) throws Exception {
    return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));
  }

  private static JsonElement typed(ResultSet result, int index, String type) throws Exception {
    JsonElement value;
    switch (type) {
      case "TIMESTAMP", "INT64", "BIGINT" -> value = JSON.toJsonTree(result.getLong(index));
      case "INT32", "INTEGER" -> value = JSON.toJsonTree(result.getInt(index));
      case "DOUBLE", "FLOAT" -> {
        double number = result.getDouble(index);
        require(result.wasNull() || Double.isFinite(number), "Nonfinite value");
        value = JSON.toJsonTree(number);
      }
      case "BOOLEAN" -> value = JSON.toJsonTree(result.getBoolean(index));
      case "STRING", "TEXT", "VARCHAR" -> value = JSON.toJsonTree(result.getString(index));
      default -> throw new IllegalStateException("Unsupported exact type: " + type);
    }
    return result.wasNull() ? JsonNull.INSTANCE : value;
  }

  private static List<Long> cpuTicks(JsonArray pids) throws Exception {
    List<Long> values = new ArrayList<>();
    for (JsonElement value : pids) {
      String raw = Files.readString(Path.of("/proc", value.getAsString(), "stat"));
      String[] fields = raw.substring(raw.lastIndexOf(')') + 2).split(" ");
      values.add(Long.parseLong(fields[11]) + Long.parseLong(fields[12]));
      values.add(Long.parseLong(fields[19]));
    }
    return values;
  }

  private static String normalizeExpected(String line, List<String> types) {
    JsonArray input = JsonParser.parseString(line).getAsJsonArray();
    require(input.size() == types.size(), "Oracle column count differs from schema");
    JsonArray output = new JsonArray();
    for (int i = 0; i < types.size(); i++) {
      JsonElement value = input.get(i);
      if (value.isJsonNull()) {
        output.add(JsonNull.INSTANCE);
        continue;
      }
      var primitive = value.getAsJsonPrimitive();
      switch (types.get(i)) {
        case "TIMESTAMP", "INT64", "BIGINT" -> {
          require(primitive.isNumber(), "Oracle integer is not a JSON number");
          output.add(Long.parseLong(primitive.getAsString()));
        }
        case "INT32", "INTEGER" -> {
          require(primitive.isNumber(), "Oracle integer is not a JSON number");
          output.add(Integer.parseInt(primitive.getAsString()));
        }
        case "DOUBLE", "FLOAT" -> {
          require(primitive.isNumber(), "Oracle double is not a JSON number");
          double number = primitive.getAsDouble();
          require(Double.isFinite(number), "Oracle double is not finite");
          output.add(number);
        }
        case "BOOLEAN" -> {
          require(primitive.isBoolean(), "Oracle boolean has wrong JSON type");
          output.add(primitive.getAsBoolean());
        }
        case "STRING", "TEXT", "VARCHAR" -> {
          require(primitive.isString(), "Oracle string has wrong JSON type");
          output.add(primitive.getAsString());
        }
        default -> throw new IllegalStateException("Unsupported oracle type");
      }
    }
    return JSON.toJson(output);
  }

  private static void sample(Connection connection, JsonObject endpoint, JsonObject query,
      String phase, int trial, int order, Path output, java.io.BufferedWriter samples)
      throws Exception {
    String name = query.get("name").getAsString();
    String side = endpoint.get("name").getAsString();
    Path attempt = output.resolve(phase).resolve(String.format("t%02d", trial))
        .resolve(name).resolve(side);
    Files.createDirectories(attempt);
    List<String> rows = new ArrayList<>();
    List<String> types = new ArrayList<>();
    Map<String, Object> receipt = new LinkedHashMap<>();
    receipt.put("phase", phase);
    receipt.put("trial", trial);
    receipt.put("orderWithinPair", order);
    receipt.put("query", name);
    receipt.put("side", side);
    receipt.put("sqlSha256", sha(query.get("sql").getAsString().getBytes(StandardCharsets.UTF_8)));
    receipt.put("startedAtUtc", java.time.Instant.now().toString());
    List<Long> before = cpuTicks(endpoint.getAsJsonArray("pids"));
    long elapsed;
    try (Statement statement = connection.createStatement()) {
      statement.setFetchSize(1024);
      statement.setQueryTimeout(180);
      long start = System.nanoTime();
      try (ResultSet result = statement.executeQuery(query.get("sql").getAsString())) {
        ResultSetMetaData metadata = result.getMetaData();
        for (int i = 1; i <= metadata.getColumnCount(); i++) types.add(metadata.getColumnTypeName(i));
        while (result.next()) {
          JsonArray row = new JsonArray();
          for (int i = 1; i <= types.size(); i++) row.add(typed(result, i, types.get(i - 1)));
          rows.add(JSON.toJson(row));
          require(rows.size() <= 100000, "Pilot output exceeded explicit 100000-row bound");
        }
      }
      elapsed = System.nanoTime() - start;
    } catch (Exception failure) {
      receipt.put("status", "FAILED_SQL_OR_DRAIN");
      receipt.put("error", failure.toString());
      Files.writeString(attempt.resolve("receipt.json"), JSON.toJson(receipt) + "\n");
      samples.write(JSON.toJson(receipt) + "\n");
      samples.flush();
      throw failure;
    }
    List<Long> after = cpuTicks(endpoint.getAsJsonArray("pids"));
    long ticks = 0;
    for (int i = 0; i < before.size(); i += 2) {
      require(before.get(i + 1).equals(after.get(i + 1)), "PID changed during sample");
      ticks += after.get(i) - before.get(i);
    }
    String raw = String.join("\n", rows) + (rows.isEmpty() ? "" : "\n");
    Files.writeString(attempt.resolve("result.rows.jsonl"), raw, StandardOpenOption.CREATE_NEW);
    List<String> compared = new ArrayList<>(rows);
    List<String> expectedTypes = new ArrayList<>();
    for (JsonElement type : query.getAsJsonArray("types")) expectedTypes.add(type.getAsString());
    List<String> expected = new ArrayList<>();
    for (String line : Files.readAllLines(Path.of(query.get("oracle").getAsString()))) {
      expected.add(normalizeExpected(line, expectedTypes));
    }
    if (!query.get("ordered").getAsBoolean()) {
      Collections.sort(compared);
      Collections.sort(expected);
    }
    receipt.put("status", types.equals(expectedTypes) && compared.equals(expected)
        ? "PASS_EXACT_COMPLETE_RESULT" : "FAIL_EXACT_COMPLETE_RESULT");
    receipt.put("endToEndNanos", elapsed);
    receipt.put("rows", rows.size());
    receipt.put("types", types);
    receipt.put("fullResultDrained", true);
    receipt.put("sequenceSha256", sha(raw.getBytes(StandardCharsets.UTF_8)));
    receipt.put("oracleSha256", sha(Files.readAllBytes(Path.of(query.get("oracle").getAsString()))));
    receipt.put("deploymentCpuTicks", ticks);
    receipt.put("cpuScope", "sum of named two DataNode processes, including their background work");
    receipt.put("finishedAtUtc", java.time.Instant.now().toString());
    Files.writeString(attempt.resolve("receipt.json"), JSON.toJson(receipt) + "\n");
    samples.write(JSON.toJson(receipt) + "\n");
    samples.flush();
    require("PASS_EXACT_COMPLETE_RESULT".equals(receipt.get("status")), "Complete typed result mismatch");
  }

  public static void main(String[] args) throws Exception {
    require(args.length == 1, "Specify exactly one parameter JSON");
    Path parameter = Path.of(args[0]).toAbsolutePath();
    JsonObject plan = JsonParser.parseString(Files.readString(parameter)).getAsJsonObject();
    Path output = Path.of(plan.get("output").getAsString());
    Files.createDirectory(output);
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    Map<String, Object> identity = new LinkedHashMap<>();
    identity.put("parameterPath", parameter.toString());
    identity.put("parameterSha256", sha(Files.readAllBytes(parameter)));
    identity.put("javaVersion", System.getProperty("java.version"));
    identity.put("javaHome", System.getProperty("java.home"));
    identity.put("classpath", System.getProperty("java.class.path"));
    identity.put("entryCodeSource", PairedSqlBenchmarkDriver.class.getProtectionDomain().getCodeSource().getLocation().toString());
    identity.put("jdbcCodeSource", Class.forName("org.apache.iotdb.jdbc.IoTDBDriver").getProtectionDomain().getCodeSource().getLocation().toString());
    identity.put("timer", "warm JDBC executeQuery + complete typed drain + ResultSet.close; excludes connection setup, canonical sorting, oracle comparison and disk writes");
    identity.put("scope", "preliminary integrated-strategy comparison, not same-graph controller-only ablation or pure server execution time");
    Files.writeString(output.resolve("loaded-identity.json"), JSON.toJson(identity) + "\n");
    JsonArray endpoints = plan.getAsJsonArray("endpoints");
    Connection[] connections = new Connection[endpoints.size()];
    Random random = new Random(plan.get("seed").getAsLong());
    try (var samples = Files.newBufferedWriter(output.resolve("samples.jsonl"), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW)) {
      try {
        for (int i = 0; i < connections.length; i++) {
          Properties properties = new Properties();
          properties.setProperty("user", "root");
          String password = System.getenv(plan.get("passwordEnv").getAsString());
          require(password != null && !password.isEmpty(), "Missing database password environment");
          properties.setProperty("password", password);
          properties.setProperty("sql_dialect", "table");
          properties.setProperty("time_zone", "UTC");
          connections[i] = DriverManager.getConnection(endpoints.get(i).getAsJsonObject().get("jdbcUrl").getAsString(), properties);
        }
        for (String phase : List.of("warmup", "measurement")) {
          int count = plan.get(phase.equals("warmup") ? "warmups" : "pairs").getAsInt();
          for (int trial = 1; trial <= count; trial++) {
            List<JsonElement> queries = new ArrayList<>(plan.getAsJsonArray("queries").asList());
            Collections.shuffle(queries, random);
            for (JsonElement query : queries) {
              List<Integer> order = new ArrayList<>(List.of(0, 1));
              Collections.shuffle(order, random);
              for (int index = 0; index < order.size(); index++) {
                int side = order.get(index);
                sample(connections[side], endpoints.get(side).getAsJsonObject(), query.getAsJsonObject(), phase, trial, index + 1, output, samples);
              }
            }
            System.out.println(phase + " trial " + trial + " complete");
          }
        }
      } finally {
        for (Connection connection : connections) if (connection != null) connection.close();
      }
    }
  }
}
