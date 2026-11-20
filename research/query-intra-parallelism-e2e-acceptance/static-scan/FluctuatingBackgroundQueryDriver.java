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

import com.google.gson.*;
import java.io.*;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.jdbc.IoTDBConnection;
import org.apache.iotdb.jdbc.IoTDBStatement;

/** Open-loop ordinary SQL arrivals; no graph injection and no dependency on foreground DOP. */
public final class FluctuatingBackgroundQueryDriver {
  private static final Gson JSON = new GsonBuilder().serializeNulls().disableHtmlEscaping().create();
  private final JsonObject params;
  private final Path output;
  private final BufferedWriter events;
  private final List<String> types;
  private final Map<String, Integer> expected;
  private final Properties properties = new Properties();
  private final String baseSql;
  private final Semaphore slots;
  private final AtomicInteger admitted = new AtomicInteger();
  private final AtomicInteger rejected = new AtomicInteger();
  private final AtomicInteger completed = new AtomicInteger();
  private final AtomicInteger failed = new AtomicInteger();
  private final AtomicInteger inFlight = new AtomicInteger();
  private final AtomicInteger peak = new AtomicInteger();

  private static void require(boolean value, String message) {
    if (!value) throw new IllegalStateException(message);
  }
  private static JsonObject read(Path path) throws IOException {
    return JsonParser.parseString(Files.readString(path)).getAsJsonObject();
  }
  private static String text(JsonObject p, String key) { return p.get(key).getAsString(); }
  private static String sha(Path path) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (InputStream stream = Files.newInputStream(path)) {
      byte[] bytes = new byte[1048576];
      for (int n; (n = stream.read(bytes)) >= 0; ) digest.update(bytes, 0, n);
    }
    return HexFormat.of().formatHex(digest.digest());
  }
  private static void write(Path path, Object value) throws IOException {
    Files.writeString(path, JSON.toJson(value) + "\n", StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW);
  }
  private synchronized void event(String kind, Object... pairs) throws IOException {
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("kind", kind); row.put("atUtc", Instant.now().toString());
    row.put("monotonicNanos", System.nanoTime());
    for (int i = 0; i < pairs.length; i += 2) row.put((String) pairs[i], pairs[i + 1]);
    events.write(JSON.toJson(row)); events.newLine(); events.flush();
  }

  private String canonical(JsonArray row) {
    require(row.size() == types.size(), "Column count mismatch");
    JsonArray key = new JsonArray();
    for (int i = 0; i < types.size(); i++) {
      JsonArray cell = new JsonArray(); String type = types.get(i); cell.add(type);
      JsonElement value = row.get(i);
      if (value.isJsonNull()) cell.add(JsonNull.INSTANCE);
      else switch (type) {
        case "DOUBLE" -> {
          double number = value.getAsDouble(); require(Double.isFinite(number), "Nonfinite value");
          cell.add(Long.toHexString(Double.doubleToRawLongBits(number)));
        }
        case "TIMESTAMP", "INT64" -> cell.add(value.getAsLong());
        case "BOOLEAN" -> cell.add(value.getAsBoolean());
        case "STRING" -> cell.add(value.getAsString());
        default -> throw new IllegalStateException(type);
      }
      key.add(cell);
    }
    return JSON.toJson(key);
  }
  private JsonArray typed(ResultSet result) throws Exception {
    JsonArray row = new JsonArray();
    for (int i = 0; i < types.size(); i++) {
      int column = i + 1;
      JsonElement value = switch (types.get(i)) {
        case "TIMESTAMP", "INT64" -> JSON.toJsonTree(result.getLong(column));
        case "DOUBLE" -> JSON.toJsonTree(result.getDouble(column));
        case "BOOLEAN" -> JSON.toJsonTree(result.getBoolean(column));
        case "STRING" -> JSON.toJsonTree(result.getString(column));
        default -> throw new IllegalStateException(types.get(i));
      };
      row.add(result.wasNull() ? JsonNull.INSTANCE : value);
    }
    return row;
  }

  private FluctuatingBackgroundQueryDriver(Path parameterFile) throws Exception {
    params = read(parameterFile); output = Path.of(text(params, "out"));
    Files.createDirectory(output);
    events = Files.newBufferedWriter(output.resolve("arrivals.jsonl"), StandardOpenOption.CREATE_NEW);
    int cap = params.get("maximumInFlight").getAsInt();
    require(cap >= 1 && cap <= 32, "Invalid in-flight cap"); slots = new Semaphore(cap);
    types = new ArrayList<>();
    for (JsonElement value : params.getAsJsonArray("types")) types.add(value.getAsString());
    Path oracle = Path.of(text(params, "oracle"));
    require(sha(oracle).equals(text(params, "oracleSha256")), "Oracle SHA changed");
    Map<String, Integer> bag = new HashMap<>();
    try (BufferedReader stream = Files.newBufferedReader(oracle)) {
      for (String line; (line = stream.readLine()) != null; )
        bag.merge(canonical(JsonParser.parseString(line).getAsJsonArray()), 1, Integer::sum);
    }
    expected = Map.copyOf(bag);
    require(expected.values().stream().mapToInt(x -> x).sum() == params.get("expectedRows").getAsInt(), "Wrong oracle rows");
    Path sql = Path.of(text(params, "sqlFile"));
    require(sha(sql).equals(text(params, "sqlSha256")), "Background SQL changed");
    baseSql = Files.readString(sql);
    String password = System.getenv(text(params, "passwordEnv"));
    require(password != null && !password.isEmpty(), "Password environment missing");
    properties.setProperty("user", text(params, "username")); properties.setProperty("password", password);
    properties.setProperty("sql_dialect", "table"); properties.setProperty("time_zone", "UTC");
    properties.setProperty("network_timeout", "195000");
    write(output.resolve("preexecution-identity.json"), Map.of(
        "parametersSha256", sha(parameterFile), "sqlSha256", sha(sql), "oracleSha256", sha(oracle),
        "javaHome", System.getProperty("java.home"), "javaVersion", System.getProperty("java.version"),
        "classPath", System.getProperty("java.class.path"),
        "driverCodeSource", IoTDBConnection.class.getProtectionDomain().getCodeSource().getLocation().toString(),
        "entryCodeSource", getClass().getProtectionDomain().getCodeSource().getLocation().toString()));
  }

  private void request(int index, long planned) {
    Path work = output.resolve(String.format("request-%03d", index));
    long started = System.nanoTime();
    try {
      Files.createDirectory(work);
      event("EXECUTE_STARTED", "index", index, "plannedNanos", planned, "startedNanos", started);
      String sql = baseSql + "\n/* background_arrival=" + text(params, "runId") + ":" + index + " */";
      Files.writeString(work.resolve("executed.sql"), sql, StandardOpenOption.CREATE_NEW);
      Map<String, Integer> actual = new HashMap<>();
      long count = 0, response, drained;
      long rpcQueryId;
      try (Connection connection = DriverManager.getConnection(text(params, "jdbcUrl"), properties);
           Statement statement = connection.createStatement();
           BufferedWriter rows = Files.newBufferedWriter(work.resolve("result.rows.jsonl"), StandardOpenOption.CREATE_NEW)) {
        require(connection instanceof IoTDBConnection && ((IoTDBConnection) connection).getTimeFactor() == 1000,
            "Expected real millisecond-precision IoTDB connection");
        statement.setFetchSize(1024); statement.setQueryTimeout(180);
        try (ResultSet result = statement.executeQuery(sql)) {
          response = System.nanoTime();
          require(statement instanceof IoTDBStatement, "Expected real IoTDB statement");
          Field field = IoTDBStatement.class.getDeclaredField("queryId"); field.setAccessible(true);
          rpcQueryId = field.getLong(statement);
          ResultSetMetaData metadata = result.getMetaData();
          require(metadata.getColumnCount() == types.size(), "Actual schema column mismatch");
          for (int i = 0; i < types.size(); i++)
            require(types.get(i).equals(metadata.getColumnTypeName(i + 1)), "Actual schema type mismatch");
          event("EXECUTE_RESPONSE", "index", index, "plannedNanos", planned, "rpcQueryId", rpcQueryId);
          while (result.next()) {
            JsonArray row = typed(result); actual.merge(canonical(row), 1, Integer::sum);
            rows.write(JSON.toJson(row)); rows.newLine(); count++;
          }
          drained = System.nanoTime();
        }
      }
      require(actual.equals(expected), "Complete typed multiset mismatch");
      write(work.resolve("result-receipt.json"), Map.of("status", "PASS_COMPLETE_TYPED_MULTISET",
          "rows", count, "plannedNanos", planned, "startedNanos", started, "responseNanos", response,
          "drainedNanos", drained, "rpcQueryId", rpcQueryId, "resultRawSha256", sha(work.resolve("result.rows.jsonl")),
          "executedSqlSha256", sha(work.resolve("executed.sql"))));
      completed.incrementAndGet();
      event("COMPLETED", "index", index, "plannedNanos", planned, "drainedNanos", drained, "rows", count);
    } catch (Throwable failure) {
      failed.incrementAndGet();
      try {
        StringWriter stack = new StringWriter(); failure.printStackTrace(new PrintWriter(stack));
        write(work.resolve("failure.json"), Map.of("error", failure.toString(), "stack", stack.toString()));
        event("FAILED", "index", index, "plannedNanos", planned, "error", failure.toString());
      } catch (IOException recordingFailure) { failure.addSuppressed(recordingFailure); failure.printStackTrace(); }
    } finally {
      inFlight.decrementAndGet(); slots.release();
    }
  }

  private void run() throws Exception {
    // Connect and close before foreground starts; no timed query or audit window is opened here.
    try (Connection connection = DriverManager.getConnection(text(params, "jdbcUrl"), properties)) {
      require(connection instanceof IoTDBConnection, "Wrong JDBC driver");
    }
    long previous = -1;
    for (JsonElement value : params.getAsJsonArray("arrivalOffsetsNanos")) {
      long offset = value.getAsLong(); require(offset >= 0 && offset > previous, "Arrivals must strictly increase"); previous = offset;
    }
    write(output.resolve("ready.json"), Map.of("status", "WAITING_FOR_REAL_FOREGROUND_ACTIVITY", "monotonicNanos", System.nanoTime()));
    Path trigger = Path.of(text(params, "trigger"));
    long waitDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
    while (!Files.exists(trigger)) {
      require(System.nanoTime() < waitDeadline, "No foreground trigger within 120 seconds"); Thread.sleep(5);
    }
    JsonObject witness = read(trigger);
    long origin = witness.get("originMonotonicNanos").getAsLong();
    long received = System.nanoTime();
    require(received >= origin && received - origin < TimeUnit.SECONDS.toNanos(2), "Clock/trigger delay invalid");
    event("TRIGGER_RECEIVED", "originNanos", origin, "triggerDelayNanos", received - origin, "witness", witness);
    ExecutorService executor = Executors.newFixedThreadPool(params.get("maximumInFlight").getAsInt());
    int scheduled = 0;
    try {
      for (JsonElement offset : params.getAsJsonArray("arrivalOffsetsNanos")) {
        int index = scheduled++; long planned = Math.addExact(origin, offset.getAsLong());
        long remaining;
        while ((remaining = planned - System.nanoTime()) > 0)
          TimeUnit.NANOSECONDS.sleep(Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(20)));
        long attempted = System.nanoTime();
        if (!slots.tryAcquire()) {
          rejected.incrementAndGet();
          event("REJECTED_CAPACITY", "index", index, "plannedNanos", planned, "attemptNanos", attempted,
              "arrivalDelayNanos", attempted - planned, "inFlight", inFlight.get());
          continue;
        }
        admitted.incrementAndGet(); int active = inFlight.incrementAndGet(); peak.accumulateAndGet(active, Math::max);
        event("ADMITTED", "index", index, "plannedNanos", planned, "attemptNanos", attempted,
            "arrivalDelayNanos", attempted - planned, "inFlight", active);
        executor.submit(() -> request(index, planned));
      }
      event("ARRIVALS_CLOSED", "scheduled", scheduled);
    } finally { executor.shutdown(); }
    require(executor.awaitTermination(240, TimeUnit.SECONDS), "Background did not drain within its deadline");
    require(inFlight.get() == 0 && slots.availablePermits() == params.get("maximumInFlight").getAsInt(), "In-flight leak");
    require(scheduled == admitted.get() + rejected.get(), "Arrival accounting mismatch");
    require(admitted.get() == completed.get() + failed.get(), "Completion accounting mismatch");
    event("DRAINED", "scheduled", scheduled, "admitted", admitted.get(), "rejected", rejected.get(),
        "completed", completed.get(), "failed", failed.get(), "peakInFlight", peak.get());
    write(output.resolve("summary.json"), Map.of("status", failed.get() == 0 ? "PASS_OPEN_LOOP_COMPLETE_TYPED_RESULTS" : "BACKGROUND_QUERY_FAILURES",
        "scheduled", scheduled, "admitted", admitted.get(), "rejected", rejected.get(), "completed", completed.get(),
        "failed", failed.get(), "peakInFlight", peak.get(), "originNanos", origin, "allAdmittedQueriesDrained", true));
    require(failed.get() == 0, "At least one background query failed");
  }

  public static void main(String[] args) throws Exception {
    require(args.length == 1, "Pass the absolute parameter JSON path");
    FluctuatingBackgroundQueryDriver driver = new FluctuatingBackgroundQueryDriver(Path.of(args[0]).toAbsolutePath());
    try { driver.run(); }
    finally { driver.events.close(); }
  }
}
