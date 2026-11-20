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
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.session.TableSessionBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.write.record.Tablet;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Append original Electricity readings outside an unchanged warm-query time window. */
public final class ElectricityWarmPartitionGrowthAppend {
  private static final Gson JSON = new Gson();
  private static final String DB = "uci_electricity_v4";
  private static final String TABLE = "electricity_nominal";
  private static final String HEADER = "time,client_id,power_kw,source_row_no,record_present";
  private static final String SOURCE_SHA = "dfeecbef66a2ccd6c5936682947bfa8755d8e9fa246ea2724906935779e23d53";
  private static final String WINDOW_PLAN_SHA =
      "180a6fbda34bf779938f9e7ae35258fbad501249c5ca20db54b6001b42ee2712";
  private static final long EXPECTED_SKIPPED_PER_CLIENT = 2002;
  private record Window(long lo, long hi) {
    boolean contains(long time) {
      return lo <= time && time < hi;
    }
  }
  private static final long PARTITION_MS = 604_800_000L;
  private static final long NOMINAL_STEP_MS = 900_000L;
  private static final long TIMES = 140_256;
  private static final long FIRST_NOMINAL = 1293840900000L;
  private static final long LAST_NOMINAL = 1420070400000L;
  private static final long EXPECTED_INPUT = 51_894_720;
  private static final long EXPECTED_SKIPPED = 740_740;
  private static final int CAPACITY = 4096;
  private static final List<String> COLUMNS = List.of("client_id", "power_kw", "source_row_no", "record_present");
  private static final List<TSDataType> TYPES = List.of(TSDataType.STRING, TSDataType.DOUBLE, TSDataType.INT64, TSDataType.BOOLEAN);
  private static final List<ColumnCategory> CATEGORIES =
      List.of(ColumnCategory.TAG, ColumnCategory.FIELD, ColumnCategory.FIELD, ColumnCategory.FIELD);

  private static void need(boolean ok, String message) {
    if (!ok) throw new IllegalStateException(message);
  }

  private static void save(Path path, Object x) throws Exception {
    try (var out = Files.newBufferedWriter(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
      JSON.toJson(x, out);
      out.newLine();
    }
  }

  private static String code(Class<?> c) {
    return c.getProtectionDomain().getCodeSource().getLocation().toString();
  }

  private static ITableSession session(String address, String password) throws Exception {
    return new TableSessionBuilder().nodeUrls(List.of(address)).username("root").password(password).build();
  }

  private static boolean excluded(List<Window> windows, long t) {
    for (Window w : windows) {
      if (w.contains(t)) return true;
    }
    return false;
  }

  private static final class Readings implements AutoCloseable {
    final String device;
    final JsonObject expected;
    final MessageDigest digest;
    final BufferedReader input;
    long row;
    long skipped;

    Readings(Path source, JsonObject expected, int deviceOrdinal) throws Exception {
      this.expected = expected;
      device = "MT_" + String.format(java.util.Locale.ROOT, "%03d", deviceOrdinal);
      need(device.equals(expected.get("client").getAsString()), "wrong device order");
      Path file = Path.of(expected.get("path").getAsString()).toRealPath();
      need(file.equals(source.getParent().resolve("csv").resolve(device + ".csv")),
          "member lookup escaped dataset");
      need(Files.size(file) == expected.get("bytes").getAsLong(), "member size mismatch");
      digest = MessageDigest.getInstance("SHA-256");
      input = new BufferedReader(new InputStreamReader(
          new DigestInputStream(Files.newInputStream(file), digest), StandardCharsets.US_ASCII), 8192);
      try {
        need(HEADER.equals(input.readLine()), "unexpected source header");
      } catch (Exception e) {
        try { input.close(); } catch (Exception close) { e.addSuppressed(close); }
        throw e;
      }
    }

    Map<String,Object> finish() throws Exception {
      need(input.readLine() == null && row == TIMES && skipped == EXPECTED_SKIPPED_PER_CLIENT
          && row == expected.get("rows").getAsLong(), "input consumption mismatch");
      String sha = HexFormat.of().formatHex(digest.digest());
      need(sha.equals(expected.get("sha256").getAsString()), "input member hash mismatch");
      return Map.of("sourceSha256", sha, "sourceRows", row, "skippedWindowRows", skipped,
          "appendedRows", row - skipped);
    }

    @Override
    public void close() throws java.io.IOException {
      input.close();
    }
  }

  public static void main(String[] args) throws Exception {
    need(args.length == 2, "Usage: bound plan path, new receipt directory");
    Path params = Path.of(args[0]).toRealPath();
    Path out = Path.of(args[1]).toRealPath();
    // Ingestion is only ever permitted into a separately launched scratch clone.
    need(out.startsWith("/tmp/Dev_0") && out.getNameCount() >= 4
        && out.getName(2).toString().startsWith("sxing-iotdb-warm-partition-"), "unrecognized experiment root");
    need(params.startsWith(out), "parameter file outside the experiment root");
    var config = JsonParser.parseString(Files.readString(params)).getAsJsonObject();
    need(config.get("scope").getAsString().equals("WARM_PARTITION_STARTUP_OUTSIDE_FROZEN_WINDOWS"), "wrong import scope");
    String rpc = config.get("rpc").getAsString();
    need(rpc.matches("127\\.0\\.0\\.1:[0-9]{4,5}"), "loopback endpoint binding failed");
    need(config.get("sourceManifestSha256").getAsString().equals(SOURCE_SHA), "source manifest pin");
    need(config.get("shiftMs").getAsLong() == 1, "unexpected signed time shift");
    var hex = HexFormat.of();
    Path plan = Path.of(config.get("frozenWindowsPlan").getAsString()).toRealPath();
    byte[] planBytes = Files.readAllBytes(plan);
    need(hex.formatHex(MessageDigest.getInstance("SHA-256").digest(planBytes)).equals(WINDOW_PLAN_SHA),
        "warm window plan does not match the frozen SQL");
    var frozen = JsonParser.parseString(new String(planBytes, StandardCharsets.UTF_8)).getAsJsonObject();
    var windows = new java.util.ArrayList<Window>();
    long skip = 0;
    long prior = Long.MIN_VALUE;
    for (var item : frozen.getAsJsonArray("windows")) {
      var w = item.getAsJsonObject();
      long lo = w.get("lowerInclusiveEpochMs").getAsLong();
      long hi = w.get("upperExclusiveEpochMs").getAsLong();
      need(lo > prior && hi > lo && (hi - lo) % NOMINAL_STEP_MS == 0,
          "duplicate, inverted or off-cadence exclusion window");
      prior = hi;
      windows.add(new Window(lo, hi));
      skip += (hi - lo) / NOMINAL_STEP_MS;
    }
    need(windows.size() == 8 && skip == EXPECTED_SKIPPED_PER_CLIENT, "changed exclusion count");
    Path deployment = Path.of(config.get("endpointManifest").getAsString()).toRealPath();
    need(deployment.startsWith(out.getParent()), "unbound scratch deployment");
    byte[] deployed = Files.readAllBytes(deployment);
    need(hex.formatHex(MessageDigest.getInstance("SHA-256").digest(deployed))
        .equals(config.get("endpointManifestSha256").getAsString()), "deployment identity mismatch");
    var svc = JsonParser.parseString(new String(deployed, StandardCharsets.UTF_8)).getAsJsonObject();
    need(svc.get("expectedDataNodeCount").getAsInt() == 2
        && svc.get("jdbcUrl").getAsString().startsWith("jdbc:iotdb://" + rpc + "/"),
        "wrong service target");
    Path source = Path.of(config.get("sourceManifest").getAsString()).toRealPath();
    byte[] src = Files.readAllBytes(source);
    need(hex.formatHex(MessageDigest.getInstance("SHA-256").digest(src)).equals(SOURCE_SHA), "actual manifest bytes differ");
    var manifest = JsonParser.parseString(new String(src, StandardCharsets.UTF_8)).getAsJsonObject();
    need(DB.equals(manifest.get("database").getAsString()) && TABLE.equals(manifest.get("table").getAsString())
        && manifest.get("firstNominalTimeMs").getAsLong() == FIRST_NOMINAL
        && manifest.get("lastNominalTimeMs").getAsLong() == LAST_NOMINAL
        && manifest.get("timeRows").getAsLong() == TIMES && manifest.get("rows").getAsLong() == EXPECTED_INPUT,
        "unexpected dataset dimensions");
    var files = manifest.getAsJsonArray("csvFiles");
    need(files.size() == 370, "exact device population required");
    String password = System.getenv("IOTDB_ACCEPTANCE_PASSWORD");
    need(password != null, "isolated-clone password is missing");
    save(out.resolve("loaded-client.json"), Map.of("entry", code(ElectricityWarmPartitionGrowthAppend.class),
        "tablet", code(Tablet.class), "session", code(TableSessionBuilder.class), "gson", code(Gson.class),
        "sourceManifestSha256", SOURCE_SHA, "java", System.getProperty("java.version")));
    var transformed = MessageDigest.getInstance("SHA-256");
    var input = ByteBuffer.allocate(33);
    var byClient = new LinkedHashMap<String,Object>();
    var tab = new Tablet(TABLE, COLUMNS, TYPES, CATEGORIES, CAPACITY);
    tab.initBitMaps();
    long total = 0, skipped = 0, written = 0;
    int batches = 0;
    long nonemptyPartitions = 0;
    var open = new java.util.ArrayList<Readings>();
    try (var db = session(rpc, password);
         var flushes = Files.newBufferedWriter(out.resolve("partition-flushes.jsonl"),
             StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
      db.executeNonQueryStatement("USE " + DB);
      for (int i = 0; i < files.size(); i++) {
        open.add(new Readings(source, files.get(i).getAsJsonObject(), i + 1));
      }
      // A tablet never spans a physical partition or a scheduled FLUSH barrier.
      var firstRegionPartition = Math.floorDiv(FIRST_NOMINAL, PARTITION_MS);
      var lastRegionPartition = Math.floorDiv(LAST_NOMINAL, PARTITION_MS);
      for (long partition = firstRegionPartition; partition <= lastRegionPartition; partition++) {
        var nextPartitionTime = (partition + 1) * PARTITION_MS;
        var upto = Math.min(TIMES, Math.floorDiv(nextPartitionTime - 1 - FIRST_NOMINAL,
            NOMINAL_STEP_MS) + 1);
        long startWritten = written;
        long startSkipped = skipped;
        int startBatches = batches;
        var perPartition = MessageDigest.getInstance("SHA-256");
        for (int i = 0; i < open.size(); i++) {
          Readings client = open.get(i);
          while (client.row < upto) {
            String line = client.input.readLine();
            need(line != null, "unexpected source end");
            int a = line.indexOf(','), b = line.indexOf(',', a + 1), c = line.indexOf(',', b + 1),
                d = line.indexOf(',', c + 1);
            need(a > 0 && c > b + 1 && d > c + 1 && line.indexOf(',', d + 1) < 0, "malformed row");
            long time = Long.parseLong(line.substring(0, a));
            long ordinal = Long.parseLong(line.substring(c + 1, d));
            need(time == FIRST_NOMINAL + client.row * NOMINAL_STEP_MS && ordinal == ++client.row,
                "timestamp or ordinal drift");
            need(line.regionMatches(a + 1, client.device, 0, client.device.length())
                && b == a + 1 + client.device.length(), "wrong device");
            double value = Double.parseDouble(line.substring(b + 1, c));
            need(Double.isFinite(value), "non-finite source reading");
            String flag = line.substring(d + 1);
            need(flag.equals("true") || flag.equals("false"), "bad presence marker");
            long shifted = Math.addExact(time, 1L);
            need(Math.floorDiv(time, PARTITION_MS) == partition
                && Math.floorDiv(shifted, PARTITION_MS) == partition, "time partition changed");
            total++;
            if (excluded(windows, time) || excluded(windows, shifted)) {
              client.skipped++;
              skipped++;
              continue;
            }
            boolean present = flag.equals("true");
            long grownOrdinal = Math.addExact(ordinal, TIMES);
            int k = tab.getRowSize();
            tab.addTimestamp(k, shifted);
            tab.addValue("client_id", k, client.device);
            tab.addValue("power_kw", k, value);
            tab.addValue("source_row_no", k, grownOrdinal);
            tab.addValue("record_present", k, present);
            input.clear();
            input.putLong(shifted).putInt(i + 1).putLong(Double.doubleToRawLongBits(value))
                .putLong(grownOrdinal).put((byte) (present ? 1 : 0));
            transformed.update(input.array(), 0, input.position());
            perPartition.update(input.array(), 0, input.position());
            written++;
            if (tab.getRowSize() == CAPACITY) {
              db.insert(tab);
              tab.reset();
              batches++;
            }
          }
        }
        if (tab.getRowSize() > 0) {
          db.insert(tab);
          tab.reset();
          batches++;
        }
        long rows = written - startWritten;
        if (rows > 0) {
          db.executeNonQueryStatement("FLUSH " + DB);
          nonemptyPartitions++;
        }
        var ordered = new LinkedHashMap<String,Object>();
        ordered.put("partition", partition);
        ordered.put("appendedRows", rows);
        ordered.put("excludedRows", skipped - startSkipped);
        ordered.put("batches", batches - startBatches);
        ordered.put("projectionSha256", hex.formatHex(perPartition.digest()));
        ordered.put("flushed", rows > 0);
        JSON.toJson(ordered, flushes);
        flushes.newLine();
        flushes.flush();
        if (nonemptyPartitions % 12 == 0) {
          System.out.printf("PARTITION_GROWTH partition=%d written=%d flushed=%d%n",
              partition, written, nonemptyPartitions);
        }
      }
      for (Readings client : open) byClient.put(client.device, client.finish());
      need(total == EXPECTED_INPUT && skipped == EXPECTED_SKIPPED && written == total - skipped,
          "source count mismatch");
      var receipt = new LinkedHashMap<String,Object>();
      receipt.put("status", "PARTITION_GROWTH_OUTSIDE_EIGHT_WINDOWS_FLUSHED_ONE_AT_A_TIME");
      receipt.put("sourceRows", total);
      receipt.put("excludedRows", skipped);
      receipt.put("appendedRows", written);
      receipt.put("projectionSha256", hex.formatHex(transformed.digest()));
      receipt.put("sourceManifestSha256", SOURCE_SHA);
      receipt.put("frozenWindowsPlanSha256", WINDOW_PLAN_SHA);
      receipt.put("deploymentManifestSha256", config.get("endpointManifestSha256").getAsString());
      receipt.put("batches", batches);
      receipt.put("flushedPartitions", nonemptyPartitions);
      receipt.put("members", byClient);
      save(out.resolve("import-receipt.json"), receipt);
    } finally {
      for (Readings readings : open) readings.close();
    }
  }
}
