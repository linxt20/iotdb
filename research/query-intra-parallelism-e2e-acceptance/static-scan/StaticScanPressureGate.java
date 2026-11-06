/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/** Persistent client of the independent observer MBean; never attaches or changes production state. */
public final class StaticScanPressureGate {
  private static final Gson JSON = new GsonBuilder().disableHtmlEscaping().create();
  private static final ObjectName OBSERVER;
  static {
    try {
      OBSERVER = new ObjectName("org.apache.iotdb.research.static_scan:type=ReadOnlyChannelObserver");
    } catch (Exception failure) {
      throw new ExceptionInInitializerError(failure);
    }
  }

  private static void require(boolean value, String message) {
    if (!value) throw new IllegalStateException(message);
  }

  private static JsonObject read(Path path) throws Exception {
    return JsonParser.parseString(Files.readString(path)).getAsJsonObject();
  }

  private static String sha(Path path) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (var input = Files.newInputStream(path)) {
      byte[] bytes = new byte[1048576];
      int count;
      while ((count = input.read(bytes)) >= 0) digest.update(bytes, 0, count);
    }
    return HexFormat.of().formatHex(digest.digest());
  }

  private static void save(Path path, JsonElement value) throws Exception {
    Files.writeString(path, JSON.toJson(value) + "\n", StandardOpenOption.CREATE_NEW);
  }

  private static JsonObject bound(JsonObject binding) throws Exception {
    Path path = Path.of(binding.get("path").getAsString());
    require(path.isAbsolute() && sha(path).equals(binding.get("sha256").getAsString()),
        "Manifest binding differs: " + path);
    return read(path);
  }

  private static void emit(String event, JsonObject detail) {
    JsonObject value = new JsonObject();
    value.addProperty("event", event);
    value.add("detail", detail);
    System.out.println(JSON.toJson(value));
    System.out.flush();
  }

  private static long number(JsonObject object, String key) {
    return object.get(key).getAsLong();
  }

  private static String text(JsonObject object, String key) {
    return object.get(key).getAsString();
  }

  private static final class Node implements AutoCloseable {
    final JsonObject manifest;
    final JMXConnector connector;
    final MBeanServerConnection server;
    final JsonObject identity;
    String window;

    Node(JsonObject manifest) throws Exception {
      this.manifest = manifest;
      verifyProcess();
      connector = JMXConnectorFactory.connect(new JMXServiceURL(
          "service:jmx:rmi:///jndi/rmi://127.0.0.1:" + number(manifest, "jmxPort") + "/jmxrmi"),
          Map.of("jmx.remote.x.request.waiting.timeout", 5000L));
      try {
        server = connector.getMBeanServerConnection();
        identity = JsonParser.parseString((String) server.getAttribute(OBSERVER, "Identity"))
            .getAsJsonObject();
        require(number(identity, "pid") == number(manifest, "pid"), "Observer PID differs");
        for (String name : List.of("serverJarSha256", "observerJarSha256"))
          require(text(identity, name).equals(text(manifest, name)), "Observer identity differs: " + name);
        require(identity.getAsJsonObject("classResourceSha256")
            .equals(manifest.getAsJsonObject("classSha256")), "Observer class identities differ");
      } catch (Exception | Error failure) {
        connector.close();
        throw failure;
      }
    }

    void verifyProcess() throws Exception {
      Path proc = Path.of("/proc", Long.toString(number(manifest, "pid")));
      Path java = proc.resolve("exe").toRealPath();
      require(java.equals(Path.of(text(manifest, "javaExecutable")).toRealPath())
          && sha(java).equals(text(manifest, "javaExecutableSha256")), "DataNode Java differs");
      String stat = Files.readString(proc.resolve("stat"));
      require(stat.substring(stat.lastIndexOf(')') + 2).split(" ")[19]
          .equals(text(manifest, "processStartTicks")), "DataNode PID was reused");
      List<String> args = List.of(new String(Files.readAllBytes(proc.resolve("cmdline")),
          StandardCharsets.UTF_8).split(String.valueOf((char) 0)));
      require(args.contains("org.apache.iotdb.db.service.DataNode"), "Not an actual DataNode");
      require(args.contains("-DIOTDB_HOME=" + text(manifest, "iotdbHome"))
          && args.contains("-DIOTDB_CONF=" + text(manifest, "confDir"))
          && args.contains("-Dcom.sun.management.jmxremote.port=" + number(manifest, "jmxPort")),
          "DataNode home/config/JMX binding differs");
      require(sha(Path.of(text(manifest, "serverJar"))).equals(text(manifest, "serverJarSha256"))
          && sha(Path.of(text(manifest, "observerJar"))).equals(text(manifest, "observerJarSha256")),
          "Original server or observer JAR differs");
    }

    void start(String query, long duration) throws Exception {
      window = (String) server.invoke(OBSERVER, "start",
          new Object[] {query, "", duration, 2048},
          new String[] {"java.lang.String", "java.lang.String", "long", "int"});
    }

    JsonObject operation(String name) throws Exception {
      return JsonParser.parseString((String) server.invoke(OBSERVER, name,
          new Object[] {window}, new String[] {"java.lang.String"})).getAsJsonObject();
    }

    @Override
    public void close() throws Exception {
      connector.close();
    }
  }

  private static JsonObject latest(JsonObject receipt) {
    require(receipt.getAsJsonArray("errors").size() == 0, "Observer read error");
    require(!receipt.get("historyTruncated").getAsBoolean(), "Observer history was truncated");
    require(!receipt.get("finished").getAsBoolean(), "Observer window ended before pressure proof");
    JsonArray samples = receipt.getAsJsonArray("samples");
    return samples.size() == 0 ? null : samples.get(samples.size() - 1).getAsJsonObject();
  }

  /** Validate actual channel/receiver/key binding before using any cap or reservation. */
  private static JsonObject source(JsonObject frame, JsonObject plan, Node owner, List<Node> nodes) {
    if (frame == null) return null;
    require(number(plan, "sourceCount") == 1 && number(plan, "bucketCount") == 32
        && number(plan, "hashChannelCount") == 32 && number(plan, "fragmentInstanceCount") == 34
        && number(plan, "scanDop") == 1, "Not the source32 acceptance shape");
    JsonObject found = null;
    for (JsonElement element : frame.getAsJsonArray("matching32ChannelSinks")) {
      JsonObject sink = element.getAsJsonObject();
      if (!text(sink.getAsJsonObject("sourceFragment"), "fullId")
          .equals(text(plan, "sourceFragmentInstanceId"))) continue;
      require(found == null, "Duplicate actual source sink");
      found = sink;
    }
    if (found == null) return null;
    require(number(owner.identity, "actualDataNodeId") == number(plan, "sourceDataNodeId"),
        "Source observed on a different actual DataNode");
    require(number(found, "channelNumField") == 32 && number(found, "actualChannelListSize") == 32
        && number(found, "uniquePhysicalChannels") == 32 && number(found, "uniqueActualMemoryKeys") == 1,
        "Actual source channels/keys differ");
    String[] memory = text(plan, "sourceMemoryKey").split("/", 2);
    require(memory.length == 2, "Incomplete frozen producer memory key");
    Set<Long> tokens = new HashSet<>();
    Set<Integer> indices = new HashSet<>();
    Set<String> receiverIds = new HashSet<>();
    JsonArray caps = new JsonArray();
    for (JsonElement element : found.getAsJsonArray("channels")) {
      JsonObject channel = element.getAsJsonObject();
      int index = (int) number(channel, "index");
      require(indices.add(index) && tokens.add(number(channel, "objectToken")), "Duplicate actual channel");
      JsonObject bucket = null;
      for (JsonElement e : plan.getAsJsonArray("buckets")) {
        JsonObject candidate = e.getAsJsonObject();
        if (number(candidate, "upstreamSinkChannelIndex") == index) {
          require(bucket == null, "Duplicate frozen channel edge");
          bucket = candidate;
        }
      }
      require(bucket != null && number(bucket, "bucket") == index, "Missing frozen bucket edge");
      JsonObject binding = channel.getAsJsonObject("actualReceiverBinding");
      if (!binding.get("bound").getAsBoolean()) return null;
      require(text(binding.getAsJsonObject("fragment"), "fullId").equals(text(bucket, "fragmentInstanceId"))
          && text(binding, "planNodeId").equals(text(bucket, "sourceExchangeId"))
          && receiverIds.add(text(bucket, "fragmentInstanceId")), "Actual receiver FI/edge differs");
      boolean local = binding.get("localToProducer").getAsBoolean();
      require(local == (number(bucket, "dataNodeId") == number(plan, "sourceDataNodeId")),
          "Actual local/remote placement differs");
      if (local) {
        require(number(channel, "queueObjectToken") > 0 && number(binding, "sourceHandleObjectToken") > 0,
            "Missing actual local queue/source handle");
      } else {
        Node receiver = null;
        for (Node node : nodes)
          if (number(node.identity, "actualDataNodeId") == number(bucket, "dataNodeId")) receiver = node;
        require(receiver != null && text(binding, "host").equals(text(receiver.identity, "actualInternalAddress"))
            && number(binding, "port") == number(receiver.identity, "actualMppPort"),
            "Actual remote MPP endpoint differs");
      }
      JsonObject key = channel.getAsJsonObject("actualMemoryKey");
      require(text(key, "queryId").equals(text(plan, "queryId"))
          && text(key, "fragmentInstanceId").equals(memory[0])
          && text(key, "planNodeId").equals(memory[1]), "Actual producer memory key differs");
      require(number(channel, "memoryPoolObjectToken") == number(found, "memoryPoolObjectToken")
          && number(channel, "maxBytesCanReserve") > 0, "Actual cap/pool differs");
      JsonObject cap = new JsonObject();
      cap.addProperty("bucket", index);
      cap.addProperty("maxBytesCanReserve", number(channel, "maxBytesCanReserve"));
      cap.addProperty("localToProducer", local);
      cap.addProperty("receiverFragmentInstanceId", text(bucket, "fragmentInstanceId"));
      cap.addProperty("sourceExchangeId", text(bucket, "sourceExchangeId"));
      caps.add(cap);
    }
    require(indices.size() == 32 && receiverIds.size() == 32, "Missing physical channels/receivers");
    JsonArray reservations = found.getAsJsonArray("actualPoolReservations");
    require(reservations.size() == 1, "Missing shared leaf account");
    JsonObject leaf = reservations.get(0).getAsJsonObject();
    require(text(leaf, "queryId").equals(text(plan, "queryId"))
        && text(leaf, "fragmentInstanceId").equals(memory[0])
        && text(leaf, "planNodeId").equals(memory[1]), "Pool reservation key differs");
    if (!leaf.get("planKeyPresent").getAsBoolean() || leaf.get("reservedBytes").isJsonNull()
        || number(leaf, "reservedBytes") <= 0) return null;
    JsonObject result = new JsonObject();
    result.add("sink", found.deepCopy());
    result.add("bucketCaps", caps);
    result.addProperty("reservedBytes", number(leaf, "reservedBytes"));
    result.addProperty("sourcePid", number(owner.identity, "pid"));
    result.addProperty("windowId", owner.window);
    result.addProperty("frameReadStartedNanos", number(frame, "readStartedNanos"));
    for (JsonElement element : found.getAsJsonArray("actualReservationFutures")) {
      JsonObject future = element.getAsJsonObject();
      if (future.get("isDone").getAsBoolean() || future.get("isCancelled").getAsBoolean()) continue;
      require(future.getAsJsonObject("actualMemoryKey").equals(channelKey(found)), "Pending future key differs");
      long cap = number(future, "maxBytesCanReserve");
      long request = number(future, "bytesToReserve");
      // This exact blocked request exceeds its actual account cap. No guessed 4 MiB or percentage.
      boolean boundBlockedChannel = false;
      for (JsonElement channelElement : found.getAsJsonArray("channels")) {
        JsonObject channel = channelElement.getAsJsonObject();
        JsonObject blocked = channel.getAsJsonObject("blockedFuture");
        if (number(blocked, "objectToken") == number(future, "objectToken")
            && !blocked.get("isDone").getAsBoolean() && !blocked.get("isCancelled").getAsBoolean()
            && number(channel, "maxBytesCanReserve") == cap) boundBlockedChannel = true;
      }
      if (boundBlockedChannel && cap > 0 && request > 0 && number(leaf, "reservedBytes") > cap - request) {
        result.add("matchingPendingFuture", future.deepCopy());
        break;
      }
    }
    return result;
  }

  private static JsonObject channelKey(JsonObject sink) {
    return sink.getAsJsonArray("channels").get(0).getAsJsonObject().getAsJsonObject("actualMemoryKey");
  }

  private static String signature(JsonObject candidate) {
    JsonObject sink = candidate.getAsJsonObject("sink");
    return text(candidate, "windowId") + ":" + number(candidate, "sourcePid") + ":"
        + number(sink, "sinkObjectToken") + ":" + JSON.toJson(channelKey(sink)) + ":"
        + number(candidate, "reservedBytes") + ":" + JSON.toJson(candidate.get("bucketCaps")) + ":"
        + JSON.toJson(candidate.get("matchingPendingFuture"));
  }

  private static void run(JsonObject manifest, Path output) throws Exception {
    Files.createDirectory(output);
    List<Node> nodes = new ArrayList<>();
    Throwable primary = null;
    JsonObject receipt = new JsonObject();
    receipt.addProperty("status", "FAIL");
    receipt.addProperty("scope", "INDEPENDENT_OBSERVED_SOURCE32_PRESSURE; NOT PERFORMANCE; NOT ORIGINAL_A_BC_PROTOCOL");
    boolean confirmed = false;
    try {
      String query = text(bound(manifest.getAsJsonObject("sideManifest")), "queryId");
      long duration = number(manifest, "durationMillis");
      require(duration >= 1000 && duration <= 60000, "Observer duration out of range");
      for (JsonElement binding : manifest.getAsJsonArray("observers"))
        nodes.add(new Node(bound(binding.getAsJsonObject())));
      require(nodes.size() == 2 && number(nodes.get(0).identity, "pid") != number(nodes.get(1).identity, "pid")
          && number(nodes.get(0).identity, "actualDataNodeId") != number(nodes.get(1).identity, "actualDataNodeId"),
          "Exactly two distinct actual DataNodes required");
      for (Node node : nodes) node.start(query, duration);
      JsonObject started = new JsonObject();
      JsonArray windows = new JsonArray();
      for (Node node : nodes) {
        JsonObject window = new JsonObject();
        window.addProperty("pid", number(node.identity, "pid"));
        window.addProperty("windowId", node.window);
        windows.add(window);
      }
      started.add("windows", windows);
      emit("WINDOWS_STARTED", started);
      BufferedReader control = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
      Path planPath = Path.of(text(manifest, "frozenPlanPath"));
      long deadline = System.nanoTime() + (duration - 100) * 1000000L;
      boolean reserved = false;
      boolean held = false;
      long barrierReadAt = 0;
      long barrierTicks = -1;
      long barrierUnchanged = -1;
      JsonObject first = null;
      JsonObject firstReceipt = null;
      String stable = null;
      Node sourceNode = null;
      JsonObject frozenPlan = null;
      int planWriteRetries = 0;
      while (System.nanoTime() < deadline) {
        if (control.ready()) {
          String command = control.readLine();
          require(command != null && command.equals("HOLD_ACK") && reserved && !held,
              "Unexpected or closed runner control channel");
          held = true;
          JsonObject barrier = sourceNode.operation("read");
          latest(barrier);
          // All comparisons below use the observer JVM's clock, never a cross-JVM nanoTime.
          barrierReadAt = number(barrier, "readAtNanos");
          barrierTicks = number(barrier, "ticks");
          barrierUnchanged = number(barrier, "unchangedSamples");
          save(output.resolve("hold-barrier.json"), barrier);
        }
        if (!Files.exists(planPath)) {
          Thread.sleep(2);
          continue;
        }
        if (frozenPlan == null) {
          try {
            String initialPlan = Files.readString(planPath);
            if (initialPlan.isBlank()) {
              require(++planWriteRetries <= 500, "Frozen plan remained empty during initial write");
              Thread.sleep(2);
              continue;
            }
            frozenPlan = JsonParser.parseString(initialPlan).getAsJsonObject();
          } catch (JsonParseException partialWrite) {
            // The accepted helper writes this newly owned output once, with Files.writeString.
            // Do not mistake the first partially written bytes for a complete frozen plan.
            require(++planWriteRetries <= 500, "Frozen plan never completed its initial JSON write");
            Thread.sleep(2);
            continue;
          }
          receipt.addProperty("frozenPlanSha256", sha(planPath));
          receipt.addProperty("initialPlanWriteRetries", planWriteRetries);
        }
        JsonObject plan = frozenPlan;
        require(text(plan, "queryId").equals(query), "Frozen query differs");
        JsonObject observed = null;
        JsonObject raw = null;
        for (Node node : nodes) {
          JsonObject current = node.operation("read");
          JsonObject candidate = source(latest(current), plan, node, nodes);
          if (candidate != null) {
            require(observed == null, "Actual source appears on two processes");
            observed = candidate;
            raw = current;
            sourceNode = node;
          }
        }
        if (observed != null && !reserved) {
          save(output.resolve("source-positive.json"), raw);
          save(output.resolve("source-positive-binding.json"), observed);
          reserved = true;
          emit("SOURCE_RESERVED", observed);
        }
        if (held && observed != null && observed.has("matchingPendingFuture")) {
          long frameTime = number(observed, "frameReadStartedNanos");
          boolean beyondBarrier = number(raw, "ticks") > barrierTicks
              && (frameTime > barrierReadAt || number(raw, "unchangedSamples") > barrierUnchanged);
          if (!beyondBarrier) {
            Thread.sleep(2);
            continue;
          }
          String candidateSignature = signature(observed);
          if (!candidateSignature.equals(stable)) {
            first = observed;
            firstReceipt = raw;
            stable = candidateSignature;
          } else if (number(raw, "readAtNanos") - number(firstReceipt, "readAtNanos") >= 20000000L) {
            // unchangedSamples advances only after sample() rereads ALL production fields and
            // confirms structural equality. A timer/tick alone is never accepted as a fresh read.
            boolean fresh = frameTime > number(first, "frameReadStartedNanos")
                || (frameTime == number(first, "frameReadStartedNanos")
                    && number(raw, "unchangedSamples") > number(firstReceipt, "unchangedSamples"));
            if (fresh && number(raw, "ticks") > number(firstReceipt, "ticks")) {
              save(output.resolve("pressure-first.json"), firstReceipt);
              save(output.resolve("pressure-second.json"), raw);
              receipt.add("firstBinding", first);
              receipt.add("secondBinding", observed);
              receipt.addProperty("samePendingFutureAndReservation", true);
              receipt.addProperty("freshActualSamplingProof", frameTime > number(first, "frameReadStartedNanos")
                  ? "LATEST_FRAME_PRODUCTION_REREAD" : "UNCHANGED_COUNTER_AFTER_COMPLETE_PRODUCTION_REREAD");
              confirmed = true;
              emit("PRESSURE_CONFIRMED", observed);
              break;
            }
          }
        } else {
          // Never search old history for a sink/future after the latest real frame loses it.
          first = null;
          firstReceipt = null;
          stable = null;
        }
        Thread.sleep(2);
      }
      require(confirmed, "No stable source32 positive reservation + matching unfinished-cap future");
    } catch (Exception | Error failure) {
      primary = failure;
      receipt.addProperty("failure", failure.toString());
    } finally {
      JsonArray cleanupErrors = new JsonArray();
      for (int index = 0; index < nodes.size(); index++) {
        Node node = nodes.get(index);
        try {
          if (node.window != null) {
            JsonObject ended = node.operation("stop");
            save(output.resolve("observer-final-" + index + ".json"), ended);
            require(ended.get("referencesReleased").getAsBoolean()
                && number(ended, "retainedProductionObjectReferences") == 0
                && number(ended, "retainedFieldCacheClasses") == 0
                && number(ended, "retainedVerificationCacheClasses") == 0
                && !ended.get("historyTruncated").getAsBoolean()
                && ended.getAsJsonArray("errors").size() == 0, "Observer references/errors remain");
          }
          node.verifyProcess();
        } catch (Exception | Error failure) {
          cleanupErrors.add(failure.toString());
        } finally {
          try {
            node.close();
          } catch (Exception failure) {
            cleanupErrors.add(failure.toString());
          }
        }
      }
      receipt.add("cleanupErrors", cleanupErrors);
      receipt.addProperty("status", confirmed && primary == null && cleanupErrors.size() == 0 ? "PASS" : "FAIL");
      save(output.resolve("gate-receipt.json"), receipt);
      if (primary != null) throw new IllegalStateException("Pressure gate failed", primary);
      require(cleanupErrors.size() == 0, "Pressure gate cleanup failed");
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0 || args[0].equals("--help")) {
      System.out.println("StaticScanPressureGate BOUND_GATE_MANIFEST NEW_OUTPUT_DIRECTORY");
      return;
    }
    require(args.length == 2, "Expected bound gate manifest and new output directory");
    run(read(Path.of(args[0])), Path.of(args[1]));
  }
}
