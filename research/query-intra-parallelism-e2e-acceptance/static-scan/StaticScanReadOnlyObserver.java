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
import com.sun.tools.attach.VirtualMachine;
import java.io.InputStream;
import java.lang.instrument.Instrumentation;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.jar.JarFile;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

/**
 * An optional diagnostic attachment, separate from the unobserved frozen-A correctness run. It only
 * reads existing production fields. All writes belong to this tool's own MBean/window.
 */
public final class StaticScanReadOnlyObserver {
  private static final Gson JSON = new GsonBuilder().serializeNulls().create();
  private static final String DOMAIN =
      "org.apache.iotdb.research.static_scan:type=ReadOnlyChannelObserver";
  private static final String SERVICE =
      "org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService";
  private static final String MAIN = "org.apache.iotdb.db.service.DataNode";
  private static final long PERIOD_NANOS = 1000000L;
  private static final int MAX_OBJECTS = 8192;
  private static final int MAX_HISTORY_BYTES = 16 * 1024 * 1024;
  private static final ThreadLocal<Window> SAMPLING_WINDOW = new ThreadLocal<>();

  private static void require(boolean condition, String message) {
    if (!condition) throw new IllegalStateException(message);
  }

  private static String digest(byte[] bytes) throws Exception {
    return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
  }

  private static String digest(Path path) throws Exception {
    MessageDigest hash = MessageDigest.getInstance("SHA-256");
    try (InputStream stream = Files.newInputStream(path)) {
      for (byte[] block = new byte[1048576]; ; ) {
        int length = stream.read(block);
        if (length < 0) break;
        hash.update(block, 0, length);
      }
    }
    return java.util.HexFormat.of().formatHex(hash.digest());
  }

  private static Field field(Class<?> type, String name) throws Exception {
    Window window = SAMPLING_WINDOW.get();
    Map<String, Field> cached =
        window == null ? null : window.fields.computeIfAbsent(type, ignored -> new HashMap<>());
    if (cached != null && cached.containsKey(name)) return cached.get(name);
    for (Class<?> current = type; current != null; current = current.getSuperclass()) {
      try {
        Field field = current.getDeclaredField(name);
        field.setAccessible(true); // Only the reflection access flag; never Field.set.
        if (cached != null) cached.put(name, field);
        return field;
      } catch (NoSuchFieldException absent) {
        // Search inherited fields without initializing any production class.
      }
    }
    throw new NoSuchFieldException(type.getName() + "." + name);
  }

  private static Object readField(Object target, String name) throws Exception {
    require(target != null, "Cannot read a null production object: " + name);
    return field(target.getClass(), name).get(target);
  }

  private static Object optional(Object target, String name) throws Exception {
    try {
      return readField(target, name);
    } catch (NoSuchFieldException absent) {
      return null;
    }
  }

  private static String text(Object value) {
    require(value instanceof String, "Expected a String field");
    return (String) value;
  }

  private static long number(Object value) {
    require(value instanceof Number, "Expected a numeric field");
    return ((Number) value).longValue();
  }

  private static Map<?, ?> map(Object value) {
    require(value instanceof Map, "Expected an existing Map field");
    return (Map<?, ?>) value;
  }

  private static JsonObject fragment(Object value) throws Exception {
    JsonObject result = new JsonObject();
    String query = text(readField(value, "queryId"));
    long fragment = number(readField(value, "fragmentId"));
    String instance = text(readField(value, "instanceId"));
    result.addProperty("queryId", query);
    result.addProperty("fragmentId", fragment);
    result.addProperty("instanceId", instance);
    result.addProperty("fullId", query + "." + fragment + "." + instance);
    return result;
  }

  private static Class<?> loaded(Instrumentation instrumentation, String name) {
    List<Class<?>> found = new ArrayList<>();
    for (Class<?> type : instrumentation.getAllLoadedClasses())
      if (type.getName().equals(name)) found.add(type);
    require(found.size() == 1, "Expected exactly one already-loaded class: " + name);
    return found.get(0);
  }

  private static Path location(Class<?> type) throws Exception {
    require(type.getProtectionDomain().getCodeSource() != null, "Missing loaded code source");
    return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI()).toRealPath();
  }

  public interface ObserverMBean {
    String getIdentity();

    String start(
        String queryId, String sourceFragmentInstanceId, long durationMillis, int maxSnapshots);

    String read(String windowId);

    String stop(String windowId);
  }

  private static final class Window {
    final String id = UUID.randomUUID().toString();
    final String query;
    final String fragment;
    final long started = System.nanoTime();
    final long deadline;
    final int maxSnapshots;
    final IdentityHashMap<Object, Long> objects = new IdentityHashMap<>();
    final IdentityHashMap<Class<?>, Map<String, Field>> fields = new IdentityHashMap<>();
    final Set<Class<?>> verifiedClasses = Collections.newSetFromMap(new IdentityHashMap<>());
    final List<String> history = new ArrayList<>();
    final List<String> errors = new ArrayList<>();
    volatile boolean stop;
    volatile boolean finished;
    volatile boolean referencesReleased;
    Thread worker;
    long nextToken = 1, ticks, matches, unchanged, maxTickGap, previousTick, bytes;
    String previousStructure, reason = "ACTIVE";
    boolean truncated;

    Window(String query, String fragment, long durationMillis, int maxSnapshots) {
      this.query = query;
      this.fragment = fragment;
      this.maxSnapshots = maxSnapshots;
      deadline = started + TimeUnit.MILLISECONDS.toNanos(durationMillis);
    }

    long token(Object object) {
      if (object == null) return 0;
      Long existing = objects.get(object);
      if (existing != null) return existing;
      require(objects.size() < MAX_OBJECTS, "Observer object-token bound exceeded");
      long token = nextToken++;
      objects.put(object, token);
      return token;
    }

    void error(Throwable failure) {
      String error = failure.getClass().getName() + ": " + failure.getMessage();
      if (errors.size() < 64 && !errors.contains(error)) errors.add(error);
    }

    void finish(String reason) {
      synchronized (this) {
        this.reason = reason;
        objects.clear();
        fields.clear();
        verifiedClasses.clear();
        previousStructure = null;
        referencesReleased = true;
        finished = true;
      }
    }
  }

  public static final class Observer implements ObserverMBean {
    final Class<?> serviceClass;
    final JsonObject identity;
    final Path serverJar;
    private Window window;

    Observer(Class<?> serviceClass, JsonObject identity, Path serverJar) {
      this.serviceClass = serviceClass;
      this.identity = identity;
      this.serverJar = serverJar;
    }

    private Object manager() throws Exception {
      // agentmain has already required the original service MBean to exist and match its port.
      // In the reviewed frozen source the private service constructor is invoked only by this
      // holder.
      Class<?> holder = null;
      for (Class<?> nested : serviceClass.getDeclaredClasses())
        if (nested.getSimpleName().equals("MPPDataExchangeServiceHolder")) holder = nested;
      require(holder != null, "Missing already-active service singleton holder");
      Object service = field(holder, "INSTANCE").get(null);
      require(service != null, "Original MPP service singleton is absent");
      return readField(service, "mppDataExchangeManager");
    }

    private void productionObject(Window window, Object value) throws Exception {
      require(value != null, "Existing production object is absent");
      if (window.verifiedClasses.contains(value.getClass())) return;
      require(
          location(value.getClass()).equals(serverJar),
          "Observed object is not from the bound original server JAR");
      window.verifiedClasses.add(value.getClass());
    }

    private JsonObject future(Window w, Object future) throws Exception {
      JsonObject result = new JsonObject();
      result.addProperty("objectToken", w.token(future));
      if (future == null) return result;
      require(future instanceof Future, "Existing pending field is not a Future");
      Future<?> observed = (Future<?>) future;
      result.addProperty("isDone", observed.isDone());
      result.addProperty("isCancelled", observed.isCancelled());
      return result;
    }

    private JsonObject key(Object holder) throws Exception {
      JsonObject key = new JsonObject();
      JsonObject id = fragment(readField(holder, "localFragmentInstanceId"));
      key.addProperty("queryId", id.get("queryId").getAsString());
      key.addProperty("fragmentInstanceId", text(readField(holder, "fullFragmentInstanceId")));
      key.addProperty("planNodeId", text(readField(holder, "localPlanNodeId")));
      return key;
    }

    private JsonObject channel(Window w, Object channel, int index) throws Exception {
      productionObject(w, channel);
      JsonObject result = new JsonObject();
      result.addProperty("index", index);
      result.addProperty("class", channel.getClass().getName());
      result.addProperty("objectToken", w.token(channel));
      Object accounting;
      String name = channel.getClass().getName();
      if (name.endsWith(".LocalSinkChannel")) {
        accounting = readField(channel, "queue");
        productionObject(w, accounting);
        result.addProperty("queueObjectToken", w.token(accounting));
        Object receiver = readField(accounting, "sourceHandle");
        JsonObject binding = new JsonObject();
        if (receiver != null) {
          productionObject(w, receiver);
          Object receiverId = readField(receiver, "localFragmentInstanceId");
          Object receiverPlan = readField(receiver, "localPlanNodeId");
          binding.addProperty("bound", receiverId != null && receiverPlan instanceof String);
          if (receiverId != null) binding.add("fragment", fragment(receiverId));
          if (receiverPlan instanceof String)
            binding.addProperty("planNodeId", (String) receiverPlan);
          binding.addProperty("sourceHandleObjectToken", w.token(receiver));
        } else binding.addProperty("bound", false);
        binding.addProperty("localToProducer", true);
        result.add("actualReceiverBinding", binding);
        result.add("blockedFuture", future(w, readField(accounting, "blockedOnMemory")));
      } else {
        require(name.endsWith(".SinkChannel"), "Unrecognized existing physical channel");
        accounting = channel;
        JsonObject binding = new JsonObject();
        binding.addProperty("bound", true);
        binding.addProperty("localToProducer", false);
        binding.add("fragment", fragment(readField(channel, "remoteFragmentInstanceId")));
        binding.addProperty("planNodeId", text(readField(channel, "remotePlanNodeId")));
        Object endpoint = readField(channel, "remoteEndpoint");
        binding.addProperty("host", text(readField(endpoint, "ip")));
        binding.addProperty("port", number(readField(endpoint, "port")));
        result.add("actualReceiverBinding", binding);
        result.add("blockedFuture", future(w, readField(accounting, "blocked")));
      }
      result.add("actualMemoryKey", key(accounting));
      result.addProperty("maxBytesCanReserve", number(readField(accounting, "maxBytesCanReserve")));
      result.addProperty(
          "bufferRetainedSizeInBytes", number(readField(accounting, "bufferRetainedSizeInBytes")));
      result.addProperty(
          "memoryPoolObjectToken",
          w.token(readField(readField(accounting, "localMemoryManager"), "queryPool")));
      Object closed = optional(accounting, "closed");
      result.add("closed", closed == null ? JsonNull.INSTANCE : JSON.toJsonTree(closed));
      return result;
    }

    private JsonObject reservation(Window w, Object pool, JsonObject key) throws Exception {
      JsonObject result = key.deepCopy();
      Map<?, ?> queries = map(readField(pool, "queryMemoryReservations"));
      Object fragments = queries.get(key.get("queryId").getAsString());
      Object leaves =
          fragments instanceof Map
              ? ((Map<?, ?>) fragments).get(key.get("fragmentInstanceId").getAsString())
              : null;
      Object bytes =
          leaves instanceof Map
              ? ((Map<?, ?>) leaves).get(key.get("planNodeId").getAsString())
              : null;
      result.addProperty("queryPresent", fragments != null);
      result.addProperty("fragmentPresent", leaves != null);
      result.addProperty("planKeyPresent", bytes != null);
      result.add(
          "reservedBytes", bytes == null ? JsonNull.INSTANCE : JSON.toJsonTree(number(bytes)));
      return result;
    }

    private JsonObject sink(Window w, Object manager, Object sink) throws Exception {
      productionObject(w, sink);
      JsonObject result = new JsonObject();
      result.addProperty("sinkObjectToken", w.token(sink));
      result.addProperty("class", sink.getClass().getName());
      result.add("sourceFragment", fragment(readField(sink, "localFragmentInstanceId")));
      result.addProperty("channelNumField", number(readField(sink, "channelNum")));
      result.addProperty("maxBytesCanReserve", number(readField(sink, "maxBytesCanReserve")));
      Object strategy = readField(sink, "shuffleStrategy");
      result.addProperty("strategyClass", strategy.getClass().getName());
      Object threshold = optional(strategy, "channelMemoryThreshold");
      result.addProperty("roundRobinThresholdFieldPresent", threshold != null);
      result.add(
          "roundRobinChannelMemoryThreshold",
          threshold == null ? JsonNull.INSTANCE : JSON.toJsonTree(number(threshold)));
      Object pool = readField(readField(manager, "localMemoryManager"), "queryPool");
      productionObject(w, pool);
      long poolToken = w.token(pool);
      result.addProperty("memoryPoolObjectToken", poolToken);
      result.addProperty(
          "poolMaxBytesPerFragmentInstance",
          number(readField(pool, "maxBytesPerFragmentInstance")));
      Object list = readField(sink, "downStreamChannelList");
      require(list instanceof List, "Existing sink channels are not a List");
      List<?> channels = (List<?>) list;
      result.addProperty("actualChannelListSize", channels.size());
      JsonArray observations = new JsonArray();
      Map<String, JsonObject> keys = new LinkedHashMap<>();
      Set<Long> physical = new java.util.HashSet<>();
      for (int index = 0; index < channels.size(); index++) {
        JsonObject observed = channel(w, channels.get(index), index);
        physical.add(observed.get("objectToken").getAsLong());
        JsonObject key = observed.getAsJsonObject("actualMemoryKey");
        keys.put(JSON.toJson(key), key);
        observations.add(observed);
      }
      result.addProperty("uniquePhysicalChannels", physical.size());
      result.addProperty("uniqueActualMemoryKeys", keys.size());
      result.add("channels", observations);
      JsonArray reservations = new JsonArray();
      for (JsonObject key : keys.values()) reservations.add(reservation(w, pool, key));
      result.add("actualPoolReservations", reservations);
      JsonArray futures = new JsonArray();
      Object pending = readField(pool, "memoryReservationFutures");
      require(pending instanceof Iterable, "Existing pool future queue is not Iterable");
      for (Object item : (Iterable<?>) pending) {
        if (!w.query.equals(readField(item, "queryId"))) continue;
        JsonObject itemKey = new JsonObject();
        itemKey.addProperty("queryId", text(readField(item, "queryId")));
        itemKey.addProperty("fragmentInstanceId", text(readField(item, "fragmentInstanceId")));
        itemKey.addProperty("planNodeId", text(readField(item, "planNodeId")));
        if (!keys.containsKey(JSON.toJson(itemKey))) continue;
        JsonObject observed = future(w, item);
        observed.add("actualMemoryKey", itemKey);
        observed.addProperty("bytesToReserve", number(readField(item, "bytesToReserve")));
        observed.addProperty("maxBytesCanReserve", number(readField(item, "maxBytesCanReserve")));
        futures.add(observed);
      }
      result.add("actualReservationFutures", futures);
      return result;
    }

    private JsonObject sample(Window w) throws Exception {
      Object manager = manager();
      productionObject(w, manager);
      JsonArray sinks = new JsonArray();
      Map<?, ?> existing = map(readField(manager, "shuffleSinkHandles"));
      for (Map.Entry<?, ?> entry : existing.entrySet()) {
        JsonObject id = fragment(entry.getKey());
        if (!w.query.equals(id.get("queryId").getAsString())) continue;
        if (!w.fragment.isEmpty() && !w.fragment.equals(id.get("fullId").getAsString())) continue;
        Object value = entry.getValue();
        if (number(readField(value, "channelNum")) != 32) continue;
        sinks.add(sink(w, manager, value));
      }
      JsonObject sample = new JsonObject();
      sample.add("matching32ChannelSinks", sinks);
      sample.addProperty("matchedSinkCount", sinks.size());
      sample.addProperty("consistency", "WEAK_CONCURRENT_READ");
      return sample;
    }

    private void sampleLoop(Window w) {
      String reason = "TIME_LIMIT";
      SAMPLING_WINDOW.set(w); // This observer's own daemon-local cache only.
      try {
        long next = System.nanoTime();
        while (!w.stop && System.nanoTime() < w.deadline) {
          long began = System.nanoTime();
          synchronized (w) {
            if (w.previousTick != 0) w.maxTickGap = Math.max(w.maxTickGap, began - w.previousTick);
            w.previousTick = began;
            w.ticks++;
            try {
              JsonObject observed = sample(w);
              long ended = System.nanoTime();
              if (observed.get("matchedSinkCount").getAsInt() > 0) w.matches++;
              String structure = JSON.toJson(observed);
              if (structure.equals(w.previousStructure)) {
                w.unchanged++;
              } else {
                observed.addProperty("readStartedNanos", began);
                observed.addProperty("readEndedNanos", ended);
                String frozen = JSON.toJson(observed);
                int length = frozen.getBytes(StandardCharsets.UTF_8).length;
                if (w.history.size() >= w.maxSnapshots || w.bytes + length > MAX_HISTORY_BYTES) {
                  w.truncated = true;
                  reason = "HISTORY_BOUND";
                  break;
                }
                w.history.add(frozen);
                w.bytes += length;
                w.previousStructure = structure;
              }
            } catch (Exception | Error failure) {
              w.error(failure);
              reason = "READ_ERROR";
              break;
            }
          }
          next += PERIOD_NANOS;
          long remaining = next - System.nanoTime();
          if (remaining > 0) LockSupport.parkNanos(remaining);
          else next = System.nanoTime();
          if (Thread.interrupted() && w.stop) break;
        }
        if (w.stop) reason = "STOP_REQUESTED";
      } finally {
        SAMPLING_WINDOW.remove();
        w.finish(reason);
      }
    }

    @Override
    public String getIdentity() {
      return JSON.toJson(identity);
    }

    @Override
    public synchronized String start(
        String queryId, String sourceFragmentInstanceId, long durationMillis, int maxSnapshots) {
      require(window == null || window.finished, "An observer window is already active");
      require(queryId != null && !queryId.isBlank(), "An explicit query ID is required");
      require(
          durationMillis >= 1 && durationMillis <= 60000, "Window must be between 1 and 60000 ms");
      require(
          maxSnapshots >= 16 && maxSnapshots <= 2048, "Snapshot bound must be between 16 and 2048");
      Window next =
          new Window(
              queryId,
              sourceFragmentInstanceId == null ? "" : sourceFragmentInstanceId,
              durationMillis,
              maxSnapshots);
      window = next;
      next.worker = new Thread(() -> sampleLoop(next), "static-scan-read-only-observer");
      next.worker.setDaemon(true);
      next.worker.start();
      return next.id;
    }

    @Override
    public synchronized String read(String windowId) {
      require(window != null && window.id.equals(windowId), "Unknown observer window");
      Window w = window;
      synchronized (w) {
        JsonObject result = new JsonObject();
        result.addProperty("windowId", w.id);
        result.addProperty("queryId", w.query);
        result.addProperty("sourceFragmentFilter", w.fragment);
        result.addProperty("periodNanosRequested", PERIOD_NANOS);
        result.addProperty("startedNanos", w.started);
        result.addProperty("deadlineNanos", w.deadline);
        result.addProperty("readAtNanos", System.nanoTime());
        result.addProperty("retainedFieldCacheClasses", w.fields.size());
        result.addProperty("retainedVerificationCacheClasses", w.verifiedClasses.size());
        result.addProperty("actualMaximumTickGapNanos", w.maxTickGap);
        result.addProperty("ticks", w.ticks);
        result.addProperty("matchedTicks", w.matches);
        result.addProperty("unchangedSamples", w.unchanged);
        result.addProperty("finished", w.finished);
        result.addProperty("referencesReleased", w.referencesReleased);
        result.addProperty("endReason", w.reason);
        result.addProperty("historyTruncated", w.truncated);
        result.addProperty("historyBytes", w.bytes);
        result.addProperty("retainedProductionObjectReferences", w.objects.size());
        result.add("errors", JSON.toJsonTree(w.errors));
        JsonArray history = new JsonArray();
        for (String sample : w.history) history.add(JsonParser.parseString(sample));
        result.add("samples", history);
        result.add("identity", identity.deepCopy());
        result.addProperty(
            "scope",
            "Independent observed resource run; not the unobserved frozen-A run or a performance"
                + " sample");
        return JSON.toJson(result);
      }
    }

    @Override
    public String stop(String windowId) {
      Window w;
      synchronized (this) {
        require(window != null && window.id.equals(windowId), "Unknown observer window");
        w = window;
        w.stop = true;
        w.worker.interrupt(); // Only this observer's own daemon thread.
      }
      try {
        w.worker.join(3000);
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
      }
      require(
          w.finished && w.referencesReleased,
          "Observer has not finished releasing its own references");
      return read(windowId);
    }
  }

  public static void agentmain(String arguments, Instrumentation instrumentation) throws Exception {
    String[] binding = arguments.split("\n", -1);
    require(binding.length == 2 && binding[1].matches("[0-9a-f]{64}"), "Invalid manifest binding");
    Path bindingPath = Path.of(binding[0]);
    require(bindingPath.isAbsolute(), "Absolute agent manifest path required");
    byte[] manifestBytes = Files.readAllBytes(bindingPath);
    require(digest(manifestBytes).equals(binding[1]), "Agent manifest SHA differs");
    JsonObject bound =
        JsonParser.parseString(new String(manifestBytes, StandardCharsets.UTF_8)).getAsJsonObject();
    bound.addProperty("_manifestPath", bindingPath.toString());
    bound.addProperty("_manifestSha256", binding[1]);
    Path expectedJar = Path.of(bound.get("serverJar").getAsString()).toRealPath();
    require(
        digest(expectedJar).equals(bound.get("serverJarSha256").getAsString()),
        "Original server JAR SHA differs");
    Class<?> main = loaded(instrumentation, MAIN);
    Class<?> service = loaded(instrumentation, SERVICE);
    require(
        location(main).equals(expectedJar) && location(service).equals(expectedJar),
        "Actual loaded DataNode/MPP service code source differs");
    var server = ManagementFactory.getPlatformMBeanServer();
    boolean originalServiceRegistered = false;
    for (ObjectName name : server.queryNames(null, null)) {
      if (!SERVICE.equals(server.getMBeanInfo(name).getClassName())) continue;
      Object port = server.getAttribute(name, "RPCPort");
      if (port instanceof Number && ((Number) port).intValue() == bound.get("mppPort").getAsInt())
        originalServiceRegistered = true;
    }
    require(originalServiceRegistered, "Original active MPP service MBean/port is not registered");
    JsonObject identity = new JsonObject();
    identity.addProperty("pid", ProcessHandle.current().pid());
    identity.addProperty("serverJar", expectedJar.toString());
    identity.addProperty("serverJarSha256", digest(expectedJar));
    Path agentJar = location(StaticScanReadOnlyObserver.class);
    identity.addProperty("observerJar", agentJar.toString());
    identity.addProperty("observerJarSha256", digest(agentJar));
    require(
        identity
            .get("observerJarSha256")
            .getAsString()
            .equals(bound.get("observerJarSha256").getAsString()),
        "Actual observer JAR differs");
    JsonObject classes = new JsonObject();
    for (String requiredClass :
        List.of(
            MAIN,
            SERVICE,
            "org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager",
            "org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle",
            "org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel",
            "org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel",
            "org.apache.iotdb.db.queryengine.execution.exchange.SharedTsBlockQueue",
            "org.apache.iotdb.db.queryengine.execution.memory.MemoryPool"))
      require(
          bound.getAsJsonObject("classSha256").has(requiredClass),
          "Missing original class binding");
    try (JarFile jar = new JarFile(expectedJar.toFile())) {
      for (Map.Entry<String, JsonElement> entry : bound.getAsJsonObject("classSha256").entrySet()) {
        String path = entry.getKey().replace('.', '/') + ".class";
        var member = jar.getJarEntry(path);
        require(member != null, "Original class entry is absent: " + path);
        String actual;
        try (InputStream stream = jar.getInputStream(member)) {
          actual = digest(stream.readAllBytes());
        }
        require(
            actual.equals(entry.getValue().getAsString()),
            "Original classfile bytes differ: " + path);
        classes.addProperty(entry.getKey(), actual);
      }
    }
    identity.add("classResourceSha256", classes);
    identity.addProperty(
        "classIdentityScope",
        "Original on-disk classfile bytes and loaded code sources; no claim of extracting live"
            + " bytecode");
    identity.addProperty(
        "readOnlyPolicy",
        "Field.get only; no production state writes, transformers, class replacement, setters,"
            + " cancellation or service lifecycle calls");
    Object config = field(service, "config").get(null);
    identity.addProperty("actualDataNodeId", number(readField(config, "dataNodeId")));
    identity.addProperty("actualInternalAddress", text(readField(config, "internalAddress")));
    identity.addProperty("actualInternalPort", number(readField(config, "internalPort")));
    identity.addProperty("actualMppPort", number(readField(config, "mppDataExchangePort")));
    ObjectName object = new ObjectName(DOMAIN);
    require(
        !server.isRegistered(object),
        "Observer MBean is already registered; reuse its own interface");
    server.registerMBean(
        new StandardMBean(new Observer(service, identity, expectedJar), ObserverMBean.class),
        object);
  }

  private static JsonObject verifyTarget(JsonObject manifest) throws Exception {
    long pid = manifest.get("pid").getAsLong();
    require(pid != ProcessHandle.current().pid(), "Target cannot be the attach client itself");
    Path proc = Path.of("/proc", Long.toString(pid));
    String command =
        new String(Files.readAllBytes(proc.resolve("cmdline")), StandardCharsets.UTF_8);
    List<String> arguments = List.of(command.split(String.valueOf((char) 0)));
    require(arguments.contains(MAIN), "Target is not the actual Java DataNode main");
    Path java = proc.resolve("exe").toRealPath();
    require(
        java.equals(Path.of(manifest.get("javaExecutable").getAsString()).toRealPath()),
        "Actual target Java executable differs");
    require(
        digest(java).equals(manifest.get("javaExecutableSha256").getAsString()),
        "Actual target Java SHA differs");
    require(
        arguments.contains("-DIOTDB_HOME=" + manifest.get("iotdbHome").getAsString()),
        "Actual target IOTDB_HOME differs");
    require(
        arguments.contains("-DIOTDB_CONF=" + manifest.get("confDir").getAsString()),
        "Actual target IOTDB_CONF differs");
    require(
        arguments.contains(
            "-Dcom.sun.management.jmxremote.port=" + manifest.get("jmxPort").getAsInt()),
        "Actual target JMX port binding differs");
    String stat = Files.readString(proc.resolve("stat"));
    String startTicks = stat.substring(stat.lastIndexOf(')') + 2).split(" ")[19];
    require(
        startTicks.equals(manifest.get("processStartTicks").getAsString()),
        "PID was reused or start identity differs");
    Path jar = Path.of(manifest.get("serverJar").getAsString()).toRealPath();
    require(
        jar.startsWith(Path.of(manifest.get("iotdbHome").getAsString()).toRealPath()),
        "Bound server JAR is outside the target distribution");
    require(
        digest(jar).equals(manifest.get("serverJarSha256").getAsString()),
        "Bound original server JAR differs");
    JsonObject identity = new JsonObject();
    identity.addProperty("pid", pid);
    identity.addProperty("processStartTicks", startTicks);
    identity.addProperty("javaExecutable", java.toString());
    identity.addProperty("javaExecutableSha256", digest(java));
    identity.addProperty("commandLine", command.replace((char) 0, ' '));
    identity.addProperty("serverJarSha256", digest(jar));
    identity.addProperty("manifestPath", manifest.get("_manifestPath").getAsString());
    identity.addProperty("manifestSha256", manifest.get("_manifestSha256").getAsString());
    identity.addProperty("clientJavaRuntime", System.getProperty("java.runtime.version"));
    identity.addProperty("clientClasspath", System.getProperty("java.class.path"));
    Path clientJar = location(StaticScanReadOnlyObserver.class);
    identity.addProperty("clientObserverCodeSource", clientJar.toString());
    identity.addProperty("clientObserverJarSha256", digest(clientJar));
    require(
        clientJar.equals(Path.of(manifest.get("observerJar").getAsString()).toRealPath())
            && digest(clientJar).equals(manifest.get("observerJarSha256").getAsString()),
        "Actual attach/control client is not the bound observer JAR");
    return identity;
  }

  private static void save(Path path, Object value) throws Exception {
    require(!Files.exists(path), "Evidence output already exists: " + path);
    Files.writeString(path, JSON.toJson(value));
  }

  private static void attach(JsonObject manifest, Path output) throws Exception {
    JsonObject before = verifyTarget(manifest);
    Path agent = Path.of(manifest.get("observerJar").getAsString()).toRealPath();
    require(
        digest(agent).equals(manifest.get("observerJarSha256").getAsString()),
        "Observer JAR SHA differs");
    before.addProperty("observerJar", agent.toString());
    before.addProperty("observerJarSha256", digest(agent));
    before.addProperty("operation", "ATTACH_READ_ONLY_OBSERVER_NO_WINDOW_STARTED");
    String arguments =
        manifest.get("_manifestPath").getAsString() + "\n" + manifest.get("_manifestSha256").getAsString();
    int requestBytes = (agent + "=" + arguments).getBytes(StandardCharsets.UTF_8).length;
    require(requestBytes < 1024, "Agent path and binding exceed HotSpot attach argument limit");
    before.addProperty("agentBindingTransport", "ABSOLUTE_MANIFEST_PATH_AND_SHA256");
    before.addProperty("agentLoadArgumentBytes", requestBytes);
    save(output.resolveSibling(output.getFileName() + ".before.json"), before);
    VirtualMachine machine = VirtualMachine.attach(Long.toString(manifest.get("pid").getAsLong()));
    try {
      var properties = machine.getSystemProperties();
      require(
          manifest.get("iotdbHome").getAsString().equals(properties.getProperty("IOTDB_HOME")),
          "Actual attached VM home differs");
      require(
          manifest.get("confDir").getAsString().equals(properties.getProperty("IOTDB_CONF")),
          "Actual attached VM config differs");
      machine.loadAgent(agent.toString(), arguments);
    } finally {
      machine.detach();
    }
    save(output, Map.of("status", "ATTACHED_NO_WINDOW", "target", verifyTarget(manifest)));
  }

  private static void control(String[] args, JsonObject manifest, Path output) throws Exception {
    JsonObject before = verifyTarget(manifest);
    before.addProperty("operation", args[0]);
    save(output.resolveSibling(output.getFileName() + ".before.json"), before);
    int port = manifest.get("jmxPort").getAsInt();
    try (JMXConnector connection =
        JMXConnectorFactory.connect(
            new JMXServiceURL("service:jmx:rmi:///jndi/rmi://127.0.0.1:" + port + "/jmxrmi"),
            Map.of("jmx.remote.x.request.waiting.timeout", 15000L))) {
      MBeanServerConnection server = connection.getMBeanServerConnection();
      ObjectName observer = new ObjectName(DOMAIN);
      JsonObject identity =
          JsonParser.parseString((String) server.getAttribute(observer, "Identity"))
              .getAsJsonObject();
      require(
          identity.get("pid").getAsLong() == manifest.get("pid").getAsLong()
              && identity
                  .get("serverJarSha256")
                  .getAsString()
                  .equals(manifest.get("serverJarSha256").getAsString())
              && identity
                  .get("observerJarSha256")
                  .getAsString()
                  .equals(manifest.get("observerJarSha256").getAsString()),
          "Actual observer MBean identity differs");
      if (args[0].equals("start")) {
        require(
            args.length == 7,
            "start MANIFEST QUERY_ID SOURCE_FI_OR_EMPTY DURATION_MS MAX_SNAPSHOTS NEW_OUTPUT");
        String window =
            (String)
                server.invoke(
                    observer,
                    "start",
                    new Object[] {
                      args[2], args[3], Long.parseLong(args[4]), Integer.parseInt(args[5])
                    },
                    new String[] {"java.lang.String", "java.lang.String", "long", "int"});
        save(output, Map.of("status", "WINDOW_STARTED", "windowId", window, "identity", identity));
      } else {
        require(args.length == 4, "read|stop MANIFEST WINDOW_ID NEW_OUTPUT");
        String raw =
            (String)
                server.invoke(
                    observer, args[0], new Object[] {args[2]}, new String[] {"java.lang.String"});
        save(output, JsonParser.parseString(raw));
      }
    }
    save(output.resolveSibling(output.getFileName() + ".after.json"), verifyTarget(manifest));
  }

  private static void schema(JsonObject manifest, Path output) throws Exception {
    Path jar = Path.of(manifest.get("serverJar").getAsString()).toRealPath();
    require(
        digest(jar).equals(manifest.get("serverJarSha256").getAsString()), "Schema JAR differs");
    Map<String, List<String>> fields = new LinkedHashMap<>();
    String exchange = "org.apache.iotdb.db.queryengine.execution.exchange.";
    String memory = "org.apache.iotdb.db.queryengine.execution.memory.";
    fields.put(SERVICE, List.of("config", "mppDataExchangeManager"));
    fields.put(SERVICE + "$MPPDataExchangeServiceHolder", List.of("INSTANCE"));
    fields.put(
        exchange + "MPPDataExchangeManager", List.of("shuffleSinkHandles", "localMemoryManager"));
    fields.put(
        exchange + "sink.ShuffleSinkHandle",
        List.of(
            "localFragmentInstanceId",
            "channelNum",
            "downStreamChannelList",
            "maxBytesCanReserve",
            "shuffleStrategy"));
    fields.put(
        exchange + "sink.SinkChannel",
        List.of(
            "localFragmentInstanceId",
            "fullFragmentInstanceId",
            "localPlanNodeId",
            "maxBytesCanReserve",
            "bufferRetainedSizeInBytes",
            "localMemoryManager",
            "blocked",
            "closed",
            "remoteEndpoint",
            "remoteFragmentInstanceId",
            "remotePlanNodeId"));
    fields.put(exchange + "sink.LocalSinkChannel", List.of("queue"));
    fields.put(
        exchange + "source.LocalSourceHandle",
        List.of("localFragmentInstanceId", "localPlanNodeId"));
    fields.put(
        exchange + "SharedTsBlockQueue",
        List.of(
            "localFragmentInstanceId",
            "fullFragmentInstanceId",
            "localPlanNodeId",
            "maxBytesCanReserve",
            "bufferRetainedSizeInBytes",
            "localMemoryManager",
            "blockedOnMemory",
            "closed",
            "sourceHandle"));
    fields.put(memory + "LocalMemoryManager", List.of("queryPool"));
    fields.put(
        memory + "MemoryPool",
        List.of(
            "maxBytesPerFragmentInstance", "queryMemoryReservations", "memoryReservationFutures"));
    fields.put(
        memory + "MemoryPool$MemoryReservationFuture",
        List.of(
            "queryId", "fragmentInstanceId", "planNodeId", "bytesToReserve", "maxBytesCanReserve"));
    fields.put(
        "org.apache.iotdb.db.conf.IoTDBConfig",
        List.of("dataNodeId", "internalAddress", "internalPort", "mppDataExchangePort"));
    List<URL> urls = new ArrayList<>();
    try (var paths = Files.list(jar.getParent())) {
      for (Path path : paths.filter(p -> p.toString().endsWith(".jar")).sorted().toList())
        urls.add(path.toUri().toURL());
    }
    JsonObject result = new JsonObject();
    JsonArray checked = new JsonArray();
    try (URLClassLoader isolated =
        new URLClassLoader(urls.toArray(new URL[0]), ClassLoader.getPlatformClassLoader())) {
      for (Map.Entry<String, List<String>> entry : fields.entrySet()) {
        // Standalone ABI inspection only. No class initialization, instance creation or Field.get.
        Class<?> type = Class.forName(entry.getKey(), false, isolated);
        require(location(type).equals(jar), "Schema class loaded outside the exact server JAR");
        for (String name : entry.getValue()) {
          Field field = field(type, name);
          checked.add(
              JSON.toJsonTree(
                  Map.of(
                      "class", entry.getKey(), "field", name, "type", field.getType().getName())));
        }
      }
      Class<?> strategy =
          Class.forName(
              exchange + "sink.ShuffleSinkHandle$SimpleRoundRobinStrategy", false, isolated);
      result.addProperty(
          "roundRobinThresholdField",
          field(strategy, "channelMemoryThreshold").getType().getName());
    }
    result.addProperty("status", "SCHEMA_CHECKED_NO_RUNTIME_EXECUTION");
    result.addProperty("serverJar", jar.toString());
    result.addProperty("serverJarSha256", digest(jar));
    result.addProperty("classInitializationRequested", false);
    result.add("fields", checked);
    save(output, result);
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0 || args[0].equals("--help")) {
      System.out.println(
          "schema|attach MANIFEST NEW_OUTPUT | start MANIFEST QUERY_ID SOURCE_FI_OR_EMPTY"
              + " DURATION_MS MAX_SNAPSHOTS NEW_OUTPUT | read|stop MANIFEST WINDOW_ID NEW_OUTPUT");
      return;
    }
    require(
        Set.of("schema", "attach", "start", "read", "stop").contains(args[0]),
        "Unknown observer operation");
    require(args.length >= 3, "A bound manifest and a new evidence path are required");
    Path bindingPath = Path.of(args[1]).toRealPath();
    JsonObject manifest = JsonParser.parseString(Files.readString(bindingPath)).getAsJsonObject();
    manifest.addProperty("_manifestPath", bindingPath.toString());
    manifest.addProperty("_manifestSha256", digest(bindingPath));
    Path output = Path.of(args[args.length - 1]);
    require(output.isAbsolute() && !Files.exists(output), "Absolute new output path is required");
    if (args[0].equals("schema")) {
      require(args.length == 3, "schema MANIFEST NEW_OUTPUT");
      schema(manifest, output);
    } else if (args[0].equals("attach")) {
      require(args.length == 3, "attach MANIFEST NEW_OUTPUT");
      attach(manifest, output);
    } else control(args, manifest, output);
  }
}
