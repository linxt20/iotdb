/* Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information.
 * Licensed under the Apache License, Version 2.0, http://www.apache.org/licenses/LICENSE-2.0
 * Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND. */
import java.lang.instrument.Instrumentation;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;

/** Reads actual loaded Class identities. No transformer, redefinition or production writes. */
public final class NegativeContractClassIdentityAgent {
  public static void premain(String configuration, Instrumentation instrumentation) throws Exception {
    Properties properties = new Properties();
    try (var reader = Files.newBufferedReader(Path.of(configuration))) { properties.load(reader); }
    Path output = Path.of(properties.getProperty("outputDirectory"));
    List<String> expected = List.of(properties.getProperty("expectedClasses").split(","));
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      Map<String, Object> receipt = new LinkedHashMap<>();
      List<Object> classes = new ArrayList<>(), errors = new ArrayList<>();
      String command = System.getProperty("sun.java.command", "");
      boolean testFork = command.contains("surefirebooter") || command.contains("ForkedBooter");
      receipt.put("scope", testFork ? "ACTUAL_SUREFIRE_LOADED_CLASS_RESOURCES" : "IGNORED_MAVEN_PARENT");
      receipt.put("pid", ProcessHandle.current().pid());
      receipt.put("javaCommand", command);
      receipt.put("javaVersion", System.getProperty("java.runtime.version"));
      receipt.put("classpath", System.getProperty("java.class.path"));
      receipt.put("bytecodeScope", "Original class-resource bytes of actual loaded Class/loader; no extraction or claim of transformed live bytecode");
      try {
        Path java = Path.of("/proc/self/exe").toRealPath();
        receipt.put("javaExecutable", java.toString()); receipt.put("javaExecutableSha256", sha(java));
        receipt.put("actualProcCommandLine", Files.readString(Path.of("/proc/self/cmdline")).replace((char)0, ' '));
        String maps = Files.readString(Path.of("/proc/self/maps"));
        receipt.put("actualProcMaps", maps);
        List<Object> nativeFiles = new ArrayList<>();
        Set<String> seenNative = new HashSet<>();
        for (String line : maps.split("\\n")) {
          String[] fields = line.trim().split("\\s+");
          if (fields.length < 6 || !fields[5].startsWith("/") || !fields[5].contains(".so")) continue;
          String name = fields[5];
          if (!seenNative.add(name)) continue;
          Map<String, Object> nativeFile = new LinkedHashMap<>();
          nativeFile.put("path", name);
          nativeFile.put("deletedMapping", line.endsWith(" (deleted)"));
          nativeFile.put("sha256", Files.isRegularFile(Path.of(name)) ? sha(Path.of(name)) : null);
          nativeFiles.add(nativeFile);
        }
        receipt.put("actualNativeFileIdentities", nativeFiles);
        Path agent = Path.of(NegativeContractClassIdentityAgent.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        receipt.put("agentJar", agent.toString()); receipt.put("agentJarSha256", sha(agent));
        if (testFork) for (Class<?> type : instrumentation.getAllLoadedClasses()) {
          if (expected.stream().noneMatch(name -> type.getName().equals(name) || type.getName().startsWith(name + "$"))) continue;
          Map<String, Object> value = new LinkedHashMap<>();
          value.put("class", type.getName());
          ClassLoader loader = type.getClassLoader();
          value.put("loaderClass", loader == null ? null : loader.getClass().getName());
          value.put("loaderIdentity", loader == null ? null : System.identityHashCode(loader));
          if (type.isHidden()) {
            value.put("identityScope", "HIDDEN_RUNTIME_CLASS_NO_CLASS_RESOURCE");
            value.put("resourceIdentityAvailable", false);
            value.put("nestHost", type.getNestHost().getName());
            classes.add(value);
            continue;
          }
          try {
            var source = type.getProtectionDomain().getCodeSource();
            value.put("codeSource", source == null ? null : source.getLocation().toString());
            String resource = "/" + type.getName().replace('.', '/') + ".class";
            var url = type.getResource(resource); value.put("classResourceUrl", url == null ? null : url.toString());
            try (var stream = type.getResourceAsStream(resource)) {
              if (stream == null) throw new IllegalStateException("Loaded class resource absent");
              value.put("classResourceSha256", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(stream.readAllBytes())));
            }
          } catch (Throwable failure) { value.put("failure", failure.toString()); errors.add(type.getName() + ": " + failure); }
          classes.add(value);
        }
      } catch (Throwable failure) { errors.add(failure.toString()); }
      receipt.put("loadedClasses", classes); receipt.put("errors", errors);
      try { Files.writeString(output.resolve("loaded-class-identity-" + ProcessHandle.current().pid() + ".json"),
          json(receipt) + "\n", StandardOpenOption.CREATE_NEW); }
      catch (Throwable failure) { failure.printStackTrace(); }
    }, "negative-contract-read-only-class-identity"));
  }

  private static String sha(Path path) throws Exception {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    try (var stream = Files.newInputStream(path)) { byte[] block = new byte[1048576]; int count;
      while ((count = stream.read(block)) >= 0) digest.update(block, 0, count); }
    return HexFormat.of().formatHex(digest.digest());
  }

  private static String json(Object value) {
    if (value == null) return "null";
    if (value instanceof Number || value instanceof Boolean) return value.toString();
    if (value instanceof Map<?, ?> map) { List<String> fields = new ArrayList<>();
      map.forEach((key, item) -> fields.add(json(key.toString()) + ":" + json(item))); return "{" + String.join(",", fields) + "}"; }
    if (value instanceof Collection<?> list) return "[" + String.join(",", list.stream().map(NegativeContractClassIdentityAgent::json).toList()) + "]";
    StringBuilder result = new StringBuilder("\"");
    for (char character : value.toString().toCharArray()) {
      if (character == '"' || character == '\\') result.append('\\').append(character);
      else if (character < 32) result.append(String.format("\\u%04x", (int)character));
      else result.append(character);
    }
    return result.append('"').toString();
  }
}
