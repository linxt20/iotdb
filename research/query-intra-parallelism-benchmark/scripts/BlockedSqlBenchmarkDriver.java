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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/** Four-policy randomized blocks reusing the frozen paired driver's exact timed sample method. */
public final class BlockedSqlBenchmarkDriver {
  private static final Gson JSON =
      new GsonBuilder().serializeNulls().disableHtmlEscaping().setPrettyPrinting().create();
  public static void main(String[] args) throws Exception {
    if (args.length != 1) throw new IllegalArgumentException("Specify parameter JSON");
    Path parameter=Path.of(args[0]).toAbsolutePath();
    JsonObject plan=JsonParser.parseString(Files.readString(parameter)).getAsJsonObject();
    var endpoints=plan.getAsJsonArray("endpoints");
    if (endpoints.size()!=4 || plan.get("warmups").getAsInt()<5 || plan.get("blocks").getAsInt()<15)
      throw new IllegalArgumentException("Require four endpoints, at least five warmups and fifteen blocks");
    var names=new HashSet<String>();
    var urls=new HashSet<String>();
    for (JsonElement e:endpoints) {
      if (!names.add(e.getAsJsonObject().get("name").getAsString())
          || !urls.add(e.getAsJsonObject().get("jdbcUrl").getAsString()))
        throw new IllegalArgumentException("Duplicate endpoint name or JDBC URL");
    }
    Path output=Path.of(plan.get("output").getAsString());
    Files.createDirectory(output);
    // This is an explicit compatibility adapter for the frozen method; no timer/canonicalizer copy.
    Method sample=PairedSqlBenchmarkDriver.class.getDeclaredMethod("sample",
        Connection.class,JsonObject.class,JsonObject.class,String.class,int.class,int.class,
        Path.class,BufferedWriter.class);
    sample.setAccessible(true);
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    JsonObject identity=new JsonObject();
    identity.addProperty("parameterPath",parameter.toString());
    identity.addProperty("parameterSha256",HexFormat.of().formatHex(
        MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(parameter))));
    identity.addProperty("entryCodeSource",BlockedSqlBenchmarkDriver.class.getProtectionDomain().getCodeSource().getLocation().toString());
    identity.addProperty("sampleOwnerCodeSource",PairedSqlBenchmarkDriver.class.getProtectionDomain().getCodeSource().getLocation().toString());
    identity.addProperty("sampleMethod",sample.toGenericString());
    identity.addProperty("jdbcCodeSource",Class.forName("org.apache.iotdb.jdbc.IoTDBDriver").getProtectionDomain().getCodeSource().getLocation().toString());
    identity.addProperty("javaHome",System.getProperty("java.home"));
    identity.addProperty("javaVersion",System.getProperty("java.version"));
    identity.addProperty("classpath",System.getProperty("java.class.path"));
    identity.addProperty("scope","Four endpoint randomized blocks; legacy orderWithinPair receipt field is the within-block order 1..4");
    identity.addProperty("timer","Unchanged frozen sample: warm executeQuery, complete typed drain and ResultSet.close");
    Files.writeString(output.resolve("loaded-identity.json"),JSON.toJson(identity)+System.lineSeparator());
    Connection[] connections=new Connection[4];
    Random random=new Random(plan.get("seed").getAsLong());
    try (BufferedWriter samples=Files.newBufferedWriter(output.resolve("samples.jsonl"))) {
      try {
        String password=System.getenv(plan.get("passwordEnv").getAsString());
        if (password==null || password.isEmpty()) throw new IllegalArgumentException("Missing DB password");
        for (int i=0;i<4;i++) {
          Properties p=new Properties();
          p.setProperty("user","root");p.setProperty("password",password);
          p.setProperty("sql_dialect","table");p.setProperty("time_zone","UTC");
          connections[i]=DriverManager.getConnection(endpoints.get(i).getAsJsonObject().get("jdbcUrl").getAsString(),p);
        }
        for (String phase:List.of("warmup","measurement")) {
          int count=plan.get(phase.equals("warmup")?"warmups":"blocks").getAsInt();
          for (int trial=1;trial<=count;trial++) {
            List<JsonElement> queries=new ArrayList<>(plan.getAsJsonArray("queries").asList());
            Collections.shuffle(queries,random);
            for (JsonElement q:queries) {
              List<Integer> order=new ArrayList<>(List.of(0,1,2,3));
              Collections.shuffle(order,random);
              for (int position=0;position<4;position++) {
                int side=order.get(position);
                try {
                  sample.invoke(null,connections[side],endpoints.get(side).getAsJsonObject(),
                      q.getAsJsonObject(),phase,trial,position+1,output,samples);
                } catch (InvocationTargetException e) {
                  if (e.getCause() instanceof Exception x) throw x;
                  if (e.getCause() instanceof Error x) throw x;
                  throw new IllegalStateException(e.getCause());
                }
              }
            }
            System.out.println(phase+" block "+trial+" complete");
          }
        }
      } finally {
        for (Connection c:connections) if (c!=null) c.close();
      }
    }
  }
}
