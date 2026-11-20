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

import com.google.gson.*;
import java.io.BufferedWriter;
import java.lang.reflect.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.sql.*;
import java.util.*;

/** Matched ordinary-query server history, reusing the unchanged full-result timed sample. */
public final class ServerHistoryBlockedBenchmarkDriver {
  static final Gson JSON=new GsonBuilder().serializeNulls().disableHtmlEscaping().setPrettyPrinting().create();
  static final String HISTORY="SELECT query_id,state,start_time,end_time,cost_time,statement FROM information_schema.current_queries WHERE state='FINISHED'";
  static String sha(byte[] bytes)throws Exception{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));}
  static String canonical(String s){return s.replaceAll("/\\*.*?\\*/"," ").replaceAll("--[^\\r\\n]*"," ").replaceAll("\\s+"," ").trim().replaceFirst(";$","").trim().toLowerCase(Locale.ROOT);}
  static void write(Path p,JsonElement value)throws Exception{Files.createDirectories(p.getParent());Files.writeString(p,JSON.toJson(value)+System.lineSeparator(),StandardOpenOption.CREATE_NEW);}
  static JsonObject history(Connection c)throws Exception{
    JsonObject capture=new JsonObject();JsonArray names=new JsonArray(),types=new JsonArray(),rows=new JsonArray();
    try(Statement st=c.createStatement()){
      st.setQueryTimeout(30);st.setFetchSize(1024);
      try(ResultSet rs=st.executeQuery(HISTORY)){
        ResultSetMetaData meta=rs.getMetaData();
        for(int i=1;i<=meta.getColumnCount();i++){names.add(meta.getColumnLabel(i));types.add(meta.getColumnTypeName(i));}
        while(rs.next()){
          JsonObject row=new JsonObject();
          row.addProperty("queryId",rs.getString(1));row.addProperty("state",rs.getString(2));
          row.addProperty("startTime",rs.getString(3));row.addProperty("endTime",rs.getString(4));
          double cost=rs.getDouble(5);if(rs.wasNull())row.add("costSeconds",JsonNull.INSTANCE);else{if(!Double.isFinite(cost))throw new IllegalStateException("Nonfinite server cost");row.addProperty("costSeconds",cost);}
          row.addProperty("statement",rs.getString(6));
          if(!"FINISHED".equals(row.get("state").getAsString()))throw new IllegalStateException("History returned non-FINISHED row");
          rows.add(row);if(rows.size()>20000)throw new IllegalStateException("History exceeded explicit row bound");
        }
      }
    }
    capture.addProperty("sql",HISTORY);capture.add("columnNames",names);capture.add("columnTypes",types);capture.add("rows",rows);return capture;
  }
  public static void main(String[] args)throws Exception{
    if(args.length!=1)throw new IllegalArgumentException("Specify plan JSON");
    Path parameter=Path.of(args[0]).toAbsolutePath();JsonObject plan=JsonParser.parseString(Files.readString(parameter)).getAsJsonObject();
    JsonArray endpoints=plan.getAsJsonArray("endpoints");
    if(endpoints.size()!=4||plan.get("warmups").getAsInt()<5||plan.get("blocks").getAsInt()<15)throw new IllegalArgumentException("Require four endpoints, five warmups and fifteen blocks");
    Path out=Path.of(plan.get("output").getAsString());Files.createDirectory(out);
    Method sample=PairedSqlBenchmarkDriver.class.getDeclaredMethod("sample",Connection.class,JsonObject.class,JsonObject.class,String.class,int.class,int.class,Path.class,BufferedWriter.class);sample.setAccessible(true);
    Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
    JsonObject identity=new JsonObject();identity.addProperty("parameterPath",parameter.toString());identity.addProperty("parameterSha256",sha(Files.readAllBytes(parameter)));
    identity.addProperty("entryCodeSource",ServerHistoryBlockedBenchmarkDriver.class.getProtectionDomain().getCodeSource().getLocation().toString());identity.addProperty("sampleOwnerCodeSource",PairedSqlBenchmarkDriver.class.getProtectionDomain().getCodeSource().getLocation().toString());identity.addProperty("sampleMethod",sample.toGenericString());identity.addProperty("jdbcCodeSource",Class.forName("org.apache.iotdb.jdbc.IoTDBDriver").getProtectionDomain().getCodeSource().getLocation().toString());identity.addProperty("classpath",System.getProperty("java.class.path"));identity.addProperty("javaVersion",System.getProperty("java.version"));identity.addProperty("historySql",HISTORY);identity.addProperty("historyFingerprintRule","Same conservative comment/whitespace/case canonicalization as frozen server_current_query_metrics.py; restricted to this preregistered SQL family");identity.addProperty("clientTimer","Unchanged PairedSqlBenchmarkDriver.sample: executeQuery+full typed drain+ResultSet.close");identity.addProperty("serverTimer","Matched newly FINISHED current_queries.cost_time; not CPU time and not an additive phase decomposition");write(out.resolve("loaded-identity.json"),identity);
    String password=System.getenv(plan.get("passwordEnv").getAsString());if(password==null||password.isEmpty())throw new IllegalArgumentException("Missing DB password");
    Connection[] normal=new Connection[4],observer=new Connection[4];Random rng=new Random(plan.get("seed").getAsLong());
    try(BufferedWriter samples=Files.newBufferedWriter(out.resolve("samples.jsonl"));BufferedWriter costs=Files.newBufferedWriter(out.resolve("server-costs.jsonl"))){
      try{
        for(int i=0;i<4;i++){
          Properties p=new Properties();p.setProperty("user","root");p.setProperty("password",password);p.setProperty("sql_dialect","table");p.setProperty("time_zone","UTC");String url=endpoints.get(i).getAsJsonObject().get("jdbcUrl").getAsString();normal[i]=DriverManager.getConnection(url,p);observer[i]=DriverManager.getConnection(url,p);
        }
        for(String phase:List.of("warmup","measurement")){
          int blocks=plan.get(phase.equals("warmup")?"warmups":"blocks").getAsInt();
          for(int trial=1;trial<=blocks;trial++){
            List<JsonElement> queries=new ArrayList<>(plan.getAsJsonArray("queries").asList());Collections.shuffle(queries,rng);
            for(JsonElement qe:queries){
              JsonObject query=qe.getAsJsonObject();List<Integer> order=new ArrayList<>(List.of(0,1,2,3));Collections.shuffle(order,rng);
              for(int position=0;position<4;position++){
                int side=order.get(position);JsonObject endpoint=endpoints.get(side).getAsJsonObject();String name=query.get("name").getAsString();String policy=endpoint.get("name").getAsString();
                Path attempt=out.resolve(phase).resolve(String.format("t%02d",trial)).resolve(name).resolve(policy);Files.createDirectories(attempt);
                JsonObject before=history(observer[side]);write(attempt.resolve("history-before.json"),before);Set<String> old=new HashSet<>();for(JsonElement e:before.getAsJsonArray("rows"))old.add(e.getAsJsonObject().get("queryId").getAsString());
                try{sample.invoke(null,normal[side],endpoint,query,phase,trial,position+1,out,samples);}catch(InvocationTargetException e){if(e.getCause() instanceof Exception x)throw x;if(e.getCause() instanceof Error x)throw x;throw new IllegalStateException(e.getCause());}
                String fingerprint=canonical(query.get("sql").getAsString());JsonObject matched=null;long until=System.nanoTime()+5_000_000_000L;int poll=0;
                while(System.nanoTime()<=until){
                  JsonObject after=history(observer[side]);write(attempt.resolve(String.format("history-after-%03d.json",++poll)),after);List<JsonObject> matches=new ArrayList<>();
                  for(JsonElement e:after.getAsJsonArray("rows")){JsonObject row=e.getAsJsonObject();if(!old.contains(row.get("queryId").getAsString())&&fingerprint.equals(canonical(row.get("statement").getAsString())))matches.add(row);}
                  if(matches.size()>1)throw new IllegalStateException("Ambiguous newly finished target SQL");if(matches.size()==1){matched=matches.get(0);break;}Thread.sleep(25);
                }
                if(matched==null)throw new IllegalStateException("No newly finished SQL fingerprint match; no client-time fallback");
                double seconds=matched.get("costSeconds").getAsDouble();if(!(seconds>0)||!Double.isFinite(seconds))throw new IllegalStateException("Expected positive native server cost");
                JsonObject result=new JsonObject();result.addProperty("status","PASS_NEW_FINISHED_SERVER_QUERY_ID_AND_COMPLETE_RESULT");result.addProperty("phase",phase);result.addProperty("trial",trial);result.addProperty("query",name);result.addProperty("side",policy);result.addProperty("orderWithinBlock",position+1);result.addProperty("canonicalSqlSha256",sha(fingerprint.getBytes(java.nio.charset.StandardCharsets.UTF_8)));result.addProperty("serverCostMs",seconds*1000);result.add("matchedHistoryRow",matched);result.addProperty("normalReceiptSha256",sha(Files.readAllBytes(attempt.resolve("receipt.json"))));result.addProperty("serverMetricSource","information_schema.current_queries.cost_time");result.addProperty("historyObserverOutsideClientTimer",true);write(attempt.resolve("server-history-match.json"),result);costs.write(new GsonBuilder().serializeNulls().create().toJson(result)+System.lineSeparator());costs.flush();
              }
            }
            System.out.println(phase+" block "+trial+" complete");
          }
        }
      }finally{for(Connection c:normal)if(c!=null)c.close();for(Connection c:observer)if(c!=null)c.close();}
    }
  }
}
