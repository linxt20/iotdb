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

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExplainOutputFormat;
import org.apache.iotdb.db.queryengine.statistics.FragmentInstanceStatisticsJsonDrawer;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.time.ZoneId;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class ExplainAnalyzeOperatorTest {

  @Test
  public void testNextBuildsSingleJsonAnalyzeResult() throws Exception {
    ExplainAnalyzeOperator explainAnalyzeOperator =
        Mockito.mock(ExplainAnalyzeOperator.class, Mockito.CALLS_REAL_METHODS);
    Operator child = Mockito.mock(Operator.class);
    when(child.hasNextWithTimer()).thenReturn(false);

    MPPQueryContext context = createTestContext();
    context.setAnalyzeCost(1000000L);
    context.recordDispatchCost(2000000L);
    FragmentInstanceStatisticsJsonDrawer jsonDrawer = new FragmentInstanceStatisticsJsonDrawer();
    jsonDrawer.renderPlanStatistics(context);

    setField(explainAnalyzeOperator, "child", child);
    setField(explainAnalyzeOperator, "verbose", true);
    setField(explainAnalyzeOperator, "instances", Collections.emptyList());
    setField(explainAnalyzeOperator, "outputFormat", ExplainOutputFormat.JSON);
    setField(explainAnalyzeOperator, "fragmentInstanceStatisticsJsonDrawer", jsonDrawer);
    setField(explainAnalyzeOperator, "clientManager", null);
    setField(explainAnalyzeOperator, "mppQueryContext", context);

    TsBlock result = explainAnalyzeOperator.next();

    assertEquals(1, result.getPositionCount());
    String json = result.getColumn(0).getBinary(0).getStringValue(TSFileConfig.STRING_CHARSET);
    JsonObject root = JsonParser.parseString(json).getAsJsonObject();
    assertEquals(0, root.get("fragmentInstancesCount").getAsInt());
    assertTrue(root.getAsJsonObject("planStatistics").has("analyzeCostMs"));
    assertTrue(root.getAsJsonObject("planStatistics").has("dispatchCostMs"));
  }

  @Test
  public void largeTextResultIsBatchedWithoutChangingAnyRows() throws Exception {
    org.apache.iotdb.db.conf.DataNodeMemoryConfig memory =
        org.apache.iotdb.db.conf.IoTDBDescriptor.getInstance().getMemoryConfig();
    long original = memory.getMaxBytesPerFragmentInstance();
    try {
      memory.setMaxBytesPerFragmentInstance(4096);
      Operator child = Mockito.mock(Operator.class);
      ExplainAnalyzeOperator operator =
          Mockito.mock(ExplainAnalyzeOperator.class, Mockito.CALLS_REAL_METHODS);
      setField(operator, "child", child);
      java.util.List<String> expected = new java.util.ArrayList<>();
      org.apache.tsfile.read.common.block.TsBlockBuilder builder =
          new org.apache.tsfile.read.common.block.TsBlockBuilder(
              Collections.singletonList(org.apache.tsfile.enums.TSDataType.TEXT));
      for (int i = 0; i < 100; i++) {
        String line = "诊断" + i + "x".repeat(1000);
        expected.add(line);
        builder.getTimeColumnBuilder().writeLong(0);
        builder
            .getColumnBuilder(0)
            .writeBinary(
                new org.apache.tsfile.utils.Binary(line.getBytes(TSFileConfig.STRING_CHARSET)));
        builder.declarePosition();
      }
      setField(operator, "pendingResult", builder.build());
      java.util.List<String> actual = new java.util.ArrayList<>();
      int blocks = 0;
      while (operator.hasNext()) {
        assertTrue(operator.isBlocked().isDone());
        TsBlock block = operator.next();
        assertTrue(block.getSizeInBytes() <= 4096);
        assertTrue(
            "exchange accounts retained backing arrays", block.getRetainedSizeInBytes() <= 4096);
        for (int i = 0; i < block.getPositionCount(); i++)
          actual.add(block.getColumn(0).getBinary(i).getStringValue(TSFileConfig.STRING_CHARSET));
        blocks++;
      }
      assertEquals(expected, actual);
      assertTrue(blocks > 1);
      assertTrue(operator.isFinished());
      assertEquals(4096, operator.calculateMaxReturnSize());
      Mockito.verify(child, Mockito.never()).nextWithTimer();
    } finally {
      memory.setMaxBytesPerFragmentInstance(original);
    }
  }

  @Test
  public void closeDropsPendingOutputEvenWhenChildCloseFails() throws Exception {
    ExplainAnalyzeOperator operator =
        Mockito.mock(ExplainAnalyzeOperator.class, Mockito.CALLS_REAL_METHODS);
    Operator child = Mockito.mock(Operator.class);
    Mockito.doThrow(new IllegalStateException("injected child close failure")).when(child).close();
    setField(operator, "child", child);
    setField(operator, "pendingResult", Mockito.mock(TsBlock.class));
    try {
      operator.close();
      org.junit.Assert.fail("close failure was hidden");
    } catch (IllegalStateException expected) {
      Field field = ExplainAnalyzeOperator.class.getDeclaredField("pendingResult");
      field.setAccessible(true);
      org.junit.Assert.assertNull(field.get(operator));
    }
  }

  @Test
  public void oversizedSingleRowFailsWithExplicitMemoryReason() throws Exception {
    org.apache.iotdb.db.conf.DataNodeMemoryConfig memory =
        org.apache.iotdb.db.conf.IoTDBDescriptor.getInstance().getMemoryConfig();
    long original = memory.getMaxBytesPerFragmentInstance();
    try {
      memory.setMaxBytesPerFragmentInstance(32);
      ExplainAnalyzeOperator operator =
          Mockito.mock(ExplainAnalyzeOperator.class, Mockito.CALLS_REAL_METHODS);
      setField(operator, "child", Mockito.mock(Operator.class));
      org.apache.tsfile.read.common.block.TsBlockBuilder builder =
          new org.apache.tsfile.read.common.block.TsBlockBuilder(
              Collections.singletonList(org.apache.tsfile.enums.TSDataType.TEXT));
      builder.getTimeColumnBuilder().writeLong(0);
      builder
          .getColumnBuilder(0)
          .writeBinary(
              new org.apache.tsfile.utils.Binary(
                  "x".repeat(100).getBytes(TSFileConfig.STRING_CHARSET)));
      builder.declarePosition();
      setField(operator, "pendingResult", builder.build());
      try {
        operator.next();
        org.junit.Assert.fail("oversized row must fail");
      } catch (org.apache.iotdb.calc.exception.MemoryNotEnoughException expected) {
        assertTrue(expected.getMessage().contains("32"));
      }
    } finally {
      memory.setMaxBytesPerFragmentInstance(original);
    }
  }

  private static MPPQueryContext createTestContext() {
    SessionInfo sessionInfo = new SessionInfo(0, "root", ZoneId.systemDefault());
    MPPQueryContext context =
        new MPPQueryContext(
            "EXPLAIN ANALYZE (FORMAT JSON) SELECT * FROM table1",
            new QueryId("test_query_id"),
            sessionInfo,
            new TEndPoint("127.0.0.1", 6667),
            new TEndPoint("127.0.0.1", 10730));
    context.setAnalyzeCost(0);
    return context;
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = ExplainAnalyzeOperator.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }
}
