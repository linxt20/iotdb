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

package org.apache.iotdb.db.queryengine.statistics;

import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.mpp.rpc.thrift.TOperatorStatistics;
import org.apache.iotdb.mpp.rpc.thrift.TQueryStatistics;

import org.junit.Test;

import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for the metric-tree rendering added to {@link FragmentInstanceStatisticsDrawer}.
 *
 * <p>These tests do NOT run a real query or touch {@code TestUtils.QUERY_CONTEXT}. They hand-
 * construct {@link TOperatorStatistics} maps and feed them directly to the drawer, verifying:
 *
 * <ol>
 *   <li>FI-level blocking ratio appears when block queue time is non-zero.
 *   <li>Operator-level throughput (rows/s) appears when CPU time and rows are both non-zero.
 *   <li>Parallel sub-scan pipelines keyed as {@code "__pipeline_<nodeId>-parallel-<i>"} are
 *       rendered as individual entries in the output (multi-driver expansion).
 *   <li>Aggregated output rows across N parallel drivers match the per-driver sum.
 *   <li>TsBlock output count injected via {@code __mt_tsBlockOutputCount} is rendered per pipeline.
 *   <li>Internal {@code __mt_} keys are not leaked into the rendered specifiedInfo section of the
 *       root operator.
 * </ol>
 */
public class MetricTreeRenderTest {

  // ── helpers ──────────────────────────────────────────────────────────────

  /** Creates a fresh MPPQueryContext each time — never reuses a shared static singleton. */
  private static MPPQueryContext freshContext() {
    SessionInfo sessionInfo = new SessionInfo(0, "root", ZoneId.systemDefault());
    MPPQueryContext ctx =
        new MPPQueryContext(
            "SELECT * FROM test",
            new QueryId("metric_tree_test"),
            sessionInfo,
            new org.apache.iotdb.common.rpc.thrift.TEndPoint("127.0.0.1", 6667),
            new org.apache.iotdb.common.rpc.thrift.TEndPoint("127.0.0.1", 10730));
    ctx.setAnalyzeCost(0);
    return ctx;
  }

  private static TFetchFragmentInstanceStatisticsResp buildFiStats(
      long readyQueueNs, long blockQueueNs, Map<String, TOperatorStatistics> opMap) {
    TFetchFragmentInstanceStatisticsResp stats = new TFetchFragmentInstanceStatisticsResp();
    stats.setDataRegion("root.db.region1");
    stats.setIp("127.0.0.1");
    stats.setState("FINISHED");
    stats.setStartTimeInMS(1000L);
    stats.setEndTimeInMS(2000L);
    stats.setInitDataQuerySourceCost(0L);
    stats.setSeqUnclosedNum(0);
    stats.setSeqClosednNum(0);
    stats.setUnseqUnclosedNum(0);
    stats.setUnseqClosedNum(0);
    stats.setReadyQueuedTime(readyQueueNs);
    stats.setBlockQueuedTime(blockQueueNs);
    TQueryStatistics qs = new TQueryStatistics();
    stats.setQueryStatistics(qs);
    stats.setOperatorStatisticsMap(new HashMap<>(opMap));
    return stats;
  }

  private static FragmentInstance buildInstance(PlanNodeId rootNodeId) {
    LimitNode planNode = new LimitNode(rootNodeId, null, 10, Optional.empty());
    PlanFragment fragment = new PlanFragment(new PlanFragmentId("q", 1), planNode);
    FragmentInstanceId fiId = new FragmentInstanceId(new PlanFragmentId("q", 1), "inst-0");
    SessionInfo sessionInfo = new SessionInfo(0, "root", ZoneId.systemDefault());
    return new FragmentInstance(fragment, fiId, null, null, 60000L, sessionInfo, false, false);
  }

  /** Renders and returns the flat list of line values. */
  private static List<String> render(
      FragmentInstance instance,
      Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStats) {
    FragmentInstanceStatisticsDrawer drawer = new FragmentInstanceStatisticsDrawer();
    MPPQueryContext ctx = freshContext();
    drawer.renderPlanStatistics(ctx);
    drawer.renderDispatchCost(ctx);
    return drawer
        .renderFragmentInstances(Collections.singletonList(instance), allStats, false)
        .stream()
        .map(StatisticLine::getValue)
        .collect(Collectors.toList());
  }

  /** Renders through the JSON drawer and returns the parsed root object. */
  private static com.google.gson.JsonObject renderJson(
      FragmentInstance instance,
      Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStats) {
    FragmentInstanceStatisticsJsonDrawer drawer = new FragmentInstanceStatisticsJsonDrawer();
    MPPQueryContext ctx = freshContext();
    drawer.renderPlanStatistics(ctx);
    drawer.renderDispatchCost(ctx);
    String json =
        drawer.renderFragmentInstancesAsJson(Collections.singletonList(instance), allStats, false);
    return com.google.gson.JsonParser.parseString(json).getAsJsonObject();
  }

  /** Returns the first fragment instance object from a JSON rendering. */
  private static com.google.gson.JsonObject firstFi(com.google.gson.JsonObject root) {
    return root.getAsJsonArray("fragmentInstances").get(0).getAsJsonObject();
  }

  // ── tests ─────────────────────────────────────────────────────────────────

  /**
   * When blockQueuedTime > 0, the FI-level output should contain a "blocking ratio" line. The ratio
   * should equal blockTime / (readyTime + blockTime) × 100 %.
   */
  @Test
  public void testFiLevelBlockingRatio() {
    PlanNodeId rootId = new PlanNodeId("root1");
    FragmentInstance instance = buildInstance(rootId);
    FragmentInstanceId fiId = instance.getId();

    // 300 ns ready, 700 ns blocked → 70 % blocking ratio
    Map<String, TOperatorStatistics> opMap = new HashMap<>();
    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStats = new HashMap<>();
    allStats.put(fiId, buildFiStats(300L, 700L, opMap));

    List<String> lines = render(instance, allStats);

    boolean hasBlockingRatio =
        lines.stream().anyMatch(l -> l.contains("blocking ratio:") && l.contains("70.0%"));
    assertTrue("Expected a 'blocking ratio: 70.0%' line in output", hasBlockingRatio);
  }

  /** When blockQueuedTime is zero, no blocking ratio line should appear. */
  @Test
  public void testNoBlockingRatioWhenZeroBlockTime() {
    PlanNodeId rootId = new PlanNodeId("root2");
    FragmentInstance instance = buildInstance(rootId);
    FragmentInstanceId fiId = instance.getId();

    Map<String, TOperatorStatistics> opMap = new HashMap<>();
    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStats = new HashMap<>();
    allStats.put(fiId, buildFiStats(1000L, 0L, opMap));

    List<String> lines = render(instance, allStats);

    assertFalse(
        "Should not render blocking ratio when block time is zero",
        lines.stream().anyMatch(l -> l.contains("blocking ratio:")));
  }

  /**
   * Operator-level throughput (rows/s) must appear when both CPU time and output rows are non-zero,
   * and the value should be numerically correct within a small tolerance.
   */
  @Test
  public void testOperatorThroughput() {
    PlanNodeId rootId = new PlanNodeId("root3");
    FragmentInstance instance = buildInstance(rootId);
    FragmentInstanceId fiId = instance.getId();

    // 1_000_000 ns = 1 ms CPU time; 500 output rows → 500_000 rows/s
    TOperatorStatistics opStats = new TOperatorStatistics();
    opStats.setOperatorType("LimitOperator");
    opStats.setTotalExecutionTimeInNanos(1_000_000L);
    opStats.setOutputRows(500);
    opStats.hasNextCalledCount = 10;
    opStats.nextCalledCount = 5;
    opStats.setSpecifiedInfo(new HashMap<>());

    Map<String, TOperatorStatistics> opMap = new HashMap<>();
    opMap.put("root3", opStats);

    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStats = new HashMap<>();
    allStats.put(fiId, buildFiStats(100L, 0L, opMap));

    List<String> lines = render(instance, allStats);

    // Look for a throughput line that contains "500000.0 rows/s"
    boolean hasThroughput =
        lines.stream().anyMatch(l -> l.contains("throughput:") && l.contains("rows/s"));
    assertTrue("Expected a throughput line for the root operator", hasThroughput);

    String throughputLine =
        lines.stream()
            .filter(l -> l.contains("throughput:") && l.contains("rows/s"))
            .findFirst()
            .orElse("");
    // The value should be ~500000.0 rows/s
    assertTrue(
        "Throughput should be ~500000.0 rows/s, got: " + throughputLine,
        throughputLine.contains("500000.0"));
  }

  /**
   * Parallel sub-scan entries stored under {@code "__pipeline_<nodeId>-parallel-<i>"} keys must be
   * expanded individually in the output. This is the multi-driver aggregation correctness check.
   *
   * <p>With 3 parallel drivers each producing 100 rows, the output should show 3 individual
   * pipeline entries and the per-pipeline row counts must be visible.
   */
  @Test
  public void testParallelSubScanPipelinesRendered() {
    PlanNodeId rootId = new PlanNodeId("scan1");
    FragmentInstance instance = buildInstance(rootId);
    FragmentInstanceId fiId = instance.getId();

    Map<String, TOperatorStatistics> opMap = new HashMap<>();

    // Merged root entry (what mergeOperatorStatisticsIfDuplicate would leave behind)
    TOperatorStatistics merged = new TOperatorStatistics();
    merged.setOperatorType("TableScanOperator");
    merged.setTotalExecutionTimeInNanos(3_000_000L); // sum of 3 drivers
    merged.setOutputRows(300); // sum of 3 drivers
    merged.hasNextCalledCount = 30;
    merged.nextCalledCount = 15;
    merged.setSpecifiedInfo(new HashMap<>());
    opMap.put("scan1", merged);

    // 3 parallel pipeline entries (stashed before merge)
    for (int i = 0; i < 3; i++) {
      TOperatorStatistics sub = new TOperatorStatistics();
      sub.setOperatorType("TableScanOperator");
      sub.setTotalExecutionTimeInNanos(1_000_000L);
      sub.setOutputRows(100);
      sub.hasNextCalledCount = 10;
      sub.nextCalledCount = 5;
      Map<String, String> specInfo = new HashMap<>();
      specInfo.put("DEVICE_NUMBER", "2");
      specInfo.put("__mt_tsBlockOutputCount", "5");
      sub.setSpecifiedInfo(specInfo);
      opMap.put("__pipeline_scan1_parallel_" + i, sub);
    }

    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStats = new HashMap<>();
    allStats.put(fiId, buildFiStats(500L, 100L, opMap));

    List<String> lines = render(instance, allStats);

    // The "Parallel Pipelines" section header must appear
    boolean hasHeader = lines.stream().anyMatch(l -> l.contains("Parallel Pipelines"));
    assertTrue("Expected 'Parallel Pipelines' header in output", hasHeader);

    // All three parallel entries should be listed
    long parallelEntryCount = lines.stream().filter(l -> l.contains("-parallel-")).count();
    assertEquals("Expected 3 parallel pipeline entries in output", 3L, parallelEntryCount);

    // Each entry shows output rows
    long rowLines = lines.stream().filter(l -> l.contains("output: 100 rows")).count();
    assertTrue("Expected per-pipeline row count lines", rowLines >= 3);

    // TsBlock count should appear per pipeline
    boolean hasTsBlockCount = lines.stream().anyMatch(l -> l.contains("TsBlock output count: 5"));
    assertTrue("Expected TsBlock output count per pipeline", hasTsBlockCount);
  }

  /**
   * Aggregated output rows across N parallel drivers must equal the sum of per-driver rows.
   * Verifies that the merged root operator entry shows the correct sum.
   */
  @Test
  public void testParallelDriverAggregatedRowsCorrect() {
    PlanNodeId rootId = new PlanNodeId("scan2");
    FragmentInstance instance = buildInstance(rootId);
    FragmentInstanceId fiId = instance.getId();

    Map<String, TOperatorStatistics> opMap = new HashMap<>();

    // 4 parallel drivers: 50 + 75 + 60 + 65 = 250 rows total
    int[] perDriverRows = {50, 75, 60, 65};
    long totalRows = 0;
    for (int i = 0; i < perDriverRows.length; i++) {
      TOperatorStatistics sub = new TOperatorStatistics();
      sub.setOperatorType("TableScanOperator");
      sub.setTotalExecutionTimeInNanos(1_000_000L);
      sub.setOutputRows(perDriverRows[i]);
      sub.hasNextCalledCount = 5;
      sub.nextCalledCount = 3;
      sub.setSpecifiedInfo(new HashMap<>());
      opMap.put("__pipeline_scan2_parallel_" + i, sub);
      totalRows += perDriverRows[i];
    }

    // Merged entry reflecting the aggregated totals
    TOperatorStatistics merged = new TOperatorStatistics();
    merged.setOperatorType("TableScanOperator");
    merged.setTotalExecutionTimeInNanos(4_000_000L);
    merged.setOutputRows(totalRows);
    merged.hasNextCalledCount = 20;
    merged.nextCalledCount = 12;
    merged.setSpecifiedInfo(new HashMap<>());
    opMap.put("scan2", merged);

    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStats = new HashMap<>();
    allStats.put(fiId, buildFiStats(200L, 50L, opMap));

    List<String> lines = render(instance, allStats);

    // Merged root entry must show the aggregate (250 rows)
    boolean hasMergedRows = lines.stream().anyMatch(l -> l.contains("output: 250 rows"));
    assertTrue("Merged operator entry should show 250 aggregated rows", hasMergedRows);

    // Each per-driver row count must also be visible
    long driver0 = lines.stream().filter(l -> l.contains("output: 50 rows")).count();
    assertTrue("Driver 0 row count should appear", driver0 >= 1);
    long driver1 = lines.stream().filter(l -> l.contains("output: 75 rows")).count();
    assertTrue("Driver 1 row count should appear", driver1 >= 1);

    // Blocking ratio: 50 / (200 + 50) = 20 %
    boolean hasRatio =
        lines.stream().anyMatch(l -> l.contains("blocking ratio:") && l.contains("20.0%"));
    assertTrue("Blocking ratio should be 20.0%", hasRatio);
  }

  /**
   * The synthetic {@code __mt_}-prefixed keys used to carry metric-tree values inside specifiedInfo
   * must be indistinguishable from ordinary keys in the output: the TsBlock count appears as a
   * labelled line, the raw internal key is suppressed, and operator-authored entries are still
   * rendered verbatim.
   */
  @Test
  public void testInternalMtKeysRenderedAsLabelledLines() {
    PlanNodeId rootId = new PlanNodeId("root5");
    FragmentInstance instance = buildInstance(rootId);
    FragmentInstanceId fiId = instance.getId();

    TOperatorStatistics opStats = new TOperatorStatistics();
    opStats.setOperatorType("LimitOperator");
    opStats.setTotalExecutionTimeInNanos(500_000L);
    opStats.setOutputRows(50);
    opStats.hasNextCalledCount = 5;
    opStats.nextCalledCount = 3;
    Map<String, String> specInfo = new HashMap<>();
    specInfo.put("__mt_tsBlockOutputCount", "10");
    specInfo.put("visibleKey", "visibleValue");
    opStats.setSpecifiedInfo(specInfo);

    Map<String, TOperatorStatistics> opMap = new HashMap<>();
    opMap.put("root5", opStats);

    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStats = new HashMap<>();
    allStats.put(fiId, buildFiStats(100L, 0L, opMap));

    List<String> lines = render(instance, allStats);

    // The TsBlock count is surfaced as a human-readable line...
    assertTrue(
        "TsBlock count should be rendered as a labelled line",
        lines.stream().anyMatch(l -> l.contains("TsBlock output count: 10")));
    // ...the raw internal key is not dumped verbatim...
    assertFalse(
        "Raw __mt_ key should not be dumped into the output",
        lines.stream().anyMatch(l -> l.contains("__mt_")));
    // ...and operator-authored entries are unaffected.
    assertTrue(
        "Operator-authored specifiedInfo key should still be rendered",
        lines.stream().anyMatch(l -> l.contains("visibleKey") && l.contains("visibleValue")));

    // No parallel pipelines => no pipeline section.
    assertFalse(
        "No 'Parallel Pipelines' header should appear without parallel pipelines",
        lines.stream().anyMatch(l -> l.contains("Parallel Pipelines")));
  }

  // ── JSON drawer mirror ────────────────────────────────────────────────────

  /**
   * The JSON drawer must mirror the text drawer: blocking ratio, per-operator throughput and the
   * parallel pipeline breakdown all appear with the same values.
   */
  @Test
  public void testJsonMirrorsMetricTreeFields() {
    PlanNodeId rootId = new PlanNodeId("scanJ");
    FragmentInstance instance = buildInstance(rootId);
    FragmentInstanceId fiId = instance.getId();

    Map<String, TOperatorStatistics> opMap = new HashMap<>();

    // Root entry: 2 ms CPU, 400 rows => 200000 rows/s
    TOperatorStatistics merged = new TOperatorStatistics();
    merged.setOperatorType("TableScanOperator");
    merged.setTotalExecutionTimeInNanos(2_000_000L);
    merged.setOutputRows(400);
    merged.hasNextCalledCount = 20;
    merged.nextCalledCount = 10;
    Map<String, String> mergedSpec = new HashMap<>();
    mergedSpec.put("__mt_tsBlockOutputCount", "8");
    merged.setSpecifiedInfo(mergedSpec);
    opMap.put("scanJ", merged);

    // Two parallel drivers
    for (int i = 0; i < 2; i++) {
      TOperatorStatistics sub = new TOperatorStatistics();
      sub.setOperatorType("TableScanOperator");
      sub.setTotalExecutionTimeInNanos(1_000_000L);
      sub.setOutputRows(200);
      sub.hasNextCalledCount = 10;
      sub.nextCalledCount = 5;
      Map<String, String> spec = new HashMap<>();
      spec.put("__mt_tsBlockOutputCount", "4");
      spec.put("DEVICE_NUMBER", "3");
      sub.setSpecifiedInfo(spec);
      opMap.put("__pipeline_scanJ_parallel_" + i, sub);
    }

    // 150 ns ready, 450 ns blocked => 75 % blocking ratio
    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStats = new HashMap<>();
    allStats.put(fiId, buildFiStats(150L, 450L, opMap));

    com.google.gson.JsonObject root = renderJson(instance, allStats);
    com.google.gson.JsonObject fi = firstFi(root);

    // FI-level blocking ratio
    assertEquals(75.0, fi.get("blockingRatioPercent").getAsDouble(), 0.01);

    // Per-operator throughput and promoted TsBlock count
    com.google.gson.JsonObject operators = fi.getAsJsonObject("operators");
    assertEquals(200000.0, operators.get("throughputRowsPerSec").getAsDouble(), 0.1);
    assertEquals(8, operators.get("tsBlockOutputCount").getAsInt());

    // Parallel pipeline breakdown
    assertTrue("parallelPipelines array should be present", fi.has("parallelPipelines"));
    com.google.gson.JsonArray pipelines = fi.getAsJsonArray("parallelPipelines");
    assertEquals(2, pipelines.size());
    for (int i = 0; i < 2; i++) {
      com.google.gson.JsonObject p = pipelines.get(i).getAsJsonObject();
      assertEquals("scanJ-parallel-" + i, p.get("planNodeId").getAsString());
      assertEquals(200, p.get("outputRows").getAsInt());
      assertEquals(4, p.get("tsBlockOutputCount").getAsInt());
      assertEquals(200000.0, p.get("throughputRowsPerSec").getAsDouble(), 0.1);
      // Operator-authored specifiedInfo survives; the synthetic key does not.
      assertEquals("3", p.getAsJsonObject("specifiedInfo").get("DEVICE_NUMBER").getAsString());
      assertFalse(
          "Raw __mt_ key must not be leaked into JSON specifiedInfo",
          p.getAsJsonObject("specifiedInfo").has("__mt_tsBlockOutputCount"));
    }
  }

  /** JSON blocking ratio is omitted when nothing blocked, matching the text drawer. */
  @Test
  public void testJsonOmitsBlockingRatioWhenZeroBlockTime() {
    PlanNodeId rootId = new PlanNodeId("rootJ2");
    FragmentInstance instance = buildInstance(rootId);
    FragmentInstanceId fiId = instance.getId();

    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStats = new HashMap<>();
    allStats.put(fiId, buildFiStats(1000L, 0L, new HashMap<>()));

    com.google.gson.JsonObject fi = firstFi(renderJson(instance, allStats));
    assertFalse(
        "blockingRatioPercent should be absent when block time is zero",
        fi.has("blockingRatioPercent"));
    // No parallel pipelines => no array.
    assertFalse(
        "parallelPipelines should be absent without parallel pipelines",
        fi.has("parallelPipelines"));
  }

  /**
   * Merging appends specifiedInfo values with a space for sink and shuffle operators, so the
   * injected TsBlock count can arrive as "10 5". Both drawers must report the sum (15) rather than
   * echoing the raw string or throwing.
   */
  @Test
  public void testMergedTsBlockCountIsSummed() {
    PlanNodeId rootId = new PlanNodeId("root6");
    FragmentInstance instance = buildInstance(rootId);
    FragmentInstanceId fiId = instance.getId();

    TOperatorStatistics opStats = new TOperatorStatistics();
    opStats.setOperatorType("ExchangeOperator");
    opStats.setTotalExecutionTimeInNanos(1_000_000L);
    opStats.setOutputRows(100);
    opStats.hasNextCalledCount = 10;
    opStats.nextCalledCount = 5;
    Map<String, String> specInfo = new HashMap<>();
    specInfo.put("__mt_tsBlockOutputCount", "10 5");
    opStats.setSpecifiedInfo(specInfo);

    Map<String, TOperatorStatistics> opMap = new HashMap<>();
    opMap.put("root6", opStats);

    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStats = new HashMap<>();
    allStats.put(fiId, buildFiStats(100L, 0L, opMap));

    // Text drawer reports the sum.
    List<String> lines = render(instance, allStats);
    assertTrue(
        "Merged TsBlock counts should be summed to 15",
        lines.stream().anyMatch(l -> l.contains("TsBlock output count: 15")));

    // JSON drawer reports the same sum, as a number.
    com.google.gson.JsonObject operators =
        firstFi(renderJson(instance, allStats)).getAsJsonObject("operators");
    assertEquals(15, operators.get("tsBlockOutputCount").getAsInt());
  }

  /** A non-numeric TsBlock value must not break rendering; the line is simply omitted. */
  @Test
  public void testMalformedTsBlockCountIsIgnored() {
    PlanNodeId rootId = new PlanNodeId("root7");
    FragmentInstance instance = buildInstance(rootId);
    FragmentInstanceId fiId = instance.getId();

    TOperatorStatistics opStats = new TOperatorStatistics();
    opStats.setOperatorType("LimitOperator");
    opStats.setTotalExecutionTimeInNanos(1_000_000L);
    opStats.setOutputRows(10);
    opStats.hasNextCalledCount = 2;
    opStats.nextCalledCount = 1;
    Map<String, String> specInfo = new HashMap<>();
    specInfo.put("__mt_tsBlockOutputCount", "not-a-number");
    opStats.setSpecifiedInfo(specInfo);

    Map<String, TOperatorStatistics> opMap = new HashMap<>();
    opMap.put("root7", opStats);

    Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStats = new HashMap<>();
    allStats.put(fiId, buildFiStats(100L, 0L, opMap));

    List<String> lines = render(instance, allStats);
    assertFalse(
        "Malformed TsBlock count should be omitted, not rendered",
        lines.stream().anyMatch(l -> l.contains("TsBlock output count:")));

    // The JSON drawer must not throw on the same input.
    com.google.gson.JsonObject operators =
        firstFi(renderJson(instance, allStats)).getAsJsonObject("operators");
    assertFalse(
        "Malformed TsBlock count should be omitted from JSON", operators.has("tsBlockOutputCount"));
  }
}
