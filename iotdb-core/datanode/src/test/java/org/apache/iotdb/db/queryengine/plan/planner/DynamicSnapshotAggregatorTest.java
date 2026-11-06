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

package org.apache.iotdb.db.queryengine.plan.planner;

import org.apache.iotdb.db.queryengine.statistics.snapshot.*;
import org.apache.iotdb.mpp.rpc.thrift.*;

import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class DynamicSnapshotAggregatorTest {
  static DynamicSourcePlan source(
      String query, String source, int devices, int dop, boolean dynamic) {
    List<String> domain = new ArrayList<>();
    for (int i = 0; i < devices; i++) {
      domain.add("d" + i);
    }
    ScanParallelPlanner.Input input =
        new ScanParallelPlanner.Input(
            domain, List.of(), null, dop, true, false, false, false, false, false, query, "0.i",
            source);
    DynamicScanGrouping grouping = new DynamicScanGroupingBuilder().build(input);
    ScanParallelPlanner planner = new ScanParallelPlanner();
    ScanParallelPlan plan =
        dynamic ? planner.planWithGrouping(input, grouping, dop) : planner.plan(input);
    DynamicQueryShape shape =
        new DynamicQueryShape(
            "db",
            "table",
            "region0",
            source,
            domain,
            "all",
            "all",
            Map.of("s", "INT64"),
            "none",
            ScanParallelPlan.Mode.DEVICE);
    return new DynamicSourcePlan(
        shape, plan, dynamic && plan.getMode() != ScanParallelPlan.Mode.SERIAL ? grouping : null);
  }

  static DynamicSnapshotRecord record(
      String query,
      TFetchFragmentInstanceStatisticsResp response,
      List<DynamicSourcePlan> sources) {
    DynamicParallelTransportCompatibilityTest.signed(response);
    return DynamicSnapshotRecord.validate(
        DynamicSnapshotRecord.capture(
            query, "0.i", DynamicSnapshotHistoryStoreTest.ENDPOINT, response, sources));
  }

  @Test
  public void coarseToFineProjectionConservesMeasuredTimeAndMarksEstimates() {
    String q = "q0";
    DynamicSourcePlan source = source(q, "s", 12, 4, false);
    TFetchFragmentInstanceStatisticsResp r =
        DynamicSnapshotHistoryStoreTest.bind(
            q, DynamicSnapshotHistoryStoreTest.response(q), List.of(source));
    long[] times = {9000, 6000, 1500, 3000};
    r.getDriverSnapshots().get(0).setDriverWallNanos(999999);
    r.getDriverSnapshots().get(0).setDriverOutputRows(99999);
    for (int i = 0; i < 4; i++) {
      r.getDriverSnapshots().get(i + 1).setDriverWallNanos(times[i]);
      r.getDriverSnapshots().get(i + 1).setDriverOutputRows(0);
    }
    DynamicSnapshotAggregator aggregator = new DynamicSnapshotAggregator();
    DynamicSnapshotAggregator.Result aggregate =
        aggregator.aggregate(record(q, r, List.of(source)));
    assertTrue(aggregate.isValid());
    DynamicScanGrouping target =
        new DynamicScanGroupingBuilder()
            .build(
                new ScanParallelPlanner.Input(
                    source.getShape().getDevices(),
                    List.of(),
                    4,
                    true,
                    false,
                    false,
                    false,
                    false,
                    false));
    DynamicSnapshotAggregator.Projection projection =
        aggregator.project(aggregate, source, target).orElseThrow();
    assertEquals(
        19500, projection.getGrouping().getCosts().stream().mapToLong(Long::longValue).sum());
    assertEquals(
        List.of(3000L, 3000L, 3000L, 2000L, 2000L, 2000L, 500L, 500L, 500L, 1000L, 1000L, 1000L),
        projection.getGrouping().getCosts());
    for (DynamicCostSource origin : projection.getGrouping().getCostSources()) {
      assertEquals(DynamicCostSource.BUCKET_AVERAGE, origin);
    }
  }

  @Test
  public void oneDriverWithTwoSerialSourcesIsChargedOnlyOnce() {
    String q = "q0";
    DynamicSourcePlan a = source(q, "a", 1, 1, true), b = source(q, "b", 3, 1, true);
    TFetchFragmentInstanceStatisticsResp r =
        DynamicSnapshotHistoryStoreTest.bind(
            q, DynamicSnapshotHistoryStoreTest.response(q), List.of(a, b));
    TDriverRuntimeSnapshot fused = r.getDriverSnapshots().get(0);
    fused.setDriverWallNanos(100);
    fused.setScanAssignments(
        List.of(
            new TDriverBucketAssignment(
                "a", 0, List.of(a.getPlan().getWorkUnits().get(0).getSourceId())),
            new TDriverBucketAssignment(
                "b", 0, List.of(b.getPlan().getWorkUnits().get(0).getSourceId()))));
    r.setDriverSnapshots(List.of(fused));
    r.setActualPipelineCount(1);
    DynamicSnapshotAggregator.Result aggregate =
        new DynamicSnapshotAggregator().aggregate(record(q, r, List.of(a, b)));
    assertTrue(aggregate.isValid());
    assertEquals(25, aggregate.getSources().get("a").get(0).getCostNanos());
    assertEquals(75, aggregate.getSources().get("b").get(0).getCostNanos());
    assertEquals(
        DynamicCostSource.BUCKET_AVERAGE, aggregate.getSources().get("a").get(0).getCostSource());
  }

  @Test
  public void duplicateOwnersAndAlienWorkIdsAreRejectedAfterSelfChecksum() {
    String q = "q0";
    DynamicSourcePlan source = source(q, "s", 4, 4, true);
    TFetchFragmentInstanceStatisticsResp r = DynamicSnapshotHistoryStoreTest.response(q);
    TDriverRuntimeSnapshot extra = r.getDriverSnapshots().get(1).deepCopy();
    extra.setDriverPipelineId("extra");
    extra.setDriverTaskId("extra-task");
    r.addToDriverSnapshots(extra);
    r.setActualPipelineCount(r.getDriverSnapshotsSize());
    assertEquals(
        DynamicSnapshotAggregator.Reason.DUPLICATE_OWNERSHIP,
        new DynamicSnapshotAggregator().aggregate(record(q, r, List.of(source))).getReason());
    r = DynamicSnapshotHistoryStoreTest.response(q);
    r.getDriverSnapshots().get(1).getScanAssignments().get(0).setWorkUnitIds(List.of("alien"));
    assertEquals(
        DynamicSnapshotAggregator.Reason.WORK_COVERAGE_MISMATCH,
        new DynamicSnapshotAggregator().aggregate(record(q, r, List.of(source))).getReason());
  }

  @Test
  public void missingWallCannotBeReplacedWithOutputRows() {
    String q = "q0";
    DynamicSourcePlan source = source(q, "s", 4, 4, true);
    TFetchFragmentInstanceStatisticsResp r = DynamicSnapshotHistoryStoreTest.response(q);
    r.getDriverSnapshots().get(1).unsetDriverWallNanos();
    r.getDriverSnapshots().get(1).setDriverOutputRows(0);
    assertEquals(
        DynamicSnapshotAggregator.Reason.MISSING_WALL_TIME,
        new DynamicSnapshotAggregator().aggregate(record(q, r, List.of(source))).getReason());
  }

  @Test
  public void remainderAllocationIsConservedAndDeterministic() {
    String q = "q0";
    DynamicSourcePlan source = source(q, "s", 12, 4, false);
    TFetchFragmentInstanceStatisticsResp r =
        DynamicSnapshotHistoryStoreTest.bind(
            q, DynamicSnapshotHistoryStoreTest.response(q), List.of(source));
    for (int i = 1; i < r.getDriverSnapshotsSize(); i++) {
      r.getDriverSnapshots().get(i).setDriverWallNanos(10);
    }
    DynamicSnapshotAggregator aggregator = new DynamicSnapshotAggregator();
    DynamicSnapshotAggregator.Result result = aggregator.aggregate(record(q, r, List.of(source)));
    DynamicScanGrouping target =
        new DynamicScanGroupingBuilder()
            .build(
                new ScanParallelPlanner.Input(
                    source.getShape().getDevices(),
                    List.of(),
                    4,
                    true,
                    false,
                    false,
                    false,
                    false,
                    false));
    DynamicSnapshotAggregator.Projection projection =
        aggregator.project(result, source, target).orElseThrow();
    assertEquals(
        List.of(4L, 3L, 3L, 4L, 3L, 3L, 4L, 3L, 3L, 4L, 3L, 3L),
        projection.getGrouping().getCosts());
    assertEquals(40, projection.getGrouping().getCosts().stream().mapToLong(Long::longValue).sum());
  }

  @Test
  public void emptyBucketsRetainCoverageAndDoNotFabricateWallTime() {
    String q = "q0";
    DynamicSourcePlan base = source(q, "s", 12, 4, true);
    DynamicScanGrouping zero =
        base.getGrouping()
            .withCosts(
                Collections.nCopies(12, 0L),
                Collections.nCopies(12, DynamicCostSource.BUCKET_AVERAGE));
    ScanParallelPlanner.Input input =
        new ScanParallelPlanner.Input(
            base.getShape().getDevices(),
            List.of(),
            null,
            4,
            true,
            false,
            false,
            false,
            false,
            false,
            q,
            "0.i",
            "s");
    DynamicSourcePlan source =
        new DynamicSourcePlan(
            base.getShape(), new ScanParallelPlanner().planWithGrouping(input, zero, 4), zero);
    TFetchFragmentInstanceStatisticsResp r =
        DynamicSnapshotHistoryStoreTest.bind(
            q, DynamicSnapshotHistoryStoreTest.response(q), List.of(source));
    for (TDriverRuntimeSnapshot driver : r.getDriverSnapshots()) {
      if (driver.isSetScanAssignments()
          && driver.getScanAssignments().get(0).getWorkUnitIds().isEmpty()) {
        driver.unsetDriverWallNanos();
      } else {
        driver.setDriverWallNanos(100);
      }
    }
    DynamicSnapshotAggregator aggregator = new DynamicSnapshotAggregator();
    DynamicSnapshotAggregator.Result result = aggregator.aggregate(record(q, r, List.of(source)));
    assertTrue(result.isValid());
    assertEquals(4, result.getSources().get("s").size());
    assertEquals(
        100,
        result.getSources().get("s").stream().mapToLong(DynamicBucketRuntime::getCostNanos).sum());
  }

  @Test
  public void projectedTargetMustPreserveTheFullDomain() {
    String q = "q0";
    DynamicSourcePlan source = source(q, "s", 4, 4, true);
    DynamicSnapshotAggregator aggregator = new DynamicSnapshotAggregator();
    DynamicSnapshotAggregator.Result result =
        aggregator.aggregate(
            record(q, DynamicSnapshotHistoryStoreTest.response(q), List.of(source)));
    DynamicScanGrouping original = source.getGrouping();
    List<List<String>> bad = new ArrayList<>(original.getDeviceGroups());
    bad.set(1, bad.get(0));
    DynamicScanGrouping duplicate =
        new DynamicScanGrouping(
            original.getMode(),
            bad,
            original.getTimeGroups(),
            original.getCosts(),
            original.getCostSources());
    assertTrue(aggregator.project(result, source, duplicate).isEmpty());
  }

  @Test
  public void readThroughViewProjectsOnlyTwoRecentCompleteRecords() {
    AtomicLong clock = new AtomicLong();
    DynamicSnapshotHistoryStore store =
        new DynamicSnapshotHistoryStore(DynamicSnapshotHistoryStore.Limits.defaults(), clock::get);
    for (int i = 0; i < 3; i++) {
      String q = "q" + i;
      store.publish(DynamicSnapshotHistoryStoreTest.record(q, "all"));
    }
    DynamicSourcePlan source = source("q3", "s", 4, 4, true);
    DynamicCostHistoryStore.Estimate estimate =
        new DynamicCostHistoryStore(store)
            .estimate(
                source.getShape(), DynamicSnapshotHistoryStoreTest.ENDPOINT, source.getGrouping());
    assertEquals(Integer.valueOf(4), estimate.getPreviousDop());
    assertEquals(2, estimate.getObservations().size());
    assertEquals(2, estimate.getObservations().get(0).getSequence());
    assertEquals(3, estimate.getObservations().get(1).getSequence());
    assertEquals(3, store.sampleCount());
  }

  @Test
  public void previousWidthComesFromLatestQualifiedObservation() {
    DynamicSnapshotHistoryStore store = new DynamicSnapshotHistoryStore();
    DynamicSourcePlan prior = source("prior_four", "s", 4, 4, true);
    DynamicSourcePlan newest = source("latest_two", "s", 4, 2, true);
    for (DynamicSourcePlan source : List.of(prior, newest)) {
      String q = source.getPlan().getWorkUnits().get(0).getSourceKey().getQueryId();
      TFetchFragmentInstanceStatisticsResp response =
          DynamicSnapshotHistoryStoreTest.bind(
              q, DynamicSnapshotHistoryStoreTest.response(q), List.of(source));
      assertTrue(store.publish(record(q, response, List.of(source))));
    }
    DynamicCostHistoryStore.Estimate estimate =
        new DynamicCostHistoryStore(store)
            .estimate(
                prior.getShape(), DynamicSnapshotHistoryStoreTest.ENDPOINT, prior.getGrouping());
    assertEquals(Integer.valueOf(2), estimate.getPreviousDop());
    assertEquals(2, estimate.getObservations().size());
  }

  @Test
  public void incompleteNewestObservationCannotReplaceQualifiedPreviousWidth() {
    DynamicSnapshotHistoryStore store = new DynamicSnapshotHistoryStore();
    DynamicSourcePlan prior = source("prior_qualified", "s", 4, 4, true);
    assertNull(
        new DynamicCostHistoryStore(store)
            .estimate(
                prior.getShape(), DynamicSnapshotHistoryStoreTest.ENDPOINT, prior.getGrouping())
            .getPreviousDop());
    TFetchFragmentInstanceStatisticsResp response =
        DynamicSnapshotHistoryStoreTest.bind(
            "prior_qualified",
            DynamicSnapshotHistoryStoreTest.response("prior_qualified"),
            List.of(prior));
    assertTrue(store.publish(record("prior_qualified", response, List.of(prior))));
    DynamicSourcePlan incomplete = source("newest_incomplete", "s", 4, 2, true);
    response =
        DynamicSnapshotHistoryStoreTest.bind(
            "newest_incomplete",
            DynamicSnapshotHistoryStoreTest.response("newest_incomplete"),
            List.of(incomplete));
    response.getDriverSnapshots().get(1).unsetDriverWallNanos();
    assertTrue(store.publish(record("newest_incomplete", response, List.of(incomplete))));
    DynamicCostHistoryStore.Estimate estimate =
        new DynamicCostHistoryStore(store)
            .estimate(
                prior.getShape(), DynamicSnapshotHistoryStoreTest.ENDPOINT, prior.getGrouping());
    assertEquals(Integer.valueOf(4), estimate.getPreviousDop());
    assertEquals(1, estimate.getObservations().size());
    assertEquals(1, estimate.getRejected().size());
  }
}
