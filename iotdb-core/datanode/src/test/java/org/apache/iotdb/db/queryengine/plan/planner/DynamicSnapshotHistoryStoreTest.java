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

import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.statistics.snapshot.*;
import org.apache.iotdb.mpp.rpc.thrift.*;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class DynamicSnapshotHistoryStoreTest {
  static final String ENDPOINT = "127.0.0.1:10730";

  static DynamicSourcePlan source(String query, String source, String filter) {
    List<String> devices = List.of("d0", "d1", "d2", "d3");
    ScanParallelPlanner.Input input =
        new ScanParallelPlanner.Input(
            devices, List.of(), null, 4, true, false, false, false, false, false, query, "0.i",
            source);
    DynamicScanGrouping grouping = new DynamicScanGroupingBuilder().build(input);
    ScanParallelPlan plan = new ScanParallelPlanner().planWithGrouping(input, grouping, 4);
    DynamicQueryShape shape =
        new DynamicQueryShape(
            "db",
            "table",
            "region0",
            source,
            devices,
            "all",
            filter,
            Map.of("s", "INT64"),
            "none",
            ScanParallelPlan.Mode.DEVICE);
    return new DynamicSourcePlan(shape, plan, grouping);
  }

  static TFetchFragmentInstanceStatisticsResp response(String query) {
    TFetchFragmentInstanceStatisticsResp response =
        DynamicParallelTransportCompatibilityTest.response(2);
    response.getFragmentInstanceId().setQueryId(query);
    response.getDriverSnapshots().get(0).setFragmentInstanceId(query + ".0.i");
    response.getDriverSnapshots().get(0).setDriverTaskId(query + ".0.i.0");
    return bind(query, response, List.of(source(query, "s", "all")));
  }

  static TFetchFragmentInstanceStatisticsResp bind(
      String query,
      TFetchFragmentInstanceStatisticsResp response,
      List<DynamicSourcePlan> sources) {
    FragmentInstanceId id = new FragmentInstanceId(new PlanFragmentId(new QueryId(query), 0), "i");
    FragmentInstanceContext context =
        FragmentInstanceContext.createFragmentInstanceContext(
            id, new FragmentInstanceStateMachine(id, Runnable::run), null, false, false);
    context.setPlanGeneration(1);
    List<TDriverRuntimeSnapshot> drivers = new ArrayList<>();
    TDriverRuntimeSnapshot root = response.getDriverSnapshots().get(0);
    root.setDriverRole("root");
    drivers.add(root);
    int pipeline = 1;
    for (DynamicSourcePlan source : sources) {
      ScanParallelPlan plan = source.getPlan();
      context.setScanParallelMetadata(
          plan.getTargetParallelism(),
          plan.getBuckets().size(),
          plan.getMode().name(),
          plan.getRawSplitReason());
      context.recordScanSourcePlanIdentity(
          source.getSourcePlanNodeId(),
          StaticParallelPlanChecksum.withGeneration(plan.getPlanIdentityChecksum(), 1),
          plan.getWorkUnits().stream()
              .map(ScanParallelPlan.WorkUnit::getSourceId)
              .collect(java.util.stream.Collectors.toList()),
          plan.getBuckets().stream()
              .map(ScanParallelPlan.Bucket::getBucketId)
              .collect(java.util.stream.Collectors.toList()),
          List.of(),
          plan.getCanonicalFallbackReason());
      for (ScanParallelPlan.Bucket bucket : plan.getBuckets()) {
        String pid = Integer.toString(pipeline++);
        TDriverRuntimeSnapshot driver =
            new TDriverRuntimeSnapshot(query + ".0.i", pid, query + ".0.i." + pid, 1);
        driver.setDriverRole("input");
        driver.setDriverWallNanos(10);
        driver.setDriverOutputRows(2);
        driver.setDriverTsBlockCount(1);
        driver.setScanAssignments(
            List.of(
                new TDriverBucketAssignment(
                    source.getSourcePlanNodeId(),
                    bucket.getBucketId(),
                    bucket.getWorkUnits().stream()
                        .map(ScanParallelPlan.WorkUnit::getSourceId)
                        .collect(java.util.stream.Collectors.toList()))));
        drivers.add(driver);
      }
    }
    response.setDriverSnapshots(drivers);
    response.setPlanIdentityChecksum(context.getPlanIdentityChecksum());
    response.setPlanGeneration(1);
    response.setPlannedDop(context.getPlannedDop());
    response.setActualPipelineCount(drivers.size());
    response.setWorkUnitIds(context.getWorkUnitIds());
    response.setBucketIds(context.getBucketIds());
    response.setExchangeEdgeIds(List.of());
    response.setSplitReason("NONE");
    context.finished();
    return DynamicParallelTransportCompatibilityTest.signed(response);
  }

  static DynamicSnapshotRecord record(String query, String filter) {
    return DynamicSnapshotRecord.validate(
        DynamicSnapshotRecord.capture(
            query, "0.i", ENDPOINT, response(query), List.of(source(query, "s", filter))));
  }

  static DynamicSnapshotHistoryStore store(
      AtomicLong clock, int perKey, int samples, int keys, long bytes, long ttl) {
    return new DynamicSnapshotHistoryStore(
        new DynamicSnapshotHistoryStore.Limits(perKey, samples, keys, bytes, ttl), clock::get);
  }

  @Test
  public void duplicatesDoNotExtendHistoryAndConflictsAreRejected() {
    AtomicLong clock = new AtomicLong();
    DynamicSnapshotHistoryStore store = store(clock, 2, 10, 4, 1000000, 100);
    DynamicSnapshotRecord a = record("q0", "all");
    assertTrue(store.publish(a));
    assertFalse(store.publish(a));
    assertEquals(1, store.sampleCount());
    DynamicSnapshotRecord changed = record("q0", "s>1");
    assertThrows(IllegalArgumentException.class, () -> store.publish(changed));
    assertEquals(1, store.sampleCount());
  }

  @Test
  public void perKeyCapEvictsOldestGlobalRecord() {
    AtomicLong clock = new AtomicLong();
    DynamicSnapshotHistoryStore store = store(clock, 2, 10, 4, 1000000, 100);
    for (int i = 0; i < 3; i++) {
      assertTrue(store.publish(record("q" + i, "all")));
    }
    List<DynamicSnapshotHistoryStore.Entry> history =
        store.history(source("q0", "s", "all").getShape(), ENDPOINT);
    assertEquals(2, history.size());
    assertEquals("q1", history.get(0).getRecord().getSnapshot().getQueryId());
    assertEquals("q2", history.get(1).getRecord().getSnapshot().getQueryId());
    assertEquals(2, store.sampleCount());
  }

  @Test
  public void totalSamplesAndKeysAreBoundedIndependently() {
    AtomicLong clock = new AtomicLong();
    DynamicSnapshotHistoryStore samples = store(clock, 10, 2, 10, 1000000, 100);
    for (int i = 0; i < 3; i++) {
      samples.publish(record("q" + i, "all"));
    }
    assertEquals(2, samples.sampleCount());
    DynamicSnapshotHistoryStore keys = store(clock, 10, 10, 2, 1000000, 100);
    for (int i = 0; i < 3; i++) {
      keys.publish(record("q" + i, "s>" + i));
      assertTrue(keys.keyCount() <= 2);
    }
    assertEquals(2, keys.sampleCount());
    assertTrue(keys.history(source("q0", "s", "s>0").getShape(), ENDPOINT).isEmpty());
  }

  @Test
  public void byteCapCountsResponseAndSourceMetadata() {
    AtomicLong clock = new AtomicLong();
    DynamicSnapshotRecord first = record("q0", "all");
    long capacity = 2 * first.getChargedBytes();
    DynamicSnapshotHistoryStore store = store(clock, 10, 10, 10, capacity, 100);
    for (int i = 0; i < 3; i++) {
      store.publish(record("q" + i, "all"));
      assertTrue(store.retainedBytes() <= capacity);
    }
    assertEquals(2, store.sampleCount());
    DynamicSnapshotHistoryStore tiny = store(clock, 10, 10, 10, first.getChargedBytes() - 1, 100);
    assertFalse(tiny.publish(first));
    assertEquals(0, tiny.keyCount());
  }

  @Test
  public void ttlUsesLocalReceptionTimeAndReleasesSlots() {
    AtomicLong clock = new AtomicLong(10);
    DynamicSnapshotHistoryStore store = store(clock, 10, 10, 10, 1000000, 100);
    store.publish(record("q0", "all"));
    clock.set(109);
    assertEquals(1, store.sampleCount());
    clock.set(110);
    assertEquals(0, store.sampleCount());
    assertEquals(0, store.retainedBytes());
    assertEquals(0, store.keyCount());
  }

  @Test
  public void endpointAndRegionHistoryCannotLeak() {
    AtomicLong clock = new AtomicLong();
    DynamicSnapshotHistoryStore store = store(clock, 10, 10, 10, 1000000, 100);
    DynamicSourcePlan source = source("q0", "s", "all");
    store.publish(record("q0", "all"));
    assertTrue(store.history(source.getShape(), "other:1").isEmpty());
    DynamicQueryShape otherRegion =
        new DynamicQueryShape(
            "db",
            "table",
            "region1",
            "s",
            source.getShape().getDevices(),
            "all",
            "all",
            Map.of("s", "INT64"),
            "none",
            ScanParallelPlan.Mode.DEVICE);
    assertTrue(store.history(otherRegion, ENDPOINT).isEmpty());
  }

  @Test
  public void concurrentChecksGetUniqueMonotonicSequences() throws Exception {
    AtomicLong clock = new AtomicLong();
    DynamicSnapshotHistoryStore store = store(clock, 10, 10, 2, 1000000, 100);
    DynamicQueryShape shape = source("q0", "s", "all").getShape();
    ExecutorService executor = Executors.newFixedThreadPool(4);
    try {
      List<Future<Long>> calls = new ArrayList<>();
      for (int i = 0; i < 20; i++) {
        calls.add(
            executor.submit(
                () ->
                    store
                        .decide(
                            shape,
                            ENDPOINT,
                            new DynamicParallelScheduleInput(4, 4, 4, null, false, List.of()),
                            new DynamicParallelScheduler())
                        .getState()
                        .getCheckSequence()));
      }
      java.util.Set<Long> values = new java.util.HashSet<>();
      for (Future<Long> call : calls) {
        assertTrue(values.add(call.get(10, TimeUnit.SECONDS)));
      }
      assertEquals(20, values.size());
      assertTrue(values.contains(20L));
      assertEquals(1, store.keyCount());
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  public void stateEvictionDoesNotReuseEarlierCheckSequence() {
    AtomicLong clock = new AtomicLong();
    DynamicSnapshotHistoryStore store = store(clock, 10, 10, 1, 1000000, 100);
    DynamicParallelScheduleInput input =
        new DynamicParallelScheduleInput(4, 4, 4, null, false, List.of());
    DynamicParallelScheduler scheduler = new DynamicParallelScheduler();
    DynamicQueryShape first = source("q0", "s", "a").getShape(),
        other = source("q0", "s", "b").getShape();
    long a = store.decide(first, ENDPOINT, input, scheduler).getState().getCheckSequence();
    store.decide(other, ENDPOINT, input, scheduler);
    assertTrue(store.decide(first, ENDPOINT, input, scheduler).getState().getCheckSequence() > a);
  }

  @Test
  public void nineNearMaximumPayloadsRetainOnlyEight() {
    AtomicLong clock = new AtomicLong();
    DynamicSnapshotHistoryStore store =
        new DynamicSnapshotHistoryStore(DynamicSnapshotHistoryStore.Limits.defaults(), clock::get);
    String padding = "0".repeat(650000);
    for (int i = 0; i < 9; i++) {
      String query = "q" + i;
      TFetchFragmentInstanceStatisticsResp response = response(query);
      response.setSourceNodeId(padding);
      DynamicParallelTransportCompatibilityTest.signed(response);
      DynamicSnapshotRecord.Payload payload =
          DynamicSnapshotRecord.capture(
              query, "0.i", ENDPOINT, response, List.of(source(query, "s", "all")));
      assertNotNull(payload);
      assertTrue(store.publish(DynamicSnapshotRecord.validate(payload)));
      assertTrue(store.retainedBytes() <= 32L * 1024 * 1024);
    }
    assertEquals(8, store.sampleCount());
  }

  @Test
  public void admissionReceiptsShareTheByteAndKeyBudgetAndExpireByReceptionTime() {
    AtomicLong clock = new AtomicLong();

    DynamicSourcePlan a = source("admission0", "s", "filter0"),
        b = source("admission1", "s", "filter1");
    DynamicParallelScheduleDecision decision =
        new DynamicParallelScheduleDecision(
            DynamicParallelScheduleDecision.Action.KEEP_STATIC,
            4,
            DynamicParallelDecisionReason.NO_NEW_FEEDBACK);
    DynamicDecisionReceipt first =
        new DynamicDecisionReceipt(a, decision, ENDPOINT, 1, 4, 4, 4, false)
            .finish("ADMISSION_REJECTED", "INSUFFICIENT_OPERATOR_MEMORY");
    DynamicDecisionReceipt second =
        new DynamicDecisionReceipt(b, decision, ENDPOINT, 2, 4, 4, 4, false)
            .finish("ADMISSION_REJECTED", "INSUFFICIENT_OPERATOR_MEMORY");
    long charge =
        1024 + first.estimatedRetainedBytes() + first.getSource().estimatedRetainedBytes();
    DynamicSnapshotHistoryStore history = store(clock, 10, 10, 2, charge, 100);
    assertTrue(history.publishAdmission(first));
    assertSame(first, history.admissionRejection(a.getShape(), ENDPOINT).getReceipt());
    assertEquals(charge, history.retainedBytes());
    assertTrue(history.publishAdmission(second));
    assertEquals(charge, history.retainedBytes());
    assertNull(history.admissionRejection(a.getShape(), ENDPOINT));
    assertNotNull(history.admissionRejection(b.getShape(), ENDPOINT));
    assertNull(history.admissionRejection(b.getShape(), "other:1"));
    clock.set(99);
    history.decide(
        b.getShape(),
        ENDPOINT,
        new DynamicParallelScheduleInput(
            4, 4, 4, ScanParallelPlan.FallbackReason.NONE, false, List.of()),
        new DynamicParallelScheduler());
    clock.set(100);
    assertNull(history.admissionRejection(b.getShape(), ENDPOINT));
    assertEquals(0, history.retainedBytes());
  }

  @Test
  public void acceptedReceiptClearsOnlyOlderSameShapeRejectionAndLateRepliesCannotRestoreIt() {
    AtomicLong clock = new AtomicLong();
    DynamicSnapshotHistoryStore history = store(clock, 10, 10, 2, 1000000, 100);
    DynamicSourcePlan source = source("admission2", "s", "filter");
    DynamicParallelScheduleDecision decision =
        new DynamicParallelScheduleDecision(
            DynamicParallelScheduleDecision.Action.KEEP_STATIC,
            4,
            DynamicParallelDecisionReason.NO_NEW_FEEDBACK);
    DynamicDecisionReceipt rejected =
        new DynamicDecisionReceipt(source, decision, ENDPOINT, 1, 4, 4, 4, false)
            .finish("ADMISSION_REJECTED", "INSUFFICIENT_OPERATOR_MEMORY");
    DynamicDecisionReceipt accepted =
        new DynamicDecisionReceipt(source, decision, ENDPOINT, 2, 4, 4, 4, false)
            .finish("ADMISSION_ACCEPTED", null);
    assertTrue(history.publishAdmission(rejected));
    assertTrue(history.publishAdmission(accepted));
    assertFalse(history.publishAdmission(rejected));
    assertNull(history.admissionRejection(source.getShape(), ENDPOINT));
    assertEquals(0, history.retainedBytes());
  }

  @Test
  public void rejectedReceiptReplacementReleasesItsOldChargeAndAcceptedReceiptClearsIt() {
    AtomicLong clock = new AtomicLong();
    DynamicSnapshotHistoryStore history = store(clock, 10, 10, 2, 1000000, 100);
    DynamicSourcePlan source = source("replay0", "s", "filter");
    DynamicParallelScheduleDecision decision =
        new DynamicParallelScheduleDecision(
            DynamicParallelScheduleDecision.Action.REDUCE_DOP,
            4,
            DynamicParallelDecisionReason.ADMISSION_REJECTED);
    DynamicDecisionReceipt first =
        new DynamicDecisionReceipt(source, decision, ENDPOINT, 1, 8, 4, 5, false)
            .finish("ADMISSION_REJECTED", "INSUFFICIENT_OPERATOR_MEMORY");
    DynamicDecisionReceipt second =
        new DynamicDecisionReceipt(source, decision, ENDPOINT, 2, 8, 4, 5, false)
            .finish("ADMISSION_REJECTED", "x".repeat(1000));
    assertTrue(history.publishAdmission(first));
    long firstBytes = history.retainedBytes();
    assertTrue(history.publishAdmission(second));
    assertSame(second, history.admissionRejection(source.getShape(), ENDPOINT).getReceipt());
    assertEquals(
        1024 + second.estimatedRetainedBytes() + source.estimatedRetainedBytes(),
        history.retainedBytes());
    assertTrue(history.retainedBytes() > firstBytes);
    DynamicDecisionReceipt accepted =
        new DynamicDecisionReceipt(source, decision, ENDPOINT, 3, 8, 4, 5, false)
            .finish("ADMISSION_ACCEPTED", null);
    assertTrue(history.publishAdmission(accepted));
    assertEquals(0, history.retainedBytes());
  }

  @Test
  public void oversizedRefusalCannotEvictAnExistingRefusalOrCreateAnEmptySlot() {
    AtomicLong clock = new AtomicLong();
    DynamicSnapshotHistoryStore history = store(clock, 10, 10, 2, 100000, 100);
    DynamicSourcePlan source = source("small", "s", "filter");
    DynamicParallelScheduleDecision decision =
        new DynamicParallelScheduleDecision(
            DynamicParallelScheduleDecision.Action.KEEP_STATIC,
            4,
            DynamicParallelDecisionReason.NO_NEW_FEEDBACK);
    DynamicDecisionReceipt small =
        new DynamicDecisionReceipt(source, decision, ENDPOINT, 1, 4, 4, 4, false)
            .finish("ADMISSION_REJECTED", "memory");
    assertTrue(history.publishAdmission(small));
    long retained = history.retainedBytes();
    DynamicSourcePlan other = source("oversize", "s", "other");
    DynamicDecisionReceipt huge =
        new DynamicDecisionReceipt(other, decision, ENDPOINT, 2, 4, 4, 4, false)
            .finish("ADMISSION_REJECTED", "x".repeat(100000));
    assertFalse(history.publishAdmission(huge));
    assertEquals(retained, history.retainedBytes());
    assertEquals(1, history.keyCount());
    assertSame(small, history.admissionRejection(source.getShape(), ENDPOINT).getReceipt());
  }
}
