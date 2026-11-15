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

package org.apache.iotdb.db.queryengine.execution.runtime;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class RuntimeDopExecutionSnapshotTest {
  @Test
  public void snapshotDefensivelyCopiesGroupsCountsEventsAndJsonTree() {
    Map<RuntimeDopExecutionSnapshot.Outcome, Long> counts =
        new EnumMap<>(RuntimeDopExecutionSnapshot.Outcome.class);
    counts.put(RuntimeDopExecutionSnapshot.Outcome.INITIAL_ACTIVATED, 1L);
    counts.put(RuntimeDopExecutionSnapshot.Outcome.QUOTA_DENIED, 1L);
    List<RuntimeDopExecutionSnapshot.ControlEvent> events = new ArrayList<>();
    events.add(
        new RuntimeDopExecutionSnapshot.ControlEvent(
            1, 4, 2, RuntimeDopExecutionSnapshot.Outcome.QUOTA_DENIED));
    RuntimeDopExecutionSnapshot.GroupSnapshot group =
        new RuntimeDopExecutionSnapshot.GroupSnapshot("g", 4, 2, 2, 2, 2, 2, counts, events);
    Map<String, RuntimeDopExecutionSnapshot.GroupSnapshot> groups = new LinkedHashMap<>();
    groups.put("g", group);
    RuntimeDopExecutionSnapshot snapshot =
        new RuntimeDopExecutionSnapshot("q", "q.0.i", 123, "FINISHED", groups);
    counts.clear();
    events.clear();
    groups.clear();
    assertEquals(1, snapshot.getGroups().size());
    assertEquals(1, group.events().size());
    assertEquals(
        1L,
        group.outcomeCounts().get(RuntimeDopExecutionSnapshot.Outcome.QUOTA_DENIED).longValue());
    assertThrows(UnsupportedOperationException.class, () -> snapshot.getGroups().clear());
    assertThrows(UnsupportedOperationException.class, () -> group.events().clear());
    assertThrows(UnsupportedOperationException.class, () -> group.outcomeCounts().clear());
    assertThrows(UnsupportedOperationException.class, () -> snapshot.toMap().clear());
    Map<?, ?> jsonGroups = (Map<?, ?>) snapshot.toMap().get("groups");
    assertThrows(UnsupportedOperationException.class, jsonGroups::clear);
    Map<?, ?> jsonGroup = (Map<?, ?>) jsonGroups.get("g");
    assertThrows(UnsupportedOperationException.class, jsonGroup::clear);
    List<?> jsonEvents = (List<?>) jsonGroup.get("events");
    assertThrows(UnsupportedOperationException.class, jsonEvents::clear);
    Map<?, ?> jsonEvent = (Map<?, ?>) jsonEvents.get(0);
    assertThrows(UnsupportedOperationException.class, jsonEvent::clear);
  }

  @Test
  public void jsonContainsActualActivationAndOutcomeRatherThanRequestedDop() {
    RuntimeDopExecutionSnapshot.GroupSnapshot group =
        new RuntimeDopExecutionSnapshot.GroupSnapshot(
            "scan",
            4,
            2,
            2,
            2,
            2,
            2,
            Map.of(RuntimeDopExecutionSnapshot.Outcome.QUOTA_DENIED, 3L),
            List.of(
                new RuntimeDopExecutionSnapshot.ControlEvent(
                    3, 4, 2, RuntimeDopExecutionSnapshot.Outcome.QUOTA_DENIED)));
    RuntimeDopExecutionSnapshot snapshot =
        new RuntimeDopExecutionSnapshot("q", "q.0.i", 456, "FINISHED", Map.of("scan", group));
    JsonObject json = JsonParser.parseString(snapshot.toJson()).getAsJsonObject();
    assertEquals(1, json.get("schemaVersion").getAsInt());
    assertEquals(456L, json.get("originalDeadlineMillis").getAsLong());
    JsonObject scan = json.getAsJsonObject("groups").getAsJsonObject("scan");
    assertEquals(2, scan.get("initialActivatedWorkers").getAsInt());
    assertEquals(2, scan.get("maxAppliedWorkers").getAsInt());
    assertEquals(0L, scan.getAsJsonObject("outcomeCounts").get("APPLIED").getAsLong());
    assertEquals(3L, scan.getAsJsonObject("outcomeCounts").get("QUOTA_DENIED").getAsLong());
    assertEquals(
        4,
        scan.getAsJsonArray("events").get(0).getAsJsonObject().get("requestedWorkers").getAsInt());
    assertEquals(
        2,
        scan.getAsJsonArray("events").get(0).getAsJsonObject().get("observedWorkers").getAsInt());
    assertEquals(snapshot.toJson(), snapshot.toJson());
  }

  @Test
  public void exactLaneBucketWorkIdentitySurvivesImmutableJsonWithoutDescriptorReferences() {
    List<String> units = new ArrayList<>(List.of("canonical-work-unit"));
    Map<Integer, RuntimeDopExecutionSnapshot.LaneIdentity> lanes = new LinkedHashMap<>();
    lanes.put(0, new RuntimeDopExecutionSnapshot.LaneIdentity(0, 7, 0, units));
    RuntimeDopExecutionSnapshot.GroupSnapshot group =
        new RuntimeDopExecutionSnapshot.GroupSnapshot(
            "g",
            "scan-source",
            "lane-plan-sha",
            "source-set-sha",
            lanes,
            1,
            1,
            1,
            1,
            1,
            1,
            Map.of(RuntimeDopExecutionSnapshot.Outcome.INITIAL_ACTIVATED, 1L),
            List.of());
    units.clear();
    lanes.clear();
    assertTrue(group.layoutIdentitySupported());
    assertEquals(List.of("canonical-work-unit"), group.laneIdentities().get(0).workUnitIds());
    assertThrows(UnsupportedOperationException.class, () -> group.laneIdentities().clear());
    assertThrows(
        UnsupportedOperationException.class,
        () -> group.laneIdentities().get(0).workUnitIds().clear());
    RuntimeDopExecutionSnapshot snapshot =
        new RuntimeDopExecutionSnapshot("q", "q.0.i", 123, "FINISHED", Map.of("g", group));
    JsonObject jsonGroup =
        JsonParser.parseString(snapshot.toJson())
            .getAsJsonObject()
            .getAsJsonObject("groups")
            .getAsJsonObject("g");
    assertTrue(jsonGroup.get("layoutIdentitySupported").getAsBoolean());
    assertEquals("scan-source", jsonGroup.get("sourcePlanNodeId").getAsString());
    assertEquals("lane-plan-sha", jsonGroup.get("lanePlanChecksum").getAsString());
    assertEquals("source-set-sha", jsonGroup.get("lanePlanIdentityChecksum").getAsString());
    JsonObject lane = jsonGroup.getAsJsonObject("laneIdentities").getAsJsonObject("0");
    assertEquals(7, lane.get("originalPipelineId").getAsInt());
    assertEquals(0, lane.get("bucketId").getAsInt());
    assertEquals("canonical-work-unit", lane.getAsJsonArray("workUnitIds").get(0).getAsString());
  }

  @Test
  public void oversizedOrPhysicallyImpossibleEvidenceIsRejected() {
    List<RuntimeDopExecutionSnapshot.ControlEvent> events = new ArrayList<>();
    for (int i = 0; i <= RuntimeDopExecutionSnapshot.EVENT_LIMIT; i++) {
      events.add(
          new RuntimeDopExecutionSnapshot.ControlEvent(
              i, 2, 2, RuntimeDopExecutionSnapshot.Outcome.APPLIED));
    }
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RuntimeDopExecutionSnapshot.GroupSnapshot("g", 4, 2, 2, 2, 2, 2, Map.of(), events));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RuntimeDopExecutionSnapshot.GroupSnapshot(
                "g", 2, 2, 3, 3, 2, 3, Map.of(), List.of()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RuntimeDopExecutionSnapshot.GroupSnapshot(
                "g",
                2,
                2,
                2,
                2,
                2,
                2,
                Map.of(RuntimeDopExecutionSnapshot.Outcome.APPLIED, -1L),
                List.of()));
  }

  @Test
  public void controllerDiagnosticsPreserveCoverageWithoutInventingProgressOrAppliedEvents() {
    RuntimeDopExecutionSnapshot.GroupSnapshot group =
        new RuntimeDopExecutionSnapshot.GroupSnapshot("g", 4, 1, 1, 1, 1, 1, Map.of(), List.of());
    List<RuntimeDopController.Event> events =
        List.of(
            new RuntimeDopController.Event(
                "g",
                100,
                7,
                1,
                1,
                RuntimeDopPolicy.Action.KEEP,
                RuntimeDopPolicy.Reason.COMPLETED_WORK_UNSUPPORTED,
                RuntimeDopPolicy.MetricValue.unsupported(),
                RuntimeDopPolicy.MetricValue.unknown()),
            new RuntimeDopController.Event(
                "g",
                200,
                8,
                1,
                2,
                RuntimeDopPolicy.Action.GROW,
                RuntimeDopPolicy.Reason.READY_BACKLOG,
                RuntimeDopPolicy.MetricValue.valid(13),
                RuntimeDopPolicy.MetricValue.valid(100)));
    RuntimeDopExecutionSnapshot snapshot =
        new RuntimeDopExecutionSnapshot("q", "q.0.i", 123, "FINISHED", Map.of("g", group), events);
    JsonObject json = JsonParser.parseString(snapshot.toJson()).getAsJsonObject();
    JsonObject diagnostics = json.getAsJsonObject("controllerDiagnostics");
    assertEquals("RECORDED", diagnostics.get("status").getAsString());
    assertEquals(128, diagnostics.get("capacity").getAsInt());
    assertEquals(2, diagnostics.get("retainedCount").getAsInt());
    assertEquals(7, diagnostics.get("firstRetainedSequence").getAsLong());
    assertEquals(8, diagnostics.get("lastRetainedSequence").getAsLong());
    JsonObject first = diagnostics.getAsJsonArray("events").get(0).getAsJsonObject();
    assertEquals("KEEP", first.get("action").getAsString());
    assertEquals("COMPLETED_WORK_UNSUPPORTED", first.get("reason").getAsString());
    assertEquals(
        "UNSUPPORTED", first.getAsJsonObject("completedWork").get("coverage").getAsString());
    assertEquals("UNKNOWN", first.getAsJsonObject("totalWork").get("coverage").getAsString());
    assertFalse(first.getAsJsonObject("completedWork").has("value"));
    assertFalse(first.getAsJsonObject("totalWork").has("value"));
    JsonObject second = diagnostics.getAsJsonArray("events").get(1).getAsJsonObject();
    assertEquals(13, second.getAsJsonObject("completedWork").get("value").getAsLong());
    assertEquals(100, second.getAsJsonObject("totalWork").get("value").getAsLong());
    JsonObject actual = json.getAsJsonObject("groups").getAsJsonObject("g");
    assertEquals(0, actual.getAsJsonObject("outcomeCounts").get("APPLIED").getAsLong());
    assertEquals(0, actual.getAsJsonArray("events").size());
    assertEquals(1, actual.get("maxAppliedWorkers").getAsInt());
  }

  @Test
  public void controllerDiagnosticTreeIsDetachedImmutableAndStrictlyBounded() {
    List<RuntimeDopController.Event> events = new ArrayList<>();
    for (int i = 0; i < RuntimeDopExecutionSnapshot.EVENT_LIMIT; i++) {
      events.add(
          new RuntimeDopController.Event(
              "g",
              i,
              i + 1,
              1,
              1,
              RuntimeDopPolicy.Action.KEEP,
              RuntimeDopPolicy.Reason.NO_PROGRESS,
              RuntimeDopPolicy.MetricValue.valid(0),
              RuntimeDopPolicy.MetricValue.valid(100)));
    }
    RuntimeDopExecutionSnapshot snapshot =
        new RuntimeDopExecutionSnapshot("q", "q.0.i", 123, "FINISHED", Map.of(), events);
    events.add(events.get(0));
    assertThrows(
        IllegalArgumentException.class,
        () -> new RuntimeDopExecutionSnapshot("q", "q.0.i", 123, "FINISHED", Map.of(), events));
    events.clear();
    Map<?, ?> diagnostics = (Map<?, ?>) snapshot.toMap().get("controllerDiagnostics");
    assertEquals(128, diagnostics.get("retainedCount"));
    assertThrows(UnsupportedOperationException.class, diagnostics::clear);
    List<?> retained = (List<?>) diagnostics.get("events");
    assertEquals(128, retained.size());
    assertThrows(UnsupportedOperationException.class, retained::clear);
    Map<?, ?> first = (Map<?, ?>) retained.get(0);
    assertThrows(UnsupportedOperationException.class, first::clear);
    Map<?, ?> metric = (Map<?, ?>) first.get("completedWork");
    assertEquals(0L, metric.get("value"));
    assertThrows(UnsupportedOperationException.class, metric::clear);
    assertEquals(snapshot.toJson(), snapshot.toJson());
  }

  @Test
  public void missingControllerAndCreatedWithoutSamplesRemainDistinct() {
    RuntimeDopExecutionSnapshot original =
        new RuntimeDopExecutionSnapshot("q", "q.0.i", 123, "FINISHED", Map.of());
    RuntimeDopExecutionSnapshot absent =
        new RuntimeDopExecutionSnapshot("q", "q.0.i", 123, "FINISHED", Map.of(), null);
    assertEquals(original.toJson(), absent.toJson());
    assertFalse(original.toMap().containsKey("controllerDiagnostics"));
    RuntimeDopExecutionSnapshot created =
        new RuntimeDopExecutionSnapshot("q", "q.0.i", 123, "FINISHED", Map.of(), List.of());
    JsonObject diagnostics =
        JsonParser.parseString(created.toJson())
            .getAsJsonObject()
            .getAsJsonObject("controllerDiagnostics");
    assertEquals("CREATED_BUT_NO_SAMPLE", diagnostics.get("status").getAsString());
    assertEquals(0, diagnostics.get("retainedCount").getAsInt());
    assertEquals(0, diagnostics.getAsJsonArray("events").size());
    assertFalse(diagnostics.has("firstRetainedSequence"));
    assertFalse(diagnostics.has("lastRetainedSequence"));
  }
}
