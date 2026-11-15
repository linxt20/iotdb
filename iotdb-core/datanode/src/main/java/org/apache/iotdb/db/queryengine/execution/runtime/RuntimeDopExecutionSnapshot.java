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

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Immutable bounded evidence of actual native admissions and settled worker-membership changes. */
public final class RuntimeDopExecutionSnapshot {
  public static final int EVENT_LIMIT = 128;

  public enum Outcome {
    INITIAL_ACTIVATED,
    APPLIED,
    QUOTA_DENIED,
    MEMORY_DENIED,
    STOPPED,
    BUSY,
    FAILED
  }

  public record ControlEvent(
      long sequence, int requestedWorkers, int observedWorkers, Outcome outcome) {
    public ControlEvent {
      Objects.requireNonNull(outcome);
      if (sequence < 0 || requestedWorkers < 0 || observedWorkers < 0) {
        throw new IllegalArgumentException();
      }
    }
  }

  /** The original logical lane and bucket identities never change when workers are resized. */
  public record LaneIdentity(
      int laneIndex, int originalPipelineId, int bucketId, List<String> workUnitIds) {
    public LaneIdentity {
      if (laneIndex < 0 || originalPipelineId < 0 || bucketId < 0) {
        throw new IllegalArgumentException();
      }
      workUnitIds = List.copyOf(workUnitIds);
    }
  }

  public record GroupSnapshot(
      String groupId,
      String sourcePlanNodeId,
      String lanePlanChecksum,
      String lanePlanIdentityChecksum,
      Map<Integer, LaneIdentity> laneIdentities,
      int logicalLaneCount,
      int initialRequestedWorkers,
      int initialActivatedWorkers,
      int lastObservedWorkers,
      int minAppliedWorkers,
      int maxAppliedWorkers,
      Map<Outcome, Long> outcomeCounts,
      List<ControlEvent> events) {
    public GroupSnapshot {
      Objects.requireNonNull(groupId);
      Objects.requireNonNull(sourcePlanNodeId);
      Objects.requireNonNull(lanePlanChecksum);
      Objects.requireNonNull(lanePlanIdentityChecksum);
      Map<Integer, LaneIdentity> lanes = new LinkedHashMap<>(laneIdentities);
      for (Map.Entry<Integer, LaneIdentity> lane : lanes.entrySet()) {
        if (lane.getKey() != lane.getValue().laneIndex()
            || lane.getKey() < 0
            || lane.getKey() >= logicalLaneCount) {
          throw new IllegalArgumentException();
        }
      }
      laneIdentities = Collections.unmodifiableMap(lanes);
      if (logicalLaneCount < 1
          || initialRequestedWorkers < 0
          || initialActivatedWorkers < 0
          || lastObservedWorkers < 0
          || minAppliedWorkers < 0
          || maxAppliedWorkers < minAppliedWorkers
          || initialRequestedWorkers > logicalLaneCount
          || initialActivatedWorkers > logicalLaneCount
          || lastObservedWorkers > logicalLaneCount
          || maxAppliedWorkers > logicalLaneCount
          || events.size() > EVENT_LIMIT) {
        throw new IllegalArgumentException();
      }
      EnumMap<Outcome, Long> counts = new EnumMap<>(Outcome.class);
      for (Outcome outcome : Outcome.values()) {
        long count = outcomeCounts.getOrDefault(outcome, 0L);
        if (count < 0) throw new IllegalArgumentException();
        counts.put(outcome, count);
      }
      outcomeCounts = Collections.unmodifiableMap(counts);
      events = List.copyOf(events);
    }

    /** Fixtures without a real descriptor explicitly carry unsupported layout identity. */
    public GroupSnapshot(
        String groupId,
        int logicalLaneCount,
        int initialRequestedWorkers,
        int initialActivatedWorkers,
        int lastObservedWorkers,
        int minAppliedWorkers,
        int maxAppliedWorkers,
        Map<Outcome, Long> outcomeCounts,
        List<ControlEvent> events) {
      this(
          groupId,
          "",
          "",
          "",
          Map.of(),
          logicalLaneCount,
          initialRequestedWorkers,
          initialActivatedWorkers,
          lastObservedWorkers,
          minAppliedWorkers,
          maxAppliedWorkers,
          outcomeCounts,
          events);
    }

    public boolean layoutIdentitySupported() {
      return !sourcePlanNodeId.isEmpty()
          && !lanePlanChecksum.isEmpty()
          && !lanePlanIdentityChecksum.isEmpty()
          && laneIdentities.size() == logicalLaneCount;
    }
  }

  private static final Gson JSON = new Gson();
  private final String queryId;
  private final String fragmentInstanceId;
  private final long originalDeadlineMillis;
  private final String fragmentState;
  private final Map<String, GroupSnapshot> groups;
  private final Map<String, Object> jsonValues;

  RuntimeDopExecutionSnapshot(
      String queryId,
      String fragmentInstanceId,
      long originalDeadlineMillis,
      String fragmentState,
      Map<String, GroupSnapshot> groups) {
    this(queryId, fragmentInstanceId, originalDeadlineMillis, fragmentState, groups, null);
  }

  RuntimeDopExecutionSnapshot(
      String queryId,
      String fragmentInstanceId,
      long originalDeadlineMillis,
      String fragmentState,
      Map<String, GroupSnapshot> groups,
      List<RuntimeDopController.Event> controllerEvents) {
    this.queryId = Objects.requireNonNull(queryId);
    this.fragmentInstanceId = Objects.requireNonNull(fragmentInstanceId);
    this.originalDeadlineMillis = originalDeadlineMillis;
    this.fragmentState = Objects.requireNonNull(fragmentState);
    this.groups = Collections.unmodifiableMap(new LinkedHashMap<>(groups));
    Map<String, Object> values = new LinkedHashMap<>();
    values.put("schemaVersion", 1);
    values.put("queryId", queryId);
    values.put("fragmentInstanceId", fragmentInstanceId);
    values.put("originalDeadlineMillis", originalDeadlineMillis);
    values.put("fragmentState", fragmentState);
    Map<String, Object> groupValues = new LinkedHashMap<>();
    this.groups.forEach(
        (id, group) -> {
          Map<String, Object> value = new LinkedHashMap<>();
          value.put("groupId", group.groupId());
          value.put("sourcePlanNodeId", group.sourcePlanNodeId());
          value.put("lanePlanChecksum", group.lanePlanChecksum());
          value.put("lanePlanIdentityChecksum", group.lanePlanIdentityChecksum());
          value.put("layoutIdentitySupported", group.layoutIdentitySupported());
          Map<String, Object> lanes = new LinkedHashMap<>();
          group
              .laneIdentities()
              .forEach(
                  (laneIndex, lane) -> {
                    Map<String, Object> identity = new LinkedHashMap<>();
                    identity.put("laneIndex", lane.laneIndex());
                    identity.put("originalPipelineId", lane.originalPipelineId());
                    identity.put("bucketId", lane.bucketId());
                    identity.put("workUnitIds", lane.workUnitIds());
                    lanes.put(Integer.toString(laneIndex), Collections.unmodifiableMap(identity));
                  });
          value.put("laneIdentities", Collections.unmodifiableMap(lanes));
          value.put("logicalLaneCount", group.logicalLaneCount());
          value.put("initialRequestedWorkers", group.initialRequestedWorkers());
          value.put("initialActivatedWorkers", group.initialActivatedWorkers());
          value.put("lastObservedWorkers", group.lastObservedWorkers());
          value.put("minAppliedWorkers", group.minAppliedWorkers());
          value.put("maxAppliedWorkers", group.maxAppliedWorkers());
          Map<String, Object> counts = new LinkedHashMap<>();
          group.outcomeCounts().forEach((outcome, count) -> counts.put(outcome.name(), count));
          value.put("outcomeCounts", Collections.unmodifiableMap(counts));
          List<Map<String, Object>> events = new ArrayList<>();
          for (ControlEvent event : group.events()) {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("sequence", event.sequence());
            entry.put("requestedWorkers", event.requestedWorkers());
            entry.put("observedWorkers", event.observedWorkers());
            entry.put("outcome", event.outcome().name());
            events.add(Collections.unmodifiableMap(entry));
          }
          value.put("events", Collections.unmodifiableList(events));
          groupValues.put(id, Collections.unmodifiableMap(value));
        });
    values.put("groups", Collections.unmodifiableMap(groupValues));
    // Absence means diagnostics were not captured. Do not allocate diagnostics on the
    // default-off/no-controller path, or substitute policy intent for settled actuation.
    if (controllerEvents != null) {
      if (controllerEvents.size() > EVENT_LIMIT) throw new IllegalArgumentException();
      List<RuntimeDopController.Event> retained = List.copyOf(controllerEvents);
      Map<String, Object> diagnostics = new LinkedHashMap<>();
      diagnostics.put("status", retained.isEmpty() ? "CREATED_BUT_NO_SAMPLE" : "RECORDED");
      diagnostics.put("capacity", EVENT_LIMIT);
      diagnostics.put("retainedCount", retained.size());
      if (!retained.isEmpty()) {
        diagnostics.put("firstRetainedSequence", retained.get(0).sampleSequence());
        diagnostics.put("lastRetainedSequence", retained.get(retained.size() - 1).sampleSequence());
      }
      List<Map<String, Object>> policyEvents = new ArrayList<>();
      for (RuntimeDopController.Event event : retained) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("groupId", event.groupId());
        entry.put("sampleNanos", event.sampleNanos());
        entry.put("sampleSequence", event.sampleSequence());
        entry.put("observedWorkers", event.observedWorkers());
        entry.put("requestedWorkers", event.requestedWorkers());
        entry.put("action", event.action().name());
        entry.put("reason", event.reason().name());
        entry.put("completedWork", metricValues(event.completedWork()));
        entry.put("totalWork", metricValues(event.totalWork()));
        policyEvents.add(Collections.unmodifiableMap(entry));
      }
      diagnostics.put("events", Collections.unmodifiableList(policyEvents));
      values.put("controllerDiagnostics", Collections.unmodifiableMap(diagnostics));
    }
    jsonValues = Collections.unmodifiableMap(values);
  }

  private static Map<String, Object> metricValues(RuntimeDopPolicy.MetricValue metric) {
    Map<String, Object> values = new LinkedHashMap<>();
    values.put("coverage", metric.coverage().name());
    // UNKNOWN/UNSUPPORTED carry no value; absence must never become a measured zero.
    if (metric.value() != null) values.put("value", metric.value());
    return Collections.unmodifiableMap(values);
  }

  public String getQueryId() {
    return queryId;
  }

  public String getFragmentInstanceId() {
    return fragmentInstanceId;
  }

  public long getOriginalDeadlineMillis() {
    return originalDeadlineMillis;
  }

  public String getFragmentState() {
    return fragmentState;
  }

  public Map<String, GroupSnapshot> getGroups() {
    return groups;
  }

  public Map<String, Object> toMap() {
    return jsonValues;
  }

  public String toJson() {
    return JSON.toJson(jsonValues);
  }
}
