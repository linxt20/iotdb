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

package org.apache.iotdb.db.queryengine.statistics.snapshot;

import org.apache.iotdb.db.queryengine.plan.planner.DynamicParallelPolicy;
import org.apache.iotdb.db.queryengine.plan.planner.DynamicParallelScheduleInput;
import org.apache.iotdb.db.queryengine.plan.planner.DynamicParallelScheduler;
import org.apache.iotdb.db.queryengine.plan.planner.DynamicQueryShape;
import org.apache.iotdb.db.queryengine.plan.planner.DynamicSourcePlan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/** One bounded store owns samples, coverage and policy state. No second unbounded cache exists. */
public final class DynamicSnapshotHistoryStore {
  public static final class Limits {
    final int perKey, samples, keys;
    final long bytes, ttl;

    public Limits(int perKey, int samples, int keys, long bytes, long ttl) {
      if (perKey < 1 || samples < 1 || keys < 1 || bytes < 1 || ttl < 1) {
        throw new IllegalArgumentException(
            org.apache.iotdb.db.i18n.DataNodeQueryMessages
                .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
      }
      this.perKey = perKey;
      this.samples = samples;
      this.keys = keys;
      this.bytes = bytes;
      this.ttl = ttl;
    }

    public static Limits defaults() {
      return new Limits(1024, 4096, 256, 32L * 1024 * 1024, 30L * 60 * 1_000_000_000);
    }
  }

  public static final class Entry {
    private final DynamicSnapshotRecord record;
    private final long sequence, receivedAt;
    private final Set<String> keys;

    private Entry(DynamicSnapshotRecord record, long sequence, long receivedAt, Set<String> keys) {
      this.record = record;
      this.sequence = sequence;
      this.receivedAt = receivedAt;
      this.keys = keys;
    }

    public DynamicSnapshotRecord getRecord() {
      return record;
    }

    public long getSequence() {
      return sequence;
    }

    public long getReceivedAt() {
      return receivedAt;
    }
  }

  private static final class Slot {
    final LinkedHashSet<String> ids = new LinkedHashSet<>();
    DynamicParallelPolicy.State state = DynamicParallelPolicy.State.empty();
    long touched;
    AdmissionRejection rejection;
    long admissionSequence;

    Slot(long now) {
      touched = now;
    }
  }

  private final Limits limits;
  private final LongSupplier clock;
  private final LinkedHashMap<String, Entry> records = new LinkedHashMap<>();
  private final LinkedHashMap<String, Slot> slots = new LinkedHashMap<>();
  private long bytes, sequence, planningSequence, rejectionBytes;
  private static final long REJECTION_HEADER_BYTES = 1024;

  /** One immutable trusted refusal per bounded slot, including the plan needed for replay. */
  public static final class AdmissionRejection {
    private final int previousDop;
    private final long receivedAt;
    private final long chargedBytes;
    private final org.apache.iotdb.db.queryengine.plan.planner.DynamicDecisionReceipt receipt;
    private final String runtimeIdentity, planChecksum, groupingChecksum;

    private AdmissionRejection(
        org.apache.iotdb.db.queryengine.plan.planner.DynamicDecisionReceipt receipt,
        long now,
        long chargedBytes) {
      this.receipt = receipt;
      this.chargedBytes = chargedBytes;
      previousDop = receipt.getTargetDop();
      receivedAt = now;
      runtimeIdentity =
          org.apache.iotdb.db.queryengine.plan.planner.StaticParallelPlanChecksum.sha256(
              receipt.getQueryId().length()
                  + ":"
                  + receipt.getQueryId()
                  + receipt.getFragmentId().length()
                  + ":"
                  + receipt.getFragmentId());
      planChecksum = receipt.getSource().getPlanIdentityChecksum();
      groupingChecksum = receipt.getSource().getGroupingChecksum();
    }

    public org.apache.iotdb.db.queryengine.plan.planner.DynamicDecisionReceipt getReceipt() {
      return receipt;
    }

    public long getChargedBytes() {
      return chargedBytes;
    }

    public int getPreviousDop() {
      return previousDop;
    }

    public String getRuntimeIdentity() {
      return runtimeIdentity;
    }

    public String getPlanChecksum() {
      return planChecksum;
    }

    public String getGroupingChecksum() {
      return groupingChecksum;
    }
  }

  public synchronized AdmissionRejection admissionRejection(
      DynamicQueryShape shape, String endpoint) {
    expire(clock.getAsLong());
    Slot slot = slots.get(key(shape, endpoint));
    return slot == null ? null : slot.rejection;
  }

  public synchronized boolean publishAdmission(
      org.apache.iotdb.db.queryengine.plan.planner.DynamicDecisionReceipt receipt) {
    long now = clock.getAsLong();
    expire(now);
    if (receipt == null
        || receipt.getCheckSequence() <= 0
        || !receipt.getSource().verifies()
        || (!"ADMISSION_REJECTED".equals(receipt.getAdmissionStatus())
            && !"ADMISSION_ACCEPTED".equals(receipt.getAdmissionStatus()))) return false;
    long charge = 0;
    if ("ADMISSION_REJECTED".equals(receipt.getAdmissionStatus())) {
      try {
        charge =
            Math.addExact(
                REJECTION_HEADER_BYTES,
                Math.addExact(
                    receipt.estimatedRetainedBytes(),
                    receipt.getSource().estimatedRetainedBytes()));
      } catch (ArithmeticException e) {
        return false;
      }
      // Invalid oversized publications cannot evict valid history or create an empty slot.
      if (charge > Math.min(limits.bytes, DynamicSnapshotRecord.MAX_RECORD_BYTES)) return false;
    }
    String key = key(receipt.getSource().getShape(), receipt.getEndpoint());
    Slot slot = slots.get(key);
    if (slot != null && receipt.getCheckSequence() <= slot.admissionSequence) return false;
    if (slot == null) {
      while (slots.size() >= limits.keys) evictSlotFor(Collections.singleton(key));
      slot = new Slot(now);
      slots.put(key, slot);
    }
    long previousCharge = slot.rejection == null ? 0 : slot.rejection.chargedBytes;
    long additionalCharge = charge - previousCharge;
    if (additionalCharge > 0) {
      while (!records.isEmpty() && bytes + rejectionBytes > limits.bytes - additionalCharge)
        remove(records.keySet().iterator().next());
      while (bytes + rejectionBytes > limits.bytes - additionalCharge)
        evictSlotFor(Collections.singleton(key));
    }
    rejectionBytes += additionalCharge;
    slot.rejection =
        "ADMISSION_REJECTED".equals(receipt.getAdmissionStatus())
            ? new AdmissionRejection(receipt, now, charge)
            : null;
    slot.admissionSequence = receipt.getCheckSequence();
    slot.touched = now;
    return true;
  }

  public DynamicSnapshotHistoryStore() {
    this(Limits.defaults(), System::nanoTime);
  }

  public DynamicSnapshotHistoryStore(Limits limits, LongSupplier clock) {
    if (limits == null || clock == null) {
      throw new IllegalArgumentException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    }
    this.limits = limits;
    this.clock = clock;
  }

  public synchronized boolean publish(DynamicSnapshotRecord record) {
    long now = clock.getAsLong();
    expire(now);
    if (record == null || record.getChargedBytes() > limits.bytes) {
      return false;
    }
    Set<String> keys = new LinkedHashSet<>();
    for (DynamicSourcePlan source : record.getSourcePlans().values()) {
      keys.add(key(source.getShape(), record.getEndpoint()));
    }
    if (keys.isEmpty() || keys.size() > limits.keys) {
      return false;
    }
    Entry duplicate = records.get(record.getRuntimeIdentity());
    if (duplicate != null) {
      if (!duplicate.record.getContentChecksum().equals(record.getContentChecksum())) {
        throw new IllegalArgumentException(
            org.apache.iotdb.db.i18n.DataNodeQueryMessages
                .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
      }
      return false;
    }
    // Reject unusable writes before eviction. Keep all of a sample's sources indexed together.
    while (slots.size() + newKeyCount(keys) > limits.keys) {
      evictSlotFor(keys);
    }
    for (String key : keys) {
      Slot slot = slots.computeIfAbsent(key, k -> new Slot(now));
      while (slot.ids.size() >= limits.perKey) {
        remove(slot.ids.iterator().next());
      }
    }
    while (!records.isEmpty()
        && (records.size() >= limits.samples
            || bytes + rejectionBytes > limits.bytes - record.getChargedBytes())) {
      remove(records.keySet().iterator().next());
    }
    if (bytes + rejectionBytes > limits.bytes - record.getChargedBytes()) return false;
    long next = Math.addExact(sequence, 1);
    sequence = next;
    Entry entry = new Entry(record, next, now, Collections.unmodifiableSet(keys));
    records.put(record.getRuntimeIdentity(), entry);
    bytes = Math.addExact(bytes, record.getChargedBytes());
    for (String key : keys) {
      Slot slot = slots.get(key);
      slot.ids.add(record.getRuntimeIdentity());
      slot.touched = now;
    }
    return true;
  }

  public synchronized List<Entry> history(DynamicQueryShape shape, String endpoint) {
    long now = clock.getAsLong();
    expire(now);
    Slot slot = slots.get(key(shape, endpoint));
    if (slot == null) {
      return Collections.emptyList();
    }
    List<Entry> result = new ArrayList<>();
    for (String id : slot.ids) {
      result.add(records.get(id));
    }
    return Collections.unmodifiableList(result);
  }

  /** Scheduler is pure, so this short critical section only replaces one bounded slot's state. */
  public synchronized DynamicParallelScheduler.Result decide(
      DynamicQueryShape shape,
      String endpoint,
      DynamicParallelScheduleInput input,
      DynamicParallelScheduler scheduler) {
    long now = clock.getAsLong();
    expire(now);
    String key = key(shape, endpoint);
    if (!slots.containsKey(key)) {
      while (slots.size() >= limits.keys) {
        evictSlotFor(Collections.singleton(key));
      }
      slots.put(key, new Slot(now));
    }
    Slot slot = slots.get(key);
    DynamicParallelScheduler.Result result =
        scheduler.decide(input, slot.state.withCheckSequence(planningSequence));
    planningSequence = result.getState().getCheckSequence();
    slot.state = result.getState();
    slot.touched = now;
    return result;
  }

  public synchronized int sampleCount() {
    expire(clock.getAsLong());
    return records.size();
  }

  public synchronized int keyCount() {
    expire(clock.getAsLong());
    return slots.size();
  }

  public synchronized long retainedBytes() {
    expire(clock.getAsLong());
    return bytes + rejectionBytes;
  }

  private int newKeyCount(Set<String> keys) {
    int n = 0;
    for (String key : keys) {
      if (!slots.containsKey(key)) {
        n++;
      }
    }
    return n;
  }

  private void evictSlotFor(Set<String> protectedKeys) {
    String victim = null;
    for (Map.Entry<String, Slot> entry : slots.entrySet()) {
      if (!protectedKeys.contains(entry.getKey())) {
        victim = entry.getKey();
        break;
      }
    }
    if (victim == null) {
      throw new IllegalStateException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    }
    Slot slot = slots.get(victim);
    for (String id : new ArrayList<>(slot.ids)) {
      remove(id);
    }
    if (slot.rejection != null) rejectionBytes -= slot.rejection.chargedBytes;
    slots.remove(victim);
  }

  private void remove(String id) {
    Entry entry = records.remove(id);
    if (entry == null) {
      return;
    }
    bytes -= entry.record.getChargedBytes();
    for (String key : entry.keys) {
      Slot slot = slots.get(key);
      if (slot != null) {
        slot.ids.remove(id);
      }
    }
  }

  private void expire(long now) {
    List<String> expired = new ArrayList<>();
    for (Map.Entry<String, Entry> e : records.entrySet()) {
      long age = now - e.getValue().receivedAt;
      if (age < 0 || age >= limits.ttl) {
        expired.add(e.getKey());
      }
    }
    for (String id : expired) {
      remove(id);
    }
    Iterator<Map.Entry<String, Slot>> iterator = slots.entrySet().iterator();
    while (iterator.hasNext()) {
      Slot slot = iterator.next().getValue();
      if (slot.rejection != null) {
        long rejectionAge = now - slot.rejection.receivedAt;
        if (rejectionAge < 0 || rejectionAge >= limits.ttl) {
          rejectionBytes -= slot.rejection.chargedBytes;
          slot.rejection = null;
        }
      }
      long age = now - slot.touched;
      if (slot.ids.isEmpty() && (age < 0 || age >= limits.ttl)) {
        iterator.remove();
      }
    }
  }

  private static String key(DynamicQueryShape shape, String endpoint) {
    if (shape == null || endpoint == null) {
      throw new IllegalArgumentException(
          org.apache.iotdb.db.i18n.DataNodeQueryMessages
              .EXCEPTION_INVALID_DYNAMIC_SNAPSHOT_RECORD_F2BC0753);
    }
    return shape.getChecksum() + "|" + endpoint;
  }
}
