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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class DynamicScanGroupingTest {
  static ScanParallelPlanner.Input input(
      int devices, int partitions, int dop, boolean ordered, boolean time) {
    List<String> ds = new ArrayList<>();
    for (int i = 0; i < devices; i++) {
      ds.add("d" + i);
    }
    List<Long> ts = new ArrayList<>();
    for (int i = 0; i < partitions; i++) {
      ts.add((long) i);
    }
    return new ScanParallelPlanner.Input(
        ds, ts, null, dop, true, false, false, ordered, ordered, time, "q", "f", "s");
  }

  @Test
  public void twelveUnitsFourBucketsConsumeEachDeviceOnce() {
    ScanParallelPlanner.Input in = input(12, 0, 4, false, false);
    DynamicScanGrouping g = new DynamicScanGroupingBuilder().build(in);
    ScanParallelPlan p = new ScanParallelPlanner().planWithGrouping(in, g, 4);
    assertEquals(12, p.getWorkUnits().size());
    assertEquals(4, p.getBuckets().size());
    java.util.Set<String> seen = new java.util.HashSet<>();
    for (ScanParallelPlan.Bucket b : p.getBuckets()) {
      for (ScanParallelPlan.WorkUnit u : b.getWorkUnits()) {
        for (String d : u.getDevices()) {
          assertTrue(seen.add(d));
        }
      }
    }
    assertEquals(new java.util.HashSet<>(in.getDevices()), seen);
  }

  @Test
  public void sameDopRebalancesRealUnits() {
    ScanParallelPlanner.Input in = input(12, 0, 4, false, false);
    List<Long> costs = List.of(9L, 9L, 9L, 9L, 6L, 6L, 6L, 6L, 3L, 3L, 3L, 3L);
    DynamicScanGrouping g =
        new DynamicScanGroupingBuilder()
            .build(in)
            .withCosts(costs, Collections.nCopies(12, DynamicCostSource.BUCKET_AVERAGE));
    ScanParallelPlan p = new ScanParallelPlanner().planWithGrouping(in, g, 4);
    for (ScanParallelPlan.Bucket b : p.getBuckets()) {
      assertEquals(18L, b.getEstimatedCost());
      assertEquals(3, b.getWorkUnits().size());
    }
  }

  @Test
  public void thousandUnitsMayShareOneTimeBucket() {
    ScanParallelPlanner.Input in = input(2, 512, 8, false, true);
    ScanParallelPlan p =
        new ScanParallelPlanner()
            .planWithGrouping(in, new DynamicScanGroupingBuilder().build(in), 1);
    assertEquals(ScanParallelPlan.Mode.TIME_PARTITION, p.getMode());
    assertEquals(1024, p.getWorkUnits().size());
    assertEquals(1, p.getBuckets().size());
    assertEquals(1024, p.getBuckets().get(0).getWorkUnits().size());
  }

  @Test
  public void millionsOfPossibleRangesStayBounded() {
    ScanParallelPlanner.Input in = input(100000, 30000, 8, false, true);
    DynamicScanGrouping g = new DynamicScanGroupingBuilder().build(in);
    assertTrue(g.getWorkUnitCount() <= 1024);
    assertTrue(g.getGroupedReferenceCount() <= DynamicScanGrouping.MAX_GROUPED_REFERENCES);
    assertEquals(90, g.getWorkUnitCount());
    assertEquals(8, new ScanParallelPlanner().planWithGrouping(in, g, 8).getBuckets().size());
    assertEquals(8, new ScanParallelPlanner().plan(in).getWorkUnits().size());
  }

  @Test
  public void invalidCoverageFallsBackToOneRealUnrestrictedUnit() {
    ScanParallelPlanner.Input in = input(12, 0, 4, false, false);
    DynamicScanGrouping g = new DynamicScanGroupingBuilder().build(in);
    List<List<String>> ds = new ArrayList<>(g.getDeviceGroups());
    ds.set(1, ds.get(0));
    g =
        new DynamicScanGrouping(
            g.getMode(), ds, g.getTimeGroups(), g.getCosts(), g.getCostSources());
    ScanParallelPlan p = new ScanParallelPlanner().planWithGrouping(in, g, 4);
    assertEquals(ScanParallelPlan.Mode.SERIAL, p.getMode());
    assertEquals(1, p.getWorkUnits().size());
    assertEquals(in.getDevices(), p.getWorkUnits().get(0).getDevices());
    assertTrue(p.getWorkUnits().get(0).getTimePartitions().isEmpty());
    assertEquals(
        ScanParallelPlan.FallbackReason.DYNAMIC_GROUPING_COVERAGE_MISMATCH, p.getFallbackReason());
    assertEquals("PLAN_ERROR", p.getCanonicalFallbackReason());
  }

  @Test
  public void inputCapIsNeverReplacedByPolicyTarget() {
    ScanParallelPlanner.Input in = input(12, 0, 2, false, false);
    assertEquals(
        2,
        new ScanParallelPlanner()
            .planWithGrouping(in, new DynamicScanGroupingBuilder().build(in), 4)
            .getBuckets()
            .size());
  }

  @Test
  public void singleDeviceAndActualInputOneRetainDGuards() {
    for (ScanParallelPlanner.Input in :
        List.of(input(1, 8, 4, false, true), input(4, 8, 1, false, true))) {
      ScanParallelPlan p =
          new ScanParallelPlanner()
              .planWithGrouping(in, new DynamicScanGroupingBuilder().build(in), 1);
      assertEquals(new ScanParallelPlanner().plan(in).getFallbackReason(), p.getFallbackReason());
      assertEquals(1, p.getWorkUnits().size());
    }
  }

  @Test
  public void onePartitionAndOrderedDoNotUseTimeFormula() {
    for (ScanParallelPlanner.Input in :
        List.of(
            input(12, 1, 4, false, true),
            input(12, 20, 4, true, true),
            input(12, 0, 4, false, false))) {
      DynamicScanGrouping g = new DynamicScanGroupingBuilder().build(in);
      assertEquals(1, g.getTimeGroups().size());
      assertEquals(12, g.getWorkUnitCount());
    }
  }

  @Test
  public void immutableCopiesIsolateCallerMutation() {
    List<List<String>> devices = new ArrayList<>();
    List<String> values = new ArrayList<>(List.of("d0"));
    devices.add(values);
    List<Long> costs = new ArrayList<>(List.of(1L));
    DynamicScanGrouping g =
        new DynamicScanGrouping(
            ScanParallelPlan.Mode.DEVICE,
            devices,
            List.of(List.of()),
            costs,
            List.of(DynamicCostSource.EQUAL_WORK));
    values.clear();
    devices.clear();
    costs.clear();
    assertEquals(List.of(List.of("d0")), g.getDeviceGroups());
    assertEquals(List.of(1L), g.getCosts());
  }

  @Test
  public void actualCanonicalKeyOrdersTwoBeforeTen() {
    ScanParallelPlanner.Input in = input(12, 0, 4, false, false);
    DynamicScanGrouping g = new DynamicScanGroupingBuilder().build(in);
    ScanParallelPlan p = new ScanParallelPlanner().planWithGrouping(in, g, 4);
    String two = p.getWorkUnits().get(2).getSourceId();
    String ten = p.getWorkUnits().get(10).getSourceId();
    assertTrue(two.compareTo(ten) < 0);
    assertEquals(
        "2", p.getBuckets().get(2).getWorkUnits().get(0).getSourceKey().getDeviceGroupId());
    assertEquals(
        "10", p.getBuckets().get(2).getWorkUnits().get(2).getSourceKey().getDeviceGroupId());
  }

  @Test
  public void everyOriginalQualificationGuardRetainsItsReason() {
    List<String> devices = List.of("d0", "d1");
    List<Long> times = List.of(0L, 1L);
    List<ScanParallelPlanner.Input> inputs =
        List.of(
            new ScanParallelPlanner.Input(
                List.of(), times, 4, true, false, false, false, false, true),
            new ScanParallelPlanner.Input(
                devices, List.of(), 4, true, false, false, false, false, true),
            new ScanParallelPlanner.Input(
                List.of("d0", "d0"), times, 4, true, false, false, false, false, true),
            new ScanParallelPlanner.Input(
                devices, List.of(0L, 0L), 4, true, false, false, false, false, true),
            new ScanParallelPlanner.Input(
                devices, times, List.of(1L), 4, true, false, false, false, false, true),
            new ScanParallelPlanner.Input(
                devices,
                times,
                java.util.Arrays.asList(null, 1L),
                4,
                true,
                false,
                false,
                false,
                false,
                true),
            new ScanParallelPlanner.Input(
                devices, times, List.of(-1L, 1L), 4, true, false, false, false, false, true),
            new ScanParallelPlanner.Input(
                devices,
                times,
                List.of(Long.MAX_VALUE, 1L),
                4,
                true,
                false,
                false,
                false,
                false,
                true),
            new ScanParallelPlanner.Input(
                devices, times, 1, true, false, false, false, false, true),
            new ScanParallelPlanner.Input(
                List.of("d0"), times, 4, true, false, false, false, false, true),
            new ScanParallelPlanner.Input(devices, times, 4, true, true, false, false, false, true),
            new ScanParallelPlanner.Input(devices, times, 4, true, false, true, false, false, true),
            new ScanParallelPlanner.Input(devices, times, 4, true, false, false, true, false, true),
            new ScanParallelPlanner.Input(
                devices, times, 4, false, false, false, false, false, true));
    List<ScanParallelPlan.FallbackReason> expected =
        List.of(
            ScanParallelPlan.FallbackReason.EMPTY_INPUT,
                ScanParallelPlan.FallbackReason.EMPTY_INPUT,
            ScanParallelPlan.FallbackReason.INVALID_METADATA,
                ScanParallelPlan.FallbackReason.INVALID_METADATA,
            ScanParallelPlan.FallbackReason.INVALID_METADATA,
                ScanParallelPlan.FallbackReason.INVALID_COST,
            ScanParallelPlan.FallbackReason.INVALID_COST, ScanParallelPlan.FallbackReason.OVERFLOW,
            ScanParallelPlan.FallbackReason.DOP_NOT_GREATER_THAN_ONE,
                ScanParallelPlan.FallbackReason.SINGLE_DEVICE,
            ScanParallelPlan.FallbackReason.SPILLED_DEVICE_ENTRIES,
                ScanParallelPlan.FallbackReason.GLOBAL_LIMIT_OR_OFFSET,
            ScanParallelPlan.FallbackReason.ORDERING_NOT_PROVEN,
                ScanParallelPlan.FallbackReason.PARALLEL_SCAN_NOT_ALLOWED);
    ScanParallelPlanner planner = new ScanParallelPlanner();
    DynamicScanGrouping grouping =
        new DynamicScanGroupingBuilder().build(input(2, 2, 4, false, true));
    for (int i = 0; i < inputs.size(); i++) {
      assertEquals(expected.get(i), planner.plan(inputs.get(i)).getFallbackReason());
      assertEquals(
          expected.get(i),
          planner.planWithGrouping(inputs.get(i), grouping, 4).getFallbackReason());
    }
  }

  @Test
  public void oversizeExternalGroupingReturnsExplicitBudgetReason() {
    ScanParallelPlanner.Input in = input(1025, 0, 4, false, false);
    List<List<String>> groups = new ArrayList<>();
    for (String device : in.getDevices()) {
      groups.add(List.of(device));
    }
    DynamicScanGrouping g =
        new DynamicScanGrouping(
            ScanParallelPlan.Mode.DEVICE,
            groups,
            List.of(List.of()),
            Collections.nCopies(1025, 1L),
            Collections.nCopies(1025, DynamicCostSource.EQUAL_WORK));
    ScanParallelPlan p = new ScanParallelPlanner().planWithGrouping(in, g, 4);
    assertEquals(
        ScanParallelPlan.FallbackReason.DYNAMIC_GROUPING_BUDGET_EXCEEDED, p.getFallbackReason());
    assertEquals(1, p.getWorkUnits().size());
    assertEquals(in.getDevices(), p.getWorkUnits().get(0).getDevices());
  }

  @Test
  public void negativeOrOverflowingExternalCostsCannotEnterLpt() {
    ScanParallelPlanner.Input in = input(2, 0, 4, false, false);
    DynamicScanGrouping g = new DynamicScanGroupingBuilder().build(in);
    ScanParallelPlanner planner = new ScanParallelPlanner();
    assertEquals(
        ScanParallelPlan.FallbackReason.INVALID_COST,
        planner
            .planWithGrouping(
                in,
                g.withCosts(
                    List.of(-1L, 1L), Collections.nCopies(2, DynamicCostSource.MEASURED_BUCKET)),
                4)
            .getFallbackReason());
    assertEquals(
        ScanParallelPlan.FallbackReason.OVERFLOW,
        planner
            .planWithGrouping(
                in,
                g.withCosts(
                    List.of(Long.MAX_VALUE, 1L),
                    Collections.nCopies(2, DynamicCostSource.MEASURED_BUCKET)),
                4)
            .getFallbackReason());
  }

  @Test
  public void zeroCostsStillHaveCompleteOwnership() {
    ScanParallelPlanner.Input in = input(12, 0, 4, false, false);
    DynamicScanGrouping g =
        new DynamicScanGroupingBuilder()
            .build(in)
            .withCosts(
                Collections.nCopies(12, 0L),
                Collections.nCopies(12, DynamicCostSource.BUCKET_AVERAGE));
    ScanParallelPlan p = new ScanParallelPlanner().planWithGrouping(in, g, 4);
    assertEquals(12, p.getWorkUnits().size());
    int owned = 0;
    for (ScanParallelPlan.Bucket bucket : p.getBuckets()) {
      owned += bucket.getWorkUnits().size();
      assertEquals(0, bucket.getEstimatedCost());
    }
    assertEquals(12, owned);
  }
}
