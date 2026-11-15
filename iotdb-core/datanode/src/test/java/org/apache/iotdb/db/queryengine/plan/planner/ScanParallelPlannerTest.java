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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ScanParallelPlannerTest {

  private final ScanParallelPlanner planner = new ScanParallelPlanner();

  @Test
  public void serialReasonsRespectModeInputs() {
    assertEquals(
        ScanParallelPlan.FallbackReason.DOP_NOT_GREATER_THAN_ONE,
        planner.plan(input(1, true, false, false, false, false, false)).getFallbackReason());
    assertEquals(
        ScanParallelPlan.FallbackReason.SPILLED_DEVICE_ENTRIES,
        planner.plan(input(4, true, true, false, false, false, false)).getFallbackReason());
    assertEquals(
        ScanParallelPlan.FallbackReason.GLOBAL_LIMIT_OR_OFFSET,
        planner.plan(input(4, true, false, true, false, false, false)).getFallbackReason());
    assertEquals(
        ScanParallelPlan.FallbackReason.ORDERING_NOT_PROVEN,
        planner.plan(input(4, true, false, false, true, false, false)).getFallbackReason());
  }

  @Test
  public void deviceGroupsAreContiguousAndComplete() {
    ScanParallelPlan plan = planner.plan(input(4, true, false, false, false, false, false));
    assertEquals(ScanParallelPlan.Mode.DEVICE, plan.getMode());
    assertEquals(4, plan.getPipelineCount());
    assertEquals(Arrays.asList("d0", "d1"), plan.getDeviceGroups().get(0));
    assertEquals(Arrays.asList("d6", "d7"), plan.getDeviceGroups().get(3));
  }

  @Test
  public void timePartitionProductUsesLongBeforeClamping() {
    ScanParallelPlan plan = planner.plan(input(4, true, false, false, false, true, false));
    assertEquals(ScanParallelPlan.Mode.TIME_PARTITION, plan.getMode());
    assertEquals(3, plan.getTimePartitionGroups().size());
    assertEquals(1, plan.getDeviceGroups().size());
    assertEquals(3, plan.getPipelineCount());
  }

  @Test
  public void orderedModeUsesOrderingProofWithoutUnorderedFlag() {
    ScanParallelPlan plan = planner.plan(input(3, false, false, false, true, false, true));
    assertEquals(ScanParallelPlan.Mode.ORDERED, plan.getMode());
    assertEquals(3, plan.getPipelineCount());
  }

  @Test
  public void arithmeticRejectsNegativeAndIntegerOverflow() {
    assertEquals(3_000_000_000L, ScanParallelPlanner.safeProduct(100_000, 30_000));
    assertEquals(
        ScanParallelPlanner.Estimate.Status.INVALID_METADATA,
        ScanParallelPlanner.estimateDopByBytes(Arrays.asList(10L, -1L), 128, 4).getStatus());
    assertEquals(
        4,
        ScanParallelPlanner.estimateDopByBytes(
                Arrays.asList(Long.MAX_VALUE, Long.MAX_VALUE), 128, 4)
            .getPipelineCount());
  }

  @Test
  public void missingMetadataKeepsDeviceFallback() {
    ScanParallelPlanner.Estimate estimate = ScanParallelPlanner.estimateDopByBytes(null, 128, 7);
    assertEquals(ScanParallelPlanner.Estimate.Status.INVALID_METADATA, estimate.getStatus());
    assertEquals(7, estimate.getPipelineCount());
  }

  @Test
  public void inputAndPlanAreImmutableSnapshots() {
    List<String> devices = new ArrayList<>(Arrays.asList("d0", "d1", "d2", "d3"));
    List<Long> partitions = new ArrayList<>(Arrays.asList(0L, 1L));
    ScanParallelPlanner.Input input =
        new ScanParallelPlanner.Input(
            devices, partitions, 2, true, false, false, false, false, true);
    ScanParallelPlan plan = planner.plan(input);
    devices.set(0, "changed");
    partitions.add(2L);
    assertEquals("d0", plan.getDeviceGroups().get(0).get(0));
    assertNotSame(devices, plan.getDeviceGroups().get(0));
    assertTrue(plan.getTimePartitionGroups().get(0).contains(0L));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void planGroupsCannotBeMutated() {
    planner
        .plan(input(4, true, false, false, false, false, false))
        .getDeviceGroups()
        .get(0)
        .add("x");
  }

  private static ScanParallelPlanner.Input input(
      int dop,
      boolean allow,
      boolean spilled,
      boolean globalLimit,
      boolean ordered,
      boolean timeMorsel,
      boolean orderingProven) {
    return new ScanParallelPlanner.Input(
        Arrays.asList("d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7"),
        Arrays.asList(0L, 1L, 2L),
        dop,
        allow,
        spilled,
        globalLimit,
        ordered,
        orderingProven,
        timeMorsel);
  }

  @Test
  public void emptyScanUsesSerialFallback() {
    ScanParallelPlan noDevices =
        planner.plan(
            new ScanParallelPlanner.Input(
                java.util.Collections.emptyList(),
                Arrays.asList(0L, 1L),
                8,
                true,
                false,
                false,
                false,
                false,
                true));
    assertEquals(ScanParallelPlan.Mode.SERIAL, noDevices.getMode());
    assertEquals(ScanParallelPlan.FallbackReason.EMPTY_INPUT, noDevices.getFallbackReason());
    assertTrue(noDevices.getWorkUnits().isEmpty());
    ScanParallelPlan noPartitions =
        planner.plan(
            new ScanParallelPlanner.Input(
                Arrays.asList("d0", "d1"),
                java.util.Collections.emptyList(),
                8,
                true,
                false,
                false,
                false,
                false,
                true));
    assertEquals(ScanParallelPlan.Mode.SERIAL, noPartitions.getMode());
    assertEquals(ScanParallelPlan.FallbackReason.EMPTY_INPUT, noPartitions.getFallbackReason());
  }

  @Test
  public void singleDeviceRemainsSerialWithMultiplePartitions() {
    ScanParallelPlan plan =
        planner.plan(
            new ScanParallelPlanner.Input(
                Arrays.asList("d0"),
                Arrays.asList(0L, 1L, 2L),
                8,
                true,
                false,
                false,
                false,
                false,
                true));
    assertEquals(ScanParallelPlan.Mode.SERIAL, plan.getMode());
    assertEquals(ScanParallelPlan.FallbackReason.SINGLE_DEVICE, plan.getFallbackReason());
    assertEquals(1, plan.getPipelineCount());
  }

  @Test
  public void duplicatePartitionsRejectMetadata() {
    ScanParallelPlan plan =
        planner.plan(
            new ScanParallelPlanner.Input(
                Arrays.asList("d0", "d1"),
                Arrays.asList(0L, 1L, 1L),
                8,
                true,
                false,
                false,
                false,
                false,
                true));
    assertEquals(ScanParallelPlan.Mode.SERIAL, plan.getMode());
    assertEquals(ScanParallelPlan.FallbackReason.INVALID_METADATA, plan.getFallbackReason());
  }

  @Test(timeout = 15000)
  public void largeMetadataProductClampsToEightBoundedWorkUnits() {
    // 130,000 metadata entries exercise the actual planner, with a 15-second upper bound.
    // Cost metadata is absent to avoid quadratic cost lookup; never materialize 3 billion pairs.
    List<String> devices = new ArrayList<>(100_000);
    List<Long> partitions = new ArrayList<>(30_000);
    for (int i = 0; i < 100_000; i++) devices.add("d" + i);
    for (long i = 0; i < 30_000; i++) partitions.add(i);
    ScanParallelPlan plan =
        planner.plan(
            new ScanParallelPlanner.Input(
                devices, partitions, 8, true, false, false, false, false, true));
    assertEquals(
        3_000_000_000L, ScanParallelPlanner.safeProduct(devices.size(), partitions.size()));
    assertEquals(ScanParallelPlan.Mode.TIME_PARTITION, plan.getMode());
    assertEquals(8, plan.getPipelineCount());
    assertEquals(8, plan.getWorkUnits().size());
    assertEquals(8, plan.getBuckets().size());
    java.util.Set<String> assignedDevices = new java.util.HashSet<>();
    int deviceEntries = 0;
    for (List<String> group : plan.getDeviceGroups()) {
      assignedDevices.addAll(group);
      deviceEntries += group.size();
    }
    java.util.Set<Long> assignedPartitions = new java.util.HashSet<>();
    int partitionEntries = 0;
    for (List<Long> group : plan.getTimePartitionGroups()) {
      assignedPartitions.addAll(group);
      partitionEntries += group.size();
    }
    assertEquals(new java.util.HashSet<>(devices), assignedDevices);
    assertEquals(devices.size(), deviceEntries);
    assertEquals(new java.util.HashSet<>(partitions), assignedPartitions);
    assertEquals(partitions.size(), partitionEntries);
  }

  @Test
  public void emptyFileMetadataKeepsDeviceFallback() {
    ScanParallelPlanner.Estimate estimate =
        ScanParallelPlanner.estimateDopByBytes(java.util.Collections.emptyList(), 128, 7);
    assertEquals(ScanParallelPlanner.Estimate.Status.INVALID_METADATA, estimate.getStatus());
    assertEquals(7, estimate.getPipelineCount());
    assertEquals(
        1, ScanParallelPlanner.estimateDopByBytes(Arrays.asList(0L), 128, 7).getPipelineCount());
    assertEquals(-1L, ScanParallelPlanner.safeProduct(-1, 4));
  }

  @Test
  public void productionByteEstimatePreservesFallbacksAndSaturates() throws Exception {
    assertEquals(7, productionByteEstimate(null));
    assertEquals(7, productionByteEstimate(java.util.Collections.emptyList()));
    assertEquals(7, productionByteEstimate(Arrays.asList(-1L)));
    assertEquals(7, productionByteEstimate(Arrays.asList(Long.MAX_VALUE, Long.MAX_VALUE)));
    assertEquals(1, productionByteEstimate(Arrays.asList(0L)));
    assertEquals(
        2,
        productionByteEstimate(
            Arrays.asList(64L * 1024 * 1024, 64L * 1024 * 1024, 64L * 1024 * 1024)));
  }

  private int productionByteEstimate(List<Long> sizes) throws Exception {
    org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager files =
        org.mockito.Mockito.mock(
            org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager.class);
    org.mockito.Mockito.when(files.getTimePartitions())
        .thenReturn(new java.util.HashSet<>(Arrays.asList(0L, 1L)));
    if (sizes == null) {
      org.mockito.Mockito.when(
              files.getAllTsFileListForQuery(
                  org.mockito.Mockito.isNull(), org.mockito.Mockito.isNull()))
          .thenReturn(null);
    } else {
      List<org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource> resources =
          new ArrayList<>();
      for (Long size : sizes) {
        org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource resource =
            org.mockito.Mockito.mock(
                org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource.class);
        org.mockito.Mockito.when(resource.getTsFileSize()).thenReturn(size);
        resources.add(resource);
      }
      org.mockito.Mockito.when(
              files.getAllTsFileListForQuery(
                  org.mockito.Mockito.isNull(), org.mockito.Mockito.isNull()))
          .thenReturn(
              new org.apache.tsfile.utils.Pair<>(resources, java.util.Collections.emptyList()));
    }
    org.apache.iotdb.db.storageengine.dataregion.DataRegion region =
        org.mockito.Mockito.mock(org.apache.iotdb.db.storageengine.dataregion.DataRegion.class);
    org.mockito.Mockito.when(region.getTsFileManager()).thenReturn(files);
    java.lang.reflect.Method estimate =
        DataNodeTableOperatorGenerator.class.getDeclaredMethod(
            "estimatePipelineNumByDataSize",
            org.apache.iotdb.db.storageengine.dataregion.IDataRegionForQuery.class,
            int.class);
    estimate.setAccessible(true);
    int result =
        (int)
            estimate.invoke(
                new DataNodeTableOperatorGenerator(
                    new org.apache.iotdb.db.queryengine.plan.relational.metadata
                        .TableMetadataImpl()),
                region,
                7);
    org.mockito.Mockito.verify(files)
        .getAllTsFileListForQuery(org.mockito.Mockito.isNull(), org.mockito.Mockito.isNull());
    return result;
  }

  @org.junit.Test
  public void largeDeviceCostVectorPreservesCompleteCostAndCoverage() {
    java.util.List<String> devices = new java.util.ArrayList<>();
    java.util.List<Long> costs = new java.util.ArrayList<>();
    for (int i = 0; i < 100000; i++) {
      devices.add("device_" + i);
      costs.add((long) i + 1);
    }
    ScanParallelPlan plan =
        new ScanParallelPlanner()
            .plan(
                new ScanParallelPlanner.Input(
                    devices,
                    java.util.Collections.emptyList(),
                    costs,
                    8,
                    true,
                    false,
                    false,
                    false,
                    false,
                    false,
                    "large_cost",
                    "0.i",
                    "source"));
    long total = 0;
    java.util.Set<String> coverage = new java.util.HashSet<>();
    for (ScanParallelPlan.WorkUnit work : plan.getWorkUnits()) {
      total += work.getEstimatedCost();
      coverage.addAll(work.getDevices());
    }
    org.junit.Assert.assertEquals(5000050000L, total);
    org.junit.Assert.assertEquals(100000, coverage.size());
    org.junit.Assert.assertEquals(8, plan.getBuckets().size());
    org.junit.Assert.assertEquals(8, plan.getWorkUnits().size());
  }

  @Test
  public void runtimeInitialWorkersDoNotChangeLogicalTopologyForAnyScanMode() {
    ScanParallelPlanner.Input[] inputs = {
      input(8, true, false, false, false, false, false),
      input(8, true, false, false, false, true, false),
      input(8, false, false, false, true, false, true)
    };
    ScanParallelPlan.Mode[] modes = {
      ScanParallelPlan.Mode.DEVICE,
      ScanParallelPlan.Mode.TIME_PARTITION,
      ScanParallelPlan.Mode.ORDERED
    };
    for (int index = 0; index < inputs.length; index++) {
      RuntimeScanLayout oneWorker = planner.planRuntimeLayout(inputs[index], null, 4, 1);
      RuntimeScanLayout fourWorkers = planner.planRuntimeLayout(inputs[index], null, 4, 4);
      assertEquals(modes[index], oneWorker.getLanePlan().getMode());
      assertEquals(4, oneWorker.getLaneCount());
      assertEquals(1, oneWorker.getInitialWorkers());
      assertEquals(4, fourWorkers.getInitialWorkers());
      assertEquals(
          runtimeTopologySignature(oneWorker.getLanePlan()),
          runtimeTopologySignature(fourWorkers.getLanePlan()));
      assertRuntimeWorkCoverage(oneWorker.getLanePlan(), runtimeDevices(), runtimePartitions());
      assertRuntimeWorkCoverage(fourWorkers.getLanePlan(), runtimeDevices(), runtimePartitions());
    }
  }

  @Test
  public void runtimeLogicalLaneCapPreservesEveryOriginalWorkUnit() {
    ScanParallelPlanner.Input input = input(8, true, false, false, false, true, false);
    ScanParallelPlan original = planner.plan(input);
    RuntimeScanLayout capped = planner.planRuntimeLayout(input, null, 2, 8);

    assertEquals(ScanParallelPlan.Mode.TIME_PARTITION, capped.getLanePlan().getMode());
    assertEquals(2, capped.getLaneCount());
    assertEquals(2, capped.getInitialWorkers());
    assertEquals(6, original.getWorkUnits().size());
    assertEquals(
        runtimeWorkUnitSignatures(original), runtimeWorkUnitSignatures(capped.getLanePlan()));
    assertTrue(
        capped.getLanePlan().getBuckets().stream()
            .anyMatch(bucket -> bucket.getWorkUnits().size() > 1));
    assertRuntimeWorkCoverage(capped.getLanePlan(), runtimeDevices(), runtimePartitions());
  }

  @Test
  public void runtimeOneLogicalLaneKeepsAllWorkWithoutRelaxingScanSemantics() {
    RuntimeScanLayout layout =
        planner.planRuntimeLayout(input(8, false, false, false, true, false, true), null, 1, 4);

    assertEquals(ScanParallelPlan.Mode.ORDERED, layout.getLanePlan().getMode());
    assertEquals(1, layout.getLaneCount());
    assertEquals(1, layout.getInitialWorkers());
    assertEquals(8, layout.getLanePlan().getBuckets().get(0).getWorkUnits().size());
    assertRuntimeWorkCoverage(layout.getLanePlan(), runtimeDevices(), runtimePartitions());
  }

  @Test
  public void runtimeLaneAndWorkerCountsRespectOriginalInputDop() {
    RuntimeScanLayout layout =
        planner.planRuntimeLayout(input(4, true, false, false, false, false, false), null, 32, 32);

    assertEquals(4, layout.getLanePlan().getInputDop());
    assertEquals(4, layout.getLaneCount());
    assertEquals(4, layout.getInitialWorkers());
    assertRuntimeWorkCoverage(layout.getLanePlan(), runtimeDevices(), runtimePartitions());
  }

  @Test
  public void runtimeWorkerCountAlsoClampsToAvailableWork() {
    List<String> devices = Arrays.asList("d0", "d1");
    ScanParallelPlanner.Input input =
        new ScanParallelPlanner.Input(
            devices, runtimePartitions(), 8, true, false, false, false, false, false);

    RuntimeScanLayout layout = planner.planRuntimeLayout(input, null, 16, 16);
    assertEquals(2, layout.getLaneCount());
    assertEquals(2, layout.getInitialWorkers());
    assertRuntimeWorkCoverage(layout.getLanePlan(), devices, runtimePartitions());
  }

  @Test
  public void runtimeFineTimePartitionGroupingRetainsAllUnitsWhenLanesAreCompacted() {
    ScanParallelPlanner.Input input = input(8, true, false, false, false, true, false);
    DynamicScanGrouping grouping = runtimeFineTimePartitionGrouping();
    RuntimeScanLayout oneWorker = planner.planRuntimeLayout(input, grouping, 4, 1);
    RuntimeScanLayout fourWorkers = planner.planRuntimeLayout(input, grouping, 4, 4);

    assertEquals(ScanParallelPlan.Mode.TIME_PARTITION, oneWorker.getLanePlan().getMode());
    assertEquals(4, oneWorker.getLaneCount());
    assertEquals(1, oneWorker.getInitialWorkers());
    assertEquals(4, fourWorkers.getInitialWorkers());
    assertEquals(24, oneWorker.getLanePlan().getWorkUnits().size());
    assertEquals(
        runtimeTopologySignature(oneWorker.getLanePlan()),
        runtimeTopologySignature(fourWorkers.getLanePlan()));
    assertRuntimeWorkCoverage(oneWorker.getLanePlan(), runtimeDevices(), runtimePartitions());

    long assignedCost = 0;
    for (ScanParallelPlan.Bucket bucket : oneWorker.getLanePlan().getBuckets()) {
      assertTrue(bucket.getWorkUnits().size() > 1);
      for (ScanParallelPlan.WorkUnit unit : bucket.getWorkUnits()) {
        assertTrue(unit.isEstimateSupported());
        assignedCost += unit.getEstimatedCost();
      }
    }
    assertEquals(300L, assignedCost);
  }

  @Test
  public void runtimeUnsafeInputsRemainSerialRegardlessOfRequestedWorkersOrGrouping() {
    ScanParallelPlanner.Input[] inputs = {
      input(1, true, false, false, false, false, false),
      input(8, true, true, false, false, false, false),
      input(8, true, false, true, false, false, false),
      input(8, true, false, false, true, false, false),
      input(8, false, false, false, false, false, false)
    };
    ScanParallelPlan.FallbackReason[] reasons = {
      ScanParallelPlan.FallbackReason.DOP_NOT_GREATER_THAN_ONE,
      ScanParallelPlan.FallbackReason.SPILLED_DEVICE_ENTRIES,
      ScanParallelPlan.FallbackReason.GLOBAL_LIMIT_OR_OFFSET,
      ScanParallelPlan.FallbackReason.ORDERING_NOT_PROVEN,
      ScanParallelPlan.FallbackReason.PARALLEL_SCAN_NOT_ALLOWED
    };
    for (int index = 0; index < inputs.length; index++) {
      // Supplying a valid fine-grained grouping must not bypass an earlier input-safety rejection.
      RuntimeScanLayout layout =
          planner.planRuntimeLayout(inputs[index], runtimeFineTimePartitionGrouping(), 4, 99);
      assertEquals(ScanParallelPlan.Mode.SERIAL, layout.getLanePlan().getMode());
      assertEquals(reasons[index], layout.getLanePlan().getFallbackReason());
      assertEquals(1, layout.getLaneCount());
      assertEquals(1, layout.getInitialWorkers());
      assertEquals(
          runtimeTopologySignature(planner.plan(inputs[index])),
          runtimeTopologySignature(layout.getLanePlan()));
      assertRuntimeWorkCoverage(layout.getLanePlan(), runtimeDevices(), runtimePartitions());
    }
  }

  @Test
  public void runtimeInvalidGroupingFallsBackToCompleteOriginalInput() {
    DynamicScanGrouping missingDevice =
        new DynamicScanGrouping(
            ScanParallelPlan.Mode.DEVICE,
            Collections.singletonList(runtimeDevices().subList(0, 7)),
            Collections.singletonList(runtimePartitions()),
            Collections.singletonList(7L),
            Collections.singletonList(DynamicCostSource.EQUAL_WORK));
    RuntimeScanLayout layout =
        planner.planRuntimeLayout(
            input(8, true, false, false, false, false, false), missingDevice, 4, 4);

    assertEquals(ScanParallelPlan.Mode.SERIAL, layout.getLanePlan().getMode());
    assertEquals(
        ScanParallelPlan.FallbackReason.DYNAMIC_GROUPING_COVERAGE_MISMATCH,
        layout.getLanePlan().getFallbackReason());
    assertEquals(1, layout.getLaneCount());
    assertEquals(1, layout.getInitialWorkers());
    assertRuntimeWorkCoverage(layout.getLanePlan(), runtimeDevices(), runtimePartitions());
  }

  @Test
  public void runtimeEmptyInputStaysSerialWithoutInventingWork() {
    ScanParallelPlanner.Input input =
        new ScanParallelPlanner.Input(
            Collections.emptyList(),
            runtimePartitions(),
            8,
            true,
            false,
            false,
            false,
            false,
            true);
    RuntimeScanLayout layout = planner.planRuntimeLayout(input, null, 4, 4);

    assertEquals(ScanParallelPlan.Mode.SERIAL, layout.getLanePlan().getMode());
    assertEquals(
        ScanParallelPlan.FallbackReason.EMPTY_INPUT, layout.getLanePlan().getFallbackReason());
    assertEquals(1, layout.getLaneCount());
    assertEquals(1, layout.getInitialWorkers());
    assertTrue(layout.getLanePlan().getWorkUnits().isEmpty());
    assertTrue(layout.getLanePlan().getBuckets().get(0).getWorkUnits().isEmpty());
  }

  @Test
  public void runtimeLayoutRejectsNonpositiveLaneAndWorkerRequests() {
    int[][] invalidRequests = {{0, 1}, {-1, 1}, {4, 0}, {4, -1}};
    for (int[] request : invalidRequests) {
      try {
        planner.planRuntimeLayout(
            input(8, true, false, false, false, false, false), null, request[0], request[1]);
        fail("Expected invalid runtime request to be rejected: " + Arrays.toString(request));
      } catch (IllegalArgumentException expected) {
        // Requests are invalid even though the scan itself could otherwise be parallelized.
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void runtimeLayoutConstructorRejectsZeroWorkers() {
    new RuntimeScanLayout(planner.plan(input(4, true, false, false, false, false, false)), 0);
  }

  @Test(expected = IllegalArgumentException.class)
  public void runtimeLayoutConstructorRejectsMoreWorkersThanLogicalLanes() {
    ScanParallelPlan plan = planner.plan(input(4, true, false, false, false, false, false));
    new RuntimeScanLayout(plan, plan.getPipelineCount() + 1);
  }

  @Test(expected = NullPointerException.class)
  public void runtimeLayoutConstructorRejectsMissingPlan() {
    new RuntimeScanLayout(null, 1);
  }

  private static DynamicScanGrouping runtimeFineTimePartitionGrouping() {
    List<List<String>> devices = new ArrayList<>();
    for (String device : runtimeDevices()) {
      devices.add(Collections.singletonList(device));
    }
    List<List<Long>> partitions = new ArrayList<>();
    for (Long partition : runtimePartitions()) {
      partitions.add(Collections.singletonList(partition));
    }
    List<Long> costs = new ArrayList<>();
    List<DynamicCostSource> sources = new ArrayList<>();
    for (int index = 1; index <= devices.size() * partitions.size(); index++) {
      costs.add((long) index);
      sources.add(DynamicCostSource.TSFILE_BYTES);
    }
    return new DynamicScanGrouping(
        ScanParallelPlan.Mode.TIME_PARTITION, devices, partitions, costs, sources);
  }

  private static List<String> runtimeDevices() {
    return Arrays.asList("d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7");
  }

  private static List<Long> runtimePartitions() {
    return Arrays.asList(0L, 1L, 2L);
  }

  private static List<String> runtimeWorkUnitSignatures(ScanParallelPlan plan) {
    List<String> signatures = new ArrayList<>();
    for (ScanParallelPlan.WorkUnit unit : plan.getWorkUnits()) {
      signatures.add(
          unit.getSourceId()
              + "|"
              + unit.getDevices()
              + "|"
              + unit.getTimePartitions()
              + "|"
              + unit.getEstimatedCost()
              + "|"
              + unit.isEstimateSupported());
    }
    return signatures;
  }

  private static List<String> runtimeTopologySignature(ScanParallelPlan plan) {
    List<String> signature = new ArrayList<>();
    signature.add(plan.getMode().name());
    signature.add(plan.getFallbackReason().name());
    signature.add(Integer.toString(plan.getInputDop()));
    signature.add(Integer.toString(plan.getTargetParallelism()));
    signature.add(plan.getDeviceGroups().toString());
    signature.add(plan.getTimePartitionGroups().toString());
    signature.add(plan.getPlanIdentityChecksum());
    signature.addAll(runtimeWorkUnitSignatures(plan));
    for (ScanParallelPlan.Bucket bucket : plan.getBuckets()) {
      List<String> sources = new ArrayList<>();
      for (ScanParallelPlan.WorkUnit unit : bucket.getWorkUnits()) {
        sources.add(unit.getSourceId());
      }
      signature.add(bucket.getBucketId() + "|" + bucket.getEstimatedCost() + "|" + sources);
    }
    return signature;
  }

  private static void assertRuntimeWorkCoverage(
      ScanParallelPlan plan, List<String> expectedDevices, List<Long> expectedPartitions) {
    Map<String, Integer> expected = new HashMap<>();
    for (String device : expectedDevices) {
      for (Long partition : expectedPartitions) {
        expected.put(device + "@" + partition, 1);
      }
    }
    Map<String, Integer> actual = new HashMap<>();
    Set<String> sources = new HashSet<>();
    for (ScanParallelPlan.WorkUnit unit : plan.getWorkUnits()) {
      assertTrue("Duplicate work-unit identity", sources.add(unit.getSourceId()));
      for (String device : unit.getDevices()) {
        for (Long partition : unit.getTimePartitions()) {
          actual.merge(device + "@" + partition, 1, Integer::sum);
        }
      }
    }
    assertEquals(
        "Work units must cover the original device/time product exactly once", expected, actual);

    Set<String> assigned = new HashSet<>();
    for (ScanParallelPlan.Bucket bucket : plan.getBuckets()) {
      for (ScanParallelPlan.WorkUnit unit : bucket.getWorkUnits()) {
        assertTrue("Work unit assigned to multiple lanes", assigned.add(unit.getSourceId()));
      }
    }
    assertEquals("Every work unit must have exactly one logical lane", sources, assigned);
  }
}
