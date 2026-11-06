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
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

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
}
