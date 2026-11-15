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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.calc.execution.filter.TopKRuntimeFilter;
import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryDataSourceLease;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.AlignedSeriesScanUtil;
import org.apache.iotdb.db.queryengine.execution.operator.source.relational.AbstractTableScanOperator.AbstractTableScanOperatorParameter;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeFragmentLifecycle;
import org.apache.iotdb.db.queryengine.execution.runtime.RuntimeScanProgress;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.spill.BatchDeviceEntrySource;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.spill.InMemoryDeviceEntrySource;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;

import com.google.common.util.concurrent.MoreExecutors;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Exercises real scan control flow while replacing only storage reads with empty results. */
public class AbstractTableScanRuntimeProgressTest {

  @Test
  public void testMoveToNextDeviceResolvesOneLogicalDevice() throws Exception {
    try (Fixture f = new Fixture(true, false, 3, plainOptions(), entries(0, 3))) {
      RuntimeScanProgress.Snapshot before = f.operator.getRuntimeWorkProgress().get();
      assertProgress(f.operator, 3, 0);
      f.operator.advanceDevice();
      assertEquals(1, f.operator.deviceIndex());
      assertProgress(f.operator, 3, 1);
      f.operator.advanceDevice();
      assertProgress(f.operator, 3, 2);
      f.operator.advanceDevice();
      assertProgress(f.operator, 3, 3);
      assertTrue(f.operator.isFinished());
      assertFalse(f.operator.hasNext());
      assertEquals(0, before.completed());
    }
  }

  @Test
  public void testRealNextCompletesEmptyDevicesWithoutCountingOutputRows() throws Exception {
    try (Fixture f = new Fixture(true, false, 4, plainOptions(), entries(0, 4))) {
      f.operator.initQueryDataSource(emptyDataSource());
      for (int resolved = 1; resolved <= 4; resolved++) {
        assertTrue(f.operator.hasNext());
        assertNull(f.operator.next());
        assertProgress(f.operator, 4, resolved);
      }
      assertTrue(f.operator.isFinished());
      assertNull(f.operator.next());
      assertTrue(f.operator.isFinished());
      assertProgress(f.operator, 4, 4);
      assertEquals(0, f.operator.getRuntimeWorkProgress().get().pruned());
    }
  }

  @Test
  public void testInMemoryBatchReleaseDoesNotResetTotalOrCompletedProgress() throws Exception {
    try (Fixture f = new Fixture(true, true, 5, plainOptions(), entries(0, 5))) {
      f.operator.initQueryDataSource(null);
      assertEquals(0, f.operator.batchDeviceCount());
      assertProgress(f.operator, 5, 0);
      for (int completed = 1; completed <= 5; completed++) {
        assertNull(f.operator.next());
        assertProgress(f.operator, 5, completed);
      }
      assertEquals(0, f.operator.deviceIndex());
      assertEquals(0, f.operator.batchDeviceCount());
      assertTrue(f.operator.batchDataSourceReleased());
      assertTrue(f.operator.isFinished());
      assertEquals(1, f.leaseRequests);
      f.operator.close();
      assertProgress(f.operator, 5, 5);
    }
  }

  @Test
  public void testBatchPruningResolvesCurrentThenPrunesRemainingWithoutReset() throws Exception {
    SeriesScanOptions options =
        new SeriesScanOptions.Builder().withTopKRuntimeFilter(new TopKRuntimeFilter(true)).build();
    try (Fixture f = new Fixture(true, true, 5, options, entries(0, 5))) {
      f.operator.initQueryDataSource(null);
      assertNull(f.operator.next());
      assertProgress(f.operator, 5, 1);
      f.operator.attachDataSource(fullyPrunedDataSource());
      assertNull(f.operator.next());
      assertProgress(f.operator, 5, 5);
      assertEquals(3, f.operator.getRuntimeWorkProgress().get().pruned());
      assertTrue(f.operator.batchDataSourceReleased());
      assertEquals(0, f.operator.batchDeviceCount());
      assertTrue(f.operator.isFinished());
      assertFalse(f.operator.hasNext());
      assertProgress(f.operator, 5, 5);
      assertEquals(1, f.leaseRequests);
    }
  }

  @Test
  public void testNonBatchRuntimeFilterEofResolvesUnvisitedDevices() throws Exception {
    SeriesScanOptions options =
        new SeriesScanOptions.Builder().withTopKRuntimeFilter(new TopKRuntimeFilter(true)).build();
    try (Fixture f = new Fixture(true, false, 4, options, entries(0, 4))) {
      f.operator.initQueryDataSource(emptyDataSource());
      f.operator.advanceDevice();
      assertProgress(f.operator, 4, 1);
      f.operator.attachDataSource(fullyPrunedDataSource());

      // A real driver asks hasNext/isFinished before next; no final moveToNextDevice is guaranteed.
      assertTrue(f.operator.isFinished());
      assertFalse(f.operator.hasNext());
      assertProgress(f.operator, 4, 4);
      assertEquals(3, f.operator.getRuntimeWorkProgress().get().pruned());
      assertTrue(f.operator.isFinished());
      assertFalse(f.operator.hasNext());
      assertProgress(f.operator, 4, 4);
    }
  }

  @Test
  public void testRuntimeFilterBeforeFirstReadMarksAllUnvisitedUnitsResolved() throws Exception {
    SeriesScanOptions options =
        new SeriesScanOptions.Builder().withTopKRuntimeFilter(new TopKRuntimeFilter(true)).build();
    try (Fixture f = new Fixture(true, false, 3, options, entries(0, 3))) {
      f.operator.initQueryDataSource(fullyPrunedDataSource());
      assertFalse(f.operator.hasNext());
      assertProgress(f.operator, 3, 3);
      assertEquals(3, f.operator.getRuntimeWorkProgress().get().pruned());
      f.operator.close();
      assertProgress(f.operator, 3, 3);
    }
  }

  @Test
  public void testEarlyCloseDoesNotFabricateUnseenProgress() throws Exception {
    try (Fixture f = new Fixture(true, true, 7, plainOptions(), entries(0, 7))) {
      f.operator.initQueryDataSource(null);
      f.operator.next();
      f.operator.next();
      assertProgress(f.operator, 7, 2);
      f.operator.close();
      assertFalse(f.deviceEntrySource.hasNextBatch());
      assertEquals(1, f.leaseRequests);
      assertEquals(0, f.operator.batchDeviceCount());
      assertTrue(f.operator.batchDataSourceReleased());
      assertProgress(f.operator, 7, 2);
      assertEquals(0, f.operator.getRuntimeWorkProgress().get().pruned());
    }
  }

  @Test
  public void testSpilledOrCustomMultiBatchSourceDoesNotClaimCompleteProgress() throws Exception {
    try (Fixture f = new Fixture(true, true, 7, plainOptions(), entries(0, 2), entries(2, 1))) {
      f.operator.initQueryDataSource(null);
      assertFalse(f.operator.getRuntimeWorkProgress().isPresent());
      while (f.operator.hasNext()) {
        f.operator.next();
        assertFalse(f.operator.getRuntimeWorkProgress().isPresent());
      }
      assertTrue(f.operator.isFinished());
      f.operator.close();
      assertFalse(f.operator.getRuntimeWorkProgress().isPresent());
      assertEquals(2, f.batchSource.nextCalls);
      assertEquals(2, f.leaseRequests);
    }
  }

  @Test
  public void testGlobalLimitResolvesRemainingAssignedUnitsAsPruned() throws Exception {
    SeriesScanOptions options =
        new SeriesScanOptions.Builder()
            .withPushDownLimit(1)
            .withPushLimitToEachDevice(false)
            .build();
    try (Fixture f = new Fixture(true, false, 4, options, entries(0, 4))) {
      f.operator.advanceDevice();
      assertProgress(f.operator, 4, 1);
      options.getPaginationController().consumeLimit(1);
      assertTrue(f.operator.isFinished());
      assertFalse(f.operator.hasNext());
      assertProgress(f.operator, 4, 4);
      assertEquals(3, f.operator.getRuntimeWorkProgress().get().pruned());
      assertTrue(f.operator.isFinished());
      assertEquals(3, f.operator.getRuntimeWorkProgress().get().pruned());
    }
  }

  @Test
  public void testEmptySpilledBatchesKeepProgressUnavailableWithoutExtraLeases() throws Exception {
    try (Fixture f =
        new Fixture(
            true,
            true,
            2,
            plainOptions(),
            Collections.emptyList(),
            entries(0, 2),
            Collections.emptyList())) {
      f.operator.initQueryDataSource(null);
      assertFalse(f.operator.getRuntimeWorkProgress().isPresent());
      assertNull(f.operator.next());
      assertNull(f.operator.next());
      assertNull(f.operator.next());
      assertTrue(f.operator.isFinished());
      assertFalse(f.operator.getRuntimeWorkProgress().isPresent());
      assertEquals(3, f.batchSource.nextCalls);
      assertEquals(1, f.leaseRequests);
    }
  }

  @Test
  public void testEmptyInputStartsAtCompletedZeroWithoutSyntheticAdvance() throws Exception {
    try (Fixture f = new Fixture(true, false, 0, plainOptions(), Collections.emptyList())) {
      assertTrue(f.operator.isFinished());
      assertFalse(f.operator.hasNext());
      assertNull(f.operator.next());
      assertProgress(f.operator, 0, 0);
      assertEquals(0, f.operator.getRuntimeWorkProgress().get().pruned());
    }
  }

  @Test
  public void testStaticModeNeverAllocatesCounterForOtherwiseEligibleInMemoryScan()
      throws Exception {
    try (Fixture f = new Fixture(false, true, 3, plainOptions(), entries(0, 3))) {
      f.operator.initQueryDataSource(null);
      assertFalse(f.operator.getRuntimeWorkProgress().isPresent());
      while (f.operator.hasNext()) {
        f.operator.next();
        assertFalse(f.operator.getRuntimeWorkProgress().isPresent());
      }
      f.operator.close();
      assertFalse(f.operator.getRuntimeWorkProgress().isPresent());
      assertEquals(1, f.leaseRequests);
    }
  }

  @Test
  public void testKnownTotalStillCannotQualifyCustomSpilledSource() throws Exception {
    try (Fixture f = new Fixture(true, true, 3, plainOptions(), entries(0, 1), entries(1, 2))) {
      f.operator.initQueryDataSource(null);
      while (f.operator.hasNext()) {
        assertFalse(f.operator.getRuntimeWorkProgress().isPresent());
        f.operator.next();
      }
      assertFalse(f.operator.getRuntimeWorkProgress().isPresent());
      assertEquals(2, f.leaseRequests);
    }
  }

  @Test
  public void testNegativeUnknownOrMismatchedDeviceCountDoesNotAllocateCounter() throws Exception {
    for (int declared : new int[] {-1, 0, 7}) {
      try (Fixture f = new Fixture(true, false, declared, plainOptions(), entries(0, 3))) {
        assertFalse(f.operator.getRuntimeWorkProgress().isPresent());
        f.operator.close();
        assertFalse(f.operator.getRuntimeWorkProgress().isPresent());
      }
    }
  }

  private static void assertProgress(TestScan operator, long total, long completed) {
    RuntimeScanProgress.Snapshot snapshot = operator.getRuntimeWorkProgress().get();
    assertEquals(total, snapshot.total());
    assertEquals(completed, snapshot.completed());
    assertTrue(snapshot.pruned() >= 0);
    assertTrue(snapshot.pruned() <= snapshot.completed());
  }

  private static SeriesScanOptions plainOptions() {
    return new SeriesScanOptions.Builder().build();
  }

  private static QueryDataSource emptyDataSource() {
    return new QueryDataSource(Collections.emptyList(), Collections.emptyList());
  }

  private static QueryDataSource fullyPrunedDataSource() {
    QueryDataSource dataSource = emptyDataSource();
    dataSource.initRuntimeFilterTracking();
    assertFalse(dataSource.hasValidResource());
    return dataSource;
  }

  private static List<DeviceEntry> entries(int first, int count) {
    List<DeviceEntry> result = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      result.add(
          new AlignedDeviceEntry(
              new StringArrayDeviceID(new String[] {"runtime_table", "device-" + (first + i)}),
              new Binary[0]));
    }
    return result;
  }

  private static final class TestScan extends TableScanOperator {
    private TestScan(AbstractTableScanOperatorParameter parameter) {
      super(parameter);
    }

    @Override
    String getNthIdColumnValue(DeviceEntry deviceEntry, int idColumnIndex) {
      return (String) deviceEntry.getNthSegment(idColumnIndex + 1);
    }

    @Override
    protected boolean readPageData() {
      return false;
    }

    @Override
    protected Optional<Boolean> readChunkData() {
      return Optional.of(false);
    }

    @Override
    protected Optional<Boolean> readFileData() {
      return Optional.of(false);
    }

    @Override
    protected void constructAlignedSeriesScanUtil() {
      // Storage traversal is outside this test; production next/move/release remain unchanged.
      seriesScanUtil = mock(AlignedSeriesScanUtil.class);
    }

    private void advanceDevice() {
      moveToNextDevice();
    }

    private int deviceIndex() {
      return currentDeviceIndex;
    }

    private int batchDeviceCount() {
      return deviceCount;
    }

    private void attachDataSource(QueryDataSource dataSource) {
      queryDataSource = dataSource;
    }

    private boolean batchDataSourceReleased() {
      return queryDataSource == null;
    }
  }

  private static final class BatchSource implements BatchDeviceEntrySource {
    private final List<List<DeviceEntry>> batches;
    private int position;
    private int nextCalls;
    private boolean closed;

    private BatchSource(List<List<DeviceEntry>> batches) {
      this.batches = batches;
    }

    @Override
    public boolean hasNextBatch() {
      return !closed && position < batches.size();
    }

    @Override
    public List<DeviceEntry> nextBatch() {
      nextCalls++;
      return batches.get(position++);
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final List<QueryDataSourceLease> leases = new ArrayList<>();
    private final BatchSource batchSource;
    private final BatchDeviceEntrySource deviceEntrySource;
    private final TestScan operator;
    private int leaseRequests;

    @SafeVarargs
    private Fixture(
        boolean runtime,
        boolean batched,
        int declaredTotal,
        SeriesScanOptions options,
        List<DeviceEntry>... batches)
        throws Exception {
      FragmentInstanceId id =
          new FragmentInstanceId(new PlanFragmentId(new QueryId("runtime_progress"), 0), "0");
      FragmentInstanceContext instance = mock(FragmentInstanceContext.class);
      when(instance.getId()).thenReturn(id);
      if (runtime) {
        when(instance.getRuntimeLifecycle())
            .thenReturn(new RuntimeFragmentLifecycle(instance, MoreExecutors.directExecutor()));
      }
      FragmentInstanceContext leaseOwner =
          FragmentInstanceContext.createFragmentInstanceContext(
              id, mock(FragmentInstanceStateMachine.class), mock(MemoryReservationManager.class));
      when(instance.initBatchQueryDataSource(anyList()))
          .thenAnswer(
              invocation -> {
                QueryDataSourceLease lease =
                    leaseOwner.initBatchQueryDataSource(Collections.emptyList());
                leases.add(lease);
                leaseRequests++;
                return lease;
              });
      PlanNodeId sourceId = new PlanNodeId("runtime-progress-scan");
      OperatorContext context =
          new OperatorContext(0, sourceId, TestScan.class.getSimpleName(), instance);
      List<DeviceEntry> inMemory = batches.length == 1 ? batches[0] : Collections.emptyList();
      if (batches.length == 1) {
        batchSource = null;
        deviceEntrySource = new InMemoryDeviceEntrySource(inMemory);
      } else {
        batchSource = new BatchSource(Arrays.asList(batches));
        deviceEntrySource = batchSource;
      }
      operator =
          new TestScan(
              new AbstractTableScanOperatorParameter(
                  Collections.singleton("s0"),
                  context,
                  sourceId,
                  Collections.singletonList(
                      new ColumnSchema(
                          "s0",
                          TypeFactory.getType(TSDataType.INT64),
                          false,
                          TsTableColumnCategory.FIELD)),
                  new int[] {0},
                  inMemory,
                  declaredTotal,
                  deviceEntrySource,
                  batched,
                  Ordering.ASC,
                  options,
                  Collections.singletonList("s0"),
                  Collections.singletonList(new MeasurementSchema("s0", TSDataType.INT64)),
                  64));
    }

    @Override
    public void close() throws Exception {
      try {
        operator.close();
      } finally {
        for (QueryDataSourceLease lease : leases) {
          lease.close();
        }
      }
    }
  }
}
