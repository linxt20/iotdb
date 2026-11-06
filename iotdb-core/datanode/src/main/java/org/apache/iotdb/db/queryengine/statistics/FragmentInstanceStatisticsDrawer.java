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

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TFetchFragmentInstanceStatisticsResp;
import org.apache.iotdb.mpp.rpc.thrift.TOperatorStatistics;
import org.apache.iotdb.mpp.rpc.thrift.TQueryStatistics;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FragmentInstanceStatisticsDrawer {
  private int maxLineLength = 0;
  private static final double EPSILON = 1e-10;
  private final List<StatisticLine> planHeader = new ArrayList<>();
  private static final double NS_TO_MS_FACTOR = 1.0 / 1000000;

  // Metric-tree values travel inside TOperatorStatistics.specifiedInfo so that no thrift schema
  // change is needed.  Keys carrying these synthetic values are prefixed to keep them apart from
  // operator-authored entries, and "__pipeline_" marks the per-driver snapshots of parallel
  // sub-scan operators.
  static final String INTERNAL_KEY_PREFIX = "__mt_";
  static final String TS_BLOCK_COUNT_KEY = INTERNAL_KEY_PREFIX + "tsBlockOutputCount";
  static final String PIPELINE_KEY_PREFIX = "__pipeline_";
  // Infix used by DataNodeTableOperatorGenerator when it splits one DeviceTableScanNode into
  // per-driver sub scans, producing planNodeIds of the form "<nodeId>-parallel-<i>".
  public static final String PARALLEL_SUB_SCAN_INFIX = "-parallel-";
  public static final String MORSEL_SUB_SCAN_INFIX = "-morsel-";

  public void renderPlanStatistics(MPPQueryContext context) {
    addLine(
        planHeader,
        0,
        String.format(
            DataNodeQueryMessages.EXCEPTION_ANALYZE_COST_ARG_MS_222A306D,
            context.getAnalyzeCost() * NS_TO_MS_FACTOR));
    addLine(
        planHeader,
        0,
        String.format(
            DataNodeQueryMessages.EXCEPTION_FETCH_PARTITION_COST_ARG_MS_67861DDF,
            context.getFetchPartitionCost() * NS_TO_MS_FACTOR));
    addLine(
        planHeader,
        0,
        String.format(
            DataNodeQueryMessages.EXCEPTION_FETCH_SCHEMA_COST_ARG_MS_67816EA5,
            context.getFetchSchemaCost() * NS_TO_MS_FACTOR));
    addLine(
        planHeader,
        1,
        String.format(
            DataNodeQueryMessages.EXCEPTION_DISK_IO_SIZE_ARG_BYTES_CD6872DC,
            context.getDiskIOSizeForDeviceEntryDuringFetchSchema()));
    addLine(
        planHeader,
        1,
        String.format(
            DataNodeQueryMessages.EXCEPTION_DISK_IO_TIME_COST_ARG_MS_36FC3B10,
            context.getDiskIOTimeCostForDeviceEntryDuringFetchSchema() * NS_TO_MS_FACTOR));
    addLine(
        planHeader,
        1,
        String.format(
            DataNodeQueryMessages.EXCEPTION_DEVICEENTRY_COUNT_ARG_3CF4F4B6,
            context.getDeviceEntryCount()));
    addLine(
        planHeader,
        0,
        String.format(
            DataNodeQueryMessages.EXCEPTION_LOGICAL_PLAN_COST_ARG_MS_1A40B609,
            context.getLogicalPlanCost() * NS_TO_MS_FACTOR));
    addLine(
        planHeader,
        0,
        String.format(
            DataNodeQueryMessages.EXCEPTION_LOGICAL_OPTIMIZATION_COST_ARG_MS_1D48CF38,
            context.getLogicalOptimizationCost() * NS_TO_MS_FACTOR));
    addLine(
        planHeader,
        0,
        String.format(
            DataNodeQueryMessages.EXCEPTION_DISTRIBUTION_PLAN_COST_ARG_MS_EE261BB5,
            context.getDistributionPlanCost() * NS_TO_MS_FACTOR));
    addLine(
        planHeader,
        1,
        String.format(
            DataNodeQueryMessages.EXCEPTION_DISK_IO_SIZE_ARG_BYTES_CD6872DC,
            context.getDiskIOSizeForDeviceEntryDuringDistributionPlan()));
    addLine(
        planHeader,
        1,
        String.format(
            DataNodeQueryMessages.EXCEPTION_DISK_IO_TIME_COST_ARG_MS_36FC3B10,
            context.getDiskIOTimeCostForDeviceEntryDuringDistributionPlan() * NS_TO_MS_FACTOR));
  }

  public void renderDispatchCost(MPPQueryContext context) {
    addLine(
        planHeader,
        0,
        String.format(
            DataNodeQueryMessages.EXCEPTION_DISPATCH_COST_ARG_MS_E2A08F90,
            context.getDispatchCost() * NS_TO_MS_FACTOR));
  }

  public List<StatisticLine> renderFragmentInstances(
      List<FragmentInstance> instancesToBeRendered,
      Map<FragmentInstanceId, TFetchFragmentInstanceStatisticsResp> allStatistics,
      boolean verbose) {
    List<StatisticLine> table = new ArrayList<>(planHeader);
    List<FragmentInstance> validInstances =
        instancesToBeRendered.stream()
            .filter(
                instance -> {
                  TFetchFragmentInstanceStatisticsResp statistics =
                      allStatistics.get(instance.getId());
                  return statistics != null && statistics.getDataRegion() != null;
                })
            .collect(Collectors.toList());

    addLine(
        table,
        0,
        String.format(
            DataNodeQueryMessages.EXCEPTION_FRAGMENT_INSTANCES_COUNT_ARG_2B7FA76E,
            validInstances.size()));
    for (FragmentInstance instance : validInstances) {
      List<StatisticLine> singleFragmentInstanceArea = new ArrayList<>();
      TFetchFragmentInstanceStatisticsResp statistics = allStatistics.get(instance.getId());

      addBlankLine(singleFragmentInstanceArea);
      addLine(
          singleFragmentInstanceArea,
          0,
          String.format(
              DataNodeQueryMessages
                  .EXCEPTION_FRAGMENT_INSTANCE_ID_ARG_IP_ARG_DATAREGION_ARG_STATE_ARG_32F467DE,
              instance.getId().toString(),
              statistics.getIp(),
              statistics.getDataRegion(),
              statistics.getState()));
      addLine(
          singleFragmentInstanceArea,
          1,
          String.format(
              DataNodeQueryMessages.EXCEPTION_TOTAL_WALL_TIME_ARG_MS_F65FF0B0,
              (statistics.getEndTimeInMS() - statistics.getStartTimeInMS())));
      addLine(
          singleFragmentInstanceArea,
          1,
          String.format(
              DataNodeQueryMessages.EXCEPTION_COST_OF_INITDATAQUERYSOURCE_ARG_MS_55A2E155,
              statistics.getInitDataQuerySourceCost() * NS_TO_MS_FACTOR));

      if (statistics.isSetInitDataQuerySourceRetryCount()
          && statistics.getInitDataQuerySourceRetryCount() > 0) {
        addLine(
            singleFragmentInstanceArea,
            1,
            String.format(
                DataNodeQueryMessages.EXCEPTION_RETRY_COUNT_OF_INITDATAQUERYSOURCE_ARG_70E5C772,
                statistics.getInitDataQuerySourceRetryCount()));
      }

      addLine(
          singleFragmentInstanceArea,
          1,
          String.format(
              DataNodeQueryMessages.EXCEPTION_SEQ_FILE_UNCLOSED_ARG_SEQ_FILE_CLOSED_ARG_CBB3422D,
              statistics.getSeqUnclosedNum(),
              statistics.getSeqClosednNum()));
      addLine(
          singleFragmentInstanceArea,
          1,
          String.format(
              DataNodeQueryMessages
                  .EXCEPTION_UNSEQ_FILE_UNCLOSED_ARG_UNSEQ_FILE_CLOSED_ARG_61850A9D,
              statistics.getUnseqUnclosedNum(),
              statistics.getUnseqClosedNum()));
      addLine(
          singleFragmentInstanceArea,
          1,
          String.format(
              DataNodeQueryMessages
                  .EXCEPTION_READY_QUEUED_TIME_ARG_MS_BLOCKED_QUEUED_TIME_ARG_MS_24FB1E4C,
              statistics.getReadyQueuedTime() * NS_TO_MS_FACTOR,
              statistics.getBlockQueuedTime() * NS_TO_MS_FACTOR));

      // FI-level blocking ratio: the fraction of scheduling time the drivers spent blocked rather
      // than ready, i.e. how much of their wait was backpressure instead of plain queueing.
      // Rendered only when blocking actually happened — a "0.0%" line would be noise on every
      // fragment instance and would just restate the "blocked queued time: 0.000 ms" line above.
      if (statistics.getBlockQueuedTime() > 0) {
        long totalQueueTime = statistics.getReadyQueuedTime() + statistics.getBlockQueuedTime();
        double blockRatio = (double) statistics.getBlockQueuedTime() / totalQueueTime * 100.0;
        addLine(singleFragmentInstanceArea, 1, String.format("blocking ratio: %.1f%%", blockRatio));
      }

      renderRuntimeSnapshot(statistics, singleFragmentInstanceArea);
      renderQueryStatistics(statistics.getQueryStatistics(), singleFragmentInstanceArea, verbose);

      // render operator
      PlanNode planNodeTree = instance.getFragment().getPlanNodeTree();
      renderOperator(
          planNodeTree, statistics.getOperatorStatisticsMap(), singleFragmentInstanceArea, 2);

      // render parallel sub-scan pipelines.  These are keyed as "__pipeline_<nodeId>-parallel-<i>"
      // in the map (stashed before mergeOperatorStatisticsIfDuplicate removed the hyphenated keys)
      // so the tree walker above cannot reach them.  Expand them here at the same indent level so
      // the metric tree shows each pipeline's stats individually.
      renderParallelSubScans(statistics.getOperatorStatisticsMap(), singleFragmentInstanceArea);

      table.addAll(singleFragmentInstanceArea);
    }

    return table;
  }

  private void renderRuntimeSnapshot(
      TFetchFragmentInstanceStatisticsResp statistics, List<StatisticLine> area) {
    if (!statistics.isSetSnapshotGeneration() && !statistics.isSetDriverSnapshots()) {
      return;
    }
    addLine(area, 1, "Runtime Snapshot:");
    if (statistics.isSetSchemaVersion()) {
      addLine(area, 2, "schemaVersion: " + statistics.getSchemaVersion());
    }
    if (statistics.isSetSnapshotGeneration()) {
      addLine(area, 2, "generation: " + statistics.getSnapshotGeneration());
    }
    if (statistics.isSetPlannedDop()) {
      addLine(area, 2, "plannedDop: " + statistics.getPlannedDop());
    }
    if (statistics.isSetActualPipelineCount()) {
      addLine(area, 2, "actualPipelineCount: " + statistics.getActualPipelineCount());
    }
    if (statistics.isSetSplitMode()) {
      addLine(area, 2, "splitMode: " + statistics.getSplitMode());
    }
    if (statistics.isSetSplitReason()) {
      addLine(area, 2, "splitReason: " + statistics.getSplitReason());
    }
    if (statistics.isSetPlanIdentityChecksum()) {
      addLine(area, 2, "planIdentityChecksum: " + statistics.getPlanIdentityChecksum());
    }
    if (statistics.isSetWorkUnitIds()) {
      addLine(area, 2, "workUnitIds: " + statistics.getWorkUnitIds());
    }
    if (statistics.isSetBucketIds()) {
      addLine(area, 2, "bucketIds: " + statistics.getBucketIds());
    }
    if (statistics.isSetExchangeEdgeIds()) {
      addLine(area, 2, "exchangeEdgeIds: " + statistics.getExchangeEdgeIds());
    }
    if (statistics.isSetCanonicalFallbackReason()) {
      addLine(area, 2, "canonicalFallbackReason: " + statistics.getCanonicalFallbackReason());
    }
    if (statistics.isSetAdmissionStatus()) {
      addLine(area, 2, "admissionStatus: " + statistics.getAdmissionStatus());
    }
    if (statistics.isSetAdmissionReason()) {
      addLine(area, 2, "admissionReason: " + statistics.getAdmissionReason());
    }
    if (statistics.isSetResourceObjectCount()) {
      addLine(area, 2, "resourceObjectCount: " + statistics.getResourceObjectCount());
    }
    if (statistics.isSetResourceUniqueMemoryKeyCount()) {
      addLine(
          area, 2, "resourceUniqueMemoryKeyCount: " + statistics.getResourceUniqueMemoryKeyCount());
    }
    if (statistics.isSetResourceNominalShareBytes()) {
      addLine(area, 2, "resourceNominalShareBytes: " + statistics.getResourceNominalShareBytes());
    }
    if (statistics.isSetSnapshotChecksum()) {
      addLine(area, 2, "checksum: " + statistics.getSnapshotChecksum());
    }
    if (statistics.isSetValidityReasons()) {
      addLine(area, 2, "validity: " + statistics.getValidityReasons());
    }
    if (statistics.isSetDriverSnapshots()) {
      statistics.getDriverSnapshots().stream()
          .sorted(
              java.util.Comparator.comparing(
                      org.apache.iotdb.mpp.rpc.thrift.TDriverRuntimeSnapshot::getFragmentInstanceId)
                  .thenComparing(
                      org.apache.iotdb.mpp.rpc.thrift.TDriverRuntimeSnapshot::getDriverPipelineId)
                  .thenComparing(
                      org.apache.iotdb.mpp.rpc.thrift.TDriverRuntimeSnapshot::getDriverTaskId))
          .forEach(
              driver -> {
                addLine(
                    area,
                    2,
                    String.format(
                        "DRIVER[id=%s][pipeline=%s][role=%s]",
                        driver.getDriverTaskId(),
                        driver.getDriverPipelineId(),
                        driver.isSetDriverRole() ? driver.getDriverRole() : "unknown"));
                if (driver.isSetDriverWallNanos()) {
                  addLine(area, 3, "driverWallNanos: " + driver.getDriverWallNanos());
                }
                if (driver.isSetDriverOutputRows()) {
                  addLine(area, 3, "driverOutputRows: " + driver.getDriverOutputRows());
                }
                if (driver.isSetDriverOutputBytes()) {
                  addLine(area, 3, "driverOutputBytes: " + driver.getDriverOutputBytes());
                }
                if (driver.isSetDriverTsBlockCount()) {
                  addLine(area, 3, "driverTsBlocks: " + driver.getDriverTsBlockCount());
                }
                if (driver.isSetValidityReasons()) {
                  addLine(area, 3, "validity: " + driver.getValidityReasons());
                }
              });
    }
  }

  private void addLineWithValueCheck(
      List<StatisticLine> singleFragmentInstanceArea, int level, String valueName, long value) {
    if (value != 0) {
      addLine(singleFragmentInstanceArea, level, valueName + String.format(": %s", value));
    }
  }

  private void addLineWithoutValueCheck(
      List<StatisticLine> singleFragmentInstanceArea, int level, String valueName, long value) {
    addLine(singleFragmentInstanceArea, level, valueName + String.format(": %s", value));
  }

  private void addLineWithValueCheck(
      List<StatisticLine> singleFragmentInstanceArea, int level, String valueName, double value) {
    if (Math.abs(value) > EPSILON) {
      addLine(singleFragmentInstanceArea, level, valueName + String.format(": %.3f", value));
    }
  }

  private void addLineWithoutValueCheck(
      List<StatisticLine> singleFragmentInstanceArea, int level, String valueName, double value) {
    addLine(singleFragmentInstanceArea, level, valueName + String.format(": %.3f", value));
  }

  private void addBlankLine(List<StatisticLine> singleFragmentInstanceArea) {
    addLine(singleFragmentInstanceArea, 0, " ");
  }

  private void renderQueryStatistics(
      TQueryStatistics queryStatistics,
      List<StatisticLine> singleFragmentInstanceArea,
      boolean verbose) {
    addLine(
        singleFragmentInstanceArea, 1, DataNodeQueryMessages.EXCEPTION_QUERY_STATISTICS_0F0FEAC4);

    if (verbose) {
      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadBloomFilterFromCacheCount",
          queryStatistics.loadBloomFilterFromCacheCount);
      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadBloomFilterFromDiskCount",
          queryStatistics.loadBloomFilterFromDiskCount);
      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadBloomFilterActualIOSize",
          queryStatistics.loadBloomFilterActualIOSize);
      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadBloomFilterTime",
          queryStatistics.loadBloomFilterTime * NS_TO_MS_FACTOR);

      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataDiskSeqCount",
          queryStatistics.loadTimeSeriesMetadataDiskSeqCount);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataDiskUnSeqCount",
          queryStatistics.loadTimeSeriesMetadataDiskUnSeqCount);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataMemSeqCount",
          queryStatistics.loadTimeSeriesMetadataMemSeqCount);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataMemUnSeqCount",
          queryStatistics.loadTimeSeriesMetadataMemUnSeqCount);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataAlignedDiskSeqCount",
          queryStatistics.loadTimeSeriesMetadataAlignedDiskSeqCount);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataAlignedDiskUnSeqCount",
          queryStatistics.loadTimeSeriesMetadataAlignedDiskUnSeqCount);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataAlignedMemSeqCount",
          queryStatistics.loadTimeSeriesMetadataAlignedMemSeqCount);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataAlignedMemUnSeqCount",
          queryStatistics.loadTimeSeriesMetadataAlignedMemUnSeqCount);

      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataDiskSeqTime",
          queryStatistics.loadTimeSeriesMetadataDiskSeqTime * NS_TO_MS_FACTOR);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataDiskUnSeqTime",
          queryStatistics.loadTimeSeriesMetadataDiskUnSeqTime * NS_TO_MS_FACTOR);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataMemSeqTime",
          queryStatistics.loadTimeSeriesMetadataMemSeqTime * NS_TO_MS_FACTOR);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataMemUnSeqTime",
          queryStatistics.loadTimeSeriesMetadataMemUnSeqTime * NS_TO_MS_FACTOR);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataAlignedDiskSeqTime",
          queryStatistics.loadTimeSeriesMetadataAlignedDiskSeqTime * NS_TO_MS_FACTOR);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataAlignedDiskUnSeqTime",
          queryStatistics.loadTimeSeriesMetadataAlignedDiskUnSeqTime * NS_TO_MS_FACTOR);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataAlignedMemSeqTime",
          queryStatistics.loadTimeSeriesMetadataAlignedMemSeqTime * NS_TO_MS_FACTOR);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataAlignedMemUnSeqTime",
          queryStatistics.loadTimeSeriesMetadataAlignedMemUnSeqTime * NS_TO_MS_FACTOR);

      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataFromCacheCount",
          queryStatistics.loadTimeSeriesMetadataFromCacheCount);
      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataFromDiskCount",
          queryStatistics.loadTimeSeriesMetadataFromDiskCount);
      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadTimeSeriesMetadataActualIOSize",
          queryStatistics.loadTimeSeriesMetadataActualIOSize);

      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "alignedTimeSeriesMetadataModificationCount",
          queryStatistics.getAlignedTimeSeriesMetadataModificationCount());
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "alignedTimeSeriesMetadataModificationTime",
          queryStatistics.getAlignedTimeSeriesMetadataModificationTime() * NS_TO_MS_FACTOR);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "nonAlignedTimeSeriesMetadataModificationCount",
          queryStatistics.getNonAlignedTimeSeriesMetadataModificationCount());
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "nonAlignedTimeSeriesMetadataModificationTime",
          queryStatistics.getNonAlignedTimeSeriesMetadataModificationTime() * NS_TO_MS_FACTOR);

      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "constructNonAlignedChunkReadersDiskCount",
          queryStatistics.constructNonAlignedChunkReadersDiskCount);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "constructNonAlignedChunkReadersMemCount",
          queryStatistics.constructNonAlignedChunkReadersMemCount);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "constructAlignedChunkReadersDiskCount",
          queryStatistics.constructAlignedChunkReadersDiskCount);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "constructAlignedChunkReadersMemCount",
          queryStatistics.constructAlignedChunkReadersMemCount);

      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "constructNonAlignedChunkReadersDiskTime",
          queryStatistics.constructNonAlignedChunkReadersDiskTime * NS_TO_MS_FACTOR);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "constructNonAlignedChunkReadersMemTime",
          queryStatistics.constructNonAlignedChunkReadersMemTime * NS_TO_MS_FACTOR);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "constructAlignedChunkReadersDiskTime",
          queryStatistics.constructAlignedChunkReadersDiskTime * NS_TO_MS_FACTOR);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "constructAlignedChunkReadersMemTime",
          queryStatistics.constructAlignedChunkReadersMemTime * NS_TO_MS_FACTOR);

      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadChunkFromCacheCount",
          queryStatistics.loadChunkFromCacheCount);
      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadChunkFromDiskCount",
          queryStatistics.loadChunkFromDiskCount);
      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "loadChunkActualIOSize",
          queryStatistics.loadChunkActualIOSize);

      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "pageReadersDecodeAlignedDiskCount",
          queryStatistics.pageReadersDecodeAlignedDiskCount);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "pageReadersDecodeAlignedDiskTime",
          queryStatistics.pageReadersDecodeAlignedDiskTime * NS_TO_MS_FACTOR);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "pageReadersDecodeAlignedMemCount",
          queryStatistics.pageReadersDecodeAlignedMemCount);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "pageReadersDecodeAlignedMemTime",
          queryStatistics.pageReadersDecodeAlignedMemTime * NS_TO_MS_FACTOR);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "pageReadersDecodeNonAlignedDiskCount",
          queryStatistics.pageReadersDecodeNonAlignedDiskCount);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "pageReadersDecodeNonAlignedDiskTime",
          queryStatistics.pageReadersDecodeNonAlignedDiskTime * NS_TO_MS_FACTOR);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "pageReadersDecodeNonAlignedMemCount",
          queryStatistics.pageReadersDecodeNonAlignedMemCount);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "pageReadersDecodeNonAlignedMemTime",
          queryStatistics.pageReadersDecodeNonAlignedMemTime * NS_TO_MS_FACTOR);
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "pageReaderMaxUsedMemorySize",
          queryStatistics.pageReaderMaxUsedMemorySize);

      addLineWithValueCheck(
          singleFragmentInstanceArea,
          2,
          "chunkWithMetadataErrorsCount",
          queryStatistics.chunkWithMetadataErrorsCount);
    }

    addLineWithoutValueCheck(
        singleFragmentInstanceArea,
        2,
        "timeSeriesIndexFilteredRows",
        queryStatistics.timeSeriesIndexFilteredRows);

    addLineWithoutValueCheck(
        singleFragmentInstanceArea,
        2,
        "chunkIndexFilteredRows",
        queryStatistics.chunkIndexFilteredRows);

    addLineWithoutValueCheck(
        singleFragmentInstanceArea,
        2,
        "pageIndexFilteredRows",
        queryStatistics.pageIndexFilteredRows);

    if (verbose) {
      addLineWithoutValueCheck(
          singleFragmentInstanceArea,
          2,
          "rowScanFilteredRows",
          queryStatistics.rowScanFilteredRows);
    }
  }

  private void addLine(List<StatisticLine> resultForSingleInstance, int level, String value) {
    maxLineLength = Math.max(maxLineLength, value.length());

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < level; i++) {
      sb.append("  ");
    }
    sb.append(value);
    maxLineLength = Math.max(maxLineLength, sb.length());
    resultForSingleInstance.add(new StatisticLine(sb.toString(), level));
  }

  private void renderOperator(
      PlanNode planNodeTree,
      Map<String, TOperatorStatistics> operatorStatistics,
      List<StatisticLine> singleFragmentInstanceArea,
      int indentNum) {
    if (planNodeTree == null) return;
    TOperatorStatistics operatorStatistic =
        operatorStatistics.get(planNodeTree.getPlanNodeId().toString());
    if (operatorStatistic != null) {
      addLine(
          singleFragmentInstanceArea,
          indentNum,
          String.format(
              "[PlanNodeId %s]: %s(%s) %s",
              planNodeTree.getPlanNodeId().toString(),
              planNodeTree.getClass().getSimpleName(),
              operatorStatistic.getOperatorType(),
              operatorStatistic.isSetCount() ? "Count: * " + operatorStatistic.getCount() : ""));
      addLine(
          singleFragmentInstanceArea,
          indentNum + 2,
          String.format(
              DataNodeQueryMessages.EXCEPTION_CPU_TIME_ARG_MS_42555E8C,
              operatorStatistic.getTotalExecutionTimeInNanos() * NS_TO_MS_FACTOR));
      addLine(
          singleFragmentInstanceArea,
          indentNum + 2,
          String.format(
              DataNodeQueryMessages.EXCEPTION_OUTPUT_ARG_ROWS_5EE0EF28,
              operatorStatistic.getOutputRows()));

      // Throughput: rows per second based on CPU time. Shows how fast the operator produces
      // rows when actually executing, which highlights bottlenecks in a parallel plan.
      double cpuTimeMs = operatorStatistic.getTotalExecutionTimeInNanos() * NS_TO_MS_FACTOR;
      if (cpuTimeMs > EPSILON && operatorStatistic.getOutputRows() > 0) {
        double throughput = operatorStatistic.getOutputRows() / (cpuTimeMs / 1000.0);
        addLine(
            singleFragmentInstanceArea,
            indentNum + 2,
            String.format("throughput: %.1f rows/s", throughput));
      }

      addLine(
          singleFragmentInstanceArea,
          indentNum + 2,
          String.format(
              DataNodeQueryMessages.EXCEPTION_HASNEXT_CALLED_COUNT_ARG_32D7AEBA,
              operatorStatistic.hasNextCalledCount));
      addLine(
          singleFragmentInstanceArea,
          indentNum + 2,
          String.format(
              DataNodeQueryMessages.EXCEPTION_NEXT_CALLED_COUNT_ARG_1AD71747,
              operatorStatistic.nextCalledCount));
      addLineWithValueCheck(
          singleFragmentInstanceArea,
          indentNum + 2,
          DataNodeQueryMessages.EXCEPTION_ESTIMATED_MEMORY_SIZE_27D7E3B5,
          operatorStatistic.getMemoryUsage());

      if (operatorStatistic.getSpecifiedInfoSize() != 0) {
        Map<String, String> specifiedInfo = operatorStatistic.getSpecifiedInfo();
        // Metric-tree fields are transported inside specifiedInfo (no thrift schema change) but
        // are rendered as first-class lines rather than raw "__mt_"-prefixed keys.
        Long tsBlockCount = sumTsBlockOutputCount(specifiedInfo);
        if (tsBlockCount != null) {
          addLine(
              singleFragmentInstanceArea,
              indentNum + 2,
              String.format("TsBlock output count: %d", tsBlockCount));
        }

        for (Map.Entry<String, String> entry : specifiedInfo.entrySet()) {
          if (entry.getKey().startsWith(INTERNAL_KEY_PREFIX)) {
            continue;
          }
          addLine(
              singleFragmentInstanceArea,
              indentNum + 2,
              String.format("%s: %s", entry.getKey(), entry.getValue()));
        }
      }
    }

    for (PlanNode child : planNodeTree.getChildren()) {
      renderOperator(child, operatorStatistics, singleFragmentInstanceArea, indentNum + 1);
    }
  }

  /**
   * Renders parallel sub-scan pipeline entries from the operator statistics map. These entries are
   * stashed under a {@code "__pipeline_"} prefix before {@code mergeOperatorStatisticsIfDuplicate}
   * removes the original hyphenated keys, so they survive the merge and reach this point intact.
   * Each entry displays per-pipeline metrics (output rows, CPU time, throughput, TsBlock count)
   * enabling observation of load balance across parallel scan drivers.
   */
  private void renderParallelSubScans(
      Map<String, TOperatorStatistics> operatorStatistics,
      List<StatisticLine> singleFragmentInstanceArea) {
    boolean hasParallel =
        operatorStatistics.keySet().stream().anyMatch(k -> k.startsWith(PIPELINE_KEY_PREFIX));
    if (!hasParallel) {
      return;
    }

    addBlankLine(singleFragmentInstanceArea);
    addLine(singleFragmentInstanceArea, 2, "Parallel Pipelines (per-driver breakdown):");

    // Sort by the original planNodeId so order is deterministic (e.g. parallel-0, parallel-1, …).
    operatorStatistics.entrySet().stream()
        .filter(e -> e.getKey().startsWith(PIPELINE_KEY_PREFIX))
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            entry -> {
              TOperatorStatistics op = entry.getValue();
              String displayKey =
                  entry
                      .getKey()
                      .substring(PIPELINE_KEY_PREFIX.length())
                      .replaceFirst("_(parallel|morsel)_(\\d+)$", "-$1-$2");
              addLine(
                  singleFragmentInstanceArea,
                  3,
                  String.format("[%s]: %s", displayKey, op.getOperatorType()));
              addLine(
                  singleFragmentInstanceArea,
                  4,
                  String.format(
                      "CPU Time: %.3f ms", op.getTotalExecutionTimeInNanos() * NS_TO_MS_FACTOR));
              addLine(
                  singleFragmentInstanceArea,
                  4,
                  String.format("output: %s rows", op.getOutputRows()));
              double cpuTimeMs = op.getTotalExecutionTimeInNanos() * NS_TO_MS_FACTOR;
              if (cpuTimeMs > EPSILON && op.getOutputRows() > 0) {
                double throughput = op.getOutputRows() / (cpuTimeMs / 1000.0);
                addLine(
                    singleFragmentInstanceArea,
                    4,
                    String.format("throughput: %.1f rows/s", throughput));
              }
              // Show TsBlock output count if present (injected via __mt_tsBlockOutputCount).
              if (op.getSpecifiedInfo() != null) {
                Long tsBlockCount = sumTsBlockOutputCount(op.getSpecifiedInfo());
                if (tsBlockCount != null) {
                  addLine(
                      singleFragmentInstanceArea,
                      4,
                      String.format("TsBlock output count: %d", tsBlockCount));
                }
              }
              // Show any user-visible specifiedInfo (skip internal __mt_ keys).
              if (op.getSpecifiedInfoSize() != 0) {
                for (Map.Entry<String, String> info : op.getSpecifiedInfo().entrySet()) {
                  if (!info.getKey().startsWith(INTERNAL_KEY_PREFIX)) {
                    addLine(
                        singleFragmentInstanceArea,
                        4,
                        String.format("%s: %s", info.getKey(), info.getValue()));
                  }
                }
              }
            });
  }

  /**
   * Sums the TsBlock output count carried in specifiedInfo, or returns {@code null} when absent.
   * The value may hold several space-separated numbers after operators of the same type are merged
   * (see {@code SpecifiedInfoMergerFactory}); the sum is what a merged operator should report. A
   * non-numeric value yields {@code null} instead of failing the rendering.
   */
  private static Long sumTsBlockOutputCount(Map<String, String> specifiedInfo) {
    String rawValue = specifiedInfo.get(TS_BLOCK_COUNT_KEY);
    if (rawValue == null || rawValue.isEmpty()) {
      return null;
    }
    long total = 0;
    for (String part : rawValue.trim().split("\s+")) {
      try {
        total += Long.parseLong(part);
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return total;
  }

  public int getMaxLineLength() {
    return maxLineLength;
  }
}
