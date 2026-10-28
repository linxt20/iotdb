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

package org.apache.iotdb.db.queryengine.plan.relational.planner.distribute;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.AssignUniqueId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.EnforceSingleRowNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.GapFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.GroupNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.LinearFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.MarkDistinctNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.NextFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.PreviousFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.RowNumberNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SemiJoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.StreamSortNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.TopKRankingNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.UnionNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValueFillNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValuesNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.WindowNode;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AlignedAggregationTreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.CopyToNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExchangeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExplainAnalyzeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExternalTsFileAggregationScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExternalTsFileScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.InformationSchemaTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.IntoNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.NonAlignedAggregationTreeDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableDiskUsageInformationSchemaTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableHashPartitioningShuffleSinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeAlignedDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TreeNonAlignedDeviceViewScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceFetchNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryCountNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryScanNode;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/** Reads physical output evidence without consulting the parent's requirement or changing nodes. */
public class PlanPropertyProvider {
  // Exact classes make a newly introduced node or subclass require explicit coverage.
  private static final Set<Class<?>> KNOWN_TYPES =
      Set.of(
          AggregationNode.class,
          AssignUniqueId.class,
          CollectNode.class,
          EnforceSingleRowNode.class,
          FilterNode.class,
          GapFillNode.class,
          GroupNode.class,
          JoinNode.class,
          LimitNode.class,
          MarkDistinctNode.class,
          MergeSortNode.class,
          OffsetNode.class,
          OutputNode.class,
          PatternRecognitionNode.class,
          ProjectNode.class,
          RowNumberNode.class,
          SemiJoinNode.class,
          SortNode.class,
          StreamSortNode.class,
          TableFunctionProcessorNode.class,
          TopKNode.class,
          TopKRankingNode.class,
          UnionNode.class,
          ValueFillNode.class,
          LinearFillNode.class,
          PreviousFillNode.class,
          NextFillNode.class,
          ValuesNode.class,
          WindowNode.class,
          AggregationTableScanNode.class,
          AlignedAggregationTreeDeviceViewScanNode.class,
          NonAlignedAggregationTreeDeviceViewScanNode.class,
          CopyToNode.class,
          DeviceTableScanNode.class,
          ExchangeNode.class,
          ExplainAnalyzeNode.class,
          ExternalTsFileAggregationScanNode.class,
          ExternalTsFileScanNode.class,
          InformationSchemaTableScanNode.class,
          IntoNode.class,
          TableDiskUsageInformationSchemaTableScanNode.class,
          TableHashPartitioningShuffleSinkNode.class,
          TreeAlignedDeviceViewScanNode.class,
          TreeNonAlignedDeviceViewScanNode.class,
          TableDeviceQueryScanNode.class,
          TableDeviceQueryCountNode.class,
          TableDeviceFetchNode.class);

  public enum Coverage {
    KNOWN,
    UNKNOWN
  }

  public static final class ProvidedProperties {
    private final PlanProperties properties;
    private final Coverage coverage;

    private ProvidedProperties(PlanProperties properties, Coverage coverage) {
      this.properties = properties;
      this.coverage = coverage;
    }

    public PlanProperties getProperties() {
      return properties;
    }

    public Coverage getCoverage() {
      return coverage;
    }

    @Nullable
    public FallbackReason getFallbackReason() {
      return coverage == Coverage.UNKNOWN ? FallbackReason.UNKNOWN_PROVIDER : null;
    }
  }

  public ProvidedProperties providedBy(
      PlanNode node, Map<PlanNodeId, OrderingScheme> recordedOrderings) {
    if (!KNOWN_TYPES.contains(node.getClass())) {
      return new ProvidedProperties(PlanProperties.any(), Coverage.UNKNOWN);
    }
    OrderingScheme recorded = recordedOrderings.get(node.getPlanNodeId());
    OrderingScheme intrinsic = null;
    if (node instanceof SortNode) {
      intrinsic = ((SortNode) node).getOrderingScheme();
    } else if (node instanceof MergeSortNode) {
      intrinsic = ((MergeSortNode) node).getOrderingScheme();
    } else if (node instanceof TopKNode) {
      intrinsic = ((TopKNode) node).getOrderingScheme();
    }
    // A Collect interleaves its inputs, so even a stale map entry cannot prove ordering.
    OrderingScheme ordering = node instanceof CollectNode ? null : recorded;
    if (intrinsic != null) {
      ordering = recorded == null || recorded.equals(intrinsic) ? intrinsic : null;
    }
    if (ordering != null && !node.getOutputSymbols().containsAll(ordering.getOrderBy())) {
      ordering = null;
    }
    return new ProvidedProperties(
        PlanProperties.of(DistributionProperty.single(), ordering), Coverage.KNOWN);
  }

  public ProvidedProperties providedByBranches(
      List<PlanNode> branches,
      @Nullable OrderingScheme observedOrdering,
      Map<PlanNodeId, OrderingScheme> recordedOrderings) {
    checkArgument(
        branches != null,
        DataNodeQueryMessages.EXCEPTION_CHILDRENNODES_SHOULD_NOT_BE_NULL_DOT_0C93B063);
    checkArgument(
        !branches.isEmpty(),
        DataNodeQueryMessages.EXCEPTION_CHILDRENNODES_SHOULD_NOT_BE_EMPTY_DOT_E5555FD9);
    boolean everyBranchOrdered = observedOrdering != null;
    boolean known = true;
    for (PlanNode branch : branches) {
      ProvidedProperties output = providedBy(branch, recordedOrderings);
      known &= output.coverage == Coverage.KNOWN;
      // The explicit observation and every physical map entry must agree, as in the baseline.
      everyBranchOrdered &=
          observedOrdering != null
              && observedOrdering.equals(recordedOrderings.get(branch.getPlanNodeId()))
              && observedOrdering.equals(output.properties.getOrdering());
    }
    if (!known) {
      return new ProvidedProperties(PlanProperties.any(), Coverage.UNKNOWN);
    }
    return new ProvidedProperties(
        PlanProperties.of(
            branches.size() == 1 ? DistributionProperty.single() : DistributionProperty.arbitrary(),
            everyBranchOrdered ? observedOrdering : null),
        Coverage.KNOWN);
  }
}
