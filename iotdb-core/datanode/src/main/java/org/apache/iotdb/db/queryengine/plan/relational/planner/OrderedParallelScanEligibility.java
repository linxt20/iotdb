/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.calc.utils.datastructure.SortKey;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.iotdb.calc.execution.operator.process.join.merge.MergeSortComparator.getComparatorForTable;
import static org.apache.iotdb.commons.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;

/** Shared proof for retaining and consuming an explicit ordered-scan execution marker. */
public final class OrderedParallelScanEligibility {

  private OrderedParallelScanEligibility() {}

  /**
   * A split device scan is mergeable only for per-device constant keys followed by scan-time order.
   * This checks structural safety; the local planner still checks its available DOP and resource
   * budget. It deliberately derives nothing from allowParallelScan.
   */
  public static boolean canUseLocalMergeTree(PlanNode source, OrderingScheme orderingScheme) {
    // Aggregation, tree-view and external-file scan subclasses have different execution contracts.
    if (source == null
        || source.getClass() != DeviceTableScanNode.class
        || orderingScheme == null) {
      return false;
    }
    DeviceTableScanNode node = (DeviceTableScanNode) source;
    boolean hasGlobalPagination =
        (node.getPushDownLimit() > 0 || node.getPushDownOffset() > 0)
            && !node.isPushLimitToEachDevice();
    if (hasGlobalPagination
        || node.getDeviceEntryDataSetHandle().isPresent()
        || node.getDeviceEntries() == null
        || node.getDeviceEntries().size() <= 1) {
      return false;
    }

    List<Symbol> orderBy = orderingScheme.getOrderBy();
    if (orderBy.isEmpty()) {
      return false;
    }
    for (int i = 0; i < orderBy.size(); i++) {
      Symbol symbol = orderBy.get(i);
      ColumnSchema column = node.getAssignments().get(symbol);
      if (column == null || !node.getOutputSymbols().contains(symbol)) {
        return false;
      }
      if (column.getColumnCategory() == TsTableColumnCategory.TIME) {
        return i == orderBy.size() - 1
            && (orderingScheme.getOrdering(symbol).isAscending()
                == (node.getScanOrder() == Ordering.ASC));
      }
      if (column.getColumnCategory() != TsTableColumnCategory.TAG
          && column.getColumnCategory() != TsTableColumnCategory.ATTRIBUTE) {
        return false;
      }
    }
    return false;
  }

  /**
   * Prove that the concrete serial scan already satisfies the whole explicit order. Every adjacent
   * pair must have strictly increasing constant-key tuples under the SQL comparator. Equal tuples,
   * or time-only ordering, cannot prove the order across devices. This proof is independent of the
   * marker's origin and survives fragment serialization.
   */
  public static boolean canUseSerialConcatenation(PlanNode source, OrderingScheme orderingScheme) {
    if (!canUseLocalMergeTree(source, orderingScheme)) return false;
    DeviceTableScanNode scan = (DeviceTableScanNode) source;
    int prefixSize = orderingScheme.getOrderBy().size() - 1;
    if (prefixSize == 0 || scan.getTagAndAttributeIndexMap() == null) return false;
    List<Symbol> prefix = orderingScheme.getOrderBy().subList(0, prefixSize);
    List<TSDataType> types = new ArrayList<>(prefixSize);
    List<Integer> positions = new ArrayList<>(prefixSize);
    for (int i = 0; i < prefixSize; i++) {
      Symbol symbol = prefix.get(i);
      Integer metadataIndex = scan.getTagAndAttributeIndexMap().get(symbol);
      if (metadataIndex == null || metadataIndex < 0) return false;
      TSDataType type = getTSDataType(scan.getAssignments().get(symbol).getType());
      // TAG and ATTRIBUTE are binary string values in the table scan.
      if (type != TSDataType.STRING && type != TSDataType.TEXT) return false;
      types.add(type);
      positions.add(i);
    }
    Comparator<SortKey> comparator =
        getComparatorForTable(
            orderingScheme.getOrderingList().subList(0, prefixSize), positions, types);
    TsBlockBuilder builder = new TsBlockBuilder(2, types);
    Binary[] previous = constantKeys(scan, prefix, scan.getDeviceEntries().get(0));
    if (previous == null) return false;
    for (int i = 1; i < scan.getDeviceEntries().size(); i++) {
      Binary[] current = constantKeys(scan, prefix, scan.getDeviceEntries().get(i));
      if (current == null) return false;
      builder.reset();
      appendKeys(builder, previous);
      appendKeys(builder, current);
      TsBlock pair = builder.build();
      if (comparator.compare(new SortKey(pair, 0), new SortKey(pair, 1)) >= 0) return false;
      previous = current;
    }
    return true;
  }

  private static Binary[] constantKeys(
      DeviceTableScanNode scan, List<Symbol> prefix, DeviceEntry device) {
    Binary[] keys = new Binary[prefix.size()];
    for (int i = 0; i < prefix.size(); i++) {
      Symbol symbol = prefix.get(i);
      int index = scan.getTagAndAttributeIndexMap().get(symbol);
      if (scan.getAssignments().get(symbol).getColumnCategory() == TsTableColumnCategory.TAG) {
        Object value = device.getNthSegment(index + 1);
        if (value != null && !(value instanceof String)) return null;
        keys[i] = value == null ? null : new Binary((String) value, TSFileConfig.STRING_CHARSET);
      } else {
        Binary[] attributes = device.getAttributeColumnValues();
        if (attributes == null || index >= attributes.length) return null;
        keys[i] = attributes[index];
      }
    }
    return keys;
  }

  private static void appendKeys(TsBlockBuilder builder, Binary[] keys) {
    builder.getTimeColumnBuilder().writeLong(0);
    for (int i = 0; i < keys.length; i++) {
      if (keys[i] == null) builder.getColumnBuilder(i).appendNull();
      else builder.getColumnBuilder(i).writeBinary(keys[i]);
    }
    builder.declarePosition();
  }
}
