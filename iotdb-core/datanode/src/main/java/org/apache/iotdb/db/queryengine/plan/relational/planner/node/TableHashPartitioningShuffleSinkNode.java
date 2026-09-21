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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.sink.ShuffleSinkNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.HashPartitioningDescriptor;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A table-model shuffle sink with a declarative hash-partitioning contract.
 *
 * <p>The table operator generator consumes {@link #partitioningDescriptor} row by row and routes
 * each bucket to its matching downstream channel. Planner selection is deliberately restricted to
 * the default-off, single-source GROUP BY experiment until the general N x N topology is ready.
 */
public class TableHashPartitioningShuffleSinkNode extends ShuffleSinkNode {

  private final HashPartitioningDescriptor partitioningDescriptor;

  public TableHashPartitioningShuffleSinkNode(
      PlanNodeId id, HashPartitioningDescriptor partitioningDescriptor) {
    super(id);
    this.partitioningDescriptor = Objects.requireNonNull(partitioningDescriptor);
  }

  public TableHashPartitioningShuffleSinkNode(
      PlanNodeId id,
      List<DownStreamChannelLocation> downStreamChannelLocationList,
      HashPartitioningDescriptor partitioningDescriptor) {
    super(id, downStreamChannelLocationList);
    this.partitioningDescriptor = Objects.requireNonNull(partitioningDescriptor);
    if (downStreamChannelLocationList.size() != partitioningDescriptor.getPartitionCount()) {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public PlanNodeType getType() {
    return PlanNodeType.TABLE_HASH_PARTITIONING_SHUFFLE_SINK_NODE;
  }

  @Override
  public PlanNode clone() {
    return new TableHashPartitioningShuffleSinkNode(
        getPlanNodeId(), getDownStreamChannelLocationList(), partitioningDescriptor);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    TableHashPartitioningShuffleSinkNode replacement =
        new TableHashPartitioningShuffleSinkNode(
            getPlanNodeId(), getDownStreamChannelLocationList(), partitioningDescriptor);
    newChildren.forEach(replacement::addChild);
    return replacement;
  }

  public HashPartitioningDescriptor getPartitioningDescriptor() {
    return partitioningDescriptor;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    validateChannelCount();
    PlanNodeType.TABLE_HASH_PARTITIONING_SHUFFLE_SINK_NODE.serialize(byteBuffer);
    ReadWriteIOUtils.write(getDownStreamChannelLocationList().size(), byteBuffer);
    for (DownStreamChannelLocation downStreamChannelLocation : getDownStreamChannelLocationList()) {
      downStreamChannelLocation.serialize(byteBuffer);
    }
    partitioningDescriptor.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    validateChannelCount();
    PlanNodeType.TABLE_HASH_PARTITIONING_SHUFFLE_SINK_NODE.serialize(stream);
    ReadWriteIOUtils.write(getDownStreamChannelLocationList().size(), stream);
    for (DownStreamChannelLocation downStreamChannelLocation : getDownStreamChannelLocationList()) {
      downStreamChannelLocation.serialize(stream);
    }
    partitioningDescriptor.serialize(stream);
  }

  public static TableHashPartitioningShuffleSinkNode deserialize(ByteBuffer byteBuffer) {
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    List<DownStreamChannelLocation> downStreamChannelLocationList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      downStreamChannelLocationList.add(DownStreamChannelLocation.deserialize(byteBuffer));
    }
    HashPartitioningDescriptor partitioningDescriptor =
        HashPartitioningDescriptor.deserialize(byteBuffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(byteBuffer);
    return new TableHashPartitioningShuffleSinkNode(
        planNodeId, downStreamChannelLocationList, partitioningDescriptor);
  }

  private void validateChannelCount() {
    if (getDownStreamChannelLocationList().size() != partitioningDescriptor.getPartitionCount()) {
      throw new IllegalStateException();
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof TableHashPartitioningShuffleSinkNode) || !super.equals(obj)) {
      return false;
    }
    TableHashPartitioningShuffleSinkNode other = (TableHashPartitioningShuffleSinkNode) obj;
    return partitioningDescriptor.equals(other.partitioningDescriptor)
        && getDownStreamChannelLocationList().equals(other.getDownStreamChannelLocationList());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(), getDownStreamChannelLocationList(), partitioningDescriptor);
  }
}
