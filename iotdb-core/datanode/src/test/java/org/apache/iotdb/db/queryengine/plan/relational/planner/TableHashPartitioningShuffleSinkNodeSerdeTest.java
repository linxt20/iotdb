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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.plan.planner.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.queryengine.plan.relational.planner.distribute.HashPartitioningDescriptor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableHashPartitioningShuffleSinkNode;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class TableHashPartitioningShuffleSinkNodeSerdeTest {

  @Test
  public void testSerializeAndDeserializeHashPartitioningContract() throws Exception {
    HashPartitioningDescriptor descriptor =
        new HashPartitioningDescriptor(
            List.of(Symbol.of("city"), Symbol.of("device")),
            HashPartitioningDescriptor.HashVersion.TABLE_HASH_V1,
            HashPartitioningDescriptor.NullRouting.HASH_NULL,
            2);
    TableHashPartitioningShuffleSinkNode node =
        new TableHashPartitioningShuffleSinkNode(
            new PlanNodeId("hash-shuffle"),
            List.of(location("downstream-0", 0), location("downstream-1", 1)),
            descriptor);

    ByteBuffer buffer = ByteBuffer.allocate(4096);
    node.serialize(buffer);
    buffer.flip();

    PlanNode deserialized = PlanNodeDeserializeHelper.deserialize(buffer);
    assertEquals(node.getType(), deserialized.getType());
    assertEquals(node.getPlanNodeId(), deserialized.getPlanNodeId());
    assertEquals(
        node.getDownStreamChannelLocationList().size(),
        ((TableHashPartitioningShuffleSinkNode) deserialized)
            .getDownStreamChannelLocationList()
            .size());
    for (int index = 0; index < node.getDownStreamChannelLocationList().size(); index++) {
      DownStreamChannelLocation expected = node.getDownStreamChannelLocationList().get(index);
      DownStreamChannelLocation actual =
          ((TableHashPartitioningShuffleSinkNode) deserialized)
              .getDownStreamChannelLocationList()
              .get(index);
      assertEquals(expected.getRemoteEndpoint().getIp(), actual.getRemoteEndpoint().getIp());
      assertEquals(expected.getRemoteEndpoint().getPort(), actual.getRemoteEndpoint().getPort());
      assertEquals(
          expected.getRemoteFragmentInstanceId().getQueryId(),
          actual.getRemoteFragmentInstanceId().getQueryId());
      assertEquals(
          expected.getRemoteFragmentInstanceId().getFragmentId(),
          actual.getRemoteFragmentInstanceId().getFragmentId());
      assertEquals(
          expected.getRemoteFragmentInstanceId().getInstanceId(),
          actual.getRemoteFragmentInstanceId().getInstanceId());
      assertEquals(expected.getRemotePlanNodeId(), actual.getRemotePlanNodeId());
    }
    assertEquals(
        descriptor,
        ((TableHashPartitioningShuffleSinkNode) deserialized).getPartitioningDescriptor());
  }

  @Test
  public void testRejectsInvalidPartitioningContract() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new HashPartitioningDescriptor(
                List.of(),
                HashPartitioningDescriptor.HashVersion.TABLE_HASH_V1,
                HashPartitioningDescriptor.NullRouting.HASH_NULL,
                1));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new HashPartitioningDescriptor(
                List.of(Symbol.of("city"), Symbol.of("city")),
                HashPartitioningDescriptor.HashVersion.TABLE_HASH_V1,
                HashPartitioningDescriptor.NullRouting.HASH_NULL,
                1));
  }

  @Test
  public void testRejectsPhysicalNodeWithIncompleteOutputPartitionSet() {
    HashPartitioningDescriptor descriptor =
        new HashPartitioningDescriptor(
            List.of(Symbol.of("city")),
            HashPartitioningDescriptor.HashVersion.TABLE_HASH_V1,
            HashPartitioningDescriptor.NullRouting.HASH_NULL,
            2);
    TableHashPartitioningShuffleSinkNode node =
        new TableHashPartitioningShuffleSinkNode(new PlanNodeId("hash-shuffle"), descriptor);

    assertThrows(IllegalStateException.class, () -> node.serialize(ByteBuffer.allocate(1024)));
  }

  private static DownStreamChannelLocation location(String planNodeId, int index) {
    return new DownStreamChannelLocation(
        new TEndPoint("test", 10000 + index),
        new TFragmentInstanceId("test", index, "instance"),
        planNodeId);
  }
}
