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

package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TypeProviderSerializationContractTest {
  private static final Symbol KEY = Symbol.of("key");

  private static TypeProvider table() {
    return new TypeProvider(new HashMap<>(Map.of(KEY, INT64)));
  }

  private static TypeProvider mixed() {
    TypeProvider p = new TypeProvider();
    p.setTreeModelType("root.d.s", TSDataType.INT32);
    p.setTableModelTypes(new HashMap<>(Map.of(KEY, INT64)));
    return p;
  }

  private static ByteBuffer direct(TypeProvider p) {
    ByteBuffer b = ByteBuffer.allocate(32768);
    p.serialize(b);
    b.flip();
    return b;
  }

  private static ByteBuffer stream(TypeProvider p) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    p.serialize(new DataOutputStream(bytes));
    return ByteBuffer.wrap(bytes.toByteArray());
  }

  private static void checkTable(TypeProvider expected, ByteBuffer b) {
    TypeProvider restored = TypeProvider.deserialize(b);
    assertEquals(
        expected.getTableModelType(KEY).getTypeEnum(),
        restored.getTableModelType(KEY).getTypeEnum());
    assertEquals(0, b.remaining());
  }

  private static PlanFragment fragment(boolean types) {
    PlanFragment f =
        new PlanFragment(
            new QueryId("serde_contract").genPlanFragmentId(),
            new CollectNode(new PlanNodeId("collect"), List.of(KEY)));
    if (types) f.setTypeProvider(table());
    return f;
  }

  private static byte[] fragmentStream(PlanFragment f) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    f.serialize(new DataOutputStream(bytes));
    return bytes.toByteArray();
  }

  private static byte[] fragmentBuffer(PlanFragment f) {
    ByteBuffer b = ByteBuffer.allocate(32768);
    f.serialize(b);
    b.flip();
    byte[] bytes = new byte[b.remaining()];
    b.get(bytes);
    return bytes;
  }

  private static void checkFragment(PlanFragment original, ByteBuffer b) {
    PlanFragment restored = PlanFragment.deserialize(b);
    assertEquals(original.getId(), restored.getId());
    assertEquals(original.getPlanNodeTree().getClass(), restored.getPlanNodeTree().getClass());
    assertEquals(
        original.getPlanNodeTree().getOutputSymbols(),
        restored.getPlanNodeTree().getOutputSymbols());
    if (original.getTypeProvider() != null)
      assertEquals(
          INT64.getTypeEnum(), restored.getTypeProvider().getTableModelType(KEY).getTypeEnum());
    else assertNull(restored.getTypeProvider());
    assertEquals(0, b.remaining());
  }

  @Test
  public void tableProviderBufferRoundTrip() {
    checkTable(table(), direct(table()));
  }

  @Test
  public void mixedProviderBufferRoundTrip() {
    ByteBuffer b = direct(mixed());
    TypeProvider restored = TypeProvider.deserialize(b);
    assertEquals(TSDataType.INT32, restored.getTreeModelType("root.d.s"));
    assertEquals(INT64.getTypeEnum(), restored.getTableModelType(KEY).getTypeEnum());
    assertEquals(0, b.remaining());
  }

  @Test
  public void tableProviderStreamRoundTrip() throws Exception {
    checkTable(table(), stream(table()));
  }

  @Test
  public void mixedProviderStreamRoundTrip() throws Exception {
    ByteBuffer b = stream(mixed());
    TypeProvider restored = TypeProvider.deserialize(b);
    assertEquals(TSDataType.INT32, restored.getTreeModelType("root.d.s"));
    assertEquals(INT64.getTypeEnum(), restored.getTableModelType(KEY).getTypeEnum());
    assertEquals(0, b.remaining());
  }

  @Test
  public void emptyTableProviderBufferRoundTrip() {
    ByteBuffer b = direct(TypeProvider.empty());
    TypeProvider restored = TypeProvider.deserialize(b);
    assertEquals(0, restored.allTableModelTypes().size());
    assertEquals(0, b.remaining());
  }

  @Test
  public void treeProviderBufferRoundTrip() {
    TypeProvider p = new TypeProvider(new HashMap<>(Map.of("root.d.s", TSDataType.INT32)), null);
    ByteBuffer b = direct(p);
    TypeProvider restored = TypeProvider.deserialize(b);
    assertEquals(TSDataType.INT32, restored.getTreeModelType("root.d.s"));
    assertEquals(0, b.remaining());
  }

  @Test
  public void mixedProviderEncodingMatchesStream() throws Exception {
    TypeProvider p = mixed();
    ByteBuffer a = direct(p), b = stream(p);
    byte[] av = new byte[a.remaining()], bv = new byte[b.remaining()];
    a.get(av);
    b.get(bv);
    assertArrayEquals(bv, av);
  }

  @Test
  public void tableFragmentStreamRoundTrip() throws Exception {
    PlanFragment f = fragment(true);
    checkFragment(f, ByteBuffer.wrap(fragmentStream(f)));
  }

  @Test
  public void tableFragmentBufferRoundTrip() throws Exception {
    PlanFragment f = fragment(true);
    byte[] encoded = fragmentBuffer(f);
    assertArrayEquals(fragmentStream(f), encoded);
    checkFragment(f, ByteBuffer.wrap(encoded));
  }

  @Test
  public void untypedFragmentBufferRoundTrip() throws Exception {
    PlanFragment f = fragment(false);
    byte[] encoded = fragmentBuffer(f);
    assertArrayEquals(fragmentStream(f), encoded);
    checkFragment(f, ByteBuffer.wrap(encoded));
  }
}
