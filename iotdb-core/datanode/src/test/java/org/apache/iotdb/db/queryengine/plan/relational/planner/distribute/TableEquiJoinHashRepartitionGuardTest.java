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

import org.apache.iotdb.calc.execution.relational.HashKeyCanonicalizer;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.ValuesNode;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.type.Type;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.DateType.DATE;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.read.common.type.TimestampType.TIMESTAMP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Unit tests for the join selection boundary, independent of fragment placement. */
public class TableEquiJoinHashRepartitionGuardTest {

  @Test
  public void supportedLookingInnerEquiJoinStillWaitsForHashJoinExecutorAndTwoSidedBuckets() {
    assertEquals(
        FallbackReason.HASH_JOIN_EXECUTOR_AND_TWO_SIDED_BUCKET_TOPOLOGY_REQUIRED,
        TableEquiJoinHashRepartitionGuard.getFallbackReason(createJoin(JoinNode.JoinType.INNER)));
  }

  @Test
  public void outerJoinIsRejectedBeforeAnyHashExchangeShapeIsConsidered() {
    assertEquals(
        FallbackReason.NON_INNER_JOIN,
        TableEquiJoinHashRepartitionGuard.getFallbackReason(createJoin(JoinNode.JoinType.LEFT)));
  }

  @Test
  public void floatingPointKeyFallsBackBeforeAnyHashExchangeIsBuilt() {
    JoinNode join = createJoin(JoinNode.JoinType.INNER);
    TypeProvider types = TypeProvider.viewOf(new HashMap<>());
    types.putTableModelType(join.getCriteria().get(0).getLeft(), DOUBLE);
    types.putTableModelType(join.getCriteria().get(0).getRight(), DOUBLE);

    assertEquals(
        FallbackReason.FLOATING_POINT_HASH_SEMANTICS_UNSUPPORTED,
        TableEquiJoinHashRepartitionGuard.getFallbackReason(join, types));
  }

  @Test
  public void integerKeyRemainsEligibleButStillRequiresExecutorAndTopology() {
    JoinNode join = createJoin(JoinNode.JoinType.INNER);
    TypeProvider types = TypeProvider.viewOf(new HashMap<>());
    types.putTableModelType(join.getCriteria().get(0).getLeft(), INT32);
    types.putTableModelType(join.getCriteria().get(0).getRight(), INT32);

    assertEquals(
        FallbackReason.HASH_JOIN_EXECUTOR_AND_TWO_SIDED_BUCKET_TOPOLOGY_REQUIRED,
        TableEquiJoinHashRepartitionGuard.getFallbackReason(join, types));
  }

  @Test
  public void coercedIntegerWidthIsRefused() {
    JoinNode join = createJoin(JoinNode.JoinType.INNER);
    assertEquals(
        FallbackReason.COERCED_KEY_TYPES_DISAGREE,
        TableEquiJoinHashRepartitionGuard.getFallbackReason(join, keyTypesOf(join, INT32, INT64)));
  }

  @Test
  public void dateAndTimestampAreSeparateIntegerForms() {
    JoinNode join = createJoin(JoinNode.JoinType.INNER);
    assertEquals(
        FallbackReason.COERCED_KEY_TYPES_DISAGREE,
        TableEquiJoinHashRepartitionGuard.getFallbackReason(
            join, keyTypesOf(join, DATE, TIMESTAMP)));
  }

  /**
   * The measurement the width refusal rests on, asserted on both sides of the gate. A probe that
   * only tries a non-negative value reads the two integer widths as one form. A negative value
   * shows they are not, so the gate has to refuse the pair.
   */
  @Test
  public void negativeValueProvesTheTwoIntegerWidthsAreNotOneForm() {
    TsBlock narrow = buildInts(-5);
    TsBlock wide = buildLongs(-5L);
    assertNotEquals(
        HashKeyCanonicalizer.routingHash(narrow.getColumn(0), 0, INT32),
        HashKeyCanonicalizer.routingHash(wide.getColumn(0), 0, INT64));

    JoinNode join = createJoin(JoinNode.JoinType.INNER);
    assertEquals(
        FallbackReason.COERCED_KEY_TYPES_DISAGREE,
        TableEquiJoinHashRepartitionGuard.getFallbackReason(join, keyTypesOf(join, INT32, INT64)));
  }

  /**
   * The refusal is stated per type, not per value, so it also covers the values that happen to
   * agree. The planner sees types, and a value-conditional gate would have to inspect every row.
   */
  @Test
  public void nonNegativeValuesAgreeButTheWidthChangeIsStillRefused() {
    TsBlock narrow = buildInts(5);
    TsBlock wide = buildLongs(5L);
    assertEquals(
        HashKeyCanonicalizer.routingHash(narrow.getColumn(0), 0, INT32),
        HashKeyCanonicalizer.routingHash(wide.getColumn(0), 0, INT64));

    JoinNode join = createJoin(JoinNode.JoinType.INNER);
    assertEquals(
        FallbackReason.COERCED_KEY_TYPES_DISAGREE,
        TableEquiJoinHashRepartitionGuard.getFallbackReason(join, keyTypesOf(join, INT32, INT64)));
  }

  private static TypeProvider keyTypesOf(JoinNode join, Type left, Type right) {
    TypeProvider types = TypeProvider.viewOf(new HashMap<>());
    types.putTableModelType(join.getCriteria().get(0).getLeft(), left);
    types.putTableModelType(join.getCriteria().get(0).getRight(), right);
    return types;
  }

  private static JoinNode createJoin(JoinNode.JoinType joinType) {
    Symbol leftKey = new Symbol("left_key");
    Symbol rightKey = new Symbol("right_key");
    ValuesNode left =
        new ValuesNode(
            new PlanNodeId("left_values"), ImmutableList.of(leftKey), Collections.emptyList());
    ValuesNode right =
        new ValuesNode(
            new PlanNodeId("right_values"), ImmutableList.of(rightKey), Collections.emptyList());
    return new JoinNode(
        new PlanNodeId("join"),
        joinType,
        left,
        right,
        ImmutableList.of(new JoinNode.EquiJoinClause(leftKey, rightKey)),
        Optional.empty(),
        ImmutableList.of(leftKey),
        ImmutableList.of(rightKey),
        Optional.empty(),
        Optional.empty());
  }

  private static TsBlock buildInts(int value) {
    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(TSDataType.INT32));
    builder.getTimeColumnBuilder().writeLong(0);
    builder.getColumnBuilder(0).writeInt(value);
    builder.declarePosition();
    return builder.build();
  }

  private static TsBlock buildLongs(long value) {
    TsBlockBuilder builder = new TsBlockBuilder(Arrays.asList(TSDataType.INT64));
    builder.getTimeColumnBuilder().writeLong(0);
    builder.getColumnBuilder(0).writeLong(value);
    builder.declarePosition();
    return builder.build();
  }

  private static JoinNode withConditions(
      JoinNode base,
      List<JoinNode.EquiJoinClause> keys,
      Optional<JoinNode.AsofJoinClause> asof,
      Optional<org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression> filter) {
    return new JoinNode(
        new PlanNodeId("conditions"),
        base.getJoinType(),
        base.getLeftChild(),
        base.getRightChild(),
        keys,
        asof,
        base.getLeftOutputSymbols(),
        base.getRightOutputSymbols(),
        filter,
        Optional.empty());
  }

  @Test
  public void crossJoinAndFilteredNonEquiJoinHaveNoEquiKeys() {
    JoinNode base = createJoin(JoinNode.JoinType.INNER);
    for (Optional<org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression> filter :
        List.of(
            Optional
                .<org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression>empty(),
            Optional.<org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression>of(
                BooleanLiteral.TRUE_LITERAL))) {
      assertEquals(
          FallbackReason.NO_EQUI_JOIN_KEYS,
          TableEquiJoinHashRepartitionGuard.getFallbackReason(
              withConditions(base, List.of(), Optional.empty(), filter)));
    }
  }

  @Test
  public void asofIsRejectedBeforeResidualFilter() {
    JoinNode base = createJoin(JoinNode.JoinType.INNER);
    JoinNode.EquiJoinClause key = base.getCriteria().get(0);
    JoinNode join =
        withConditions(
            base,
            base.getCriteria(),
            Optional.of(
                new JoinNode.AsofJoinClause(
                    ComparisonExpression.Operator.LESS_THAN, key.getLeft(), key.getRight())),
            Optional.of(BooleanLiteral.TRUE_LITERAL));
    assertEquals(
        FallbackReason.ASOF_JOIN, TableEquiJoinHashRepartitionGuard.getFallbackReason(join));
  }

  @Test
  public void residualFilterIsRejectedBeforeMissingTypes() {
    JoinNode base = createJoin(JoinNode.JoinType.INNER);
    JoinNode join =
        withConditions(
            base, base.getCriteria(), Optional.empty(), Optional.of(BooleanLiteral.TRUE_LITERAL));
    assertEquals(
        FallbackReason.RESIDUAL_JOIN_FILTER,
        TableEquiJoinHashRepartitionGuard.getFallbackReason(
            join, TypeProvider.viewOf(new HashMap<>())));
  }

  @Test
  public void eitherMissingKeyTypeIsRejected() {
    JoinNode join = createJoin(JoinNode.JoinType.INNER);
    for (Symbol present :
        List.of(join.getCriteria().get(0).getLeft(), join.getCriteria().get(0).getRight())) {
      TypeProvider types = TypeProvider.viewOf(new HashMap<>());
      types.putTableModelType(present, INT32);
      assertEquals(
          FallbackReason.UNKNOWN_HASH_KEY_TYPE,
          TableEquiJoinHashRepartitionGuard.getFallbackReason(join, types));
    }
  }

  @Test
  public void unsupportedTypeOnEitherSideIsRejected() {
    JoinNode join = createJoin(JoinNode.JoinType.INNER);
    Type unsupported = mock(Type.class);
    when(unsupported.getTypeEnum()).thenReturn(org.apache.tsfile.read.common.type.TypeEnum.ROW);
    for (boolean left : List.of(true, false)) {
      assertEquals(
          FallbackReason.UNSUPPORTED_HASH_KEY_TYPE,
          TableEquiJoinHashRepartitionGuard.getFallbackReason(
              join, keyTypesOf(join, left ? unsupported : INT32, left ? INT32 : unsupported)));
    }
  }
}
