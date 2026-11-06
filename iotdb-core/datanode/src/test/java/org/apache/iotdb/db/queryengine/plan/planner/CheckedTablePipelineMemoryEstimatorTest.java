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

import org.apache.iotdb.calc.execution.operator.CommonOperatorContext;
import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.plan.planner.memory.PipelineMemoryEstimator;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.common.SqlDialect;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.node.CollectNode;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.fragment.DataNodeQueryContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.memory.CheckedTablePipelineMemoryEstimator;
import org.apache.iotdb.db.queryengine.plan.planner.memory.ConsumeChildrenOneByOnePipelineMemoryEstimator;
import org.apache.iotdb.db.queryengine.plan.planner.memory.PipelineMemoryEstimatorFactory;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;

import org.apache.tsfile.read.common.block.TsBlock;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class CheckedTablePipelineMemoryEstimatorTest {
  public static class StubOperator implements Operator {
    @Override
    public com.google.common.util.concurrent.ListenableFuture<?> isBlocked() {
      return NOT_BLOCKED;
    }

    private final CommonOperatorContext context = mock(CommonOperatorContext.class);

    @Override
    public CommonOperatorContext getOperatorContext() {
      return context;
    }

    @Override
    public TsBlock next() throws Exception {
      return null;
    }

    @Override
    public boolean hasNext() throws Exception {
      return false;
    }

    @Override
    public void close() throws Exception {}

    @Override
    public boolean isFinished() throws Exception {
      return !hasNext();
    }

    @Override
    public long calculateMaxPeekMemory() {
      return 0;
    }

    @Override
    public long calculateMaxReturnSize() {
      return 0;
    }

    @Override
    public long calculateRetainedSizeAfterCallingNext() {
      return 0;
    }

    @Override
    public long ramBytesUsed() {
      return 0;
    }

    @Override
    public TsBlock nextWithTimer() throws Exception {
      return Operator.super.nextWithTimer();
    }

    @Override
    public boolean hasNextWithTimer() throws Exception {
      return Operator.super.hasNextWithTimer();
    }

    @Override
    public long calculateMaxPeekMemoryWithCounter() {
      return Operator.super.calculateMaxPeekMemoryWithCounter();
    }
  }

  private Operator operator(long running, long retained) {
    Operator op = spy(new StubOperator());
    when(op.getOperatorContext()).thenReturn(mock(CommonOperatorContext.class));
    when(op.calculateMaxPeekMemory()).thenReturn(running);
    when(op.ramBytesUsed()).thenReturn(retained);
    return op;
  }

  @Test
  public void allChildrenRunningAndRetainedAreCountedIncludingFinalSum() {
    CheckedTablePipelineMemoryEstimator root =
        new CheckedTablePipelineMemoryEstimator(operator(100, 10), -1);
    CheckedTablePipelineMemoryEstimator left =
        new CheckedTablePipelineMemoryEstimator(operator(200, 20), 7);
    CheckedTablePipelineMemoryEstimator right =
        new CheckedTablePipelineMemoryEstimator(operator(300, 30), -1);
    root.addChildren(Arrays.asList(left, right));
    assertEquals(600, root.calculateEstimatedRunningMemorySize());
    assertEquals(660, root.getEstimatedMemoryUsageInBytes());
    assertEquals(7, root.getChildren().get(0).getDependencyPipelineIndex());
    assertSame(left, root.getChildren().get(0));
    assertSame(right, root.getChildren().get(1));
  }

  @Test
  public void invalidRunningRetainedAndFinalAdditionFailClosed() {
    assertInvalid(
        new CheckedTablePipelineMemoryEstimator(operator(-1, 0), -1),
        IllegalArgumentException.class);
    assertInvalid(
        new CheckedTablePipelineMemoryEstimator(operator(0, -1), -1),
        IllegalArgumentException.class);
    assertInvalid(
        new CheckedTablePipelineMemoryEstimator(operator(Long.MAX_VALUE, 1), -1),
        ArithmeticException.class);
    CheckedTablePipelineMemoryEstimator running =
        new CheckedTablePipelineMemoryEstimator(operator(Long.MAX_VALUE, 0), -1);
    running.addChildren(
        Collections.singletonList(new CheckedTablePipelineMemoryEstimator(operator(1, 0), -1)));
    assertInvalid(running, ArithmeticException.class);
    CheckedTablePipelineMemoryEstimator retained =
        new CheckedTablePipelineMemoryEstimator(operator(0, Long.MAX_VALUE), -1);
    retained.addChildren(
        Collections.singletonList(new CheckedTablePipelineMemoryEstimator(operator(0, 1), -1)));
    assertInvalid(retained, ArithmeticException.class);
  }

  private void assertInvalid(PipelineMemoryEstimator estimator, Class<?> expected) {
    try {
      estimator.getEstimatedMemoryUsageInBytes();
      fail("Invalid estimate must not become a negative or zero admission");
    } catch (RuntimeException failure) {
      assertEquals(expected, failure.getClass());
    }
  }

  @Test
  public void foreignUncheckedChildCannotBypassTableArithmetic() {
    CheckedTablePipelineMemoryEstimator root =
        new CheckedTablePipelineMemoryEstimator(operator(1, 1), -1);
    root.addChildren(Collections.singletonList(mock(PipelineMemoryEstimator.class)));
    assertInvalid(root, IllegalArgumentException.class);
  }

  private LocalExecutionPlanContext context(SqlDialect dialect) {
    FragmentInstanceContext fi = mock(FragmentInstanceContext.class);
    when(fi.getId())
        .thenReturn(
            new FragmentInstanceId(
                new PlanFragmentId(
                    new QueryId(
                        "table_checked_"
                            + String.valueOf(dialect).toLowerCase(java.util.Locale.ROOT)),
                    0),
                "0"));
    if (dialect != null) {
      when(fi.getSessionInfo())
          .thenReturn(new SessionInfo(1, "root", ZoneId.of("UTC"), "db", dialect));
    }
    return new LocalExecutionPlanContext(new TypeProvider(), fi, new DataNodeQueryContext(1));
  }

  private PlanNode identity(String id) {
    PlanNode node = mock(PlanNode.class);
    when(node.getPlanNodeId()).thenReturn(new PlanNodeId(id));
    when(node.getType()).thenReturn(PlanNodeType.IDENTITY_SINK);
    return node;
  }

  @Test
  public void tableAndSubcontextUseCheckedEstimatorAndPreserveRegistrationOrder() {
    LocalExecutionPlanContext context = context(SqlDialect.TABLE);
    LocalExecutionPlanContext sub = context.createSubContext();
    assertSame(context.getInstanceContext(), sub.getInstanceContext());
    PlanNode rootNode = identity("parent");
    PipelineMemoryEstimator first =
        sub.constructPipelineMemoryEstimator(
            operator(2, 3), rootNode.getPlanNodeId(), identity("first"), 9);
    PipelineMemoryEstimator second =
        context.constructPipelineMemoryEstimator(
            operator(5, 7), rootNode.getPlanNodeId(), identity("second"), -1);
    PipelineMemoryEstimator root =
        context.constructPipelineMemoryEstimator(operator(11, 13), null, rootNode, -1);
    assertTrue(root instanceof CheckedTablePipelineMemoryEstimator);
    assertTrue(first instanceof CheckedTablePipelineMemoryEstimator);
    assertTrue(second instanceof CheckedTablePipelineMemoryEstimator);
    assertEquals(Arrays.asList(first, second), root.getChildren());
    assertEquals(9, first.getDependencyPipelineIndex());
    assertEquals(41, root.getEstimatedMemoryUsageInBytes());
  }

  @Test
  public void nullAndTreeSessionsRetainOriginalFactoryPath() {
    PlanNode node = identity("legacy");
    for (SqlDialect dialect : Arrays.asList(null, SqlDialect.TREE)) {
      Operator op = operator(2, 3);
      PipelineMemoryEstimator estimator =
          context(dialect).constructPipelineMemoryEstimator(op, null, node, -1);
      assertEquals(
          PipelineMemoryEstimatorFactory.createPipelineMemoryEstimator(op, node, -1).getClass(),
          estimator.getClass());
      assertTrue(estimator instanceof ConsumeChildrenOneByOnePipelineMemoryEstimator);
      assertEquals(5, estimator.getEstimatedMemoryUsageInBytes());
    }
  }

  @Test
  public void tableVisitCollectUsesFairFanInForGenericNonExchangeChildren() throws Exception {
    LocalExecutionPlanContext context = context(SqlDialect.TABLE);
    PlanNode leaf = identity("generic");
    Operator output = operator(2, 3);
    doReturn(output).when(leaf).accept(any(), same(context));
    CollectNode collect =
        new CollectNode(
            new PlanNodeId("collect"), Collections.singletonList(leaf), Collections.emptyList());
    DataNodeTableOperatorGenerator generator =
        new DataNodeTableOperatorGenerator(mock(Metadata.class));
    Operator actual = generator.visitCollect(collect, context);
    assertEquals(
        org.apache.iotdb.calc.execution.operator.process.CollectOperator.class, actual.getClass());
    assertSame(
        output,
        ((org.apache.iotdb.calc.execution.operator.process.CollectOperator) actual)
            .getChildren()
            .get(0));
    assertEquals("CollectOperator", actual.getOperatorContext().getOperatorType());
    actual.close();
  }

  @Test
  public void finalRunningPlusRetainedOverflowCannotReachZeroReservationAdmission()
      throws Exception {
    IoTDBDescriptor descriptor = IoTDBDescriptor.getInstance();
    boolean before = descriptor.getMemoryConfig().isEnableQueryMemoryEstimation();
    LocalExecutionPlanner planner = LocalExecutionPlanner.getInstance();
    long free = planner.getFreeMemoryForOperators();
    FragmentInstanceContext fi = mock(FragmentInstanceContext.class);
    try {
      descriptor.getMemoryConfig().setEnableQueryMemoryEstimation(true);
      Method method =
          LocalExecutionPlanner.class.getDeclaredMethod(
              "checkMemory", PipelineMemoryEstimator.class, FragmentInstanceContext.class);
      method.setAccessible(true);
      try {
        method.invoke(
            planner, new CheckedTablePipelineMemoryEstimator(operator(Long.MAX_VALUE, 1), -1), fi);
        fail("Overflow must be rejected before allocation");
      } catch (InvocationTargetException expected) {
        assertTrue(expected.getCause() instanceof ArithmeticException);
      }
      assertEquals(free, planner.getFreeMemoryForOperators());
      verify(fi, never()).setAdmissionAccepted(any());
    } finally {
      descriptor.getMemoryConfig().setEnableQueryMemoryEstimation(before);
    }
  }
}
