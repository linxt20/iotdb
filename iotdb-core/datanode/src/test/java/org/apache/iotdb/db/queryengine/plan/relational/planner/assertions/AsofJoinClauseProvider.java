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

package org.apache.iotdb.db.queryengine.plan.relational.planner.assertions;

import org.apache.iotdb.db.queryengine.plan.relational.planner.node.JoinNode;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AsofJoinClauseProvider implements ExpectedValueProvider<JoinNode.AsofJoinClause> {
  private final SymbolAlias left;
  private final SymbolAlias right;
  private final ComparisonExpression.Operator operator;

  public AsofJoinClauseProvider(
      ComparisonExpression.Operator operator, SymbolAlias left, SymbolAlias right) {
    this.operator = requireNonNull(operator, "operator is null");
    this.left = requireNonNull(left, "left is null");
    this.right = requireNonNull(right, "right is null");
  }

  public ComparisonExpression toExpression() {
    return new ComparisonExpression(
        operator, new SymbolReference(left.toString()), new SymbolReference(right.toString()));
  }

  @Override
  public JoinNode.AsofJoinClause getExpectedValue(SymbolAliases aliases) {
    return new JoinNode.AsofJoinClause(operator, left.toSymbol(aliases), right.toSymbol(aliases));
  }

  @Override
  public String toString() {
    return format("%s %s %s", left, operator.getValue(), right);
  }
}
