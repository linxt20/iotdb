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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class Row extends Expression {
  private final List<Expression> items;

  public Row(List<Expression> items) {
    super(null);
    this.items = ImmutableList.copyOf(requireNonNull(items, "items is null"));
  }

  public Row(NodeLocation location, List<Expression> items) {
    super(requireNonNull(location, "location is null"));
    this.items = ImmutableList.copyOf(requireNonNull(items, "items is null"));
  }

  public Row(ByteBuffer byteBuffer) {
    super(null);
    int size = ReadWriteIOUtils.readInt(byteBuffer);
    ImmutableList.Builder<Expression> builder = new ImmutableList.Builder<>();
    while (size-- > 0) {
      builder.add(Expression.deserialize(byteBuffer));
    }
    this.items = builder.build();
  }

  protected void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(items.size(), byteBuffer);
    for (Expression expression : items) {
      Expression.serialize(expression, byteBuffer);
    }
  }

  protected void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(items.size(), stream);
    for (Expression expression : items) {
      Expression.serialize(expression, stream);
    }
  }

  @Override
  public TableExpressionType getExpressionType() {
    return TableExpressionType.ROW;
  }

  public List<Expression> getItems() {
    return items;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitRow(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return items;
  }

  @Override
  public int hashCode() {
    return Objects.hash(items);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    Row other = (Row) obj;
    return Objects.equals(this.items, other.items);
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }
}
