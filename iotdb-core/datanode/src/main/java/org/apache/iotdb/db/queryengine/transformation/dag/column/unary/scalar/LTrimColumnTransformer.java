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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar;

import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.UnaryColumnTransformer;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.BytesUtils;

public class LTrimColumnTransformer extends UnaryColumnTransformer {
  private final String character;

  public LTrimColumnTransformer(
      Type returnType, ColumnTransformer childColumnTransformer, String character) {
    super(returnType, childColumnTransformer);
    this.character = character;
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        String currentValue = column.getBinary(i).getStringValue(TSFileConfig.STRING_CHARSET);
        columnBuilder.writeBinary(BytesUtils.valueOf(ltrim(currentValue)));
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  private String ltrim(String value) {
    if (value.isEmpty() || character.isEmpty()) {
      return value;
    }

    int start = 0;

    while (start < value.length() && character.indexOf(value.charAt(start)) >= 0) start++;

    if (start >= value.length()) {
      return "";
    } else {
      return value.substring(start);
    }
  }
}
