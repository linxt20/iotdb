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

package org.apache.iotdb.db.queryengine.statistics.snapshot;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Bounded structural estimate. It performs no serialization and stops before copying oversize data.
 */
public final class DynamicSnapshotBudget {
  private DynamicSnapshotBudget() {}

  private static final class Exceeded extends RuntimeException {
    private static final long serialVersionUID = 1L;

    Exceeded() {
      super(null, null, false, false);
    }
  }

  private static final class Counter {
    long bytes;
    final long limit;

    Counter(long limit) {
      this.limit = limit;
    }

    void charge(long amount) {
      bytes = Math.addExact(bytes, amount);
      if (bytes > limit) {
        throw new Exceeded();
      }
    }
  }

  public static long estimate(Object value, long limit) {
    if (limit < 0) {
      return Long.MAX_VALUE;
    }
    Counter counter = new Counter(limit);
    try {
      visit(value, counter, 0);
      return counter.bytes;
    } catch (Exceeded | ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void visit(Object value, Counter counter, int depth) {
    if (depth > 64) {
      throw new Exceeded();
    }
    if (value == null) {
      counter.charge(8);
      return;
    }
    if (value instanceof String) {
      counter.charge(40L + 2L * ((String) value).length());
    } else if (value instanceof Number
        || value instanceof Boolean
        || value instanceof org.apache.thrift.TEnum) {
      counter.charge(24);
    } else if (value instanceof ByteBuffer) {
      counter.charge(48L + ((ByteBuffer) value).remaining());
    } else if (value instanceof byte[]) {
      counter.charge(24L + ((byte[]) value).length);
    } else if (value instanceof List) {
      List<?> list = (List<?>) value;
      counter.charge(24L + 8L * list.size());
      for (Object item : list) {
        visit(item, counter, depth + 1);
      }
    } else if (value instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) value;
      counter.charge(48L + 64L * map.size());
      for (Map.Entry<?, ?> e : map.entrySet()) {
        visit(e.getKey(), counter, depth + 1);
        visit(e.getValue(), counter, depth + 1);
      }
    } else if (value instanceof TBase) {
      TBase base = (TBase) value;
      Map<TFieldIdEnum, FieldMetaData> fields =
          (Map) FieldMetaData.getStructMetaDataMap((Class) base.getClass());
      counter.charge(64L + 8L * fields.size());
      for (TFieldIdEnum field : fields.keySet()) {
        if (base.isSet(field)) {
          visit(base.getFieldValue(field), counter, depth + 1);
        }
      }
    } else {
      throw new Exceeded();
    }
  }
}
