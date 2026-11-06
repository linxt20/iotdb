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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.queryengine.statistics.snapshot;

import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Pure aggregation rules for runtime snapshot fields. */
public final class RuntimeSnapshotDerivation {

  private RuntimeSnapshotDerivation() {
    // utility class
  }

  public static long sumNonNull(Collection<Long> values) {
    long result = 0;
    for (Long value : values) {
      if (value != null) {
        if (value < 0)
          throw new IllegalArgumentException(
              DataNodeQueryMessages.EXCEPTION_NEGATIVE_VALUE_62B19D51);
        result = Math.addExact(result, value);
      }
    }
    return result;
  }

  public static OptionalDouble blockedRatio(Long blockedNanos, Long readyNanos) {
    if (blockedNanos == null || readyNanos == null) {
      return OptionalDouble.empty();
    }
    if (blockedNanos < 0 || readyNanos < 0) return OptionalDouble.empty();
    long denominator;
    try {
      denominator = Math.addExact(blockedNanos, readyNanos);
    } catch (ArithmeticException e) {
      return OptionalDouble.empty();
    }
    if (denominator == 0) {
      return OptionalDouble.empty();
    }
    return OptionalDouble.of(((double) blockedNanos) / denominator);
  }

  public static OptionalDouble throughput(Long numerator, Long denominator) {
    if (numerator == null || denominator == null || denominator == 0) {
      return OptionalDouble.empty();
    }
    if (numerator < 0 || denominator < 0) return OptionalDouble.empty();
    return OptionalDouble.of(((double) numerator) / denominator);
  }

  public static OptionalDouble minDriverWall(List<DriverRuntimeSnapshot> drivers) {
    return wall(drivers, true);
  }

  public static OptionalDouble maxDriverWall(List<DriverRuntimeSnapshot> drivers) {
    return wall(drivers, false);
  }

  private static OptionalDouble wall(List<DriverRuntimeSnapshot> drivers, boolean min) {
    Objects.requireNonNull(drivers);
    return drivers.stream()
        .map(DriverRuntimeSnapshot::getWallNanos)
        .filter(Objects::nonNull)
        .mapToDouble(Long::doubleValue)
        .reduce((left, right) -> min ? Math.min(left, right) : Math.max(left, right));
  }

  public static long sumDriverField(
      Collection<DriverRuntimeSnapshot> drivers, Function<DriverRuntimeSnapshot, Long> field) {
    return sumNonNull(drivers.stream().map(field).collect(Collectors.toList()));
  }

  public static Comparator<DriverRuntimeSnapshot> stableDriverOrder() {
    return Comparator.comparing(DriverRuntimeSnapshot::getFragmentInstanceId)
        .thenComparing(DriverRuntimeSnapshot::getDriverPipelineId)
        .thenComparing(DriverRuntimeSnapshot::getDriverTaskId);
  }
}
