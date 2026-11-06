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

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/** Immutable validity set used by all snapshot value objects. */
public final class RuntimeSnapshotValiditySet {

  private final Set<RuntimeSnapshotValidity> values;

  private RuntimeSnapshotValiditySet(Set<RuntimeSnapshotValidity> values) {
    this.values = Collections.unmodifiableSet(EnumSet.copyOf(values));
  }

  public static RuntimeSnapshotValiditySet empty() {
    return new RuntimeSnapshotValiditySet(EnumSet.noneOf(RuntimeSnapshotValidity.class));
  }

  public static RuntimeSnapshotValiditySet of(RuntimeSnapshotValidity value) {
    return new RuntimeSnapshotValiditySet(EnumSet.of(Objects.requireNonNull(value)));
  }

  public static RuntimeSnapshotValiditySet copyOf(Set<RuntimeSnapshotValidity> values) {
    Objects.requireNonNull(values);
    if (values.isEmpty()) {
      return empty();
    }
    return new RuntimeSnapshotValiditySet(EnumSet.copyOf(values));
  }

  public RuntimeSnapshotValiditySet plus(RuntimeSnapshotValidity value) {
    EnumSet<RuntimeSnapshotValidity> copy = EnumSet.noneOf(RuntimeSnapshotValidity.class);
    copy.addAll(values);
    copy.add(Objects.requireNonNull(value));
    return new RuntimeSnapshotValiditySet(copy);
  }

  public boolean contains(RuntimeSnapshotValidity value) {
    return values.contains(value);
  }

  public boolean isEmpty() {
    return values.isEmpty();
  }

  public Set<RuntimeSnapshotValidity> asSet() {
    return values;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof RuntimeSnapshotValiditySet)) {
      return false;
    }
    RuntimeSnapshotValiditySet that = (RuntimeSnapshotValiditySet) other;
    return values.equals(that.values);
  }

  @Override
  public int hashCode() {
    return values.hashCode();
  }

  @Override
  public String toString() {
    return values.toString();
  }
}
