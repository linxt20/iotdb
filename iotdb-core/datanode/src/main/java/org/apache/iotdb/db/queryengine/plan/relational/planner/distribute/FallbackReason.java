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

/**
 * Stable optimization fallback codes. An applied or satisfied decision has no fallback reason.
 * Descriptor and ownership contract violations remain planning errors.
 */
public enum FallbackReason {
  NON_INNER_JOIN,
  NO_EQUI_JOIN_KEYS,
  ASOF_JOIN,
  RESIDUAL_JOIN_FILTER,
  UNKNOWN_HASH_KEY_TYPE,
  FLOATING_POINT_HASH_SEMANTICS_UNSUPPORTED,
  UNSUPPORTED_HASH_KEY_TYPE,
  COERCED_KEY_TYPES_DISAGREE,
  HASH_JOIN_EXECUTOR_AND_TWO_SIDED_BUCKET_TOPOLOGY_REQUIRED,
  PARTITIONING_UNSUPPORTED,
  DISTINCT_OR_MASKED_AGGREGATION,
  MULTIPLE_GROUPING_SETS,
  UNKNOWN_PROVIDER
}
