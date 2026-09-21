<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Table-model hash-partitioning exchange contract

`DistributionProperty.partitioned(keys)` is a planner property. It becomes a physical guarantee
only when a hash exchange routes every row with the same ordered key tuple to the same output
partition. `HashPartitioningDescriptor` is the serializable boundary between those two layers.

The descriptor contains, in wire order:

1. ordered `Symbol` list;
2. `HashVersion.TABLE_HASH_V1`;
3. `NullRouting.HASH_NULL`;
4. positive output partition count.

Order is semantic: `(a, b)` and `(b, a)` are distinct contracts. `HASH_NULL` requires the future
router to hash a typed null marker; it must not discard null rows or use an untyped Java hash.

`TableHashPartitioningShuffleSinkNode` has its own plan-node type (1046), rather than extending
the old `SHUFFLE_SINK` wire layout. This keeps the existing round-robin shuffle serialization
compatible with previous nodes. The new type can be serialized and deserialized by every
DataNode built from this branch, but it is intentionally not selected by
`TableDistributedPlanGenerator`.

The new type is a rolling-upgrade boundary: a mixed-version cluster must keep the feature disabled,
because an older DataNode cannot deserialize type 1046.

The current `ShuffleSinkHandle` and `ShuffleHelperOperator` route TsBlocks round-robin. They do
not consume this descriptor and therefore do not establish a `Partitioned(keys)` property. Before
the node can be selected, the execution stage must supply all of the following:

1. a row-level splitter that evaluates the ordered symbols and the versioned, typed hash;
2. deterministic null/type encoding and partition-index calculation;
3. per-output-partition buffering, backpressure, and close/error propagation;
4. an N-to-N fragment topology whose downstream exchange indices match the descriptor count;
5. grouped aggregation and equi-join consumers that request and verify the same key contract;
6. multi-DataNode result-equivalence, partition-consistency, skew, and shuffle-byte tests.

Until those conditions hold, the contract is deliberately default-off. Its round-trip test proves
that a future stage can pass the necessary semantics through the plan without changing legacy
shuffle behavior; it is not evidence of data-parallel execution.
