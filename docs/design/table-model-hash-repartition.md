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

# Table-model hash repartition: implementation boundary and plan

`DistributionProperty.partitioned(keys)` is currently a planner algebra type, not a data-plane
capability.  This is intentional: in the current implementation a partitioned requirement that
does not already hold is conservatively enforced with a `CollectNode`, yielding one branch.  One
branch keeps every equal-key group together, but it is not a repartition and must not be reported
as a parallel acceleration.

The executable guard for that boundary is
`PropertyDrivenDistributionTest#partitionedRequirementConservativelyCollectsUntilHashExchangeExists`.
It must be replaced by positive plan and result-equivalence tests only when every step below is
implemented.

## What exists, and why it cannot implement `Partitioned(keys)`

| Area | Existing class | Current behaviour | Why it is insufficient |
| --- | --- | --- | --- |
| Table fragment boundary | `plan.relational.planner.node.ExchangeNode` | Each exchange has one upstream `IdentitySinkNode`. | It represents one input stream, not N hash buckets. |
| Table distributed planner | `plan.relational.planner.distribute.AddExchangeNodes` and `TableDistributedPlanner#adjustUpStreamHelper` | Creates one `ExchangeNode` per child and then an `IdentitySinkNode`. | There is no mapping from a relational `Symbol` to a downstream partition/channel. |
| Generic shuffle sink | `plan.planner.plan.node.sink.ShuffleSinkNode` and `OperatorTreeGenerator#visitShuffleSink` | Sends whole `TsBlock`s with `SIMPLE_ROUND_ROBIN`. | Round-robin sends equal keys in different blocks to different channels. |
| Exchange handle | `execution.exchange.sink.ShuffleSinkHandle` | Supports only `PLAIN` and `SIMPLE_ROUND_ROBIN`. | It selects a channel per block, has no row key or row splitter. |
| Table execution | `DataNodeTableOperatorGenerator#visitIdentitySink` | Always requests `ShuffleStrategyEnum.PLAIN`. | It cannot describe or execute hash distribution. |

`CollectNode`, `MergeSortNode`, and a one-input `ExchangeNode` are therefore not valid evidence of
hash repartitioning.  They may converge streams, but they cannot create parallel key ownership.

## Required production changes

The implementation should be one vertical slice, with group-by first and equi-join second.  Do
not enable planner selection until the corresponding execution and result tests pass.

1. **Represent the exchange contract.** Add a table-model `HashPartitionExchangeNode` (or extend
   `ExchangeNode` with a serialised distribution descriptor) under
   `plan.relational.planner.node`.  The descriptor needs ordered partition symbols, their input
   column indices after projection, a stable null policy, a hash-function/version identifier, and
   the downstream partition count.  It must be included in `clone`, equality, serialization,
   `DataNodePlanNodeDeserializer`, `PlanVisitor`, and `PlanGraphPrinter`.
2. **Split rows at the sink, not blocks.** Add a key-aware sink mode to
   `ShuffleSinkHandle.ShuffleStrategyEnum`, configured by a new descriptor rather than bare enum
   values.  The implementation needs a table-aware operator in
   `DataNodeTableOperatorGenerator` which takes each input `TsBlock`, hashes every row over the
   declared key columns, builds one output `TsBlock` per channel, and sends each bucket to its
   assigned channel.  `SIMPLE_ROUND_ROBIN` must remain unchanged for tree-model users.
3. **Build N-to-N fragment edges.** Generalize `AddExchangeNodes` and
   `TableDistributedPlanner#adjustUpStreamHelper` so every upstream producing fragment has one
   hash sink with a channel for every destination partition, and every destination fragment has
   an exchange source for every upstream.  Assign downstream locations before serialisation and
   keep the existing `indexOfUpstreamSinkHandle` ownership rules valid for every edge.

   The first grouped-aggregation slice has to build this exact shape, rather than replace one
   `IdentitySinkNode` with one `TableHashPartitioningShuffleSinkNode`:

   ```text
   partial aggregation for source region r0 -- hash sink {p0, p1, ..., p(P-1)} --+
   partial aggregation for source region r1 -- hash sink {p0, p1, ..., p(P-1)} --+--> final[p0]
   ...                                                                  ...      |
   partial aggregation for source region r(R-1) -- hash sink {p0, ..., p(P-1)} --+

   (the same R incoming channels are constructed independently for final[p1] ... final[p(P-1)])
   final[p0], ..., final[p(P-1)] -- ordinary CollectNode --> output
   ```

   `AddExchangeNodes` must therefore recognize the partial-to-final grouping boundary, clone the
   final aggregation into `P` destination fragments, and give each destination exchange one
   upstream channel from every partial fragment. `adjustUpStreamHelper` cannot keep its current
   one-`ExchangeNode`/one-`IdentitySinkNode` assumption: it must create one hash sink per partial
   fragment and add all `P` downstream locations before `SubPlanGenerator` cuts the plan. Finally,
   `TableModelQueryFragmentPlanner#calculateNodeTopologyBetweenInstance` must resolve every
   `(source fragment, destination partition)` edge; its current `instanceMap.putIfAbsent` mapping
   is only sufficient when one fragment has one instance. The general multi-source planner flag
   remains off until this topology is represented, serialized, and exercised on at least two
   DataNodes; the separate single-source 1 x P experiment is described below.
4. **Make properties truthful.** Add a provided-distribution map beside the existing
   `nodeOrderingMap` in `TableDistributedPlanGenerator`.  A scan may report `Partitioned(keys)`
   only when its actual source assignment guarantees it; a new hash exchange reports exactly its
   descriptor keys.  Projections must remap symbols, filters preserve distribution, and a generic
   union is `Arbitrary` unless every input has the same partitioning descriptor.
5. **First consumer: final grouped aggregation.** In `visitAggregation`, when a final aggregation
   has non-empty grouping keys and the child does not satisfy `Partitioned(groupKeys)`, insert the
   hash exchange between partial and final aggregation.  A global aggregation continues to require
   `Single`; no behaviour change is allowed for non-streamable or distinct/masked cases until a
   separate correctness proof exists.
6. **Second consumer: equi-join.** In `visitJoin`, only an inner equi-join with supported equality
   criteria may choose co-partitioning.  Both sides must use the same hash version, null policy,
   partition count and compatible key order.  Cross join, non-equality/as-of join, outer join,
   dynamic filters, and joins with non-deterministic expressions retain the current single/merge
   fallback until individually implemented and tested.

## Correctness gates

Before flipping the planner feature flag, add the following tests.

* Unit-test the hash descriptor: symbol-to-column resolution, key order, null handling, stable
  hash values, and failures for missing/duplicate symbols.
* Unit-test the sink with a multi-row, multi-`TsBlock` input: all rows with equal keys reach the
  same channel; every input row appears exactly once; nulls and mixed scalar types have defined
  behaviour; changing block boundaries does not change routing.
* Plan-shape tests: a partial/final `GROUP BY` and supported equi-join each contain hash exchange
  nodes on both inputs, and unsupported joins/groups contain no hash exchange.
* Multi-DataNode integration tests: compare the sorted results and multiplicities of DOP=1 against
  DOP>1 for skewed groups, repeated join keys, empty input, null keys, and key values spanning
  regions.  Assert that every hash bucket receives only its owned keys.
* Benchmark assertions: archive partition count, hash version, per-channel rows/bytes, spill or
  backpressure time, and final result checksum.  Speedup without the checksum is not acceptance.

## Rollout and observability

The current code exposes a default-off `enable_table_group_by_hash_repartition` flag independent
of `enable_property_driven_planning`. It is deliberately narrower than the production N-to-N
flag: it selects only a direct, non-streamable, non-distinct `GROUP BY` with one
`DeviceTableScan` source. That restricted 1 x P shape is executable: one partial aggregation uses
a `TableHashPartitioningShuffleSinkNode`, each bucket has a distinct final aggregation exchange,
and the output Collect merges only disjoint groups. Filters, ordered/streamable grouping, complex
grouping sets, global aggregations, and every multi-source plan retain the Collect fallback.

EXPLAIN must identify the key list, hash version, partition count and every explicit fallback
reason. Metrics should include rows and bytes per channel, largest/smallest bucket ratio, blocked
sink time, source-handle wait time, and per-driver finish time. The general N-to-N feature flag
must remain off until the multi-DataNode correctness gates above pass; joins remain separately
gated.

## Multi-source executable-shape gate

The source-by-bucket matrix is now accompanied by
`TableGroupByHashRepartitionTopology#validateFragmentOwnership`. It is deliberately a planning
gate rather than a configuration switch. For a two-source, three-bucket GROUP BY, it accepts only
the following ownership shape:

```text
partial-0: hash-sink-0 -> exchange-00, exchange-01, exchange-02
partial-1: hash-sink-1 -> exchange-10, exchange-11, exchange-12
final-0: exchange-00, exchange-10
final-1: exchange-01, exchange-11
final-2: exchange-02, exchange-12
```

It rejects three common false-positive shapes: missing source/destination fragments, exchanges
for one bucket placed in different final fragments, and all bucket exchanges left under one serial
final fragment. It also rejects a partial fragment reused as a final fragment. The unit suite has a
positive 2 x 3 ownership proof and negative tests for the two error shapes that would otherwise
silently overwrite or serialize the exchange graph.

This gate documents an important current limitation. `SubPlanGenerator` de-duplicates a shared
sink by id and cuts it into one child `SubPlan`; it does not clone the parent final aggregation into
P `PlanFragment`s. The existing single-source experiment must remain unchanged while this is
addressed. A multi-source implementation must first:

1. build one source fragment per partial aggregation and one final fragment per bucket;
2. put all source-specific exchanges for bucket `p` inside final fragment `p`;
3. collect the independently executed final fragments only after their grouped aggregations; and
4. invoke the ownership gate after fragment splitting, before `TableModelQueryFragmentPlanner`
   assigns endpoints and fragment-instance ids.

Only after that gate passes can the fragment planner's existing per-fragment instance selection be
used to resolve every `(source, bucket)` channel. The remaining release gates are an actual
two-DataNode result-equivalence run (including skew and null groups), per-channel shuffle
accounting, and an explicit fallback assertion for every unsupported aggregate shape.
