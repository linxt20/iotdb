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

# Table-model static property-enforcement matrix

This matrix is the acceptance contract for the **static** planning slice.  It distinguishes a
semantic requirement from a capability that is executable in the current data plane.  In
particular, `Partitioned(keys)` is not a claim of hash distribution until the implementation in
`table-model-hash-repartition.md` exists.

| SQL/operator family | Child requirement | Current provided/physical fact | Current enforcer or fallback | Scan parallelism decision | Executable regression |
| --- | --- | --- | --- | --- | --- |
| Unordered scan | none | unordered branches (`Arbitrary` at a convergence point) | none | allowed | `plainQueryAllowsParallelScan` |
| Filter | preserves child requirement | preserves its child's order/distribution | none for a simple filter | allowed unless a parent needs order/single stream | `filteredQueryAllowsParallelScan`, `filteredOrderedQueryForbidsParallelScan` |
| Project | semantically transparent only after symbol remapping | a computed project is not copied to each branch in the current planner | safe serial fallback for computed project | forbidden for computed project | `computedProjectDocumentsTheNoEnforcerFallback` |
| Global sort / ordered scan | `Single + Ordered(keys)` at its consumer | an unordered input is not ordered merely because it is a single child | `SortNode`, or `MergeSortNode` for real ordered branches | forbidden by the static planner; the separately gated local merge-tree is an execution-path optimization | `orderedRequirementOnUnorderedChildInsertsSortEnforcer`, `orderedQueryForbidsParallelScan` |
| Top-k | root result must be globally ordered/single | pushed-down TopK copies do not themselves converge inputs | TopK performs its own convergence; do not insert an extra collect below it | forbidden: the global ordered result is still serial on this static path | `topKDoesNotConvergeItsOwnChildren` |
| Row number | no order when the window has neither `PARTITION BY` nor `ORDER BY`; otherwise semantic target `Partitioned(partitionKeys)` and, if ordered, ordered input | unordered form has no synthetic sort; current static plan does not own a key-hash partitioning | no enforcer for the unrestricted form; source ordering or sort/single-stream fallback for an ordered form; partition-only forms retain existing Group/serial-safe planning | forbidden when ordered; no partition-parallel claim before the hash consumer exists | `unorderedRowNumberPlansWithoutSyntheticSort`, `orderedRowNumberForbidsParallelScan` |
| Other window functions | no partition/order requirement only when both clauses are absent; `PARTITION BY` targets `Partitioned(partitionKeys)` and `ORDER BY` additionally needs order | no order requirement is invented for an order-free window; an ordered WindowNode is non-splittable; current static plan does not own a key-hash partitioning | retain existing Group/collect/sort fallback; source order or a SortNode is implementation detail | no acceleration claim; forbidden when ordered and no hash-partitioned claim for partitioned windows | `unorderedWindowPlansWithoutOrderingRequirement`, `orderedWindowForbidsParallelScan` |
| Scalar aggregation | `Single` | final aggregate must observe all rows | collect/single logical stream | forbidden | `scalarAggregationUsesAForcedSerializationFallback` |
| Grouped final aggregation | semantic target: `Partitioned(groupKeys)` | no key-aware exchange exists | `CollectNode` is the only sound current fallback, not a repartition | forbidden until hash path lands | `partitionedRequirementConservativelyCollectsUntilHashExchangeExists` |
| Equi-join | semantic target: compatible `Partitioned(joinKeys)` on both inputs | current exchange is one-to-one or whole-block round-robin, never row-key routing | existing merge/sort serial fallback | forbidden | `equiJoinDocumentsThePartitionedKeysFallback` |
| Union all | no global ordering guarantee; compatible partitioning would require matching descriptors | generic union does not manufacture an owned partitioning | retain separate serial-safe branches until an explicit consumer exists | forbidden | `unionKeepsBranchSerializationUntilAnExplicitConsumer` |
| Output / global limit-offset | `Single` | final client result is one stream | `CollectNode` for unordered convergence, `MergeSortNode` when order is required | forbidden | `unorderedBranchesRecordArbitraryToSingleCollectEnforcement` |

## Trace invariants

When `enable_property_driven_planning=true`, every enforcement decision shown by textual EXPLAIN
must expose `required=`, `provided=`, and the selected glue node.  The executable unit coverage
asserts both important non-trivial transitions:

* `Arbitrary -> Single` uses `CollectNode`.
* `Single -> Single + Ordered(keys)` on an unordered input uses `SortNode`.

Textual EXPLAIN is an observability surface, not a correctness proof.  Every claim that an
execution path is parallel additionally needs DOP=1 versus DOP>1 result equivalence; every timing
claim additionally needs the benchmark kit's raw result and environment archive.

## Scope of the static assertions

The trace is emitted by the common convergence helper, so it has executable
`required`/`provided`/enforcer coverage for `Arbitrary -> Single` and
`Single -> Single + Ordered(keys)`.  Operator-specific visitors still contain the current static
fallbacks for TopK, windows, joins, and union.  Their regression tests therefore assert the
physical plan shape (the operator exists, no synthetic order is invented for order-free cases, and
order-sensitive cases keep scans non-splittable) as well as the compatibility comparison between
the legacy and property-driven flags.  This is deliberately not presented as proof that those
operators have consumed `Partitioned(keys)`; that requires the N-to-N hash-exchange consumers.
