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

# Seven-task status: executable evidence versus remaining scope

This status page separates code that has been exercised on an isolated DataNode from contracts,
test gates, and deliberately unsupported paths. It is not a performance conclusion.

| Task | Current state | Evidence / boundary |
| --- | --- | --- |
| Static property matrix | **Implemented and reactor-validated** | Plan assertions cover scan, filter, project, sort, top-k, row number, window, aggregation, join, and union. Operator-specific fallbacks are explicit. |
| `Partitioned(keys)` | **Partially executable** | A versioned descriptor, per-row `TABLE_HASH_V1` router, exact channel sink, and real default-off single-source GROUP BY 1 x P consumer exist. General multi-source N x P and join are gated off. |
| Independent cluster E2E | **Partially accepted** | Static paths were accepted previously. A fresh isolated hash-on DataNode emitted `TableHashPartitioningShuffleSinkNode(HashPartitioningSinkOperator)` with two downstream exchanges; a hash-off isolated control produced the same canonical result rows. |
| Benchmark matrix | **Assets ready; no published speedup data** | The DOP/warm/cold runner, real CLI adapter, validators and environment capture exist. No P50/P95 or acceleration number may be reported until the fixed-DOP matrix completes on a suitably sized fixture. |
| Multi-query cases | **Static cases and controls ready** | Scan/filter/project/ordered/top-k/morsel cases are staged. GROUP BY is a restricted one-source correctness case; join remains baseline-only. |
| Balance/backpressure | **Instrumentation and parser ready** | Morsel estimated/actual workload, driver wall time and LPT versus equal-count extraction are implemented. The complete skewed-data comparison remains an experiment gate. |
| Reproducible assets | **Implemented** | Deterministic fixture generator, DOP runner, CLI adapter, result validator, E2E scripts, configuration/version capture, and raw JSON/CSV archive layout are present. |

## Validated hash GROUP BY slice

The enabled configuration is default-off in production and explicitly sets
`enable_table_group_by_hash_repartition=true` with two partitions. The accepted query filters to
one physical device then groups on field `s1`. Its `EXPLAIN ANALYZE` shows one partial aggregation,
`TableHashPartitioningShuffleSinkNode(HashPartitioningSinkOperator)`, two explicit downstream
exchange IDs, and two final aggregation instances. The hash-on and hash-off result-row files have
the same SHA-256. This establishes physical property consumption for that narrow slice.

It does not establish a general distributed GROUP BY: a query that creates multiple partial
sources must stay on the Collect path until each bucket has an independently materialized final
fragment and all source-to-bucket edges are resolved by the fragment-instance planner. The
`TableGroupByHashRepartitionTopology` ownership tests reject the common false topology in which
multiple bucket branches remain aliases in one parent fragment.

## Remaining critical path

1. Materialize multi-source final bucket fragments and source x bucket channels in
   `SubPlanGenerator`, exchange insertion and `TableModelQueryFragmentPlanner`; accept it across
   at least two DataNodes.
2. Consume two compatible key partitions in a restricted equi-join, then add duplicate/null/skew
   result-equivalence tests before widening join eligibility.
3. Run the benchmark matrix at DOP `1,2,4,8,16,1`, warm and cold cache, with a fixture large
   enough to avoid measurement noise; preserve result checksums, P50/P95, throughput, CPU, peak
   memory, shuffle bytes and raw plans.
4. Run the weighted-LPT and equal-count arms on deliberately skewed time partitions; compare
   per-morsel and per-driver long-tail evidence, not only query elapsed time.

Dynamic DOP selection and broader executor optimizations are intentionally outside this static
seven-task path. They must not be used to mask an unfinished property or exchange topology.
