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
| `Partitioned(keys)` | **Executable for guarded GROUP BY** | A versioned descriptor, per-row `TABLE_HASH_V1` router, exact channel sink, and default-off GROUP BY consumer exist. It materializes one partial source per input and one final aggregation per bucket (N x P); join remains gated off. |
| Independent cluster E2E | **Partially accepted; multi-source runner ready** | Static paths were accepted previously. A fresh isolated hash-on DataNode emitted `TableHashPartitioningShuffleSinkNode(HashPartitioningSinkOperator)` with two downstream exchanges; a hash-off isolated control produced the same canonical result rows. The multi-source runner archives two explicit endpoints and rejects a missing N x P trace, sink/exchange matrix, or result multiset mismatch; it still needs a real isolated-cluster run. |
| Benchmark matrix | **Assets ready; no published speedup data** | The DOP/warm/cold runner, real CLI adapter, validators and environment capture exist. No P50/P95 or acceleration number may be reported until the fixed-DOP matrix completes on a suitably sized fixture. |
| Multi-query cases | **Static cases and controls ready** | Scan/filter/project/ordered/top-k have a read-only enabled-versus-fallback acceptance runner. GROUP BY has guarded one-source and multi-source topology correctness cases; join records a merge-sort fallback baseline and is rejected for hash repartition. |
| Balance/backpressure | **Instrumentation and parser ready** | Morsel estimated/actual workload, driver wall time and LPT versus equal-count extraction are implemented. The complete skewed-data comparison remains an experiment gate. |
| Reproducible assets | **Implemented** | Deterministic fixture generator, DOP runner, CLI adapter, result validator, single- and multi-source E2E scripts, configuration/version capture, and raw JSON/CSV archive layout are present. |

## Validated hash GROUP BY slices

The enabled configuration is default-off in production and explicitly sets
`enable_table_group_by_hash_repartition=true` with two partitions. The accepted query filters to
one physical device then groups on field `s1`. Its `EXPLAIN ANALYZE` shows one partial aggregation,
`TableHashPartitioningShuffleSinkNode(HashPartitioningSinkOperator)`, two explicit downstream
exchange IDs, and two final aggregation instances. The hash-on and hash-off result-row files have
the same SHA-256. This establishes physical property consumption for that narrow slice.

For multiple direct table-scan sources, the guarded planner now creates one partial aggregation and
one hash sink per source. Each bucket gets its own final aggregation over a `Collect` of the
source-specific exchanges. `AddExchangeNodes` preserves these pre-built hash exchanges rather than
wrapping them in ordinary exchanges. The ownership test verifies that every source has every
bucket channel, all exchanges for a bucket reside in one final fragment, and different buckets use
different final fragments. This proves planner materialization of the N x P topology; it does not
yet prove runtime result equivalence on an isolated multi-DataNode deployment.

## Remaining critical path

1. Run the guarded multi-source N x P GROUP BY acceptance kit across its two explicit isolated
   endpoints, including result equivalence, plan trace and channel/fragment evidence.
2. Implement a hash equi-join executor before selecting a hash join path. The existing 2 x P
   topology gate already rejects incompatible descriptors, split buckets and serial aliases; once
   an executor exists, add duplicate/null/skew result-equivalence tests before widening eligibility.
3. Run the benchmark matrix at DOP `1,2,4,8,16,1`, warm and cold cache, with a fixture large
   enough to avoid measurement noise; preserve result checksums, P50/P95, throughput, CPU, peak
   memory, shuffle bytes and raw plans.
4. Run the weighted-LPT and equal-count arms on deliberately skewed time partitions; compare
   per-morsel and per-driver long-tail evidence, not only query elapsed time.

Dynamic DOP selection and broader executor optimizations are intentionally outside this static
seven-task path. They must not be used to mask an unfinished property or exchange topology.
