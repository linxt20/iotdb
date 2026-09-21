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

# Seven-task dependency plan for table-model query parallelism

The seven tasks are not seven independent features.  The critical path is a truthful row-key hash
exchange; its group-by and join consumers cannot be enabled by a property annotation alone.  The
other streams should produce reusable tests, datasets, and observability while that path is being
implemented.

```text
T1 static property matrix ───────┬──────────────────────> T3 E2E acceptance
                                 ├──────────────────────> T4 benchmark matrix
                                 └──────────────────────> T5 multi-query cases

T2 true Partitioned(keys) ──> T2a exchange contract ─┐
                                                     ├─> T2c N×N wiring ─> group-by consumer
                             T2b row hash router ────┘                     └> equi-join consumer
                                                                                 │
                                                                                 ├─> T3 full cluster E2E
                                                                                 ├─> T4 group/join measurements
                                                                                 └─> T5 group/join speedup cases

T6 load-balance/backpressure evidence ───────────────────────────────────────> T4/T5 interpretation
T7 reproducible experiment assets ───────────────────────────────────────────> T3/T4/T5/T6 execution
```

## Task map and current gates

| ID | Task | Depends on | Parallel work that may start now | Completion gate |
| --- | --- | --- | --- | --- |
| T1 | Static property-enforcement coverage matrix | none | plan assertions and EXPLAIN trace assertions | every listed operator has required/provided/enforcer/fallback coverage; no unsupported parallel claim |
| T2 | True `Partitioned(keys)` | none, but internally ordered as T2a → T2c and T2b → T2c | contract/serialization and row routing can proceed in parallel | rows with equal key always reach one bucket; descriptor survives fragment serialization; planner consumes it |
| T3 | Isolated cluster E2E | T1 for static paths; T2 for group/join paths | fixture, runner, archive protocol | real DataNode results agree for enabled, ordered/fallback, morsel/fallback, then hash group/join |
| T4 | Comparable benchmark matrix | T7; T2 only for group/join acceleration claims | workload, warm/cold runner, statistics/export | DOP 1/2/4/8/16 data with result checksum, P50/P95, throughput, CPU, memory and shuffle bytes |
| T5 | Multi-query optimization cases | T1; T2 for group/join; T4 for reported gains | scan/filter/ordered/top-k cases and explicit fallback cases | one explained gain and one fallback per family; group/join held as baseline until T2 |
| T6 | Load balance and backpressure evidence | existing morsel implementation; T2 later extends to hash skew | estimated/actual workload and driver-tail collection | equal-count versus weighted-LPT comparison with per-morsel and per-driver raw evidence |
| T7 | Reproducible experiment assets | none | fixture generator, version/config capture, validators | one command sequence can create/load/warm/run/validate/archive every T3–T6 experiment |

## Parallel ownership for the current iteration

* **Hash contract worker (T2a):** table exchange descriptor, serialization, visitors and a
  default-off contract.  It must not select the operator yet.
* **Row-router worker (T2b):** deterministic row-to-bucket primitive, independent of a particular
  plan-node representation.  It must prove no dropped, duplicated, or split equal keys.
* **Evidence worker (T6):** morsel load/long-tail observability and comparison evidence; this is
  useful before hash repartition and later becomes the skew guard for T2.
* **Integrator:** reconcile T2a/T2b into T2c, add grouped aggregation first, then the restricted
  equi-join path; merge only independently tested commits.  T3–T5 are rerun at every consumer
  milestone.

T1 and T7 are already available as a foundation in this branch, but remain inputs, not proof that
T2/T3/T4/T5/T6 have completed.  Dynamic DOP and generic execution-engine optimization are
deliberately outside this seven-task critical path.
