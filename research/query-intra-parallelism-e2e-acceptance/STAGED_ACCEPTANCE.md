<!--

    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

-->

# Staged multi-query acceptance

This protocol turns the existing E2E and benchmark kits into one reviewable acceptance sequence.
It covers scan/filter, filter/project, ordered scan, top-k control, and time-partition morsels.
It deliberately keeps grouped aggregation and equi-join as **DOP=1 semantic baselines** until a
real key-aware hash exchange has been wired and accepted. A `Partitioned(keys)` planner property,
a DOP setting, or a plan annotation alone does not change that boundary.

The machine and service lifecycle remain operator-owned. The E2E runner and the matrix runner
require explicitly named isolated endpoints and commands; this verifier is read-only and only
checks their archived files. Never point any command at the existing baseline experiment service.

## Case contract

The machine-readable source of truth is
[`manifests/multi-query-cases.json`](manifests/multi-query-cases.json). Its statuses have these
meanings:

| Status | Cases | Required conclusion |
| --- | --- | --- |
| `static_parallel_candidate` | scan/filter, filter/project, ordered scan, morsel | Verify DOP=1 vs DOP>1 result equivalence and retain raw metrics/plan evidence. A speedup is measured from the archive, never assumed. |
| `required_fallback_control` | top-k | Retain all DOP results and EXPLAIN traces as a global-order control. It is not a scan-parallel acceleration claim. |
| `baseline_only_until_hash_repartition` | group-by, self-join | Retain one DOP=1 result and plan for later comparison. Do not publish a key-shuffle, co-partitioned execution, or acceleration result. |

## Stage 0 — fixed inputs

1. Record the checked-out Git SHA and copy the isolated configuration.
2. Create and import the deterministic benchmark fixture using the benchmark kit. Run its small
   import smoke test first.
3. Keep the same database/table and immutable fixture manifest throughout all stages.
4. Use a fixed-DOP deployment (`enable_dop_estimation=false`) and retain every DOP transition
   command stdout/stderr. The accepted sequence is `1,2,4,8,16,1`; the final DOP=1 detects cache
   drift and is not an additional independent setting.

## Stage 1 — endpoint correctness

Run the existing read-only E2E kit against the three isolated endpoints: enabled,
ordered-fallback, and morsel-fallback. It checks the textual property trace, ordered row sequence,
and morsel row multiset. It also retains the plan markers required to distinguish an enabled path
from mere result equality.

```bash
export IOTDB_E2E_PASSWORD='isolated-password'
python3 scripts/run_e2e_acceptance.py \
  --output /root/iotdb-next-artifacts/staged-e2e \
  --table benchdb.bench \
  --enabled-cli /root/iotdb-next-enabled/sbin/start-cli.sh \
  --enabled-host 127.0.0.1 --enabled-port 16667 \
  --ordered-fallback-cli /root/iotdb-next-ordered-fallback/sbin/start-cli.sh \
  --ordered-fallback-host 127.0.0.1 --ordered-fallback-port 26667 \
  --morsel-fallback-cli /root/iotdb-next-morsel-fallback/sbin/start-cli.sh \
  --morsel-fallback-host 127.0.0.1 --morsel-fallback-port 36667 \
  --config /root/iotdb-next-enabled/conf/iotdb-system.properties \
  --config /root/iotdb-next-ordered-fallback/conf/iotdb-system.properties \
  --config /root/iotdb-next-morsel-fallback/conf/iotdb-system.properties
```

## Stage 2 — static-query measurement matrix

Run only the static families through the supplied matrix runner. `group_by` and `self_join` are
intentionally omitted here: they have no true hash repartition consumer yet. `top_k` remains in
the matrix as a required global-order fallback control, not as a speedup target.

```bash
python3 ../query-intra-parallelism-benchmark/scripts/run_matrix.py \
  --database benchdb --table benchdb.bench \
  --queries scan_filter,filter_project,ordered_scan,top_k \
  --dops 1,2,4,8,16,1 --cache-modes warm \
  --query-command 'python3 /root/bench/query_adapter.py --sql {sql_file} --out {attempt_dir}' \
  --dop-command '/root/iotdb-next-deploy/bin/set-isolated-dop.sh {dop}' \
  --config /root/iotdb-next-deploy/conf/iotdb-system.properties \
  --output /root/iotdb-next-artifacts/staged-matrix
```

If a separately audited cold-cache command exists, add `--cache-modes warm,cold` and
`--cold-cache-command`. Do not call warmed measurements cold. The adapter must report server-side
`query_ms`; client transfer time is not a substitute. Preserve nullable CPU, memory, and shuffle
metrics as unmeasured rather than zero.

## Stage 3 — result-equivalence artifacts

For every static case and each DOP in `2,4,8,16`, export the validation SQL result to CSV and
compare it to the DOP=1 export. Store each output using the exact name required by the verifier:
`<case>-dop-<dop>.json`.

```bash
python3 ../query-intra-parallelism-benchmark/scripts/validate_results.py \
  --baseline /root/results/scan_filter-dop-1.csv \
  --candidate /root/results/scan_filter-dop-16.csv \
  --report /root/iotdb-next-artifacts/staged-validation/scan_filter-dop-16.json

python3 ../query-intra-parallelism-benchmark/scripts/validate_results.py \
  --baseline /root/results/ordered_scan-dop-1.csv \
  --candidate /root/results/ordered_scan-dop-16.csv --ordered \
  --report /root/iotdb-next-artifacts/staged-validation/ordered_scan-dop-16.json
```

Use `--ordered` for `ordered_scan` and `top_k`; omit it for scan/filter and filter/project.
Record a DOP=1 plan/result for `group_by` and `self_join` separately under `baseline/`; that
evidence is intentionally excluded from a parallel acceptance decision.

## Stage 4 — reproducible acceptance decision

Run the new verifier after stages 1–3. It does not touch IoTDB; it rejects incomplete DOP coverage,
missing or failed result reports, a missing E2E comparison, or a wrongly ordered validator mode.
The report permanently records the group-by/join claim prohibition.

```bash
python3 scripts/verify_staged_acceptance.py \
  --stage full \
  --e2e-output /root/iotdb-next-artifacts/staged-e2e \
  --matrix-output /root/iotdb-next-artifacts/staged-matrix \
  --validation-dir /root/iotdb-next-artifacts/staged-validation \
  --output /root/iotdb-next-artifacts/staged-decision
```

For an intermediate endpoint-only gate, use `--stage e2e`; for a matrix-only rerun, use
`--stage matrix`. A successful decision says the archived static evidence is internally complete.
It does not claim universal acceleration and cannot unblock group-by/join key partitioning.
