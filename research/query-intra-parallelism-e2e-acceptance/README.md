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

# Isolated static-parallelism E2E acceptance

This is a correctness acceptance kit for an already running, isolated IoTDB deployment. It
never starts, stops, reconfigures, clears, creates, or writes to a server. Its only server
statements are `EXPLAIN`, `EXPLAIN ANALYZE`, and `SELECT`. Every endpoint and CLI executable
must be named explicitly on the command line, so the kit cannot accidentally target a
pre-existing experiment cluster.

For the six-query-family result/fallback gate (scan/filter, filter/project, ordered scan, top-k,
GROUP BY, and equi-join), use the separate read-only
[`MULTI_QUERY_CASE_ACCEPTANCE.md`](MULTI_QUERY_CASE_ACCEPTANCE.md) runner. It records exact
result equivalence for static candidates while keeping GROUP BY and join as explicit
fallback-only baselines; it does not report speedups.

It proves three narrow claims for the current static implementation:

1. Text-format `EXPLAIN` on the enabled endpoint contains the `Property enforcement:` trace.
2. An enabled ordered merge-tree candidate and its ordered-parallel fallback return the same
   headers and row sequence.
3. An enabled time-partition morsel candidate and its morsel-disabled fallback return the same
   headers and row multiset (including duplicate counts).
4. The saved morsel `EXPLAIN ANALYZE` contains per-driver actual output, CPU time, TsBlock count,
   elapsed driver wall time, its assigned time partitions, and (for the LPT arm) estimated TsFile
   bytes. This lets a reviewer compare equal-count and LPT-size grouping without treating the
   static estimate as observed work.

It does **not** prove join repartition. The separately scoped single-source GROUP BY experiment
below has a real 1 x P hash exchange. The guarded direct-scan multi-source GROUP BY rewrite has a
separate, stricter N x P acceptance entry point in
[`MULTISOURCE_HASH_GROUPBY_ACCEPTANCE.md`](MULTISOURCE_HASH_GROUPBY_ACCEPTANCE.md): it requires
an isolated hash-on/control pair, a source-by-bucket plan proof, and result equivalence. Until an
operator archives a successful run, it must not be presented as a cluster-accepted result. Likewise,
equality alone is not evidence that a path was used: the raw `EXPLAIN` and `EXPLAIN ANALYZE`
outputs are retained and each must contain its configured execution marker.

## Restricted hash GROUP BY acceptance

For the default-off experimental setting
`enable_table_group_by_hash_repartition=true`, use a fresh isolated deployment and a query that
maps to exactly one physical source. The helper scripts are intentionally mutating and therefore
are not called by the read-only runner:

```bash
bash scripts/load_hash_groupby_fixture.sh <isolated-home> <artifact-dir> 26667
bash scripts/run_hash_groupby_e2e.sh <isolated-home> <artifact-dir> 26667
```

The first script creates only the `p0e2e` fixture on the explicitly selected isolated endpoint.
The second records an `EXPLAIN ANALYZE` and result for `device_id='d0' GROUP BY s1`. Accept it
only when the raw plan contains both
`TableHashPartitioningShuffleSinkNode(HashPartitioningSinkOperator)` and two downstream
`ExchangeNode` instances. Run the same fixture/query on another isolated endpoint with the hash
flag off, then compare canonicalized result rows. This verifies the selected 1 x P path's result
equivalence; it does not justify a speedup claim on the small fixture.

## Deployment contract

Prepare three isolated endpoint states. They may be three separate isolated deployments, or
the same isolated deployment restarted by an operator between runs. The runner itself never
performs that restart. Use a DOP of at least two and keep `enable_dop_estimation=false` for a
fixed-DOP acceptance.

| Endpoint | Required configuration | Purpose |
| --- | --- | --- |
| enabled | property planning, time-partition morsels, and ordered scans enabled | candidate path and EXPLAIN trace |
| ordered fallback | `enable_ordered_parallel_scan=false` | ordered correctness control |
| morsel fallback | `enable_timepartition_morsel=false` | morsel correctness control |

For the P1 balance experiment, run a fourth isolated endpoint with
`enable_timepartition_morsel=true` and `enable_timepartition_morsel_size_weighting=false`. Compare
its exported `EXPLAIN ANALYZE` against an otherwise identical endpoint where the weighting flag is
true. Retain both raw plans and compare the `MORSEL_*` fields per `-morsel-` pipeline: estimated
TsFile bytes are planning evidence, while output rows, CPU time, TsBlock count, and driver wall
time are observed execution evidence. The maximum driver wall time is the scan-stage long-tail
proxy; it is not a whole-query latency claim.

After preserving the two raw `EXPLAIN ANALYZE` stdout files, generate a reviewable comparison
without accessing the server again:

```bash
python3 scripts/summarize_morsel_evidence.py \
  --lpt-plan raw/lpt-morsel-plan.stdout \
  --equal-count-plan raw/equal-count-morsel-plan.stdout \
  --output morsel-balance.json
```

The summarizer rejects plans that lack morsel scheduling, assigned partition, or actual driver
wall-time fields. It preserves every parsed per-driver record and reports maximum driver wall time
and CPU time for both arms. It does not infer a speedup or whole-query latency from these records.

Copy the relevant keys from `config/iotdb-system.properties.template` into each deployment's
own `conf/iotdb-system.properties`; do not edit a shared production/baseline configuration. Pass
those exact files through `--config` so they are copied into the evidence directory.

The table must be a simple unquoted `database.table` identifier with columns
`device_id STRING TAG`, `s1 INT64 FIELD`, and `s2 DOUBLE FIELD`. It needs non-empty rows from at
least two devices and two physical time partitions. `sql/fixture.sql` is a deliberately
**manual, mutating** smoke fixture. Review and execute it only against a new isolated database;
the runner will never execute it. Increase its data before using this kit as performance evidence.

## Run

The CLI password is read from the explicitly named environment variable and is never written to
the artifact. For a standard all-in-one distribution, the executable is normally
`<isolated-home>/sbin/start-cli.sh`; verify the path with `--help` first.

```bash
export IOTDB_E2E_PASSWORD='your-isolated-password'
python3 scripts/run_e2e_acceptance.py \
  --output /root/iotdb-next-artifacts/e2e-$(date -u +%Y%m%dT%H%M%SZ) \
  --table p0e2e.telemetry \
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

The runner records the exact commands with passwords redacted, CLI raw output, canonical CSV
extracted from CLI tables, plan traces, copied configuration, Git context, and
`acceptance.json`. It fails on a missing property trace, missing configured marker, malformed
CLI table, non-empty output directory, command failure, or either result mismatch.

The default `TableMergeSortOperator` and `-morsel-` markers are intentionally tied to this
prototype. If a later implementation changes its observable operator names, use
`--ordered-marker` and `--morsel-marker` with the reviewed replacement strings and retain the
raw plan outputs. Do not weaken a marker to a generic word such as `scan`.
