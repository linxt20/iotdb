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

# Query-intra-parallelism P0 benchmark kit

This directory is the reproducible evidence kit for the static query-intra-parallelism
prototype. It deliberately separates *workload definition*, *measurement adapter*,
and *raw evidence*. Do not report a speedup until all three are archived.

Scope is deliberately narrow: this kit evaluates fixed, static DOP only. It neither
implements nor evaluates dynamic DOP, task stealing, pipeline fusion, SIMD, asynchronous
prefetch, or other executor optimizations. It must not be used to make a universal
speedup claim: every reported acceleration is scoped to its SQL, data shape, cache state,
configuration, and observed parallel execution marker.

It is designed for the isolated server deployment, not for the pre-existing experiment
cluster. The kit never starts, stops, reconfigures, or clears a server by itself. Those
potentially disruptive actions are explicit commands supplied by the operator.

## Workload contract

The SQL files use `${DATABASE}` and `${TABLE}` placeholders. The fixture is a table-model
table with `device_id` as a TAG and `time`, `s1`, and `s2` columns. The intended fixture is
64 devices x 10 time partitions x 100,000 rows per device/partition. `s1` must be an
increasing integral sequence and `s2 = s1 + 0.5`.

The six workload queries cover the promised classes:

| Query | Purpose | Expected parallelism outcome |
| --- | --- | --- |
| `scan_filter` | Full scan with a non-prunable modulo predicate | scan/morsel acceleration candidate |
| `filter_project` | Filter and project after scan | candidate; isolates downstream single-stream cost |
| `ordered_scan` | device/time ordering | ordered merge-tree candidate only when its safety contract holds |
| `top_k` | global order + limit | required fallback/control case |
| `group_by` | aggregation by device | repartition/aggregation baseline; do not claim key-partition acceleration until it is implemented |
| `self_join` | equality join on device/time | repartition/join baseline; do not claim key-partition acceleration until it is implemented |

Generate the canonical deterministic input with only Python's standard library:

```bash
python3 scripts/generate_fixture.py --output /root/bench-fixture
```

It writes ten CSV shards with `Time,device_id,s1,s2` headers and a `fixture-manifest.json`.
Create the schema first by rendering `workload/schema.sql`, then import each shard with the
isolated distribution's `import-data.sh` in table dialect (for example, `-sql_dialect table
-ft csv -db benchdb -table bench -f <shard>`). Validate the exact CLI flags against the built
distribution's `import-data.sh -help` before a full load; retain the manifest and importer
stdout/stderr alongside the benchmark output. A tiny fixture (for example `--devices 2
--partitions 1 --rows-per-partition 10`) is the required import smoke test before generating
the 64-million-row dataset.

`validation/` contains result-producing forms of the same semantics. Export exactly one
CSV per DOP and compare it with DOP=1 using `validate_results.py`. It compares complete
rows and multiplicity, rather than an XOR digest; that detects even-count duplicates.

## Measurement adapter

`run_matrix.py` is intentionally database-client agnostic. The supplied `--query-command`
must execute one rendered SQL file and print **one JSON object** to stdout. It receives these
format variables: `{sql_file}`, `{query_id}`, `{dop}`, `{cache_mode}`, `{iteration}`, and
`{attempt_dir}`. Its JSON object must contain server-side `query_ms`; it may additionally
contain `planning_ms`, `execution_ms`, `result_rows`, `cpu_pct`, `peak_rss_bytes`, and
`shuffle_bytes`.

Using client wall-clock time as `query_ms` is prohibited: client transfer/deserialization
can dominate this workload. The adapter should collect IoTDB server-side planning and
FragmentInstance wall time, while using a normal query (not `EXPLAIN ANALYZE`) for the timed
execution because EXPLAIN ANALYZE can alter the table-model plan.

`scripts/iotdb_cli_adapter.py` supplies the standard CLI bridge. It invokes an already-running
isolated endpoint only; it never starts, stops, reconfigures, or clears anything. It archives
the actual CLI stdout/stderr, a redacted command manifest, SQL/result hashes, and then prints
the one JSON metrics object required by `run_matrix.py`. To prevent an accidental client-time
claim, it requires an explicit regular expression for a **server-side** millisecond field that
the deployed CLI or an operator-maintained server-metrics wrapper emits. If that field is not
available, the command fails after retaining the raw CLI evidence rather than inventing
`query_ms` from local elapsed time.

Example measured matrix shape (replace the regex with the field documented by the isolated
deployment):

```bash
python3 scripts/run_matrix.py \
  --database benchdb --table benchdb.bench \
  --query-command 'python3 scripts/iotdb_cli_adapter.py measure --cli /root/iotdb-next-deploy/sbin/start-cli.sh --host 127.0.0.1 --port 11710 --database benchdb --password-env IOTDB_PASSWORD --sql-file {sql_file} --raw-dir {attempt_dir} --server-query-ms-regex "server_query_ms=([0-9.]+)"' \
  --dop-command '/root/iotdb-next-deploy/bin/set-isolated-dop.sh {dop}' \
  --config /root/iotdb-next-deploy/conf/iotdb-system.properties \
  --output /root/iotdb-next-artifacts/p0-$(date -u +%Y%m%dT%H%M%SZ)
```

The outer matrix captures the git SHA, copied configuration, CPU/memory/storage inventory,
warm/cold label, DOP sequence, raw attempt directories, and P50/P95 summaries. The adapter
adds endpoint/database/SQL provenance within every attempt. Pass any deployment-specific,
non-secret CLI flag with repeated `--cli-arg`; secrets are read only from the named environment
variable and are redacted from the archived command metadata.

`--dop-command` is run before each DOP step and must perform any required isolated-node
restart. The default sequence is `1,2,4,8,16,1`: the final DOP=1 is a cache-drift control,
not a sixth independent setting. Keep every raw attempt even when it is an outlier.

### Server time and DataNode resources

`scripts/server_current_query_metrics.py` is the concrete server-side timing bridge for an
isolated table-model endpoint. It first snapshots `information_schema.current_queries`, executes
the supplied ordinary SQL once, and then accepts only a *new* `FINISHED` entry whose canonical
SQL fingerprint matches. Its `cost_time` (server seconds) is exported as `query_ms`; CLI
`It costs ...` / local elapsed time is neither parsed nor archived as a metric. The DataNode
must start with `query_cost_stat_window` set to a positive number of minutes, otherwise IoTDB
does not retain completed rows (the default is zero). Run one timed query at a time on the
endpoint, retain the generated `query_id`, and keep the history snapshots in the attempt folder.

For example, the query wrapper can be nested inside the resource collector (the SQL must already
be fully qualified or otherwise not need a preceding `USE` statement):

```bash
python3 scripts/collect_datanode_proc.py \
  --pids 142732,142733 --output /root/p0/attempt-01/proc -- \
  python3 scripts/server_current_query_metrics.py \
    --cli /root/iotdb-next-deploy/sbin/start-cli.sh --host 127.0.0.1 --port 21667 \
    --sql-file /root/p0/attempt-01/query.sql --raw-dir /root/p0/attempt-01/server
```

`collect_datanode_proc.py` is Linux-only and read-only: it samples each named DataNode PID from
`/proc/<pid>/stat` and `/proc/<pid>/status`, records raw JSONL, verifies the process start-time
on every sample to reject PID reuse, and archives `cpu_core_pct` plus peak combined RSS. A value
of 100 CPU percent means one full CPU core, not 100 percent of the host. The PIDs must come from
the benchmark deployment manifest, not a broad `pgrep` that could capture another experiment.

Prometheus is useful only for process-level corroboration. A dedicated deployment may enable
`dn_metric_reporter_list=PROMETHEUS` and assign a different
`dn_metric_prometheus_reporter_port` to every DataNode. Existing `query_execution`,
`driver_scheduler`, and `data_exchange_*` metrics are process-cumulative and **not query-scoped**;
they must not become `query_ms` or be attributed to a query while concurrent work exists.
Existing `data_exchange_size` measures live-handle counts, and `data_exchange_cost/count` expose
time/block-count information. This build has no exported cumulative **shuffle byte** counter, so
`shuffle_bytes` remains unmeasured/null until a separately reviewed instrumentation change adds a
byte counter at the actual remote TsBlock payload path. Do not report zero in its place.

For matrix use, prefer the one-command composition adapter
`scripts/server_query_metrics_with_proc.py`. It runs the same normal SQL through
`server_current_query_metrics.py`, wraps only that command in the `/proc` collector, requires
**exactly two explicit DataNode PIDs**, archives both child evidence directories below the matrix
attempt, and prints exactly one JSON line for `run_matrix.py`. It does not start, stop, change,
or clear a node. `cpu_pct` is the collector's combined CPU-core percentage (100 = one full core),
and `peak_rss_bytes` is the combined two-DataNode peak. `shuffle_bytes` is explicitly JSON
`null`, never `0`, until a query-scoped byte counter exists; therefore the P0 verifier will
correctly reject a matrix as incomplete rather than allow an invented shuffle metric.

```bash
python3 scripts/run_matrix.py \
  --database benchdb --table benchdb.bench \
  --query-command 'python3 scripts/server_query_metrics_with_proc.py --cli /root/iotdb-next-deploy/sbin/start-cli.sh --host 127.0.0.1 --port 11710 --password-env IOTDB_PASSWORD --datanode-pids 142732,142733 --sql-file {sql_file} --raw-dir {attempt_dir}' \
  --dop-command '/root/iotdb-next-deploy/bin/set-isolated-dop.sh {dop}' \
  --config /root/iotdb-next-deploy/conf/iotdb-system.properties \
  --output /root/iotdb-next-artifacts/p0-$(date -u +%Y%m%dT%H%M%SZ)
```

For a cold-cache pass, require an explicit, audited command that affects only the isolated
deployment (for example, a deployment-local restart plus the lab-approved cache reset):

```bash
  --cache-modes warm,cold \
  --cold-cache-command '/root/iotdb-next-deploy/bin/reset-isolated-cache.sh'
```

The cold command is called before every measured cold attempt. If the operator cannot reset
the cache safely, omit `cold` and record it as unmeasured; never label a warmed result cold.

## Evidence layout

Each invocation creates the following self-contained directory:

```text
<output>/
  manifest.json                    # invocation, query/DOP protocol, git SHA
  environment/                     # machine, git state, copied server config
  raw/<step>/<query>/attempt-*/     # SQL, adapter stdout/stderr, one metrics JSON
  summary/attempts.csv             # all raw numeric observations
  summary/summary.csv              # P50/P95 and medians, never a replacement for raw data
```

Archive this directory unchanged with the thesis artifact. `shuffle_bytes`, CPU, and memory
are nullable until the corresponding server metric is available; a blank value means
**unmeasured**, never zero.

## Result equivalence

Have the adapter export validation SQL to CSV, then run:

```bash
python3 scripts/validate_results.py \
  --baseline /root/results/dop1/ordered_scan.csv \
  --candidate /root/results/dop16/ordered_scan.csv --ordered
```

For an ordinary IoTDB CLI ASCII result table, create each of those CSVs without manually
copying terminal output:

```bash
IOTDB_PASSWORD='...' python3 scripts/iotdb_cli_adapter.py export \
  --cli /root/iotdb-next-deploy/sbin/start-cli.sh --host 127.0.0.1 --port 11710 \
  --database benchdb --table benchdb.bench --sql-file validation/ordered_scan.sql \
  --raw-dir /root/results/dop16/ordered_scan.raw --output /root/results/dop16/ordered_scan.csv
```

Run `python3 scripts/iotdb_cli_adapter.py --self-test` to check its metric and pipe-table
parsers without connecting to a service. The exporter refuses malformed or ambiguous output;
the unmodified CLI stdout/stderr remain in `*.raw` for diagnosis.

Without `--ordered`, rows are canonically sorted before hashing and comparing, appropriate
for unordered scans. With `--ordered`, row sequence is also checked. The command writes a
JSON report and exits non-zero on a mismatch. Run it for every workload/DOP pair and retain
the reports under `<output>/validation/`. Use the stable layout
`<output>/validation/<query_id>/dop-<dop>/report.json`, including the DOP=1 self-check.

Before reporting a matrix, run the following read-only acceptance gate. It rejects incomplete
DOP/cache cells, nullable CPU/RSS/shuffle fields, missing raw attempt records, missing captured
SHA/configuration, and failed or absent result-equivalence reports:

```bash
python3 scripts/verify_p0_matrix.py --output /root/iotdb-next-artifacts/p0-<timestamp>
```

The gate does not contact the cluster and never changes the evidence directory. A failed gate
means the run is incomplete evidence, not a zero or a successful performance result.

## What this kit does not prove

This kit is a test protocol, not a performance claim. A plan trace, an enabled flag, or a
configured DOP is insufficient evidence. Record the normal-query execution marker that
shows scan parallelism actually occurred; record fallback plans for top-k and unsupported
ordered cases; and do not attribute group-by/join speedups to `Partitioned(keys)` before a
real hash/exchange repartition consumes that property.
