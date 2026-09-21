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

It proves three narrow claims for the current static implementation:

1. Text-format `EXPLAIN` on the enabled endpoint contains the `Property enforcement:` trace.
2. An enabled ordered merge-tree candidate and its ordered-parallel fallback return the same
   headers and row sequence.
3. An enabled time-partition morsel candidate and its morsel-disabled fallback return the same
   headers and row multiset (including duplicate counts).

It does **not** prove hash repartition, `Partitioned(keys)`, group-by repartition, or join
repartition. Those capabilities need their own physical-exchange acceptance once implemented.
Likewise, equality alone is not evidence that a path was used: the two `EXPLAIN ANALYZE` outputs
are retained and each must contain its configured execution marker.

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
