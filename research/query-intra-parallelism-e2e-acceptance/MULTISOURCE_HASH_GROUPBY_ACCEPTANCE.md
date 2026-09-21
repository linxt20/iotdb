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

# Multi-source N x P hash GROUP BY acceptance

This is the isolated-cluster acceptance gate for the guarded multi-source GROUP BY rewrite. It
is deliberately separate from the static-path E2E runner: a matching result alone is not evidence
of a source-by-bucket exchange. The runner is read-only against IoTDB. It will not create data,
change configuration/DOP, or start, stop, or restart a service.

## Operator preparation

Prepare **two distinct, isolated DataNode endpoints** from the same Git SHA and fixture:

| Endpoint | Required state |
| --- | --- |
| Candidate | `enable_property_driven_planning=true`, `enable_table_group_by_hash_repartition=true`, fixed DOP, and at least two direct table-scan sources. |
| Control | The two flags above are both `false`; all other relevant settings and the fixture are identical. |

Use the manual [fixture](sql/multisource_hash_groupby_fixture.sql) only against fresh isolated
databases. Its duplicated group keys are intentional: a wrong partitioning route will change a
final `COUNT(*)`. Data placement is operator-owned, so the runner requires the expected source
count and rejects a plan with fewer sources. For a 2 x 2 run, configure both deployments with
the respective copied configuration files; do not edit a shared configuration.

```properties
# candidate
enable_property_driven_planning=true
enable_table_group_by_hash_repartition=true
table_group_by_hash_repartition_partition_count=2

# control
enable_property_driven_planning=false
enable_table_group_by_hash_repartition=false
table_group_by_hash_repartition_partition_count=2
```

## Run

The password is read only from an explicitly named environment variable and never stored in the
archive. `--enabled-isolation-id` and `--baseline-isolation-id` are auditable operator labels;
the program rejects identical labels or endpoints. The output directory must be new or empty.

```bash
export IOTDB_E2E_PASSWORD='isolated-password'
python3 scripts/run_multisource_hash_groupby_e2e.py \
  --output /root/iotdb-next-artifacts/multisource-hash-$(date -u +%Y%m%dT%H%M%SZ) \
  --table p0multi.telemetry \
  --expected-sources 2 --expected-buckets 2 \
  --enabled-isolation-id hash-enabled-dn-a \
  --enabled-cli /root/iotdb-hash-enabled/sbin/start-cli.sh \
  --enabled-host 127.0.0.1 --enabled-port 26667 \
  --enabled-config /root/iotdb-hash-enabled/conf/iotdb-system.properties \
  --baseline-isolation-id hash-disabled-dn-b \
  --baseline-cli /root/iotdb-hash-disabled/sbin/start-cli.sh \
  --baseline-host 127.0.0.1 --baseline-port 36667 \
  --baseline-config /root/iotdb-hash-disabled/conf/iotdb-system.properties
```

## What acceptance proves

`multisource-hash-groupby-acceptance.json` is accepted only when both enabled plans include:

1. `Property enforcement:`, `TableHashPartitioningShuffleSinkNode`, and `TABLE_HASH_V1`;
2. an exact `sources=N partitions=P (N x P experimental path)` trace matching command-line
   expectations;
3. at least `N` hash-sink markers and at least `N*P` `ExchangeNode` markers; and
4. the same grouped-result header and row multiset as the hash-disabled control, with canonical
   CSVs and SHA-256 values retained.

The control's `EXPLAIN ANALYZE` must contain none of the hash-path markers. The archive also
contains raw stdout/stderr, password-redacted commands, generated SQL, copied configuration, and
Git context. This proves only the configured GROUP BY correctness slice; it is not a latency,
throughput, join, or general-query acceleration claim.

Run the offline self-test before a server run:

```bash
python3 scripts/run_multisource_hash_groupby_e2e.py --self-test
```
