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

# Multi-source hash GROUP BY acceptance

Purpose: define the topology and result checks for the restricted multi-source GROUP BY path.

Abstract: repeated group keys must reach the same final aggregation bucket. Result equality alone cannot establish the route used by a query. This document specifies cluster preparation, concrete ID checks, result comparison, and the scope of the archived A acceptance.

Conclusion: acceptance requires distinct running DataNodes, completed fragment instances, unique exchange edges, bucket ownership, and equal complete result multisets. The A archive additionally covers nullable keys, skewed data, and empty input with typed JDBC results. These fixtures establish correctness for the selected queries.

## Query requirements

Each source must send rows for a key to the same final bucket. For example, two sources containing two and three rows for one key must produce one group with a count of five. A two-source, two-bucket query must have four distinct exchange edges.

The runner reads from explicitly selected isolated deployments. Cluster setup, configuration, and fixture loading are separate operator actions. The runner issues explanatory queries, a DataNode query, and result queries.

Sources: [runner](scripts/run_multisource_hash_groupby_e2e.py), [fixture](sql/multisource_hash_groupby_fixture.sql), and [cluster launcher](linux-isolated-cluster/README.md).

## Isolated cluster preparation

Prepare a candidate and control deployment from the same source and deterministic fixture. Each deployment needs its own configuration and data directories. Place the fixture across the requested source count.

The candidate enables property planning and the restricted hash path. The control disables both flags. Keep other relevant settings and fixed query parallelism equal.

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

For two sources and two buckets, the default minimum is five fragment instances: two source owners, two bucket owners, and the result owner. The runtime checks require finished fragments and verify the owners of individual exchange edges.

Sources: the runner checks the candidate's actual registered DataNode IDs and runtime source endpoints. The A JDBC archive separately captures both deployments and their fixture identities.

## Runner invocation

Use a new or empty output directory and distinct endpoint and isolation labels. The password comes from the named environment variable. Commands saved in the archive redact it.

```bash
python3 scripts/run_multisource_hash_groupby_e2e.py --self-test

python3 scripts/run_multisource_hash_groupby_e2e.py \
  --output /root/iotdb-next-artifacts/multisource-hash-review \
  --table p0multi.telemetry \
  --expected-sources 2 --expected-buckets 2 --expected-datanodes 2 \
  --minimum-fragment-instances 5 \
  --enabled-isolation-id hash-enabled \
  --enabled-cli /root/iotdb-hash-enabled/sbin/start-cli.sh \
  --enabled-host 127.0.0.1 --enabled-port 26667 \
  --enabled-config /root/iotdb-hash-enabled/conf/iotdb-system.properties \
  --baseline-isolation-id hash-disabled \
  --baseline-cli /root/iotdb-hash-disabled/sbin/start-cli.sh \
  --baseline-host 127.0.0.1 --baseline-port 36667 \
  --baseline-config /root/iotdb-hash-disabled/conf/iotdb-system.properties
```

Set the configured password environment variable before the command. The runner's result acceptance requires a positive minimum row count. Empty-input service acceptance uses the independent JDBC fixture harness described below.

Sources: command-line arguments and configuration validation are defined in [runner](scripts/run_multisource_hash_groupby_e2e.py).

## Planning and runtime evidence

Planning evidence records the property decision and the unique source, bucket, and exchange matrix IDs. Runtime evidence comes from a separate analysis query. Each query has its own IDs, so ownership checks use the IDs within the corresponding runtime output.

The runtime parser checks each hash sink's downstream channels. Each source must own the requested number of unique edges. Every bucket column must terminate at exactly one final aggregation fragment, and different buckets must have different owners. Hash source fragments must span the requested number of actual DataNode endpoints.

For example, the first source can expose edges 101 and 102, while the second exposes 105 and 106. The first bucket must own 101 and 105, and the second must own 102 and 106. A duplicated edge or a column split across fragments fails acceptance.

The control analysis must show the hash-disabled path. All accepted fragments must finish. The runner also checks that the declared fragment count agrees with the unique fragment IDs it parsed.

Sources: `enabled_explain`, `enabled_datanodes`, and `enabled_explain_analyze` in the generated `multisource-hash-groupby-acceptance.json`, with raw query output retained in the archive.

## Complete result comparison

The CLI runner compares complete headers and row multisets, including duplicates, and retains canonical CSV files and SHA-256 values. CLI parsing requires a completed result footer and a consistent row count. An empty stdout cannot establish successful query execution.

The A JDBC harness additionally preserves SQL types and null values. It encodes rows as compact JSON, sorts the encoded rows, joins them with newlines, and appends a final newline. Its empty result is one newline. Hashes are comparable only when both sides use this same encoding.

For example, one result containing two copies of a row differs from a result containing one copy, even if both contain the same distinct values. Candidate, control, and deterministic expected output must all match.

Sources: CLI comparison is implemented in [runner](scripts/run_multisource_hash_groupby_e2e.py). Typed results are retained in the A archive's expected-results and per-case JSONL artifacts.

## A service acceptance archive

The rebuilt A candidate and control each run one ConfigNode and two DataNodes. Four fixtures pass complete expected-result checks: balanced repeated keys, a nullable key, skewed input, and an empty filtered input. Each nonempty fixture also passes the four-edge ownership check.

The balanced fixture contains twelve rows and three groups. The nullable fixture contains sixteen rows and four groups. The skewed fixture contains eighty-four rows, split into groups of eighty and four. The empty query completes with zero groups.

The archive binds source hashes to the distribution, actual loaded classes, JVMs, and native libraries. Six nodes are stopped and their ports are released after acceptance. These results apply to the recorded correctness fixtures. Performance requires a separate measurement.

Sources on sc01: `/root/property-a-artifacts/A6-20261028T080437+0000/two-datanode-05/acceptance.json`, `expected-results.json`, `frozen-loaded-runtime.json`, and `stop-verification.json`. The distribution binding is in the same A6 root under `isolated-distribution-06/distribution-binding.json`. Parser self-tests are recorded under `runtime-ownership-01/exit.json`.
