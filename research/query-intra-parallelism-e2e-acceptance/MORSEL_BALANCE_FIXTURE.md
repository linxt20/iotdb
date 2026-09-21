<!-- Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0. -->

# Skewed time-partition morsel fixture

This is a **manual, mutating** fixture preparation procedure for the read-only
`scripts/run_morsel_balance_evidence.py` runner. Use two new, otherwise-identical isolated
deployments: the equal-count arm has `enable_timepartition_morsel=true` and
`enable_timepartition_morsel_size_weighting=false`; the LPT arm changes only the latter to
`true`. Do not point either arm at the existing N x P correctness cluster.

The goal is to create actual TsFiles in six physical time partitions, with two deliberately
large adjacent partitions. At fixed DOP 2, equal-count scheduling will group the first three
partitions and leave both large partitions in one morsel. LPT scheduling should put one large
partition in each morsel. This is a planning/balance experiment, not a general speedup claim.

## Generate deterministic input

Run the generator on the server, once, and copy the immutable directory to both isolated
endpoints. The small fixture has 6,160 rows: it is appropriate for validating the artifact
pipeline, not for reporting a performance number.

```bash
python3 research/query-intra-parallelism-benchmark/scripts/generate_fixture.py \
  --output /root/morsel-skew-fixture \
  --devices 2 --partitions 6 \
  --rows-per-partition-list 1500,1500,20,20,20,20 \
  --partition-interval-ms 10000 --row-interval-ms 1
```

The generated timestamps occupy `[0, 10000)`, `[10000, 20000)`, through
`[50000, 60000)`. Its manifest records the exact per-partition row counts. `s1` is globally
unique per device and `s2 = s1 + 0.5`, so the result query can retain all rows and compare
multisets exactly.

## Load each isolated arm

Against each fresh isolated table-model endpoint, execute the following schema first. The
database partition interval must match the generator; relying on a server-default interval does
not prove that the CSV shards are physical time partitions.

```sql
CREATE DATABASE morselbench WITH (time_partition_interval=10000);
USE morselbench;
CREATE TABLE telemetry (
  device_id STRING TAG,
  s1 INT64 FIELD,
  s2 DOUBLE FIELD
);
```

Import **one CSV shard at a time** in table dialect, then issue `FLUSH;` before importing the
next shard. Preserve each importer stdout/stderr and the SQL command log. For a standard
distribution, the import invocation is typically:

```bash
<isolated-home>/tools/import-data.sh -sql_dialect table -ft csv \
  -db morselbench -table telemetry -f /root/morsel-skew-fixture/fixture-partition-00.csv
```

Validate that exact flags with that distribution's `import-data.sh -help` first. Repeat for
`fixture-partition-00.csv` through `fixture-partition-05.csv`, flushing after each import. The
flush is required: LPT uses the DataNode's TsFile byte snapshot. A still-memtable-only fixture
has no stable byte estimate and intentionally falls back to equal-count scheduling.

## Fixed-DOP and evidence gate

Set both deployments to fixed DOP 2 with `enable_dop_estimation=false`, restart only their
isolated DataNodes through the audited lifecycle helper, then use these SQL files:

```sql
-- explain-analyze.sql
EXPLAIN ANALYZE SELECT device_id, time, s1, s2 FROM morselbench.telemetry WHERE time >= 0;

-- result.sql
SELECT device_id, time, s1, s2 FROM morselbench.telemetry WHERE time >= 0;
```

Run `scripts/run_morsel_balance_evidence.py` with the two distinct endpoint/CLI/config tuples as
documented in `README.md`. Accept the small smoke artifact only if both raw plans contain
`-morsel-`, `MORSEL_TIME_PARTITIONS`, `MORSEL_DRIVER_WALL_TIME_MS`, and nonzero
`MORSEL_ESTIMATED_TSFILE_BYTES` in the LPT arm; its JSON must also say `accepted: true`.

Expected allocation shape at DOP 2 is an invariant to inspect, not a result to forge:

| Arm | Expected partition grouping | Consequence |
| --- | --- | --- |
| Equal count | `[0, 10000, 20000]` and `[30000, 40000, 50000]` | both 1,500-row partitions share the first morsel |
| LPT byte weighting | one 1,500-row partition per morsel | static TsFile-byte estimate is more even |

The observed CPU, TsBlock, output-row, and driver-wall-time fields in the raw plans are the
evidence. Do not derive a timing claim from the expected grouping, and do not label the tiny
fixture a throughput benchmark. Current instrumentation has fragment-level blocked queue time
only; it cannot attribute backpressure causally to an individual morsel. That requires a future
per-morsel blocked/queued-time metric.
