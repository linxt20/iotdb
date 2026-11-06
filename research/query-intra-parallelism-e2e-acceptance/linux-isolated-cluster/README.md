<!-- Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0. -->

# Linux isolated 1C2D + 1C2D N x P deployment

`launch-isolated-nxp.sh` creates a loopback-only candidate/control pair from one built all-bin
distribution. Each has one ConfigNode and two DataNodes. Candidate hash flags are enabled and its
client ports are `27667`/`28667`; control is hash-disabled at `37667`/`38667`.

```bash
tool=research/query-intra-parallelism-e2e-acceptance/linux-isolated-cluster/launch-isolated-nxp.sh
bash "$tool" --mode prepare --root /root/iotdb-isolated-nxp --distribution-root /root/iotdb/distribution/target/apache-iotdb-2.0.11-SNAPSHOT-all-bin/apache-iotdb-2.0.11-SNAPSHOT-all-bin
bash "$tool" --mode start --root /root/iotdb-isolated-nxp --deployment both
bash "$tool" --mode status --root /root/iotdb-isolated-nxp --deployment both
```

Load the reviewed multi-source fixture and run `../scripts/run_multisource_hash_groupby_e2e.py`
against candidate `27667` and control `37667`. Startup alone proves neither two physical scan
sources nor N x P correctness; require the runner's plan and result archive.

```bash
bash "$tool" --mode stop --root /root/iotdb-isolated-nxp --deployment both
```

Safety: `prepare` rejects a non-empty root and occupied assigned ports. The all-bin distribution
is only read. Before both `prepare` and `start`, the launcher opens
`iotdb-server-2.0.11-SNAPSHOT.jar` and requires the hash-channel-index guard in
`TableDistributedPlanner`; this prevents a newer source checkout being paired with an old,
pre-repair all-bin directory that can produce a superficially valid `EXPLAIN` but wrong N x P
results. `stop` reads only manifests below that root and refuses a PID unless `/proc` shows both
the node root and expected IoTDB service class. It never deletes the root; preserve evidence
before manually removing a verified root. The version-2 manifest captures the source SHA, runtime
jar count, server-jar SHA-256, and required guard marker. This check is intentionally limited to
the experimental hash GROUP BY harness; it is not a general IoTDB distribution compatibility test.

## Dedicated fixed-DOP benchmark endpoint

Use a new root solely for the server matrix; it is deliberately separate from the correctness
candidate/control pair. `prepare-benchmark-deployment.sh` creates a candidate-only 1C2D endpoint,
starts it, and makes its first audited transition to DOP 1. It never deletes or adopts an existing
directory.

```bash
tool=research/query-intra-parallelism-e2e-acceptance/linux-isolated-cluster
bash "$tool/prepare-benchmark-deployment.sh" \
  --root /root/iotdb-static-benchmark-20260921 \
  --distribution-root /root/iotdb-next-p0-server/distribution/target/apache-iotdb-2.0.11-SNAPSHOT-all-bin/apache-iotdb-2.0.11-SNAPSHOT-all-bin \
  --initial-dop 1 --query-cost-stat-window 30 --port-offset 16000
```

For each matrix step, invoke `set-isolated-dop.sh` with the same explicitly named root. It stops
only the two DataNodes after `/proc` ownership checks, leaves the ConfigNode alive, rewrites exactly
one DOP property per DataNode, restarts those DataNodes, verifies both listeners, and archives the
before/after configurations, SHA-256 files, lifecycle output, and status under
`<root>/benchmark-dop-audit/`.

```bash
bash "$tool/set-isolated-dop.sh" \
  --root /root/iotdb-static-benchmark-20260921 --deployment candidate --dop 8
```

The benchmark matrix can retain each transition's stdout/stderr without a shell wrapper:

```bash
--dop-command 'bash /root/iotdb-next-p0-runner/research/query-intra-parallelism-e2e-acceptance/linux-isolated-cluster/set-isolated-dop.sh --root /root/iotdb-static-benchmark-20260921 --deployment candidate --dop {dop}'
```

This performs configuration and process-readiness verification only; it supplies no timing or
speedup claim. The normal-query matrix and explicit server-side metric adapter remain responsible
for performance evidence.

`--query-cost-stat-window` is optional and defaults to `0`, preserving the normal IoTDB default.
Pass a positive value only for a dedicated benchmark root when the normal-query timing adapter will
read the completed-query history. The value is written and verified in both DataNode configurations
*before* their first start, then archived with the two configuration hashes. It does not turn CLI
elapsed time into a metric, and it does not create query-scoped shuffle-byte metrics.

`--port-offset` defaults to `0` and adds the same explicit offset to every isolated ConfigNode and
DataNode port. It is recorded in the deployment manifest and is reloaded by every later
start/stop/status/DOP command, so a new benchmark root can coexist with a correctness deployment
without port reuse. The launcher rejects offsets over 26000 to keep all derived ports valid.
