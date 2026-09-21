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
is only read. `stop` reads only manifests below that root and refuses a PID unless `/proc` shows
both the node root and expected IoTDB service class. It never deletes the root; preserve evidence
before manually removing a verified root. The manifest captures git SHA and distribution jar count.
