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

# Multi-query result and fallback acceptance

`scripts/run_multi_query_case_acceptance.py` is the query-family correctness gate that
complements the lower-level property and fragment-plan tests. It runs only `EXPLAIN` and `SELECT`
against explicitly named, already-running endpoints. It cannot start, stop, reconfigure, load, or
clear an IoTDB server.

The runner executes the validation SQL from the benchmark kit and keeps the raw CLI stdout/stderr,
redacted commands, rendered result CSVs, copied configuration, and Git state. Its result report is
not a benchmark: it has no elapsed-time field and makes no speedup conclusion.

| Family | Candidate/control | Acceptance decision |
| --- | --- | --- |
| scan/filter | enabled versus fallback | Header and complete row multiset must agree; enabled `EXPLAIN` must expose the property trace. |
| filter/project | enabled versus fallback | Header and complete row multiset must agree. This remains valid when the computed projection takes its conservative serial fallback. |
| ordered scan | enabled versus fallback | Header and complete row **sequence** must agree. |
| top-k | enabled versus fallback control | Header and complete row **sequence** must agree. This is a global-order correctness control, not a parallel-scan claim. |
| GROUP BY | fallback-only semantic baseline | Saves plan/result/hash and rejects a hash sink in the fallback plan. It cannot support a repartition or acceleration claim. |
| equi-join | fallback-only semantic baseline | Saves plan/result/hash and rejects a hash sink in the fallback plan. It cannot support a co-partitioned or acceleration claim. |

The DOP/P50/P95 matrix remains the separate benchmark protocol. Only after this result gate and
the corresponding matrix reports have been archived can an individual static candidate's measured
performance be discussed. This gate deliberately does not widen GROUP BY or join eligibility.

## Run on isolated endpoints

Use a candidate endpoint with `enable_property_driven_planning=true`, and a fallback endpoint with
the experimental static paths disabled. Both must point to the same immutable table-model fixture.
The tool does not alter either endpoint. The password is read only from the named environment
variable and is redacted from the artifact.

```bash
export IOTDB_E2E_PASSWORD='isolated-password'
python3 scripts/run_multi_query_case_acceptance.py \
  --output /root/iotdb-next-artifacts/multi-query-$(date -u +%Y%m%dT%H%M%SZ) \
  --table benchdb.bench \
  --enabled-cli /root/iotdb-next-enabled/sbin/start-cli.sh \
  --enabled-host 127.0.0.1 --enabled-port 16667 \
  --fallback-cli /root/iotdb-next-fallback/sbin/start-cli.sh \
  --fallback-host 127.0.0.1 --fallback-port 26667 \
  --config /root/iotdb-next-enabled/conf/iotdb-system.properties \
  --config /root/iotdb-next-fallback/conf/iotdb-system.properties
```

Run the offline smoke check before deployment:

```bash
python3 scripts/run_multi_query_case_acceptance.py --self-test
```

A zero exit code proves only the listed result-equivalence and fallback conditions. Archive the
whole output directory unchanged. A nonzero exit leaves the raw output and `failure.txt` for
diagnosis; do not replace failed evidence with a manually edited summary.
