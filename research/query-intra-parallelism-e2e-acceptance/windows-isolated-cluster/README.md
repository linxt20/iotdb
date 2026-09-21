<!--
    Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to You under
    the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may
    obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
    BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language
    governing permissions and limitations under the License.
-->

# Windows isolated 1C2D + 1C2D setup for N x P acceptance

`Invoke-IsolatedNxPCluster.ps1` creates two **local and independent** 1 ConfigNode + 2 DataNode
clusters from an already-built all-bin distribution:

| Role | Candidate | Control |
| --- | --- | --- |
| ConfigNode internal / consensus | `21010` / `21020` | `31010` / `31020` |
| DataNode 1 client RPC | `21667` | `31667` |
| DataNode 2 client RPC | `22667` | `32667` |
| Hash flags | true / true | false / false |
| Fixed DOP / hash partitions | 4 / 2 | 4 / 2 |

The non-default port sets intentionally avoid a normal local IoTDB server (`10710`, `6667`). The
script does not copy or edit the distribution. It creates only node-local `conf`, `data`, `logs`,
and process manifests under the explicitly supplied root.

## Safe preparation and diagnostic run

Run this from Windows PowerShell after building the all-bin distribution. `Prepare` rejects an
existing non-empty root and checks every reserved port before writing. `Start` checks ports again,
starts ConfigNode before DataNodes, and writes the PID plus exact launch root for every process.

```powershell
$repo = 'D:\study-note\项目\IoTDB\iotdb'
$dist = Join-Path $repo 'distribution\target\apache-iotdb-2.0.11-SNAPSHOT-all-bin\apache-iotdb-2.0.11-SNAPSHOT-all-bin'
$root = 'D:\iotdb-isolated-nxp-20260921'

.\Invoke-IsolatedNxPCluster.ps1 -Mode Prepare -Root $root -DistributionRoot $dist
.\Invoke-IsolatedNxPCluster.ps1 -Mode Start -Root $root -Deployment Candidate
.\Invoke-IsolatedNxPCluster.ps1 -Mode Status -Root $root -Deployment Candidate
.\Invoke-IsolatedNxPCluster.ps1 -Mode Stop -Root $root -Deployment Candidate
```

For the actual enabled-versus-control result-equivalence gate, prepare once, start both clusters,
load the fixture into both clusters, then give the distinct endpoints and copied configuration
paths to `../scripts/run_multisource_hash_groupby_e2e.py`. The runner itself remains read-only.

```powershell
.\Invoke-IsolatedNxPCluster.ps1 -Mode Start -Root $root -Deployment Both

# Load the explicitly reviewed fixture with the appropriate client endpoint first.
# Then invoke the Python acceptance runner with candidate 21667 and control 31667.
# It must still observe two physical table-scan sources; 1C2D membership alone is insufficient.

.\Invoke-IsolatedNxPCluster.ps1 -Mode Stop -Root $root -Deployment Both
```

## Safety boundaries

* The script never reuses the normal default port block and refuses occupied assigned ports.
* It never starts, stops, changes, or discovers another IoTDB installation.
* `Stop` acts only on process IDs written under this root and refuses a PID whose live command line
  does not contain the same absolute root.
* It does not delete the root. Archive the evidence first, then remove a **manually verified** root
  using the local operating-system procedure.
* A listening client port proves only node startup. It does not prove N x P query topology or query
  correctness. That claim requires the E2E runner's plan and result archive.

## Known acceptance preconditions

An N x P proof requires more than starting two DataNodes: the fixture must produce two direct
physical scan sources (for example, distinct region placement), and the candidate plan must expose
the expected `sources=2 partitions=2` trace, hash sinks, and four exchange edges. Do not report a
speedup from this setup. It is a topology-correctness harness only.
