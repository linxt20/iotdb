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

# IoTDB 查询并行化与调度实验收尾

两个实验都在一台物理机上的两个 DataNode 运行。检出与实验匹配的源码提交：

| 实验 | 源码提交 | DataNode 发行 JAR SHA256 |
| --- | --- | --- |
| London 联合与顺序选择 | `5d9d68ac0b2c4a1743f02e5c21510ca646c1c17d` | `529ed2ef961eb9d22ca90c394e9629a6e8de7162078d54f399d5c2d52182a40a` |
| Electricity 暖历史 | `43163efc4c4df94cb3461ae8f9432c47895080d2` | `526a18b3bd25b8b99a08338b557b9ee9f124e78f3f416d2358b7e8f191af7cf6` |

[源码绑定](records/github-source-preparation.remote.json)列出只补许可注释的文档和数据准备脚本。Java 运行和测试源码按实测候选核对。

**暖历史。** 两组各追加 51,153,980 条窗外数据，文件内容和逻辑布局相同，十次各返回与独立真值相同的 740,740 行。增长后两源估计均为 3；首条查询实际初宽从无历史组 3/3 变为暖组 2/2。两源各使用两份同源历史，并读回此前重平衡查询的快照。这只验证启动机制。候选全量 8,118 项无失败和错误，56 项跳过；89 项专项通过，4,530 个 DataNode 类与测试产物一致。

**London。** 两种规则只用 Electricity 标定，分别选择八名启动并开启反馈、两名启动并关闭反馈。同一长查询、数据、布局和两条后台流按冻结顺序跑四对，第二个脉冲始于 8.1 秒。四对中 4 对有完整结果，2 对也符合压力阶段和排空要求。后台 p95 门槛为 1.831 秒，联合与顺序配置分别有 4/4 和 3/4 格达标。完整配对中，联合减顺序的中位差为前台 -1.231 秒、后台 p95 -0.149 秒。 初宽和反馈同时变化，不作单因素归因。

## 复核与复现

1. [冻结协议](protocols/)、[产物索引](artifact-index.json)、[暖历史复核](records/warm-adjudication.json)和[London 复核](records/london-adjudication.json)记录数据及条件。
2. 从交付材料取得 `iotdb-closeout-experiment-evidence.tar.gz`，SHA256 为 `02b57442cc74177cd8df742019da60ddaa5390287707066d880fc60b50da817a`。原始工具按字节保留在归档中；仓库副本补许可注释，个别行删除尾随空白。在新目录解压后，使用 Python 3.12 或更新版本复核：

   ```sh
   python3 seal/seal_iotdb_closeout_evidence_20261120.py --verify /实际路径/iotdb-closeout-experiment-evidence.tar.gz
   python3 seal/seal_iotdb_closeout_evidence_20261120.py --warm-replay warm > warm-replayed.json
   python3 london/audit/audit_london_delayed_selector_holdout_20261120.py london/trials london/audit/assets london-replayed.json
   ```

3. 服务重跑按[部署说明](../REPRODUCING_RUNTIME_SQL.md)恢复对应源码、JDK、发行包、数据和冻结种子配置。归档中的 warm/driver/ 和 london/driver/ 记录入口和输入 SHA。仓库不包含稿件 PDF、完整 SQL 输出、数据库状态、发行 JAR 或工具链，离线复核不等同于跨物理机复现。
