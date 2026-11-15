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

# 运行期动态 DOP 开发入口

**目的**：提供本轮扩展的能力边界、设计入口和标准验证命令。

**摘要**：当前交付为固定逻辑扫描流上的执行权、工作者、驱动静止和纯策略组件，新增 59 项专项通过，完整单测 7580 项通过，中英文编译通过。真实 SQL 的运行期调整仍在接线，排序前沿账户还在独立暂存区。

**结论**：开发与评审从下列状态记录和两份设计稿开始。复现本轮组件可使用标准 Maven 命令；组件结果不能代替双节点真实运行验收。

## 阅读入口

| 内容 | 文档 |
| --- | --- |
| 当前实现与未完成项 | [实现进度与验证记录](实现进度与验证记录_20261115.md) |
| 总体设计与正确性边界 | [运行期动态 DOP 设计草案](运行期动态DOP设计草案.md) |
| 文件职责、集成顺序与测试 | [运行期动态 DOP 实现规划草案](运行期动态DOP实现规划草案.md) |
| 工作项与预计投入 | [开发待办与排期](开发待办与排期.md) |
| 新实例迁移与旧产物缺口 | [迁移与基线核查](迁移与基线核查_20261115.md) |

原 A–E 冻结分支保留已有设计和复现指南。本轮扩展不会把完成片段的历史反馈重新称为同查询运行期弹性，也不会把 property enforcement 本身当作新的理论贡献。

## 当前源码与证据

生产代码位于 sc01 `/root/rivermind-data/iotdb-runtime-dop-20261115/repo`，分支 `feat/runtime-dop`，从冻结提交 `da2f455c3a2216f1f676844f91b32e724d7d5709` 分出。换机先从用户仓库取得本分支对应提交，再按下面命令验证。提交号、工作区和实际 Java、Maven、settings 与源码摘要随运行保存。

历史 `reproduction-manifest.json` 的代码树校验值专门绑定原冻结实现，不适用于新增运行组件后的代码树。当前扩展的源码摘要及验证索引由本目录的运行清单提供，不能把历史值直接覆盖成新值而丢失冻结身份。

新实例冻结基线原始材料位于 `/root/rivermind-data/iotdb-runtime-dop-20261115/baseline-run-20261115T061040Z`。首批实现的成功验证位于 `/root/rivermind-data/iotdb-runtime-dop-20261115/implementation-run-20261115T064126Z`，首次 Mockito API 编译失败保留在 `implementation-run-20261115T063533Z`。

## 标准验证

以下命令在源码根目录的 Bash 中运行。`IOTDB_RUNTIME_RUN` 使用新的持久目录，不覆盖已有结果；构建前记录实际代码、入口、settings、TsFile 依赖与运行时身份。依赖锁定继续使用原冻结清单，不复用旧脏工作区的发行 ZIP。

```bash
set -euo pipefail
export IOTDB_RUNTIME_RUN=/data/iotdb-runtime-validation/run-unique
mkdir -p "$(dirname "$IOTDB_RUNTIME_RUN")"
mkdir "$IOTDB_RUNTIME_RUN"
mkdir "$IOTDB_RUNTIME_RUN/tmp"
export TMPDIR="$IOTDB_RUNTIME_RUN/tmp"
export JAVA_TOOL_OPTIONS="-Djava.io.tmpdir=$TMPDIR"
cp docs/design/query-parallelism/maven-settings.xml "$IOTDB_RUNTIME_RUN/maven-settings.xml"
FLAGS="-s $IOTDB_RUNTIME_RUN/maven-settings.xml -nsu -Dspotless.check.skip=true -Drat.skip=true -Dlicense.skip=true -Dmaven.javadoc.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false"

mvn -B test-compile -DskipTests $FLAGS \
  > "$IOTDB_RUNTIME_RUN/reactor-en.log" 2>&1
mvn -B -pl iotdb-core/datanode -am test \
  -Dtest=DriverQuiescenceTest,RuntimeScanGroupTest,RuntimeScanWorkerTest,RuntimeDopPolicyTest \
  $FLAGS > "$IOTDB_RUNTIME_RUN/targeted-tests.log" 2>&1
find . -path '*/target/surefire-reports/TEST-*.xml' -print0 \
  | tar --null -T - -czf "$IOTDB_RUNTIME_RUN/targeted-tests-reports.tar.gz"

mvn -B -pl iotdb-core/datanode -am test \
  -DforkCount=4 -Dtest.port.closed=true $FLAGS \
  > "$IOTDB_RUNTIME_RUN/full-tests.log" 2>&1
find . -path '*/target/surefire-reports/TEST-*.xml' -print0 \
  | tar --null -T - -czf "$IOTDB_RUNTIME_RUN/full-tests-reports.tar.gz"

mvn -B test-compile -DskipTests -P with-zh-locale $FLAGS \
  > "$IOTDB_RUNTIME_RUN/reactor-zh.log" 2>&1
```

每个 Maven 退出码与每个阶段新生成的 XML 分别判定；原始报告先归档，再开始下一阶段。复用已有 target 时，不能把上次遗留 XML 算进本次专项，必须按本次选择和时间边界筛选。专项 59 项已被完整 7580 项包含，不重复求和。日志带 ANSI 时先移除控制码，最终仍以退出码和 XML 为准。

这套命令只验收组件与回归。当前还没有可交付的 SQL 配置能够触发同查询 `1→2→4→2→1`；后续需要原生提交、用户额度、队列和终态全部接通后，另行保存实际服务、执行事件、完整结果与资源回执。
