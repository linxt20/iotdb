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

# 查询并行化与运行期 DOP 开发入口

**目的**：说明 A–E 与同查询运行期扩展的关系，提供设计、实现状态和标准验证入口。

**摘要**：A–E 覆盖属性判定、固定布局、原生指标快照、静态估计及历史反馈。本轮增加原生扫描组在同一查询中的实际 worker 调整。最终 v6 已完成 51 类 808 项专项、DataNode 全量 6875 项及中文编译；19 个接受运行、65 个 FI 资源回执和八组完整结果比较均通过。运行期扫描组可在同一 SQL 内实际增减 worker。

**结论**：从总体设计理解能力边界，从进度记录判断各冻结版本的证据。v6 的专项、全量、发行包与服务矩阵已分别闭环；性能收益仍需独立的重复实验。

## 阅读入口

| 内容 | 文档 |
| --- | --- |
| 不涉及代码细节的 A–E 完整设计 | [查询并行化与动静结合 DOP 最终设计](查询并行化与动静结合DOP_最终设计.md) |
| 版本、测试、服务结果与未关闭项 | [实现进度与验证记录](实现进度与验证记录_20261115.md) |
| 运行期协议和正确性约束 | [运行期设计草案](运行期动态DOP设计草案.md) |
| 文件职责及既有测试体系中的验证 | [运行期实现规划](运行期动态DOP实现规划草案.md) |
| 交付顺序、并发边界和时间估计 | [开发待办与排期](开发待办与排期.md) |
| 换机构建、确定性输入与完整 SQL 复现 | [真实 SQL 复现指南](REPRODUCING_RUNTIME_SQL.md) |
| 冻结 A–E 的阶段原件 | [原项目入口](../README.md) |

property enforcement 是既有方法。本项目的研究对象是其在 IoTDB 查询语义、分布计划和执行资源中的应用，以及布局、指标、并发度选择和运行期调整的完整结合。扫描范围之外的所有算子不自动获得动态 DOP 能力。

原 E 使用已完成片段的可信反馈影响后续初值或分组；本轮扩展保持逻辑布局，调整同一个实际扫描组的 worker。例子：一个组固定八个 lane，实际 worker 先为一、再为二、三，最后回到二，才构成同查询运行期扩缩容。

数据来源：[完整设计](查询并行化与动静结合DOP_最终设计.md)、[原 A–E 入口](../README.md)、[版本证据](实现进度与验证记录_20261115.md)。

## 源码与冻结版本

开发主机为 sc01，仓库为 `/root/rivermind-data/iotdb-runtime-dop-20261115/repo`，分支为 `feat/runtime-dop`。开发基线提交为 `1c23f83e2c7495e5340e13172b7c462cd3f7a889`，最终代码提交为 `38fc937fe3b3cc1d34827fb76a3ce96380b9b58c`，文档与验证索引随同一分支交付。HEAD、分支名和目录名都不能单独识别实际构建输入。

最终执行记录必须绑定源码快照、入口、Java、依赖锁、发行 JAR、CLI classpath、部署、SQL、参数和夹具。启动后继续核实际加载来源。文档与运行清单绑定最终 v6。完整回执见 [最终验收摘要](audit/final-runtime-v6-validation.json)，原始失败、缓存补跑与上游后台断言均单独保留。

数据来源：[进度记录](实现进度与验证记录_20261115.md)、[迁移核查](迁移与基线核查_20261115.md)、[复现指南](REPRODUCING_RUNTIME_SQL.md)。

## 标准验证

下列命令在冻结源码根目录的 Bash 中运行。先按复现指南准备锁定依赖和 Java，再使用新的持久输出目录。专项选择包含本轮 51 类；它与完整回归分开报告，不累加测试总数。

~~~bash
set -euo pipefail
export IOTDB_RUNTIME_RUN=/data/iotdb-runtime-validation/run-unique
mkdir -p "$(dirname "$IOTDB_RUNTIME_RUN")"
mkdir "$IOTDB_RUNTIME_RUN"
mkdir "$IOTDB_RUNTIME_RUN/tmp"
export TMPDIR="$IOTDB_RUNTIME_RUN/tmp"
export JAVA_TOOL_OPTIONS="-Djava.io.tmpdir=$TMPDIR"
# This repository disables local Develocity test caching when GITHUB_ACTIONS=true.
export GITHUB_ACTIONS=true
cp docs/design/query-parallelism/maven-settings.xml "$IOTDB_RUNTIME_RUN/maven-settings.xml"
FLAGS="-s $IOTDB_RUNTIME_RUN/maven-settings.xml -nsu -Dspotless.check.skip=true -Drat.skip=true -Dlicense.skip=true -Dmaven.javadoc.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false"
TESTS="RuntimeDriverSchedulerTest,DriverSchedulerTest,RuntimeFragmentLifecycleTest,RuntimeCleanupCoordinatorTest,FragmentQuotaMemoryLeaseTest,RuntimeScanProgressTest,AbstractTableScanRuntimeProgressTest,RuntimePlannerAdmissionTest,RuntimeDopControllerTest,RuntimeResourceSnapshotTest,LocalExecutionResourceRegistryTest,ResourceAuditManagerTest,ResourceAuditFactoryProductionTest,RuntimeCompactOutputOperatorTest,ScanParallelPlannerTest,LocalExecutionPlanContextBudgetTest,DeviceTableScanParallelPipelineTest,DynamicParallelPlanningProductionTest,LocalScanPipelineFailureIntegrationTest,SourceHandleTest,SinkChannelTest,DriverQuiescenceTest,RuntimeScanGroupTest,RuntimeScanWorkerTest,RuntimeDopPolicyTest,SharedTsBlockQueueTest,LocalSinkChannelTest,LocalSourceHandleTest,FragmentInstanceContextTest,RuntimeSnapshotStatisticsBarrierTest,ExternalTsFileTableScanOperatorRuntimeFilterTest,TableScanOperatorTest,RuntimeDopExecutionSnapshotTest,FragmentRuntimeSnapshotProductionTest,FragmentInstanceExecutionTest,RuntimeExchangeBlockCodecTest,RuntimeExchangeResponseProcessorTest,MPPDataExchangeManagerTest,RuntimeFrontierReservationTest,QueryExecutionRetryTest,QueryExecutionDescriptionTest,AnnotationTest,TSDataTypeSwitchArchitectureTest,TSDataTypeSwitchRuleTest,SingleSourceGroupByHashRepartitionPlanningTest,PropertyPlanningConfigurationIsolationTest,FragmentInstanceSerdeTest,MemoryPoolTest,PropertyDrivenDistributionTest,OrderedScanPipelineContractTest,DriverTaskTimeoutSentinelThreadTest"

mvn -B test-compile -DskipTests $FLAGS \
  > "$IOTDB_RUNTIME_RUN/english-compile.log" 2>&1
mvn -B -pl iotdb-core/datanode -am test -DforkCount=4 -Dtest.port.closed=true \
  -Dtest="$TESTS" $FLAGS > "$IOTDB_RUNTIME_RUN/targeted-tests.log" 2>&1
find . -path '*/target/surefire-reports/TEST-*.xml' -print0 \
  | tar --null -T - -czf "$IOTDB_RUNTIME_RUN/targeted-raw-reports.tar.gz"

mvn -B -pl iotdb-core/datanode -am test -DforkCount=4 -Dtest.port.closed=true \
  $FLAGS > "$IOTDB_RUNTIME_RUN/full-tests.log" 2>&1
find . -path '*/target/surefire-reports/TEST-*.xml' -print0 \
  | tar --null -T - -czf "$IOTDB_RUNTIME_RUN/full-raw-reports.tar.gz"

mvn -B test-compile -DskipTests -P with-zh-locale $FLAGS \
  > "$IOTDB_RUNTIME_RUN/chinese-compile.log" 2>&1
mvn -B clean package -pl distribution -am -DskipTests $FLAGS \
  > "$IOTDB_RUNTIME_RUN/clean-package.log" 2>&1
~~~

上述环境变量按仓库现有 Develocity 配置禁用本地测试缓存；还须检查 XML 的实际 tmpdir 和 classpath，不能只看文件 mtime。v6 首次完整 reactor 有两份上游 pipe-api 报告来自 v4 缓存，已禁用缓存补跑其七项测试；DataNode 的 964 份 XML 全部来自本次实际执行。

归档保留原 XML，计数只使用本阶段实际选择且新生成的报告。复用 target 时要筛除旧 XML；不能把缺失类当成零测试成功。每阶段的 Maven 退出、XML tests/failures/errors/skipped 和原始日志同时判定，clean 前完成归档。

ANSI 控制码必须先清除再搜索日志。XML 零失败仍需核后台 AssertionError、进程退出和超时；v4 的两个后台断言已经在纯上游精确复现，记录保留该事实，没有宣称断言通过。

这套命令只验证组件、规划和回归。双节点服务按复现指南另跑实际 SQL，完整结果比较与终态资源验证均成功后才形成对应服务结论。运行期开关默认关闭；单次正确性耗时和请求日志不产生性能或实际缩容结论。

数据来源：[实现规划的验证范围](运行期动态DOP实现规划草案.md#专项与全量验证)、[历史回归与上游复现](实现进度与验证记录_20261115.md)、[真实 SQL 复现](REPRODUCING_RUNTIME_SQL.md)。
