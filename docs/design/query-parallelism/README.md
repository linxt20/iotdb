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

# IoTDB 查询执行并行化设计与复现

**目的**：为接手本分支的开发者提供阶段设计、实现规划和换机复现入口。

**摘要**：本项目为表模型查询增加查询内并行执行能力。五个阶段分别处理计划要求、执行资源、运行统计、静态分组和历史反馈。本文选择各阶段的设计入口，并登记原始设计与审查文件。执行命令和环境要求集中在独立复现指南。

**结论**：代码、阶段文档和确定性验收输入均随本分支交付，换机执行从[复现指南](REPRODUCING.md)开始。

未决事项：论文性能倍率、通用哈希连接的生产规划范围和 有色 Petri 网形式化的论文位置等待 sxing 决定。

## 项目背景

单个查询包含扫描、交换和聚合等工作，多个处理器可以同时执行其中的独立部分。正确实现需要同时保留查询结果、行序要求和资源释放行为。换机验收需要从同一份代码构建发行包，再执行完整结果和执行结构检查。

并行度表示一项扫描工作的目标执行宽度。例如，目标为四时，系统为该扫描源选择最多四组工作，实际分组仍受输入和内存限制。

## 总体设计

总体设计按属性判定、管线构建、统计反馈和后续计划选择解释一次查询的流程。阶段核查记录给出源码与证据定位，日期对应原执行版本。

资料：[ABCDE 完整总体设计](records/IoTDB查询执行并行化_ABCDE完整总体设计_20261104.md)、[整体实现核查记录](records/ABCDE整体实现核查记录_20261104.md)、[代码改动与既有优化核查](records/ABCDE代码改动量与既有优化影响核查_20261105.md)。

## 阶段设计

### A 属性驱动并行判定

A 判定输入是否满足分布与顺序要求，并在需要时建立交换或收集结构。扫描并行资格由该判定传递。

资料：[A 设计](records/属性驱动并行判定框架设计_20261027.md)、[A 实现规划](records/属性驱动并行判定框架实现规划_20261027.md)、[A 实施记录](records/属性驱动并行判定框架实施记录_20261028.md)、[A 验收结果](records/A_属性框架验收结果_20261028.md)。

### B 扫描管线与资源管理

B 统一构造扫描管线，并管理队列、端点、驱动和内存账户的创建、移交与终止。无序收集和受检内存估算由补充设计说明。

资料：[B 设计](records/B_静态数据并行统一设计_20261031_实施版_复审修订.md)、[B 实现规划](records/B_静态数据并行统一实现规划_20261031_实施版_复审修订.md)、[B 实施记录](records/B_静态数据并行统一实施记录_20261031.md)、[无序收集补充设计](records/B_无序Collect活性补充设计_20261103.md)、[无序收集补充规划](records/B_无序Collect活性补充实现规划_20261103.md)。

### C 运行指标快照

C 从 IoTDB 的驱动、算子和统计传输流程读取运行数据，并保存带有归属及有效范围的快照。新增字段用于补充原统计未表达的信息。

资料：[C 设计](records/C_运行指标快照设计_20261101.md)、[C 实现规划](records/C_运行指标快照实现规划_20261101.md)、[C 实施记录](records/C_运行指标快照实施记录_20261101.md)。

### D 静态数据并行

D 根据扫描输入和成本估计选择工作分组，并保存确定的计划身份。设备、时间分区和有序扫描共同使用安全条件与覆盖检查。

资料：[D 设计](records/D_静态数据并行设计_20261101.md)、[D 实现规划](records/D_静态数据并行实现规划_20261101.md)、[D 第五轮处理说明](records/D_静态数据并行第五轮审查处理说明_20261101.md)。

### E 动态数据并行

E 使用已完成查询的可信反馈，为后续片段选择保持静态、重新均衡、降低并行度或串行回退。已经运行的驱动继续使用自己的工作分配。

资料：[E 设计](records/E_动态数据并行设计_20261103_第五轮修订.md)、[E 实现规划](records/E_动态数据并行实现规划_20261103_第五轮修订.md)、[E 最终实施记录](records/E_动态数据并行实施进度_20261104.md)。

## 合并后的执行规则

表模型的普通收集流程使用 master 的就绪输入处理，交换额度使用片段共享预算。资源快照在最终额度选定后冻结，因此诊断记录与执行使用同一份额度。

物理内存对象与包装端点分别记录。例如，两个端点共用同一个队列时，额度按一个队列登记，端点仍各自保留终止责任。阶段原件中的对象计数按其版本解释，合并后合同由对应资源测试检查。

窗口、FILL、TopK 和谓词下推沿用 master 对应的计划分支。功能回归使用同时覆盖原生优化与并行执行的测试选择，完整单测和双节点验收分别保存自己的范围。

资料：[合并后完整验收摘要](evidence/merged-20261106/final-validation-summary.json)、[专项测试选择](test-selection.json)、[代码身份](reproduction-manifest.json)。

## 六个提交的职责

六个提交按代码依赖划分，各阶段与提交之间存在多对多关系。实现提交随附对应单元测试，专项负例和文档分别集中管理。

1. 属性契约与哈希交换基础包含分布、顺序、哈希路由、拓扑和序列化。
2. 分布式规划接线包含属性强制、分组重分区、扫描资格和解释输出。
3. 静态管线与运行快照包含扫描构造、资源管理、静态分组和统计冻结。
4. 动态扫描并行决策包含历史反馈、工作归属、内存准入和第二版统计协议。
5. 生命周期与协议兼容性测试包含失败清理、计时、回退原因和 Python 传输回归。
6. 验收工具与设计记录包含隔离部署、确定性输入、阶段文档和复现入口。

资料：[代码身份与入口摘要](reproduction-manifest.json)、[拆分验证记录](evidence/split-20261106/part5-commit.json)。

## 文档原件与证据

文档原件保留评审时的内容，用于审查和来源追溯。该归档按用户要求保留全部修订稿，正文采用原文，此处属于技术写作规范关于历史副本与终态叙事的有意例外。执行步骤采用复现指南，阶段记录中的日期、绝对路径和完成状态按其绑定版本解释。

归档只增加缺失的 Apache 许可证头。清单记录原文摘要、入库摘要及前缀长度，移除对应长度的前缀后即可恢复原文字节。

例如，某文件的前缀长度为 900 字节时，取入库内容从第 900 字节之后的部分，所得摘要必须等于清单中的原文摘要。实际前缀长度由逐文件清单给出。

证据摘要按阶段版本和合并后版本分别保存。JSON 内的绝对路径表示原主机的执行位置，完整进程日志和数据库运行目录由独立归档管理。

资料：[文档清单](document-manifest.json)、[证据清单](evidence-manifest.json)、[合并后验收摘要](evidence/merged-20261106/final-validation-summary.json)、[历史 E 交付记录](evidence/stage-e-20261104/delivery-final.json)。
