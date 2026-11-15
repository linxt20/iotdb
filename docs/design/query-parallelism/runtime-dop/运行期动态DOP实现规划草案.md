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

# IoTDB 运行期动态 DOP 实现规划草案

**目的**：把运行期动态 DOP 设计落实到文件职责、集成顺序和可检查的测试集合。

**摘要**：实现采用固定逻辑扫描图与动态执行工作者，涉及规划、驱动调度、资源、终态和协议。本文逐项给出新增与修改位置，并把每个阶段绑定到验证。生产修改以迁移核查和远端源码绑定通过为前提。运行组件已开始写入独立开发分支，其余新增名字仍表示计划接口；完成状态见开发排期。

**结论**：接口、运行状态和资源责任统一后再并发开发，按组件测试、同片段升降宽、兼容与异常、双节点真实运行的顺序集成。完整完成条件继续采用[开发待办与排期](开发待办与排期.md)，实现不得用日志中的策略动作代替真实执行证据。

当前状态：新实例认证、密钥与冻结源码迁移核查已完成。执行权状态机、工作者、纯控制策略和驱动静止接口开始实现；原生调度登记、资源准入、终态接线与真实服务验收尚未完成。本文列出的接口仍需通过实现与测试证实，源码评审不能替代运行验收。

## 执行对象与开发前置

开发在新 sc01 的独立分支进行，冻结分支保留原件。先核对最终提交、未提交变化、用户仓库 remote 和数据盘挂载。编译、测试与服务启动前，记录实际入口、直接执行 driver、配置、构建输出和依赖身份。

源码参考为 `da2f455c3a2216f1f676844f91b32e724d7d5709`。以下路径相对 `iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/`，新实例核查后再固定实际基线。本文同时引用[运行期动态 DOP 设计](运行期动态DOP设计草案.md)中的状态与资源条件。

## 规划与逻辑管线构造

### 静态规划文件

`plan/planner/ScanParallelPlanner.java` 新增运行工作者模式下的纯规划入口，分别返回逻辑管线布局、初始工作者数和最大工作者数。现有静态入口与默认结果保持原语义。输入上限、工作数、引用增长和基础内存预算共同限制布局。

新增 `plan/planner/RuntimeScanPlan.java`，保存不可变布局、许可范围、初始配置与估计来源。初始宽度从现有静态策略开始，候选布局的比较与取舍分别记录，控制器可以检验其前提。

`plan/planner/DataNodeTableOperatorGenerator.java` 在已证明合法的扫描路径增加新模式选择，复用原设备、时间范围、过滤、投影和局部有序构造。新模式必须覆盖现有设备、时间分区与有序管线接线，聚合扫描保持明确专用合同。

### 构造与归属文件

`plan/planner/LocalScanPipelineBuilder.java` 增加“逻辑管线构造结果”入口，返回原驱动工厂、固定输出与运行组描述。逻辑驱动创建后归执行组持有，禁止重复提交到原生调度器。

`plan/planner/LocalExecutionPlanContext.java` 分别保存固定逻辑身份、直接提交任务与运行组，分配不复用的工作者任务 ID。既有源归属注册表继续冻结静态布局，运行期状态独立维护。

`plan/planner/LocalExecutionPlanner.java` 汇总固定图估计、运行组和直接提交任务，初始化基础预留。批内依赖下标必须转换成原 Exchange 消费者关闭 future 的稳定引用，根部与最终汇聚继续提交原生调度器。禁止仅过滤逻辑驱动列表而保留原下标。 原 `ExchangeOperator.getBlockedDependencyDriver()` 是惰性创建 future，`close()` 只完成当时已创建的对象，因此必须在任何驱动提交、执行或关闭前捕获一次；增量工作者只复用捕获结果，不能在消费者关闭后首次创建依赖 future。

`plan/planner/OperatorTreeGenerator.java` 的 one-by-one 依赖和 `execution/operator/source/ExchangeOperator.java` 的关闭事件保持原义。测试构造上游 EOF 已到、队列尚未耗尽且消费者尚未关闭的状态，确认后继分支继续等待。

`plan/planner/PipelineDriverFactory.java` 保持单次消费与未创建关闭责任，运行组接管逻辑驱动后明确释放构建者责任。worker 由独立工厂构造，避免现有工厂“一次一个算子树”的合同失效。

### 规划验证

在既有 planner 测试包新增初始宽度与逻辑数分离测试，覆盖八条逻辑管线、两个初始工作者、最大四个工作者。检查原谓词、时间条件、排序键、输出类型和源身份完整保留。默认关闭模式对比原计划及已有快照。

## 运行组与执行权

新增 `execution/runtime/RuntimeScanGroup.java`，统一维护逻辑管线就绪集合、执行权、阻塞通知、工作者成员、待激活事务和终止状态。成员锁只用于状态转换，锁外调用驱动、队列、调度器和资源账户。

新增 `execution/runtime/RuntimeLaneLease.java`，保存管线、工作者和领取代次，保证同一管线独占。归还接口按自然结束、继续就绪或阻塞发布状态，旧代次通知只能结算原责任。

新增 `execution/runtime/RuntimeScanWorker.java`，实现现有 `IDriver`。一个执行调用在可用量子内轮转就绪逻辑驱动，原驱动的阻塞 future 转换成组级就绪通知。worker 正常结束必须主动幂等结算自己，关闭时保留逻辑驱动与队列。

`execution/driver/IDriver.java` 增加明确的调度资源角色接口，保留原实现默认值与 `close()` 声明。根、普通数据驱动和运行工作者的配额贡献分别识别，逻辑驱动仍使用其原生执行入口与算子关闭机制。

`execution/driver/Driver.java` 增加协作静止入口与完成 future，覆盖逻辑驱动及直接提交的根和归并驱动。停止标记阻止新量子，原独占锁退出后才发布静止，任何监听器都不能在该锁内等待自己。新模式只接受可以参与这一协议的驱动实现。

运行组单测使用既有测试框架提供的可控逻辑驱动，覆盖重复领取、阻塞立即完成、过期通知、没有就绪成员、退休唤醒和重复关闭。DOP 为一且一条流背压时，另一条就绪流必须得到执行。

## 调度提交与正常退休

`execution/schedule/DriverScheduler.java` 新增增量工作者登记与激活入口。新任务使用原查询绝对 deadline、共享计费上下文及原用户配额，运行组代次必须有效。查询或片段已注销时返回明确拒绝，禁止通过增量入口重建映射。

新增任务先注册为待激活，只有整批登记成功后才完成激活 future。发生部分失败时唤醒已登记任务正常结束，并核对各自成员回执。正常降宽不调用 `toAborted`、`abortFragmentInstance` 或交换注销接口。

`execution/schedule/DriverTaskThread.java` 复用运行、就绪、阻塞和完成流程，确保退休返回正常 future。配额计算不再仅依赖 `instanceof DataDriver`，新增 CPU 任务与其真实增量内存都必须计入。

原用户配额管理器增加整批额度票据，计入存活及待激活工作者，按用户原子申请 CPU 与增量内存额度。两个扫描组的并发升宽必须共享同一账本，登记失败、取消与退休只能结算一次。管理器路径为 `org/apache/iotdb/db/storageengine/rescon/quotas/DataNodeThrottleQuotaManager.java` 与 `ThrottleQuotaLimit.java`。

`execution/fragment/FragmentInstanceManager.java` 的初始提交先登记原直接执行任务和执行组存活代次，再登记首批运行工作者。固定计数包含原逻辑和直接执行的数据驱动槽位，原工厂创建失败的槽位明确结算，运行工作者排除在该计数之外。后续增量入口始终要求已有存活代次。

相关单测覆盖取消与激活、旧 deadline、非 root 用户的两个组并发升宽、整批部分提交失败、退休中的 blocked worker、调度映射延迟清理和迟到唤醒。工作者停止、资源结算、原生映射移除分别确认，测试必须证明退役一个工作者后其他成员和交换继续有效。

## 资源与终态接线

新增 `execution/runtime/RuntimeWorkerResourceLease.java`，管理工作者增量算子与元数据预留，统一正常退役、提交失败与终态释放。固定图基础预留继续由片段级责任持有。普通队列保留原逐块账户规则，预付前沿模式使用独立组级预留和内部额度，两个路径分别实现。

`execution/fragment/FragmentInstanceContext.java` 保留固定逻辑驱动的关闭计数，并增加动态成员结算条件。数据源、批量 lease 和构建者引用的最终释放需要等待运行组静止与固定驱动关闭。禁止仅替换原 latch 或在计数已归零后新增逻辑成员。

`execution/fragment/FragmentInstanceExecution.java` 分别管理逻辑驱动、直接调度任务和运行组。终态先停止新增与领取，再异步等待活动量子退出，随后记录与冻结统计、完成剩余清理。执行中的工作者可以触发终态，监听器必须避免等待自己退出。

新增 `execution/runtime/RuntimeCleanupCoordinator.java`，在新模式规划开始前建立并由片段上下文持有，接收构建、驱动静止、端点、账户和数据引用的清理责任。该对象必须处理 FragmentInstanceExecution 尚未构建成功的失败路径，防止早期 MemoryNotEnough 后资源无人结算。

`LocalExecutionPlanner.checkMemory` 的原终态释放监听器在新模式改绑该协调器的安全释放事件，完整基础图必须实际准入。全局估计与配额均关闭、最高优先级返回零预留等路径，均不能代替新模式的严格预留。静态路径延续原策略，运行模式真实准入失败时完整回滚当前构造。

`FragmentInstanceContext.updateStatsIfDone` 的查询上下文释放与 sourcePaths 清空，也在新模式改绑安全释放事件。`MPPDataExchangeManager` 与调度异常路径的强制账户注销委托同一协调器，先通知停止并唤醒阻塞者，再按队列、组预留、片段注销顺序完成释放。

`plan/planner/LocalExecutionResourceRegistry.java` 继续表示固定构造责任，运行工作者增量采用独立代次登记。`LocalExecutionPlanner` 的整树基础预留与增量 lease 必须明确拆分，禁止重复释放同一笔字节。

驱动静止 future 只证明后续 `processFor` 不再写入；`isFinished()` 在停止请求后可以立即为真，不能代替该 future。确认之后新调用的 `close()` 仍可能写统计和释放资源，必须由协调器统一排序。静止确认复用原驱动锁的关闭补偿，已发起的清理失败通过异常 future 暴露，不能让等待者永远悬挂。

相关测试在部分预留成功、工作者创建后、调度登记后、激活前和自然 EOF 时注入异常或取消。还需在根部归并量子内部触发终态，证明所有统计写入者退出前基础预留和查询上下文仍保留，冻结后载荷与校验和稳定。验证实际池余额、未关闭句柄、工作者回执、基础引用和终态统计，所有关键交错使用 latch 或 barrier 固定执行顺序。

## 有序、交换与聚合

固定逻辑队列和有序输入成员保留原合同，`TableReadyCollectOperator` 继续消费固定流。`SharedTsBlockQueue.java` 与内存账户需要提供可检查的排序前沿资源保证，并维持现有锁顺序及异步通知规则。

新增 `execution/runtime/RuntimeFrontierReservation.java`，通过现有 `MemoryPool.tryReserve` 为有序组实际预留每流前沿容量，再划分不可借出的内部额度。`SharedTsBlockQueue` 增加预留额度模式，发布、消费、关闭分别结算内部额度与最终池预留，保留原路径的记账方法。

组级账户使用稳定独立 key、片段 OUTPUT 总额度和唯一预留责任。预付模式的 `add/remove/abort/close` 全部旁路逐块 `reserve/free`，只借还内部额度。必须先结清生产者与队列，再释放组级预留，最后允许片段账户注销。快照分别记录组级实际预留与各队列内部使用，防止重复报告整组字节。

预留模式必须限制每个队列的一块有界输出，并核查块大小、源输出上限、局部与根部归并 head 的基础内存估计。 原队列逐块账户使用 `TsBlock.getSizeInBytes()`，新模式需要证明驻留大小及额外对象开销已有预算，不能仅凭序列化大小推出物理内存上界。消费释放队列额度后，归并仍持有的 head 由基础预留覆盖。测试检查池预留只发生一次、消费只归还内部额度、组关闭精确归还池预留，以及多个组争用时启动前拒绝。最高优先级的零字节准入不能计为前沿预留成功。

实现前必须证明 DOP 为一时，所有必要输入能提供首行及后续前沿。测试覆盖满队列、共享预算紧张、多个排序键、空值、升降序、空流和连续退休。完整顺序比较与多重集比较同时记录。

最低活性用例使用真实两层 TableMergeSortOperator、预付 SharedTsBlockQueue 与 MemoryPool，固定三条流和一个工作者。第一条长流填满自身队列，第二条包含更小首值，第三条延迟确认空流，测试必须完成并保持顺序。受控宽度测试还需证明新增工作者各自处理真实量子，且资源足够时同时存在目标数量的执行权。

Hash GROUP BY 保留固定源、桶和通道。运行工作者退休不触发逻辑通道 EOS，EOF 只来自原逻辑驱动。多源测试覆盖跨源同键、空桶、Filter/Project 包装和远端交换，核对实际交换身份与完整 COUNT/SUM 结果。

GROUP BY 的生产资格必须在现有多源合法性和递归来源证明之后接入，partial aggregation 与 Hash sink 保持单一所有者。既有合并版双源用例实际走过串行回退，因此新验收必须证明真实存在逻辑扫描管线与运行工作者，禁止仅复用回退结果宣称动态路径完成。

如果上述资源保证无法在现有共享池内直接成立，设计必须补充有界保留方案并重新估计基础内存。完成报告必须保留这项未关闭条件，不能把有序路径回退当作该项实现通过。

## 观测与协议接线

新增 `execution/runtime/RuntimeDopSnapshot.java`，分别表示逻辑图、实际工作者、代次、运行进度和资源回执。终态冻结保留已退休工作者的累计贡献，运行中采样使用不阻塞主路径的不可变副本。

`statistics/snapshot/RuntimeSnapshotAdapters.java` 与现有 snapshot 构造入口新增显式运行期结构，旧静态结构保持原版本语义。新结构必须说明逻辑驱动与物理执行工作者的计时范围，避免重复相加输出。

`iotdb-protocol/thrift-datanode/src/main/thrift/datanode.thrift` 增加版本明确的可选运行载荷，Python 生成和 reader 随标准流程更新。新结构与历史反馈适配一并测试，未经支持的新版本返回明确原因。

协议测试覆盖运行中、终态冻结准备、已退休工作者、取消、未知版本和缺失量。每个字段必须能绑定真实来源，驱动 CPU 与未采集的字节保持未支持状态。

## 控制策略与静态比较

新增 `execution/runtime/RuntimeDopController.java` 和 `RuntimeDopPolicy.java`，处理采样窗口、目标变化、静态约束、资源反馈及冷却。纯策略可以在独立单测中并发开发，执行权与成员生命周期由运行组统一实现。

策略测试覆盖冷启动、稳定、资源竞争、误差、临近完成、预算拒绝和重复调整。自动控制验收前，先使用确定性请求在同一次查询中验证完整升降宽，隔离策略判断与执行机制。

性能对照使用同一源码和输入，分别关闭运行控制、固定初始选择、使用合理默认起点的运行控制、直接组合和联合方法。报告控制及调整开销、端到端时延、资源与退化，所有结论绑定实际执行事件。

## 标准验证与交付

新测试进入既有 datanode 测试包和 Python 协议单测，测试选择、入口与命令写入工程 README 或复现指南。Maven 延续仓库规定的 RAT 等跳过参数，检查 Surefire XML 的失败与错误，日志匹配先去除 ANSI 颜色码。

验证顺序为纯状态测试、同片段真实升降宽、资源与取消竞态、排序与多源聚合、完整回归、双节点候选与对照、性能测量。每次运行保存输入身份与独立输出目录，已有失败与基线原件保留。

完整交付包含源代码、设计与实现说明、专项及完整回归结果、真实运行事件、完整结果比较、资源证据和复现命令。工作分支推送到用户仓库，原冻结分支保持可复现。认证恢复后已开始组件实现与标准基线验证。端到端运行能力须以实际服务接线及独立验收报告为准。
