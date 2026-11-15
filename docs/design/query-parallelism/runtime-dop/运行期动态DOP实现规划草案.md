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

# IoTDB 运行期动态 DOP 实现规划

**目的**：为实现、集成与验收人员给出运行期扫描并发度的文件职责、集成顺序和标准验证步骤。

**摘要**：运行工作者已经接入原生调度，固定扫描状态、资源预留与统计冻结需要按同一生命周期核查。本文按规划、执行、资源、终态与观测划分修改职责，并列出对应专项测试。本文同时给出隔离集群、确定性数据和普通 SQL 的七项服务验收。源码、构建产物与运行回执分别绑定，当前实测统计绑定最终 v6 的独立回执。

**结论**：最终 v6 已完成 51 类专项 808/0/0/5、DataNode 全量 6875/0/0/53、中文编译和 clean package。19 个普通 SQL 运行、八组完整结果比较和 65 个 FI 资源回执已闭环，同一查询实际 worker 为 1→2→3→2→1。严格错误码、类型与切块修订均在同包服务中验证；上游后台断言、缓存补跑及能力边界见[最终验收摘要](audit/final-runtime-v6-validation.json)。

## 开发与验收背景

本实现让一次表模型查询在运行期间改变扫描工作者数量，并继续使用原有扫描与归并状态。开发者必须把计划资格、任务提交、资源责任和查询终止作为一套处理流程。集成者根据同一版本的测试和服务回执判断完成范围。

完整验收需要普通 SQL 自行经过生产规划和调度。构造执行图的组件测试用于固定并发交错和验证局部协议。普通查询的结果、实际工作者变化和资源归还分别留下原始证据。

数据来源：[运行期动态 DOP 设计](运行期动态DOP设计草案.md)定义状态和资源合同，本文给出文件职责与验收动作。

## 源码与交付身份

实现与验收使用独立开发分支和数据盘目录，源码身份由提交号与工作区文件摘要共同确定。仅有提交号无法代表未提交的集成修改。每次编译或启动前，执行者必须记录入口、直接驱动、配置、数据清单、JDK 和实际依赖的绝对路径与 SHA256。

例如，同一个分支提交下的源码发生修改，而部署 JAR 仍保留原摘要时，服务启动前必须重新构建并绑定产物。启动后必须核对真实 Java 进程、监听端口和加载类的代码来源。命令行参数可以证明启动意图，加载身份还需要运行中的实际观测。

独立暂存交付必须先按清单核对基线和新文件摘要，再纳入生产仓库。失败输出和已有夹具保持原位置，新的验证使用新目录。本文只描述经源码核查确认的职责，最终产物身份由集成者冻结。

数据来源：源码目录为 `/root/rivermind-data/iotdb-runtime-dop-20261115/repo`，分支为 `feat/runtime-dop`，核查时提交为 `1c23f83e2c7495e5340e13172b7c462cd3f7a889`。本文的独立文档候选及基线摘要在 `runtime-docs-next-staging-v2-20261115T150358Z/delivery.json` 中登记，最终仓库文档由集成者重新生成清单。执行记录根目录为 `/root/rivermind-data/iotdb-runtime-dop-20261115/`。

## 片段协议与派发快照

整图协议选择必须在生产分布式规划结束后、片段派发前完成，并传给全部执行实例。任何 SchemaRegion 来源使整张图采用原有交换，QueryExecutor 合并与根部保持相同选择。序列化边界、兼容读取范围和节点拒绝条件按[整图执行协议](运行期动态DOP设计草案.md#整图执行协议)验收。

协议测试必须覆盖纯数据图、混合 SchemaRegion 图、单实例结果生产者和跨节点交换。例如，两个 SchemaRegion 来源加一个 QueryExecutor 根部时，三个实例的协议位必须全部为零。运行期位为一而工作节点开关关闭时，实例必须在执行上下文分配前失败。

查询身份快照必须在派发释放算子树前完成不可变复制。每次重试替换整份计划绑定，关闭采集门控的重试清空绑定。默认关闭运行期功能且审计关闭时，专项必须断言快照复制执行零次。

构造示例中，第一次规划包含五个实例，重试包含六个实例。读接口必须返回一次完整规划，或返回明确过渡状态。专项同时释放原计划树，验证派发后的描述仍能读取已复制的片段和交换边。

数据来源：整图选择入口为 `TableDistributedPlanner.configureRuntimeExecution`，接收验证入口为 `FragmentInstanceContext.validateRuntimeExecutionSettings`。协议字段属于 `FragmentInstance`。查询快照入口为 `QueryExecution.doDistributedPlan` 与 `QueryExecutionDescription.capturePlan`。对应测试包括 `SingleSourceGroupByHashRepartitionPlanningTest`、`FragmentInstanceContextTest`、`FragmentInstanceSerdeTest`、`QueryExecutionRetryTest` 和 `QueryExecutionDescriptionTest`，精确专项集合由[修复版专项回执](#专项与全量验证)绑定。

## 计划资格与固定布局

计划资格先执行原优化器的语义否决，再为合法的透明无序扫描批准 Hash 路径恢复。修复必须检查每层过滤条件及每项投影表达式。任何一层包含相邻差值计算时，下层扫描保持原并行否决。

构造示例中，外层投影只是改列名，内层过滤却依赖相邻差值。资格检查必须继续看到内层表达式，并拒绝为下层原始扫描增加批准。透明包装的正例也必须保留，防止保守拒绝扩大到已证明合法的来源。

全局下推行数限制、偏移、溢写和单设备继续从可支付集合排除。例如，一个来源因全局限制选择串行时，批准集合与可支付集合取交集后仍不包含该来源。原优化器的并行标记保持原值。

布局规划把固定管线数与初始工作者数分别保存。静态估计与有效历史提示确定初始人数，并可在构造阶段选择分组。查询开始后，布局与分组保持固定。预算限制固定布局上限。时间分区的最终工作单元重建后，规划器必须重新绑定实际布局与身份摘要。

局部执行计划在驱动提交前固定全部依赖引用。一次构造的依赖 future 后续由初始与增量工作者共用。例如，消费者已关闭后才首次获取惰性依赖对象，可能得到永远等待的新对象，因此构造阶段必须先取得稳定引用。

数据来源：`LocalExecutionPlanner.collectHashRuntimeApprovals` 的保守拒绝修复已进入源码，测试位于 `RuntimePlannerAdmissionTest` 和 `SingleSourceGroupByHashRepartitionPlanningTest`。固定布局由 `ScanParallelPlanner.planRuntimeLayout` 与 `RuntimeScanLayout` 表示。`DynamicParallelPlanningService`、`DataNodeTableOperatorGenerator`、`LocalScanPipelineBuilder` 和 `RuntimeScanGroupDescriptor` 负责最终布局、原算子构造和管线归属，路径均位于 [planner 目录](../../../../iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/planner/)。

## 有序标记的集成

有序标记及串行回退已经接入生产规划。分布式规划和最终排序消除保留符合资格的显式排序节点，本地构造使用相同资格判断。设备类型、排序键和实际拼接证明按[自然有序扫描的规划标记](运行期动态DOP设计草案.md#自然有序扫描的规划标记)执行。

专项必须同时检查跨区域与单区域计划，以及关闭开关后的冗余排序消除。序列化回归必须验证排序字段和设备列表。字段排序、时间键位置、分页、溢写、单设备和专用扫描类型分别验证原有回退。

串行回退专项必须构造实际设备条目并检查算子树。例如，严格递增的设备键加时间可以直接构造串行扫描。相同设备键前缀、仅时间排序或错误方向的输入继续保留排序，两个结果分别用完整行对照验证。

原有排序节点的非序列化附加位只承担其原用途。串行证明必须读取实际设备条目和排序字段。例如，节点经过序列化后，新增附加位即使保持默认值，设备顺序证明仍必须给出相同结果。

数据来源：生产修改位于 `TableDistributedPlanGenerator.java`、`SortElimination.java`、`DataNodeTableOperatorGenerator.java` 与 `OrderedParallelScanEligibility.java`。规划与本地回归位于 `PropertyDrivenDistributionTest`、`DeviceTableScanParallelPipelineTest` 和 `OrderedScanPipelineContractTest`，对应标准回执见[专项与全量验证](#专项与全量验证)。

## 运行组与原生任务

运行组保存固定逻辑驱动和独占执行权，原生调度器保存直接执行任务与运行工作者。逻辑驱动的原管线编号、工作单元和输出对象保持固定。根部与其他直接执行驱动保留原调度身份。

初始提交按构造归属创建逻辑驱动，建立执行组，再批量登记直接任务和初始工作者。片段管理器在成功或失败时都封闭构造。构造失败已经创建的驱动与尚未消费的工厂分别完成责任结算。

升宽先申请工作者增量内存，再准备原生登记和用户额度，最后整批激活。新增任务沿用原查询的截止时间和用户。例如，初始截止时间只剩三秒时，新增成员也只使用这三秒，后续调整保持原绝对值。

降宽等待目标成员停止执行、移除任务映射并结算额度。队列中的就绪任务、正在阻塞的任务和已领取执行权的任务分别使用对应唤醒与退出机制。例如，阻塞工作者退役后，阻塞完成的迟到通知仍必须服从已退役状态。

数据来源：[RuntimeScanGroup.java](../../../../iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/execution/runtime/RuntimeScanGroup.java) 与 [RuntimeScanWorker.java](../../../../iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/execution/runtime/RuntimeScanWorker.java) 负责领取与执行。原生接线位于 `DriverScheduler`、`RuntimeSchedulingGroup`、`PreparedDriverBatch` 和 `TaskRegistration`，其路径在 [schedule 目录](../../../../iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/execution/schedule/)。`FragmentInstanceManager` 负责构造，`RuntimeFragmentLifecycle.prepareAndSubmit` 负责提交归属。

## 控制器与进度采集

控制器复用执行组的就绪、阻塞、执行权、未完成数量和设备进度。驱动计时、扫描输出计数与资源采集继续使用原有入口。设备完成量和扫描输出量按[剩余工作与扫描活动](运行期动态DOP设计草案.md#剩余工作与扫描活动)分别解释。

生产策略先判断就绪与背压，再按设备完成增量和剩余设备判断升宽。高输出负载可能受消费者限制，策略可以保持最少工作者。诊断保存动作、原因、进度覆盖和采样范围，实际结果独立记录。

低输出用例必须保留完整扫描的执行工作，并限制返回数据规模。例如，执行阶段对完整扫描计算数据相关条件，只返回选定短时间范围，可以减少结果通道背压。独立真值仍按完整 SQL 的结果定义生成。

独立扫描活动候选继续保留在隔离暂存。生产集成与验收集合采用已接线的设备进度和控制器诊断。集成者必须先核对真实分支原因，再决定后续策略扩展。

数据来源：控制与策略位于 `RuntimeDopController` 和 `RuntimeDopPolicy`，进度来自 `RuntimeScanProgress`。`RuntimeDopExecutionSnapshot.controllerDiagnostics` 保存策略诊断，实际成员变化保存在 `groups.events`。低输出 SQL 的合同位于执行记录根目录下 `runtime-fullscan-window-contract-staging/`。

## 资源准入与交换

固定图基础内存、工作者增量内存、前沿预留和端点额度分别记账。运行模式必须完成真实基础预留，即使原配置关闭内存估计，或查询具有最高优先级。预留失败必须关闭已创建的资源，并保留失败原因。

构造示例中，固定图已经占用六 MiB，新工作者另申请一份增量内存。该工作者退役时只归还自己的增量份额。固定图及其输出仍由片段持有，直至终态关闭完成。

输入和输出端点按同一列模式导出有界编码合同。准入先检查全体端点的模式与容量总和，再支付每个真实账户。部分支付成功后发生失败，清理对象仍持有成功支付的责任。

整图协议选择为运行期的本地回环外部交换沿用 RPC 所有权协议。响应序列化使用期间，发送端保留响应引用。接收确认、任务退出和消费者退出分别触发对应额度归还，关闭回执等待全部实际使用者结算。

资源快照分别显示实际池预留、内部额度、端点任务和响应引用。例如，同一端点被两个包装对象观察时，物理账户仍只加一次。一个响应引用未归还时，端点仍处于待关闭状态，片段账户继续保留。

数据来源：`LocalExecutionPlanContext.initializeRuntimeExchangeBudget` 支付并登记端点责任，`LocalExecutionPlanner.checkMemory` 与 `reserveRuntimeWorkerMemory` 管理基础和增量内存。`RuntimeExchangeBlockCodec`、`SourceHandle`、`SinkChannel` 与 `RuntimeExchangeResponseProcessor` 实现有界交换。`RuntimeResourceSnapshot` 使用 V3 观测结构，原账户去重由 `LocalExecutionResourceRegistry` 绑定，容量公式见[远端交换与有界编解码](运行期动态DOP设计草案.md#远端交换与有界编解码)。

## 外部输出的计划类型与有界副本

`LocalExecutionPlanner.runtimeEndpointBounds` 按输出符号类型冻结跨 FI 列模式。 `compactNativeOutput` 校验输出通道模式相同，并把不可变模式传给输出算子；行数选择与切块先按目标物理宽度预算。`SinkChannel` 把这份模式传入 `RuntimeCompactOutputOperator.copyAllRowsBounded`。该方法先按目标列表示估计完整复制内存，再构造独立副本，最后继续通过 `RuntimeExchangeBlockCodec.inspect` 的严格检查。

原生常量折叠路径可能将计划 INT64 的小整数恢复为 INT32 物理列，因此外部副本显式允许无损 INT32 到 INT64。未类型空列按目标模式构造，逻辑别名按相同物理表示处理。缩窄、浮点转整数、无关类型与列数不匹配均拒绝，原 wire gate 不放宽。

既有测试体系补齐常量 RLE 的独立 backing、负值与整数边界、空值与各物理类型、非法转换、扩宽后的精确内存边界和严格 codec 往返。例子：复制到 long 数组后的真实上界可以成功，额度少一个字节时必须在分配输出数组前拒绝。v6 专项 808/0/0/5 与真实常量 SQL 已通过；还增加多列常量及所有物理类型全空列的跨块回归，完整保留 1024 行及重复次数。

数据来源：生产修改位于 `RuntimeCompactOutputOperator.java` 与 `SinkChannel.java`，测试位于既有 `RuntimeCompactOutputOperatorTest`、`RuntimeExchangeBlockCodecTest`。类型来源及容量关系见[远端交换与有界编解码](运行期动态DOP设计草案.md#远端交换与有界编解码)。

## 内存回调与取消原因接线

内存就绪通知在注册前取得端点任务责任，并通过异步执行器转交原工作执行器。关闭屏障等待该责任实际结算。内存池的授予与取消协议继续在其原监视器内完成，具体交错见[内存就绪回调与任务责任](运行期动态DOP设计草案.md#内存就绪回调与任务责任)。

回归必须使用 latch 控制线程顺序。例如，测试线程持有输入端点锁，另一个线程完成内存授予并返回，端点执行器采用内联执行。该交错必须完成，预先登记的通知也必须持续计入关闭条件。

回归还必须覆盖通知已排队时的 close 与 abort，以及原执行器拒绝。关闭回执必须等待排队通知退出，所有内存只结算一次。拒绝造成的失败继续通过关闭回执报告。

取消原因只在异常关闭路径归一化。缺失原因时，资源注册表创建一个共享取消异常，并交给各端点和队列。真实失败已经提供异常时，注册表保持原对象。

例如，一个本地队列有等待消费者，另一个有缓冲字节，取消关闭必须同时完成唤醒与释放。重复关闭使用同一完成回执。正常关闭继续使用原有方法，片段终态保持由状态机决定。

数据来源：内存回调位于 `SourceHandle.java`，对应回归为 `SourceHandleTest` 与 `MemoryPoolTest`。取消归一化位于 `LocalExecutionResourceRegistry.closeRuntimeOwners`，对应回归为 `LocalExecutionResourceRegistryTest`。异步通知与资源屏障均继续由 `RuntimeCleanupCoordinator` 关联。

## 查询级超时原因与哨兵代际

`DriverScheduler` 为真实 deadline anchor 提供带原始异常和预期 anchor 对象的查询级停止入口。在同一个 registration lock 区内核对 anchor 身份，并截取实际 FI、注册任务与遗留任务。超时将同一个 `QueryTimeoutException` 传播给该快照内的任务；用户或遗留普通 abort 保持逐任务级联原因。

旧 anchor 已被替换时，该回调不停止新的运行组。例如，两个 FI 共用查询截止时间，更早截止时间替换 anchor 后，旧哨兵迟到触发必须无作用。当前 anchor 到期后，两组均携带原始 720 异常，配额和调度映射全部结清。

在既有 `RuntimeDriverSchedulerTest` 中覆盖真实哨兵跨组传播、替换 anchor 拒绝及普通 abort 兼容。服务验收仍要求原始正截止时间、实际运行组活动、精确 720 和全部资源结算。v4 的 715 失败原件保留；最终 v6 已以权威 JDBC vendor720、各 FI 原 deadline 及全部资源回执通过验收，原生 CAST 701 也保留真实 cause。

数据来源：生产入口为 `DriverScheduler.abortQuery` 与 `Scheduler.toAborted`，真实哨兵为 `DriverTaskTimeoutSentinelThread`。终态责任顺序见[终止与统计冻结](运行期动态DOP设计草案.md#终止与统计冻结)。

## 清理与统计接线

所有终态路径先停止执行，再按资源所有者的实际回执推进清理。构造封闭和控制证据结算分别提供等待条件。直接执行驱动与逻辑扫描驱动都必须支持协作静止和实际关闭确认。

控制调整在任何验证和执行前登记结算责任。回调先记录结果，再完成结算，随后通知外部监听器。例如，原生退役 future 的一个监听器已触发片段终态，而记录调整结果的另一个监听器还未执行时，清理必须继续等待控制结算。

片段在驱动实际关闭后冻结统计，并在控制器停止屏障后复制策略诊断，随后关闭端点、归还前沿、释放基础资源并注销账户。普通 SQL 即使关闭历史反馈，仍保存运行期控制载荷到终态缓存。实际关闭失败时，资源持有关系继续保留供失败回执和诊断使用。

成功清理再解除生命周期对象对驱动、描述、回调和端点的强引用。构造早期失败同样沿用片段上下文中的清理对象。例如，首个端点成功预留后，后续驱动构造失败，清理仍按已登记责任释放首个端点。

数据来源：`RuntimeCleanupCoordinator` 定义阶段顺序，`RuntimeFragmentLifecycle` 管理 `controlSettlementsDone`。`FragmentInstanceContext` 在终态请求停止，`FragmentInstanceExecution` 冻结统计与缓存。专项测试位于 `RuntimeCleanupCoordinatorTest`、`RuntimeFragmentLifecycleTest` 和 `FragmentRuntimeSnapshotProductionTest`，源码在 [execution 目录](../../../../iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/execution/)，测试在 [queryengine 测试目录](../../../../iotdb-core/datanode/src/test/java/org/apache/iotdb/db/queryengine/)。

## 查询身份与结果接口

验收驱动必须读取派发前固定的片段名单，再按同一次普通查询收集统计和资源回执。精确 SQL 摘要用于查找唯一执行对象，数值 RPC 查询号通过实际协调器映射解析。计划重试或移除期间，驱动必须等待与一次分布式规划绑定的明确快照。

例如，名单包含一个根片段和四个其他片段时，验收必须归档全部五个片段的主机与执行状态。尚未执行的计划片段必须提供独立派发证明。片段列表的短期稳定只能证明观察结果稳定。

结果驱动提供完整排空回执、类型模式和逐行 JSON 数组。TIMESTAMP 使用服务器毫秒整数，INT64 保留整数精度，SQL 空值使用 JSON 空值。离线比较核对原始字节摘要，再以外部排序和逐项出现次数比较完整多重集。

例如，候选与对照各有三行，且都出现同样两种行，但其中一种行的出现次数分别为二和一时，结果必须拒绝。排序键唯一的用例核对完整序列，相同排序键的用例核对键单调与逐组多重集。取消和超时保留已返回前缀，并使用各自的异常验收规则。

负例优先核对非零 JDBC vendor code，异常链出现冲突即拒绝。原 JDBC 的 ResultSet.next 可能丢失 code、SQLState 和 cause；兼容识别只允许精确 SQLException 类型、raw code 为零、SQLState 与 cause 为空、栈顶真实 JDBC 来源，以及开头严格符合 715、720 或 751 加冒号、空格、非空内容的消息。保存 raw、parsed、effective code 及 provenance。

该路径识别受限旧包装来源，客户端没有保存该次真实 RPC 的原始 TSStatus，不能将其写成原始结构化状态拦截。原生 CAST 的 701 必须来自权威非零字段；不扩展消息兼容白名单。

例如，用户取消后，真实 JDBC next 抛出 raw code 为零的 715 包装；只有完整来源约束都成立时才可识别。任意客户端文本包含 715 仍拒绝。固定发行 JDBC 的 25 项离线包装自测只验证该兼容合同，实际查询号、FI 终态和资源必须另由服务回执建立。

数据来源：JMX 查询入口为 `describeQueryExecution` 与 `describeQueryExecutionBySqlHash`，实现位于 `MPPDataExchangeService` 和 `QueryExecutionDescription`。Java 驱动入口为 `StaticScanAcceptanceDriver --mode ordinary-sql --params`，加载入口为 `--mode load-runtime-fixture`。结果文件为 `result.rows.jsonl`、`result.schema.json` 和 `result-receipt.json`。独立交付接口位于 `runtime-sql-driver-staging/parameters-spec.json` 与 `runtime-result-compare-staging/interface.json`，主流程必须按最终修订清单绑定。严格错误码识别由 `OrdinarySqlAcceptanceDriver` 和 `runtime-jdbc-sql-code-staging/` 中的工具交付处理。

## 专项与全量验证

标准测试回执必须绑定测试前的源码、依赖和实际执行入口。专项覆盖局部协议，完整模块回归覆盖同一构建的测试集合。发行包与执行服务分别继续核对实际 class 和 JAR 身份。

v2 的 DataNode 及 reactor 依赖完整回归执行 7991 项，失败和错误均为零，跳过 56 项。其中 DataNode 执行 6829 项，跳过 53 项，包含 964 份 XML。其他模块执行 1162 项，跳过 3 项。归档共包含 1147 份真实 XML，全部位于原执行时间范围。

v2 的完整测试工作树产生 4504 个 DataNode 生产 class，它们与 v2 冻结 server JAR 逐字节一致。核查包含内部类与匿名类，并核对完整文件集合。该结果绑定完整测试工作树，服务验收继续使用实际加载来源。

v3 的控制器诊断专项执行 47 类、651 项，失败、错误和跳过均为零。专项耗时 109.88 秒，发行包构建耗时 134.78 秒，两阶段均正常退出。源码指纹在执行期间保持一致。该专项的覆盖范围由其绑定源码与回执决定。

最终 v6 的 51 类标准专项为 808/0/0/5；完整 reactor 为 8037/0/0/56，其中 DataNode 为 6875/0/0/53。clean package、中文编译及全部普通 SQL 服务矩阵均已完成。七项 pipe-api 缓存测试已禁用缓存补跑，两条未传播到 JUnit 的后台断言在纯上游精确复现；原件与最终统计见[正式验收摘要](audit/final-runtime-v6-validation.json)。专项与全量不累加。

例如，v3 专项通过证明其绑定源码的诊断冻结行为。三项生产修复以新的源码摘要进入独立专项。执行被终止或缺失最终 XML 汇总时，完整模块结果保留待完成状态。

执行者必须保留失败日志和测试 XML，分析日志时先去除 ANSI 颜色码。原始回执保持其版本与路径，新增补充回执独立归档。例如，原汇总缺少测试字段，而原始 XML 完整时，补充归档必须关联原汇总摘要。

数据来源：v2 全量原运行位于 `full-v2-run-20261115T130313Z/`，补充归档与 class 对照位于 `full-release-equivalence-v2/supplemental-receipt.json` 和 `full-release-class-equivalence.json`。原回执与归档均保持不可变。补充回执 SHA256 为 `33cec01e80ac8f885d6ccd778624dae3ce8c5b1cd453f0802047b51c2f5dba42`。v3 专项与构建回执位于 `release-v3-validation-20261115T143108Z/summary.json`，发行清单为 `runtime-release-v3.json`，其 server JAR SHA256 为 `1ebc59bac8371ac92b32554899109c17ab86a221ed666e33110a25efea289954`。三项修复专项的历史运行目录为 `integration-v4-targeted-20261115T145838Z/`；当前正式结论采用 v6 的专项、fresh 全量 XML 与发行类对照，见[最终验收摘要](audit/final-runtime-v6-validation.json)。上述路径均相对于[源码与交付身份](#源码与交付身份)的执行记录根目录。

当前完整标准命令集中在 [README 的标准验证](README.md#标准验证)，包括 51 类专项、带依赖 reactor 的全量、中文编译和 clean package。执行前使用新的持久输出目录及锁定依赖，并设置 `GITHUB_ACTIONS=true` 按仓库配置关闭本地测试缓存；归档实际 XML 的 tmpdir/classpath 与退出状态。早期十类 `surefire:test` 子集不代表本次最终全量验收。

## 隔离服务与确定性数据

服务验收使用候选和对照各一 ConfigNode、两 DataNode 的隔离部署，统一使用同一份构建产物。启动器分别固定端口、JMX、堆大小、查询线程数、配置摘要和全部运行 JAR。每次启动还核对真实 PID、监听端口和 Java 可执行文件。

加载器按设备与时间分区的固定顺序使用有界批次写入，并在每个分区结束后执行刷盘。夹具真值来自独立行公式，文件摘要、解析行数和完整聚合结果分别离线核验。最终 v6 的干净候选和对照各有一个 ConfigNode、两个 DataNode，均已加载 9404416 行，并以实际进程、发行包和源区域身份完成服务验收。v2/v4 的物理布局原件仍按历史版本保留；每次新部署必须按实际区域映射核对文件，跨侧位置可以不同。

静态估计需要为运行期升宽留下空间。预检必须记录每个实际来源区域的所有顺序和乱序 TsFile 大小，且每个来源的总大小为正并小于 128 MiB。例如，某来源实测为 64 MiB 时，字节估计给出一个初始工作者，再由真实激活记录确认这一人数。

实际来源必须同时覆盖两个真实 DataNode，注册节点数与来源分布分别核查。输入压缩文件的大小只能描述夹具文件。数据放置和初始人数以加载后区域文件及实际运行组为依据。

数据来源：当前 v6 的启动、加载、实际来源和全部运行映射见[最终验收摘要](audit/final-runtime-v6-validation.json)及[接受参数清单](audit/v6-accepted-parameter-map.json)。历史 v2 物理布局位于 `fixture-layout-evidence-v2/delivery.json`。隔离启动器为 `launch-isolated-nxp.sh`，独立交付清单为 `runtime-fixture-staging/delivery.json`。确定性数据清单位于 `fixture-generation-20261115T105303Z/standard/manifest.json`，其 SHA256 为 `61db329c3a670b4144ada663d8dbd42134e3ee72592c597f0a34a0725583168b`。夹具目录及上述相对交付目录均位于本稿[源码与交付身份](#源码与交付身份)列出的执行记录根目录，标准 fixture 规模与全部真值以该不可变清单为准。

## 普通 SQL 服务验收

七项服务用例分别记录实际查询身份、真实执行路径、结果与资源状态，并按各自判据单独判定。候选由生产控制器自动调整，静态对照使用独立部署。跨查询重启设置固定 DOP 只记录为对照配置步骤。

服务驱动必须为每次 SQL 保存唯一运行标记、精确语句摘要和协调器片段名单。资源观察窗口在目标 SQL 前开启。查询结束后，驱动在终态缓存有效期内收集全部实际片段的控制载荷、原始 RPC 响应及资源回执。

例如，一次目标查询的结果正确，但升宽事件来自另一次诊断查询时，该用例的运行期证据不足。诊断 SQL 和目标普通 SQL 必须分别归档查询标识。全部零值的资源记录同样缺少真实创建与分配证据。

数据来源：验收条目以 `runtime-matrix-staging/case-manifest.json` 的 R1–R7 为准。R6 覆盖真实执行异常。R7 包含标准 GROUP BY 与 Filter/Project GROUP BY 两个子形态。

### 查询内升降宽

候选必须在同一内部查询、同一片段实例及同一来源组中依次完成升宽与降宽，固定布局身份保持一致。例如，实际激活人数为一，随后结算为二，再结算为一，且两次变化都发生在查询终态前，才满足方向顺序。

正式 R1 使用保留残余过滤的完整源扫描，读取全部 9404416 行并输出 4096 行，与固定一个工作者的对照及独立真值比较完整类型多重集。高输出扫描另作兼容性与背压用例，不能替代 R1 的同组实际扩缩容证据。事件保留范围必须覆盖目标转移，缺失时收集同查询运行中快照。自动控制未产生所需变化时，该项登记为未覆盖或失败。

历史 v3 的低输出完整扫描诊断运行在同一个查询、片段与来源组中实际激活一人，随后已应用人数依次为两人、三人和两人。两侧来源各保留八条固定逻辑管线。查询完整排空 4096 行，耗时约 1.78 秒，诊断驱动正常退出。

该历史运行提供真实成员调整的诊断证据，当时尚未构成独立真值与多重集比较结论。该 v3 集群还有前序取消用例未完成资源清理的记录；其失败原件保留，随后使用干净 v4、最终 v6 集群分别重新验收。v3 诊断仅确认当时的代码路径，最终完成由 v6 独立服务矩阵及资源基线决定。

例如，诊断记录升宽请求，而实际事件保存配额拒绝时，完整结果可以通过，运行期升降宽必须继续等待真实成员变化。策略诊断和实际事件的字段边界见[查询身份与运行证据](运行期动态DOP设计草案.md#查询身份与运行证据)。

数据来源：`R1` 的低输出完整扫描合同位于 `runtime-fullscan-window-contract-staging/`。诊断查询号为 `20261115_145040_01279_1`，结果与成员事件位于 `runtime-sql-acceptance-v3/R1/fullscan-window-diagnostic/candidate/r001/` 的 `result-receipt.json` 和 `runtime-actions-observed.json`。诊断进程回执为 `sql-v3-R1-fullscan-window-diagnostic-candidate/exit-receipt.json`。前序取消的资源失败记录位于 `runtime-sql-acceptance-v3/R4/primary/candidate/r001/failure.json`，运行事件来自 `__runtime_dop_control`，固定身份核对 `lanePlanChecksum` 与 `lanePlanIdentityChecksum`。实际变化读取 `groups.events` 的 `APPLIED`，策略诊断读取 `controllerDiagnostics`。

最终 v6 已独立完成 R1 的完整三方结果比较和资源验收；同一组实际为 1→2→3→2→1，全部源输出计数为 9404416。正式证据采用[最终验收摘要](audit/final-runtime-v6-validation.json)和[实现进度](实现进度与验证记录_20261115.md)，上述 v3 诊断及失败原件保持不变。

### 单工作者有序多流

候选必须真实建立多条有序逻辑管线，并以一个激活工作者完成查询。例如，三条固定有序流由一个工作者轮转时，全部首部与空流状态都必须推进，归并结果才能完整返回。

按设备键再按时间排序的主用例具有确定顺序，验收核对完整类型多重集和完整序列。它同时核对实际有序执行模式、多个逻辑管线以及最大已应用人数。若某次查询实际只有串行单管线，只登记其兼容性，不能替代 ORDERED 多流覆盖。

按时间单键排序的补充用例采用相同键分组合同。验收分别检查每侧的时间单调，并逐时间组比较全部带类型的行。完整多重集用于发现丢失和重复，逐组比较用于绑定正确时间，规范组内排序后的摘要用于稳定复核。

例如，两行都属于时间零，在组内交换位置满足 SQL 顺序。若时间一的行出现在时间零组之前，顺序检查必须拒绝。真实有序执行与初始人数检查继续独立执行。

多源自然顺序的生产标记与安全串行回退已接线。最终 v6 的 R2 主用例已完成 ORDERED 多 lane、初始及最大活动 worker 均为一，以及完整 4096 行序列与三方结果比较。时间单键补充用例另以完整 4096 行验证时间单调和 64 个同键组的完整多重集；其实际排序形态与主用例分别归档。

数据来源：`R2` 主用例采用 `ordered` 真值和 `device_id,time` 排序。最终主用例与 time-only 的完整比较见[三方结果摘要](audit/v6-complete-result-comparisons.json)和[正式验收摘要](audit/final-runtime-v6-validation.json)；SQL 与参数由当前 matrix 绑定。最初的 `ordered-readonly-investigation-20261115T142800Z/` 合同作为历史来源保留。

### 行数限制提前结束

候选必须找到真实运行组已经活动、下游行数限制提前结束的普通查询形态。例如，返回 257 行后下游停止时，所有已创建的工作者、队列与端点仍必须完成实际关闭和账户归还。

行数限制主用例采用 ordered 真值对应 SQL 追加 `LIMIT 257`，候选与对照必须返回同一份完整 257 行。最终 v6 的主用例合法回退 SERIAL，已通过完整结果及资源兼容性验证；该主用例不证明运行期提前结束。原生运行组覆盖由下述常量 LIMIT 扩展独立提供。

完整有序限制查询可以使用确定的真值前缀。例如，取完整有序真值的前十行时，工具必须先检查完整真值的类型、行数和文件摘要，再从同一份快照取前十行。候选与对照仍提供该 SQL 的完整排空回执，并同时比较完整多重集和有序序列。

原生提前结束扩展使用 `runtimeEarlyLimitConstant`：残余 SIN/COS 过滤读取 s1，投影 `CAST(1 AS INT64)` 后全局 `LIMIT 257`。标准夹具的完整结果是 257 个 INT64 的 [1]；无序选行不会改变该多重集。比较不得再套 `--oracle-limit` 或 tie 参数。

普通 SQL 驱动完整排空与独立比较器都通过后，还必须读取冻结统计：同查询实际源运行组有正激活，按 `__pipeline_*` TableScanOperator 实例键去重，源输出总数满足 0 < rows < 9404416。原始 planNodeId 的聚合别名不重复计入。只有这些条件连同实际清理均成立，才登记运行期提前结束通过。

数据来源：最终 v6 的 `R3-primary` 与 `R3-runtime-early` 均完整返回 257 行并通过三方比较；后者两个实际 DEVICE 组的唯一 pipeline 源输出为 70000+90000=160000，小于全部 9404416 行，资源均结清。证据见[完整结果比较](audit/v6-complete-result-comparisons.json)与[SQL独立审计](audit/sql-v6-independent-summary.json)。有序前缀接口为 `--oracle ordered --oracle-limit N`；常量 LIMIT 不使用该参数。时间单键查询若在相同键组内截断，必须另定义合法选行合同。

### 活动查询定向取消

候选必须先观测到真实运行组活动和正向资源占用，再从独立会话取消该查询。例如，已确认目标内部查询标识并观察到工作者活动后，取消会话只结束这一查询。

验收保存取消请求结果、目标查询终态、有效结果前缀和全部资源回执。实际关闭失败按失败结果保留物理责任。取消路径使用已实现的定向 SQL 入口。

数据来源：`R4` 采用 `KILL QUERY '<实际内部查询标识>'`，客户端 `Statement.cancel` 的可用性按生产实现独立核查。

### 原截止时间超时

候选必须在运行组已经活动后触发真实截止时间失败，增量工作者继续使用原截止时间。例如，一次查询的执行和后续取数跨过原截止时刻时，新增成员仍必须参与同一超时清理。

验收保存执行或取数阶段的实际超时状态，并核对全部片段的关闭、前沿和账户回执。查询解析或规划阶段的提前拒绝按对应阶段登记。运行期超时覆盖需要真实运行组证据。

数据来源：`R5` 对应 `timeoutTarget`，原截止时间由 `originalDeadlineMillis` 记录。

### 执行异常与失败清理

候选先建立真实运行组，再由执行算子遇到数据相关错误。R6 将实际非数字设备标识转换为 INT64，SIN/COS 残余过滤保持读取真实 s1。表达式在优化阶段仍依赖符号，实际执行遇到 d000 等标识时，原生 CAST 抛出 701。最终负载只在 d063 的时间区间 [1815448512,1815448576) 触发，避免快失败先于真实 roster 采集。实际启动、错误、FAILED FI 与资源结算必须属于同一次 SQL。

原除零表达式实际输出 NULL 并成功排空，不能继续作为异常触发器。701 的符号名为 SEMANTIC_ERROR，但名称本身不能区分规划和执行；同查询原生运行组激活与 FI 失败证据仍是必须条件。

规划拒绝、客户端主动断连或尚未创建运行组的失败只能证明相应阶段的行为。负例保存已产生的有效结果前缀，但不将前缀声明为完整结果或与完整真值等价。

数据来源：`R6` 的精确 SQL 为 `runtimeException.sql`（d063 最后分区尾窗），期望码为 `SEMANTIC_ERROR=701`；`TypeServices.CAST_VALUE_SERVICE` 调用 Long.parseLong 并将非法输入包装为 SemanticException。SQL 与参数绑定在最终 matrix 清单。driver 的 `expectedOutcome=EXCEPTION` 保存原异常、其他 FI 的独立统计尝试与资源关闭结果。

### 双节点多源分组聚合与透明包装

R7 包含标准分组与 Filter/Project 两个子形态。候选必须证明实际来源分布覆盖两个 DataNode，并核对全部来源、bucket、通道和 FI 的唯一身份。桶数从实际发送端计划读取，全部来源必须一致，每个来源恰有相同数量的连接。发送端与输入端逐边互证。例如，两个来源各连接三个桶时，验收必须确认六条独立连接。

候选与 runtime=false、固定 DOP=1 的对照执行同一份 SQL，并与独立 fixture 真值比较完整类型多重集。对照的实际 hash 路径按计划记录，不假定关闭。全部 FI 和连接都从同一次普通 SQL 的固定片段名单读取。升降宽的专门证明由 R1 承担。

透明包装子形态过滤非负值，并对分组键、求和值做加法。原优化器可继续合法下推过滤，验收归档实际优化后形态与结果，不为保留某个包装节点而关闭原优化。顺序敏感的 DIFF 仍保持原并行否决，另由规划专项和兼容性 SQL 核查。

数据来源：`R7` 使用 `grouped` 与 `filterProjectGrouped` 两个真值，节点身份来自 ConfigNode，交换边来自同次普通 SQL 的 `QueryExecutionDescription`。桶数读取 hash sink 的 `partitionCount`，边的两侧使用 `SINK_CHANNEL` 和 `EXCHANGE_INPUT`。

## 交付与验收记录

最终交付必须同时保存源码清单、构建产物、标准测试和七项普通查询服务回执。源码状态记录对应入口已存在，标准测试记录其实际执行范围，服务回执记录真实路径、完整结果和资源责任。性能结论由独立重复对照实验给出。

集成者必须逐项登记内存回调、取消原因、有序标记、串行回退、外部类型归一化、查询级超时原因和严格错误码识别的验收结果。每项完成后，执行者更新标准回归数量、实际 JAR 身份和服务查询标识。文档必须按已完成证据填写，缺失值保留为待验收。

例如，某个用例的结果与资源均通过，但实际计划为串行时，其结果兼容性可以登记通过，多流或成员变化覆盖仍继续执行。终态清理后的零成员只用于资源关闭核查。负例的有效结果前缀按异常规则保留。

数据来源：构建与回归记录见[专项与全量验证](#专项与全量验证)，服务判据见[普通 SQL 服务验收](#普通-sql-服务验收)。独立文档候选的来源、基线摘要与待补字段随本次不可变文档交付清单登记。正式交付必须重新生成仓库文档清单，并同步[运行期动态 DOP 复现步骤](REPRODUCING_RUNTIME_SQL.md)。
