## 查询过程状态机

IoTDB查询引擎使用多层状态机来管理查询执行的不同粒度状态。主要包括QueryStateMachine（查询级别）、FragmentStateMachine（Fragment级别）和FragmentInstanceStateMachine（FragmentInstance级别）。

### QueryStateMachine（查询级别状态机）

类路径：`org.apache.iotdb.db.queryengine.execution.QueryStateMachine`

QueryExecution使用QueryStateMachine来管理查询的整个生命周期状态。状态定义在`org.apache.iotdb.db.queryengine.execution.QueryState`枚举中，包含以下状态：

- QUEUED（排队）：查询已创建，等待执行。这是QueryExecution创建时的初始状态（doneState=false）。
- PLANNED（已计划）：逻辑计划和分布式计划都已生成完成，查询计划已准备好（doneState=false）。
- DISPATCHING（分发中）：正在将FragmentInstance分发到各个DataNode执行（doneState=false）。
- PENDING_RETRY（等待重试）：分发过程中出现错误，等待重试（doneState=false）。
- RUNNING（运行中）：所有FragmentInstance已成功分发，查询正在执行（doneState=false）。
- FINISHED（已完成）：查询成功完成，所有结果已返回（doneState=true，终止状态）。
- CANCELED（已取消）：查询被用户或系统取消（doneState=true，终止状态）。
- ABORTED（已中止）：查询因异常情况被中止（doneState=true，终止状态）。
- FAILED（失败）：查询执行失败（doneState=true，终止状态）。

状态转换流程（在`org.apache.iotdb.db.queryengine.plan.execution.QueryExecution`中）：
1. QueryExecution创建时，在构造函数中创建QueryStateMachine，状态机初始化为QUEUED状态（`QueryStateMachine.QueryStateMachine()`）
2. 在`QueryExecution.start()`函数中，如果`skipExecute()`返回true，可能直接从QUEUED转换到RUNNING（`stateMachine.transitionToRunning()`）
3. 否则，执行`doLogicalPlan()`和`doDistributedPlan()`后，调用`stateMachine.transitionToPlanned()`转换到PLANNED状态
4. 调用`schedule()`函数时，`ClusterScheduler.start()`会调用`stateMachine.transitionToDispatching()`转换到DISPATCHING状态
5. 如果分发成功，调用`stateMachine.transitionToRunning()`转换到RUNNING状态
6. 如果分发失败且需要重试，调用`stateMachine.transitionToPendingRetry(failureStatus)`转换到PENDING_RETRY状态
7. 查询完成后，根据结果调用`transitionToFinished()`、`transitionToFailed()`或`transitionToCanceled()`转换到终止状态

状态机使用`org.apache.iotdb.db.queryengine.execution.StateMachine`类实现，支持状态变更监听器（`addStateChangeListener()`），允许其他组件监听状态变化并做出相应处理。

### FragmentStateMachine（Fragment级别状态机）

类路径：`org.apache.iotdb.db.queryengine.execution.fragment.FragmentStateMachine`

Fragment的状态定义在`org.apache.iotdb.db.queryengine.execution.fragment.FragmentState`枚举中，包含以下状态：

- PLANNED（已计划）：Fragment已计划但尚未调度（doneState=false, failureState=false）
- SCHEDULING（调度中）：FragmentInstance正在节点上调度（doneState=false, failureState=false）
- RUNNING（运行中）：Fragment正在运行（doneState=false, failureState=false）
- PENDING（待处理）：Fragment已完成执行现有任务，但可能在未来调度更多实例（doneState=false, failureState=false）
- FINISHED（已完成）：Fragment已完成执行且所有输出已被消费（doneState=true, failureState=false）
- ABORTED（已中止）：Fragment因查询中的失败而被中止，失败不在该Fragment中（doneState=true, failureState=true）
- FAILED（失败）：Fragment执行失败（doneState=true, failureState=true）

### FragmentInstanceStateMachine（FragmentInstance级别状态机）

类路径：`org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine`

FragmentInstance的状态定义在`org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceState`枚举中，包含以下状态：

- PLANNED（已计划）：Instance已计划但尚未调度（doneState=false, failureState=false）
- RUNNING（运行中）：Instance正在运行（doneState=false, failureState=false）
- FLUSHING（刷新中）：Instance已完成执行，输出待消费。此状态下不会有新的driver，现有driver已完成，输出缓冲区至少处于'no-more-tsBlocks'状态（doneState=false, failureState=false）
- FINISHED（已完成）：Instance已完成执行且所有输出已被消费（doneState=true, failureState=false）
- CANCELLED（已取消）：Instance被用户取消（doneState=true, failureState=true）
- ABORTED（已中止）：Instance因读取失败而被中止，失败不在该instance中（doneState=true, failureState=true）
- FAILED（失败）：Instance执行失败（doneState=true, failureState=true）
- NO_SUCH_INSTANCE（实例不存在）：Instance未找到（doneState=true, failureState=true）

### DriverTaskStatus（DriverTask级别状态）

类路径：`org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskStatus`

DriverTask的状态定义在枚举中，包含以下状态：

- READY（就绪）：准备执行
- RUNNING（运行中）：正在执行
- BLOCKED（阻塞）：等待上游输入或下游FragmentInstance消费输出
- ABORTED（中止）：因超时或协调器取消而中断
- FINISHED（完成）：因遇到上游输入的EOF而完成

状态转换由`org.apache.iotdb.db.queryengine.execution.schedule.ITaskScheduler`接口的实现类`DriverScheduler.Scheduler`管理。

表模型的查询流程梳理（流程图之后再画）
这里描述在Session读取查询SQL语句后IoTDB内部的处理流程。
## 1 Session接收和发送SQL

主要类路径：
- `org.apache.iotdb.session.Session`：客户端Session类
- `org.apache.iotdb.session.SessionConnection`：Session连接类
- `org.apache.iotdb.rpc.IClientRPCService`：客户端RPC服务接口（Thrift生成的接口）
- `org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl`：DataNode的RPC服务实现类

SQL的接收与发送流程：

1. 客户端接收SQL（`Session.executeQueryStatement(String sql)`）：
   - 客户端调用`Session.executeQueryStatement(String sql)`接收SQL语句
   - 该方法内部调用`Session.executeStatementMayRedirect(String sql, long timeoutInMs)`，传入查询超时时间（默认使用`Session.queryTimeoutInMs`）

2. 重定向检查（`Session.executeStatementMayRedirect()`）：
   - 调用`SessionConnection.executeQueryStatement(String sql, long timeout)`执行查询
   - 如果捕获到`RedirectException`，说明需要重定向到其他节点
   - 如果启用了查询重定向（`enableQueryRedirection=true`），会调用`Session.handleQueryRedirection()`更新连接，然后重试查询
   - 如果未启用重定向或重定向失败，抛出异常

3. 封装和发送请求（`SessionConnection.executeQueryStatement(String sql, long timeout)`）：
   - 创建`TSExecuteStatementReq`请求对象，设置`sessionId`、`statementId`、`sql`、`fetchSize`、`timeout`、`enableRedirectQuery`等参数
   - 调用`client.executeQueryStatementV2(execReq)`发送请求（`client`是`IClientRPCService`接口的实现，通过Thrift生成）
   - 使用`callWithRetryAndReconnect()`封装重试和重连逻辑
   - 根据返回的`TSExecuteStatementResp`状态码验证是否成功，支持重定向验证（`RpcUtils.verifySuccessWithRedirection()`）

4. 创建结果集（`SessionConnection.executeQueryStatement()`）：
   - 使用响应数据创建`SessionDataSet`对象，包含列信息、数据类型、查询ID等
   - 返回`SessionDataSet`给调用者，用于后续的数据获取

说明：整个流程使用Thrift RPC协议进行通信，支持自动重试、重连和查询重定向功能。

## 2 DataNode接收并处理查询请求

主要类路径：
- `org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl`：DataNode的RPC服务实现类

在`ClientRPCServiceImpl`中，`executeQueryStatementV2()`函数被thrift触发，经过`executeStatementV2()`中转来到`executeStatementInternal()`，下面介绍在`executeStatementInternal()`当中的流程。

### 2.1 SQL语句解析为Statement

主要类路径：
- `org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser`：SQL解析器
- `org.apache.iotdb.db.relational.grammar.sql.RelationalSqlLexer`：词法分析器（Antlr生成）
- `org.apache.iotdb.db.relational.grammar.sql.RelationalSqlParser`：语法分析器（Antlr生成）
- `org.apache.iotdb.db.queryengine.plan.relational.sql.ast.builder.AstBuilder`：AST构建器
- 语法文件：`iotdb-core/relational-grammar/src/main/antlr4/org/apache/iotdb/db/relational/grammar/sql/RelationalSql.g4`

首先会判断客户端的`SqlDialect`是表模型`TABLE`还是树模型`TREE`。对于表模型的查询流程，这里使用`SqlParser`来实现从SQL语句到Statement树的转化工作。

解析流程：

1. 词法和语法分析（`SqlParser.invokeParser()`）：
   - 创建`RelationalSqlLexer`对象，将SQL字符串转换为token流（`CommonTokenStream`）
   - 创建`RelationalSqlParser`对象，使用token流进行语法分析
   - 语法解析采用两阶段策略：
     - 第一阶段（SLL模式）：优先使用SLL（Simplified LL）模式，该模式不处理上下文信息，效率更高，但可能有误报
     - 第二阶段（LL模式）：如果SLL模式报错，再使用完整的LL模式去校验，检查是否是真实的解析问题
   - 解析结果是一个`ParserRuleContext`对象，表示语法解析树
   - 语法规则定义在`RelationalSql.g4`文件中，从`singleStatement`规则开始，逐步识别SQL的tokens内容，构建出语法树

2. AST构建（`AstBuilder.visit()`）：
   - 新建一个`AstBuilder`对象（位于`org.apache.iotdb.db.queryengine.plan.relational.sql.ast.builder`包）
   - 使用`AstBuilder.visit()`操作解析语法树
   - `visit()`方法继承自`AbstractParseTreeVisitor`，会调用语法树节点`ParseTree`的`accept()`方法
   - 这里使用访问者（Visitor）设计模式，从语法树的根节点开始，根据节点对象的类型调用相应的处理函数
   - 表模型的抽象语法树节点类在`iotdb-relational-grammar`的`RelationalSqlParser`中定义
   - 在`accept()`中会调用`AstBuilder`中对应的`visitXXX()`处理函数
   - 对于非叶节点，会先处理其子节点，然后处理当前节点
   - 每个抽象语法树节点在解析后都会将其信息封装为`Statement`类（或`Relation`类，表结构的类继承于`Relation`，本质含义和`Statement`一致，只是名字不同）进行返回
   - 整个语法分析树解析完成后，最终得到的是一个statement的树状结构，返回的是这个树的根节点所对应的statement

### 2.2 分析Statement信息

主要类路径：
- `org.apache.iotdb.db.queryengine.plan.Coordinator`：查询协调器，负责协调查询的整个生命周期
- `org.apache.iotdb.db.queryengine.plan.execution.QueryExecution`：查询执行对象，管理查询的执行状态和结果
- `org.apache.iotdb.db.queryengine.plan.relational.planner.TableModelPlanner`：表模型计划器，负责表模型查询的计划生成
- `org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analyzer`：语义分析器，负责SQL的语义分析

Coordinator类介绍：
`Coordinator`是查询引擎的核心协调器，负责：
- 创建和管理`QueryExecution`对象
- 协调查询的各个阶段（语义分析、逻辑计划、分布式计划、调度执行）
- 管理查询的元数据获取（分区信息、schema信息）
- 提供查询执行的统一入口

这里使用的是`Coordinator.executeForTableModel()`函数，该函数接收Statement、SqlParser、客户端Session、查询ID、SessionInfo、SQL字符串、Metadata、超时时间等参数，然后调用`Coordinator.execution()`进行执行操作。下面介绍细节。

#### 2.2.1 查询准备工作

##### 2.2.1.1 QueryExecution的构建

构建流程（`Coordinator.executeForTableModel()` -> `Coordinator.execution()`）：

1. Lambda函数执行（`Coordinator.execution()`）：
   - 在`execution()`函数中，首先创建`MPPQueryContext`对象，设置查询ID、SessionInfo、SQL字符串等上下文信息
   - 然后使用`iQueryExecutionFactory.apply(queryContext, startTime)`操作创建`IQueryExecution`对象
   - 这实际上是一个lambda函数操作，实际的执行代码是`Coordinator.executeForTableModel()`中传递给`execution()`函数的lambda函数：`(queryContext, startTime) -> createQueryExecutionForTableModel(...)`

2. 创建QueryExecution（`Coordinator.createQueryExecutionForTableModel()`）：
   - 函数签名：`createQueryExecutionForTableModel(Statement statement, SqlParser sqlParser, IClientSession clientSession, MPPQueryContext queryContext, Metadata metadata, long timeOut, long startTime)`
   - 首先设置查询上下文：`queryContext.setTimeOut(timeOut)`和`queryContext.setStartTime(startTime)`
   - 检查Statement类型，如果是`IConfigStatement`（如DDL语句、ShowDB、CreateDB等），创建`ConfigExecution`；否则创建`QueryExecution`
   - 对于`QueryExecution`，创建`TableModelPlanner`对象，传入以下参数：
     - `statement.toRelationalStatement(queryContext)`：将Statement转换为关系型Statement
     - `sqlParser`：SQL解析器
     - `metadata`：元数据管理器
     - `scheduledExecutor`：调度执行器
     - `SYNC_INTERNAL_SERVICE_CLIENT_MANAGER`和`ASYNC_INTERNAL_SERVICE_CLIENT_MANAGER`：同步和异步内部服务客户端管理器
     - `statementRewrite`：语句改写器
     - `logicalPlanOptimizers`：逻辑计划优化器列表
     - `distributionPlanOptimizers`：分布式计划优化器列表
     - `AuthorityChecker.getAccessControl()`：权限检查器
     - `dataNodeLocationSupplier`：DataNode位置提供者
   - 然后使用`TableModelPlanner`和`queryContext`创建`QueryExecution`对象：`new QueryExecution(tableModelPlanner, queryContext, executor)`

3. 语义分析（`QueryExecution`构造函数 -> `QueryExecution.analyze()` -> `TableModelPlanner.analyze()` -> `Analyzer.analyze()`）：
   - 在`QueryExecution`的构造函数中，调用`this.analysis = analyze(context)`完成语义分析
   - `analyze()`函数调用`planner.analyze(context)`，对于表模型，会调用`TableModelPlanner.analyze()`
   - `TableModelPlanner.analyze()`中转到`Analyzer.analyze()`函数
   - `Analyzer.analyze()`函数中，首先调用`statementRewrite.rewrite(statement, context)`对查询语句进行改写，目前仅有的改写处理是针对`ShowStatement`（这是展示查询信息的SQL）
   - 随后创建`StatementAnalyzer`对象（`new StatementAnalyzer(...)`），并使用其`analyze()`函数进行处理
   - 语义分析的结果存储在`Analysis`对象中，返回给`QueryExecution`

##### 2.2.1.2 语义解析

主要类路径：
- `org.apache.iotdb.db.queryengine.plan.relational.analyzer.StatementAnalyzer`：语句分析器，负责Statement级别的语义分析
- `org.apache.iotdb.db.queryengine.plan.relational.analyzer.ExpressionAnalyzer`：表达式分析器，负责表达式级别的语义分析和类型推断
- `org.apache.iotdb.db.queryengine.plan.analyze.ExpressionTypeAnalyzer`：表达式类型分析器，负责检查表达式的类型兼容性
- `org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis`：语义分析结果对象，存储所有语义分析的结果
- `org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope`：作用域对象，表示一个查询作用域的上下文信息

StatementAnalyzer类介绍：
`StatementAnalyzer`是语义分析的核心类，负责：
- 将SQL的AST节点转换为数据库中的逻辑对象
- 解析表名、列名、函数名等标识符
- 验证SQL语法的语义正确性
- 构建作用域（Scope）和类型信息
- 识别和处理子查询

解析流程（`StatementAnalyzer.analyze(Node node)`）：

在`StatementAnalyzer.analyze()`函数中，会创建`StatementAnalyzer.Visitor`对象（继承于`AstVisitor`），然后调用`visitor.process(node, context)`函数。`process()`函数会根据node的数据类型调用相应的`visitXXX()`函数进行语义分析。例如Query的语义分析，就是调用`StatementAnalyzer.Visitor.visitQuery()`函数执行实际的处理逻辑。

`StatementAnalyzer`的主要作用是将SQL的string表达转化为数据库中的逻辑对象、函数对象的过程，使用`Analysis`记录全局的统计信息，使用`Scope`记录作用域内的信息：

- 将SQL中的`tableName`、`columnName`转化为数据库中的表、列对象
- 将聚合函数Name映射为实际函数的调用
- 提取join、limit等信息，并将列相关的结果封装进`Scope`返回
- 处理过程中的信息，记录在`Analysis`对象里面

语义解析过程中的关键检查和处理：

1. 表达式类型检查：
   - 使用ExpressionAnalyzer对表达式进行类型分析和验证
   - ExpressionTypeAnalyzer检查表达式的输入输出类型，确保类型匹配
   - 对于函数调用，检查函数参数的类型是否符合函数定义的要求
   - 对于比较操作符，检查左右操作数的类型是否兼容
   - 对于算术操作符，检查操作数类型是否支持该运算

2. 列引用解析：
   - 通过Scope.resolveField()函数在当前作用域和父作用域中查找列引用
   - 处理列名的歧义性，当存在多个匹配时抛出语义异常
   - 对于带表名前缀的列引用（如table.column），验证表名是否存在

3. 表名和数据库名解析：
   - 将SQL中的表名解析为QualifiedObjectName（包含数据库名和表名）
   - 验证表是否存在，检查用户是否有访问权限
   - 对于子查询，创建新的作用域来处理子查询中的表引用

4. 聚合函数处理：
   - 验证聚合函数名称是否有效
   - 检查聚合函数的参数数量和类型
   - 对于窗口函数，验证窗口规范的有效性
   - 检查聚合函数是否出现在允许的上下文中（如SELECT、HAVING子句）

5. 子查询识别和处理：
   - ExpressionAnalyzer识别表达式中的子查询（InPredicate、ScalarSubquery、ExistPredicate、QuantifiedComparison）
   - 为子查询创建独立的作用域进行分析
   - 检查子查询的返回类型是否与主查询兼容

6. JOIN条件分析：
   - 验证JOIN条件中的列引用是否有效
   - 检查JOIN类型和ON条件的兼容性
   - 对于USING子句，验证指定的列在左右表中都存在

7. GROUP BY和ORDER BY验证：
   - 检查GROUP BY中的表达式是否在SELECT列表中或可以引用
   - 验证ORDER BY中的表达式是否有效
   - 对于包含聚合的查询，检查GROUP BY和SELECT的兼容性

8. 类型强制转换：
   - 当表达式类型不匹配但可以转换时，记录类型强制转换信息
   - 这些信息在后续的逻辑计划构建阶段用于添加类型转换节点

所有语义分析的结果都记录在Analysis对象中，包括：
- expressionTypes：表达式到类型的映射
- columnReferences：列引用到ResolvedField的映射
- subqueryInPredicates：包含子查询的谓词集合
- windowFunctions：窗口函数集合
- predicateCoercions：需要类型转换的谓词信息

#### 2.2.2 逻辑计划的生成 doLogicalPlan

主要类路径：
- `org.apache.iotdb.db.queryengine.plan.execution.QueryExecution`：查询执行对象
- `org.apache.iotdb.db.queryengine.plan.relational.planner.TableModelPlanner`：表模型计划器
- `org.apache.iotdb.db.queryengine.plan.relational.planner.TableLogicalPlanner`：表模型逻辑计划器
- `org.apache.iotdb.db.queryengine.plan.relational.planner.RelationPlanner`：关系计划器，负责Relation到PlanNode的转换
- `org.apache.iotdb.db.queryengine.plan.relational.planner.QueryPlanner`：查询计划器，负责Query相关的逻辑计划构建

TableLogicalPlanner类介绍：
`TableLogicalPlanner`是表模型逻辑计划生成的核心类，负责：
- 将Statement转换为逻辑计划树（PlanNode树）
- 调用优化器对逻辑计划进行优化
- 管理逻辑计划生成的上下文信息（如SymbolAllocator）

逻辑计划节点类型：
逻辑计划节点定义在`org.apache.iotdb.db.queryengine.plan.relational.planner.node`包中，所有PlanNode都继承自`PlanNode`基类，实现了`PlanNode.accept(PlanVisitor visitor, C context)`方法用于访问者模式的处理。

##### 2.2.2.1 逻辑计划构建的准备工作

流程（`Coordinator.execution()` -> `QueryExecution.start()` -> `QueryExecution.doLogicalPlan()`）：

1. QueryExecution启动（`QueryExecution.start()`）：
   - 在`Coordinator.execution()`函数中，创建`QueryExecution`后，会将其放入查询执行的map中（`queryExecutionMap.put(queryId, queryExecution)`）
   - 然后调用`QueryExecution.start()`函数（主要的执行逻辑）
   - `start()`函数执行完成后，调用`getStatus()`检查执行状态，判断是否需要重试（`PENDING_RETRY`状态），不需要重试则直接返回结果

2. 跳过执行检查（`QueryExecution.start()` -> `skipExecute()`）：
   - 在`start()`函数中，首先会检查查询是否可以跳过（`skipExecute()`）
   - `skipExecute()`函数调用`analysis.canSkipExecute(context)`判断
   - 目前设置查询跳过的点在于以下几个：
     - 语义分析中的`Explain`、`ShowDevice`、`CountDevice`
     - 逻辑计划构建中的`FetchDevice`、`ShowDevice`、`CountDevice`、`Update`
     - 逻辑计划优化中`PushPredicateIntoTableScan`的个别情况
   - 如果可以跳过，直接构造内存结果（`constructResultForMemorySource()`）并转换状态到`RUNNING`

3. 逻辑计划生成（`QueryExecution.doLogicalPlan()` -> `TableModelPlanner.doLogicalPlan()` -> `TableLogicalPlanner.plan()`）：
   - 调用`QueryExecution.doLogicalPlan()`，内部调用`planner.doLogicalPlan(analysis, context)`
   - 对于表模型，会调用`TableModelPlanner.doLogicalPlan()`，内部创建`TableLogicalPlanner`并调用其`plan()`函数
   - `TableLogicalPlanner.plan()`函数进行实际的逻辑计划生成与优化工作，返回`LogicalQueryPlan`对象

4. 逻辑计划构建（`TableLogicalPlanner.planStatement()`）：
   - `plan()`函数内部调用`planStatement(analysis, statement)`函数
   - 这里会根据语义分析`Analysis`以及`Statement`树的信息，对于设备相关的SQL（`create/update/fetch/show/count device`和`update`）单独创建Plan树
   - 其余SQL则是需要先使用`TableLogicalPlanner.planStatementWithoutOutput()`函数生成`RelationPlan`树，再用`createOutputPlan()`为逻辑计划树的根节点补充上`OutputNode`

##### 2.2.2.2 逻辑计划构建过程

`planStatementWithoutOutput()`函数目前支持处理`Query`、`Explain`、`WrappedStatement`、`LoadTsFile`、`PipeEnriched`、`Delete`、`ExplainAnalyze`等SQL操作。这里以`Query`为例，调用`TableLogicalPlanner.createRelationPlan()`函数，使用`AstVisitor.process()`函数、`Node.accept()`函数作为中转，根据statement树上节点的实际对象类型，调用`RelationPlanner`中对应重载函数`visitQuery()`去处理。这里会构造`QueryPlanner`去plan对应节点，在plan的过程中会构建下一级relation的`RelationPlanner`，嵌套的去visit，会自底向上构建逻辑计划树，直到完成整个statement树向逻辑计划树的转化工作（例如`planQuery()` -> `planQuerySpecification()` -> `planFrom()` -> `visitTable()` -> `tableScanNode`）。也就是说最底层的操作，如表扫描（from子句中指定的表）或子查询的结果，首先被创建，然后逐步添加更高层次的操作，依次可以为join、where、select、groupby和聚合函数、orderby、sort、offset、limit等，这些都是逻辑计划节点。

目前支持的逻辑计划节点主要包括：
- TableScanNode系列：DeviceTableScanNode、TreeAlignedDeviceViewScanNode、TreeNonAlignedDeviceViewScanNode、InformationSchemaTableScanNode、AggregationTableScanNode等
- FilterNode：用于WHERE和HAVING子句的过滤操作
- ProjectNode：用于SELECT子句的列投影和表达式计算
- SortNode/StreamSortNode：用于ORDER BY子句的排序操作
- LimitNode/OffsetNode：用于LIMIT和OFFSET子句的限制操作
- TopKNode：Limit和Sort合并后的优化节点
- AggregationNode：用于GROUP BY和聚合函数的聚合操作
- JoinNode系列：InnerJoinNode、LeftJoinNode、RightJoinNode、FullJoinNode等，用于JOIN操作
- UnionNode：用于UNION操作
- ApplyNode：用于子查询的关联操作
- CorrelateJoinNode：用于标量子查询的关联连接
- EnforceSingleRowNode：用于确保子查询返回单行
- GapFillNode：用于GAP FILL子句的时间序列填充操作
- FillNode系列：PreviousFillNode、LinearFillNode、ValueFillNode等，用于FILL子句的数据填充操作
- OutputNode：用于最终结果的输出映射
- ExplainAnalyzeNode：用于EXPLAIN ANALYZE语句的执行计划分析

##### 2.2.2.2.1 逻辑计划节点构建介绍

主要类路径：
- `org.apache.iotdb.db.queryengine.plan.relational.planner.RelationPlanner`：关系计划器
- `org.apache.iotdb.db.queryengine.plan.relational.planner.QueryPlanner`：查询计划器
- `org.apache.iotdb.db.queryengine.plan.relational.planner.SubqueryPlanner`：子查询计划器

这里介绍一个查询中的主要逻辑节点构建过程，按照树结构进行组织，这里的处理与抽象语法树的处理顺序即G4的语法结构是一致的。主要涉及文件为`RelationPlanner`、`QueryPlanner`、`SubqueryPlanner`。

- 查询的逻辑计划构建Query（`QueryPlanner.planQuery()`）：
  - 提取query的body用于进行解析，如果body中仅有一个查询，则实际处理的是`QuerySpecification`（在`QueryPlanner`中），如果存在多个查询，则是需要先考虑多个查询结果的集和处理（目前对于`Intersect`、`Union`、`Except`三个集合操作还不支持，所以仅介绍单个查询的情况）
    - 处理from（`QueryPlanner.planFrom()`）：这里需要处理的是一个表结构，在解析的时候使用`Relation`类来专门表示，在逻辑计划构建的时候，使用`RelationPlanner`处理。主要有以下几种情况：
      - Table（`RelationPlanner.visitTable()`）：
        - 先检查这个table是否是已经处理过的子查询，如果是，就直接返回之前的处理结果。省时省力
        - 信息获取与处理：
          - 从语义信息中获取table的作用域`scope`和全称表名`qualifiedObjectName`，进而从作用域中获取字段信息`fields`
          - 将字段信息分别处理为`symbol`与`columnSchema`，构建`symbol`到`columnSchema`的映射关系，对于有tag的field，还会记录从`symbol`到`tagId`的映射
          - 将表名与列元数据一起记录为表的元数据信息
        - 构建扫描节点：
          - 如果表元数据类型是`treeDeviceViewSchema`（这是表模型查看树设备元数据的一种兼容方式，用于在表模型中访问树模型的设备数据），就构建`TreeDeviceViewScanNode`扫描节点
          - 如果表所在的数据库是`INFORMATION_DATABASE`，就构建`InformationSchemaTableScanNode`
          - 基于表的全名、输出信息、列信息以及属性信息，构建`DeviceTableScanNode`
      - Join（`RelationPlanner.visitJoin()`）：这里是分别解析Join的左右两个表结构
        - 如果Join的条件中使用了`JoinUsing`，随后使用`RelationPlanner.planJoinUsing()`进行逻辑计划的构建：
          - 这里为using指定的列，在左右的表结构上都构建对应的`ProjectNode`，将双方的指定列都统一数据类型
          - 随后用带有指定列的相等Clause的`JoinNode`连接两个relation
          - 最后再补充一个`ProjectNode`，去除冗余的列信息
          
          逻辑计划结构示例：
          ```
 l JOIN r USING (k1, ..., kn)
- project
        coalesce(l.k1, r.k1)
        ...,
        coalesce(l.kn, r.kn)
        l.v1,
        ...,
        l.vn,
        r.v1,
        ...,
        r.vn
  - join (l.k1 = r.k1 and ... l.kn = r.kn)
        - project
            cast(l.k1 as commonType(l.k1, r.k1))
            ...
        - project
                    cast(r.k1 as commonType(l.k1, r.k1))
                    ...
          ```
          
          说明：实际构建`ProjectNode`的主要工作就是记录输入集合、输出集合以及之间的映射关系
          
        - 其他情况直接使用`RelationPlanner.planJoin()`来构建逻辑计划：
          - on表达式处理：这里将on条件划分为左右两边的表达式处理，随后将其划分为简单表达式和复杂表达式
            - 简单表达式：指的是一种情况，即单边的表达式所含有的符号可以被单边的表结构所处理
            - 复杂表达式，有两种情况：
              - 情况1：两边的表达式含有的列，可以被某一边的表结构处理，这个加入到复杂表达式是考虑后续的下推优化
              - 情况2：剩下的其他情况都是复杂表达式
          - 处理简单表达式：先将两边的表结构都添加`ProjectNode`，统一用于连接的列的类型
            - 等值比较：构建为等值连接条件，用于后续直接构造`JoinNode`
            - 非等值比较：收集非等值连接条件，在其上构建`FilterNode`
          - 处理复杂表达式：
            - 非inner：将复杂表达式中的单边表达式进行子查询检查（目前还处于注释代码），随后更新到`JoinNode`信息中
            - inner：将先检查复杂表达式是否为子查询（目前处于注释代码），遍历复杂表达式并为其构造`ProjectNode`进行强制类型转化，最后使用复杂表达式以及非等值比较表达式构建`FilterNode`
      - TableSubQuery（`RelationPlanner.visitTableSubQuery()`）：将其中的查询进行逻辑计划的构建，返回构建出来的逻辑计划树。这里的查询逻辑计划树等效于上面的`TableScanNode`，就是作为数据的起点
      - AliasRelation（`RelationPlanner.visitAliasRelation()`）：处理表的别名，主要是将别名信息传递到后续的处理中，不影响逻辑计划的结构
    - 处理where（`QueryPlanner.planWhere()`）：这里直接调用`filter()`函数（对子查询做了检查）
      - `SubqueryPlanner.handleSubqueries()`检查处理where中的子查询情况
      - 为根节点添加一个`FilterNode`，其中附上筛选条件
    - 处理Aggregate（`QueryPlanner.planAggregation()`）：aggregate/groupby（结合groupby的信息处理所有clause的聚合函数，有子查询处理）
      - 首先收集聚合函数的输入参数（顺序和过滤条件的收集目前还处于注释状态）、分组条件（groupby），构造输入表达式列表，检查表达式是否有子查询后构建`ProjectNode`，完成类型转换，保持类型与输入要求一致
      - 处理groupset表达式：
        - 为group set中涉及的field以及复杂表达式生成`symbol`，构建field/表达式到symbol的映射
        - 构建简单表达式Sets的所有分组可能，随后进行去重，转化为`list<list>`，依据上一步的映射将其处理为分组Symbol集。随后为每个list添加上复杂表达式，完成与复杂表达式的笛卡尔积。最后检查分组数量，目前仅支持单一的分组情况，构建`ProjectNode`（注释中提及，如果分组情况大于1，则会构建`GroupIdNode`）
          - `CUBE`是算幂集2^n，用的是集合类的`powerset()`函数完成
          - `ROLLUP`是按顺序每次减少右侧的一列，这里则是根据范围逐渐缩小来完成
          - 普通的field单独作为一个集合
      - 将所有的聚合函数及其相关的信息处理成为`aggregationAssignment`（输出，函数，聚合信息），将前面的groupset的信息也加入后，将其打包成为`AggregationNode`（step为`SINGLE`）
    - 处理having（`QueryPlanner.planHaving()`）：这里也是直接调用`filter()`函数（对子查询做了检查），为根节点添加一个`FilterNode`
    - 处理Gap fill（`QueryPlanner.planGapFill()` -> `QueryPlanner.gapFill()`）：
      - Gap fill是IoTDB中用于时间序列数据填充的语法，用于在时间序列的缺失值处填充数据
      - 在`QueryPlanner.plan()`函数中，如果检测到`gapFillColumn`不为null，会调用`gapFill()`函数进行处理
      - `gapFill()`函数的处理流程：
        1. 将`gapFillColumn`表达式转换为`Symbol`（`subPlan.translate(gapFillColumn)`）
        2. 处理分组键：调用`fillGroup()`函数处理`gapFillGroupingKeys`，为分组键生成`Symbol`列表
        3. 提取时间参数：从`gapFillColumn`的children中提取`monthDuration`（月间隔）、`nonMonthDuration`（非月间隔）、`origin`（起始时间点）
        4. 提取时间范围：调用`getStartTimeAndEndTimeOfGapFill()`函数，使用`GapFillStartAndEndTimeExtractVisitor`访问者从`wherePredicate`中提取时间范围（startTime和endTime）
        5. 构建`GapFillNode`：使用提取的参数创建`GapFillNode`，包含时间范围、间隔参数、分组键等信息
      - `getStartTimeAndEndTimeOfGapFill()`函数会使用访问者模式遍历`wherePredicate`表达式树，提取时间范围信息，如果无法推断时间范围，会抛出`SemanticException(CAN_NOT_INFER_TIME_RANGE)`异常
      
    - 处理fill（`QueryPlanner.planFill()` -> `QueryPlanner.fill()`）：
      - Fill是IoTDB中用于时间序列数据填充的语法，包括previous fill、linear fill、value fill等
      - `fill()`函数根据`Fill`对象的`fillMethod`类型进行不同的处理：
        - PREVIOUS（前值填充）：
          1. 获取`PreviousFillAnalysis`分析结果
          2. 如果存在`fieldReference`（辅助列），将其转换为`Symbol`作为`previousFillHelperColumn`
          3. 如果存在`groupingKeys`（分组键），调用`fillGroup()`处理分组键
          4. 构建`PreviousFillNode`，包含时间边界（`timeBound`）、辅助列、分组键等信息
        - LINEAR（线性填充）：
          1. 获取`LinearFillAnalysis`分析结果
          2. 将`fieldReference`转换为`Symbol`作为`helperColumn`（辅助列，用于线性插值）
          3. 如果存在`groupingKeys`，调用`fillGroup()`处理分组键
          4. 构建`LinearFillNode`，包含辅助列、分组键等信息
        - CONSTANT（常量填充）：
          1. 获取`ValueFillAnalysis`分析结果
          2. 提取填充值（`filledValue`）
          3. 构建`ValueFillNode`，包含填充值信息
      - `fillGroup()`函数用于处理分组键，为分组键表达式生成`Symbol`，并在`subPlan`中添加必要的`ProjectNode`来支持分组操作
    - select order distinct处理（`QueryPlanner.planSelect()`）：
      - 这里将select的表达式抽取出来，检查是否是子查询，检查是否需要展开
      - 对于orderby aggregate的表达式构建`ProjectNode`
      - 为select的output表达式构建`ProjectNode`，这里是为后续的orderby做准备
      - 针对orderby的非聚合表达式，检查是否有子查询，随后为这些表达式构建`ProjectNode`
      - 检查是否有distinct，如果有，构建一个single的`AggregationNode`。这里是将distinct逻辑，使用singleAggregate来完成，可以理解为distinct是一种不需要计算结果的聚合分组操作，分组键就是distinct指定的键，这样指定的键下值相同的会合并为一组，但是没有额外的聚合值计算，从而实现了去重的逻辑。所以这里添加的是`AggregationNode`（step是`SINGLE`）
      - 为orderby的字段和顺序组合为`OrderingScheme`，用于构建`SortNode`
    - 处理offset、limit、构造结果映射（`QueryPlanner.planOffset()`、`QueryPlanner.planLimit()`）：这两个步骤和最外侧的query一致，不再赘述
  - 处理fill信息（`QueryPlanner.planFill()` -> `QueryPlanner.fill()`）：
    - 在`QueryPlanner.plan()`函数中，如果`QuerySpecification`的`fill`字段存在（`node.getFill().isPresent()`），会调用`fill()`函数进行处理
    - 处理流程：
      1. 首先为SELECT的输出表达式添加`ProjectNode`，确保FROM子句和SELECT子句的字段都可见
      2. 构建新的作用域，包含FROM子句和SELECT子句的字段
      3. 调用`fill()`函数构建相应的Fill节点（`PreviousFillNode`、`LinearFillNode`或`ValueFillNode`）
    - 具体处理逻辑与上面"处理fill"部分相同，`fill()`函数会根据`Fill`对象的`fillMethod`类型（PREVIOUS、LINEAR、CONSTANT）构建相应的Fill节点（`PreviousFillNode`、`LinearFillNode`或`ValueFillNode`），详细流程参看上面"处理fill"部分的说明
  - 处理orderby信息（`QueryPlanner.planOrderBy()`）：
    - 这里会从解析好的`QuerySpecification`中提取输出列的列表，存在排序的情况下，需要先在输出列中补充排序列，这里需要添加`ProjectNode`
    - 基于排序键信息，调用`QueryPlanner.sort()`函数，在逻辑计划树根上，补充新的`SortNode`节点
  - 处理limitoffset信息（`QueryPlanner.planLimitOffset()`）：
    - 依次调用`QueryPlanner.offset()`和`limit()`函数，在逻辑计划根节点上，依次补充上新的`OffsetNode`节点和`LimitNode`节点
  - 处理结果返回映射（`TableLogicalPlanner.createOutputPlan()`）：
    - 这里为树的根节点补充新的映射，仅保留实际需要输出的列信息，去除冗余的列

- 表达式中的子查询逻辑计划构建（`SubqueryPlanner`）：在前面语义分析的`ExpressionAnalyzer`中，就已经将子查询区分出来，目前存在的子查询主要有以下四种：
  - InPredicate（`SubqueryPlanner.planInPredicate()`）：格式为`value in predicateSubquery`
    - 创建返回的bool symbol，随后检查value中是否有子查询
    - 随后处理predicate子查询，将其中的子查询进行逻辑计划处理，随后加上`ProjectNode`用于指示返回的field，根据是否需要类型转化添加`ProjectNode`
    - 随后处理predicate的value，如果类型与目标类型不一致则添加上`ProjectNode`，根据是否需要类型转化添加`ProjectNode`
    - 最后用`ApplyNode`进行封装，`ApplyNode`主要用于将主查询和子查询的计划连接起来，使得它们在执行时能够协同工作
  - ScalarSubquery（`SubqueryPlanner.planScalarSubquery()`）：返回单行单列
    - 上来就处理scalarSubquery，将其中的子查询进行逻辑计划处理，随后加上`ProjectNode`指示返回的field，再根据类型转化添加`ProjectNode`
    - 在上述子查询上，再加上`EnforceSingleRowNode`，将结果处理为一行
    - 如果原来的结果是单行多列，则需要添加`ProjectNode`将row的数据处理为目标数据类型的数据
    - 由于结果仅有一行，与主查询连接的方式是inner join，这里构建`CorrelateJoinNode`进行封装
  - ExistPredicate（`SubqueryPlanner.planExistPredicate()`）：存在性判断过滤操作
    - 直接处理子查询，将最后返回结果封装为bool symbol，构建`ApplyNode`进行封装
  - QuantifiedComparison（`SubqueryPlanner.planQuantifiedComparison()`）：比较过滤`value operator quantifier subquery`
    - 首先检查value的表达式是否有子查询，设置返回结果为bool symbol
    - 根据operator和quantifier进行划分：
      - 对于equal情况，All直接处理，Any/Some情况使用等价情况即`A = ANY B`等价于`A in B`，所以使用`InPredicate`的情况进行构建
      - 对于notEqual情况，All使用等价情况，即`A != ALL B`等价于`!(A in B)`，所以使用`InPredicate`的情况进行构建；Any/Some情况使用等价情况即`A != ANY B`等价于`!(A = ALL B)`，所以后续使用All的`QuantifiedComparison`进行处理
      - 对于大于、大于等于、小于、小于等于情况，直接处理
    - 对于计划的处理，具体流程和`InPredicate`基本一致：
      - 直接处理子查询，随后加上`ProjectNode`指示返回的field，再根据类型转化添加`ProjectNode`
      - 随后处理value，这里添加上`ProjectNode`，根据是否需要类型转化添加`ProjectNode`
      - 最后也是用`ApplyNode`进行封装


#### 2.2.2.2.2 关键类介绍

##### Scope类

类路径：`org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope`

与一个`RelationPlan`对应，表示一个表结构查询的上下文信息。

主要参数：
- `Optional<Scope> parent`：当前关系的父关系的作用域
- `Boolean queryBoundary`：标记当前的作用域是否是查询的边界（最外一层作用域）
- `RelationId relationId`：relation的唯一标识信息
- `RelationType relationType`：记录relation的所有字段信息（别名，字段索引，可见性等）
- `Map<String, WithQuery> nameQueries`：命名子查询的名字到实际查询对象的映射

主要方法：
- `findLocally(Predicate<Scope> match)`：从当前作用域开始向上遍历，寻找匹配条件的scope，是这个类中最为常用的函数
- `resolveAsteriskedIdentifierChainBasis(QualifiedName identifierChain, AllColumns selectItem)`：用于解析带*的标识链（a.b.*），根据标识链的信息使用`findLocally`找到其所属的scope，并将其封装为`AsteriskedIdentifierChainBasis`对象返回
- `resolveField(Expression node, QualifiedName name, boolean local)`：在当前的作用域下，找到name对应的Field。主要是四个判断：当匹配数量为2个及以上时抛出异常；匹配数量为1个时返回该Field；没有匹配但是是列引用时抛出异常；前面都不是时，就到父作用域中找。参数`local`在当前作用域是边界时，其parent的scope在使用该函数时用false。说明：即使当前作用域是查询边界（queryBoundary=true），仍然可能有parent scope，这是因为在嵌套查询中，外层查询的作用域需要作为内层查询的parent scope，以便内层查询可以引用外层的列（相关子查询）。

##### TranslationMap类

类路径：`org.apache.iotdb.db.queryengine.plan.relational.planner.TranslationMap`

与一个`relationPlan`对应，用于记录字段/表达式到符号（Symbol）的映射关系。

主要参数：
- `Scope scope`：relationPlan的作用域
- `Analysis analysis`：整个SQL的语义分析结果
- `Optional<TranslationMap> outContext`：外部上下文的translationMap，用于处理嵌套查询/子查询
- `PlannerContext plannerContext`：查询计划的上下文，记录元数据信息（整个数据库的元数据信息）以及数据类型管理器
- `Symbol[] fieldSymbols`：当前作用域到符号的映射数组
- `Map<ScopeAware<Expression>, Symbol> astToSymbol`：AST表达式中的子表达式映射到符号
- `Map<NodeRef<Expression>, Symbol> substitutions`：IoTDB中该参数的赋值函数没有被调用，在Trino中用于模式识别过程中，用于记录表达式的替换映射，目前IoTDB中未使用

主要方法：
- `verifyAstExpression(Expression astExpression)`：要求表达式为非`SymbolReference`的表达式
- `canTranslate(Expression expression)`：表达式在translationMap中，或者是一个合法的表达式引用，但还没添加到translationMap
- `rewrite(Expression expression)`：改写主要逻辑是（这里需要关注`SymbolReference`的作用）：
  - 如果表达式存在映射到`SymbolReference`，返回该引用
  - 处理当前表达式的子表达式/参数
  - 如果当前表达式是最底层的`FieldReference`/`identifier`，直接返回其`SymbolReference`
  - 返回基于`SymbolReference`新构建的表达式

##### PlanBuilder类

类路径：`org.apache.iotdb.db.queryengine.plan.relational.planner.PlanBuilder`

与一个`relationPlan`对应，用于构建一个`relationPlan`中的逻辑计划树，并使用`translationMap`改写表达式。

主要参数：
- `TranslationMap translationMap`：记录列/表达式到对应symbol的映射
- `PlanNode root`：这里记录了逻辑计划树的根节点，是自下而上构建的，每次更新根节点即可

主要方法：
- `appendProjections(Iterable<T> expressions, SymbolAllocator symbolAllocator, MPPQueryContext queryContext)`：添加映射的作用，就是对没有被改写过的表达式进行改写，`ProjectNode`会记录symbol到改写后的表达式。而`translationMap`则会更新表达式到symbol的映射。`ProjectNode`成为新的根节点

##### Symbol和SymbolReference的区别

类路径：
- `org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol`：符号类
- `org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference`：符号引用类

- Symbol类：是一个逻辑概念，封装了列或者表达式的名称，用于标识查询操作符的输入输出信息。Symbol是计划阶段的逻辑表示，用于标识计划节点之间的数据流。
- SymbolReference类：是Symbol在执行阶段的表示形式。Symbol在执行处理时，可以转化为`SymbolReference`，可以参与到表达式的处理当中。`SymbolReference`是表达式树中的节点，用于引用一个Symbol。在逻辑计划构建时，表达式会被改写为使用`SymbolReference`的形式，这样可以在执行时通过`SymbolReference`找到对应的Symbol，进而找到对应的数据列。

##### Field、FieldReference、ResolvedField的区别

类路径：
- `org.apache.iotdb.db.queryengine.plan.relational.analyzer.Field`：字段类
- `org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FieldReference`：字段引用类
- `org.apache.iotdb.db.queryengine.plan.relational.analyzer.ResolvedField`：已解析字段类

- Field类：表示一个字段的逻辑概念，包含字段的基本信息（名称、类型等）
- FieldReference类：是AST中的节点，用于在SQL语句中引用一个字段（如`column1`、`table.column`等）。在语义分析阶段，`FieldReference`会被解析为`ResolvedField`
- ResolvedField类：是封装了作用域的Field，包含字段的完整信息以及它所属的作用域。`ResolvedField`是语义分析的结果，用于后续的逻辑计划构建

#### 2.2.2.3 逻辑计划优化

主要类路径：
- `org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.LogicalOptimizeFactory`：逻辑计划优化构建器
- `org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.IterativeOptimizer`：基于规则的迭代优化器
- `org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule`：优化规则包
- `org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations`：基于PlanOptimizer的优化器包

这里使用的是`Coordinator`创建时就准备好的，由逻辑计划优化构建器`LogicalOptimizeFactory`产出的一系列逻辑计划优化。目前IoTDB所使用的主要是RBO（基于规则的优化），而RBO有两种实现方式：

1. 基于迭代器逻辑（继承于Rule）：针对匹配规则的特定逻辑计划子树进行处理（新增一个规则就新增一个类，新增一种节点的修改较为繁琐）
   - 规则类位于：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/planner/iterative/rule`

2. 基于重载逻辑（继承于PlanOptimizer）：针对节点的特征进行优化处理（一个类中含有所有节点的处理函数，新增一个节点仅需添加对应节点的处理函数）
   - 优化器类位于：`iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/plan/relational/planner/optimizations`

优化处理逻辑：

1. 优化器执行流程（`TableLogicalPlanner.plan()`）：
   - 在`TableLogicalPlanner.plan()`函数中，逻辑计划生成完成后，会遍历`planOptimizers`列表
   - 对每个优化器调用`optimize()`方法，传入当前计划节点、Analysis、上下文等信息
   - 优化器返回优化后的计划节点，替换原节点
   - 优化过程会记录优化耗时（`queryContext.setLogicalOptimizationCost()`），用于性能监控

2. 基于Rule的迭代优化器（`IterativeOptimizer.optimize()`）：
   - 使用模式匹配（`Pattern`）来识别可以应用规则的计划子树
   - 通过`RuleIndex`索引规则，快速找到可能匹配的规则
   - 对每个匹配的规则，调用`transform()`方法尝试应用规则
   - 如果规则成功应用，使用`Memo.replace()`替换计划节点
   - 迭代执行直到没有规则可以应用（`done`标志为true）
   - 记录规则调用统计信息（`context.recordRuleInvocation()`），包括调用次数、应用次数、耗时等

3. 列裁剪优化（Column Pruning）（`LogicalOptimizeFactory`中的列裁剪规则集合）：
   - 通过`PruneXXXColumns`系列规则，移除不需要的列
   - 例如`PruneProjectColumns`移除`ProjectNode`中未使用的输出列
   - `PruneFilterColumns`移除`FilterNode`中未引用的列
   - `PruneTableScanColumns`移除`TableScanNode`中未使用的列
   - 这些规则可以显著减少数据传输量和内存占用

4. 投影下推优化（Projection Pushdown）（`PushProjectionThroughUnion`规则）：
   - `PushProjectionThroughUnion`规则将`ProjectNode`下推到`UnionNode`的子节点
   - 减少上层`ProjectNode`需要处理的数据量

5. 谓词下推优化（Predicate Pushdown）（`PushPredicateIntoTableScan`规则）：
   - 将`FilterNode`尽可能下推到靠近数据源的节点
   - 减少后续节点需要处理的数据量
   - 对于`TableScanNode`，可以将过滤条件下推到扫描操作中

6. 聚合下推优化（Aggregation Pushdown）（`PushAggregationIntoTableScan`规则）：
   - 将聚合操作下推到数据源附近
   - 在分布式场景下，可以先进行部分聚合，再进行最终聚合

7. Limit和Sort合并优化（`MergeLimitWithSort`规则）：
   - `MergeLimitWithSort`规则将`LimitNode`和`SortNode`合并为`TopKNode`
   - 减少排序操作需要处理的数据量

8. 优化器的执行顺序（`LogicalOptimizeFactory`）：
   - 优化器按照`LogicalOptimizeFactory`中定义的顺序执行
   - 通常先执行列裁剪，然后执行投影下推，最后执行其他优化
   - 某些优化可能需要多次迭代才能达到最优效果

优化过程会记录详细的统计信息，包括：
- 每个规则的调用次数和应用次数
- 每个规则的执行耗时
- 优化前后的计划节点ID变化
- 优化是否产生了实际效果

#### 2.2.3 分布式计划的生成 doDistributePlan

主要类路径：
- `org.apache.iotdb.db.queryengine.plan.execution.QueryExecution`：查询执行对象
- `org.apache.iotdb.db.queryengine.plan.relational.planner.TableModelPlanner`：表模型计划器
- `org.apache.iotdb.db.queryengine.plan.relational.planner.TableDistributedPlanner`：表模型分布式计划器
- `org.apache.iotdb.db.queryengine.plan.relational.planner.TableDistributedPlanGenerator`：表模型分布式计划生成器

逻辑计划生成后，`QueryExecution.start()`函数随后进行分布式计划的生成`doDistributePlan()`。这里使用`QueryExecution.doDistributePlan()`和`TableModelPlanner.doDistributePlan()`进行中转，创建新的`TableDistributedPlanner`对象用于返回。创建的同时调用了`TableDistributedPlanner.plan()`函数，执行具体的分布式计划生成与优化。

##### 2.2.3.1 分布式计划生成

在`TableDistributedPlanner.plan()`函数中，直接调用`generateDistributedPlanWithOptimize()`来完成分布式计划生成、优化以及exchangenode的添加工作。

具体来说，这里先用`TableDistributedPlanGenerator.genResult()`函数，这里会调用逻辑计划树根节点的`accept()`操作，最终会调用`TableDistributedPlanGenerator`中对应的`visitXXX()`函数进行处理。可以看到，对于中间节点的visit，首先都会将其孩子节点都使用`accept()`进行处理，实现一个迭代处理的效果。在处理节点时，它会根据节点的特性进行不同的处理。

这里介绍一些查询相关的分布式计划生成工作（`TableDistributedPlanGenerator`中的visit函数）：

- visitOutput/visitLimit/visitOffset/visitEnforceSingleRow（`TableDistributedPlanGenerator.visitOutput()`等）：
  - 检查子节点是否有排序属性，如果有，排序传递到当前节点，随后使用`MergeSortNode`来合并子节点，否则，就会使用`CollectNode`来合并子节点
  - 如果仅有一个子节点，则两者都不需要添加

- visitFill（`TableDistributedPlanGenerator.visitFill()`）：
  - Fill节点的分布式计划处理
  - 处理逻辑：
    1. 检查Fill节点类型：如果不是`ValueFillNode`（即`PreviousFillNode`或`LinearFillNode`），清除上下文中期望的排序方案（`context.clearExpectedOrderingScheme()`），因为前值填充和线性填充会改变数据的顺序性
    2. 调用`dealWithPlainSingleChildNode()`函数处理：这是一个通用函数，用于处理只有一个子节点的节点
    3. `dealWithPlainSingleChildNode()`会递归处理子节点的分布式计划，然后根据子节点的数量和排序属性，决定是否需要添加`MergeSortNode`或`CollectNode`来合并子节点的结果
  - 说明：`ValueFillNode`（常量填充）不会改变数据顺序，所以保留排序属性；而`PreviousFillNode`和`LinearFillNode`需要按时间顺序处理数据，会改变原有的排序属性

- visitGapFill（`TableDistributedPlanGenerator.visitGapFill()`）：
  - GapFill节点的分布式计划处理
  - 处理逻辑：
    1. 清除上下文中期望的排序方案（`context.clearExpectedOrderingScheme()`），因为GapFill会在时间序列的缺失值处填充数据，会改变数据的顺序性
    2. 调用`dealWithPlainSingleChildNode()`函数处理子节点
    3. GapFill节点通常只有一个子节点，所以会递归处理子节点的分布式计划，然后根据子节点的数量和排序属性决定是否需要添加合并节点
  - 说明：GapFill操作需要在指定的时间范围内按间隔填充数据，会生成新的时间点，因此需要清除排序属性，避免与后续的排序操作冲突

- visitProject/visitFilter（`TableDistributedPlanGenerator.visitProject()` / `visitFilter()`）：
  - 检查子节点是否有排序属性，如果有，排序传递到当前节点
  - 如果仅有一个子节点，直接构建`CollectNode`
  - 检查当前节点是否可以复制到各个子节点上，这里判断`ProjectNode`的输出列包含所有排序键（`FilterNode`不需要判断这个），同时子节点不包含`diff`表达式
    - 说明：`diff`函数是IoTDB中的一个时间序列函数，用于计算相邻时间点的差值。如果子节点包含`diff`表达式，说明需要保持数据的顺序性，不能随意复制节点
    - 如果不可以复制到各个子节点，那么就根据是否有排序构建`MergeSortNode`，还是`CollectNode`
    - 如果可以复制，那么将当前的`ProjectNode`/`FilterNode`复制到各个`childrenNode`上，最后结果返回的是`SubProjectNode`/`SubFilterNode`的list

- visitTopK（`TableDistributedPlanGenerator.visitTopK()`）：优化规则中将limit和sort融合
  - 将当前节点的排序信息记录到上下文中，确保正确传递
  - 由于逻辑计划中topK仅仅有一个子节点，所以对子节点进行分布式计划处理
  - 如果处理后的子节点还是单个子节点，就保留父子节点联系
  - 如果处理后子节点有多个，则是为每个子节点先添加上`SubTopK`，减少传输到topK的数据量

- visitSort/visitStreamSort（`TableDistributedPlanGenerator.visitSort()` / `visitStreamSort()`）：优化规则中实现的，将sort转变为streamsort
  - 将当前节点的排序信息记录到上下文中，确保正确传递
  - 遍历子节点进行分布式计划的处理
  - 如果仅有一个子节点，评估是否可以消除当前的排序操作，如果可以，则直接返回子节点，如果不可以，那么返回带有子节点的`SortNode`
  - 如果有多个子节点，则是构建`MergeSortNode`，下面为每个子节点添加单独的`SortNode`。这里再次进行评估，为每个子节点判断是否需要添加单独的`SortNode`，如果不需要，则直接连接上`MergeSortNode`
  - 这里的评估方法主要是：检查sort节点和子节点是否有一致的排序符号，以及一致的排序顺序

- visitJoin（`TableDistributedPlanGenerator.visitJoin()`）：
  - 先分别处理左右两边子节点的分布式计划构建
  - 对于非cross的join操作，左右两边分布式构建后的子节点均只能为一个（只能为`SortNode`或者`MergeSortNode`）
  - 对于cross的join，为左右两边的子节点集合添加上收集节点（如果有序添加`MergeSortNode`，无序则添加`CollectNode`）

- visitDeviceTableScan（`TableDistributedPlanGenerator.visitDeviceTableScan()`）：
  - 创建映射表，记录分区到对应`DeviceTableScanNode`的映射关系
  - 遍历设备条目，获取其所在的多个分区信息，为每个分区都构建一个`DeviceTableScanNode`节点，并将设备条目添加到扫描节点中
  - 如果映射表为空，说明当前扫描节点所包含的设备没有进行分区，返回节点
  - 如果映射表非空，遍历所有分区扫描节点，记录每个分区的设备条目数量，确定设备条目数量最多的分区，将最常用的分区记录到上下文中
  - 判断是否有排序属性，没有则直接返回`DeviceTableScanNode`的列表；如果有排序属性，调用`processSortProperty()`函数进行处理
  - processSortProperty()函数处理逻辑（`TableDistributedPlanGenerator.processSortProperty()`）：
    1. 提取可下推的排序键：
       - 遍历`expectedOrderingScheme`中的排序键（`getOrderBy()`）
       - 对于每个排序键，检查是否是时间相关列（`timeRelatedSymbol()`）或tag/attribute列
       - 如果是时间相关列：
         - 检查排序方向，如果是降序（`!isAscending()`），将所有`DeviceTableScanNode`的扫描顺序设置为`Ordering.DESC`
         - 将时间列添加到可下推的排序键列表（`newOrderingSymbols`）
         - 标记`lastIsTimeRelated = true`并跳出循环（时间列通常是第一个排序键）
       - 如果是tag/attribute列（在`tagAndAttributeIndexMap`中存在），添加到可下推的排序键列表
       - 如果既不是时间列也不是tag/attribute列，停止处理（后续的排序键无法下推）
    2. 构建排序规则：
       - 如果可下推的排序键为空，直接返回（无法下推排序属性）
       - 创建`TreeDeviceIdColumnValueExtractor`提取器（如果节点是树设备视图）
       - 为每个可下推的排序键构建排序规则函数（`Function<DeviceEntry, String>`）：
         - 对于TAG列：使用提取器从设备ID中提取对应段的值，或使用`getNthSegment()`获取
         - 对于ATTRIBUTE列：从`deviceEntry.getAttributeColumnValues()`中获取对应索引的值
    3. 构建比较器：
       - 根据排序顺序（升序/降序）和NULL值处理（`nullsFirst`/`nullsLast`）构建`Comparator<DeviceEntry>`
       - 如果存在多个排序键，使用`thenComparing()`链式组合多个比较器
    4. 应用排序：
       - 为每个`DeviceTableScanNode`的设备条目列表（`deviceEntries`）应用排序
       - 使用构建的比较器对设备条目进行排序，确保扫描时按照排序键的顺序处理设备
    5. 设置排序属性：
       - 为每个`DeviceTableScanNode`设置新的排序方案（`OrderingScheme`），包含可下推的排序键和排序顺序
       - 这样在扫描时就可以按照排序键的顺序读取数据，减少后续排序操作的开销
  - 说明：`processSortProperty()`函数的作用是将排序操作下推到`DeviceTableScanNode`，通过在扫描阶段就按照排序键的顺序读取数据，可以减少后续排序操作的数据量，提高查询性能

- visitTreeDeviceViewScan（`TableDistributedPlanGenerator.visitTreeDeviceViewScan()`）：
  - TreeDeviceViewScan节点的分布式计划处理，用于在表模型中访问树模型的设备数据
  - 处理流程：
    1. 检查分区信息：
       - 获取`DataPartition`分区信息（`analysis.getDataPartitionInfo()`）
       - 如果分区信息为空或`treeDBName`为空，设置节点为`NOT_ASSIGNED`，清空设备条目列表，返回单个节点
    2. 获取分区映射：
       - 从分区信息中获取`treeDBName`对应的系列分区映射（`seriesSlotMap`）
       - 如果数据库不存在，抛出`SemanticException`
    3. 遍历设备条目并分配分区：
       - 创建映射表`tableScanNodeMap`，记录`TRegionReplicaSet`到`Pair<TreeAlignedDeviceViewScanNode, TreeNonAlignedDeviceViewScanNode>`的映射
       - 创建缓存`cachedSeriesSlotWithRegions`，缓存系列分区槽到区域副本集的映射
       - 遍历`node.getDeviceEntries()`中的每个设备条目：
         - 调用`getDeviceReplicaSets()`获取设备所在的分区副本集列表
         - 如果设备跨多个分区（`regionReplicaSets.size() > 1`），设置`context.deviceCrossRegion = true`
         - 对于每个分区副本集：
           - 判断设备是否对齐（`deviceEntry instanceof AlignedDeviceEntry`）
           - 从`tableScanNodeMap`获取或创建对应的扫描节点对（Pair）
           - 如果是对齐设备且左节点（`TreeAlignedDeviceViewScanNode`）为空，创建新的`TreeAlignedDeviceViewScanNode`，设置分区副本集，并添加到Pair的left
           - 如果是非对齐设备且右节点（`TreeNonAlignedDeviceViewScanNode`）为空，创建新的`TreeNonAlignedDeviceViewScanNode`，设置分区副本集，并添加到Pair的right
           - 将设备条目添加到对应的扫描节点（`appendDeviceEntry(deviceEntry)`）
    4. 处理空映射情况：
       - 如果`tableScanNodeMap`为空，设置节点为`NOT_ASSIGNED`，清空设备条目列表，返回单个节点
    5. 确定最常用分区：
       - 遍历所有扫描节点，统计每个分区的设备条目数量
       - 确定设备条目数量最多的分区（`mostUsedDataRegion`），记录到上下文中
    6. 处理排序属性：
       - 如果上下文中没有排序属性（`!context.hasSortProperty`），直接返回扫描节点列表
       - 否则调用`processSortProperty()`函数处理排序属性（处理逻辑与`visitDeviceTableScan`中的`processSortProperty()`相同）
    7. 返回结果：
       - 返回所有扫描节点的列表，每个节点对应一个分区，包含该分区内的所有设备条目
  - 说明：`visitTreeDeviceViewScan()`函数的主要作用是将`TreeDeviceViewScanNode`按照数据分区拆分为多个`TreeAlignedDeviceViewScanNode`和`TreeNonAlignedDeviceViewScanNode`，每个节点负责扫描一个分区内的设备数据。这样可以实现并行扫描，提高查询性能

- visitInformationSchemaTableScan（`TableDistributedPlanGenerator.visitInformationSchemaTableScan()`）：这里是对专门的数据库Information的扫描操作
  - 先获取`informationSchema`数据所在的datanode列表，随后遍历为每个datanode构建一个`InformationSchemaTableScanNode`

- visitAggregation（`TableDistributedPlanGenerator.visitAggregation()`）：
  - 如果聚合是流式的，那么需要在分组键上构建好排序的方案，方便流式的处理
  - 遍历分布式构建其子节点，将子节点的排序信息传递到当前节点
  - 如果子节点仅有一个，那么直接返回原有的父子结构
  - 如果子节点存在多个，那么这里会构建一个两阶段的聚合，即final - partial。这里使用了`split()`函数来完成这个两阶段聚合的构建操作。这里为每个子节点都构建了partial的中间聚合
  - 最后根据中间聚合是否有序，构建`MergeSortNode`或者`CollectNode`来合并数据

- visitAggregationTableScan（`TableDistributedPlanGenerator.visitAggregationTableScan()`）：优化规则中实现的，将aggregation下推到tablescan
  - 遍历设备条目，获取其所在的分区信息。如果存在多个分区，则需要标记需要拆分聚合；如果没有分区信息，则设置默认的分区信息
  - 如果需要拆分聚合操作，调用`split()`方法将聚合操作拆分为两个阶段，即final - partial
  - 如果聚合操作是流式的且上下文中没有排序属性，则设置排序方案
  - 调用`buildRegionNodeMap()`方法，为每个分区创建独立的聚合扫描节点
  - 遍历所有分区扫描节点，记录每个分区的设备条目数量，确定设备条目数量最多的分区，将其记录到上下文中
  - 如果上下文中有排序属性，则调用`processSortProperty()`方法对扫描节点进行排序优化
  - 如果拆分聚合了，那么需要判断中间聚合节点数量，如果仅有一个，那么直接作为final的子节点。如果有多个，则根据是否需要排序去构建`MergeSortNode`还是`CollectNode`

##### 2.2.3.2 分布式计划的优化

主要类路径：
- `org.apache.iotdb.db.queryengine.plan.relational.planner.TableDistributedPlanner`：表模型分布式计划器
- `org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.DistributedOptimizeFactory`：分布式计划优化构建器
- `org.apache.iotdb.db.queryengine.plan.Coordinator`：查询协调器

在`TableDistributedPlanner.generateDistributedPlanWithOptimize()`函数中，在2.2.3.1的基础上，对于查询的计划，会使用从`Coordinator`中使用`DistributedOptimizeFactory`生成并传递过来的分布式计划优化器。

分布式计划优化器的主要规则：

1. MergeLimitWithMergeSort规则：
   - 将LimitNode和MergeSortNode合并为TopKNode
   - 优化前：Limit (limit = x) -> MergeSort (order by a, b)
   - 优化后：TopK (limit = x, order by a, b) -> Limit (limit = x)
   - 在TopKNode的每个子节点上添加LimitNode，减少传输到TopK的数据量
   - 仅适用于不带ties的LimitNode

2. MergeLimitOverProjectWithMergeSort规则：
   - 处理LimitNode和ProjectNode、MergeSortNode的组合
   - 优化前：Limit (limit = x) -> Project (identity, narrowing) -> MergeSort (order by a, b)
   - 优化后：Project (identity, narrowing) -> TopK (limit = x, order by a, b)
   - 将TopKNode放在ProjectNode下方，保持ProjectNode的输出列映射关系

3. SortElimination规则：
   - 消除冗余的SortNode
   - 如果子节点已经按照所需的顺序输出数据，则可以消除上层的SortNode
   - 检查SortNode的排序键和顺序是否与子节点的排序属性一致
   - 如果一致，直接返回子节点，移除SortNode

4. EliminateLimitWithTableScan规则：
   - 将LimitNode下推到TableScanNode
   - 在数据扫描阶段就限制返回的行数，减少数据传输
   - 适用于LimitNode直接位于TableScanNode上方的情况

5. EliminateLimitProjectWithTableScan规则：
   - 处理LimitNode、ProjectNode和TableScanNode的组合
   - 将LimitNode下推通过ProjectNode到达TableScanNode
   - 需要确保ProjectNode不会影响Limit的语义

6. PushDownOffsetIntoTableScan规则：
   - 将OffsetNode下推到TableScanNode
   - 在数据扫描阶段就跳过前面的行，减少数据传输
   - 需要与LimitNode配合使用，确保下推的语义正确

优化器的执行流程：
- 分布式计划优化器在分布式计划生成之后、ExchangeNode添加之前执行
- 使用IterativeOptimizer迭代应用规则，直到没有规则可以应用
- 每个规则使用Pattern匹配来识别可以优化的计划子树
- 规则应用后会记录统计信息，包括调用次数、应用次数、耗时等

这些优化规则主要针对分布式查询的特点：
- 减少网络传输的数据量（通过下推Limit、Offset）
- 减少排序操作的数据量（通过合并Limit和Sort为TopK）
- 消除不必要的操作（通过SortElimination）
- 在数据源附近进行过滤和限制，减少后续节点的处理负担

##### 2.2.3.3 ExchangeNode添加

主要类路径：
- `org.apache.iotdb.db.queryengine.plan.relational.planner.TableDistributedPlanner`：表模型分布式计划器
- `org.apache.iotdb.db.queryengine.plan.relational.planner.AddExchangeNodes`：ExchangeNode添加器

在`TableDistributedPlanner.generateDistributedPlanWithOptimize()`函数中，为2.2.3.2优化后的分布式计划添加`ExchangeNode`。

分布式计划优化之后，需要给不同分片的Node之间添加上`ExchangeNode`，这里通过新建`AddExchangeNodes`对象，调用其`addExchangeNodes()`函数的`accept()`来处理，实际处理函数是`AddExchangeNodes`的`visitPlan()`等函数。（目前绝大多数的node采用统一处理，仅`TableScanNode`等个别node有自己的添加逻辑）

在`AddExchangeNodes.visitPlan()`函数中，首先遍历所有子节点，为其调用`visitPlan()`函数，实现计划树的递归处理。对于单个子节点的计划树节点，会在`nodeDistributionMap`中记录计划树id与其数据分区方式的对应关系；对于多个子节点的计划树节点，使用`nodeDistributionMap`获取子节点的数据分区，对于不来自最频繁使用的数据分区的子节点与当前节点之间添加`ExchangeNode`，对于来自该数据分区的子节点则是保留原有父子关系，最后也会在`nodeDistributionMap`中记录计划树id与其数据分区方式的对应关系。（仅依赖数据的分区方式来添加`ExchangeNode`）

而最底层的`TableScanNode`，则是使用`AddExchangeNodes.visitTableScan()`函数进行操作，分区信息来自于其数据分区。

`ExchangeNode`添加后，回到`TableDistributedPlanner.plan()`函数中，随后会调用`adjustUpstream()`函数，中转到`adjustUpStreamHelper()`，为`ExchangeNode`及其孩子操作之间添加`IdentitySinkNode`，每一个`ExchangeNode`仅对应一个`IdentitySinkNode`（目前有两种sink，一种是`ShuffleSink`，一种是`IdentitySink`）

##### 2.2.3.4 分布式计划的切分

主要类路径：
- `org.apache.iotdb.db.queryengine.plan.relational.planner.TableDistributedPlanner`：表模型分布式计划器
- `org.apache.iotdb.db.queryengine.plan.relational.planner.SubPlanGenerator`：子计划生成器
- `org.apache.iotdb.db.queryengine.plan.relational.planner.TableModelQueryFragmentPlanner`：表模型查询Fragment计划器

这个部分的逻辑主要在`TableDistributedPlanner.generateDistributedPlan()`函数中，可以划分为以下三个步骤：

###### 2.2.3.4.1 生成subplan

在`generateDistributedPlan()`中，使用`SubPlanGenerator.splitToSubPlan()`函数，针对`ExchangeNode`进行切分。这里从逻辑计划树的根节点开始，为其构建`subplan`（是`PlanFragment`的逻辑概念，包含有`PlanFragment`以及`subplan`之间的树状关系），随后遍历其孩子节点，依次调用`splitToSubPlan()`函数。仅当当前节点是`ExchangeNode`的时候，会将`ExchangeNode`与其孩子节点`sinkNode`断开链接，然后用`sinkNode`作为根节点构建`subplan`的`childSubplan`，如此便完成了`subplan`的切分。（注意：此时`subplan`中还是逻辑计划节点）

###### 2.2.3.4.2 生成FragmentInstance

在`generateDistributedPlan()`中，使用`TableModelQueryFragmentPlanner.plan()`来完成。

这里划分为两个阶段，首先是准备阶段。这里遍历`planFragment`，构建出对应的`FragmentInstance`（是`PlanFragment`的封装，补充了一些执行参数，例如超时、客户端等）：
- 对于没有数据副本的情况（元数据/无数据操作，仅有`showQuery`和`aggregationQuery`），会获取`subplan`的节点是否对执行的datanode有偏好，如果有就使用该datanode的编号构建`QueryExecutor`。如果没有偏好，就使用本地的datanode编号来构建`QueryExecutor`
- 对于有数据副本的情况，这是直接用副本信息构建`storageExecutor`，同时筛选副本中筛选出可以使用的数据节点作为目标datanode。（筛选过程使用`selectTargetDataNode()`函数，逻辑为根据设置的一致性级别，如果是`weak`，就从可以使用的datanode随机选择，这里也不是很随机，使用`sessionId`模可用的datanode的size；如果不是`weak`，就直接去可用datanode的第一个）

这里会记录的信息有：
- 遍历所有的`planFragment`，构建所有的`planNode`到其`planFragment`的映射
- 记录有datanode与其含有的`fragmentInstance`列表的映射
- 记录有`fragmentId`到其构建出来的`FragmentInstance`的映射（从这个结构也可以看出`planFragment`和`FragmentInstance`是一一对应的）

在`plan()`的第二步是`calculateNodeTopologyBetweenInstance()`，用于计算和设置查询计划中的instance之间的拓扑关系，实际上就是通过遍历找到分离的`IdentitySinkNode`和`ExchangeNode`，然后分别设置对面的datanode为传输的终点和传输的来源。

###### 2.2.3.4.3 聚合数据setSinkForRootInstance

在分布式计划处理的最后，如果这个是查询语句，那么还需要给根Instance设置将数据流向`ResultNode`，这里是通过`setSinkForRootInstance()`函数来实现的。在函数中，会为根Instance（也就是查询计划中最后执行的Instance）设置一个`SinkNode`。这个`SinkNode`是一个`IdentitySinkNode`，这里使用了`MPPQueryContext.getResultNodeContext().setUpStream()`的函数设置`ResultNodeContext`作为上游信息。也就是最后的结果都会赋值给`ResultNode`。

#### 2.2.4 分布式计划的调度 doschedule

主要类路径：
- `org.apache.iotdb.db.queryengine.plan.execution.QueryExecution`：查询执行对象
- `org.apache.iotdb.db.queryengine.plan.relational.planner.TableModelPlanner`：表模型计划器
- `org.apache.iotdb.db.queryengine.plan.scheduler.ClusterScheduler`：集群调度器

在分布式计划生成之后，继续`QueryExecution.start()`函数，接下来是使用分布式任务的调度处理函数`schedule()`。这里使用`QueryExecution.schedule()`和`TableModelPlanner.doSchedule()`作为中转，最终使用`ClusterScheduler.start()`函数开始dispatch的过程（这里不考虑LoadTsfile的处理，仅看查询处理，这里的dispatch过程，包括了FI的分发，node转化为operator，FI处理为driver后提交到队列，dispatch过程结束后，主线程会切换执行状态机的状态到running））。主要流程分为以下三步

2.2.4.1 FI的分发
主要类路径：
- `org.apache.iotdb.db.queryengine.plan.scheduler.FragmentInstanceDispatcherImpl`：FragmentInstance分发器实现
- `org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager`：FragmentInstance管理器
- `org.apache.iotdb.db.queryengine.plan.scheduler.RegionReadExecutor`：区域读取执行器

针对于查询情况，这里会先使用`FragmentInstanceDispatcherImpl.dispatch()`函数进行中转。说明：这里的分发是异步的，但是异步代码下使用`Future.get()`会阻塞线程直到异步执行结束。这个设计是为了统一处理分发结果和错误，虽然看起来像同步执行，但实际上分发任务是在线程池中异步执行的，主线程通过`get()`等待结果。到达该文件的`dispatchRead()`函数。在这里会遍历所有的`FragmentInstance`（说明：这里可以改为自下而上的处理，目前应该是数据没准备好就会阻塞，逻辑上也是可以完成的），调用该文件的`dispatchOneInstance()`处理单个`FragmentInstance`，这里会区分`FragmentInstance`的目的去向是当前节点`local`，还是远处节点`remote`（这里是通过获取`fragmentInstance`所要执行的datanode与本地的datanode是否一致来判断是remote还是local）。

如果是local的情况会使用该文件的`dispatchLocally()`函数。对于使用`storageExecutor`的FI，会先根据其所使用的数据副本分区id构造共识组的`groupId`。随后，在查询场景下（说明：schema query存在write before read的情况，这是因为某些schema查询需要先写入元数据信息才能正确读取，这里用`tableSchemaQueryWriteVisitor`先处理instance一遍）：
- 有`groupId`，用`RegionReadExecutor.execute()`进行处理，会根据`groupId`的类型分别调用数据分区的共识协议/元数据分区的共识协议的`read()`函数进行中转，分别到`DataRegionStateMachine`/`SchemaRegionStateMachine`的`read()`函数，最终汇聚到`FragmentInstanceManager.execDataQueryFragmentInstance()` / `execSchemaFragmentInstance()`函数上。
- 没有`groupId`，使用`RegionReadExecutor`的同名函数`execute()`进行处理，这里直接调用`FragmentInstanceManager.execDataQueryFragmentInstance()`来进行后续处理

如果是remote的情况，会使用该类的`dispatchRemote()`进行中转（主要是封装重试逻辑），实际处理在`dispatchRemoteHelper()`当中，这里介绍其中针对查询的处理。这里将要处理的instance序列化后封装在`TSendFragmentInstanceReq`当中，如果是`storageExecutor`的情况，还会使用`setConsensusGroupId()`额外封装`groupId`进去。最终使用`sendFragmentInstance()`函数将请求发送到对应的datanode，根据处理结果判断是否需要重试。

datanode在`DataNodeInternalRPCServiceImpl`（说明：这个类实现了`IDataNodeInternalRPCService`接口，是DataNode内部RPC服务的实现类）的`sendFragmentInstance()`当中处理接受的`FragmentInstance`。这里会根据req当中的信息反序列化出`groupId`和FI。在查询情况下的处理与`dispatchLocally()`当中的处理是一致的。即处理schema query的写问题，根据是否有`groupId`判断`RegionReadExecutor`的调用情况。

#### 2.2.4.2 node转化为operator与driverFactory的组织

主要类路径：
- `org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager`：FragmentInstance管理器
- `org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner`：本地执行计划器
- `org.apache.iotdb.db.queryengine.plan.planner.TableOperatorGenerator`：表模型操作符生成器
- `org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanContext`：本地执行计划上下文

这里以`FragmentInstanceManager.execDataQueryFragmentInstance()`函数开始介绍（该函数与`execSchemaQueryFragmentInstance()`过程基本一致，仅在任务优先级、资源管理、pipeline生成上有额外处理），首先为instance创建`FragmentInstanceStateMachine`管理FI的状态，创建`DataNodeQueryContext`，管理datanode查询过程中的上下文信息，创建`FragmentInstanceContext`，管理FI执行的上下文信息。

随后这里调用`LocalExecutionPlanner.plan()`函数，完成将node转化为operator的过程，并在这个过程中将operator组织为driver（连续串行执行的operator）进行组织。这个组织过程，依托于`localExecutionPlanContext`，记录这个转化过程中产出的`pipelineDriverFactory`（后续一个Factory会create出一个driver，所以这里是先组织operator以及相关的driver信息）。

在`plan()`函数中，使用`generateOperator()`，分别处理TREE MODEL和TABLE MODEL两种模式，这里主要介绍TABLE MODEL，即这里会根据node的类型，使用`TableOperatorGenerator`中对应的`visitXXX()`函数来处理。

目前在表模型中，一个FI仅仅会在root的operator创建出一个`pipelineDriverFactory`，相对应的，在树模型中，除了会有root的`pipelineDriver`，在`operatorTreeOperator`处理的过程中，也会根据operator的情况完成相应的`PipelineDriver`的切分，逻辑和祥威学长论文中基本一致，就是处理`consumeAll`和`consumeOneByOne`的两种情况。（这里没有论文中对于数据切分的逻辑）

除了上述的处理，这里还会调用`LocalExecutionPlanContext.constructPipelineMemoryEstimator()`函数，根据分布式计划树的树结构构建内存估算器的树结构，每个点一个内存估算器，用于检查当前查询计划所需的内存是否会超过目前空闲的内存余量。

下面介绍表模型当中，一些主要的node转化为operator的逻辑，基本上都包含有构建包含有（operatorID，planNodeID，operatorName）的operatorContext：（树模型的可以之后再补充）

- IdentitySinkNode（`TableOperatorGenerator.visitIdentitySink()`）：
  - 构建`operatorContext`，使用数据交换管理器`MPPDataExchangeManager`构建`shuffleSinkHandle`（包含下游目标节点的位置列表，shuffle的策略，当前instance的id，当前node的id，instance的上下文，内部处理的时候会将目标地址列表处理为location和小sink的map）
  - 将该`shuffleSinkHandle`设置为`driverContext`的sink（可以观察到这里是driver的sink）
  - 最后校验当前节点的子节点是否仅有一个孩子，将孩子node继续解析，最终将`operatorContext`、`children`、`sinkHandle`这三个信息来构建`sinkOperator`
  - DownStreamChannelIndex说明：`DownStreamChannelIndex`用于标识下游通道索引，在表模型中通常设置为0，因为一个`IdentitySinkNode`通常只有一个下游通道。这个索引用于在多个下游通道的场景中区分不同的通道，目前表模型中每个sink只有一个下游通道，所以统一设置为0

- ExchangeNode（`TableOperatorGenerator.visitTableExchange()`）：
  - 构建`operatorContext`，判断exchange的上游节点是否为当前节点
  - 如果是本地节点，调用`MPPDataExchangeManager.createLocalSourceHandleForFragment()`构造local的Source Handle（含有本地instance的id，当前plannode的id，上游plannode的id，上游instance的id，sink索引号）
  - 如果是远程节点，调用`createSourceHandle()`构造remote的Source Handle（含有本地instance的id，当前plannode的id，sink索引号，上游节点，上游instance的id）
  - 最终使用`operatorContext`、`sourceHandle`和上游节点的id来构建最终的`ExchangeOperator`
  - `ExchangeOperator`负责从上游节点接收数据，是分布式查询中数据交换的关键组件

- ProjectNode（`TableOperatorGenerator.visitProject()`）：
  - 构建`operatorContext`
  - 如果子节点是`FilterNode`，会合并处理，提取`FilterNode`的谓词和`ProjectNode`的投影表达式
  - 调用`constructFilterAndProjectOperator()`构建`FilterAndProjectOperator`
  - 该operator负责表达式计算和列投影，将输入数据转换为输出格式
  - 对于每个输出表达式，会构建相应的`Transformer`来处理数据转换

- FilterNode（`TableOperatorGenerator.visitFilter()`）：
  - 构建`operatorContext`
  - 获取子节点的operator，提取过滤谓词表达式
  - 调用`constructFilterAndProjectOperator()`构建`FilterAndProjectOperator`
  - 该operator会对输入数据进行过滤，只保留满足谓词条件的行
  - 过滤表达式会被编译为可执行的`FilterTransformer`

- SortNode/StreamSortNode（`TableOperatorGenerator.visitSort()` / `visitStreamSort()`）：
  - 构建`operatorContext`
  - 获取子节点的operator和排序方案（`OrderingScheme`）
  - 根据排序键和排序方向构建`SortOperator`
  - `SortOperator`会对输入数据进行排序，输出有序的数据流
  - `StreamSortNode`用于流式排序，适用于大数据量的场景

- LimitNode（`TableOperatorGenerator.visitLimit()`）：
  - 构建`operatorContext`
  - 获取子节点的operator和limit数量
  - 构建`LimitOperator`，限制输出数据的行数
  - `LimitOperator`会跟踪已输出的行数，达到limit后停止输出

- OffsetNode（`TableOperatorGenerator.visitOffset()`）：
  - 构建`operatorContext`
  - 获取子节点的operator和offset数量
  - 构建`OffsetOperator`，跳过前面的指定行数
  - `OffsetOperator`会跳过前offset行数据，然后输出后续数据

- AggregationNode（`TableOperatorGenerator.visitAggregation()`）：
  - 构建`operatorContext`
  - 获取子节点的operator和聚合函数信息
  - 根据聚合步骤（`PARTIAL`或`FINAL`）构建相应的`AggregationOperator`
  - `PARTIAL`聚合在数据源附近进行部分聚合，`FINAL`聚合对部分聚合结果进行最终聚合
  - 对于流式聚合，需要在分组键上建立排序方案

- JoinNode（`TableOperatorGenerator.visitJoin()`）：
  - 构建`operatorContext`
  - 分别处理左右子节点，获取左右operator
  - 根据JOIN类型（`INNER`、`LEFT`、`RIGHT`、`FULL`）和连接条件构建相应的`JoinOperator`
  - `JoinOperator`负责实现表连接操作，根据连接条件匹配左右表的数据

- TreeAlignedDeviceViewScanNode（`TableOperatorGenerator.visitTreeAlignedDeviceViewScan()`）：
  - 根据节点的数据库名称，使用`createTreeDeviceIdColumnValueExtractor()`构建设备ID提取器用于解析树状数据库的路径
  - 准备`tablescanOperator`的构建参数，使用`constructAbstractTableScanOperatorParameter()`函数，整理输出列的元数据、列索引数组，`scanOption`（即下推的limit，offset，filter等信息）
  - 使用上述提取器和构建参数构建`TreeAlignedDeviceViewScanOperator`
  - 最后使用`addSource()`函数，将operator作为driver的`sourceOperator`，同时为每个`deviceEntry`构建对齐路径添加到driver的上下文中
  - 当前的driver作为FI的最底层输入

- TreeNonAlignedDeviceViewScanNode（`TableOperatorGenerator.visitTreeNonAlignedDeviceViewScan()`）：
  - 检查是否包含字段列，如果没有字段列则返回`EmptyDataOperator`
  - 构建设备ID提取器和扫描参数
  - 构建`DeviceIteratorScanOperator`，该operator会为每个设备生成子operator树
  - 子operator树包括`SeriesScanOperator`、`InnerTimeJoinOperator`、`FullOuterTimeJoinOperator`、`LeftOuterTimeJoinOperator`等
  - 根据是否下推limit和offset，决定是否在子operator树中添加`LimitOperator`和`OffsetOperator`

- DeviceTableScanNode（`TableOperatorGenerator.visitDeviceTableScan()`）：
  - 最基础的数据扫描操作，使用`constructAbstractTableScanOperatorParameter()`函数准备tablescan的构建参数
  - 直接构建出`tablescanOperator`（`TableScanOperator`）
  - 使用`addSource()`函数，将当前操作作为driver的`sourceOperator`，添加到上下文中
  - 查询的数据源是在构建driver的时候传入的device路径信息，在初始化的时候，就会根据路径信息，在分区数据中找到路径相关的所有tsfile资源（对齐和非对齐）

- InformationSchemaTableScanNode（`TableOperatorGenerator.visitInformationSchemaTableScan()`）：
  - 构建`operatorContext`
  - 根据表名获取对应的`InformationSchemaTableScanOperator`
  - 该operator负责扫描系统元数据表，返回数据库、表、列等元数据信息

- TopKNode（`TableOperatorGenerator.visitTopK()`）：
  - 构建`operatorContext`
  - 获取子节点的operator和排序方案、limit数量
  - 构建`TopKOperator`，该operator会维护一个大小为K的堆，只保留前K个元素
  - `TopKOperator`比`SortNode`+`LimitNode`的组合更高效，因为不需要完全排序

node转operator的通用流程（`TableOperatorGenerator`）：
1. 创建`OperatorContext`（`LocalExecutionPlanContext.addOperatorContext()`），包含`operatorID`、`planNodeId`、`operatorName`等信息
2. 递归处理子节点，将子`PlanNode`转换为子`Operator`（调用子节点的`accept()`方法）
3. 根据`PlanNode`的类型和属性，构建相应的`Operator`
4. 对于`SourceNode`（如`TableScanNode`），调用`addSource()`将其添加到driver的source列表
5. 对于`SinkNode`（如`IdentitySinkNode`），设置driver的sink（`driverContext.setSink()`）
6. 构建完成后，operator会被组织到`PipelineDriverFactory`中，后续用于创建Driver执行



##### 2.2.4.3 driver的创建与提交

主要类路径：
- `org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager`：FragmentInstance管理器
- `org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceExecution`：FragmentInstance执行对象
- `org.apache.iotdb.db.queryengine.execution.schedule.DriverScheduler`：Driver调度器
- `org.apache.iotdb.db.queryengine.execution.schedule.task.DriverTaskHandle`：Driver任务句柄

在`FragmentInstanceManager.execDataQueryFragmentInstance()`中，对于构建好的`pipelineDriverFactoryList`，遍历这个factory队列，将其处理为driver的队列。如果这个分片Instance具有高优先级，那么所有的driver也需要设置高优先级。随后在`FragmentInstanceExecution.createFragmentInstanceExecution()`函数中，将已有的信息封装为`FragmentInstanceExecution`，并使用`DriverScheduler.submitDrivers()`函数来完成任务的提交。

在`DriverScheduler.submitDrivers()`函数中，首先创建`DriverTaskHandle`对象管理任务的调度和状态信息。随后将之前处理好的Driver队列封装为`DriverTask`队列，随后检查`driverTask`之间的依赖关系，为依赖关系设置监听器，当被依赖的任务都完成，才会将当前任务添加到就绪队列（注意：这里的依赖关系，指的是`consumeOneByOne`中构建的pipeline执行先后顺序，而不是通常理解的父子pipeline依赖关系）。随后检查是否有资源限制，对资源限制场景下，会遍历所有的task，检查其CPU和内存占用是否超过阈值，抛出报错。最后将当前可以提交的任务，在`QueryMap`中注册`registerTaskToQueryMap()`（`queryId, [(taskId, task), ...]`），随后使用`submitTaskToReadyQueue()`将其添加到就绪队列中。

#### 2.2.5 task的调度与执行

主要类路径：
- `org.apache.iotdb.db.service.DataNode`：DataNode主类
- `org.apache.iotdb.db.queryengine.execution.schedule.DriverScheduler`：Driver调度器
- `org.apache.iotdb.db.queryengine.execution.schedule.DriverTaskThread`：Driver任务线程
- `org.apache.iotdb.db.queryengine.execution.schedule.AbstractDriverThread`：抽象Driver线程基类
- `org.apache.iotdb.db.queryengine.execution.driver.Driver`：Driver执行器

task执行线程池在datanode中启动，启动路径为：`DataNode.main()` -> `ServerCommandLine.run()` -> `DataNode.start()` -> `DataNode.active()` -> `DataNode.setUp()` -> `registerManager.register(DriverScheduler.getInstance())`。在这里，`DriverScheduler.getInstance()`创建出`DriverScheduler`，而在`registerManager.register()`函数中调用了`DriverScheduler.start()`函数，根据设置数量创建对应的工作线程`DriverTaskThread`，负责处理就绪队列，同时创建哨兵线程，负责处理超时的任务。

这里在创建`DriverTaskThread`线程的时候，同时调用了线程的`start()`函数，这个在底层会由Java虚拟机调用该线程类的`run()`函数，而`DriverTaskThread`的父类`AbstractDriverThread`继承并重写了这个`run()`函数，实现了从指定队列获取任务并执行的逻辑。（说明：如果获取的任务为空，就会在日志中写入error，这里规避没有任务不断写入error的方法，使用`BlockingQueue`，这样在队列为空的时候，`poll()`会阻塞当前线程）。状态调度的代码在`DriverScheduler.Scheduler`类中。

当获取到任务，这里会调用`DriverTaskThread.execute()`函数来对任务进行处理。将任务状态从ready转化到running，设置好时间片后使用`Driver.processFor()`函数进行处理。随后会根据执行结果进一步修改任务的状态信息。

在`Driver.processFor()`函数中，会使用`tryWithLock()`逻辑，间隔100ms去不断尝试获取锁，当任务没有被阻塞的时候，将会使用`processInternal()`函数处理一部分的数据，直到任务结束或者超过时间片。在`processInternal()`中，如果root的operator没有被阻塞，且sink没满、root还有数据（每个operator不断调用孩子的`hasNext()`），就会调用root的`nextWithTimer()`（就是每个operator不断调用孩子的`next()`）然后由sink操作将数据发送出去。

任务调度的详细逻辑：

1. 任务状态管理：
   - DriverTask有多个状态：READY（就绪）、RUNNING（运行中）、BLOCKED（阻塞）、FINISHED（完成）、ABORTED（中止）
   - 状态转换由ITaskScheduler接口的实现类Scheduler管理
   - 状态转换是线程安全的，使用锁机制保护

2. 多级优先级队列：
   - 使用MultilevelPriorityQueue实现多级优先级调度
   - 队列支持多个优先级级别，每个级别有不同的时间片
   - 高优先级任务获得更多CPU时间，低优先级任务获得较少时间
   - 优先级根据任务的等待时间和执行历史动态调整

3. 任务调度流程：
   - DriverTaskThread从readyQueue中获取任务（使用阻塞队列，队列为空时线程阻塞）
   - 调用scheduler.readyToRunning()将任务状态从READY转换为RUNNING
   - 根据任务优先级获取对应的时间片（TIME_SLICE_FOR_EACH_LEVEL）
   - 调用driver.processFor(timeSlice)执行任务
   - 根据执行结果更新任务状态：
     - 如果任务完成，调用scheduler.runningToFinished()转换为FINISHED
     - 如果任务被阻塞（如等待IO），调用scheduler.runningToBlocked()转换为BLOCKED
     - 如果任务时间片用完但未完成，调用scheduler.runningToReady()转换回READY

4. 阻塞和唤醒机制：
   - 当任务需要等待IO或其他资源时，会返回一个ListenableFuture
   - 任务状态转换为BLOCKED，从readyQueue移除，添加到blockedTasks集合
   - 当资源就绪时，Future的监听器会调用scheduler.blockedToReady()
   - 任务状态转换回READY，重新加入readyQueue等待调度

5. 任务依赖管理：
   - DriverTask可以设置dependencyDriverIndex，表示依赖其他DriverTask
   - 被依赖的任务完成时，会触发依赖任务的SettableFuture
   - 依赖任务的监听器会将任务添加到readyQueue
   - 这用于处理consumeOneByOne模式下的pipeline依赖关系

6. 超时处理：
   - 哨兵线程（SentinelThread）定期检查timeoutQueue中的任务
   - 如果任务执行时间超过QUERY_TIMEOUT_MS，会中止任务
   - 超时的任务会被标记为ABORTED状态

7. 资源限制检查：
   - 在submitDrivers时，会检查CPU和内存资源限制
   - 如果任务所需资源超过阈值，会抛出异常，阻止任务提交
   - 这防止了资源耗尽导致的系统不稳定

8. 优先级调整：
   - 任务优先级会根据执行情况动态调整
   - 长时间等待的任务会提高优先级，避免饥饿
   - 使用updateSchedulePriority()方法更新优先级

9. 线程池管理：
   - DriverScheduler根据配置创建固定数量的工作线程（WORKER_THREAD_NUM）
   - 每个线程独立从readyQueue获取任务执行
   - 线程使用ThreadGroup统一管理，便于监控和调试

### 2.3 使用查询ID获取查询结果

主要类路径：
- `org.apache.iotdb.db.queryengine.plan.execution.QueryExecution`：查询执行对象
- `org.apache.iotdb.db.queryengine.common.QueryDataSetUtils`：查询数据集工具类
- `org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager`：MPP数据交换管理器

在上述流程处理完之后，根据查询id获取到对应的`queryExecution`，会构建一个`TSExecuteStatementResp`类的请求对象，查询结果的元数据在`createResponse()`的时候从`queryExecution`中提取出来并设置在返回对象中。使用的是`setResult.apply()`将数据设置在返回对象中，而`setResult`实际上是预先定义好的名为`SELECT_RESULT`的`SelectResult`接口对象（下面介绍）。最后，返回对象会返回到客户端，由客户端完成进一步的处理。

在`SELECT_RESULT`中，调用了`QueryDataSetUtils.convertQueryResultByFetchSize()`函数来完成实际的数据获取，剩余数据判断以及fetchsize的数量保障（说明：row可能会比fetchsize大，在显示的时候是通过`convertQueryResultByFetchSize()`函数中的逻辑来确保返回的数据不超过fetchsize，该函数会按照fetchsize的大小分批返回数据），其中实际的数据获取由`QueryExecution.getByteBufferBatchResult()`函数来完成。

其中实际数据来源于`QueryExecution.resultHandle`。而`resultHandle`的数据则是来源于`MPPDataExchangeManager`管理下的`resultNode`数据源（local和不local，根据分布式情况确定），这个数据源的对接工作，在`QueryExecution`中，分布式计划生成后，`schedule()`执行前的`initResultHandle()`函数就已经完成。如果是`skipExecute`的情况，会对接上`MemorySource`，是在构建`queryExecution`的时候确定。（先对接上数据源，后续执行产生数据了，就可以从这里获取数据）

## 3 Session 接收与展示结果

主要类路径：
- `org.apache.iotdb.session.SessionConnection`：Session连接类
- `org.apache.iotdb.session.SessionDataSet`：Session数据集对象

回到开头客户端的位置，即`SessionConnection.executeQueryStatement()`函数，在查询执行结束后，会将查询结果的元数据、数据以及相关的状态信息封装到`SessionDataSet`对象中，向上传递，最终被用户所获取。