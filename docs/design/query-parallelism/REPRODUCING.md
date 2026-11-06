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

# 查询执行并行化换机复现指南

**目的**：帮助开发者在新的 Linux 主机上构建本分支，并重现单元测试和双节点查询正确性验收。

**摘要**：源码、设计文档和确定性输入已经随分支交付，构建产物由目标主机重新生成。复现先核对代码与入口身份，再运行单元测试、协议测试和隔离集群。本文给出数据区域配置、固定输入加载、结果判据及停止步骤。历史机器证据通过独立清单保留。

**结论**：下列步骤使用仓库内的代码与输入构建两套隔离部署，候选与对照通过完整结果和执行结构检查后，即完成本文范围的查询正确性复现。

## 复现背景

复现的目标是让另一台机器执行相同的查询实现，并产生可以重新核对的结果。成功需要同时证明源码身份、实际发行包、运行拓扑和完整查询结果。并行度表示扫描源的目标执行宽度，DataNode 表示执行查询的数据节点。

本文覆盖正常查询和动态开关开启时的受限分组验收。动态调度的四种动作由专项单元测试覆盖，历史服务验收按各自源码版本保存。性能倍率需要另行绑定数据集和统计方法。

## 环境与目录

已验证环境为 Linux x86_64、JDK 17.0.20.1、Maven 3.6.3 和 Python 3.12.13。目标主机需要 Git、Bash、coreutils、unzip，以及 ss 或 lsof。Maven 需要访问项目声明的依赖仓库，Python 环境需要访问包索引。TsFile 使用带日期的 SNAPSHOT 依赖，目标主机必须核对已登记的实际 JAR 摘要。

隔离脚本启动六个 Java 虚拟机，每个 Java 虚拟机的堆上限为 512 MiB。六个进程的堆上限合计为 3072 MiB，目标主机还需要为堆外内存和编译保留资源。

所有 shell 代码块在同一个 Bash 会话中按顺序执行。第一个变量使用目标主机的持久磁盘目录。运行目录使用唯一时间标识，创建操作在同名目录已经存在时失败。

~~~bash
set -euo pipefail
export IOTDB_REPRO_BASE=/data/iotdb-reproduction
mkdir -p "$IOTDB_REPRO_BASE"
git clone --branch feat/query-parallelism-six-parts \
  https://github.com/linxt20/iotdb.git "$IOTDB_REPRO_BASE/source"
cd "$IOTDB_REPRO_BASE/source"
export IOTDB_REPRO_REPO="$PWD"
export IOTDB_REPRO_RUN="$IOTDB_REPRO_BASE/run-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir "$IOTDB_REPRO_RUN"
git rev-parse HEAD > "$IOTDB_REPRO_RUN/source-commit.txt"
java -version 2> "$IOTDB_REPRO_RUN/java-version.txt"
mvn -version > "$IOTDB_REPRO_RUN/maven-version.txt"
sha256sum "$(readlink -f "$(command -v java)")" \
  > "$IOTDB_REPRO_RUN/java-sha256.txt"
~~~

资料：[环境与代码清单](reproduction-manifest.json)、[TsFile 依赖锁定清单](snapshot-dependencies.json)。

## 代码与入口身份

源码核对使用 Git 中的文件模式、对象身份和路径，并单独核对执行入口的 SHA256。文档和设计目录从代码摘要中排除，因此只调整文档的提交仍能使用同一份实现身份。

必须在源码目录执行以下检查。检查通过表示源码与登记入口匹配，实际加载的发行包在集群启动阶段核对。

~~~bash
python3 - <<'PY'
import hashlib, json, pathlib, subprocess
repo = pathlib.Path.cwd()
manifest = json.loads((repo / "docs/design/query-parallelism/reproduction-manifest.json").read_text())
assert not subprocess.check_output(["git", "status", "--porcelain"], text=True).strip()
tree = subprocess.check_output(["git", "ls-tree", "-rz", "HEAD"])
records = []
for record in tree.split(b"\0"):
    if not record:
        continue
    metadata, path = record.split(b"\t", 1)
    if path.endswith(b".md") or path.startswith(b"docs/"):
        continue
    records.append(record)
digest = hashlib.sha256(b"\0".join(records) + b"\0").hexdigest()
assert digest == manifest["code_git_tree_sha256"], "code tree mismatch"
for name, expected in manifest["entry_sha256"].items():
    assert hashlib.sha256((repo / name).read_bytes()).hexdigest() == expected, name
print("Source and entry identities match")
PY
~~~

资料：[源码与入口摘要](reproduction-manifest.json)。

## 工具快速检查

工具自检使用临时输入检查解析和判定规则，可以在构建发行包前执行。自检结果用于确认验收工具能够运行，服务业务结果由后续集群验收判断。

~~~bash
python3 - <<'PY'
import json, pathlib, subprocess
manifest = json.loads(pathlib.Path("docs/design/query-parallelism/reproduction-manifest.json").read_text())
for entry in manifest["self_test_entries"]:
    subprocess.run(["python3", entry, "--self-test"], check=True)
PY
~~~

资料：[自检入口清单](reproduction-manifest.json)、[已保存的工具自检](evidence/split-20261106/part6-self-tests.json)。

## Java 构建与测试

Maven 多模块构建同时解析 DataNode 及其模块依赖。显式镜像配置与已验证主机一致，配置文件只在本次命令中生效。RAT 跳过参数用于研究目录中已有文件的许可证检查问题，其他跳过参数与已保存的执行记录一致。

专项选择文件包含 108 个测试类名。完整多模块单测另行执行，报告包含失败、错误和跳过数，必须保留各模块原始 XML。

~~~bash
FLAGS="-s $IOTDB_REPRO_REPO/docs/design/query-parallelism/maven-settings.xml -nsu -Dspotless.check.skip=true -Drat.skip=true -Dlicense.skip=true -Dmaven.javadoc.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false"
tests=$(python3 -c 'import json; print(",".join(json.load(open("docs/design/query-parallelism/test-selection.json"))))')

mvn -B test-compile -DskipTests $FLAGS \
  > "$IOTDB_REPRO_RUN/reactor-en.log" 2>&1
mvn -B -pl iotdb-core/datanode -am test -Dtest="$tests" $FLAGS \
  > "$IOTDB_REPRO_RUN/selected-tests.log" 2>&1
mvn -B -pl iotdb-core/datanode -am test \
  -DforkCount=4 -Dtest.port.closed=true $FLAGS \
  > "$IOTDB_REPRO_RUN/full-tests.log" 2>&1
find . -path '*/target/surefire-reports/TEST-*.xml' -print0 \
  | tar --null -T - -czf "$IOTDB_REPRO_RUN/full-test-reports.tar.gz"

mvn -B test-compile -DskipTests -P with-zh-locale $FLAGS \
  > "$IOTDB_REPRO_RUN/reactor-zh.log" 2>&1
mvn -B clean package -pl distribution -am -DskipTests $FLAGS \
  > "$IOTDB_REPRO_RUN/distribution-build.log" 2>&1

export IOTDB_REPRO_DIST="$IOTDB_REPRO_REPO/distribution/target/apache-iotdb-2.0.11-SNAPSHOT-all-bin/apache-iotdb-2.0.11-SNAPSHOT-all-bin"
test -f "$IOTDB_REPRO_DIST/lib/iotdb-server-2.0.11-SNAPSHOT.jar"
find "$IOTDB_REPRO_DIST/lib" -maxdepth 1 -name '*.jar' -type f \
  -exec sha256sum {} + > "$IOTDB_REPRO_RUN/distribution-jars.sha256"
~~~

每条 Maven 命令必须以退出码零结束，失败时必须先处理该失败再继续下一条。上述归档先于 clean 构建执行，因此完整单测报告保存在运行目录中。

合并后保存的完整多模块记录为 7521 项，失败和错误均为零，跳过 56 项。该数字表示对应版本的运行范围，目标主机必须保存自己的结果。

例如，日志包含颜色转义时，直接按文本检索失败计数可能漏报。可以先移除转义，再查看结果，但最终结论仍使用 Maven 退出码与 XML。

~~~bash
sed 's/\x1b\[[0-9;]*m//g' "$IOTDB_REPRO_RUN/full-tests.log" \
  > "$IOTDB_REPRO_RUN/full-tests.plain.log"
~~~

资料：[Maven 公共镜像配置](maven-settings.xml)、[专项选择](test-selection.json)、[完整单测摘要](evidence/merged-20261106/full-unit-test-summary.json)、[发行构建身份](evidence/merged-20261106/clean-package-identity.json)。

## Python 协议测试

Python 测试使用本次生成的协议绑定，并核对 C 扩展已经加载。Thrift 0.25.0 与该版本生成绑定共同用于加速协议测试。

~~~bash
mvn -B -pl iotdb-client/client-py -am generate-sources $FLAGS \
  > "$IOTDB_REPRO_RUN/python-generate.log" 2>&1
python3.12 -m venv "$IOTDB_REPRO_RUN/pyenv"
"$IOTDB_REPRO_RUN/pyenv/bin/python" -m pip install \
  'thrift==0.25.0' 'pytest==8.3.5'
export PYTHONPATH="$IOTDB_REPRO_REPO/iotdb-client/client-py"
"$IOTDB_REPRO_RUN/pyenv/bin/python" - > "$IOTDB_REPRO_RUN/python-loaded-identity.json" <<'PY'
import hashlib, importlib.metadata, json, pathlib, sys
import thrift.protocol.fastbinary as fast
import iotdb.thrift.datanode.ttypes as bindings
print(json.dumps({
    "python": sys.executable,
    "thrift": importlib.metadata.version("thrift"),
    "bindings": bindings.__file__,
    "bindings_sha256": hashlib.sha256(pathlib.Path(bindings.__file__).read_bytes()).hexdigest(),
    "fastbinary": fast.__file__,
    "fastbinary_sha256": hashlib.sha256(pathlib.Path(fast.__file__).read_bytes()).hexdigest()
}, indent=2))
PY
"$IOTDB_REPRO_RUN/pyenv/bin/python" -m pytest \
  iotdb-client/client-py/tests/unit/test_runtime_snapshot_thrift.py -q \
  --junitxml="$IOTDB_REPRO_RUN/python-tests.xml" \
  > "$IOTDB_REPRO_RUN/python-tests.log" 2>&1
~~~

该测试范围包含 16 项，全部通过时才能使用加速协议往返结果。目标主机实际绑定路径必须位于本次源码目录。

资料：[Python 冻结环境](evidence/merged-20261106/python-runtime-identity-thrift25.json)、[协议测试源码](../../../iotdb-client/client-py/tests/unit/test_runtime_snapshot_thrift.py)。

## 双节点部署与输入

候选与对照各运行一个 ConfigNode 和两个 DataNode。两套部署使用相同发行包，候选打开属性规划和受限哈希分组路径，对照关闭这两项。

首次启动前，必须把两个 ConfigNode 都配置为每数据库两个数据区域。该配置配合确定性设备输入产生跨节点扫描，实际源数量仍由验收器检查。

~~~bash
export IOTDB_REPRO_CLUSTER="$IOTDB_REPRO_RUN/isolated-nxp"
launcher=research/query-intra-parallelism-e2e-acceptance/linux-isolated-cluster/launch-isolated-nxp.sh
bash "$launcher" --mode prepare --root "$IOTDB_REPRO_CLUSTER" \
  --distribution-root "$IOTDB_REPRO_DIST" --port-offset 19000

for arm in candidate control; do
  printf '\ndata_region_group_extension_policy=CUSTOM\ndefault_data_region_group_num_per_database=2\n' \
    >> "$IOTDB_REPRO_CLUSTER/$arm/confignode/conf/iotdb-system.properties"
done
export JAVA_TOOL_OPTIONS="-Xlog:class+load=info:file=$IOTDB_REPRO_RUN/classload-%p.log"
bash "$launcher" --mode start --root "$IOTDB_REPRO_CLUSTER" --deployment both
bash "$launcher" --mode status --root "$IOTDB_REPRO_CLUSTER" --deployment both \
  > "$IOTDB_REPRO_RUN/cluster-status.txt"
find "$IOTDB_REPRO_CLUSTER" -path '*/conf/iotdb-system.properties' \
  -exec sha256sum {} + > "$IOTDB_REPRO_RUN/runtime-configs.sha256"
~~~

上述偏移对应候选客户端端口 46667、47667，以及对照端口 56667、57667。配置使用回环地址，持久目录和进程清单均位于新建的运行目录。

输入加载通过命令行客户端执行仓库中的固定 SQL。密码使用新部署的数据库账户凭据，下面的读取操作将输入保存在会话环境中。

~~~bash
read -r -s -p "Isolated database password: " IOTDB_E2E_PASSWORD
printf '\n'
export IOTDB_E2E_PASSWORD
fixture_sql=$(python3 - <<'PY'
from pathlib import Path
source = Path("research/query-intra-parallelism-e2e-acceptance/sql/multisource_hash_groupby_fixture.sql").read_text()
print("\n".join(line for line in source.splitlines() if not line.lstrip().startswith("--")))
PY
)
for port in 46667 56667; do
  bash "$IOTDB_REPRO_DIST/sbin/start-cli.sh" \
    -h 127.0.0.1 -p "$port" -u root -pw "$IOTDB_E2E_PASSWORD" \
    -sql_dialect table -e "$fixture_sql" \
    > "$IOTDB_REPRO_RUN/fixture-$port.stdout" \
    2> "$IOTDB_REPRO_RUN/fixture-$port.stderr"
done
python3 - <<'PY'
from pathlib import Path
import os
root = Path(os.environ["IOTDB_REPRO_RUN"])
for port in (46667, 56667):
    text = (root / f"fixture-{port}.stdout").read_text()
    assert text.count("The statement is executed successfully.") == 4, text
print("Both deployments loaded all fixture statements")
PY
~~~

资料：[隔离部署工具](../../../research/query-intra-parallelism-e2e-acceptance/linux-isolated-cluster/README.md)、[确定性 SQL](../../../research/query-intra-parallelism-e2e-acceptance/sql/multisource_hash_groupby_fixture.sql)、[原配置身份](evidence/merged-20261106/runtime-config-identity.json)。

## 结果与拓扑验收

验收器同时核对实际节点、片段、源、桶和交换归属，并比较候选与对照的完整结果多重集。多重集会保留重复行的次数，因此错误地重复输出同一行会改变比较结果。

两源各向两个桶发送数据时，需要四条独立交换边。该固定输入的三个结果组分别包含四行，组计数合计为十二，与输入行数一致。

~~~bash
runner=research/query-intra-parallelism-e2e-acceptance/scripts/run_multisource_hash_groupby_e2e.py
python3 "$runner" \
  --output "$IOTDB_REPRO_RUN/multisource-static" \
  --table p0multi.telemetry \
  --expected-sources 2 --expected-buckets 2 --expected-datanodes 2 \
  --minimum-fragment-instances 5 \
  --enabled-isolation-id candidate --baseline-isolation-id control \
  --enabled-cli "$IOTDB_REPRO_DIST/sbin/start-cli.sh" \
  --enabled-host 127.0.0.1 --enabled-port 46667 \
  --enabled-config "$IOTDB_REPRO_CLUSTER/candidate/datanode-1/conf/iotdb-system.properties" \
  --baseline-cli "$IOTDB_REPRO_DIST/sbin/start-cli.sh" \
  --baseline-host 127.0.0.1 --baseline-port 56667 \
  --baseline-config "$IOTDB_REPRO_CLUSTER/control/datanode-1/conf/iotdb-system.properties"
~~~

必须保留验收 JSON、原始 CLI 输出、执行 SQL 和规范化结果。合格结果包含两个实际 DataNode、至少五个片段、两个扫描源、两个桶和四条归属正确的交换边。

资料：[多源验收说明](../../../research/query-intra-parallelism-e2e-acceptance/MULTISOURCE_HASH_GROUPBY_ACCEPTANCE.md)、[保存的候选与对照报告](evidence/merged-20261106/multisource-acceptance.json)。

## 动态开关验收

动态开关验收在同一份输入上单独保存报告。先停止候选的两个 DataNode，再修改两份配置并重启，ConfigNode 和对照部署继续运行。

~~~bash
bash "$launcher" --mode stop-datanodes --root "$IOTDB_REPRO_CLUSTER" --deployment candidate
python3 - <<'PY'
from pathlib import Path
import os
root = Path(os.environ["IOTDB_REPRO_CLUSTER"]) / "candidate"
for node in ("datanode-1", "datanode-2"):
    config = root / node / "conf/iotdb-system.properties"
    text = config.read_text()
    assert "enable_dynamic_scan_parallelism=" not in text
    config.write_text(text + "\nenable_dynamic_scan_parallelism=true\n")
PY
bash "$launcher" --mode start-datanodes --root "$IOTDB_REPRO_CLUSTER" --deployment candidate

python3 "$runner" \
  --output "$IOTDB_REPRO_RUN/multisource-dynamic-enabled" \
  --table p0multi.telemetry \
  --expected-sources 2 --expected-buckets 2 --expected-datanodes 2 \
  --minimum-fragment-instances 5 \
  --enabled-isolation-id candidate --baseline-isolation-id control \
  --enabled-cli "$IOTDB_REPRO_DIST/sbin/start-cli.sh" \
  --enabled-host 127.0.0.1 --enabled-port 46667 \
  --enabled-config "$IOTDB_REPRO_CLUSTER/candidate/datanode-1/conf/iotdb-system.properties" \
  --baseline-cli "$IOTDB_REPRO_DIST/sbin/start-cli.sh" \
  --baseline-host 127.0.0.1 --baseline-port 56667 \
  --baseline-config "$IOTDB_REPRO_CLUSTER/control/datanode-1/conf/iotdb-system.properties"
~~~

该受限分组场景的已存动态决策为串行回退，原因是扫描资格守卫要求保留对应执行形态。该证据用于动态开关、结果与资源统计的兼容性判断，动态加速效果使用独立性能实验。

例如，动态开关开启后，结果仍有三组且每组计数为四，只能建立该查询的结果正确性。动态调度动作和耗时分别需要读取动作回执和性能样本。

资料：[动态开关报告](evidence/merged-20261106/multisource-dynamic-acceptance.json)、[真实运行快照](evidence/merged-20261106/dynamic-runtime-rpc-snapshots.json)、[历史 E 阶段验收](evidence/stage-e-20261104/delivery-final.json)。

## 发行包与加载身份

发行包核对同时比较目标类与构建输出，启动后的类加载日志还需要显示实际运行的服务类来自本次发行包。该步骤使源码、构建和运行三者形成可检查的对应关系。

~~~bash
python3 - <<'PY' > "$IOTDB_REPRO_RUN/runtime-class-binding.json"
import hashlib, json, os, pathlib, zipfile
repo = pathlib.Path(os.environ["IOTDB_REPRO_REPO"])
dist = pathlib.Path(os.environ["IOTDB_REPRO_DIST"])
manifest = json.loads((repo / "docs/design/query-parallelism/reproduction-manifest.json").read_text())
jar = dist / "lib/iotdb-server-2.0.11-SNAPSHOT.jar"
dependencies = json.loads((repo / "docs/design/query-parallelism/snapshot-dependencies.json").read_text())
for dependency in dependencies["artifacts"]:
    if dependency["extension"] != "jar":
        continue
    pattern = dependency["artifact"] + "-" + dependency["version"] + ".jar"
    candidates = list((dist / "lib").glob(pattern))
    assert len(candidates) == 1, pattern
    assert hashlib.sha256(candidates[0].read_bytes()).hexdigest() == dependency["sha256"], pattern
bindings = {}
with zipfile.ZipFile(jar) as archive:
    for name in manifest["runtime_class_bindings"]:
        compiled = repo / "iotdb-core/datanode/target/classes" / name
        actual = archive.read(name)
        assert actual == compiled.read_bytes(), name
        bindings[name] = hashlib.sha256(actual).hexdigest()
logs = list(pathlib.Path(os.environ["IOTDB_REPRO_RUN"]).glob("classload-*.log"))
assert logs, "class loading evidence missing"
text = "\n".join(path.read_text(errors="replace") for path in logs)
for name in ("org.apache.iotdb.db.service.DataNode",
             "org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner"):
    rows = [line for line in text.splitlines() if name + " source:" in line]
    assert rows and all(str(dist) in line for line in rows), name
print(json.dumps({"jar": str(jar), "class_sha256": bindings}, indent=2))
PY
~~~

资料：[加载身份核对清单](reproduction-manifest.json)。

## 停止与运行归档

集群停止使用运行目录中的进程清单，并核对对应服务类与目录归属。运行归档包含目标主机产生的报告、配置、加载日志和数据库目录。

~~~bash
bash "$launcher" --mode stop --root "$IOTDB_REPRO_CLUSTER" --deployment both
bash "$launcher" --mode status --root "$IOTDB_REPRO_CLUSTER" --deployment both \
  > "$IOTDB_REPRO_RUN/cluster-stopped-status.txt"
tar -C "$(dirname "$IOTDB_REPRO_RUN")" \
  -czf "$IOTDB_REPRO_RUN.tar.gz" "$(basename "$IOTDB_REPRO_RUN")"
sha256sum "$IOTDB_REPRO_RUN.tar.gz" > "$IOTDB_REPRO_RUN.tar.gz.sha256"
~~~

必须在清理运行目录前，把归档和摘要复制到独立持久存储。目标机原始产物与仓库中的历史摘要分别保存，源码和设计文档通过 Git 获取。

资料：[停止脚本](../../../research/query-intra-parallelism-e2e-acceptance/linux-isolated-cluster/launch-isolated-nxp.sh)、[历史证据清单](evidence-manifest.json)。
