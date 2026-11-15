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

# 运行期动态 DOP 真实 SQL 复现指南

**目的**：在另一台 Linux 主机上重新构建本分支，产生双节点真实 SQL、调度动作、完整结果与资源结算证据。

**摘要**：候选与固定单 worker 对照各运行一个 ConfigNode 和两个 DataNode。夹具由仓库生成器重新生成，SQL、参数模板和判据随仓库交付。验收分别检查正常结果、有序结果、取消、超时、运行异常和优化兼容性。本文给出复现配方，服务结论使用目标主机的实际回执。

**结论**：成功需要源码与发行包身份、真实 SQL 与片段身份、场景专用判据以及完整资源回收共同成立。模板准备和驱动退出码不能单独建立运行期弹性或性能结论。

构建与测试前执行 `export GITHUB_ACTIONS=true`，按本仓库 Develocity 配置关闭本地测试缓存；同时核 fresh XML 的实际 tmpdir/classpath。v6 曾捕获七项上游缓存报告并完成独立补跑，不能把 mtime 更新当成测试新执行。

## 复现目标与边界

复现要回答两个问题：同一条真实查询能否安全调整运行 worker，以及实现能否保持正确结果和查询结束后的资源状态。DOP 表示执行宽度，逻辑 lane 表示固定的数据归属。运行期调整改变 worker 数量，逻辑 lane 和交换归属保持固定。

R1 检查同一查询、同一片段和同一组的实际扩容后缩容。R2 检查多个有序 lane 在单 worker 下持续推进。R3 检查完整 LIMIT 结果，R4 至 R6 检查结束路径，R7 检查两种多源哈希聚合形态。元数据查询和有状态函数分别检查兼容性。

本文是可移植复现合同。最终 v6 已完成完整服务矩阵和全量回归；实际结果见 [正式验收摘要](audit/final-runtime-v6-validation.json)。历史 v4 结果仍只绑定其原冻结产物；换机必须重新生成执行身份与验收回执。本文不承诺某个耗时或加速倍率。构造算例：同一组先实际使用一个 worker，再实际使用两个 worker，最后实际使用一个 worker，并且三个动作身份一致，才可能满足 R1。只有请求日志或终态活动数归零，不能满足该判据。

数据来源：[场景清单](../../../../research/query-intra-parallelism-e2e-acceptance/static-scan/runtime-sql-examples/matrix/case-manifest.json)、[参数规格](../../../../research/query-intra-parallelism-e2e-acceptance/static-scan/runtime-sql-examples/parameters-spec.json)。

## 环境与持久目录

目标环境使用 Linux、JDK 17、Maven、Python 3、Git、Bash、GNU coreutils、unzip，以及 ss 或 lsof。完整结果比较使用 GNU sort 和持久临时目录。六个服务进程和构建共享主机资源，堆参数必须受可用内存、堆外空间和磁盘配额约束。

以下命令在同一个 Bash 会话按顺序执行。第一个路径应当改成目标主机的数据盘路径。SSH 密码与数据库密码分别管理，数据库密码只放在环境中。

~~~bash
set -euo pipefail
export IOTDB_RUNTIME_BASE=/data/iotdb-runtime-reproduction
mkdir -p "$IOTDB_RUNTIME_BASE"
git clone --branch feat/runtime-dop https://github.com/linxt20/iotdb.git "$IOTDB_RUNTIME_BASE/source"
cd "$IOTDB_RUNTIME_BASE/source"
export IOTDB_RUNTIME_REPO="$PWD"
export IOTDB_RUNTIME_RUN="$IOTDB_RUNTIME_BASE/run-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir "$IOTDB_RUNTIME_RUN"
mkdir "$IOTDB_RUNTIME_RUN/tmp"
export TMPDIR="$IOTDB_RUNTIME_RUN/tmp"
export IOTDB_RUNTIME_JAVA="$(readlink -f "$(command -v java)")"
export IOTDB_RUNTIME_JAVAC="$(readlink -f "$(command -v javac)")"
export JAVA_HOME="$(dirname "$(dirname "$IOTDB_RUNTIME_JAVA")")"
git rev-parse HEAD > "$IOTDB_RUNTIME_RUN/source-commit.txt"
git status --porcelain > "$IOTDB_RUNTIME_RUN/source-status.txt"
test ! -s "$IOTDB_RUNTIME_RUN/source-status.txt"
"$IOTDB_RUNTIME_JAVA" -version 2> "$IOTDB_RUNTIME_RUN/java-version.txt"
"$IOTDB_RUNTIME_JAVAC" -version > "$IOTDB_RUNTIME_RUN/javac-version.txt" 2>&1
mvn -version > "$IOTDB_RUNTIME_RUN/maven-version.txt"
sha256sum pom.xml docs/design/query-parallelism/maven-settings.xml \
  docs/design/query-parallelism/snapshot-dependencies.json > "$IOTDB_RUNTIME_RUN/build-inputs.sha256"
sha256sum "$IOTDB_RUNTIME_JAVA" "$IOTDB_RUNTIME_JAVAC" > "$IOTDB_RUNTIME_RUN/java-tools.sha256"
export IOTDB_RUNTIME_MATRIX="$IOTDB_RUNTIME_REPO/research/query-intra-parallelism-e2e-acceptance/static-scan/runtime-sql-examples/matrix"
export IOTDB_RUNTIME_LAUNCHER="$IOTDB_RUNTIME_REPO/research/query-intra-parallelism-e2e-acceptance/linux-isolated-cluster/launch-isolated-nxp.sh"
export IOTDB_RUNTIME_CLUSTER="$IOTDB_RUNTIME_RUN/cluster"
export IOTDB_RUNTIME_FIXTURE="$IOTDB_RUNTIME_RUN/fixture"
export IOTDB_RUNTIME_OUTPUT="$IOTDB_RUNTIME_RUN/sql"
python3 - <<'PYVERIFY'
import hashlib, json, os
from pathlib import Path
repo = Path(os.environ["IOTDB_RUNTIME_REPO"])
matrix = Path(os.environ["IOTDB_RUNTIME_MATRIX"])
index = json.loads((matrix / "file-index.json").read_text())
case = json.loads((matrix / "case-manifest.json").read_text())
for item in index["files"]:
    assert hashlib.sha256((repo / item["path"]).read_bytes()).hexdigest() == item["sha256"], item["path"]
for item in case["entryIdentity"].values():
    if isinstance(item, dict) and "path" in item:
        assert hashlib.sha256((repo / item["path"]).read_bytes()).hexdigest() == item["sha256"], item["path"]
observability = json.loads((matrix / "statistics-observability.json").read_text())
for item in observability["sourceEvidence"]:
    assert hashlib.sha256((repo / item["path"]).read_bytes()).hexdigest() == item["sha256"], item["path"]
payload = []
for path in sorted(matrix.rglob("*")):
    if path.is_file() and path.name not in ("case-manifest.json", "file-index.json"):
        content = path.read_bytes()
        payload.append({"path": str(path.relative_to(repo)), "sha256": hashlib.sha256(content).hexdigest(), "bytes": len(content)})
encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
assert hashlib.sha256(encoded).hexdigest() == case["sourceFreeze"]["portablePayloadSha256"]
print("SQL, templates, payload and entry identities match")
PYVERIFY
~~~

历史 A–E 代码树摘要绑定原冻结实现，应当保留其身份。当前扩展核对本轮入口和运行清单。源码检查不能替代发行包和实际加载来源检查。

数据来源：[文件清单](../../../../research/query-intra-parallelism-e2e-acceptance/static-scan/runtime-sql-examples/matrix/file-index.json)、[标准单元验证](README.md#标准验证)。

## 冻结依赖与发行包

TsFile 继续使用既有带时间戳的依赖锁，目标主机不能直接接受名称相同的可变 SNAPSHOT。本配方使用独立 Maven 缓存下载锁定字节。同一产物的其他 URL 可以作为下载来源，内容仍必须通过同一 SHA256。

~~~bash
export IOTDB_RUNTIME_M2="$IOTDB_RUNTIME_RUN/m2"
python3 - <<'PYDEPENDENCIES'
import hashlib, json, os, urllib.request
from pathlib import Path
repo = Path(os.environ["IOTDB_RUNTIME_REPO"])
cache = Path(os.environ["IOTDB_RUNTIME_M2"])
lock = json.loads((repo / "docs/design/query-parallelism/snapshot-dependencies.json").read_text())
for item in lock["artifacts"]:
    target = cache / item["maven_cache_path"]
    target.parent.mkdir(parents=True, exist_ok=True)
    for url in item["download_urls"]:
        try:
            content = urllib.request.urlopen(url, timeout=120).read()
            assert hashlib.sha256(content).hexdigest() == item["sha256"], url
            target.write_bytes(content)
            break
        except Exception as failure:
            print(type(failure).__name__, url)
    else:
        raise RuntimeError("Pinned dependency unavailable: " + item["maven_cache_path"])
PYDEPENDENCIES
cp docs/design/query-parallelism/maven-settings.xml "$IOTDB_RUNTIME_RUN/maven-settings.xml"
FLAGS="-s $IOTDB_RUNTIME_RUN/maven-settings.xml -Dmaven.repo.local=$IOTDB_RUNTIME_M2 -nsu -Dspotless.check.skip=true -Drat.skip=true -Dlicense.skip=true -Dmaven.javadoc.skip=true -Dcheckstyle.skip=true -Denforcer.skip=true -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false"
mvn -B clean package -pl distribution -am -DskipTests $FLAGS > "$IOTDB_RUNTIME_RUN/distribution-build.log" 2>&1
export IOTDB_RUNTIME_DIST="$IOTDB_RUNTIME_REPO/distribution/target/apache-iotdb-2.0.11-SNAPSHOT-all-bin/apache-iotdb-2.0.11-SNAPSHOT-all-bin"
test -f "$IOTDB_RUNTIME_DIST/lib/iotdb-server-2.0.11-SNAPSHOT.jar"
test -f "$IOTDB_RUNTIME_DIST/lib/iotdb-jdbc-2.0.11-SNAPSHOT.jar"
test -f "$IOTDB_RUNTIME_DIST/lib/iotdb-session-2.0.11-SNAPSHOT.jar"
test -f "$IOTDB_RUNTIME_DIST/lib/isession-2.0.11-SNAPSHOT.jar"
find "$IOTDB_RUNTIME_DIST/lib" -maxdepth 1 -name '*.jar' -type f -exec sha256sum {} + > "$IOTDB_RUNTIME_RUN/distribution-jars.sha256"
python3 - <<'PYJARS'
import hashlib, json, os
from pathlib import Path
repo = Path(os.environ["IOTDB_RUNTIME_REPO"])
cache = Path(os.environ["IOTDB_RUNTIME_M2"])
dist = Path(os.environ["IOTDB_RUNTIME_DIST"])
lock = json.loads((repo / "docs/design/query-parallelism/snapshot-dependencies.json").read_text())
for item in lock["artifacts"]:
    assert hashlib.sha256((cache / item["maven_cache_path"]).read_bytes()).hexdigest() == item["sha256"]
    if item["extension"] == "jar":
        jar = dist / "lib" / (item["artifact"] + "-" + item["version"] + ".jar")
        assert hashlib.sha256(jar.read_bytes()).hexdigest() == item["sha256"], jar
print("Pinned cache and distribution JARs match")
PYJARS
~~~

构建退出码必须为零。构建发行包跳过测试，不能作为单元测试结论。组件及回归测试按本目录 README 的标准验证单独执行，并在 clean 前归档 XML。

数据来源：[依赖锁](../snapshot-dependencies.json)、[Maven settings](../maven-settings.xml)、[发行组装](../../../../distribution/src/assembly/all.xml)。

## 独立验收客户端

验收客户端只依赖本次发行包的 lib。源码复制到新目录，禁用隐式源码搜索和注解处理，避免旧 target/classes 或测试依赖改变执行身份。

~~~bash
export IOTDB_RUNTIME_CLI="$IOTDB_RUNTIME_RUN/acceptance-cli"
mkdir "$IOTDB_RUNTIME_CLI"
mkdir "$IOTDB_RUNTIME_CLI/classes" "$IOTDB_RUNTIME_CLI/sources" "$IOTDB_RUNTIME_CLI/empty-sourcepath"
cp research/query-intra-parallelism-e2e-acceptance/static-scan/OrdinarySqlAcceptanceDriver.java \
  research/query-intra-parallelism-e2e-acceptance/static-scan/StaticScanAcceptanceDriver.java "$IOTDB_RUNTIME_CLI/sources/"
export IOTDB_RUNTIME_LIB_CP="$(python3 - <<'PYCP'
import os
from pathlib import Path
paths = sorted((Path(os.environ["IOTDB_RUNTIME_DIST"]) / "lib").glob("*.jar"))
assert paths
print(":".join(str(p.resolve()) for p in paths))
PYCP
)"
"$IOTDB_RUNTIME_JAVAC" -verbose -encoding UTF-8 --release 17 -proc:none -implicit:none \
  -sourcepath "$IOTDB_RUNTIME_CLI/empty-sourcepath" \
  -classpath "$IOTDB_RUNTIME_LIB_CP" -d "$IOTDB_RUNTIME_CLI/classes" \
  "$IOTDB_RUNTIME_CLI/sources/OrdinarySqlAcceptanceDriver.java" \
  "$IOTDB_RUNTIME_CLI/sources/StaticScanAcceptanceDriver.java" \
  > "$IOTDB_RUNTIME_CLI/javac.stdout" 2> "$IOTDB_RUNTIME_CLI/javac.stderr"
export IOTDB_RUNTIME_CP="$IOTDB_RUNTIME_CLI/classes:$IOTDB_RUNTIME_LIB_CP"
"$IOTDB_RUNTIME_JAVA" "-Xlog:class+load=info:file=$IOTDB_RUNTIME_CLI/sql-errors.classload.log" \
  -cp "$IOTDB_RUNTIME_CP" StaticScanAcceptanceDriver --self-test-sql-errors \
  > "$IOTDB_RUNTIME_CLI/sql-errors.json" 2> "$IOTDB_RUNTIME_CLI/sql-errors.stderr"
python3 - <<'PYERRORS'
import json, os
from pathlib import Path
from urllib.parse import unquote, urlparse
cli = Path(os.environ["IOTDB_RUNTIME_CLI"])
result = json.loads((cli / "sql-errors.json").read_text())
assert result["status"] == "PASS_OFFLINE_ACTUAL_JDBC_ERROR_CONTRACT"
assert result["checks"] == 25
jar = (Path(os.environ["IOTDB_RUNTIME_DIST"]) / "lib/iotdb-jdbc-2.0.11-SNAPSHOT.jar").resolve()
source = urlparse(result["jdbcCodeSource"])
assert source.scheme == "file" and Path(unquote(source.path)).resolve() == jar
print("Actual frozen JDBC error-wrapper contract passed all 25 offline checks")
PYERRORS
printf '%s\n' "$IOTDB_RUNTIME_CP" > "$IOTDB_RUNTIME_CLI/classpath.txt"
find "$IOTDB_RUNTIME_CLI/sources" "$IOTDB_RUNTIME_CLI/classes" -type f -exec sha256sum {} + > "$IOTDB_RUNTIME_CLI/inputs-and-classes.sha256"
~~~

离线自测通过只证明实际发行 JDBC 的错误包装与来源识别合同。它模拟 RPC 返回状态，不建立真实 SQL、取消、截止时间或资源验收结论。

源码 SHA、实际 javac 加载来源、classes SHA 和 classpath 必须随运行保存。运行回执还记录驱动 CodeSource 与 Java 身份。同名类若来自多个外部 JAR，应当核对字节一致并保存实际选中的来源，不能补入旧 JAR 使编译勉强通过。

数据来源：[普通 SQL 驱动](../../../../research/query-intra-parallelism-e2e-acceptance/static-scan/OrdinarySqlAcceptanceDriver.java)、[CLI 入口](../../../../research/query-intra-parallelism-e2e-acceptance/static-scan/StaticScanAcceptanceDriver.java)。

## 两套隔离部署

候选启用运行期调度，对照固定为一个 worker。两套部署都保留属性规划、哈希重分布及有序扫描资格开关，使主要差异集中于 worker 调度。

以下堆配置是启动参数示例。构造算例：四个 DataNode 各 8 GiB，加两个 ConfigNode 各 512 MiB，堆上限合计为 33 GiB，此外还需要堆外内存和构建余量。

~~~bash
bash "$IOTDB_RUNTIME_LAUNCHER" --mode prepare \
  --root "$IOTDB_RUNTIME_CLUSTER" --distribution-root "$IOTDB_RUNTIME_DIST" \
  --candidate-profile runtime --control-profile fixed-one --java "$IOTDB_RUNTIME_JAVA" \
  --datanode-xms 1g --datanode-xmx 8g --confignode-xms 256m --confignode-xmx 512m \
  --query-thread-count 8 --jmx-enabled true --time-partition-interval-ms 604800000 --port-offset 19000
bash "$IOTDB_RUNTIME_LAUNCHER" --mode start --root "$IOTDB_RUNTIME_CLUSTER" --deployment both
bash "$IOTDB_RUNTIME_LAUNCHER" --mode status --root "$IOTDB_RUNTIME_CLUSTER" --deployment both \
  > "$IOTDB_RUNTIME_RUN/cluster-started-status.txt"
~~~

prepare 自动写入 CUSTOM 数据区域策略及两区域上限。必须保持毫秒精度和夹具相同的时间分区间隔。launcher 冻结 Java、JAR 和配置身份，非空目录不能覆盖复用。

服务启动后必须检查各节点 process.env、进程命令和类加载日志。DataNode 类必须来自本次发行包的 server JAR，JMX 与服务端口必须属于已登记进程。实际节点 ID 由 ConfigNode 发现，部署模板中的空 ID 不能填入猜测值。


~~~bash
python3 - <<'PYLOADED'
import hashlib, json, os
from pathlib import Path
cluster = Path(os.environ["IOTDB_RUNTIME_CLUSTER"])
dist = Path(os.environ["IOTDB_RUNTIME_DIST"]).resolve()
java = Path(os.environ["IOTDB_RUNTIME_JAVA"]).resolve()
for side in ("candidate", "control"):
    deployment = json.loads((cluster / side / "deployment-manifest.json").read_text())
    for node in deployment["configNodes"] + deployment["nodes"]:
        root = Path(node["nodeRoot"])
        fields = dict(line.split("=", 1) for line in (root / "process.env").read_text().splitlines())
        pid = int(fields["pid"])
        assert Path("/proc", str(pid), "exe").resolve() == java
        assert hashlib.sha256(Path(node["configPath"]).read_bytes()).hexdigest() == node["configSha256"]
        actual = (Path("/proc") / str(pid) / "cmdline").read_bytes().split(b"\0")
        assert any(str(root).encode() in value for value in actual)
        logs = list((root / "identity").glob("classes-loaded-*.log"))
        klass = "org.apache.iotdb.db.service.DataNode" if node["role"] == "DataNode" else "org.apache.iotdb.confignode.service.ConfigNode"
        matches = [line for log in logs for line in log.read_text(errors="replace").splitlines()
                   if klass + " source:" in line]
        assert matches and all(str(dist / "lib" / "iotdb-server-2.0.11-SNAPSHOT.jar") in line
                               if node["role"] == "DataNode" else str(dist / "lib") in line
                               for line in matches), (side, node["name"])
print("Actual JVM executable, configuration and service class sources match")
PYLOADED
~~~

数据来源：[隔离部署工具](../../../../research/query-intra-parallelism-e2e-acceptance/linux-isolated-cluster/launch-isolated-nxp.sh)、[部署操作说明](../../../../research/query-intra-parallelism-e2e-acceptance/linux-isolated-cluster/README.md)。

## 确定性输入与参数绑定

标准输入固定为 64 个设备、四个时间分区和 257 个分组键，共 9404416 行。大部分设备分区有 32768 行，最后一个设备的第四分区有 1048576 行。空值、负值、分组和倾斜均来自生成器的确定定义。

manifest 包含生成器 SHA，因此源码格式变化也可能改变 manifest SHA。目标主机必须绑定新生成的实际 manifest SHA，同时检查稳定 oracle SHA。历史机器的 manifest SHA 只保留为来源信息。

~~~bash
python3 research/query-intra-parallelism-e2e-acceptance/scripts/generate_hash_groupby_fixture.py \
  --mode runtime-sql-v1 --output "$IOTDB_RUNTIME_FIXTURE"
python3 - <<'PYFIXTURE'
import hashlib, json, os
from pathlib import Path
root = Path(os.environ["IOTDB_RUNTIME_FIXTURE"])
case = json.loads((Path(os.environ["IOTDB_RUNTIME_MATRIX"]) / "case-manifest.json").read_text())
fixture = json.loads((root / "manifest.json").read_text())
assert fixture["fixtureVersion"] == "runtime_sql_fixture_v1"
assert (fixture["devices"], fixture["timePartitions"], fixture["groups"], fixture["totalRows"]) == (64, 4, 257, 9404416)
assert fixture["timePartitionIntervalMs"] == 604800000
for name, expected in case["fixture"]["oracles"].items():
    actual = fixture["oracles"][name]
    assert (actual["rows"], actual["sha256"]) == (expected["rows"], expected["sha256"]), name
    assert actual["columnTypes"] == expected["columnTypes"], name
    assert actual["orderBy"] == expected["orderBy"], name
    if actual.get("materialized", True):
        assert hashlib.sha256((root / actual["path"]).read_bytes()).hexdigest() == actual["sha256"], name
(root / "manifest.actual.sha256").write_text(hashlib.sha256((root / "manifest.json").read_bytes()).hexdigest() + "\n")
PYFIXTURE
read -r -s -p "Isolated database password: " IOTDB_ACCEPTANCE_PASSWORD
printf '\n'
export IOTDB_ACCEPTANCE_PASSWORD
python3 - <<'PYBIND'
import hashlib, json, os
from pathlib import Path
matrix = Path(os.environ["IOTDB_RUNTIME_MATRIX"])
target = Path(os.environ["IOTDB_RUNTIME_RUN"]) / "params"
target.mkdir()
roots = {
    "<CLUSTER_ROOT>": os.environ["IOTDB_RUNTIME_CLUSTER"],
    "<FIXTURE_ROOT>": os.environ["IOTDB_RUNTIME_FIXTURE"],
    "<OUTPUT_ROOT>": os.environ["IOTDB_RUNTIME_OUTPUT"],
}
for source in sorted((matrix / "params").glob("*.json")):
    data = json.loads(source.read_text())
    for key, value in list(data.items()):
        if isinstance(value, str):
            for old, new in roots.items():
                value = value.replace(old, new)
            if key == "sqlFile":
                value = str((source.parent / value).resolve())
                assert Path(value).is_file()
            data[key] = value
    encoded = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    assert all(token not in encoded for token in roots)
    (target / source.name).write_text(encoded)
    (target / (source.name + ".sha256")).write_text(hashlib.sha256(encoded.encode()).hexdigest() + "\n")
PYBIND
for side in candidate control; do
  "$IOTDB_RUNTIME_JAVA" "-Xlog:class+load=info:file=$IOTDB_RUNTIME_CLI/load-$side.classload.log" \
    -cp "$IOTDB_RUNTIME_CP" StaticScanAcceptanceDriver \
    --mode load-runtime-fixture --params "$IOTDB_RUNTIME_RUN/params/load-$side.json" \
    > "$IOTDB_RUNTIME_RUN/load-$side.stdout" 2> "$IOTDB_RUNTIME_RUN/load-$side.stderr"
done
~~~

加载器核对输入和行数，用有界 Tablet 批次写入并在每个分区后 FLUSH。它要求全新数据库，重复加载必须使用新的部署目录。

加载回执中的 tableFilePlacementVerified 为 false，加载成功还不能证明实际数据分布。必须保存实际数据区域、节点、刷盘文件路径、字节数和 SHA 清单，并结合 SQL roster 核对两节点扫描。每个实际源 DataRegion 的正 TsFile 总字节数小于 134217728，才满足本夹具预期的初始一个 worker 估计条件。

构造算例：一个实际源区域的文件分别为 40 MiB 和 50 MiB，总计 90 MiB，满足该字节条件。压缩输入为 90 MiB 不能用于推断该条件。

下面的只读命令保存全部实际刷盘文件。必须按后续真实 roster 的 region.id、节点端点和路径核对归属，不能只按目录数量声称两节点分布。

~~~bash
find "$IOTDB_RUNTIME_CLUSTER" -path '*/data/datanode-data/*' -name '*.tsfile' -type f \
  -printf '%p\t%s\n' > "$IOTDB_RUNTIME_RUN/physical-tsfiles.tsv"
find "$IOTDB_RUNTIME_CLUSTER" -path '*/data/datanode-data/*' -name '*.tsfile' -type f \
  -exec sha256sum {} + > "$IOTDB_RUNTIME_RUN/physical-tsfiles.sha256"
test -s "$IOTDB_RUNTIME_RUN/physical-tsfiles.tsv"
~~~

数据来源：[夹具生成器](../../../../research/query-intra-parallelism-e2e-acceptance/scripts/generate_hash_groupby_fixture.py)、[参数模板](../../../../research/query-intra-parallelism-e2e-acceptance/static-scan/runtime-sql-examples/matrix/params)。

## 对照统计的可观测边界

候选默认使用 REQUIRE_FROZEN，必须保留每个实际片段的冻结统计与终态。固定单 worker 对照显式使用 ALLOW_UNRETAINED_CONTROL，因为关闭两种动态功能的普通查询可能不保留统计缓存。缺失统计必须标为未保留，不能填成零或称为已经验证冻结统计。

该宽限只适用于成功查询、control 部署、fixed-one profile、运行期关闭且逻辑上限为一，并且三个额外调度或拓扑要求均为 false。驱动还核对实际配置 SHA，以及 enable_runtime_scan_parallelism 和 enable_dynamic_scan_parallelism 均显式关闭。其他配置使用该策略必须拒绝。

对照仍必须完整读取结果，并证明真实资源窗口中的正向创建与后续归零，以及共享池恢复基线。能从实际 info RPC 取得 FINISHED 才记录该终态。NO_SUCH_INSTANCE 应当记为 state=null 和 terminalStateObserved=false，不能推断为成功终态。

构造算例：对照完整输出 257 行且全部资源结清，但统计缓存没有保留，可以形成受限对照回执。它仍不能作为“257 行查询的全部冻结统计已验证”的证据。

数据来源：[统计可观测契约](../../../../research/query-intra-parallelism-e2e-acceptance/static-scan/runtime-sql-examples/matrix/statistics-observability.json)。

## 普通 SQL 与完整结果比较

正常场景先保存两侧完整有类型结果，再运行独立比较器。下面依次执行 grouped、有序、LIMIT、调度动作及 Filter/Project 聚合。

~~~bash
for item in R7-grouped R2-primary R3-primary R1-primary R7-filter-project; do
  for side in candidate control; do
    "$IOTDB_RUNTIME_JAVA" "-Xlog:class+load=info:file=$IOTDB_RUNTIME_CLI/$item-$side.classload.log" \
      -cp "$IOTDB_RUNTIME_CP" StaticScanAcceptanceDriver \
      --mode ordinary-sql --params "$IOTDB_RUNTIME_RUN/params/$item-$side.json" \
      > "$IOTDB_RUNTIME_RUN/$item-$side.stdout" 2> "$IOTDB_RUNTIME_RUN/$item-$side.stderr"
  done
done
export IOTDB_RUNTIME_FIXTURE_SHA="$(cat "$IOTDB_RUNTIME_FIXTURE/manifest.actual.sha256")"
comparator=research/query-intra-parallelism-e2e-acceptance/scripts/run_multisource_hash_groupby_e2e.py
for spec in 'R7 grouped grouped' 'R2 primary ordered' 'R3 primary ordered' \
  'R1 primary fullScanWindowFiltered' 'R7 filter-project filterProjectGrouped'; do
  read -r case_id variant oracle <<< "$spec"
  extra=""
  if [[ "$case_id" == R3 ]]; then extra="--oracle-limit 257"; fi
  python3 "$comparator" --offline-typed-compare \
    --candidate-result-dir "$IOTDB_RUNTIME_OUTPUT/$case_id/$variant/candidate/r001" \
    --control-result-dir "$IOTDB_RUNTIME_OUTPUT/$case_id/$variant/control/r001" \
    --fixture-manifest "$IOTDB_RUNTIME_FIXTURE/manifest.json" \
    --expected-fixture-sha256 "$IOTDB_RUNTIME_FIXTURE_SHA" \
    --oracle "$oracle" $extra --sort-memory-mib 64 --repository-root "$IOTDB_RUNTIME_REPO" \
    --output "$IOTDB_RUNTIME_OUTPUT/$case_id/$variant/comparison/r001"
done
~~~

比较器核完整读取回执、schema、结果 SHA 和有类型编码，再用有界外排序逐项核对三方多重集与重复次数。TIMESTAMP 使用毫秒整数，INT64 保留整数精度，SQL NULL 使用 JSON null。grouped 的列类型必须为 INT64、INT64、INT64、DOUBLE、DOUBLE、INT64、INT64；Filter/Project 聚合必须为 INT64、INT64、DOUBLE。SUM 的 DOUBLE 类型属于真实表模型合同，不能把旧 INT64 期望改成容差比较、字符串比较或隐式类型转换。构造算例：SQL 返回 DOUBLE 的 3.0 时，INT64 的 3 不能作为同类型预期。

R2、R3 的主场景还核对完整 device_id、time 顺序。

构造算例：[1, 1, 2] 与 [1, 2, 2] 的去重集合相同，但多重集不同，验收必须失败。R3 先核完整 4096 行 oracle 的 SHA 与行数，再取前 257 行比较，不能跳过完整 oracle 身份。

R1 主 SQL 保留 CASE、SIN/COS 残余过滤，使全部 9404416 行进入完整扫描，只输出 time 位于 [0,64) 的 4096 行。SQL 没有 ORDER BY，oracle 虽按 device_id、time 存储，比较只检查完整有类型多重集。该场景不得加 LIMIT、指定 tie 比较或抽取顶层短时间谓词。

R1 还必须保存实际 Filter 在 DeviceTableScan 之上的形态、四个分区的扫描分配，以及没有查询导出短时间范围的证据。若冻结统计支持精确源输出计数，按真实唯一源归并后的总数必须为 9404416。数学等价论证和静态优化规则检查不能替代这些服务证据。

R1 必须出现同一组实际 GROW 后 SHRINK，且固定 lane 校验值不变。R2 必须实际使用 ORDERED、多 lane，且初始和最大活动 worker 均为一。R7 必须实际观察源 FI、哈希 sink、桶 FI 和互认交换边，Filter/Project 的实际形态也必须留证。

R3 若合法回退为 SERIAL，可以通过 LIMIT 结果和清理检查，但运行期 early-stop 必须标记 NOT_EXERCISED。必须另有原生运行组提前结束场景，才能补上覆盖。R1 未触发目标动作时应当保留失败或未覆盖证据，不能借另一条 SQL 的轨迹替代。

R2 的 time-only 扩展覆盖跨设备时间键。两侧必须完整输出 4096 行；时间键单调，每个时间值有 64 个设备行。相同时间内的设备顺序可以不同，每个 tie 组的有类型多重集必须与 oracle 相同。构造算例：time=0 的 64 行可以交换设备顺序，但把 time=1 的一行插入 time=0 组必须失败。该扩展禁止 LIMIT 和 --oracle-limit。

~~~bash
for side in candidate control; do
  "$IOTDB_RUNTIME_JAVA" "-Xlog:class+load=info:file=$IOTDB_RUNTIME_CLI/R2-time-only-$side.classload.log" \
    -cp "$IOTDB_RUNTIME_CP" StaticScanAcceptanceDriver \
    --mode ordinary-sql --params "$IOTDB_RUNTIME_RUN/params/R2-time-only-$side.json" \
    > "$IOTDB_RUNTIME_RUN/R2-time-only-$side.stdout" 2> "$IOTDB_RUNTIME_RUN/R2-time-only-$side.stderr"
done
python3 "$comparator" --offline-typed-compare --oracle ordered --ordered-key-columns time \
  --candidate-result-dir "$IOTDB_RUNTIME_OUTPUT/R2/time-only/candidate/r001" \
  --control-result-dir "$IOTDB_RUNTIME_OUTPUT/R2/time-only/control/r001" \
  --fixture-manifest "$IOTDB_RUNTIME_FIXTURE/manifest.json" \
  --expected-fixture-sha256 "$IOTDB_RUNTIME_FIXTURE_SHA" --sort-memory-mib 64 \
  --repository-root "$IOTDB_RUNTIME_REPO" --output "$IOTDB_RUNTIME_OUTPUT/R2/time-only/comparison/r001"
~~~

raw-high-output 与 trig-backup 只检查高输出兼容性和背压。每个变体有独立 SQL SHA、queryId 和 FI；两侧都运行该变体，再按 rawFiltered 的 4750916 行完整 oracle 比较。参数 requireGrowShrink 为 false，不能用这些变体替代 R1 主 SQL 的真实扩缩容。

~~~bash
for variant in raw-high-output trig-backup; do
  for side in candidate control; do
    "$IOTDB_RUNTIME_JAVA" "-Xlog:class+load=info:file=$IOTDB_RUNTIME_CLI/R1-$variant-$side.classload.log" \
      -cp "$IOTDB_RUNTIME_CP" StaticScanAcceptanceDriver \
      --mode ordinary-sql --params "$IOTDB_RUNTIME_RUN/params/R1-$variant-$side.json" \
      > "$IOTDB_RUNTIME_RUN/R1-$variant-$side.stdout" 2> "$IOTDB_RUNTIME_RUN/R1-$variant-$side.stderr"
  done
  python3 "$comparator" --offline-typed-compare --oracle rawFiltered \
    --candidate-result-dir "$IOTDB_RUNTIME_OUTPUT/R1/$variant/candidate/r001" \
    --control-result-dir "$IOTDB_RUNTIME_OUTPUT/R1/$variant/control/r001" \
    --fixture-manifest "$IOTDB_RUNTIME_FIXTURE/manifest.json" \
    --expected-fixture-sha256 "$IOTDB_RUNTIME_FIXTURE_SHA" --sort-memory-mib 64 \
    --repository-root "$IOTDB_RUNTIME_REPO" --output "$IOTDB_RUNTIME_OUTPUT/R1/$variant/comparison/r001"
done
~~~

数据来源：[场景专用判据](../../../../research/query-intra-parallelism-e2e-acceptance/static-scan/runtime-sql-examples/matrix/case-manifest.json)、[有类型比较器](../../../../research/query-intra-parallelism-e2e-acceptance/scripts/run_multisource_hash_groupby_e2e.py)。

## 原生运行组的 LIMIT 提前结束

R3 的 runtime-constant 扩展投影 INT64 常量 1，保留读取 s1 的 SIN/COS 过滤，再全局 LIMIT 257。标准夹具有 9404416 个满足过滤条件的行，因此该 SQL 的完整结果恰为 257 个有类型的 [1]。任意合法选行和行顺序都得到同一完整多重集，无需定义无序 LIMIT 的真值前缀。

数学等价论证只建立结果合同。源 FI 必须实际运行原生扫描组，唯一源输出行数必须满足 0 < rows < 9404416，才证明运行期提前停止。原始 planNodeId 的聚合统计别名可能重复出现，计数只使用冻结 operatorStatisticsMap 中 `__pipeline_*` 的 TableScanOperator 实例键。

~~~bash
for side in candidate control; do
  "$IOTDB_RUNTIME_JAVA" "-Xlog:class+load=info:file=$IOTDB_RUNTIME_CLI/R3-runtime-constant-$side.classload.log" \
    -cp "$IOTDB_RUNTIME_CP" StaticScanAcceptanceDriver \
    --mode ordinary-sql --params "$IOTDB_RUNTIME_RUN/params/R3-runtime-constant-$side.json" \
    > "$IOTDB_RUNTIME_RUN/R3-runtime-constant-$side.stdout" \
    2> "$IOTDB_RUNTIME_RUN/R3-runtime-constant-$side.stderr"
done
python3 "$comparator" --offline-typed-compare --oracle runtimeEarlyLimitConstant \
  --candidate-result-dir "$IOTDB_RUNTIME_OUTPUT/R3/runtime-constant/candidate/r001" \
  --control-result-dir "$IOTDB_RUNTIME_OUTPUT/R3/runtime-constant/control/r001" \
  --fixture-manifest "$IOTDB_RUNTIME_FIXTURE/manifest.json" \
  --expected-fixture-sha256 "$IOTDB_RUNTIME_FIXTURE_SHA" --sort-memory-mib 64 \
  --repository-root "$IOTDB_RUNTIME_REPO" \
  --output "$IOTDB_RUNTIME_OUTPUT/R3/runtime-constant/comparison/r001"
python3 - <<'PYEARLYSTOP'
import hashlib, json, os
from pathlib import Path
root = Path(os.environ["IOTDB_RUNTIME_OUTPUT"]) / "R3/runtime-constant/candidate/r001"
read = lambda name: json.loads((root / name).read_text())
proof = read("ordinary-sql-receipt.json")
assert proof["fullResultDrained"] and proof["rows"] == 257 and proof["runtimeGroups"] > 0
assert proof["auditWindowsClosed"] and proof["clientManagerClosed"]
roster = read("roster.json")
sources = {fi["fragmentInstanceId"]: fi for fi in roster["fragmentInstances"] if fi["containsDeviceScan"]}
assert len({fi["dataNodeId"] for fi in sources.values()}) == 2
groups = read("runtime-actions-observed.json")
assert any(g["groupId"].split("/scan/", 1)[0] in sources and g["initialActivatedWorkers"] > 0
           for g in groups)
rows, keys = 0, set()
for terminal in read("terminal-statistics-receipt.json"):
    fid = terminal["fragmentInstanceId"]
    assert terminal["frozenSnapshotVerified"] and terminal["checksumRecomputedAndVerified"]
    if fid not in sources:
        continue
    path = root / "statistics" / (fid + ".thrift.json")
    assert hashlib.sha256(path.read_bytes()).hexdigest() == terminal["jsonSha256"]
    raw = json.loads(path.read_text())
    assert raw["25"]["tf"] == 1 and raw["24"]["str"] == terminal["snapshotChecksum"]
    for key, op in raw["7"]["map"][3].items():
        if key.startswith("__pipeline_") and op.get("2", {}).get("str") == "TableScanOperator":
            identity = (fid, key)
            assert identity not in keys
            keys.add(identity)
            rows += op["7"]["i64"]
fixture = json.loads((Path(os.environ["IOTDB_RUNTIME_FIXTURE"]) / "manifest.json").read_text())
assert keys and 0 < rows < fixture["totalRows"]
print("Real runtime scan group and early source closeout verified:", rows, "source rows")
PYEARLYSTOP
~~~

该比较禁止 `--oracle-limit` 和 `--ordered-key-columns`。原有序 LIMIT 与 filter-before-limit 合法回退 SERIAL 的原件保留；它们建立兼容性，运行期提前结束仍标记 NOT_EXERCISED。首份常量查询在修复前发行包上因 planned INT64 与 native INT32 不同而失败，不能挪用修复后 SQL 的证据覆盖该失败。

数据来源：[常量 LIMIT 合同](../../../../research/query-intra-parallelism-e2e-acceptance/static-scan/runtime-sql-examples/matrix/sql/runtimeEarlyLimitConstant.contract.json)、[场景清单](../../../../research/query-intra-parallelism-e2e-acceptance/static-scan/runtime-sql-examples/matrix/case-manifest.json)。

## 取消、截止时间与运行异常

R4、R5、R6 分别运行候选参数。只有预期服务错误码、真实 runtime group 活动、真实片段终态和全部资源结算共同成立，驱动才返回成功。

~~~bash
for item in R4-primary R5-primary R6-primary; do
  "$IOTDB_RUNTIME_JAVA" "-Xlog:class+load=info:file=$IOTDB_RUNTIME_CLI/$item-candidate.classload.log" \
    -cp "$IOTDB_RUNTIME_CP" StaticScanAcceptanceDriver \
    --mode ordinary-sql --params "$IOTDB_RUNTIME_RUN/params/$item-candidate.json" \
    > "$IOTDB_RUNTIME_RUN/$item-candidate.stdout" 2> "$IOTDB_RUNTIME_RUN/$item-candidate.stderr"
done
~~~

R4 必须在真实活动后，由独立连接向该查询实际内部 ID 发出 KILL QUERY，实际 SQL 错误对应 715。R5 必须保留正值原始截止时间并实际到达，实际 SQL 错误对应 720。R5 的正式参数使用全扫描低输出 SQL、timeout1秒且不在首行暂停，确保 RPC 内直接观察权威720；暂停后再取已移除 query 的715不算超时验收。R6 使用运行期 CAST 将实际 device_id 转成 INT64，并取得非零 JDBC code 701；仅在 d063 最后分区尾窗 [1815448512,1815448576) 触发，CASE 保护与 SIN/COS 残余保证真实扫描先发生。设备标识 d000 等不可转换；SIN/COS 残余过滤仍读取真实 s1。必须同时证明本次查询已有真实运行组活动及 FAILED FI，701 这个名字不能区分分析失败和执行失败。原除零表达式返回 NULL，不再用作异常触发器。

错误匹配优先使用非零 JDBC vendor code，异常链出现冲突码即拒绝。冻结 JDBC 的旧包装可能丢失该字段；只有精确 java.sql.SQLException 类型、raw code 为零、SQLState 与 cause 均为空、栈顶为真实 IoTDBJDBCResultSet.next，且消息开头精确符合 715、720 或 751 加冒号、空格、非空 payload 时，才允许受限兼容识别。非零字段不会被消息覆盖，任意诊断文本也不能触发该兼容路径。

回执保存 raw、parsed、effective code 及来源 provenance。这里验证的是受限旧 JDBC 包装来源，客户端没有保存该次真实 RPC 的原始 TSStatus；离线包装自测不能补成真实 RPC 状态证据。

构造算例：客户端暂停 1500 ms 且超时为一秒，只说明暂停跨过一秒。还需要真实片段截止时间、SQL 错误和终态，才能证明服务超时。

R3 两个小结果 control 参数使用 fetchSize=1，避免首批取完257行后 server query 移出名单才启动观察；仍须完整读取全部257行。快 CAST 失败未捕获真实 roster 的原件只作诊断。

分析报错、提前失败、客户端断开和关闭异常均不能替代运行异常。部分结果只能称为已捕获前缀；专用前缀 membership 验证器尚未交付。这三类场景不能套用完整结果多重集相等的结论。

数据来源：[结束路径参数与判据](../../../../research/query-intra-parallelism-e2e-acceptance/static-scan/runtime-sql-examples/matrix/case-manifest.json)。

## 元数据与有状态函数兼容性

SHOW DEVICES 使用普通 TABLE CLI，不能交给只接受 SELECT 或 WITH 的验收模式。必须捕获两侧完整输出，核对 64 个唯一设备和单个过滤设备。

~~~bash
mkdir "$IOTDB_RUNTIME_RUN/compat-show"
for side in candidate control; do
  if [[ "$side" == candidate ]]; then port=46667; else port=56667; fi
  for name in compat-show-devices compat-show-devices-filtered; do
    sql="$(cat "$IOTDB_RUNTIME_MATRIX/sql/$name.sql")"
    bash "$IOTDB_RUNTIME_DIST/sbin/start-cli.sh" \
      -h 127.0.0.1 -p "$port" -u root -pw "$IOTDB_ACCEPTANCE_PASSWORD" \
      -sql_dialect table -e "$sql" \
      > "$IOTDB_RUNTIME_RUN/compat-show/$side-$name.stdout" \
      2> "$IOTDB_RUNTIME_RUN/compat-show/$side-$name.stderr"
  done
done
python3 - <<'PYSHOW'
import json, os
from pathlib import Path
run = Path(os.environ["IOTDB_RUNTIME_RUN"]) / "compat-show"
checks = json.loads((Path(os.environ["IOTDB_RUNTIME_MATRIX"]) / "compatibility/checks.json").read_text())
expected = checks["checks"][0]["expected"]
for side in ("candidate", "control"):
    for name, values in (
        ("compat-show-devices", expected["allDeviceIds"]),
        ("compat-show-devices-filtered", [row[0] for row in expected["filteredRows"]]),
    ):
        text = (run / (side + "-" + name + ".stdout")).read_text()
        rows = [[cell.strip() for cell in line.strip().split("|")[1:-1]]
                for line in text.splitlines() if line.strip().startswith("|")]
        assert rows and rows[0] == expected["columns"], rows[:1]
        assert sorted(rows[1:]) == sorted([[value] for value in values]), (side, name)
print("Both complete SHOW DEVICES tables match")
PYSHOW
for side in candidate control; do
  "$IOTDB_RUNTIME_JAVA" -cp "$IOTDB_RUNTIME_CP" StaticScanAcceptanceDriver \
    --mode ordinary-sql --params "$IOTDB_RUNTIME_RUN/params/COMPAT-DIFF-primary-$side.json" \
    > "$IOTDB_RUNTIME_RUN/COMPAT-DIFF-$side.stdout" 2> "$IOTDB_RUNTIME_RUN/COMPAT-DIFF-$side.stderr"
done
python3 - <<'PYDIFF'
import hashlib, json, os
from pathlib import Path
matrix = Path(os.environ["IOTDB_RUNTIME_MATRIX"])
out = Path(os.environ["IOTDB_RUNTIME_OUTPUT"]) / "COMPAT-DIFF/primary"
checks = json.loads((matrix / "compatibility/checks.json").read_text())
expected = checks["checks"][1]["expected"]
for side in ("candidate", "control"):
    result = out / side / "r001"
    content = (result / "result.rows.jsonl").read_bytes()
    receipt = json.loads((result / "result-receipt.json").read_text())
    schema = json.loads((result / "result.schema.json").read_text())
    assert receipt["status"] == "FULL_TYPED_RESULT_DRAINED" and receipt["sequencePreserved"]
    assert hashlib.sha256(content).hexdigest() == receipt["sha256"]
    assert schema["sqlSha256"] == receipt["sqlSha256"]
    assert [column["type"] for column in schema["columns"]] == expected["columnTypes"]
    assert [json.loads(line) for line in content.splitlines()] == expected["rows"]
    assert receipt["rows"] == len(expected["rows"])
print("Both complete DIFF sequences match the typed 64-row expected table")
PYDIFF
~~~

DIFF 第一行差值必须为空，后续 63 行必须为 1.0，时间顺序必须保持。构造算例：第一行若为 1.0，虽然后续差值正确，也必须失败。这两项建立兼容性，不能建立调度覆盖。

数据来源：[兼容性期望表](../../../../research/query-intra-parallelism-e2e-acceptance/static-scan/runtime-sql-examples/matrix/compatibility/checks.json)、[DIFF SQL](../../../../research/query-intra-parallelism-e2e-acceptance/static-scan/runtime-sql-examples/matrix/sql/compat-diff.sql)。

## 验收记录与停止恢复

每次运行必须保留源码提交、入口 SHA、Java、依赖锁、发行 JAR、客户端 classpath、部署配置和加载来源。SQL 证据还包括输入与实际执行 SQL 的各自 SHA、标记、真实 queryId、DataNode 和 FI、布局、动作、终态可观测性和资源回收。候选必须有完整冻结终态，对照按专用可观测契约记录。

每场景分别记录 PASS、FAIL、NOT_EXERCISED 或 INCOMPLETE。正常场景驱动回执中的 oracleCompared 为 false，必须另附比较器成功回执。不能直接改字段代替比较。

重跑必须新建输出目录并保存上次原件。每个场景的计划重复次数见场景清单，参数 out 的 r001 应当显式改成 r002 等新 ID。性能实验应当另行设计预热、重复和统计，不能把这些正确性运行的单次耗时当作加速倍率。

~~~bash
bash "$IOTDB_RUNTIME_LAUNCHER" --mode stop --root "$IOTDB_RUNTIME_CLUSTER" --deployment both
bash "$IOTDB_RUNTIME_LAUNCHER" --mode status --root "$IOTDB_RUNTIME_CLUSTER" --deployment both \
  > "$IOTDB_RUNTIME_RUN/cluster-stopped-status.txt"
unset IOTDB_ACCEPTANCE_PASSWORD
tar -C "$(dirname "$IOTDB_RUNTIME_RUN")" -czf "$IOTDB_RUNTIME_RUN.tar.gz" "$(basename "$IOTDB_RUNTIME_RUN")"
sha256sum "$IOTDB_RUNTIME_RUN.tar.gz" > "$IOTDB_RUNTIME_RUN.tar.gz.sha256"
~~~

隔离部署停止后，主机原有服务及配置应当保持原状。配置身份已经冻结，不能直接改 runtime 配置中的 DOP 再重启。需要固定 DOP 对照时必须用新目录 prepare 对应 profile，并重新加载同一夹具。

本文不代填目标主机服务结论，最终数据以进度记录和实际回执为准。归档复制到独立持久存储并核对摘要后，才可以清理运行目录。

数据来源：[实现进度与验证记录](实现进度与验证记录_20261115.md)、[场景清单](../../../../research/query-intra-parallelism-e2e-acceptance/static-scan/runtime-sql-examples/matrix/case-manifest.json)。
