#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

# Linux-only candidate/control 1 ConfigNode + 2 DataNode harness for N x P GROUP BY correctness.
# It never edits DIST, and stop only addresses a PID manifest below ROOT after /proc validation.
set -Eeuo pipefail
IFS=$'\n\t'

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly REPO_ROOT="$(cd -- "$SCRIPT_DIR/../../.." && pwd -P)"
readonly MANIFEST='isolation-manifest.env'
readonly CN_CLASS='org.apache.iotdb.confignode.service.ConfigNode'
readonly DN_CLASS='org.apache.iotdb.db.service.DataNode'
# This harness exercises the guarded N x P hash GROUP BY implementation rather than merely
# creating an arbitrary table-model cluster.  A source checkout can be newer than a stale
# all-bin directory, so validate the deployed class as well as the presence of runtime jars.
readonly TABLE_DISTRIBUTED_PLANNER_CLASS='org/apache/iotdb/db/queryengine/plan/relational/planner/distribute/TableDistributedPlanner.class'
readonly HASH_CHANNEL_GUARD_MARKER='hashChannelIndex'
MODE='' ROOT='' DIST='' DEPLOYMENT=both TIMEOUT=120 PORT_OFFSET=0
CANDIDATE_PROFILE=legacy CONTROL_PROFILE=legacy
JAVA_EXE='' DN_XMS=256m DN_XMX=512m CN_XMS=256m CN_XMX=512m
JMX_ENABLED='' QUERY_THREAD_COUNT=0 TIME_PARTITION_INTERVAL_MS=604800000
declare -A ROLE_OVERRIDES=()
readonly RUNTIME_CLASS='org/apache/iotdb/db/queryengine/execution/runtime/RuntimeFragmentLifecycle.class'
readonly MBEAN_CLASS='org/apache/iotdb/db/queryengine/execution/exchange/MPPDataExchangeServiceMBean.class'

usage() {
  printf '%s\n' 'Usage: launch-isolated-nxp.sh --mode prepare|start|stop|status|start-datanodes|stop-datanodes --root ROOT [--distribution-root DIST] [--deployment candidate|control|both] [--port-offset N]'
  printf '%s\n' 'Prepare: --candidate-profile/--control-profile legacy|runtime|fixed-one|fixed-eight|hash-disabled; --java ABS; --datanode-xms/xmx SIZE --confignode-xms/xmx SIZE; --query-thread-count N; --jmx-enabled true|false'
  printf '%s\n' 'Per-side: --candidate- or --control- followed by runtime-enabled|estimation-enabled|logical-lane-ceiling|property-planning|hash-repartition|ordered-parallel|timepartition-morsel|fixed-workers VALUE'
}
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
need() { [[ -n "${2:-}" ]] || die "Missing value for $1"; }
while (($#)); do
  case "$1" in
    --mode|--root|--distribution-root|--deployment|--startup-timeout-seconds|--port-offset|--candidate-profile|--control-profile|--java|--datanode-xms|--datanode-xmx|--confignode-xms|--confignode-xmx|--query-thread-count|--jmx-enabled|--time-partition-interval-ms)
      need "$1" "${2:-}"
      case "$1" in
        --mode) MODE="$2";; --root) ROOT="$2";; --distribution-root) DIST="$2";;
        --deployment) DEPLOYMENT="$2";; --startup-timeout-seconds) TIMEOUT="$2";;
        --port-offset) PORT_OFFSET="$2";; --candidate-profile) CANDIDATE_PROFILE="$2";;
        --control-profile) CONTROL_PROFILE="$2";; --java) JAVA_EXE="$2";;
        --datanode-xms) DN_XMS="$2";; --datanode-xmx) DN_XMX="$2";;
        --confignode-xms) CN_XMS="$2";; --confignode-xmx) CN_XMX="$2";;
        --query-thread-count) QUERY_THREAD_COUNT="$2";; --jmx-enabled) JMX_ENABLED="$2";;
        --time-partition-interval-ms) TIME_PARTITION_INTERVAL_MS="$2";;
      esac
      shift 2 ;;
    --candidate-*|--control-*)
      need "$1" "${2:-}"
      name="${1#--}"; suffix="${name#*-}"
      case "$suffix" in
        runtime-enabled|estimation-enabled|logical-lane-ceiling|fixed-workers|property-planning|hash-repartition|ordered-parallel|timepartition-morsel) ROLE_OVERRIDES["$name"]="$2";;
        *) die "Unknown argument: $1";;
      esac
      shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown argument: $1" ;;
  esac
done
[[ "$MODE" =~ ^(prepare|start|stop|status|start-datanodes|stop-datanodes)$ ]] || die 'Invalid --mode'
[[ -n "$ROOT" ]] || die '--root is required'
[[ "$DEPLOYMENT" =~ ^(candidate|control|both)$ ]] || die 'Invalid --deployment'
[[ "$TIMEOUT" =~ ^[1-9][0-9]*$ ]] || die '--startup-timeout-seconds must be positive'
[[ "$PORT_OFFSET" =~ ^(0|[1-9][0-9]*)$ ]] && ((PORT_OFFSET <= 26000)) || die '--port-offset must be 0..26000'
for profile in "$CANDIDATE_PROFILE" "$CONTROL_PROFILE"; do
  [[ "$profile" =~ ^(legacy|runtime|fixed-one|fixed-eight|hash-disabled)$ ]] || die "Unsupported profile: $profile"
done
[[ "$QUERY_THREAD_COUNT" =~ ^(0|[1-9][0-9]*)$ ]] && ((QUERY_THREAD_COUNT <= 1024)) || die '--query-thread-count must be 0..1024'
[[ "$TIME_PARTITION_INTERVAL_MS" =~ ^[1-9][0-9]*$ ]] || die 'Time partition interval must be positive'
for heap in "$DN_XMS" "$DN_XMX" "$CN_XMS" "$CN_XMX"; do
  [[ "$heap" =~ ^[1-9][0-9]*[mMgG]$ ]] || die "Heap must use positive MiB/GiB suffix: $heap"
done
heap_bytes() {
  local value="$1" number="${1%[mMgG]}" factor=1048576
  [[ "$value" != *[gG] ]] || factor=1073741824
  printf '%s' "$((number * factor))"
}
(( $(heap_bytes "$DN_XMS") <= $(heap_bytes "$DN_XMX") )) || die 'DataNode Xms exceeds Xmx'
(( $(heap_bytes "$CN_XMS") <= $(heap_bytes "$CN_XMX") )) || die 'ConfigNode Xms exceeds Xmx'
for name in "${!ROLE_OVERRIDES[@]}"; do
  value="${ROLE_OVERRIDES[$name]}"
  if [[ "$name" == *-logical-lane-ceiling ]]; then
    [[ "$value" =~ ^[1-9][0-9]*$ ]] && ((value <= 1024)) || die "Invalid logical lane ceiling: $value"
  elif [[ "$name" == *-fixed-workers ]]; then
    [[ "$value" =~ ^(0|[1-9][0-9]*)$ ]] && ((value <= 1024)) || die "Invalid fixed worker count: $value"
  else
    [[ "$value" =~ ^(true|false)$ ]] || die "Expected true|false for $name"
  fi
done
ROOT="$(realpath -m -- "$ROOT")"; [[ -z "$DIST" ]] || DIST="$(realpath -m -- "$DIST")"
[[ "$ROOT" != / && "$ROOT" != /root && "$ROOT" != /root/rivermind-data ]] || die 'Unsafe isolation root'
[[ -z "$DIST" || "$ROOT" != "$DIST" ]] || die 'Isolation root cannot equal distribution'
if [[ -n "$DIST" ]]; then case "$ROOT/" in "$DIST/"*) die 'Isolation root cannot be inside distribution';; esac; fi
role_value() {
  local d="$1" key="$2" profile="$CANDIDATE_PROFILE" value=false
  [[ "$d" == control ]] && profile="$CONTROL_PROFILE"
  case "$key" in
    fixed-workers) value=0;;
    logical-lane-ceiling) case "$profile" in runtime|fixed-eight) value=8;; fixed-one|hash-disabled) value=1;; *) value=4;; esac;;
    runtime-enabled|estimation-enabled) [[ "$profile" == runtime ]] && value=true;;
    property-planning|hash-repartition)
      if [[ "$profile" == legacy ]]; then [[ "$d" == candidate ]] && value=true
      elif [[ "$key" == property-planning || "$profile" != hash-disabled ]]; then value=true; fi;;
    ordered-parallel|timepartition-morsel) [[ "$profile" != legacy ]] && value=true;;
    *) die "Unknown role field: $key";;
  esac
  printf '%s' "${ROLE_OVERRIDES[$d-$key]:-$value}"
}
if [[ -z "$JMX_ENABLED" ]]; then
  JMX_ENABLED=false
  [[ "$CANDIDATE_PROFILE" == legacy && "$CONTROL_PROFILE" == legacy
      && "$(role_value candidate runtime-enabled)" == false
      && "$(role_value control runtime-enabled)" == false ]] || JMX_ENABLED=true
fi
[[ "$JMX_ENABLED" =~ ^(true|false)$ ]] || die '--jmx-enabled must be true|false'
jmx_port() {
  case "$1/$2" in
    candidate/confignode) printf '%s' "$((27911 + PORT_OFFSET))";;
    candidate/datanode-1) printf '%s' "$((27912 + PORT_OFFSET))";;
    candidate/datanode-2) printf '%s' "$((28912 + PORT_OFFSET))";;
    control/confignode) printf '%s' "$((37911 + PORT_OFFSET))";;
    control/datanode-1) printf '%s' "$((37912 + PORT_OFFSET))";;
    control/datanode-2) printf '%s' "$((38912 + PORT_OFFSET))";;
    *) die 'Unknown node for JMX';;
  esac
}
resolve_java() {
  [[ -n "$JAVA_EXE" ]] || JAVA_EXE="$(command -v java || true)"
  [[ "$JAVA_EXE" == /* && -x "$JAVA_EXE" ]] || die '--java must resolve to an executable absolute path'
  JAVA_EXE="$(realpath -- "$JAVA_EXE")"
}

# deployment|name|role|rpc|internal|consensus|mpp|schema|data-consensus|metric
nodes() {
  printf 'candidate|confignode|ConfigNode|-|%s|%s|-|-|-|%s\n' "$((27110 + PORT_OFFSET))" "$((27120 + PORT_OFFSET))" "$((27901 + PORT_OFFSET))"
  printf 'candidate|datanode-1|DataNode|%s|%s|-|%s|%s|%s|%s\n' "$((27667 + PORT_OFFSET))" "$((27130 + PORT_OFFSET))" "$((27140 + PORT_OFFSET))" "$((27150 + PORT_OFFSET))" "$((27160 + PORT_OFFSET))" "$((27902 + PORT_OFFSET))"
  printf 'candidate|datanode-2|DataNode|%s|%s|-|%s|%s|%s|%s\n' "$((28667 + PORT_OFFSET))" "$((28130 + PORT_OFFSET))" "$((28140 + PORT_OFFSET))" "$((28150 + PORT_OFFSET))" "$((28160 + PORT_OFFSET))" "$((28902 + PORT_OFFSET))"
  printf 'control|confignode|ConfigNode|-|%s|%s|-|-|-|%s\n' "$((37110 + PORT_OFFSET))" "$((37120 + PORT_OFFSET))" "$((37901 + PORT_OFFSET))"
  printf 'control|datanode-1|DataNode|%s|%s|-|%s|%s|%s|%s\n' "$((37667 + PORT_OFFSET))" "$((37130 + PORT_OFFSET))" "$((37140 + PORT_OFFSET))" "$((37150 + PORT_OFFSET))" "$((37160 + PORT_OFFSET))" "$((37902 + PORT_OFFSET))"
  printf 'control|datanode-2|DataNode|%s|%s|-|%s|%s|%s|%s\n' "$((38667 + PORT_OFFSET))" "$((38130 + PORT_OFFSET))" "$((38140 + PORT_OFFSET))" "$((38150 + PORT_OFFSET))" "$((38160 + PORT_OFFSET))" "$((38902 + PORT_OFFSET))"
}
selected() { [[ "$DEPLOYMENT" == both ]] && nodes || nodes | awk -F'|' -v d="$DEPLOYMENT" '$1 == d'; }
cn_internal() { [[ "$1" == candidate ]] && printf '%s' "$((27110 + PORT_OFFSET))" || printf '%s' "$((37110 + PORT_OFFSET))"; }
cn_consensus() { [[ "$1" == candidate ]] && printf '%s' "$((27120 + PORT_OFFSET))" || printf '%s' "$((37120 + PORT_OFFSET))"; }

assert_dist() {
  [[ -d "$1/lib" && -f "$1/conf/iotdb-system.properties" && -f "$1/conf/logback-confignode.xml" && -f "$1/conf/logback-datanode.xml" ]] || die "Not an all-bin distribution: $1"
  compgen -G "$1/lib/*.jar" >/dev/null || die "No runtime jars in $1/lib"
  assert_hash_channel_guard "$1"
}
server_jar() {
  local dist="$1"
  local jar="$dist/lib/iotdb-server-2.0.11-SNAPSHOT.jar"
  [[ -f "$jar" ]] || die "Missing table planner runtime jar: $jar"
  printf '%s' "$jar"
}
assert_hash_channel_guard() {
  local dist="$1" jar
  jar="$(server_jar "$dist")"
  command -v unzip >/dev/null || die 'Need unzip to verify the isolated hash GROUP BY runtime'
  # Do not accept a jar selected only by its directory or timestamp: before the channel-index
  # repair a multi-source/two-bucket plan can look valid in EXPLAIN yet duplicate one channel and
  # omit the other at execution time. The marker is the private planner method guarded by the
  # unit regression below; this harness is intentionally coupled to that experimental path.
  unzip -p "$jar" "$TABLE_DISTRIBUTED_PLANNER_CLASS" | LC_ALL=C grep -aF "$HASH_CHANNEL_GUARD_MARKER" >/dev/null || die "Distribution lacks the multi-source hash channel-index guard: $jar. Rebuild the all-bin distribution from the checked-out source before preparing or starting this harness."
}
sha256() {
  command -v sha256sum >/dev/null || die 'Need sha256sum to write the isolated runtime manifest'
  sha256sum "$1" | awk '{print $1}'
}
port_bound() {
  if command -v ss >/dev/null; then ss -H -ltn "sport = :$1" | grep -q .
  elif command -v lsof >/dev/null; then lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
  else die 'Need ss or lsof for mandatory isolated-port safety checks'; fi
}
listener_pid() {
  local port="$1" values
  if command -v ss >/dev/null; then
    values="$(ss -H -ltnp "sport = :$port" 2>/dev/null | sed -n 's/.*pid=\([0-9][0-9]*\).*/\1/p' | sort -u)"
  elif command -v lsof >/dev/null; then
    values="$(lsof -nP -t -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | sort -u)"
  else
    die 'Need ss or lsof to bind an isolated node PID to its listener'
  fi
  [[ "$(printf '%s\n' "$values" | sed '/^$/d' | wc -l | tr -d ' ')" == 1 && "$values" =~ ^[1-9][0-9]*$ ]] \
    || return 1
  printf '%s' "$values"
}
assert_ports_free() {
  local d n r rpc internal consensus mpp schema data metric p
  while IFS='|' read -r d n r rpc internal consensus mpp schema data metric; do
    for p in "$rpc" "$internal" "$consensus" "$mpp" "$schema" "$data" "$metric"; do [[ "$p" == - ]] || ! port_bound "$p" || die "Refusing occupied isolated port: $p"; done
    [[ "$JMX_ENABLED" != true ]] || ! port_bound "$(jmx_port "$d" "$n")" || die "Occupied JMX port: $d/$n"
  done < <(selected)
}
assert_datanode_ports_free() {
  local d n r rpc internal consensus mpp schema data metric p
  while IFS='|' read -r d n r rpc internal consensus mpp schema data metric; do
    [[ "$r" == DataNode ]] || continue
    for p in "$rpc" "$internal" "$mpp" "$schema" "$data" "$metric"; do
      ! port_bound "$p" || die "Refusing occupied isolated DataNode port: $p"
    done
    [[ "$JMX_ENABLED" != true ]] || ! port_bound "$(jmx_port "$d" "$n")" || die "Occupied JMX port: $d/$n"
  done < <(selected)
}
assert_runtime_distribution() {
  local jar="$(server_jar "$DIST")" side
  for side in candidate control; do
    if (( $(role_value "$side" fixed-workers) > 0 )); then
      [[ "$(role_value "$side" runtime-enabled)" == true ]] || die 'Fixed workers require runtime-enabled=true'
      unzip -p "$jar" 'org/apache/iotdb/db/conf/IoTDBConfig.class' | LC_ALL=C grep -aF 'getRuntimeScanFixedWorkers' >/dev/null || die "No fixed-worker runtime control in $jar"
    fi
  done
  if [[ "$(role_value candidate runtime-enabled)" == true || "$(role_value control runtime-enabled)" == true ]]; then
    unzip -p "$jar" "$RUNTIME_CLASS" | LC_ALL=C grep -aF 'sealConstruction' >/dev/null || die "No runtime lifecycle in $jar"
    unzip -p "$jar" "$MBEAN_CLASS" | LC_ALL=C grep -aF 'describeQueryExecutionBySqlHash' >/dev/null || die "No actual ordinary-SQL JMX identity API in $jar"
  fi
}
write_manifest() {
  local jar d key field
  jar="$(server_jar "$DIST")"
  {
    printf 'schema_version=3\npurpose=linux-isolated-1c2d-candidate-control-query-acceptance\n'
    printf 'created_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'repository_git_sha=%s\n' "$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || printf unavailable)"
    printf 'distribution_root=%q\ndeployment_root=%q\nport_offset=%s\n' "$DIST" "$ROOT" "$PORT_OFFSET"
    printf 'distribution_jar_count=%s\n' "$(find "$DIST/lib" -maxdepth 1 -name '*.jar' -type f | wc -l | tr -d ' ')"
    printf 'distribution_server_jar_sha256=%s\ndistribution_hash_channel_guard=%q\n' "$(sha256 "$jar")" "$HASH_CHANNEL_GUARD_MARKER"
    printf 'java_executable=%q\njava_sha256=%s\n' "$JAVA_EXE" "$(sha256 "$JAVA_EXE")"
    printf 'datanode_xms=%q\ndatanode_xmx=%q\nconfignode_xms=%q\nconfignode_xmx=%q\n' "$DN_XMS" "$DN_XMX" "$CN_XMS" "$CN_XMX"
    printf 'jmx_enabled=%s\nquery_thread_count=%s\ntime_partition_interval_ms=%s\n' "$JMX_ENABLED" "$QUERY_THREAD_COUNT" "$TIME_PARTITION_INTERVAL_MS"
    printf 'candidate_profile=%s\ncontrol_profile=%s\n' "$CANDIDATE_PROFILE" "$CONTROL_PROFILE"
    for d in candidate control; do
      for key in runtime-enabled estimation-enabled logical-lane-ceiling property-planning hash-repartition ordered-parallel timepartition-morsel fixed-workers; do
        field="${key//-/_}"; printf '%s_%s=%q\n' "$d" "$field" "$(role_value "$d" "$key")"
      done
    done
  } > "$ROOT/$MANIFEST"
}
read_manifest() {
  [[ -f "$ROOT/$MANIFEST" ]] || die "No $MANIFEST below $ROOT; run prepare"
  source "$ROOT/$MANIFEST"
  [[ "${deployment_root:-}" == "$ROOT" ]] || die 'Manifest root mismatch'
  DIST="$distribution_root"; PORT_OFFSET="${port_offset:-0}"
  JAVA_EXE="${java_executable:-$JAVA_EXE}"
  DN_XMS="${datanode_xms:-256m}"; DN_XMX="${datanode_xmx:-512m}"
  CN_XMS="${confignode_xms:-256m}"; CN_XMX="${confignode_xmx:-512m}"
  JMX_ENABLED="${jmx_enabled:-false}"; QUERY_THREAD_COUNT="${query_thread_count:-0}"
  TIME_PARTITION_INTERVAL_MS="${time_partition_interval_ms:-604800000}"
  CANDIDATE_PROFILE="${candidate_profile:-legacy}"; CONTROL_PROFILE="${control_profile:-legacy}"
  local d key field
  for d in candidate control; do
    for key in runtime-enabled estimation-enabled logical-lane-ceiling property-planning hash-repartition ordered-parallel timepartition-morsel fixed-workers; do
      field="$d-${key//-/_}"; field="${field//-/_}"
      [[ -z "${!field:-}" ]] || ROLE_OVERRIDES["$d-$key"]="${!field}"
    done
  done
}
write_json_manifests() {
  nodes > "$ROOT/topology.nodes.tsv"
  local candidate_values='' control_values='' key
  for key in runtime-enabled estimation-enabled logical-lane-ceiling property-planning hash-repartition ordered-parallel timepartition-morsel fixed-workers; do
    candidate_values+="$(role_value candidate "$key")|"
    control_values+="$(role_value control "$key")|"
  done
  python3 - "$ROOT" "$DIST" "$JAVA_EXE" "$DN_XMS" "$DN_XMX" "$CN_XMS" "$CN_XMX" "$JMX_ENABLED" "$QUERY_THREAD_COUNT" "$CANDIDATE_PROFILE" "$CONTROL_PROFILE" "$candidate_values" "$control_values" <<'PYJSON'
import hashlib,json,sys
from pathlib import Path
root,dist,java,dn_min,dn_max,cn_min,cn_max,jmx,threads,cp,rp,cv,rv=sys.argv[1:]
root,dist=Path(root),Path(dist)
def sha(path):
    value=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda:stream.read(1048576),b''):value.update(chunk)
    return value.hexdigest()
jar=dist/'lib/iotdb-server-2.0.11-SNAPSHOT.jar'
jars=[{'path':str(p),'sha256':sha(p),'bytes':p.stat().st_size} for p in sorted((dist/'lib').glob('*.jar'))]
keys=['runtimeEnabled','estimationEnabled','logicalLaneCeiling','propertyPlanning','hashRepartition','orderedParallel','timepartitionMorsel','fixedWorkers']
for side,profile,values in [('candidate',cp,cv),('control',rp,rv)]:
    settings=dict(zip(keys,values.rstrip('|').split('|')))
    settings={k:int(v) if k in ('logicalLaneCeiling','fixedWorkers') else v=='true' for k,v in settings.items()}
    data={'schemaVersion':1,'deployment':side,'deploymentRoot':str(root/side),
          'distributionRoot':str(dist),'profile':profile,'runtimeProfile':settings,
          'expectedDataNodeCount':2,'jmxHost':'127.0.0.1','jmxEnabled':jmx=='true',
          'javaPath':java,'javaSha256':sha(java),'serverJarSha256':sha(jar),
          'runtimeJars':jars,'queryThreadCount':int(threads) if int(threads)>0 else None,
          'nodes':[],'configNodes':[],'loadedClassIdentityStatus':'REQUIRES_POST_START_VERIFICATION'}
    for line in (root/'topology.nodes.tsv').read_text().splitlines():
        d,name,role,rpc,internal,consensus,mpp,schema,consensus_data,metric=line.split('|')
        if d!=side:continue
        node_root=root/side/name
        config=node_root/'conf/iotdb-system.properties'
        node={'name':name,'role':role,'nodeRoot':str(node_root),'dataNodeId':None,
              'nodeIdStatus':'DISCOVER_ACTUAL_ID_AFTER_START',
              'internalEndpoint':{'ip':'127.0.0.1','port':int(internal)},
              'jmxPort':int(metric)+10 if jmx=='true' else None,
              'configPath':str(config),'configSha256':sha(config),
              'xms':dn_min if role=='DataNode' else cn_min,'xmx':dn_max if role=='DataNode' else cn_max}
        if role=='DataNode':
            node['rpcEndpoint']={'ip':'127.0.0.1','port':int(rpc)}
            node['mppEndpoint']={'ip':'127.0.0.1','port':int(mpp)}
            data['nodes'].append(node)
        else:data['configNodes'].append(node)
    data['tableSessionNodeUrls']=['127.0.0.1:'+str(node['rpcEndpoint']['port']) for node in data['nodes']]
    data['jdbcUrl']='jdbc:iotdb://'+data['tableSessionNodeUrls'][0]+'/?sql_dialect=table'
    (root/side/'deployment-manifest.json').write_text(json.dumps(data,indent=2)+'\n')
PYJSON
}
assert_runtime_identity() {
  resolve_java
  [[ -z "${java_sha256:-}" || "$(sha256 "$JAVA_EXE")" == "${java_sha256:-}" ]] || die 'Java executable changed since prepare'
  [[ -z "${distribution_server_jar_sha256:-}" || "$(sha256 "$(server_jar "$DIST")")" == "${distribution_server_jar_sha256:-}" ]] || die 'Server JAR changed since prepare'
  if [[ -f "$ROOT/candidate/deployment-manifest.json" ]]; then
    python3 - "$ROOT/candidate/deployment-manifest.json" "$DIST" <<'PYIDENTITY'
import hashlib,json,sys
from pathlib import Path
manifest=json.loads(Path(sys.argv[1]).read_text())
expected=manifest['runtimeJars']
actual={str(path) for path in (Path(sys.argv[2])/'lib').glob('*.jar')}
if actual != {entry['path'] for entry in expected}:
    raise SystemExit('Runtime JAR roster changed since prepare')
for entry in expected:
    digest=hashlib.sha256()
    with Path(entry['path']).open('rb') as stream:
        for block in iter(lambda:stream.read(1048576),b''):digest.update(block)
    if digest.hexdigest()!=entry['sha256']:
        raise SystemExit('Runtime JAR identity changed: '+entry['path'])
PYIDENTITY
  fi
}
assert_config_identity() {
  local current="$1/conf/iotdb-system.properties" frozen="$1/identity/config-prepared.properties"
  [[ -f "$frozen" ]] || return 0
  [[ "$(sha256 "$current")" == "$(sha256 "$frozen")" ]] && return 0
  [[ "$(awk -F= '$1=="enable_runtime_scan_parallelism"{print $2}' "$current")" == false ]] || die "Runtime config changed: $current"
  [[ "$(awk -F= '$1=="enable_dop_estimation"{print $2}' "$current")" == false ]] || die "Estimated-DOP config changed: $current"
  local before after dop
  before="$(awk '$0 !~ /^degree_of_query_parallelism=/' "$frozen" | sha256sum | awk '{print $1}')"
  after="$(awk '$0 !~ /^degree_of_query_parallelism=/' "$current" | sha256sum | awk '{print $1}')"
  dop="$(awk -F= '$1=="degree_of_query_parallelism"{print $2}' "$current")"
  [[ "$before" == "$after" && "$dop" =~ ^[1-9][0-9]*$ ]] && ((dop<=1024)) || die "Unexpected config drift: $current"
  printf 'Accepted explicit fixed-DOP-only restart: %s dop=%s config_sha256=%s\n' "$1" "$dop" "$(sha256 "$current")"
}

write_config() {
  local d="$1" n="$2" role="$3" rpc="$4" internal="$5" consensus="$6" mpp="$7" schema="$8" data="$9" metric="${10}" root="$ROOT/$1/$2" conf="$ROOT/$1/$2/conf" enabled=false dn_rpc="$4" dn_internal="$5" dn_mpp="$7" dn_schema="$8" dn_data="$9" dn_metric="${10}"
  local runtime estimation dop property hash ordered tp profile custom_region_policy=false
  runtime="$(role_value "$d" runtime-enabled)"; estimation="$(role_value "$d" estimation-enabled)"
  dop="$(role_value "$d" logical-lane-ceiling)"; property="$(role_value "$d" property-planning)"
  hash="$(role_value "$d" hash-repartition)"; ordered="$(role_value "$d" ordered-parallel)"
  tp="$(role_value "$d" timepartition-morsel)"
  profile="$CANDIDATE_PROFILE"; [[ "$d" != control ]] || profile="$CONTROL_PROFILE"
  # The new fixture explicitly caps DataRegions at two. IoTDB permits that database
  # setting only under CUSTOM. Keep the legacy profile's original extension policy.
  [[ "$profile" == legacy && "$runtime" == false && "$estimation" == false ]] || custom_region_policy=true
  if [[ "$role" == ConfigNode ]]; then
    if [[ "$d" == candidate ]]; then dn_rpc=$((27667 + PORT_OFFSET)); dn_internal=$((27130 + PORT_OFFSET)); dn_mpp=$((27140 + PORT_OFFSET)); dn_schema=$((27150 + PORT_OFFSET)); dn_data=$((27160 + PORT_OFFSET)); dn_metric=$((27902 + PORT_OFFSET)); else dn_rpc=$((37667 + PORT_OFFSET)); dn_internal=$((37130 + PORT_OFFSET)); dn_mpp=$((37140 + PORT_OFFSET)); dn_schema=$((37150 + PORT_OFFSET)); dn_data=$((37160 + PORT_OFFSET)); dn_metric=$((37902 + PORT_OFFSET)); fi
  fi
  mkdir -p "$conf" "$root/data" "$root/logs"; cp -- "$DIST/conf/logback-confignode.xml" "$conf/"; cp -- "$DIST/conf/logback-datanode.xml" "$conf/"
  cat > "$conf/iotdb-system.properties" <<EOF
# Generated by launch-isolated-nxp.sh; never reuse outside $root.
cluster_name=isolated-nxp-$d
cn_seed_config_node=127.0.0.1:$(cn_internal "$d")
dn_seed_config_node=127.0.0.1:$(cn_internal "$d")
schema_replication_factor=1
data_replication_factor=1
degree_of_query_parallelism=$dop
enable_dop_estimation=$estimation
enable_dynamic_scan_parallelism=false
enable_runtime_scan_parallelism=$runtime
runtime_scan_parallelism_fixed_workers=$(role_value "$d" fixed-workers)
enable_property_driven_planning=$property
enable_table_group_by_hash_repartition=$hash
enable_timepartition_morsel=$tp
enable_ordered_parallel_scan=$ordered
time_partition_interval=$TIME_PARTITION_INTERVAL_MS
table_group_by_hash_repartition_partition_count=2
cn_internal_address=127.0.0.1
cn_internal_port=$(cn_internal "$d")
cn_consensus_port=$(cn_consensus "$d")
cn_system_dir=$root/data/confignode-system
cn_consensus_dir=$root/data/confignode-consensus
cn_pipe_receiver_file_dir=$root/data/confignode-pipe-receiver
dn_rpc_address=127.0.0.1
dn_rpc_port=$dn_rpc
dn_internal_address=127.0.0.1
dn_internal_port=$dn_internal
dn_mpp_data_exchange_port=$dn_mpp
dn_schema_region_consensus_port=$dn_schema
dn_data_region_consensus_port=$dn_data
dn_system_dir=$root/data/datanode-system
dn_data_dirs=$root/data/datanode-data
dn_consensus_dir=$root/data/datanode-consensus
dn_wal_dirs=$root/data/datanode-wal
dn_tracing_dir=$root/data/datanode-tracing
dn_sync_dir=$root/data/datanode-sync
dn_pipe_receiver_file_dirs=$root/data/datanode-pipe-receiver
cn_metric_reporter_list=
cn_metric_prometheus_reporter_port=$metric
dn_metric_reporter_list=
dn_metric_prometheus_reporter_port=$dn_metric
EOF
  [[ "$custom_region_policy" != true ]] || printf 'data_region_group_extension_policy=CUSTOM\n' >> "$conf/iotdb-system.properties"
  [[ "$QUERY_THREAD_COUNT" == 0 ]] || printf 'query_thread_count=%s\n' "$QUERY_THREAD_COUNT" >> "$conf/iotdb-system.properties"
  mkdir -p "$root/identity" "$root/tmp"
  cp -- "$conf/iotdb-system.properties" "$root/identity/config-prepared.properties"
}
prepare() {
  [[ -n "$DIST" ]] || die '--distribution-root is required for prepare'; assert_dist "$DIST"
  resolve_java; assert_runtime_distribution
  command -v python3 >/dev/null || die 'python3 is required for typed deployment manifests'
  [[ ! -e "$ROOT" ]] || ! find "$ROOT" -mindepth 1 -print -quit | grep -q . || die "Prepare refuses non-empty root: $ROOT"
  assert_ports_free; mkdir -p "$ROOT"
  local d n r rpc internal consensus mpp schema data metric
  while IFS='|' read -r d n r rpc internal consensus mpp schema data metric; do write_config "$d" "$n" "$r" "$rpc" "$internal" "$consensus" "$mpp" "$schema" "$data" "$metric"; done < <(nodes)
  write_manifest; write_json_manifests; printf 'Prepared %s (SHA %s)\n' "$ROOT" "$(grep '^repository_git_sha=' "$ROOT/$MANIFEST" | cut -d= -f2-)"
}
classpath() { local j; shopt -s nullglob; local jars=("$DIST"/lib/*.jar); shopt -u nullglob; (IFS=:; printf '%s' "${jars[*]}"); }
wait_port() { local p="$1" deadline=$((SECONDS + TIMEOUT)); while ((SECONDS < deadline)); do port_bound "$p" && return; sleep .5; done; return 1; }
start_node() {
  local d="$1" n="$2" role="$3" rpc="$4" internal="$5" root="$ROOT/$1/$2" conf="$ROOT/$1/$2/conf" logs="$ROOT/$1/$2/logs" data="$ROOT/$1/$2/data" class="$DN_CLASS" prefix=IOTDB log="$ROOT/$1/$2/conf/logback-datanode.xml" port="$4" pid listener cmd
  [[ "$role" == ConfigNode ]] && { class="$CN_CLASS"; prefix=CONFIGNODE; log="$conf/logback-confignode.xml"; port="$internal"; }
  local xms="$DN_XMS" xmx="$DN_XMX" jmx pid_exe
  local -a jmx_options=()
  [[ "$role" != ConfigNode ]] || { xms="$CN_XMS"; xmx="$CN_XMX"; }
  assert_config_identity "$root"
  mkdir -p "$root/tmp" "$root/identity"
  if [[ "$JMX_ENABLED" == true ]]; then
    jmx="$(jmx_port "$d" "$n")"
    jmx_options=(-Dcom.sun.management.jmxremote
      "-Dcom.sun.management.jmxremote.port=$jmx"
      "-Dcom.sun.management.jmxremote.rmi.port=$jmx"
      -Dcom.sun.management.jmxremote.host=127.0.0.1
      -Djava.rmi.server.hostname=127.0.0.1
      -Dcom.sun.management.jmxremote.local.only=true
      -Dcom.sun.management.jmxremote.authenticate=false
      -Dcom.sun.management.jmxremote.ssl=false)
  fi
  nohup "$JAVA_EXE" "${jmx_options[@]}" "-Djava.io.tmpdir=$root/tmp" "-Xlog:class+load=info:file=$root/identity/classes-loaded-%p.log:time,uptime,level,tags:filecount=0" --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED "-Dlogback.configurationFile=$log" "-D${prefix}_HOME=$DIST" "-D${prefix}_DATA_HOME=$data" "-D${prefix}_CONF=$conf" "-D${prefix}_LOGS=$logs" "-D${prefix}_LOG_DIR=$logs" "-DTSFILE_HOME=$DIST" "-DTSFILE_CONF=$conf" -Dfile.encoding=UTF-8 -Diotdb-foreground=yes "-Xms$xms" "-Xmx$xmx" -cp "$(classpath)" "$class" -s >"$logs/stdout.log" 2>"$logs/stderr.log" &
  pid=$!
  wait_port "$port" || { kill -0 "$pid" 2>/dev/null && kill "$pid" || true; die "Timed out waiting for $n on $port; inspect $logs"; }
  listener="$(listener_pid "$port")" || {
    kill -0 "$pid" 2>/dev/null && kill "$pid" || true
    die "Cannot bind $n to exactly one listener PID on port $port; inspect $logs"
  }
  cmd="$(tr '\0' ' ' < "/proc/$listener/cmdline" 2>/dev/null || true)"
  [[ "$cmd" == *"$root"* && "$cmd" == *"$class"* ]] || {
    kill -0 "$pid" 2>/dev/null && kill "$pid" || true
    die "Listener PID $listener on port $port is not isolated node $n"
  }
  pid_exe="$(readlink -f "/proc/$listener/exe")"
  [[ "$pid_exe" == "$JAVA_EXE" ]] || die "Started Java differs: $pid_exe"
  if [[ "$JMX_ENABLED" == true ]]; then
    wait_port "$jmx" || die "JMX not ready: $d/$n $jmx"
    [[ "$(listener_pid "$jmx")" == "$listener" ]] || die "JMX is not owned by isolated node $d/$n"
  fi
  {
    printf 'pid=%s\nroot=%q\nclass=%q\nstarted_at_utc=%s\n' "$listener" "$root" "$class" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'java_executable=%q\njava_sha256=%s\nxms=%s\nxmx=%s\n' "$pid_exe" "$(sha256 "$pid_exe")" "$xms" "$xmx"
    printf 'config_sha256=%s\nserver_jar_sha256=%s\njmx_enabled=%s\njmx_port=%s\n' "$(sha256 "$conf/iotdb-system.properties")" "$(sha256 "$(server_jar "$DIST")")" "$JMX_ENABLED" "${jmx:-0}"
    printf 'loaded_class_log=%q\n' "$root/identity/classes-loaded-$listener.log"
    printf 'actual_configured_dop=%s\n' "$(awk -F= '$1=="degree_of_query_parallelism"{print $2}' "$conf/iotdb-system.properties")"
  } > "$root/process.env"
  cp -- "/proc/$listener/cmdline" "$root/identity/proc-cmdline.bin"
  cp -- "$conf/iotdb-system.properties" "$root/identity/config-started.properties"
  printf '%s\n' "$cmd" > "$root/identity/proc-cmdline.txt"
}
start() {
  read_manifest; assert_dist "$DIST"; assert_runtime_identity; assert_runtime_distribution; assert_ports_free
  local d n r rpc internal consensus mpp schema data metric
  while IFS='|' read -r d n r rpc internal consensus mpp schema data metric; do [[ "$r" == ConfigNode ]] && start_node "$d" "$n" "$r" "$rpc" "$internal"; done < <(selected)
  while IFS='|' read -r d n r rpc internal consensus mpp schema data metric; do [[ "$r" == DataNode ]] && start_node "$d" "$n" "$r" "$rpc" "$internal"; done < <(selected)
  printf 'Started %s below %s; archive evidence before stop.\n' "$DEPLOYMENT" "$ROOT"
}
start_datanodes() {
  read_manifest; assert_dist "$DIST"; assert_runtime_identity; assert_runtime_distribution
  # ConfigNodes intentionally remain up during a fixed-DOP transition.  Checking their ports here
  # would make a safe DataNode-only restart impossible; verify every selected DataNode port instead.
  assert_datanode_ports_free
  local d n r rpc internal consensus mpp schema data metric
  while IFS='|' read -r d n r rpc internal consensus mpp schema data metric; do
    [[ "$r" == DataNode ]] && start_node "$d" "$n" "$r" "$rpc" "$internal"
  done < <(selected)
  printf 'Started DataNodes for %s below %s.\n' "$DEPLOYMENT" "$ROOT"
}
stop_node() {
  local d="$1" n="$2" file="$ROOT/$1/$2/process.env" pid root class cmd
  [[ -f "$file" ]] || { printf '%s: no manifest\n' "$n"; return; }; source "$file"
  [[ "$root" == "$ROOT/$d/$n" ]] || die "Refusing $n: manifest root mismatch"; kill -0 "$pid" 2>/dev/null || { printf '%s: not running\n' "$n"; return; }
  cmd="$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null || true)"; [[ "$cmd" == *"$root"* && "$cmd" == *"$class"* ]] || die "Refusing PID $pid: it is not this isolated IoTDB node"
  kill "$pid"; local deadline=$((SECONDS+30)); while kill -0 "$pid" 2>/dev/null && ((SECONDS < deadline)); do sleep .2; done; kill -0 "$pid" 2>/dev/null && kill -KILL "$pid"; printf '%s: stopped PID %s\n' "$n" "$pid"
}
stop() { read_manifest; local d n r rpc internal consensus mpp schema data metric; while IFS='|' read -r d n r rpc internal consensus mpp schema data metric; do if [[ "$r" == DataNode ]]; then stop_node "$d" "$n"; fi; done < <(selected); while IFS='|' read -r d n r rpc internal consensus mpp schema data metric; do if [[ "$r" == ConfigNode ]]; then stop_node "$d" "$n"; fi; done < <(selected); }
stop_datanodes() { read_manifest; local d n r rpc internal consensus mpp schema data metric; while IFS='|' read -r d n r rpc internal consensus mpp schema data metric; do if [[ "$r" == DataNode ]]; then stop_node "$d" "$n"; fi; done < <(selected); }
status() { read_manifest; local d n r rpc internal consensus mpp schema data metric file pid port running; while IFS='|' read -r d n r rpc internal consensus mpp schema data metric; do file="$ROOT/$d/$n/process.env"; pid=-; running=false; [[ -f "$file" ]] && { source "$file"; kill -0 "$pid" 2>/dev/null && running=true; }; port="$rpc"; [[ "$r" == ConfigNode ]] && port="$internal"; printf '%s\t%s\t%s\tpid=%s\trunning=%s\tport=%s\tlistening=%s\n' "$d" "$n" "$r" "$pid" "$running" "$port" "$(port_bound "$port" && printf true || printf false)"; done < <(selected); }
case "$MODE" in prepare) prepare;; start) start;; stop) stop;; status) status;; start-datanodes) start_datanodes;; stop-datanodes) stop_datanodes;; esac
