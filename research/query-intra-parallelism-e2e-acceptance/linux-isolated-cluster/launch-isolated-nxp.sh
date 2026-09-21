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
MODE='' ROOT='' DIST='' DEPLOYMENT=both TIMEOUT=120 PORT_OFFSET=0

usage() { printf '%s\n' 'Usage: launch-isolated-nxp.sh --mode prepare|start|stop|status|start-datanodes|stop-datanodes --root ROOT [--distribution-root DIST] [--deployment candidate|control|both] [--port-offset NON_NEGATIVE_INTEGER]'; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
need() { [[ -n "${2:-}" ]] || die "Missing value for $1"; }
while (($#)); do
  case "$1" in
    --mode|--root|--distribution-root|--deployment|--startup-timeout-seconds|--port-offset) need "$1" "${2:-}"; key="${1#--}"; key="${key//-/_}"; case "$key" in mode) MODE="$2";; root) ROOT="$2";; distribution_root) DIST="$2";; deployment) DEPLOYMENT="$2";; startup_timeout_seconds) TIMEOUT="$2";; port_offset) PORT_OFFSET="$2";; esac; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown argument: $1" ;;
  esac
done
[[ "$MODE" =~ ^(prepare|start|stop|status|start-datanodes|stop-datanodes)$ ]] || die '--mode must be prepare, start, stop, status, start-datanodes, or stop-datanodes'
[[ -n "$ROOT" ]] || die '--root is required'
[[ "$DEPLOYMENT" =~ ^(candidate|control|both)$ ]] || die '--deployment must be candidate, control, or both'
[[ "$TIMEOUT" =~ ^[1-9][0-9]*$ ]] || die '--startup-timeout-seconds must be positive'
[[ "$PORT_OFFSET" =~ ^[0-9]+$ ]] && ((PORT_OFFSET <= 26000)) || die '--port-offset must be a non-negative integer no greater than 26000'
ROOT="$(realpath -m -- "$ROOT")"; [[ -z "$DIST" ]] || DIST="$(realpath -m -- "$DIST")"

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
}
port_bound() {
  if command -v ss >/dev/null; then ss -H -ltn "sport = :$1" | grep -q .
  elif command -v lsof >/dev/null; then lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
  else die 'Need ss or lsof for mandatory isolated-port safety checks'; fi
}
assert_ports_free() {
  local d n r rpc internal consensus mpp schema data metric p
  while IFS='|' read -r d n r rpc internal consensus mpp schema data metric; do
    for p in "$rpc" "$internal" "$consensus" "$mpp" "$schema" "$data" "$metric"; do [[ "$p" == - ]] || ! port_bound "$p" || die "Refusing occupied isolated port: $p"; done
  done < <(selected)
}
assert_datanode_ports_free() {
  local d n r rpc internal consensus mpp schema data metric p
  while IFS='|' read -r d n r rpc internal consensus mpp schema data metric; do
    [[ "$r" == DataNode ]] || continue
    for p in "$rpc" "$internal" "$mpp" "$schema" "$data" "$metric"; do
      ! port_bound "$p" || die "Refusing occupied isolated DataNode port: $p"
    done
  done < <(selected)
}
write_manifest() {
  { printf 'schema_version=1\n'; printf 'purpose=linux-isolated-1c2d-candidate-control-nxp-group-by\n'; printf 'created_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"; printf 'repository_git_sha=%s\n' "$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || printf unavailable)"; printf 'distribution_root=%q\n' "$DIST"; printf 'deployment_root=%q\n' "$ROOT"; printf 'port_offset=%s\n' "$PORT_OFFSET"; printf 'distribution_jar_count=%s\n' "$(find "$DIST/lib" -maxdepth 1 -name '*.jar' -type f | wc -l | tr -d ' ')"; } > "$ROOT/$MANIFEST"
}
read_manifest() { [[ -f "$ROOT/$MANIFEST" ]] || die "No $MANIFEST below $ROOT; run prepare"; source "$ROOT/$MANIFEST"; [[ "${deployment_root:-}" == "$ROOT" ]] || die 'Manifest root mismatch'; DIST="$distribution_root"; PORT_OFFSET="${port_offset:-0}"; }

write_config() {
  local d="$1" n="$2" role="$3" rpc="$4" internal="$5" consensus="$6" mpp="$7" schema="$8" data="$9" metric="${10}" root="$ROOT/$1/$2" conf="$ROOT/$1/$2/conf" enabled=false dn_rpc="$4" dn_internal="$5" dn_mpp="$7" dn_schema="$8" dn_data="$9" dn_metric="${10}"
  [[ "$d" == candidate ]] && enabled=true
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
degree_of_query_parallelism=4
enable_dop_estimation=false
enable_property_driven_planning=$enabled
enable_table_group_by_hash_repartition=$enabled
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
}
prepare() {
  [[ -n "$DIST" ]] || die '--distribution-root is required for prepare'; assert_dist "$DIST"
  [[ ! -e "$ROOT" ]] || ! find "$ROOT" -mindepth 1 -print -quit | grep -q . || die "Prepare refuses non-empty root: $ROOT"
  assert_ports_free; mkdir -p "$ROOT"
  local d n r rpc internal consensus mpp schema data metric
  while IFS='|' read -r d n r rpc internal consensus mpp schema data metric; do write_config "$d" "$n" "$r" "$rpc" "$internal" "$consensus" "$mpp" "$schema" "$data" "$metric"; done < <(nodes)
  write_manifest; printf 'Prepared %s (SHA %s)\n' "$ROOT" "$(grep '^repository_git_sha=' "$ROOT/$MANIFEST" | cut -d= -f2-)"
}
classpath() { local j; shopt -s nullglob; local jars=("$DIST"/lib/*.jar); shopt -u nullglob; (IFS=:; printf '%s' "${jars[*]}"); }
wait_port() { local p="$1" deadline=$((SECONDS + TIMEOUT)); while ((SECONDS < deadline)); do port_bound "$p" && return; sleep .5; done; return 1; }
start_node() {
  local d="$1" n="$2" role="$3" rpc="$4" internal="$5" root="$ROOT/$1/$2" conf="$ROOT/$1/$2/conf" logs="$ROOT/$1/$2/logs" data="$ROOT/$1/$2/data" class="$DN_CLASS" prefix=IOTDB log="$ROOT/$1/$2/conf/logback-datanode.xml" port="$4"
  [[ "$role" == ConfigNode ]] && { class="$CN_CLASS"; prefix=CONFIGNODE; log="$conf/logback-confignode.xml"; port="$internal"; }
  nohup java --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED "-Dlogback.configurationFile=$log" "-D${prefix}_HOME=$DIST" "-D${prefix}_DATA_HOME=$data" "-D${prefix}_CONF=$conf" "-D${prefix}_LOGS=$logs" "-D${prefix}_LOG_DIR=$logs" "-DTSFILE_HOME=$DIST" "-DTSFILE_CONF=$conf" -Dfile.encoding=UTF-8 -Diotdb-foreground=yes -Xms256m -Xmx512m -cp "$(classpath)" "$class" -s >"$logs/stdout.log" 2>"$logs/stderr.log" &
  local pid=$!; { printf 'pid=%s\nroot=%q\nclass=%q\nstarted_at_utc=%s\n' "$pid" "$root" "$class" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"; } > "$root/process.env"
  wait_port "$port" || { kill -0 "$pid" 2>/dev/null && kill "$pid" || true; die "Timed out waiting for $n on $port; inspect $logs"; }
}
start() {
  read_manifest; assert_dist "$DIST"; command -v java >/dev/null || die 'java is absent'; assert_ports_free
  local d n r rpc internal consensus mpp schema data metric
  while IFS='|' read -r d n r rpc internal consensus mpp schema data metric; do [[ "$r" == ConfigNode ]] && start_node "$d" "$n" "$r" "$rpc" "$internal"; done < <(selected)
  while IFS='|' read -r d n r rpc internal consensus mpp schema data metric; do [[ "$r" == DataNode ]] && start_node "$d" "$n" "$r" "$rpc" "$internal"; done < <(selected)
  printf 'Started %s below %s; archive evidence before stop.\n' "$DEPLOYMENT" "$ROOT"
}
start_datanodes() {
  read_manifest; assert_dist "$DIST"; command -v java >/dev/null || die 'java is absent'
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
stop() { read_manifest; local d n r rpc internal consensus mpp schema data metric; while IFS='|' read -r d n r rpc internal consensus mpp schema data metric; do [[ "$r" == DataNode ]] && stop_node "$d" "$n"; done < <(selected); while IFS='|' read -r d n r rpc internal consensus mpp schema data metric; do [[ "$r" == ConfigNode ]] && stop_node "$d" "$n"; done < <(selected); }
stop_datanodes() { read_manifest; local d n r rpc internal consensus mpp schema data metric; while IFS='|' read -r d n r rpc internal consensus mpp schema data metric; do [[ "$r" == DataNode ]] && stop_node "$d" "$n"; done < <(selected); }
status() { read_manifest; local d n r rpc internal consensus mpp schema data metric file pid port running; while IFS='|' read -r d n r rpc internal consensus mpp schema data metric; do file="$ROOT/$d/$n/process.env"; pid=-; running=false; [[ -f "$file" ]] && { source "$file"; kill -0 "$pid" 2>/dev/null && running=true; }; port="$rpc"; [[ "$r" == ConfigNode ]] && port="$internal"; printf '%s\t%s\t%s\tpid=%s\trunning=%s\tport=%s\tlistening=%s\n' "$d" "$n" "$r" "$pid" "$running" "$port" "$(port_bound "$port" && printf true || printf false)"; done < <(selected); }
case "$MODE" in prepare) prepare;; start) start;; stop) stop;; status) status;; start-datanodes) start_datanodes;; stop-datanodes) stop_datanodes;; esac
