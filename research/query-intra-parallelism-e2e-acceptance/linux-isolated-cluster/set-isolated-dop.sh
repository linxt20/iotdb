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

# Apply one fixed DOP to both DataNodes in a named isolated deployment and retain an audit trail.
# This script deliberately has no default root and never invokes a distribution stop script.
set -Eeuo pipefail
IFS=$'\n\t'

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly LAUNCHER="$SCRIPT_DIR/launch-isolated-nxp.sh"
ROOT='' DEPLOYMENT='' DOP='' TIMEOUT=120 AUDIT_DIR=''

usage() {
  printf '%s\n' 'Usage: set-isolated-dop.sh --root ROOT --deployment candidate|control --dop POSITIVE_INTEGER [--audit-dir DIR] [--startup-timeout-seconds SECONDS]'
}
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
need() { [[ -n "${2:-}" ]] || die "Missing value for $1"; }

while (($#)); do
  case "$1" in
    --root|--deployment|--dop|--audit-dir|--startup-timeout-seconds)
      need "$1" "${2:-}"
      case "$1" in
        --root) ROOT="$2" ;;
        --deployment) DEPLOYMENT="$2" ;;
        --dop) DOP="$2" ;;
        --audit-dir) AUDIT_DIR="$2" ;;
        --startup-timeout-seconds) TIMEOUT="$2" ;;
      esac
      shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown argument: $1" ;;
  esac
done

[[ -x "$LAUNCHER" || -f "$LAUNCHER" ]] || die "Missing isolated launcher: $LAUNCHER"
[[ -n "$ROOT" && -n "$DEPLOYMENT" && -n "$DOP" ]] || { usage >&2; exit 2; }
[[ "$DEPLOYMENT" =~ ^(candidate|control)$ ]] || die '--deployment must name exactly candidate or control'
[[ "$DOP" =~ ^[1-9][0-9]*$ ]] && ((DOP <= 1024)) || die '--dop must be a positive integer no greater than 1024'
[[ "$TIMEOUT" =~ ^[1-9][0-9]*$ ]] || die '--startup-timeout-seconds must be positive'
ROOT="$(realpath -m -- "$ROOT")"
[[ -f "$ROOT/isolation-manifest.env" ]] || die "No isolated deployment manifest below $ROOT"
[[ -z "$AUDIT_DIR" ]] && AUDIT_DIR="$ROOT/benchmark-dop-audit"
AUDIT_DIR="$(realpath -m -- "$AUDIT_DIR")"
case "$AUDIT_DIR" in "$ROOT"/*) ;; *) die '--audit-dir must stay below --root';; esac

readonly CONFIG_1="$ROOT/$DEPLOYMENT/datanode-1/conf/iotdb-system.properties"
readonly CONFIG_2="$ROOT/$DEPLOYMENT/datanode-2/conf/iotdb-system.properties"
[[ -f "$CONFIG_1" && -f "$CONFIG_2" ]] || die "Missing both DataNode configurations below $ROOT/$DEPLOYMENT"

config_value() {
  local config="$1" key="$2"
  awk -F= -v key="$key" '
    /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
    $1 ~ "^[[:space:]]*" key "[[:space:]]*$" { value=$2; count++ }
    END { if (count != 1) exit 2; gsub(/^[[:space:]]+|[[:space:]]+$/, "", value); print value }
  ' "$config"
}
assert_config() {
  local config="$1" current fixed
  current="$(config_value "$config" degree_of_query_parallelism)" || die "Expected exactly one degree_of_query_parallelism in $config"
  fixed="$(config_value "$config" enable_dop_estimation)" || die "Expected exactly one enable_dop_estimation in $config"
  [[ "$current" == "$DOP" ]] || die "DOP verification failed for $config: expected $DOP, got $current"
  [[ "$fixed" == false ]] || die "DOP estimation must remain false in $config, got $fixed"
}
replace_dop() {
  local config="$1" tmp
  tmp="$config.tmp.$$"
  awk -v value="$DOP" '
    /^[[:space:]]*degree_of_query_parallelism[[:space:]]*=/ {
      if (count++) exit 2
      print "degree_of_query_parallelism=" value
      next
    }
    { print }
    END { if (count != 1) exit 2 }
  ' "$config" > "$tmp" || { rm -f -- "$tmp"; die "Refusing to rewrite ambiguous DOP property in $config"; }
  mv -- "$tmp" "$config"
}

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
transition="$AUDIT_DIR/${timestamp}-dop-${DOP}"
if [[ -e "$transition" ]]; then
  suffix=1
  while [[ -e "${transition}-${suffix}" ]]; do ((suffix++)); done
  transition="${transition}-${suffix}"
fi
mkdir -p "$transition/before" "$transition/after"
cp -- "$CONFIG_1" "$transition/before/datanode-1.properties"
cp -- "$CONFIG_2" "$transition/before/datanode-2.properties"
sha256sum "$CONFIG_1" "$CONFIG_2" > "$transition/before/config.sha256"

# The launcher validates /proc command lines before stopping only the named DataNodes.  ConfigNode
# stays alive, which retains cluster metadata while this fixed-DOP process transition occurs.
bash "$LAUNCHER" --mode stop-datanodes --root "$ROOT" --deployment "$DEPLOYMENT" \
  --startup-timeout-seconds "$TIMEOUT" > "$transition/stop.stdout" 2> "$transition/stop.stderr"
replace_dop "$CONFIG_1"
replace_dop "$CONFIG_2"
assert_config "$CONFIG_1"
assert_config "$CONFIG_2"

if ! bash "$LAUNCHER" --mode start-datanodes --root "$ROOT" --deployment "$DEPLOYMENT" \
  --startup-timeout-seconds "$TIMEOUT" > "$transition/start.stdout" 2> "$transition/start.stderr"; then
  cp -- "$transition/before/datanode-1.properties" "$CONFIG_1"
  cp -- "$transition/before/datanode-2.properties" "$CONFIG_2"
  bash "$LAUNCHER" --mode start-datanodes --root "$ROOT" --deployment "$DEPLOYMENT" \
    --startup-timeout-seconds "$TIMEOUT" > "$transition/rollback-start.stdout" 2> "$transition/rollback-start.stderr" || true
  die "DOP=$DOP restart failed; previous configuration was restored and a rollback restart was attempted. Audit: $transition"
fi

bash "$LAUNCHER" --mode status --root "$ROOT" --deployment "$DEPLOYMENT" > "$transition/status.tsv"
grep -Eq "^${DEPLOYMENT}[[:space:]]+datanode-1[[:space:]].*running=true.*listening=true$" "$transition/status.tsv" || die "DataNode 1 is not ready after DOP restart; audit: $transition"
grep -Eq "^${DEPLOYMENT}[[:space:]]+datanode-2[[:space:]].*running=true.*listening=true$" "$transition/status.tsv" || die "DataNode 2 is not ready after DOP restart; audit: $transition"
cp -- "$CONFIG_1" "$transition/after/datanode-1.properties"
cp -- "$CONFIG_2" "$transition/after/datanode-2.properties"
sha256sum "$CONFIG_1" "$CONFIG_2" > "$transition/after/config.sha256"
{
  printf 'timestamp_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'root=%q\n' "$ROOT"
  printf 'deployment=%s\n' "$DEPLOYMENT"
  printf 'configured_dop=%s\n' "$DOP"
  printf 'enable_dop_estimation=false\n'
  printf 'status_file=%q\n' "$transition/status.tsv"
} > "$transition/transition.env"
printf 'DOP transition accepted: deployment=%s dop=%s audit=%s\n' "$DEPLOYMENT" "$DOP" "$transition"
