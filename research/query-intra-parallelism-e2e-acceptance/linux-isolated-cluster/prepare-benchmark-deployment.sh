#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

# Create and start a *new* candidate-only benchmark endpoint.  Existing roots are rejected by the
# underlying launcher, so this cannot repurpose a running baseline service.
set -Eeuo pipefail
IFS=$'\n\t'

readonly SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
readonly LAUNCHER="$SCRIPT_DIR/launch-isolated-nxp.sh"
readonly DOP_TOOL="$SCRIPT_DIR/set-isolated-dop.sh"
ROOT='' DIST='' INITIAL_DOP=1 TIMEOUT=120

usage() { printf '%s\n' 'Usage: prepare-benchmark-deployment.sh --root NEW_ROOT --distribution-root ALL_BIN_DIST [--initial-dop N] [--startup-timeout-seconds SECONDS]'; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
need() { [[ -n "${2:-}" ]] || die "Missing value for $1"; }
while (($#)); do
  case "$1" in
    --root|--distribution-root|--initial-dop|--startup-timeout-seconds)
      need "$1" "${2:-}"
      case "$1" in
        --root) ROOT="$2" ;;
        --distribution-root) DIST="$2" ;;
        --initial-dop) INITIAL_DOP="$2" ;;
        --startup-timeout-seconds) TIMEOUT="$2" ;;
      esac
      shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown argument: $1" ;;
  esac
done
[[ -n "$ROOT" && -n "$DIST" ]] || { usage >&2; exit 2; }
[[ "$INITIAL_DOP" =~ ^[1-9][0-9]*$ ]] && ((INITIAL_DOP <= 1024)) || die '--initial-dop must be a positive integer no greater than 1024'
[[ "$TIMEOUT" =~ ^[1-9][0-9]*$ ]] || die '--startup-timeout-seconds must be positive'
ROOT="$(realpath -m -- "$ROOT")"
[[ ! -e "$ROOT" ]] || ! find "$ROOT" -mindepth 1 -print -quit | grep -q . || die "Refusing benchmark root that already contains files: $ROOT"

bash "$LAUNCHER" --mode prepare --root "$ROOT" --distribution-root "$DIST" --deployment candidate --startup-timeout-seconds "$TIMEOUT"
bash "$LAUNCHER" --mode start --root "$ROOT" --deployment candidate --startup-timeout-seconds "$TIMEOUT"
bash "$DOP_TOOL" --root "$ROOT" --deployment candidate --dop "$INITIAL_DOP" --startup-timeout-seconds "$TIMEOUT"
printf 'Dedicated benchmark endpoint is ready at 127.0.0.1:27667 (root=%s, initial DOP=%s).\n' "$ROOT" "$INITIAL_DOP"
