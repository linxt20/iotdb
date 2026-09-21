#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.

# Import generated table-model fixture shards with reproducible server-side acceptance evidence.
set -Eeuo pipefail
IFS=$'\n\t'

CLI='' IMPORTER='' HOST=127.0.0.1 PORT='' DATABASE='' TABLE='' INPUT='' ARTIFACT='' PASSWORD_ENV=IOTDB_PASSWORD
usage() { printf '%s\n' 'Usage: load_table_fixture.sh --cli START_CLI --importer IMPORT_DATA --host HOST --port PORT --database NAME --table NAME --input-dir FIXTURE_DIR --artifact-dir NEW_DIR [--password-env ENV]'; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
need() { [[ -n "${2:-}" ]] || die "Missing value for $1"; }
while (($#)); do
  case "$1" in
    --cli|--importer|--host|--port|--database|--table|--input-dir|--artifact-dir|--password-env)
      need "$1" "${2:-}"
      case "$1" in
        --cli) CLI="$2" ;; --importer) IMPORTER="$2" ;; --host) HOST="$2" ;;
        --port) PORT="$2" ;; --database) DATABASE="$2" ;; --table) TABLE="$2" ;;
        --input-dir) INPUT="$2" ;; --artifact-dir) ARTIFACT="$2" ;; --password-env) PASSWORD_ENV="$2" ;;
      esac
      shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown argument: $1" ;;
  esac
done
[[ -x "$CLI" ]] || die "--cli is not executable: $CLI"
[[ -x "$IMPORTER" ]] || die "--importer is not executable: $IMPORTER"
[[ "$PORT" =~ ^[1-9][0-9]*$ ]] && ((PORT <= 65535)) || die '--port must be in 1..65535'
[[ "$DATABASE" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || die '--database must be a simple identifier'
[[ "$TABLE" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || die '--table must be a simple identifier'
INPUT="$(realpath -e -- "$INPUT")"
[[ -f "$INPUT/fixture-manifest.json" ]] || die "Missing fixture manifest: $INPUT/fixture-manifest.json"
ARTIFACT="$(realpath -m -- "$ARTIFACT")"
[[ ! -e "$ARTIFACT" ]] || ! find "$ARTIFACT" -mindepth 1 -print -quit | grep -q . || die "Refusing non-empty artifact directory: $ARTIFACT"
PASSWORD="${!PASSWORD_ENV:-}"
[[ -n "$PASSWORD" ]] || die "Password environment variable is unset: $PASSWORD_ENV"
mapfile -t SHARDS < <(find "$INPUT" -maxdepth 1 -type f -name 'fixture-partition-*.csv' -printf '%f\n' | sort)
((${#SHARDS[@]} > 0)) || die "No fixture-partition-*.csv files below $INPUT"
EXPECTED_ROWS="$(python3 - "$INPUT/fixture-manifest.json" <<'PY'
import json, sys
rows = json.load(open(sys.argv[1], encoding='utf-8')).get('total_rows')
if not isinstance(rows, int) or rows < 1:
    raise SystemExit('fixture manifest total_rows must be a positive integer')
print(rows)
PY
)"
mkdir -p "$ARTIFACT/output" "$ARTIFACT/failed"
cp -- "$INPUT/fixture-manifest.json" "$ARTIFACT/fixture-manifest.json"
printf 'timestamp_utc=%s\nhost=%s\nport=%s\ndatabase=%s\ntable=%s\nexpected_rows=%s\nshard_count=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$HOST" "$PORT" "$DATABASE" "$TABLE" "$EXPECTED_ROWS" "${#SHARDS[@]}" > "$ARTIFACT/manifest.env"
schema_sql="CREATE DATABASE IF NOT EXISTS $DATABASE; CREATE TABLE IF NOT EXISTS $DATABASE.$TABLE(time TIMESTAMP TIME, device_id STRING TAG, s1 INT64 FIELD, s2 DOUBLE FIELD);"
printf '%s\n' "$schema_sql" > "$ARTIFACT/create-schema.sql"
"$CLI" -h "$HOST" -p "$PORT" -u root -pw "$PASSWORD" -sql_dialect table -e "$schema_sql" > "$ARTIFACT/output/create-schema.stdout" 2> "$ARTIFACT/output/create-schema.stderr"
for shard in "${SHARDS[@]}"; do
  stem="${shard%.csv}"
  "$IMPORTER" -ft csv -sql_dialect table -h "$HOST" -p "$PORT" -u root -pw "$PASSWORD" -db "$DATABASE" -table "$TABLE" -s "$INPUT/$shard" -fd "$ARTIFACT/failed" -lpf 100 -tn 1 -batch 100 > "$ARTIFACT/output/import-$stem.stdout" 2> "$ARTIFACT/output/import-$stem.stderr"
  grep -Fq 'Import completely!' "$ARTIFACT/output/import-$stem.stdout" || die "Importer did not report completion for $shard"
  ! grep -Eqi 'unexpected error|failed to import|mapping for time not found' "$ARTIFACT/output/import-$stem.stdout" || die "Importer reported an error for $shard"
  "$CLI" -h "$HOST" -p "$PORT" -u root -pw "$PASSWORD" -sql_dialect table -e "FLUSH $DATABASE" > "$ARTIFACT/output/flush-$stem.stdout" 2> "$ARTIFACT/output/flush-$stem.stderr"
done
if find "$ARTIFACT/failed" -type f -print -quit | grep -q .; then die "Importer wrote failed records below $ARTIFACT/failed"; fi
"$CLI" -h "$HOST" -p "$PORT" -u root -pw "$PASSWORD" -sql_dialect table -e "SELECT COUNT(*) AS rows FROM $DATABASE.$TABLE" > "$ARTIFACT/output/row-count.stdout" 2> "$ARTIFACT/output/row-count.stderr"
ACTUAL_ROWS="$(awk -F'|' '/^\|/ { value=$2; gsub(/^[[:space:]]+|[[:space:]]+$/, "", value); if (value ~ /^[0-9]+$/) print value }' "$ARTIFACT/output/row-count.stdout" | tail -n 1)"
[[ "$ACTUAL_ROWS" == "$EXPECTED_ROWS" ]] || die "Row-count mismatch: expected $EXPECTED_ROWS, got ${ACTUAL_ROWS:-missing}"
printf 'accepted=true\nactual_rows=%s\n' "$ACTUAL_ROWS" > "$ARTIFACT/result.env"
printf 'Fixture import accepted: rows=%s artifact=%s\n' "$ACTUAL_ROWS" "$ARTIFACT"
