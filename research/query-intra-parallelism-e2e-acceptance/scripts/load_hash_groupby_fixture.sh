#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

install_dir=${1:?usage: load_hash_groupby_fixture.sh <iotdb-install-dir> <artifact-dir> [port]}
artifact_dir=${2:?usage: load_hash_groupby_fixture.sh <iotdb-install-dir> <artifact-dir> [port]}
port=${3:-26667}
cli="${install_dir}/sbin/start-cli.sh"

mkdir -p "${artifact_dir}"
run_sql() {
  "${cli}" -h 127.0.0.1 -p "${port}" -u root -pw root -sql_dialect table -e "$1"
}

{
  run_sql 'CREATE DATABASE IF NOT EXISTS p0e2e;'
  run_sql 'USE p0e2e; CREATE TABLE IF NOT EXISTS telemetry (device_id STRING TAG, s1 INT64 FIELD, s2 DOUBLE FIELD);'
  run_sql "USE p0e2e; INSERT INTO telemetry(time, device_id, s1, s2) VALUES (0, 'd0', 1, 1.5), (1, 'd0', 2, 2.5), (604800001, 'd0', 3, 3.5), (604800002, 'd0', 4, 4.5), (0, 'd1', 11, 11.5), (1, 'd1', 12, 12.5), (604800001, 'd1', 13, 13.5), (604800002, 'd1', 14, 14.5), (0, 'd2', 21, 21.5), (1, 'd2', 22, 22.5), (604800001, 'd2', 23, 23.5), (604800002, 'd2', 24, 24.5), (0, 'd3', 31, 31.5), (1, 'd3', 32, 32.5), (604800001, 'd3', 33, 33.5), (604800002, 'd3', 34, 34.5);"
} > "${artifact_dir}/hash-e2e-fixture.stdout" 2> "${artifact_dir}/hash-e2e-fixture.stderr"
