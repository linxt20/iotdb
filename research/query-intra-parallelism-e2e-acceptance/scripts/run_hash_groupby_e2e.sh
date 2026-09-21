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

install_dir=${1:?usage: run_hash_groupby_e2e.sh <iotdb-install-dir> <artifact-dir> [port]}
artifact_dir=${2:?usage: run_hash_groupby_e2e.sh <iotdb-install-dir> <artifact-dir> [port]}
port=${3:-26667}
cli="${install_dir}/sbin/start-cli.sh"

mkdir -p "${artifact_dir}"
"${cli}" -h 127.0.0.1 -p "${port}" -u root -pw root -sql_dialect table \
  -e "USE p0e2e; EXPLAIN ANALYZE SELECT s1, COUNT(*) AS row_count FROM telemetry WHERE device_id = 'd0' GROUP BY s1;" \
  > "${artifact_dir}/hash-e2e-explain-group.stdout" \
  2> "${artifact_dir}/hash-e2e-explain-group.stderr"
"${cli}" -h 127.0.0.1 -p "${port}" -u root -pw root -sql_dialect table \
  -e "USE p0e2e; SELECT s1, COUNT(*) AS row_count FROM telemetry WHERE device_id = 'd0' GROUP BY s1 ORDER BY s1;" \
  > "${artifact_dir}/hash-e2e-group-result.stdout" \
  2> "${artifact_dir}/hash-e2e-group-result.stderr"
