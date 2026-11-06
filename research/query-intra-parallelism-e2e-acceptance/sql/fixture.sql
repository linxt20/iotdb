-- Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
-- See the NOTICE file distributed with this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0.
-- You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing, software distributed under the License is
-- distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and limitations under the License.

-- MANUAL, MUTATING SMOKE FIXTURE. Review this file and run it only against a new isolated
-- deployment. The acceptance runner never invokes it. 604800001 is in the second default
-- seven-day time partition; change the timestamps if this isolated deployment uses another interval.
CREATE DATABASE IF NOT EXISTS p0e2e;
USE p0e2e;
CREATE TABLE IF NOT EXISTS telemetry (
  device_id STRING TAG,
  s1 INT64 FIELD,
  s2 DOUBLE FIELD
);

INSERT INTO telemetry(time, device_id, s1, s2) VALUES
  (0, 'd0', 1, 1.5),
  (1, 'd0', 2, 2.5),
  (604800001, 'd0', 3, 3.5),
  (604800002, 'd0', 4, 4.5),
  (0, 'd1', 11, 11.5),
  (1, 'd1', 12, 12.5),
  (604800001, 'd1', 13, 13.5),
  (604800002, 'd1', 14, 14.5),
  (0, 'd2', 21, 21.5),
  (1, 'd2', 22, 22.5),
  (604800001, 'd2', 23, 23.5),
  (604800002, 'd2', 24, 24.5),
  (0, 'd3', 31, 31.5),
  (1, 'd3', 32, 32.5),
  (604800001, 'd3', 33, 33.5),
  (604800002, 'd3', 34, 34.5);
