-- Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
-- See the NOTICE file distributed with this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.  You may obtain a copy of the
-- License at http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software distributed under the License
-- is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
-- or implied.  See the License for the specific language governing permissions and limitations
-- under the License.

-- MANUAL, MUTATING FIXTURE. Do not let an acceptance script execute this file. Run it only on a
-- new isolated deployment whose data placement has already been arranged to produce at least two
-- direct table-scan sources. The acceptance runner rejects a one-source plan.
CREATE DATABASE IF NOT EXISTS p0multi;
USE p0multi;
CREATE TABLE IF NOT EXISTS telemetry (
  device_id STRING TAG,
  s1 INT64 FIELD,
  s2 DOUBLE FIELD
);

-- Each group occurs in more than one device/source. A split that fails to co-locate equal s1
-- values changes COUNT(*), which makes hash-on/off result equivalence a meaningful correctness
-- gate. Use the same fixture on candidate and control endpoints.
INSERT INTO telemetry(time, device_id, s1, s2) VALUES
  (0, 'd0', 1, 1.0), (1, 'd0', 2, 2.0), (2, 'd0', 3, 3.0),
  (0, 'd1', 1, 11.0), (1, 'd1', 2, 12.0), (2, 'd1', 3, 13.0),
  (0, 'd2', 1, 21.0), (1, 'd2', 2, 22.0), (2, 'd2', 3, 23.0),
  (0, 'd3', 1, 31.0), (1, 'd3', 2, 32.0), (2, 'd3', 3, 33.0);
