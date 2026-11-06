-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements. See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership. The ASF licenses this file
-- to You under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
-- See the License for the specific language governing permissions
-- and limitations under the License.
CREATE DATABASE static_scan32 WITH (max_data_region_group_num=1);
USE static_scan32;
CREATE TABLE fixture(device_id STRING TAG,s1 INT64 FIELD,s2 DOUBLE FIELD);
INSERT INTO fixture(time,device_id,s1,s2) VALUES (1,'d0',null,0.5),(2,'d0',0,1.5),(3,'d0',1,2.5),(604800001,'d0',null,100.5),(604800002,'d0',0,101.5),(604800003,'d0',1,102.5),(1,'d1',null,1000.5),(2,'d1',0,1001.5),(3,'d1',1,1002.5),(604800001,'d1',null,1100.5),(604800002,'d1',0,1101.5),(604800003,'d1',1,1102.5),(1,'d2',null,2000.5),(2,'d2',0,2001.5),(3,'d2',1,2002.5),(604800001,'d2',null,2100.5),(604800002,'d2',0,2101.5),(604800003,'d2',1,null),(1,'d3',null,3000.5),(2,'d3',0,3001.5),(3,'d3',1,3002.5),(604800001,'d3',null,3100.5),(604800002,'d3',0,3101.5),(604800003,'d3',1,3102.5);
FLUSH;
