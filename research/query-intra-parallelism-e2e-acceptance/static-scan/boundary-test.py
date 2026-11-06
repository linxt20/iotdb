#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

"""Native unittest checks for boundary fixture and fail-closed evidence validation."""

from copy import deepcopy
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('boundary_acceptance', HERE/'boundary-acceptance.py')
boundary = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(boundary)


class BoundaryAcceptanceTest(unittest.TestCase):
    def setUp(self):
        self.spec = boundary.read(HERE/'boundary-fixture.json')
        self.cases = {c['id']: c for c in boundary.read(HERE/'boundary-manifest.json')['cases']}
        self.expected = boundary.oracle(self.spec)

    def result(self, name):
        return {'columns': deepcopy(self.cases[name]['columns']), 'types': deepcopy(self.cases[name]['types']),
                'rows': deepcopy(self.expected[name])}

    def test_effective_service_properties_are_a_key_value_mapping(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'iotdb-system.properties'
            path.write_text('# ignored\ndegree_of_query_parallelism=4\n'
                            'enable_ordered_parallel_scan = true\nvalue=a=b\n')
            actual = boundary.properties(path)
            self.assertEqual(actual['degree_of_query_parallelism'], '4')
            self.assertEqual(actual.get('enable_ordered_parallel_scan'), 'true')
            self.assertEqual(actual['value'], 'a=b')

    def test_probe_input_removes_license_semicolon_without_changing_sql(self):
        sql = '-- license; text\nCREATE DATABASE d;\n-- comment\n'
        sql += "INSERT INTO t(k) VALUES ('--literal');\n"
        executable = boundary.executable_sql(sql)
        self.assertEqual(executable, "CREATE DATABASE d;\nINSERT INTO t(k) VALUES ('--literal');\n")
        fixture = boundary.fixture_sql(self.spec)
        self.assertTrue(boundary.executable_sql(fixture).startswith('CREATE DATABASE '))

    def runtime(self):
        snap = {'sourceNodeId': '1', 'sourceEndpoint': '127.0.0.1:27130',
                'isTerminal': True, 'generation': 1, 'checksum': 's', 'planIdentityChecksum': 'p',
                'admissionStatus': 'ADMISSION_ACCEPTED', 'resourceObjectCount': 1,
                'resourceUniqueMemoryKeyCount': 1, 'resourceNominalShareBytes': 100,
                'actualPipelineCount': 1, 'splitMode': 'SERIAL', 'plannedDop': 1,
                'splitReason': 'SINGLE_DEVICE', 'workUnitIds': ['q.work'], 'bucketIds': [0],
                'exchangeEdgeIds': [], 'drivers': [{'fragmentInstanceId': 'q.1.0',
                'driverPipelineId': '0', 'driverTaskId': 'q.1.0.0', 'driverSampleGeneration': 1}]}
        return {'fragmentInstancesCount': 1, 'fragmentInstances': [
            {'id': 'q.1.0', 'ip': '127.0.0.1:27130', 'runtimeSnapshot': snap}]}

    def verify_runtime(self, doc, side='candidate'):
        return boundary.verify_runtime(self.cases['single-device'], side, doc,
                                       {'1': {'internalEndpoint': '127.0.0.1:27130'}})

    def test_fixture_is_reproducible_with_isolated_databases(self):
        rendered = boundary.fixture_sql(self.spec)
        self.assertEqual(rendered, (HERE/'boundary-fixture.sql').read_text())
        for db in ['main', 'singlepart', 'ties', 'diff', 'skew']:
            self.assertEqual(rendered.count('CREATE DATABASE b6_boundary_'+db+' '), 1)

    def test_diff_fixture_retains_one_region_and_other_fixtures_two(self):
        rendered = boundary.fixture_sql(self.spec)
        self.assertIn('CREATE DATABASE b6_boundary_diff WITH (max_data_region_group_num=1);', rendered)
        for name in ['main', 'singlepart', 'ties', 'skew']:
            self.assertIn('CREATE DATABASE b6_boundary_'+name+' WITH (max_data_region_group_num=2);', rendered)

    def test_negative_boundary_oracle_and_nonempty_intersection(self):
        interval = self.spec['partitionIntervalMillis']
        self.assertEqual({row[1] for row in self.expected['single-device']},
                         {-interval-1, -interval, -interval+1, -1, 0, 1, interval-1, interval, interval+1})
        self.assertEqual(len(self.expected['adjacent-half-open']), 64)
        self.assertEqual(len(self.expected['nonempty-time-filter']), 192)

    def test_skew_fixture_contains_real_row_count_imbalance(self):
        rows = boundary.fixture_rows(self.spec)['skew']
        counts = boundary.Counter(row[1]//self.spec['partitionIntervalMillis'] for row in rows)
        self.assertEqual(counts, {0: 16384, 1: 256, 2: 256, 3: 256})
        self.assertEqual(len(self.expected['skewed-partitions']), 17152)

    def test_tied_output_has_null_and_actual_duplicate_complete_keys(self):
        rows = self.expected['duplicate-keys-nulls']
        self.assertTrue(any(row[0] is None for row in rows))
        self.assertLess(len({tuple(row) for row in rows}), len(rows))
        self.assertEqual(len(rows), 96)

    def test_complete_result_accepts_all_rows(self):
        name = 'negative-boundaries'
        result = boundary.verify_result(self.cases[name], self.result(name), self.expected[name])
        self.assertEqual(result['rows'], 288)

    def test_missing_row_is_rejected(self):
        name = 'negative-boundaries'; actual = self.result(name); actual['rows'].pop()
        with self.assertRaises(AssertionError):
            boundary.verify_result(self.cases[name], actual, self.expected[name])

    def test_duplicate_row_is_rejected(self):
        name = 'negative-boundaries'; actual = self.result(name); actual['rows'].append(actual['rows'][0])
        with self.assertRaises(AssertionError):
            boundary.verify_result(self.cases[name], actual, self.expected[name])

    def test_wrong_numeric_type_is_rejected(self):
        name = 'negative-boundaries'; actual = self.result(name); actual['types'][2] = 'INT32'
        with self.assertRaises(AssertionError):
            boundary.verify_result(self.cases[name], actual, self.expected[name])

    def test_wrong_full_output_order_is_rejected(self):
        name = 'global-limit-offset'; actual = self.result(name); actual['rows'].reverse()
        with self.assertRaises(AssertionError):
            boundary.verify_result(self.cases[name], actual, self.expected[name])

    def test_global_offset_oracle(self):
        rows = self.expected['global-limit-offset']
        self.assertEqual(rows[0][:2], ['d00', -1])
        self.assertEqual(len(rows), 11)

    def test_single_device_runtime_is_valid_on_actual_node_subset(self):
        self.verify_runtime(self.runtime())

    def test_duplicate_fi_is_rejected(self):
        doc = self.runtime(); doc['fragmentInstances'] *= 2; doc['fragmentInstancesCount'] = 2
        with self.assertRaises(AssertionError): self.verify_runtime(doc)

    def test_zero_resource_fields_are_rejected(self):
        for field in ['resourceObjectCount', 'resourceUniqueMemoryKeyCount', 'resourceNominalShareBytes']:
            doc = self.runtime(); doc['fragmentInstances'][0]['runtimeSnapshot'][field] = 0
            with self.assertRaises(AssertionError): self.verify_runtime(doc)

    def test_wrong_runtime_endpoint_is_rejected(self):
        doc = self.runtime(); doc['fragmentInstances'][0]['ip'] = '127.0.0.1:9999'
        with self.assertRaises(AssertionError): self.verify_runtime(doc)

    def test_control_dop_must_be_one(self):
        doc = self.runtime(); doc['fragmentInstances'][0]['runtimeSnapshot']['plannedDop'] = 4
        with self.assertRaises(AssertionError): self.verify_runtime(doc, 'control')

    def test_driver_identity_is_bound_to_actual_fragment(self):
        doc = self.runtime()
        doc['fragmentInstances'][0]['runtimeSnapshot']['drivers'][0]['driverTaskId'] = 'other.0'
        with self.assertRaises(AssertionError): self.verify_runtime(doc)

    def test_device_limit_requires_actual_pushdown_property(self):
        plan = {'name':'DeviceTableScanNode-1','id':'1','properties': {
            'PushDownLimit':'11','PushDownLimitToEachDevice':'true'}}
        boundary.verify_plan(self.cases['device-limit'], plan)
        plan['properties']['PushDownLimitToEachDevice'] = 'false'
        with self.assertRaises(AssertionError): boundary.verify_plan(self.cases['device-limit'], plan)

    def test_diff_requires_direct_scan_child(self):
        scan = {'name':'DeviceTableScanNode-1','id':'1'}
        plan = {'name':'FilterNode-2','id':'2','properties': {'Predicate':'diff(s1) IS NULL'},
                'children':[scan]}
        boundary.verify_plan(self.cases['filter-diff'], plan)
        plan['children'] = [{'name':'SortNode-3','id':'3','children':[scan]}]
        with self.assertRaises(AssertionError): boundary.verify_plan(self.cases['filter-diff'], plan)

    def test_singlepartition_metadata_rejects_pollution(self):
        data = {'candidate': {'singlepart':[{'partition':0}, {'partition':1}], 'skew':[]}}
        with self.assertRaises(AssertionError): boundary.verify_metadata(data, 'candidate')

    def test_old_defective_source_is_blocked_before_any_service_access(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'binding.json'
            boundary.save(path, {'status':'READY_B4_REPAIRED_BOUNDARY',
                                  'sourceManifest':{'sha256':boundary.OLD_MANIFEST,'path':'not-read'}})
            with self.assertRaisesRegex(AssertionError, 'old B4-defective source'):
                boundary.preflight(path, self.spec)

    def test_diff_filter_in_parallel_morsel_is_rejected(self):
        managed = [{'operators':{'operatorType':'FilterAndProjectOperator'},
                    'parallelPipelines':[{'operatorType':'FilterAndProjectOperator'}]}]
        with self.assertRaises(AssertionError): boundary.verify_diff_placement(managed)

    def test_diff_filter_retained_above_scan_is_accepted(self):
        managed = [{'operators':{'operatorType':'FilterAndProjectOperator',
                                'children':[{'operatorType':'CollectOperator'}]},
                    'parallelPipelines':[{'operatorType':'TableScanOperator'}]}]
        boundary.verify_diff_placement(managed)


if __name__ == '__main__':
    unittest.main(verbosity=2)
