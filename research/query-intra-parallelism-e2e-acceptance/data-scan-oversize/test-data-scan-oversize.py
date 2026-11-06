#!/usr/bin/env python3
"""Memory error classification tests: preserve the distinction between schema/bootstrap/data."""
import importlib.util
from pathlib import Path
import unittest
import os
import tempfile

source = Path(__file__).with_name('run-data-scan-oversize.py')
spec = importlib.util.spec_from_file_location('data_oversize', source)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class ActualDataOversizeClassificationTests(unittest.TestCase):
    def test_real_pool_max_fi_request_is_accepted(self):
        self.assertEqual(runner.error_bytes('bytesToReserve should be in (0,maxBytesPerFI]. maxBytesPerFI: 2000000, bytesToReserve: 8388616'),
                         {'actualRequestBytes': 8388616, 'actualCapBytes': 2000000})

    def test_real_channel_limit_data_request_is_accepted(self):
        self.assertEqual(runner.error_bytes('Query is aborted since it requests more memory than can be allocated, bytesToReserve: 8388616B, maxBytesCanReserve: 2000000B'),
                         {'actualRequestBytes': 8388616, 'actualCapBytes': 2000000})

    def test_schema_68_over_62_is_rejected(self):
        with self.assertRaises(AssertionError):
            runner.error_bytes('Fetch Table Device Schema failed because bytesToReserve: 68B, maxBytesCanReserve: 62B')

    def test_initial_block_and_too_small_cap_are_rejected(self):
        for message in ('bytesToReserve: 1048576B, maxBytesCanReserve: 2000B',
                        'bytesToReserve: 8388608B, maxBytesCanReserve: 2000B'):
            with self.subTest(message=message), self.assertRaises(AssertionError):
                runner.error_bytes(message)

    def test_generic_query_error_does_not_count(self):
        for message in ('IllegalArgumentException', 'out of memory', 'disk free below 0.05', 'not enough memory'):
            with self.subTest(message=message), self.assertRaises(AssertionError):
                runner.error_bytes(message)


class RuntimeArtifactIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=os.environ.get("TMPDIR"))
        self.addCleanup(self.temp.cleanup)
        self.dist = Path(self.temp.name)
        self.files = {"lib/HdrHistogram.jar": b"hdr", "lib/caffeine.jar": b"cache",
                      "conf/iotdb-system.properties": b"fixed=true\n"}
        for relative, data in self.files.items():
            path = self.dist / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        self.baseline = {name: runner.sha(self.dist / name) for name in self.files}
        self.jars = sorted((self.dist / "lib").glob("*.jar"))
        self.jar_sha = {str(p): runner.sha(p) for p in self.jars}

    def test_real_order_is_preserved_while_same_complete_jars_are_accepted(self):
        actual = list(reversed(self.jars))
        result = runner.actual_classpath_identity(os.pathsep.join(map(str, actual)),
                                                 self.jars, self.jar_sha)
        self.assertEqual(result["actualOrderedClasspath"], list(map(str, actual)))
        self.assertEqual([item["path"] for item in result["actualOrderedLibraries"]],
                         list(map(str, actual)))

    def test_missing_duplicate_extra_wildcard_or_relative_classpath_is_rejected(self):
        cases = [[self.jars[0]], self.jars + [self.jars[0]],
                 self.jars + [self.dist / "extra.jar"],
                 [self.dist / "lib/*"], [Path("relative.jar")]]
        for entries in cases:
            with self.subTest(entries=entries), self.assertRaises(AssertionError):
                runner.actual_classpath_identity(os.pathsep.join(map(str, entries)),
                                                 self.jars, self.jar_sha)

    def test_real_classpath_jar_byte_change_is_rejected(self):
        self.jars[0].write_bytes(b"changed")
        with self.assertRaises(AssertionError):
            runner.actual_classpath_identity(os.pathsep.join(map(str, self.jars)),
                                             self.jars, self.jar_sha)

    def test_added_runtime_log_is_preserved_separately_from_zip_identity(self):
        log = self.dist / "logs/log_datanode_all.log"
        log.parent.mkdir()
        log.write_bytes(b"actual runtime log")
        result = runner.distribution_identity(self.dist, self.baseline)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["generatedRuntimeLogs"],
                         [{"path": "logs/log_datanode_all.log", "sha256": runner.sha(log),
                           "bytes": log.stat().st_size}])
        self.assertTrue(result["allPackagedFilesUnchanged"])
        self.assertTrue(result["exactLibrarySetUnchanged"])

    def test_changed_original_zip_file_remains_a_failure(self):
        (self.dist / "conf/iotdb-system.properties").write_bytes(b"changed=true")
        result = runner.distribution_identity(self.dist, self.baseline)
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["packagedFilesChanged"], ["conf/iotdb-system.properties"])

    def test_missing_original_zip_file_remains_a_failure(self):
        (self.dist / "conf/iotdb-system.properties").unlink()
        result = runner.distribution_identity(self.dist, self.baseline)
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["packagedFilesMissing"], ["conf/iotdb-system.properties"])

    def test_extra_library_is_rejected(self):
        (self.dist / "lib/extra.jar").write_bytes(b"extra")
        result = runner.distribution_identity(self.dist, self.baseline)
        self.assertEqual(result["status"], "FAIL")
        self.assertFalse(result["exactLibrarySetUnchanged"])

    def test_lossless_archive_keeps_generated_distribution_logs(self):
        ram = self.dist / "runtime"
        copied = ram / "distribution"
        (copied / "lib").mkdir(parents=True)
        (copied / "logs").mkdir()
        (copied / "lib/original.jar").write_bytes(b"immutable package original")
        (copied / "logs/log_datanode_all.log").write_bytes(b"generated runtime evidence")
        (ram / "raw-query.json").write_bytes(b"actual query evidence")
        evidence = self.dist / "evidence"
        evidence.mkdir()
        runtime = runner.Runtime.__new__(runner.Runtime)
        runtime.ram, runtime.dist, runtime.evidence = ram, copied, evidence
        runtime.distribution_runtime_paths = {"logs/log_datanode_all.log"}
        runtime.m = {"persistentArchiveBudgetBytes": 1048576,
                     "package": {"path": "/fixture-only-package.zip", "sha256": "fixture"}}
        runtime.archive()
        members = runner.read(evidence / "archive-members.json")
        self.assertEqual({item["path"] for item in members},
                         {"distribution/logs/log_datanode_all.log", "raw-query.json"})
        receipt = runner.read(evidence / "archive-receipt.json")
        self.assertEqual(receipt["status"], "PASS")
        self.assertTrue(receipt["allMembersDecompressedAndShaChecked"])

    def test_nonlog_addition_under_logs_is_rejected(self):
        log = self.dist / "logs/payload.jar"
        log.parent.mkdir()
        log.write_bytes(b"unexpected executable")
        result = runner.distribution_identity(self.dist, self.baseline)
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["unexpectedAddedFiles"], ["logs/payload.jar"])


class ProfiledTerminalStatisticsTests(unittest.TestCase):
    def source(self):
        return {'statusCode': 200, 'state': 'FAILED', 'isTerminalSet': True, 'isTerminal': True,
                'dataRegion': 'dataRegion-1', 'snapshotChecksum': 'snapshot', 'planIdentityChecksum': 'plan',
                'drivers': [{'firstProcessPresent': True, 'firstProcessNanos': 1,
                             'wallPresent': True, 'wallNanos': 1}],
                'operators': [{'operatorType': 'TableScanOperator', 'nextCalledCount': 1}]}

    def missing(self):
        return {'statusCode': 301, 'rawThrift': {'23': {'lst': ['str', 1, 'NOT_FOUND']}}}

    def test_exact_unprofiled_auxiliary_record_is_preserved_without_becoming_witness(self):
        source, missing = self.source(), self.missing()
        available, unprofiled, started, scans, emitted = runner.profiled_terminal_statistics([missing, source])
        self.assertEqual(available, [source])
        self.assertEqual(unprofiled, [missing])
        self.assertEqual(started, [source])
        self.assertEqual(scans, [source])
        self.assertEqual(emitted, [])

    def test_all_missing_empty_or_no_failed_source_is_rejected(self):
        source = self.source()
        source['state'] = 'FINISHED'
        for responses in ([], [self.missing()], [source, self.missing()]):
            with self.subTest(responses=responses), self.assertRaises(AssertionError):
                runner.profiled_terminal_statistics(responses)

    def test_rpc_failure_unknown_status_or_nonexact_not_found_is_rejected(self):
        for bad in ({'rpcFailure': 'connection refused'}, {'statusCode': 500},
                    {'statusCode': 301}, {'statusCode': 301, 'rawThrift': {'23': {'lst': ['str', 1, 'UNKNOWN']}}},
                    {'statusCode': 301, 'rawThrift': {'23': {'lst': ['str', 2, 'NOT_FOUND', 'OTHER']}}}):
            with self.subTest(bad=bad), self.assertRaises(AssertionError):
                runner.profiled_terminal_statistics([self.source(), bad])

    def test_available_nonterminal_or_missing_terminal_flag_is_rejected(self):
        for change in ({'state': 'RUNNING'}, {'isTerminal': False}, {'isTerminalSet': False}):
            source = self.source()
            source.update(change)
            with self.subTest(change=change), self.assertRaises(AssertionError):
                runner.profiled_terminal_statistics([source])

    def test_missing_started_driver_data_region_scan_or_checksum_is_rejected(self):
        for change in ({'dataRegion': None}, {'drivers': []}, {'operators': []},
                       {'snapshotChecksum': None}, {'planIdentityChecksum': None}):
            source = self.source()
            source.update(change)
            with self.subTest(change=change), self.assertRaises(AssertionError):
                runner.profiled_terminal_statistics([source, self.missing()])

    def test_real_driver_emission_can_witness_data_without_operator_instrumentation(self):
        source = self.source()
        source['operators'] = []
        source['drivers'][0].update(outputRowsPresent=True, outputRows=1,
                                     tsBlockCountPresent=True, tsBlockCount=1)
        self.assertEqual(runner.profiled_terminal_statistics([source])[4], [source])

    def test_zero_or_absent_first_wall_and_partial_emission_are_rejected(self):
        for change in ({'firstProcessPresent': False}, {'firstProcessNanos': 0},
                       {'wallPresent': False}, {'wallNanos': 0}):
            source = self.source()
            source['drivers'][0].update(change)
            with self.subTest(change=change), self.assertRaises(AssertionError):
                runner.profiled_terminal_statistics([source])
        source = self.source()
        source['operators'] = []
        source['drivers'][0].update(outputRowsPresent=True, outputRows=1,
                                     tsBlockCountPresent=True, tsBlockCount=0)
        with self.assertRaises(AssertionError):
            runner.profiled_terminal_statistics([source])


class ExactDriverMemoryFailureProvenanceTests(unittest.TestCase):
    task = '20261102_221628_00012_1.2.0.0'
    message = '301: DriverTask ' + task + ' is aborted by internal error scheduled'
    numeric = 'java.lang.IllegalArgumentException: bytesToReserve should be in (0,maxBytesPerFI]. maxBytesPerFI: 3868689, bytesToReserve: 8388785'

    def log(self, task=None, cause=None):
        return ('2026-11-03 06:16:28,261 [Query-Worker-Thread-0$' + (task or self.task)
                + '] WARN AbstractDriverThread - [ExecuteFailed]\n' + (cause or self.numeric) + '\n')

    def test_client_numeric_exception_still_requires_real_large_data_request(self):
        result = runner.memory_failure_provenance(self.numeric, '')
        self.assertEqual(result['source'], 'NUMERIC_CLIENT_MEMORY_POOL_EXCEPTION')
        self.assertEqual(result['numericMemoryPoolFailure']['actualRequestBytes'], 8388785)

    def test_generic_scheduler_exception_requires_the_exact_driver_and_immediate_cause(self):
        result = runner.memory_failure_provenance(self.message, self.log())
        self.assertEqual(result['queryId'], '20261102_221628_00012_1')
        self.assertEqual(result['fragmentInstanceId'], '20261102_221628_00012_1.2.0')
        self.assertEqual(result['driverTaskId'], self.task)
        self.assertEqual(result['numericMemoryPoolFailure'],
                         {'actualRequestBytes': 8388785, 'actualCapBytes': 3868689})

    def test_unrelated_query_driver_or_thread_is_rejected_even_with_real_numbers(self):
        for log in (self.log(task='20261102_221628_00013_1.2.0.0'),
                    self.log(task='20261102_221628_00012_1.2.0.1'),
                    self.log().replace('Query-Worker-Thread', 'Query-Sentinel-Thread'),
                    self.numeric):
            with self.subTest(log=log), self.assertRaises(AssertionError):
                runner.memory_failure_provenance(self.message, log)

    def test_other_error_schema_small_request_and_duplicate_event_are_rejected(self):
        for message, log in (('generic failure', self.log()),
                             (self.message, self.log(cause='Fetch Table Device Schema failed bytesToReserve: 68B, maxBytesCanReserve: 62B')),
                             (self.message, self.log(cause='bytesToReserve: 1048576B, maxBytesCanReserve: 2000B')),
                             (self.message, self.log() + self.log()),
                             (self.message, self.log(cause='IllegalStateException: bad state'))):
            with self.subTest(message=message, log=log), self.assertRaises(AssertionError):
                runner.memory_failure_provenance(message, log)


class ExactFailedSourceBindingTests(unittest.TestCase):
    def fixture(self):
        source = ProfiledTerminalStatisticsTests().source()
        source.update(queryId='query', fragmentInstanceId='query.2.0')
        source['drivers'][0]['driverTaskId'] = 'query.2.0.0'
        identity = {'queryId': 'query', 'fragmentInstanceId': 'query.2.0', 'driverTaskId': 'query.2.0.0'}
        return source, identity

    def test_the_same_query_fi_and_started_driver_are_required(self):
        source, identity = self.fixture()
        self.assertEqual(runner.profiled_terminal_statistics([source], identity)[2], [source])
        for key, wrong in (('queryId', 'other'), ('fragmentInstanceId', 'query.3.0'),
                           ('driverTaskId', 'query.2.0.1')):
            bad = dict(identity)
            bad[key] = wrong
            with self.subTest(key=key), self.assertRaises(AssertionError):
                runner.profiled_terminal_statistics([source], bad)

    def test_another_started_driver_cannot_cover_missing_timing_of_the_failed_driver(self):
        source, identity = self.fixture()
        source['drivers'].append(dict(source['drivers'][0], driverTaskId='query.2.0.1'))
        source['drivers'][0]['wallPresent'] = False
        with self.assertRaises(AssertionError):
            runner.profiled_terminal_statistics([source], identity)


if __name__ == '__main__':
    unittest.main()
