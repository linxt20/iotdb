#!/usr/bin/env python3
"""Pure Python receipt/mutation guard tests. No Java or project test execution."""
import argparse
import importlib.util
from pathlib import Path
import unittest
import os
import tarfile

SPEC = importlib.util.spec_from_file_location('negative_runner', Path(__file__).with_name('run-negative-contracts.py'))
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)
OUTPUT = None
TARGET = {'class': 'p.ContractTest', 'method': 'required',
          'assertionMessageRegex': 'REQUIRED_ASSERTION', 'assertionStackRegex': r'p\.ContractTest\.required'}


class ReceiptGuardTests(unittest.TestCase):
    def reports(self, label, body):
        path = OUTPUT / label
        path.mkdir()
        (path / 'TEST-p.ContractTest.xml').write_text('<testsuite>' + body + '</testsuite>')
        return path

    def failure(self, message='REQUIRED_ASSERTION', tag='failure', kind='java.lang.AssertionError'):
        return '<testcase classname="p.ContractTest" name="required"><' + tag + ' type="' + kind + '" message="' + message + '">p.ContractTest.required(ContractTest.java:42)</' + tag + '></testcase>'

    def test_powermock_extra_passed_methods_do_not_invalidate_unique_target(self):
        path = self.reports('powermock-extra', '<testcase classname="p.ContractTest" name="extra"/>' + self.failure())
        self.assertEqual(len(RUNNER.xml_contract(path, [TARGET], True)), 2)

    def test_duplicate_logical_method_is_rejected(self):
        path = self.reports('duplicate', self.failure() + self.failure().replace('name="required"', 'name="required[0]"'))
        with self.assertRaises(AssertionError):
            RUNNER.xml_contract(path, [TARGET], True)

    def test_confirmed_powermock_failure_and_empty_finish_preserve_both_nodes(self):
        target = dict(TARGET, allowPowerMockFailureFinishArtifact=True)
        path = self.reports("confirmed-empty-finish", self.failure()
                            + '<testcase classname="p.ContractTest" name="required"/>')
        result = RUNNER.xml_contract(path, [target], True)
        self.assertEqual(len(result), 2)
        self.assertEqual({row["providerEventRole"] for row in result},
                         {"SOLE_EXPECTED_ASSERTION_FAILURE", "POWERMOCK_EMPTY_SUCCESS_FINISH_ARTIFACT"})
        self.assertTrue(all(row["completeXmlNode"] for row in result))

    def test_confirmed_powermock_still_rejects_two_failures(self):
        target = dict(TARGET, allowPowerMockFailureFinishArtifact=True)
        path = self.reports("two-failures", self.failure() + self.failure())
        with self.assertRaises(AssertionError):
            RUNNER.xml_contract(path, [target], True)

    def test_confirmed_powermock_still_rejects_two_successes(self):
        target = dict(TARGET, allowPowerMockFailureFinishArtifact=True)
        success = '<testcase classname="p.ContractTest" name="required"/>'
        path = self.reports("two-successes", success + success)
        with self.assertRaises(AssertionError):
            RUNNER.xml_contract(path, [target], True)

    def test_confirmed_powermock_still_rejects_error_plus_empty_finish(self):
        target = dict(TARGET, allowPowerMockFailureFinishArtifact=True)
        path = self.reports("error-empty-finish", self.failure(tag="error", kind="java.lang.NoClassDefFoundError")
                            + '<testcase classname="p.ContractTest" name="required"/>')
        with self.assertRaises(AssertionError):
            RUNNER.xml_contract(path, [target], True)

    def test_confirmed_powermock_still_rejects_flaky_or_rerun_finish(self):
        target = dict(TARGET, allowPowerMockFailureFinishArtifact=True)
        for tag in ("flakyFailure", "rerunFailure", "skipped"):
            path = self.reports("nonempty-finish-" + tag, self.failure()
                                + '<testcase classname="p.ContractTest" name="required"><'
                                + tag + '/></testcase>')
            with self.assertRaises(AssertionError):
                RUNNER.xml_contract(path, [target], True)

    def test_jvm_or_dependency_error_is_not_a_killed_mutant(self):
        path = self.reports('jvm-error', self.failure(tag='error', kind='java.lang.NoClassDefFoundError'))
        with self.assertRaises(AssertionError):
            RUNNER.xml_contract(path, [TARGET], True)

    def test_wrong_assertion_is_not_a_killed_mutant(self):
        path = self.reports('wrong-assertion', self.failure('some other assertion'))
        with self.assertRaises(AssertionError):
            RUNNER.xml_contract(path, [TARGET], True)

    def test_missing_unique_method_is_rejected(self):
        path = self.reports('missing', '<testcase classname="p.ContractTest" name="other"/>')
        with self.assertRaises(AssertionError):
            RUNNER.xml_contract(path, [TARGET], True)

    def test_restored_method_must_pass(self):
        path = self.reports('restored-fails', self.failure())
        with self.assertRaises(AssertionError):
            RUNNER.xml_contract(path, [TARGET], False)

    def test_mutation_anchor_must_be_unique(self):
        with self.assertRaises(AssertionError):
            RUNNER.mutate('anchor anchor', [{'old': 'anchor', 'new': 'fault'}])
        self.assertEqual(RUNNER.mutate('before anchor after', [{'old': 'anchor', 'new': 'fault'}]), 'before fault after')

    def test_new_pvc_work_root_is_accepted(self):
        identity = RUNNER.validate_work_root(OUTPUT / "new-work", OUTPUT)
        self.assertEqual(identity["pvcRealpath"], str(OUTPUT.resolve()))
        self.assertNotEqual(identity["filesystem"], "tmpfs")

    def test_existing_work_root_is_rejected(self):
        with self.assertRaises(AssertionError):
            RUNNER.validate_work_root(OUTPUT, OUTPUT)

    def test_work_root_outside_bound_pvc_is_rejected(self):
        with self.assertRaises(AssertionError):
            RUNNER.validate_work_root(Path("/unexpected-new-work"), OUTPUT)

    def test_lossless_stream_archive_preserves_hardlink_and_symlink_file_bytes(self):
        work = OUTPUT / "archive-source"
        work.mkdir()
        original = work / "original.bin"
        original.write_bytes(b"real bytes required in each archived member")
        os.link(original, work / "hardlink.bin")
        (work / "symlink.bin").symlink_to(original)
        output = OUTPUT / "archive-evidence"
        output.mkdir()
        runner = RUNNER.Runner.__new__(RUNNER.Runner)
        runner.work, runner.output = work, output
        runner.archive()
        receipt = RUNNER.read(output / "archive-receipt.json")
        self.assertEqual(receipt["status"], "PASS")
        with tarfile.open(receipt["path"], "r|gz") as archive:
            members = []
            for member in archive:
                self.assertTrue(member.isfile())
                self.assertEqual(archive.extractfile(member).read(), original.read_bytes())
                members.append(member.name)
        self.assertEqual(set(members), {"original.bin", "hardlink.bin", "symlink.bin"})

    def test_main_and_inner_family_excludes_sibling_prefix_classes(self):
        target = OUTPUT / 'family-target'
        classes = target / 'classes/p'
        classes.mkdir(parents=True)
        for name in ['Actual.class', 'Actual$Inner.class', 'ActualSibling.class', 'ActualExtra.class']:
            (classes / name).write_bytes(b'fixture-only')
        runner = RUNNER.Runner.__new__(RUNNER.Runner)
        runner.target = target
        self.assertEqual([path.name for path in runner.family('p.Actual')], ['Actual$Inner.class', 'Actual.class'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    args, extra = parser.parse_known_args()
    OUTPUT = args.output
    OUTPUT.mkdir()
    unittest.main(argv=[__file__, *extra])
