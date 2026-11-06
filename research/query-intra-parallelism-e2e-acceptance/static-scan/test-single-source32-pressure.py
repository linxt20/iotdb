#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to You under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
# See the License for the specific language governing permissions
# and limitations under the License.

"""Pure parser/type/multiset tests; no JVM, services, attach, fixture load or pressure run."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("pressure_runner", ROOT / "run-single-source32-pressure.py")
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)
OUTPUT = None


class FullResultVerificationTests(unittest.TestCase):
    def write(self, name, text):
        path = OUTPUT / name
        path.write_text(text)
        return path

    def small_receipt(self, strict, suffix):
        path = self.write("strict-b4-" + suffix + ".json", json.dumps(strict))
        manifest = {"acceptedHelperSource": {"sha256": "helper"},
                    "candidate": {"sourceManifest": {"path": "/source", "sha256": "new-source"},
                                  "package": {"path": "/package", "sha256": "new-package"}}}
        accepted = {"status": "PASS", "acceptedHelperSourceSha256": "helper",
                    "candidateSourceManifest": manifest["candidate"]["sourceManifest"].copy(),
                    "candidatePackage": manifest["candidate"]["package"].copy(),
                    "strictSourceB4Receipt": {"path": str(path), "sha256": RUNNER.sha(path)}}
        return accepted, manifest

    def strict_receipt(self):
        return {"status": "PASS", "observedNumbers": {"38": 32, "39": 1, "40": 125703},
                "observedRawThrift": {"38": {"i32": 32}, "39": {"i32": 1}, "40": {"i64": 125703}}}

    def test_strict_small_receipt_accepts_current_canonical_raw_values(self):
        accepted, manifest = self.small_receipt(self.strict_receipt(), "valid")
        RUNNER.check_completed_small_acceptance(accepted, manifest)

    def test_generic_small_result_pass_cannot_bypass_strict_b4(self):
        accepted, manifest = self.small_receipt(self.strict_receipt(), "generic")
        del accepted["strictSourceB4Receipt"]
        with self.assertRaisesRegex(ValueError, "missing strict"):
            RUNNER.check_completed_small_acceptance(accepted, manifest)

    def test_stale_source_or_package_small_pass_is_rejected(self):
        for field in ("candidateSourceManifest", "candidatePackage"):
            accepted, manifest = self.small_receipt(self.strict_receipt(), field)
            accepted[field]["sha256"] = "old"
            with self.assertRaisesRegex(ValueError, "different candidate"):
                RUNNER.check_completed_small_acceptance(accepted, manifest)

    def test_mixed_locality_raw_48_is_rejected_even_if_summary_says_32(self):
        strict = self.strict_receipt()
        strict["observedRawThrift"]["38"]["i32"] = 48
        strict["observedRawThrift"]["40"]["i64"] = 83802
        accepted, manifest = self.small_receipt(strict, "raw-mismatch")
        with self.assertRaisesRegex(ValueError, "original typed Thrift"):
            RUNNER.check_completed_small_acceptance(accepted, manifest)

    def test_pretty_rows_across_read_boundaries(self):
        rows = [RUNNER.row_at(index) for index in range(8192)]
        path = self.write("pretty-boundaries.json", json.dumps(rows, indent=2))
        self.assertEqual(list(RUNNER.stream_array(path)), rows)

    def test_duplicate_rows_are_preserved_in_complete_sort(self):
        rows = [[2, None], [1, 0.5], [2, None], [1, 0.5], [2, 3.5]]
        result = RUNNER.external_sort(iter(rows), OUTPUT, "duplicate-check")
        expected = "[1,0.5]\n[1,0.5]\n[2,3.5]\n[2,null]\n[2,null]\n"
        self.assertEqual(result["rows"], 5)
        self.assertEqual(Path(result["path"]).read_text(), expected)
        self.assertEqual(result["sha256"], hashlib.sha256(expected.encode()).hexdigest())

    def test_trailing_comma_is_rejected(self):
        with self.assertRaises(ValueError):
            list(RUNNER.stream_array(self.write("trailing-comma.json", "[[1,0.5],]")))

    def test_truncated_result_is_rejected(self):
        with self.assertRaises(ValueError):
            list(RUNNER.stream_array(self.write("truncated.json", "[[1,0.5]")))

    def test_trailing_data_is_rejected(self):
        with self.assertRaises(ValueError):
            list(RUNNER.stream_array(self.write("trailing-data.json", "[[1,0.5]]null")))

    def test_missing_separator_is_rejected(self):
        with self.assertRaises(ValueError):
            list(RUNNER.stream_array(self.write("missing-separator.json", "[[1,0.5][2,3.5]]")))

    def test_typed_rows_reject_booleans_and_wrong_number_types(self):
        for row in ([True, 1.5], [1.0, 1.5], [1, 1], [1, float("nan")], [None, 1.5], [1]):
            with self.subTest(row=row), self.assertRaises(ValueError):
                RUNNER.canonical_row(row)

    def test_null_and_double_are_kept_distinct(self):
        self.assertEqual(RUNNER.canonical_row([1, None]), "[1,null]\n")
        self.assertEqual(RUNNER.canonical_row([1, 0.5]), "[1,0.5]\n")

    def test_failed_raw_evidence_archive_round_trip(self):
        source = OUTPUT / "archive-source"
        source.mkdir()
        (source / "raw-result.json").write_text("[[1,null],[2,3.5],[2,3.5]]\n")
        (source / "failure.log").write_text("Original failure must be preserved.\n")
        (source / "pressure-acceptance.json").write_text('{"status":"FAIL"}\n')
        persistent = OUTPUT / "archive-persistent"
        RUNNER.persist_evidence(source, {"persistentEvidenceRoot": str(persistent),
                                        "persistentArchiveBudgetBytes": 1048576})
        receipt = json.loads((persistent / "archive-receipt.json").read_text())
        self.assertEqual(receipt["pressureRuntimeStatus"], "FAIL")
        self.assertTrue(receipt["allMembersDecompressedAndShaVerified"])
        self.assertEqual(receipt["members"], 3)
        self.assertTrue((source / "failure.log").is_file())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args, unittest_args = parser.parse_known_args()
    OUTPUT = args.output.resolve()
    OUTPUT.mkdir()
    unittest.main(argv=[__file__, *unittest_args])
