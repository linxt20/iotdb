#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Verify archived E2E and benchmark evidence against the staged case manifest.

This tool is deliberately read-only with respect to IoTDB. It does not invoke a CLI, change DOP,
restart a node, or generate a benchmark. Run the E2E and matrix runners first, then use this tool
to make the acceptance decision reproducible from their archived artifacts.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "manifests" / "multi-query-cases.json"
STATIC_STATUSES = {"static_parallel_candidate", "required_fallback_control"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path, label: str) -> dict[str, Any]:
    if not path.is_file():
        raise ValueError(f"{label} does not exist: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValueError(f"{label} is not valid JSON: {path}") from error
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object: {path}")
    return payload


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def case_index(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    if manifest.get("schema_version") != 1:
        raise ValueError("case manifest schema_version must be 1")
    cases = manifest.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError("case manifest must contain a non-empty cases array")
    indexed: dict[str, dict[str, Any]] = {}
    for case in cases:
        if not isinstance(case, dict) or not isinstance(case.get("id"), str):
            raise ValueError("every case must have a string id")
        case_id = case["id"]
        if case_id in indexed:
            raise ValueError(f"duplicate case id: {case_id}")
        if case.get("status") not in STATIC_STATUSES | {"baseline_only_until_hash_repartition"}:
            raise ValueError(f"unsupported status for {case_id}: {case.get('status')!r}")
        dops = case.get("required_dops")
        if not isinstance(dops, list) or not dops or any(not isinstance(dop, int) for dop in dops):
            raise ValueError(f"case {case_id} must declare integer required_dops")
        indexed[case_id] = case
    return indexed


def verify_e2e(output: Path, cases: dict[str, dict[str, Any]]) -> dict[str, Any]:
    report = read_json(output / "acceptance.json", "E2E acceptance report")
    comparisons = report.get("comparisons")
    if report.get("accepted") is not True or not isinstance(comparisons, list):
        raise ValueError("E2E acceptance report is not accepted or lacks comparisons")
    results = {item.get("name"): item for item in comparisons if isinstance(item, dict)}
    required = {
        str(case["e2e_comparison"]): case_id
        for case_id, case in cases.items()
        if "e2e_comparison" in case
    }
    missing = sorted(name for name in required if name not in results)
    if missing:
        raise ValueError(f"E2E report lacks required comparisons: {', '.join(missing)}")
    failed = [name for name in required if results[name].get("matched") is not True]
    if failed:
        raise ValueError(f"E2E comparison did not match: {', '.join(failed)}")
    return {
        "output": str(output),
        "accepted": True,
        "comparisons": {name: {"case": required[name], "matched": True} for name in sorted(required)},
    }


def attempt_dops(matrix_output: Path) -> dict[str, set[int]]:
    attempts = matrix_output / "summary" / "attempts.csv"
    if not attempts.is_file():
        raise ValueError(f"benchmark attempts CSV does not exist: {attempts}")
    observed: dict[str, set[int]] = {}
    with attempts.open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        required_columns = {"query_id", "dop", "query_ms"}
        if not reader.fieldnames or not required_columns.issubset(reader.fieldnames):
            raise ValueError(f"benchmark attempts CSV lacks {sorted(required_columns)}: {attempts}")
        for row in reader:
            query_id = row["query_id"]
            try:
                dop = int(row["dop"])
                float(row["query_ms"])
            except (TypeError, ValueError) as error:
                raise ValueError(f"invalid benchmark observation for {query_id!r}: {row}") from error
            observed.setdefault(query_id, set()).add(dop)
    return observed


def validation_report(validation_dir: Path, case_id: str, dop: int) -> dict[str, Any]:
    return read_json(validation_dir / f"{case_id}-dop-{dop}.json", "validation report")


def verify_matrix(
    matrix_output: Path, validation_dir: Path, cases: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    matrix_manifest = read_json(matrix_output / "manifest.json", "benchmark manifest")
    configured_queries = matrix_manifest.get("queries")
    if not isinstance(configured_queries, list):
        raise ValueError("benchmark manifest lacks a query list")
    observed = attempt_dops(matrix_output)
    accepted: dict[str, Any] = {}
    for case_id, case in cases.items():
        if case["status"] not in STATIC_STATUSES:
            continue
        if case_id not in configured_queries:
            raise ValueError(f"benchmark manifest did not execute required static case: {case_id}")
        required_dops = set(case["required_dops"])
        missing_dops = sorted(required_dops - observed.get(case_id, set()))
        if missing_dops:
            raise ValueError(f"benchmark is missing {case_id} DOP observations: {missing_dops}")
        validation = {}
        for dop in sorted(required_dops - {1}):
            report = validation_report(validation_dir, case_id, dop)
            if report.get("matched") is not True:
                raise ValueError(f"result validation failed for {case_id} at DOP={dop}")
            expected_ordered = case["comparison"] == "ordered"
            if report.get("ordered") is not expected_ordered:
                raise ValueError(f"validation ordering mode is wrong for {case_id} at DOP={dop}")
            validation[str(dop)] = {"matched": True, "ordered": expected_ordered}
        accepted[case_id] = {"dops": sorted(required_dops), "validation": validation}
    return {"output": str(matrix_output), "accepted": True, "cases": accepted}


def baseline_status(cases: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        case_id: {
            "status": case["status"],
            "required_dops": case["required_dops"],
            "claim_prohibited": "hash repartition and parallel acceleration",
        }
        for case_id, case in cases.items()
        if case["status"] == "baseline_only_until_hash_repartition"
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--stage", choices=("e2e", "matrix", "full"), default="full")
    parser.add_argument("--case-manifest", default=DEFAULT_MANIFEST, type=Path)
    parser.add_argument("--e2e-output", type=Path)
    parser.add_argument("--matrix-output", type=Path)
    parser.add_argument("--validation-dir", type=Path)
    args = parser.parse_args()

    if args.output.exists() and any(args.output.iterdir()):
        parser.error("--output must be new or empty so acceptance evidence is never overwritten")
    if args.stage in {"e2e", "full"} and args.e2e_output is None:
        parser.error("--e2e-output is required for the selected stage")
    if args.stage in {"matrix", "full"} and (args.matrix_output is None or args.validation_dir is None):
        parser.error("--matrix-output and --validation-dir are required for the selected stage")

    args.output.mkdir(parents=True, exist_ok=True)
    try:
        manifest = read_json(args.case_manifest, "case manifest")
        cases = case_index(manifest)
        report: dict[str, Any] = {
            "completed_at_utc": utc_now(),
            "stage": args.stage,
            "case_manifest": str(args.case_manifest),
            "case_manifest_sha256": sha256(args.case_manifest),
            "baseline_only_until_hash_repartition": baseline_status(cases),
        }
        if args.stage in {"e2e", "full"}:
            report["e2e"] = verify_e2e(args.e2e_output, cases)
        if args.stage in {"matrix", "full"}:
            report["matrix"] = verify_matrix(args.matrix_output, args.validation_dir, cases)
        report["accepted"] = True
        (args.output / "staged-acceptance.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    except (OSError, ValueError) as error:
        (args.output / "failure.txt").write_text(f"{error}\n", encoding="utf-8")
        print(f"staged acceptance failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
