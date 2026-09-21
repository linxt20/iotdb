#!/usr/bin/env python3
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

"""Read-only acceptance gate for a complete P0 server benchmark evidence bundle.

This verifier never connects to, starts, stops, reconfigures, or clears a server. It refuses
to label a matrix complete when a required DOP/cache/query cell, server resource metric, raw
attempt, configuration/SHA capture, or DOP=1 result-equivalence report is absent.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import tempfile
from pathlib import Path
from typing import Any


P0_DOPS = [1, 2, 4, 8, 16]
P0_CACHE_MODES = ["warm", "cold"]
P0_QUERIES = ["scan_filter", "filter_project", "ordered_scan", "top_k", "group_by", "self_join"]
REQUIRED_METRICS = ["query_ms", "cpu_pct", "peak_rss_bytes", "shuffle_bytes"]
MIN_WARMUPS_PER_CELL = 2
MIN_MEASURED_SAMPLES_PER_CELL = 7


def read_json(path: Path, problems: list[str]) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        problems.append(f"cannot read {path}: {error}")
        return None
    if not isinstance(value, dict):
        problems.append(f"{path} must contain a JSON object")
        return None
    return value


def numeric_metric(value: Any, name: str) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and (number > 0 if name == "query_ms" else number >= 0)


def check_manifest(output: Path, problems: list[str]) -> None:
    manifest = read_json(output / "manifest.json", problems)
    if manifest is None:
        return
    if manifest.get("dop_sequence") != [1, 2, 4, 8, 16, 1]:
        problems.append("manifest dop_sequence must be exactly [1, 2, 4, 8, 16, 1]")
    modes = manifest.get("cache_modes")
    if not isinstance(modes, list) or set(modes) != set(P0_CACHE_MODES):
        problems.append("manifest cache_modes must contain exactly warm and cold")
    queries = manifest.get("queries")
    if not isinstance(queries, list) or set(queries) != set(P0_QUERIES):
        problems.append("manifest queries must contain the six P0 workload classes")
    if manifest.get("warmups") != MIN_WARMUPS_PER_CELL:
        problems.append(f"manifest warmups must be exactly {MIN_WARMUPS_PER_CELL}")
    if manifest.get("iterations") != MIN_MEASURED_SAMPLES_PER_CELL:
        problems.append(f"manifest iterations must be exactly {MIN_MEASURED_SAMPLES_PER_CELL}")


def check_environment(output: Path, problems: list[str]) -> None:
    environment = read_json(output / "environment" / "environment.json", problems)
    if environment is None:
        return
    git = environment.get("git")
    head = git.get("head") if isinstance(git, dict) else None
    if not isinstance(head, dict) or head.get("exit_code") != 0 or not str(head.get("stdout", "")).strip():
        problems.append("environment must capture a successful immutable git HEAD SHA")
    configs = environment.get("configs")
    if not isinstance(configs, list) or not configs or any(item.get("missing") for item in configs if isinstance(item, dict)):
        problems.append("environment must contain at least one successfully copied server config")


def check_attempts(output: Path, problems: list[str]) -> None:
    attempts_path = output / "summary" / "attempts.csv"
    if not attempts_path.is_file():
        problems.append(f"missing {attempts_path}")
        return
    try:
        with attempts_path.open(newline="", encoding="utf-8") as source:
            rows = list(csv.DictReader(source))
    except OSError as error:
        problems.append(f"cannot read {attempts_path}: {error}")
        return
    counts: dict[tuple[str, int, str], int] = {}
    for row_number, row in enumerate(rows, start=2):
        try:
            key = (row["query_id"], int(row["dop"]), row["cache_mode"])
        except (KeyError, ValueError):
            problems.append(f"{attempts_path}:{row_number} lacks a valid query_id/dop/cache_mode")
            continue
        counts[key] = counts.get(key, 0) + 1
        for metric in REQUIRED_METRICS:
            if not numeric_metric(row.get(metric), metric):
                problems.append(f"{attempts_path}:{row_number} has missing/invalid {metric}")
        attempt_dir = row.get("attempt_dir")
        if not attempt_dir or not (Path(attempt_dir) / "record.json").is_file():
            problems.append(f"{attempts_path}:{row_number} is missing its raw record.json")
    for query in P0_QUERIES:
        for dop in P0_DOPS:
            for mode in P0_CACHE_MODES:
                count = counts.get((query, dop, mode), 0)
                if count == 0:
                    problems.append(f"missing measured cell query={query}, dop={dop}, cache={mode}")
                elif count < MIN_MEASURED_SAMPLES_PER_CELL:
                    problems.append(
                        f"insufficient measured samples query={query}, dop={dop}, cache={mode}: "
                        f"expected at least {MIN_MEASURED_SAMPLES_PER_CELL}, got {count}"
                    )


def check_results(output: Path, problems: list[str]) -> None:
    for query in P0_QUERIES:
        for dop in P0_DOPS:
            report_path = output / "validation" / query / f"dop-{dop}" / "report.json"
            report = read_json(report_path, problems)
            if report is not None and report.get("matched") is not True:
                problems.append(f"result equivalence failed: {report_path}")


def verify(output: Path) -> dict[str, Any]:
    problems: list[str] = []
    if not output.is_dir():
        return {"accepted": False, "output": str(output), "problems": ["output directory does not exist"]}
    check_manifest(output, problems)
    check_environment(output, problems)
    check_attempts(output, problems)
    check_results(output, problems)
    return {"accepted": not problems, "output": str(output), "problems": problems}


def self_test() -> int:
    with tempfile.TemporaryDirectory() as temporary:
        output = Path(temporary)
        (output / "environment").mkdir()
        (output / "summary").mkdir()
        (output / "validation").mkdir()
        (output / "manifest.json").write_text(json.dumps({
            "dop_sequence": [1, 2, 4, 8, 16, 1], "cache_modes": ["warm", "cold"],
            "queries": P0_QUERIES, "warmups": MIN_WARMUPS_PER_CELL,
            "iterations": MIN_MEASURED_SAMPLES_PER_CELL,
        }), encoding="utf-8")
        (output / "environment" / "environment.json").write_text(json.dumps({
            "git": {"head": {"exit_code": 0, "stdout": "0123456789abcdef\\n"}},
            "configs": [{"source": "/isolated/conf/iotdb-system.properties"}],
        }), encoding="utf-8")
        rows = []
        for query in P0_QUERIES:
            for dop in P0_DOPS:
                for mode in P0_CACHE_MODES:
                    attempt = output / "raw" / query / str(dop) / mode
                    attempt.mkdir(parents=True)
                    (attempt / "record.json").write_text("{}", encoding="utf-8")
                    for iteration in range(MIN_MEASURED_SAMPLES_PER_CELL):
                        rows.append({"query_id": query, "dop": dop, "cache_mode": mode,
                                     "query_ms": "1", "cpu_pct": "0", "peak_rss_bytes": "0",
                                     "shuffle_bytes": "0", "attempt_dir": str(attempt)})
        with (output / "summary" / "attempts.csv").open("w", newline="", encoding="utf-8") as target:
            writer = csv.DictWriter(target, fieldnames=rows[0])
            writer.writeheader()
            writer.writerows(rows)
        for query in P0_QUERIES:
            for dop in P0_DOPS:
                report = output / "validation" / query / f"dop-{dop}" / "report.json"
                report.parent.mkdir(parents=True)
                report.write_text('{"matched": true}', encoding="utf-8")
        if not verify(output)["accepted"]:
            return 1
        (output / "validation" / "top_k" / "dop-16" / "report.json").write_text(
            '{"matched": false}', encoding="utf-8")
        if verify(output)["accepted"]:
            return 1
        (output / "validation" / "top_k" / "dop-16" / "report.json").write_text(
            '{"matched": true}', encoding="utf-8")
        with (output / "summary" / "attempts.csv").open("w", newline="", encoding="utf-8") as target:
            writer = csv.DictWriter(target, fieldnames=rows[0])
            writer.writeheader()
            writer.writerows(rows[:-1])
        if verify(output)["accepted"]:
            return 1
    print("verify_p0_matrix self-test passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="completed run_matrix.py output directory")
    parser.add_argument("--self-test", action="store_true", help="run local parser-only tests")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if args.output is None:
        parser.error("--output is required unless --self-test is used")
    result = verify(args.output)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["accepted"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
