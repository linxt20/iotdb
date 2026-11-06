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

"""Read-only result and fallback acceptance for six table-model query families.

The runner addresses correctness only. It sends EXPLAIN and SELECT statements to two explicitly
named, already-running endpoints and writes local evidence; it never starts, stops, reconfigures,
or writes to an IoTDB server. Static candidates are compared as enabled-versus-fallback results.
GROUP BY and equi-join are captured as fallback-only semantic baselines, so a passing report makes
no data-parallel or performance claim for either family.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
WORKLOAD_ROOT = ROOT.parent / "query-intra-parallelism-benchmark"
WORKLOADS = WORKLOAD_ROOT / "validation"
TABLE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\Z")
ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
HASH_SINK_MARKER = "TableHashPartitioningShuffleSinkNode"

# These are deliberately property contracts, not operator-name claims. An operator name alone does
# not establish whether the required ordering/distribution property was preserved or enforced.
STATIC_PROPERTY_EXPECTATIONS = {
    "scan_filter": r"required=Single provided=Arbitrary -> CollectNode",
    "filter_project": r"required=Single provided=Arbitrary -> CollectNode",
    "ordered_scan": (
        r"required=Single\+Ordered\[device_id, time\] "
        r"provided=Single\+Ordered\[device_id, time\] -> satisfied, no enforcer"
    ),
    "top_k": (
        r"required=Single\+Ordered\[time, device_id\] "
        r"provided=Single\+Ordered\[time, device_id\] -> satisfied, no enforcer"
    ),
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def command_output(command: list[str]) -> dict[str, Any]:
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    return {
        "command": command,
        "exit_code": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def find_git_root(start: Path) -> Path | None:
    for candidate in (start.resolve(), *start.resolve().parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def capture_context(output: Path, configs: list[Path]) -> None:
    environment = output / "environment"
    config_dir = environment / "config"
    config_dir.mkdir(parents=True)
    copied_configs = []
    for index, config in enumerate(configs, start=1):
        if not config.is_file():
            raise ValueError(f"configuration file does not exist: {config}")
        # Candidate and control deployments normally use the same configuration basename. Keep
        # both immutable snapshots instead of rejecting a valid two-endpoint acceptance run.
        destination = config_dir / f"{index:02d}-{config.name}"
        shutil.copy2(config, destination)
        copied_configs.append({"source": str(config), "copy": str(destination)})
    git_root = find_git_root(ROOT)
    payload: dict[str, Any] = {"captured_at_utc": utc_now(), "configs": copied_configs}
    if git_root is not None:
        payload["git"] = {
            "root": str(git_root),
            "head": command_output(["git", "-C", str(git_root), "rev-parse", "HEAD"]),
            "status": command_output(["git", "-C", str(git_root), "status", "--short"]),
        }
    else:
        payload["git"] = {"available": False}
    (environment / "environment.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def render_sql(case_id: str, table: str) -> str:
    source = WORKLOADS / f"{case_id}.sql"
    if not source.is_file():
        raise ValueError(f"validation SQL does not exist: {source}")
    lines = []
    for line in source.read_text(encoding="utf-8").splitlines():
        if not line.lstrip().startswith("--"):
            lines.append(line)
    statement = "\n".join(lines).replace("${TABLE}", table).strip()
    if not statement.endswith(";"):
        raise ValueError(f"validation SQL must end with a semicolon: {source}")
    return statement


def endpoint_arguments(args: argparse.Namespace, endpoint: str) -> tuple[Path, str, int]:
    cli = Path(getattr(args, f"{endpoint}_cli"))
    if not cli.is_file():
        raise ValueError(f"CLI executable does not exist: {cli}")
    return cli, getattr(args, f"{endpoint}_host"), getattr(args, f"{endpoint}_port")


def run_cli(
    args: argparse.Namespace,
    endpoint: str,
    statement: str,
    label: str,
    raw_dir: Path,
    password: str,
) -> Path:
    cli, host, port = endpoint_arguments(args, endpoint)
    command = [
        str(cli),
        "-h",
        host,
        "-p",
        str(port),
        "-u",
        args.username,
        "-pw",
        password,
        "-sql_dialect",
        "table",
        "-e",
        " ".join(statement.splitlines()),
    ]
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    name = f"{endpoint}-{label}"
    stdout = raw_dir / f"{name}.stdout"
    stderr = raw_dir / f"{name}.stderr"
    stdout.write_text(completed.stdout, encoding="utf-8")
    stderr.write_text(completed.stderr, encoding="utf-8")
    redacted = command.copy()
    redacted[redacted.index("-pw") + 1] = "<redacted>"
    (raw_dir / f"{name}.command.json").write_text(
        json.dumps({"command": redacted, "exit_code": completed.returncode}, indent=2) + "\n",
        encoding="utf-8",
    )
    if completed.returncode:
        raise RuntimeError(f"CLI failed for {name}; see {stderr}")
    return stdout


def table_rows(path: Path) -> tuple[tuple[str, ...], list[tuple[str, ...]]]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        clean = ANSI_ESCAPE.sub("", line).strip()
        if clean.startswith("|") and clean.endswith("|"):
            rows.append(tuple(cell.strip() for cell in clean[1:-1].split("|")))
    if len(rows) < 2:
        raise RuntimeError(f"no header and data rows found in CLI output: {path}")
    width = len(rows[0])
    if width == 0 or any(len(row) != width for row in rows):
        raise RuntimeError(f"inconsistent CLI table shape: {path}")
    return rows[0], rows[1:]


def write_canonical_csv(path: Path, header: tuple[str, ...], rows: list[tuple[str, ...]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as target:
        writer = csv.writer(target)
        writer.writerow(header)
        writer.writerows(rows)


def digest(rows: list[tuple[str, ...]]) -> str:
    hasher = hashlib.sha256()
    for row in rows:
        hasher.update("\x1f".join(row).encode("utf-8"))
        hasher.update(b"\n")
    return hasher.hexdigest()


def compare(
    case_id: str,
    baseline: tuple[tuple[str, ...], list[tuple[str, ...]]],
    candidate: tuple[tuple[str, ...], list[tuple[str, ...]]],
    ordered: bool,
) -> dict[str, Any]:
    baseline_header, baseline_rows = baseline
    candidate_header, candidate_rows = candidate
    same_header = baseline_header == candidate_header
    same_multiset = Counter(baseline_rows) == Counter(candidate_rows)
    same_order = baseline_rows == candidate_rows
    return {
        "case": case_id,
        "ordered": ordered,
        "same_header": same_header,
        "same_multiset": same_multiset,
        "same_order": same_order,
        "baseline_rows": len(baseline_rows),
        "candidate_rows": len(candidate_rows),
        "baseline_multiset_sha256": digest(sorted(baseline_rows)),
        "candidate_multiset_sha256": digest(sorted(candidate_rows)),
        "matched": same_header and same_multiset and (same_order if ordered else True),
    }


def require_marker(path: Path, marker: str, purpose: str) -> None:
    if marker not in path.read_text(encoding="utf-8"):
        raise RuntimeError(f"{purpose} marker {marker!r} is absent from {path}")


def require_no_marker(path: Path, marker: str, purpose: str) -> None:
    if marker in path.read_text(encoding="utf-8"):
        raise RuntimeError(f"{purpose} unexpectedly contains {marker!r}: {path}")


def require_property_contract(
    path: Path, marker: str, expectation: str, purpose: str
) -> list[str]:
    """Return the exact rendered property rows that satisfy a case-specific contract."""
    content = path.read_text(encoding="utf-8")
    if marker not in content:
        raise RuntimeError(f"{purpose} marker {marker!r} is absent from {path}")
    matches = [
        " ".join(line.split())
        for line in content.splitlines()
        if re.search(expectation, line)
    ]
    if not matches:
        raise RuntimeError(
            f"{purpose} does not satisfy property contract {expectation!r}: {path}"
        )
    return matches


def self_test() -> int:
    rows = (("device_id", "time"), [("d0", "1"), ("d1", "2")])
    reordered = (("device_id", "time"), [("d1", "2"), ("d0", "1")])
    assert compare("unordered", rows, reordered, ordered=False)["matched"]
    assert not compare("ordered", rows, reordered, ordered=True)["matched"]
    assert "${TABLE}" not in render_sql("scan_filter", "benchdb.bench")
    with tempfile.TemporaryDirectory() as temporary:
        plan = Path(temporary) / "plan.txt"
        plan.write_text(
            "Property enforcement:\n"
            "1: required=Single+Ordered[device_id, time] "
            "provided=Single+Ordered[device_id, time] -> satisfied, no enforcer\n",
            encoding="utf-8",
        )
        assert require_property_contract(
            plan,
            "Property enforcement:",
            STATIC_PROPERTY_EXPECTATIONS["ordered_scan"],
            "self-test",
        )
    print(json.dumps({"self_test": "passed"}, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--table", help="unquoted database.table fixture name")
    parser.add_argument("--username", default="root")
    parser.add_argument("--password-env", default="IOTDB_E2E_PASSWORD")
    parser.add_argument("--config", action="append", default=[], type=Path)
    parser.add_argument("--property-marker", default="Property enforcement:")
    for endpoint in ("enabled", "fallback"):
        parser.add_argument(f"--{endpoint}-cli")
        parser.add_argument(f"--{endpoint}-host")
        parser.add_argument(f"--{endpoint}-port", type=int)
    args = parser.parse_args()

    if args.self_test:
        return self_test()
    required = ("output", "table", "enabled_cli", "enabled_host", "enabled_port", "fallback_cli", "fallback_host", "fallback_port")
    missing = [name.replace("_", "-") for name in required if getattr(args, name) is None]
    if missing:
        parser.error(f"required without --self-test: {', '.join('--' + name for name in missing)}")
    if not TABLE_NAME.fullmatch(args.table):
        parser.error("--table must be a simple unquoted database.table identifier")
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("--output must be new or empty so evidence is never overwritten")
    password = os.environ.get(args.password_env)
    if not password:
        parser.error(f"password environment variable is unset or empty: {args.password_env}")

    args.output.mkdir(parents=True, exist_ok=True)
    raw_dir = args.output / "raw"
    canonical_dir = args.output / "canonical"
    raw_dir.mkdir()
    canonical_dir.mkdir()
    static_cases = {
        "scan_filter": False,
        "filter_project": False,
        "ordered_scan": True,
        "top_k": True,
    }
    baseline_cases = ("group_by", "self_join")
    try:
        capture_context(args.output, args.config)
        comparisons = []
        plans = {}
        for case_id, ordered in static_cases.items():
            statement = render_sql(case_id, args.table)
            enabled_plan = run_cli(args, "enabled", f"EXPLAIN {statement}", f"{case_id}-plan", raw_dir, password)
            property_rows = require_property_contract(
                enabled_plan,
                args.property_marker,
                STATIC_PROPERTY_EXPECTATIONS[case_id],
                f"{case_id} property trace",
            )
            plans[case_id] = {
                "enabled_plan": str(enabled_plan),
                "property_marker": args.property_marker,
                "property_contract": STATIC_PROPERTY_EXPECTATIONS[case_id],
                "matched_property_rows": property_rows,
            }
            enabled_rows = table_rows(run_cli(args, "enabled", statement, case_id, raw_dir, password))
            fallback_rows = table_rows(run_cli(args, "fallback", statement, case_id, raw_dir, password))
            write_canonical_csv(canonical_dir / f"{case_id}-enabled.csv", *enabled_rows)
            write_canonical_csv(canonical_dir / f"{case_id}-fallback.csv", *fallback_rows)
            comparisons.append(compare(case_id, fallback_rows, enabled_rows, ordered))

        baselines = {}
        for case_id in baseline_cases:
            statement = render_sql(case_id, args.table)
            plan = run_cli(args, "fallback", f"EXPLAIN {statement}", f"{case_id}-baseline-plan", raw_dir, password)
            require_no_marker(plan, HASH_SINK_MARKER, f"{case_id} fallback plan")
            rows = table_rows(run_cli(args, "fallback", statement, f"{case_id}-baseline", raw_dir, password))
            write_canonical_csv(canonical_dir / f"{case_id}-fallback-baseline.csv", *rows)
            baselines[case_id] = {
                "plan": str(plan),
                "rows": len(rows[1]),
                "multiset_sha256": digest(sorted(rows[1])),
                "hash_repartition_claim": "prohibited",
                "parallel_acceleration_claim": "prohibited",
            }

        report = {
            "completed_at_utc": utc_now(),
            "table": args.table,
            "plans": plans,
            "static_candidate_comparisons": comparisons,
            "fallback_only_baselines": baselines,
            "accepted": all(item["matched"] for item in comparisons),
            "conclusion": (
                "Result equivalence is accepted only for the listed static candidates. This report "
                "contains no performance measurement. GROUP BY and equi-join are fallback-only "
                "semantic baselines and cannot support hash-repartition or acceleration claims."
            ),
        }
        (args.output / "multi-query-acceptance.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["accepted"] else 1
    except (OSError, RuntimeError, ValueError) as error:
        (args.output / "failure.txt").write_text(f"{error}\n", encoding="utf-8")
        print(f"multi-query acceptance failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
