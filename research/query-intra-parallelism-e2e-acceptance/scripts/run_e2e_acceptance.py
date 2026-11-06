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

"""Read-only E2E acceptance for the isolated static-parallelism prototype."""

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
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
TABLE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\Z")


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
    for config in configs:
        if not config.is_file():
            raise ValueError(f"configuration file does not exist: {config}")
        destination = config_dir / config.name
        if destination.exists():
            raise ValueError(f"configuration basenames must be unique: {config.name}")
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


def write_sql_files(query_dir: Path, table: str) -> dict[str, Path]:
    statements = {
        "property_trace": f"EXPLAIN SELECT device_id, time, s1, s2 FROM {table};\n",
        "ordered_plan": (
            f"EXPLAIN ANALYZE SELECT device_id, time, s1, s2 FROM {table} "
            "ORDER BY device_id ASC, time ASC;\n"
        ),
        "ordered_result": (
            f"SELECT device_id, time, s1, s2 FROM {table} ORDER BY device_id ASC, time ASC;\n"
        ),
        "morsel_plan": f"EXPLAIN ANALYZE SELECT device_id, time, s1, s2 FROM {table} WHERE s1 >= 0;\n",
        "morsel_result": f"SELECT device_id, time, s1, s2 FROM {table} WHERE s1 >= 0;\n",
    }
    query_dir.mkdir(parents=True)
    paths = {}
    for name, statement in statements.items():
        path = query_dir / f"{name}.sql"
        path.write_text(statement, encoding="utf-8")
        paths[name] = path
    return paths


def endpoint_arguments(args: argparse.Namespace, endpoint: str) -> tuple[Path, str, int]:
    cli = Path(getattr(args, f"{endpoint}_cli"))
    if not cli.is_file():
        raise ValueError(f"CLI executable does not exist: {cli}")
    return cli, getattr(args, f"{endpoint}_host"), getattr(args, f"{endpoint}_port")


def run_cli(
    args: argparse.Namespace,
    endpoint: str,
    sql_file: Path,
    raw_dir: Path,
    password: str,
) -> Path:
    cli, host, port = endpoint_arguments(args, endpoint)
    sql = " ".join(sql_file.read_text(encoding="utf-8").splitlines())
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
        sql,
    ]
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    label = f"{endpoint}-{sql_file.stem}"
    stdout = raw_dir / f"{label}.stdout"
    stderr = raw_dir / f"{label}.stderr"
    stdout.write_text(completed.stdout, encoding="utf-8")
    stderr.write_text(completed.stderr, encoding="utf-8")
    redacted = command.copy()
    redacted[redacted.index("-pw") + 1] = "<redacted>"
    (raw_dir / f"{label}.command.json").write_text(
        json.dumps({"command": redacted, "exit_code": completed.returncode}, indent=2) + "\n",
        encoding="utf-8",
    )
    if completed.returncode:
        raise RuntimeError(f"CLI failed for {label}; see {stderr}")
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
    name: str,
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
        "name": name,
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--table", required=True, help="unquoted database.table fixture name")
    parser.add_argument("--username", default="root")
    parser.add_argument("--password-env", default="IOTDB_E2E_PASSWORD")
    parser.add_argument("--config", action="append", default=[], type=Path)
    parser.add_argument("--ordered-marker", default="TableMergeSortOperator")
    parser.add_argument("--morsel-marker", default="-morsel-")
    for endpoint in ("enabled", "ordered_fallback", "morsel_fallback"):
        parser.add_argument(f"--{endpoint.replace('_', '-')}-cli", required=True)
        parser.add_argument(f"--{endpoint.replace('_', '-')}-host", required=True)
        parser.add_argument(f"--{endpoint.replace('_', '-')}-port", required=True, type=int)
    args = parser.parse_args()

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
    try:
        capture_context(args.output, args.config)
        sql = write_sql_files(args.output / "queries", args.table)
        property_trace = run_cli(args, "enabled", sql["property_trace"], raw_dir, password)
        require_marker(property_trace, "Property enforcement:", "property trace")

        ordered_plan = run_cli(args, "enabled", sql["ordered_plan"], raw_dir, password)
        require_marker(ordered_plan, args.ordered_marker, "ordered merge-tree")
        ordered_enabled = table_rows(run_cli(args, "enabled", sql["ordered_result"], raw_dir, password))
        ordered_fallback = table_rows(
            run_cli(args, "ordered_fallback", sql["ordered_result"], raw_dir, password)
        )
        write_canonical_csv(canonical_dir / "ordered-enabled.csv", *ordered_enabled)
        write_canonical_csv(canonical_dir / "ordered-fallback.csv", *ordered_fallback)

        morsel_plan = run_cli(args, "enabled", sql["morsel_plan"], raw_dir, password)
        require_marker(morsel_plan, args.morsel_marker, "time-partition morsel")
        morsel_enabled = table_rows(run_cli(args, "enabled", sql["morsel_result"], raw_dir, password))
        morsel_fallback = table_rows(
            run_cli(args, "morsel_fallback", sql["morsel_result"], raw_dir, password)
        )
        write_canonical_csv(canonical_dir / "morsel-enabled.csv", *morsel_enabled)
        write_canonical_csv(canonical_dir / "morsel-fallback.csv", *morsel_fallback)

        report = {
            "completed_at_utc": utc_now(),
            "table": args.table,
            "property_trace": {"matched": True, "marker": "Property enforcement:"},
            "ordered_marker": args.ordered_marker,
            "morsel_marker": args.morsel_marker,
            "comparisons": [
                compare("ordered_merge_tree", ordered_fallback, ordered_enabled, ordered=True),
                compare("timepartition_morsel", morsel_fallback, morsel_enabled, ordered=False),
            ],
        }
        report["accepted"] = all(item["matched"] for item in report["comparisons"])
        (args.output / "acceptance.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        if not report["accepted"]:
            return 1
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    except (OSError, RuntimeError, ValueError) as error:
        (args.output / "failure.txt").write_text(f"{error}\n", encoding="utf-8")
        print(f"acceptance failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
