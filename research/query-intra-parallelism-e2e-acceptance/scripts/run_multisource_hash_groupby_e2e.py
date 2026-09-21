#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may obtain a copy of the
# License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.  See the License for the specific language governing permissions and limitations
# under the License.

"""Read-only acceptance for the experimental multi-source N x P hash GROUP BY path.

This program never creates data, changes a configuration, changes DOP, or starts/stops an IoTDB
process.  An operator must prepare two explicitly named, isolated deployments beforehand: an
enabled candidate and a hash-disabled control.  It archives the commands (with passwords
redacted), raw CLI output, copied configuration, canonical result CSVs, and an acceptance report.

The acceptance proof is intentionally stronger than a matching result.  The enabled EXPLAIN and
EXPLAIN ANALYZE output must both expose the property trace, TABLE_HASH_V1, at least N hash sinks,
at least N*P ExchangeNode occurrences, and a reviewed ``sources=N partitions=P (N x P
experimental path)`` trace.  The control must contain none of the hash-path markers.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import platform
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
ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
TABLE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\Z")
TOPOLOGY = re.compile(
    r"sources\s*=\s*(?P<sources>\d+)\s+partitions\s*=\s*(?P<buckets>\d+)"
    r"\s*\(N\s*x\s*P experimental path\)",
    re.IGNORECASE,
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


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


def property_values(path: Path) -> dict[str, str]:
    if not path.is_file():
        raise ValueError(f"configuration file does not exist: {path}")
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def require_config_state(path: Path, enabled: bool) -> dict[str, str]:
    values = property_values(path)
    expected = "true" if enabled else "false"
    for key in ("enable_property_driven_planning", "enable_table_group_by_hash_repartition"):
        if values.get(key, "").lower() != expected:
            raise ValueError(
                f"{path} must set {key}={expected} for the selected {'candidate' if enabled else 'control'}"
            )
    if enabled:
        try:
            partition_count = int(values.get("table_group_by_hash_repartition_partition_count", "0"))
        except ValueError as error:
            raise ValueError(f"{path} has a non-integer hash partition count") from error
        if partition_count < 2:
            raise ValueError(f"{path} must configure at least two hash partitions")
    return values


def capture_context(output: Path, enabled_config: Path, baseline_config: Path) -> None:
    environment = output / "environment"
    configs = environment / "config"
    configs.mkdir(parents=True)
    copied: list[dict[str, str]] = []
    for role, config in (("enabled", enabled_config), ("baseline", baseline_config)):
        destination = configs / f"{role}-{config.name}"
        shutil.copy2(config, destination)
        copied.append({"role": role, "source": str(config), "copy": str(destination), "sha256": sha256(destination)})
    payload: dict[str, Any] = {
        "captured_at_utc": utc_now(),
        "platform": platform.platform(),
        "configs": copied,
    }
    git_root = find_git_root(ROOT)
    if git_root:
        payload["git"] = {
            "root": str(git_root),
            "head": command_output(["git", "-C", str(git_root), "rev-parse", "HEAD"]),
            "status": command_output(["git", "-C", str(git_root), "status", "--short"]),
        }
    (environment / "environment.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def endpoint(args: argparse.Namespace, name: str) -> tuple[Path, str, int]:
    cli = Path(getattr(args, f"{name}_cli"))
    if not cli.is_file():
        raise ValueError(f"{name} CLI executable does not exist: {cli}")
    return cli, getattr(args, f"{name}_host"), getattr(args, f"{name}_port")


def run_cli(
    args: argparse.Namespace, name: str, sql_file: Path, raw_dir: Path, password: str
) -> Path:
    cli, host, port = endpoint(args, name)
    statement = " ".join(sql_file.read_text(encoding="utf-8").splitlines())
    command = [
        str(cli), "-h", host, "-p", str(port), "-u", args.username, "-pw", password,
        "-sql_dialect", "table", "-e", statement,
    ]
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    label = f"{name}-{sql_file.stem}"
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
        raise RuntimeError(f"CLI failed for {label}; inspect {stderr}")
    return stdout


def table_rows(path: Path) -> tuple[tuple[str, ...], list[tuple[str, ...]]]:
    rows: list[tuple[str, ...]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        clean = ANSI_ESCAPE.sub("", line).strip()
        if clean.startswith("|") and clean.endswith("|"):
            rows.append(tuple(cell.strip() for cell in clean[1:-1].split("|")))
    if len(rows) < 2:
        raise RuntimeError(f"CLI output has no table header and data rows: {path}")
    width = len(rows[0])
    if not width or any(len(row) != width for row in rows):
        raise RuntimeError(f"CLI output has an inconsistent table shape: {path}")
    return rows[0], rows[1:]


def write_canonical_csv(path: Path, table: tuple[tuple[str, ...], list[tuple[str, ...]]]) -> None:
    header, rows = table
    with path.open("w", newline="", encoding="utf-8") as target:
        writer = csv.writer(target)
        writer.writerow(header)
        writer.writerows(sorted(rows))


def digest(rows: list[tuple[str, ...]]) -> str:
    result = hashlib.sha256()
    for row in sorted(rows):
        result.update("\x1f".join(row).encode("utf-8"))
        result.update(b"\n")
    return result.hexdigest()


def compare_results(
    baseline: tuple[tuple[str, ...], list[tuple[str, ...]]],
    candidate: tuple[tuple[str, ...], list[tuple[str, ...]]],
    minimum_rows: int,
) -> dict[str, Any]:
    baseline_header, baseline_rows = baseline
    candidate_header, candidate_rows = candidate
    return {
        "same_header": baseline_header == candidate_header,
        "same_multiset": Counter(baseline_rows) == Counter(candidate_rows),
        "baseline_rows": len(baseline_rows),
        "candidate_rows": len(candidate_rows),
        "minimum_required_rows": minimum_rows,
        "baseline_multiset_sha256": digest(baseline_rows),
        "candidate_multiset_sha256": digest(candidate_rows),
        "matched": (
            baseline_header == candidate_header
            and Counter(baseline_rows) == Counter(candidate_rows)
            and len(candidate_rows) >= minimum_rows
        ),
    }


def validate_enabled_plan(path: Path, expected_sources: int, expected_buckets: int) -> dict[str, int]:
    text = path.read_text(encoding="utf-8")
    for marker in ("Property enforcement:", "TableHashPartitioningShuffleSinkNode", "TABLE_HASH_V1"):
        if marker not in text:
            raise RuntimeError(f"enabled plan is missing required marker {marker!r}: {path}")
    matches = list(TOPOLOGY.finditer(text))
    if not matches:
        raise RuntimeError(f"enabled plan has no reviewed N x P source/bucket trace: {path}")
    reported_sources = {int(match.group("sources")) for match in matches}
    reported_buckets = {int(match.group("buckets")) for match in matches}
    if reported_sources != {expected_sources} or reported_buckets != {expected_buckets}:
        raise RuntimeError(
            "enabled plan source/bucket trace does not match requested topology: "
            f"expected {expected_sources} x {expected_buckets}, saw sources={sorted(reported_sources)} "
            f"partitions={sorted(reported_buckets)}"
        )
    hash_sinks = text.count("TableHashPartitioningShuffleSinkNode")
    exchanges = text.count("ExchangeNode")
    required_exchanges = expected_sources * expected_buckets
    if hash_sinks < expected_sources:
        raise RuntimeError(
            f"enabled plan has {hash_sinks} hash-sink markers, below required source count {expected_sources}"
        )
    if exchanges < required_exchanges:
        raise RuntimeError(
            f"enabled plan has {exchanges} ExchangeNode markers, below required source x bucket edges "
            f"{expected_sources} x {expected_buckets} = {required_exchanges}"
        )
    return {"hash_sink_markers": hash_sinks, "exchange_markers": exchanges, "required_exchange_edges": required_exchanges}


def validate_baseline_plan(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    forbidden = ("TableHashPartitioningShuffleSinkNode", "TABLE_HASH_V1", "N x P experimental path")
    present = [marker for marker in forbidden if marker in text]
    if present:
        raise RuntimeError(f"hash-disabled control unexpectedly exposed hash-path markers {present}: {path}")


def write_queries(query_dir: Path, table: str) -> dict[str, Path]:
    core = f"SELECT s1, COUNT(*) AS row_count FROM {table} GROUP BY s1"
    statements = {
        "enabled-explain": f"EXPLAIN {core};\n",
        "enabled-explain-analyze": f"EXPLAIN ANALYZE {core};\n",
        "baseline-explain-analyze": f"EXPLAIN ANALYZE {core};\n",
        "enabled-result": f"{core} ORDER BY s1;\n",
        "baseline-result": f"{core} ORDER BY s1;\n",
    }
    query_dir.mkdir(parents=True)
    files: dict[str, Path] = {}
    for name, statement in statements.items():
        destination = query_dir / f"{name}.sql"
        destination.write_text(statement, encoding="utf-8")
        files[name] = destination
    return files


def execute(args: argparse.Namespace) -> dict[str, Any]:
    if not TABLE_NAME.fullmatch(args.table):
        raise ValueError("--table must be a simple unquoted database.table identifier")
    if args.expected_sources < 2 or args.expected_buckets < 2:
        raise ValueError("--expected-sources and --expected-buckets must both be at least 2")
    if args.minimum_result_rows < 1:
        raise ValueError("--minimum-result-rows must be positive")
    enabled_endpoint = (args.enabled_host, args.enabled_port)
    baseline_endpoint = (args.baseline_host, args.baseline_port)
    if enabled_endpoint == baseline_endpoint:
        raise ValueError("candidate and control endpoints must differ; do not toggle a live deployment in this runner")
    if args.enabled_isolation_id == args.baseline_isolation_id:
        raise ValueError("candidate and control must have distinct explicit isolation identifiers")
    enabled_values = require_config_state(args.enabled_config, enabled=True)
    baseline_values = require_config_state(args.baseline_config, enabled=False)
    configured_buckets = int(enabled_values["table_group_by_hash_repartition_partition_count"])
    if configured_buckets != args.expected_buckets:
        raise ValueError(
            f"--expected-buckets={args.expected_buckets} does not match enabled configuration "
            f"partition count {configured_buckets}"
        )
    if baseline_values.get("table_group_by_hash_repartition_partition_count") not in (None, str(configured_buckets)):
        raise ValueError("control and candidate must use the same configured hash partition count")
    password = os.environ.get(args.password_env)
    if not password:
        raise ValueError(f"password environment variable is unset or empty: {args.password_env}")
    if args.output.exists() and any(args.output.iterdir()):
        raise ValueError("--output must be new or empty so evidence is never overwritten")

    args.output.mkdir(parents=True, exist_ok=True)
    raw_dir = args.output / "raw"
    canonical_dir = args.output / "canonical"
    raw_dir.mkdir()
    canonical_dir.mkdir()
    capture_context(args.output, args.enabled_config, args.baseline_config)
    queries = write_queries(args.output / "queries", args.table)

    enabled_explain = run_cli(args, "enabled", queries["enabled-explain"], raw_dir, password)
    explain_shape = validate_enabled_plan(enabled_explain, args.expected_sources, args.expected_buckets)
    enabled_analyze = run_cli(args, "enabled", queries["enabled-explain-analyze"], raw_dir, password)
    analyze_shape = validate_enabled_plan(enabled_analyze, args.expected_sources, args.expected_buckets)
    baseline_analyze = run_cli(args, "baseline", queries["baseline-explain-analyze"], raw_dir, password)
    validate_baseline_plan(baseline_analyze)

    enabled_result = table_rows(run_cli(args, "enabled", queries["enabled-result"], raw_dir, password))
    baseline_result = table_rows(run_cli(args, "baseline", queries["baseline-result"], raw_dir, password))
    write_canonical_csv(canonical_dir / "enabled-result.csv", enabled_result)
    write_canonical_csv(canonical_dir / "baseline-result.csv", baseline_result)
    comparison = compare_results(baseline_result, enabled_result, args.minimum_result_rows)
    report = {
        "completed_at_utc": utc_now(),
        "claim": "multi-source N x P GROUP BY hash topology correctness only; no performance conclusion",
        "table": args.table,
        "candidate": {"isolation_id": args.enabled_isolation_id, "endpoint": f"{args.enabled_host}:{args.enabled_port}"},
        "control": {"isolation_id": args.baseline_isolation_id, "endpoint": f"{args.baseline_host}:{args.baseline_port}"},
        "expected_topology": {"sources": args.expected_sources, "buckets": args.expected_buckets},
        "enabled_explain": explain_shape,
        "enabled_explain_analyze": analyze_shape,
        "control_hash_path": "absent",
        "result_equivalence": comparison,
        "accepted": comparison["matched"],
    }
    (args.output / "multisource-hash-groupby-acceptance.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return report


def self_test() -> int:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        plan = root / "plan.stdout"
        plan.write_text(
            "Property enforcement:\n"
            "TableHashPartitioningShuffleSinkNode TABLE_HASH_V1 sources=2 partitions=3 (N x P experimental path)\n"
            "TableHashPartitioningShuffleSinkNode TABLE_HASH_V1\n"
            + "\n".join("ExchangeNode" for _ in range(6)),
            encoding="utf-8",
        )
        shape = validate_enabled_plan(plan, 2, 3)
        assert shape["required_exchange_edges"] == 6
        bad = root / "bad.stdout"
        bad.write_text("Property enforcement: TABLE_HASH_V1", encoding="utf-8")
        try:
            validate_enabled_plan(bad, 2, 3)
        except RuntimeError:
            pass
        else:
            raise AssertionError("missing source x bucket proof was accepted")
        table = root / "table.stdout"
        table.write_text("| s1 | row_count |\n| 1 | 4 |\n| 2 | 8 |\n", encoding="utf-8")
        parsed = table_rows(table)
        assert compare_results(parsed, parsed, 2)["matched"]
    print("multisource hash GROUP BY E2E self-test passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--table", help="unquoted database.table fixture name")
    parser.add_argument("--username", default="root")
    parser.add_argument("--password-env", default="IOTDB_E2E_PASSWORD")
    parser.add_argument("--expected-sources", type=int, default=2)
    parser.add_argument("--expected-buckets", type=int, default=2)
    parser.add_argument("--minimum-result-rows", type=int, default=2)
    for endpoint_name in ("enabled", "baseline"):
        parser.add_argument(f"--{endpoint_name}-cli")
        parser.add_argument(f"--{endpoint_name}-host")
        parser.add_argument(f"--{endpoint_name}-port", type=int)
        parser.add_argument(f"--{endpoint_name}-config", type=Path)
        parser.add_argument(f"--{endpoint_name}-isolation-id")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    required = ["output", "table"]
    for endpoint_name in ("enabled", "baseline"):
        required.extend(
            [f"{endpoint_name}_cli", f"{endpoint_name}_host", f"{endpoint_name}_port", f"{endpoint_name}_config", f"{endpoint_name}_isolation_id"]
        )
    missing = [name.replace("_", "-") for name in required if getattr(args, name) is None]
    if missing:
        parser.error("required for a server run: " + ", ".join(f"--{name}" for name in missing))
    try:
        report = execute(args)
    except (OSError, RuntimeError, ValueError) as error:
        if args.output is not None:
            args.output.mkdir(parents=True, exist_ok=True)
            (args.output / "failure.txt").write_text(f"{error}\n", encoding="utf-8")
        print(f"multi-source hash GROUP BY acceptance failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
