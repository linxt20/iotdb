#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.

"""Measure one ordinary table-model SQL statement from IoTDB server history.

The normal query is sent through the supplied CLI.  Its client elapsed time is deliberately
not read.  Instead, the script snapshots ``information_schema.current_queries`` before the
query, then finds the newly finished query whose server-returned statement has the same
canonical SQL fingerprint.  Its server-side ``cost_time`` (seconds) becomes ``query_ms``.

The target DataNode must have ``query_cost_stat_window`` set to a positive number of minutes.
This script never sets that property and fails rather than falling back to client timing.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def canonical_sql(sql: str) -> str:
    """Conservative SQL fingerprint: comments/whitespace/case do not distinguish a query."""

    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    sql = re.sub(r"--[^\r\n]*", " ", sql)
    return re.sub(r"\s+", " ", sql).strip().rstrip(";").strip().lower()


def cli_command(args: argparse.Namespace, sql: str) -> tuple[list[str], list[str]]:
    password = os.environ.get(args.password_env)
    if not password:
        raise ValueError(f"password environment variable is unset: {args.password_env}")
    command = [
        str(args.cli), "-h", args.host, "-p", str(args.port), "-u", args.username,
        "-pw", password, "-sql_dialect", "table", *args.cli_arg, "-e", sql,
    ]
    redacted = command.copy()
    redacted[redacted.index(password)] = "<redacted-password>"
    return command, redacted


def run_cli(args: argparse.Namespace, sql: str) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    command, redacted = cli_command(args, sql)
    return subprocess.run(command, capture_output=True, text=True, check=False), redacted


def archive(path: Path, stdout: str, stderr: str, command: list[str]) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / "stdout").write_text(stdout, encoding="utf-8")
    (path / "stderr").write_text(stderr, encoding="utf-8")
    (path / "command.json").write_text(json.dumps(command, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def pipe_rows(output: str) -> list[dict[str, str]]:
    rows: list[list[str]] = []
    for line in output.splitlines():
        line = line.strip()
        if line.startswith("|") and line.endswith("|"):
            rows.append([cell.strip() for cell in line[1:-1].split("|")])
    if len(rows) < 1:
        raise ValueError("current_queries CLI output has no pipe-table header")
    header = rows[0]
    if not header or any(len(row) != len(header) for row in rows):
        raise ValueError("current_queries CLI output is not a rectangular pipe table")
    return [dict(zip(header, row)) for row in rows[1:]]


HISTORY_SQL = (
    "SELECT query_id,state,start_time,end_time,cost_time,statement "
    "FROM information_schema.current_queries WHERE state='FINISHED'"
)


def history(args: argparse.Namespace, archive_dir: Path) -> list[dict[str, str]]:
    completed, redacted = run_cli(args, HISTORY_SQL)
    archive(archive_dir, completed.stdout, completed.stderr, redacted)
    if completed.returncode:
        raise RuntimeError(f"current_queries observer failed with exit code {completed.returncode}")
    return pipe_rows(completed.stdout)


def find_match(
    rows: list[dict[str, str]], baseline_ids: set[str], fingerprint: str
) -> dict[str, str] | None:
    matches = [
        row for row in rows
        if row.get("query_id") not in baseline_ids
        and canonical_sql(row.get("statement", "")) == fingerprint
    ]
    if not matches:
        return None
    # query IDs are generated monotonically, but start_time gives a documented chronological tie-break.
    return max(matches, key=lambda row: (row.get("start_time", ""), row.get("query_id", "")))


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run_measure(args: argparse.Namespace) -> int:
    sql = args.sql_file.read_text(encoding="utf-8")
    fingerprint = canonical_sql(sql)
    if not fingerprint:
        raise ValueError("SQL file is empty after canonicalization")
    if args.raw_dir.exists() and any(args.raw_dir.iterdir()):
        raise ValueError("raw-dir must be new or empty to preserve evidence")
    args.raw_dir.mkdir(parents=True, exist_ok=True)
    write_json(args.raw_dir / "request.json", {
        "started_at_utc": utc_now(), "endpoint": {"host": args.host, "port": args.port},
        "sql_file": str(args.sql_file), "sql_sha256": sha256_text(sql),
        "canonical_sql_sha256": sha256_text(fingerprint), "history_sql": HISTORY_SQL,
        "measurement": "server current_queries.cost_time * 1000", "client_elapsed_used": False,
    })
    before = history(args, args.raw_dir / "history-before")
    baseline_ids = {row.get("query_id", "") for row in before}
    normal, redacted = run_cli(args, sql)
    archive(args.raw_dir / "normal-query", normal.stdout, normal.stderr, redacted)
    if normal.returncode:
        write_json(args.raw_dir / "failure.json", {"reason": "normal query CLI failed", "exit_code": normal.returncode})
        return normal.returncode
    deadline = time.monotonic() + args.history_timeout_seconds
    observations: list[dict[str, Any]] = []
    matched: dict[str, str] | None = None
    attempt = 0
    while time.monotonic() <= deadline:
        attempt += 1
        rows = history(args, args.raw_dir / "history-after" / f"poll-{attempt:03d}")
        observations.append({"at_utc": utc_now(), "query_ids": [row.get("query_id") for row in rows]})
        matched = find_match(rows, baseline_ids, fingerprint)
        if matched is not None:
            break
        time.sleep(args.history_poll_seconds)
    write_json(args.raw_dir / "history-observations.json", observations)
    if matched is None:
        write_json(args.raw_dir / "failure.json", {
            "reason": "no newly finished SQL-fingerprint match in current_queries; ensure positive query_cost_stat_window and a serial endpoint",
            "canonical_sql_sha256": sha256_text(fingerprint), "baseline_query_ids": sorted(baseline_ids),
        })
        print("server query history did not retain an unambiguous finished query", file=sys.stderr)
        return 2
    try:
        query_ms = float(matched["cost_time"]) * 1000.0
    except (KeyError, ValueError) as error:
        raise ValueError(f"matched cost_time is not numeric: {matched.get('cost_time')!r}") from error
    if query_ms < 0:
        raise ValueError("matched server cost_time is negative")
    metrics = {
        "query_ms": query_ms,
        "metric_source": "information_schema.current_queries.cost_time",
        "query_id": matched["query_id"], "statement_canonical_sha256": sha256_text(fingerprint),
        "server_cost_time_seconds": float(matched["cost_time"]),
        "server_start_time": matched.get("start_time"), "server_end_time": matched.get("end_time"),
        "client_elapsed_used": False,
    }
    write_json(args.raw_dir / "metrics.json", metrics)
    print(json.dumps(metrics, ensure_ascii=False))
    return 0


def self_test() -> int:
    if canonical_sql(" SELECT /* x */ A -- tail\n FROM T; ") != "select a from t":
        return 1
    sample = "+--+\n| query_id | state | statement |\n+--+\n| q1 | FINISHED | SELECT 1 |\n+--+\n"
    rows = pipe_rows(sample)
    if rows != [{"query_id": "q1", "state": "FINISHED", "statement": "SELECT 1"}]:
        return 1
    match = find_match(rows, set(), canonical_sql("select 1"))
    if match is None or match["query_id"] != "q1":
        return 1
    print("server_current_query_metrics self-test passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--cli", type=Path)
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--username", default="root")
    parser.add_argument("--password-env", default="IOTDB_PASSWORD")
    parser.add_argument("--cli-arg", action="append", default=[])
    parser.add_argument("--sql-file", type=Path)
    parser.add_argument("--raw-dir", type=Path)
    parser.add_argument("--history-timeout-seconds", type=float, default=5.0)
    parser.add_argument("--history-poll-seconds", type=float, default=0.1)
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    required = ("cli", "host", "port", "sql_file", "raw_dir")
    if any(getattr(args, name) is None for name in required):
        parser.error("--cli, --host, --port, --sql-file, and --raw-dir are required")
    if not args.cli.is_file() or not args.sql_file.is_file():
        parser.error("--cli and --sql-file must name existing files")
    if not 1 <= args.port <= 65535 or args.history_timeout_seconds <= 0 or args.history_poll_seconds <= 0:
        parser.error("port and history timeouts must be positive")
    try:
        return run_measure(args)
    except (OSError, ValueError, RuntimeError) as error:
        print(str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
