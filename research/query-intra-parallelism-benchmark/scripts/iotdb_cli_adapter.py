#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run an existing IoTDB CLI and preserve its raw output for benchmark evidence.

This adapter never starts, stops, reconfigures, or clears a server.  It deliberately refuses
to turn client elapsed time into ``query_ms``.  A measured run therefore needs an explicit
regular expression which identifies the server-side timing field emitted by the deployed CLI
or a site wrapper.  The same adapter can export ordinary validation-query pipe tables to CSV.
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DATABASE_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\\Z")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def command_for(args: argparse.Namespace, sql: str) -> tuple[list[str], list[str]]:
    """Return the executable command and a version safe to archive."""

    password = os.environ.get(args.password_env)
    if not password:
        raise ValueError(f"password environment variable is unset: {args.password_env}")
    command = [
        str(args.cli),
        "-h",
        args.host,
        "-p",
        str(args.port),
        "-u",
        args.username,
        "-pw",
        password,
        "-sql_dialect",
        "table",
    ]
    command.extend(args.cli_arg)
    command.extend(["-e", sql])
    redacted = command.copy()
    # The default username and password may have the same text (for example ``root``),
    # so redact by the option position rather than the first matching argument value.
    password_index = redacted.index("-pw") + 1
    redacted[password_index] = "<redacted-password>"
    return command, redacted


def sql_for(args: argparse.Namespace) -> tuple[str, str]:
    text = args.sql_file.read_text(encoding="utf-8")
    if args.table:
        text = text.replace("${DATABASE}", args.database).replace("${TABLE}", args.table)
    if "${DATABASE}" in text or "${TABLE}" in text:
        raise ValueError("SQL has benchmark placeholders; provide --table to render them")
    if args.skip_use_database:
        return text, text
    if not DATABASE_PATTERN.fullmatch(args.database):
        raise ValueError("database must be a simple identifier unless --skip-use-database is used")
    return f"USE {args.database};\n{text}", text


def execute(args: argparse.Namespace) -> tuple[subprocess.CompletedProcess[str], dict[str, Any]]:
    sql, original_sql = sql_for(args)
    command, redacted_command = command_for(args, sql)
    started = utc_now()
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    finished = utc_now()
    metadata = {
        "adapter": "iotdb_cli_adapter.py",
        "started_at_utc": started,
        "finished_at_utc": finished,
        "endpoint": {"host": args.host, "port": args.port, "database": args.database},
        "cli": str(args.cli),
        "command": redacted_command,
        "sql_file": str(args.sql_file),
        "sql_sha256": sha256_file(args.sql_file),
        "sql_with_database_prefix": not args.skip_use_database,
        "exit_code": completed.returncode,
        "stdout_sha256": hashlib.sha256(completed.stdout.encode("utf-8")).hexdigest(),
        "stderr_sha256": hashlib.sha256(completed.stderr.encode("utf-8")).hexdigest(),
        "submitted_sql_sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "original_sql_bytes": len(original_sql.encode("utf-8")),
    }
    return completed, metadata


def archive_raw(raw_dir: Path, completed: subprocess.CompletedProcess[str], metadata: dict[str, Any]) -> None:
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "iotdb-cli.stdout").write_text(completed.stdout, encoding="utf-8")
    (raw_dir / "iotdb-cli.stderr").write_text(completed.stderr, encoding="utf-8")
    (raw_dir / "iotdb-cli-command.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def extract_numeric_metric(output: str, expression: str) -> float:
    match = re.search(expression, output, flags=re.MULTILINE)
    if match is None:
        raise ValueError("server-side query metric regex did not match CLI stdout")
    try:
        return float(match.group(1))
    except (IndexError, ValueError) as error:
        raise ValueError("server-side query metric regex must contain numeric capture group 1") from error


def pipe_table_rows(output: str) -> list[list[str]]:
    """Parse the ordinary ASCII table emitted by the IoTDB CLI without guessing data rows."""

    candidates = []
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("|") and stripped.endswith("|"):
            candidates.append([part.strip() for part in stripped[1:-1].split("|")])
    if len(candidates) < 2:
        raise ValueError("CLI stdout did not contain an IoTDB pipe table with a header and a row")
    width = len(candidates[0])
    if width == 0 or any(len(row) != width for row in candidates):
        raise ValueError("CLI pipe table has inconsistent column counts")
    return candidates


def run_measure(args: argparse.Namespace) -> int:
    completed, metadata = execute(args)
    archive_raw(args.raw_dir, completed, metadata)
    if completed.returncode:
        return completed.returncode
    try:
        query_ms = extract_numeric_metric(completed.stdout, args.server_query_ms_regex)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2
    metrics = {
        "query_ms": query_ms,
        "metric_source": "explicit_server_query_ms_regex",
        "server_query_ms_regex": args.server_query_ms_regex,
    }
    (args.raw_dir / "iotdb-cli-metrics.json").write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(metrics, ensure_ascii=False))
    return 0


def run_export(args: argparse.Namespace) -> int:
    completed, metadata = execute(args)
    archive_raw(args.raw_dir, completed, metadata)
    if completed.returncode:
        return completed.returncode
    try:
        rows = pipe_table_rows(completed.stdout)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", newline="", encoding="utf-8") as target:
        writer = csv.writer(target)
        writer.writerows(rows)
    report = {
        "csv": str(args.output),
        "rows_including_header": len(rows),
        "columns": len(rows[0]),
        "csv_sha256": sha256_file(args.output),
    }
    (args.raw_dir / "iotdb-cli-export.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False))
    return 0


def self_test() -> int:
    sample = """+----+-----+
| a  | b   |
+----+-----+
| 1  | two |
+----+-----+
Total line number = 1
"""
    if pipe_table_rows(sample) != [["a", "b"], ["1", "two"]]:
        return 1
    if extract_numeric_metric("server_query_ms=12.5", r"server_query_ms=([0-9.]+)") != 12.5:
        return 1
    variable = "IOTDB_CLI_ADAPTER_SELF_TEST_PASSWORD"
    previous = os.environ.get(variable)
    os.environ[variable] = "root"
    try:
        _, redacted = command_for(argparse.Namespace(
            cli=Path("/example/start-cli.sh"), host="127.0.0.1", port=1, username="root",
            password_env=variable, cli_arg=[]), "SELECT 1")
    finally:
        if previous is None:
            os.environ.pop(variable, None)
        else:
            os.environ[variable] = previous
    if redacted[redacted.index("-u") + 1] != "root" or redacted[redacted.index("-pw") + 1] != "<redacted-password>":
        return 1
    print("iotdb_cli_adapter self-test passed")
    return 0


def add_connection_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--cli", required=True, type=Path, help="existing start-cli.sh or start-cli.bat")
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--database", required=True)
    parser.add_argument("--table", help="replace ${DATABASE} and ${TABLE} placeholders in a workload SQL file")
    parser.add_argument("--username", default="root")
    parser.add_argument("--password-env", default="IOTDB_PASSWORD")
    parser.add_argument("--cli-arg", action="append", default=[], help="additional non-secret CLI argument")
    parser.add_argument("--sql-file", required=True, type=Path)
    parser.add_argument("--raw-dir", required=True, type=Path)
    parser.add_argument("--skip-use-database", action="store_true")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", help="run parser-only checks without a CLI or server")
    subparsers = parser.add_subparsers(dest="action")
    measure = subparsers.add_parser("measure", help="archive a timed CLI query for run_matrix.py")
    add_connection_arguments(measure)
    measure.add_argument(
        "--server-query-ms-regex",
        required=True,
        help="regex with numeric capture group 1 for a server-side query time in milliseconds",
    )
    export = subparsers.add_parser("export", help="archive a CLI query and export its ASCII pipe table to CSV")
    add_connection_arguments(export)
    export.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if args.action is None:
        parser.error("one action is required unless --self-test is used")
    if not args.cli.is_file():
        parser.error(f"CLI does not exist or is not a file: {args.cli}")
    if not args.sql_file.is_file():
        parser.error(f"SQL file does not exist: {args.sql_file}")
    if not 1 <= args.port <= 65535:
        parser.error("port must be in 1..65535")
    try:
        return run_measure(args) if args.action == "measure" else run_export(args)
    except ValueError as error:
        print(str(error), file=sys.stderr)
        return 2
    except OSError as error:
        print(f"failed to execute CLI: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
