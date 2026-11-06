#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

"""Archive a read-only, server-side equal-count versus LPT morsel experiment.

The caller supplies two already-running, isolated DataNode endpoints.  This runner never starts
or stops a server, changes a configuration, or submits a mutating SQL statement.  It captures both
``EXPLAIN ANALYZE`` plans plus a result query, verifies result multisets, then invokes the morsel
summarizer to retain every per-driver record and the scan long-tail proxy.
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
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from summarize_morsel_evidence import parse_plan, summarize


ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
MUTATING_SQL = re.compile(
    r"\b(?:INSERT|CREATE|ALTER|DROP|DELETE|UPDATE|SET|FLUSH|LOAD|GRANT|REVOKE|KILL)\b",
    re.IGNORECASE,
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_sql(path: Path, label: str, require_explain: bool) -> str:
    if not path.is_file():
        raise ValueError(f"{label} SQL file does not exist: {path}")
    statement = " ".join(
        line.strip() for line in path.read_text(encoding="utf-8").splitlines() if not line.strip().startswith("--")
    ).strip()
    if not statement:
        raise ValueError(f"{label} SQL file is empty: {path}")
    if MUTATING_SQL.search(statement):
        raise ValueError(f"{label} SQL contains a mutating keyword")
    if require_explain and not statement.upper().startswith("EXPLAIN ANALYZE "):
        raise ValueError(f"{label} must begin with EXPLAIN ANALYZE")
    if not require_explain and statement.upper().startswith("EXPLAIN"):
        raise ValueError(f"{label} must be a result SELECT, not EXPLAIN")
    if not statement.endswith(";"):
        statement += ";"
    return statement


def config_values(path: Path, label: str, weighting: bool) -> dict[str, str]:
    if not path.is_file():
        raise ValueError(f"{label} configuration does not exist: {path}")
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    expected = "true"
    if values.get("enable_timepartition_morsel", "").lower() != expected:
        raise ValueError(f"{label} config must set enable_timepartition_morsel=true")
    expected_weighting = str(weighting).lower()
    if values.get("enable_timepartition_morsel_size_weighting", "").lower() != expected_weighting:
        raise ValueError(
            f"{label} config must set enable_timepartition_morsel_size_weighting={expected_weighting}"
        )
    return values


def run_cli(
    cli: Path, host: str, port: int, username: str, password: str, statement: str, output: Path, label: str
) -> Path:
    if not cli.is_file():
        raise ValueError(f"{label} CLI executable does not exist: {cli}")
    command = [
        str(cli), "-h", host, "-p", str(port), "-u", username, "-pw", password,
        "-sql_dialect", "table", "-e", statement,
    ]
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    stdout = output / "raw" / f"{label}.stdout"
    stderr = output / "raw" / f"{label}.stderr"
    stdout.write_text(completed.stdout, encoding="utf-8")
    stderr.write_text(completed.stderr, encoding="utf-8")
    redacted = command.copy()
    redacted[redacted.index("-pw") + 1] = "<redacted>"
    (output / "raw" / f"{label}.command.json").write_text(
        json.dumps({"command": redacted, "exit_code": completed.returncode}, indent=2) + "\n",
        encoding="utf-8",
    )
    if completed.returncode:
        raise RuntimeError(f"CLI failed for {label}; inspect {stderr}")
    return stdout


def table_rows(path: Path) -> tuple[tuple[str, ...], list[tuple[str, ...]]]:
    rows: list[tuple[str, ...]] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = ANSI_ESCAPE.sub("", raw).strip()
        if line.startswith("|") and line.endswith("|"):
            rows.append(tuple(value.strip() for value in line[1:-1].split("|")))
    if len(rows) < 2:
        raise RuntimeError(f"CLI result has no header and data rows: {path}")
    width = len(rows[0])
    if width == 0 or any(len(row) != width for row in rows):
        raise RuntimeError(f"CLI result has inconsistent table rows: {path}")
    return rows[0], rows[1:]


def write_canonical(path: Path, table: tuple[tuple[str, ...], list[tuple[str, ...]]]) -> None:
    header, rows = table
    with path.open("w", newline="", encoding="utf-8") as target:
        writer = csv.writer(target)
        writer.writerow(header)
        writer.writerows(sorted(rows))


def row_digest(rows: list[tuple[str, ...]]) -> str:
    digest = hashlib.sha256()
    for row in sorted(rows):
        digest.update("\x1f".join(row).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--plan-sql", required=True, type=Path)
    parser.add_argument("--result-sql", required=True, type=Path)
    parser.add_argument("--equal-cli", required=True, type=Path)
    parser.add_argument("--equal-host", default="127.0.0.1")
    parser.add_argument("--equal-port", required=True, type=int)
    parser.add_argument("--equal-config", required=True, type=Path)
    parser.add_argument("--lpt-cli", required=True, type=Path)
    parser.add_argument("--lpt-host", default="127.0.0.1")
    parser.add_argument("--lpt-port", required=True, type=int)
    parser.add_argument("--lpt-config", required=True, type=Path)
    parser.add_argument("--username", default="root")
    parser.add_argument("--password-env", default="IOTDB_E2E_PASSWORD")
    args = parser.parse_args()
    try:
        if (args.equal_host, args.equal_port) == (args.lpt_host, args.lpt_port):
            raise ValueError("equal-count and LPT endpoints must be distinct isolated deployments")
        if args.output.exists() and any(args.output.iterdir()):
            raise ValueError("--output must be new or empty; evidence is append-only")
        password = os.environ.get(args.password_env)
        if not password:
            raise ValueError(f"password environment variable is unset or empty: {args.password_env}")
        equal_config = config_values(args.equal_config, "equal-count", weighting=False)
        lpt_config = config_values(args.lpt_config, "LPT", weighting=True)
        plan_sql = read_sql(args.plan_sql, "plan", require_explain=True)
        result_sql = read_sql(args.result_sql, "result", require_explain=False)

        raw = args.output / "raw"
        canonical = args.output / "canonical"
        raw.mkdir(parents=True)
        canonical.mkdir()
        shutil.copy2(args.plan_sql, args.output / "plan.sql")
        shutil.copy2(args.result_sql, args.output / "result.sql")
        shutil.copy2(args.equal_config, args.output / "equal-count.properties")
        shutil.copy2(args.lpt_config, args.output / "lpt.properties")

        equal_plan = run_cli(
            args.equal_cli, args.equal_host, args.equal_port, args.username, password,
            plan_sql, args.output, "equal-count-plan"
        )
        lpt_plan = run_cli(
            args.lpt_cli, args.lpt_host, args.lpt_port, args.username, password,
            plan_sql, args.output, "lpt-plan"
        )
        equal_result = run_cli(
            args.equal_cli, args.equal_host, args.equal_port, args.username, password,
            result_sql, args.output, "equal-count-result"
        )
        lpt_result = run_cli(
            args.lpt_cli, args.lpt_host, args.lpt_port, args.username, password,
            result_sql, args.output, "lpt-result"
        )

        equal_table = table_rows(equal_result)
        lpt_table = table_rows(lpt_result)
        write_canonical(canonical / "equal-count.csv", equal_table)
        write_canonical(canonical / "lpt.csv", lpt_table)
        equal_pipelines = parse_plan(equal_plan)
        lpt_pipelines = parse_plan(lpt_plan)
        result_equivalence = {
            "same_header": equal_table[0] == lpt_table[0],
            "same_multiset": Counter(equal_table[1]) == Counter(lpt_table[1]),
            "equal_count_rows": len(equal_table[1]),
            "lpt_rows": len(lpt_table[1]),
            "equal_count_multiset_sha256": row_digest(equal_table[1]),
            "lpt_multiset_sha256": row_digest(lpt_table[1]),
        }
        accepted = result_equivalence["same_header"] and result_equivalence["same_multiset"]
        payload: dict[str, Any] = {
            "captured_at_utc": datetime.now(timezone.utc).isoformat(),
            "platform": platform.platform(),
            "arms": {
                "equal_count": {
                    "endpoint": f"{args.equal_host}:{args.equal_port}",
                    "config_sha256": sha256(args.output / "equal-count.properties"),
                    "config": {
                        "enable_timepartition_morsel": equal_config["enable_timepartition_morsel"],
                        "enable_timepartition_morsel_size_weighting": equal_config[
                            "enable_timepartition_morsel_size_weighting"
                        ],
                    },
                    **summarize("EQUAL_PARTITION_COUNT", equal_pipelines),
                },
                "lpt": {
                    "endpoint": f"{args.lpt_host}:{args.lpt_port}",
                    "config_sha256": sha256(args.output / "lpt.properties"),
                    "config": {
                        "enable_timepartition_morsel": lpt_config["enable_timepartition_morsel"],
                        "enable_timepartition_morsel_size_weighting": lpt_config[
                            "enable_timepartition_morsel_size_weighting"
                        ],
                    },
                    **summarize("LPT_TSFILE_BYTES", lpt_pipelines),
                },
            },
            "result_equivalence": result_equivalence,
            "accepted": accepted,
            "limitations": [
                "This is an observed scan-stage balance experiment, not a whole-query speedup claim.",
                "Fragment-level blocked queue time is visible in raw EXPLAIN ANALYZE, but no per-morsel backpressure attribution exists yet.",
            ],
        }
        (args.output / "morsel-balance-acceptance.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        if not accepted:
            raise RuntimeError("equal-count and LPT result multisets differ; see canonical CSVs")
    except (OSError, RuntimeError, ValueError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    sys.exit(main())
