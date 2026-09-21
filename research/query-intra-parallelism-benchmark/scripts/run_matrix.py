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

"""Execute and archive a DOP/cache benchmark matrix through an explicit site adapter.

The command templates are intentionally operator-provided because deployment, authentication,
and server-side timing collection differ by lab. They are trusted local commands, not input
from an untrusted user.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_WORKLOAD = ROOT / "workload"
METRIC_COLUMNS = (
    "query_ms",
    "planning_ms",
    "execution_ms",
    "result_rows",
    "cpu_pct",
    "peak_rss_bytes",
    "shuffle_bytes",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_csv_values(value: str) -> list[str]:
    values = [part.strip() for part in value.split(",") if part.strip()]
    if not values:
        raise ValueError("at least one value is required")
    return values


def render_sql(source: Path, database: str, table: str, destination: Path) -> None:
    text = source.read_text(encoding="utf-8")
    text = text.replace("${DATABASE}", database).replace("${TABLE}", table)
    destination.write_text(text, encoding="utf-8")


def run_shell(command: str, context: dict[str, Any], attempt_dir: Path) -> subprocess.CompletedProcess[str]:
    rendered = command.format(**context)
    return subprocess.run(
        rendered,
        cwd=attempt_dir,
        shell=True,
        text=True,
        capture_output=True,
        check=False,
    )


def extract_metrics(output: str) -> dict[str, Any]:
    for line in reversed(output.splitlines()):
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and "query_ms" in payload:
            try:
                payload["query_ms"] = float(payload["query_ms"])
            except (TypeError, ValueError) as error:
                raise ValueError("adapter query_ms must be numeric") from error
            if not math.isfinite(payload["query_ms"]) or payload["query_ms"] <= 0:
                raise ValueError("adapter query_ms must be finite and positive")
            return payload
    raise ValueError("adapter stdout must contain a JSON object with server-side query_ms")


def write_attempt(
    attempt_dir: Path,
    completed: subprocess.CompletedProcess[str],
    metrics: dict[str, Any] | None,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    attempt_dir.mkdir(parents=True, exist_ok=True)
    (attempt_dir / "adapter.stdout").write_text(completed.stdout, encoding="utf-8")
    (attempt_dir / "adapter.stderr").write_text(completed.stderr, encoding="utf-8")
    record = {**metadata, "adapter_exit_code": completed.returncode, "metrics": metrics}
    (attempt_dir / "record.json").write_text(
        json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    if completed.returncode != 0:
        raise RuntimeError(f"adapter failed with exit code {completed.returncode}: {attempt_dir}")
    if metrics is None:
        raise RuntimeError(f"adapter did not return required metrics: {attempt_dir}")
    return record


def numeric(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = (len(ordered) - 1) * fraction
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (index - lower)


def write_summaries(records: list[dict[str, Any]], output: Path) -> None:
    summary_dir = output / "summary"
    summary_dir.mkdir(exist_ok=True)
    flat_rows = []
    for record in records:
        metrics = record["metrics"]
        flat_rows.append(
            {
                "timestamp_utc": record["timestamp_utc"],
                "step": record["step"],
                "query_id": record["query_id"],
                "dop": record["dop"],
                "cache_mode": record["cache_mode"],
                "iteration": record["iteration"],
                **{column: metrics.get(column) for column in METRIC_COLUMNS},
                "attempt_dir": record["attempt_dir"],
            }
        )
    with (summary_dir / "attempts.csv").open("w", newline="", encoding="utf-8") as target:
        writer = csv.DictWriter(target, fieldnames=list(flat_rows[0]) if flat_rows else [])
        if flat_rows:
            writer.writeheader()
            writer.writerows(flat_rows)

    # Keep repeated DOP steps separate. In the default 1,2,4,8,16,1 sequence, merging both
    # DOP=1 steps would hide the very cache-drift control that the final step is meant to expose.
    groups: dict[tuple[int, str, int, str], list[dict[str, Any]]] = {}
    for row in flat_rows:
        groups.setdefault(
            (int(row["step"]), row["query_id"], int(row["dop"]), row["cache_mode"]), []
        ).append(row)
    summary_rows = []
    for (step, query_id, dop, cache_mode), rows in sorted(groups.items()):
        row: dict[str, Any] = {
            "step": step,
            "query_id": query_id,
            "dop": dop,
            "cache_mode": cache_mode,
            "sample_count": len(rows),
        }
        for column in METRIC_COLUMNS:
            values = [numeric(item[column]) for item in rows]
            actual = [value for value in values if value is not None]
            row[f"{column}_median"] = median(actual) if actual else ""
            row[f"{column}_p50"] = percentile(actual, 0.50) if actual else ""
            row[f"{column}_p95"] = percentile(actual, 0.95) if actual else ""
        query_ms = [numeric(item["query_ms"]) for item in rows]
        result_rows = [numeric(item["result_rows"]) for item in rows]
        rates = [count * 1000 / elapsed for count, elapsed in zip(result_rows, query_ms) if count is not None and elapsed]
        row["throughput_rows_per_second_median"] = median(rates) if rates else ""
        summary_rows.append(row)
    if summary_rows:
        with (summary_dir / "summary.csv").open("w", newline="", encoding="utf-8") as target:
            writer = csv.DictWriter(target, fieldnames=list(summary_rows[0]))
            writer.writeheader()
            writer.writerows(summary_rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--table", required=True, help="fully qualified table, for example benchdb.bench")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--workload-dir", default=DEFAULT_WORKLOAD, type=Path)
    parser.add_argument("--queries", default="scan_filter,filter_project,ordered_scan,top_k,group_by,self_join")
    parser.add_argument("--dops", default="1,2,4,8,16,1")
    parser.add_argument("--cache-modes", default="warm")
    parser.add_argument("--warmups", default=2, type=int)
    parser.add_argument("--iterations", default=7, type=int)
    parser.add_argument("--query-command", required=True)
    parser.add_argument("--dop-command", required=True)
    parser.add_argument("--cold-cache-command")
    parser.add_argument("--config", action="append", default=[], type=Path)
    args = parser.parse_args()

    queries = parse_csv_values(args.queries)
    try:
        dops = [int(value) for value in parse_csv_values(args.dops)]
    except ValueError as error:
        parser.error(f"DOP values must be integers: {error}")
    if any(dop <= 0 for dop in dops):
        parser.error("all DOP values must be positive")
    cache_modes = parse_csv_values(args.cache_modes)
    if "cold" in cache_modes and not args.cold_cache_command:
        parser.error("--cold-cache-command is mandatory when cache mode includes cold")
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("--output must be a new or empty directory to preserve raw evidence")
    if args.iterations < 1 or args.warmups < 0:
        parser.error("iterations must be positive and warmups must not be negative")
    for query in queries:
        if not (args.workload_dir / f"{query}.sql").is_file():
            parser.error(f"missing workload SQL for {query}")

    args.output.mkdir(parents=True, exist_ok=True)
    environment_dir = args.output / "environment"
    capture = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("capture_environment.py")),
            "--output",
            str(environment_dir),
            "--git-root",
            str(REPOSITORY_ROOT),
            *sum((["--config", str(path)] for path in args.config), []),
        ],
        check=False,
    )
    if capture.returncode:
        return capture.returncode
    manifest = {
        "started_at_utc": utc_now(),
        "database": args.database,
        "table": args.table,
        "queries": queries,
        "dop_sequence": dops,
        "cache_modes": cache_modes,
        "warmups": args.warmups,
        "iterations": args.iterations,
        "query_command": args.query_command,
        "dop_command": args.dop_command,
        "cold_cache_command": args.cold_cache_command,
    }
    (args.output / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    records: list[dict[str, Any]] = []
    try:
        for step, dop in enumerate(dops, start=1):
            state_dir = args.output / "raw" / f"step-{step:02d}-dop-{dop}"
            state_dir.mkdir(parents=True)
            state_context = {"dop": dop, "step": step, "attempt_dir": str(state_dir)}
            dop_result = run_shell(args.dop_command, state_context, state_dir)
            (state_dir / "dop-command.stdout").write_text(dop_result.stdout, encoding="utf-8")
            (state_dir / "dop-command.stderr").write_text(dop_result.stderr, encoding="utf-8")
            if dop_result.returncode:
                raise RuntimeError(f"DOP command failed for DOP={dop}")
            for cache_mode in cache_modes:
                for query_id in queries:
                    source_sql = args.workload_dir / f"{query_id}.sql"
                    for warmup in range(args.warmups):
                        warmup_dir = state_dir / cache_mode / query_id / f"warmup-{warmup + 1:02d}"
                        warmup_dir.mkdir(parents=True)
                        sql_file = warmup_dir / "query.sql"
                        render_sql(source_sql, args.database, args.table, sql_file)
                        context = {**state_context, "query_id": query_id, "cache_mode": cache_mode,
                                   "iteration": warmup + 1, "sql_file": str(sql_file), "attempt_dir": str(warmup_dir)}
                        completed = run_shell(args.query_command, context, warmup_dir)
                        (warmup_dir / "adapter.stdout").write_text(completed.stdout, encoding="utf-8")
                        (warmup_dir / "adapter.stderr").write_text(completed.stderr, encoding="utf-8")
                        if completed.returncode:
                            raise RuntimeError(f"warmup failed for {query_id}, DOP={dop}")
                    for iteration in range(1, args.iterations + 1):
                        attempt_dir = state_dir / cache_mode / query_id / f"attempt-{iteration:02d}"
                        attempt_dir.mkdir(parents=True)
                        if cache_mode == "cold":
                            reset = run_shell(args.cold_cache_command, {**state_context, "attempt_dir": str(attempt_dir)}, attempt_dir)
                            (attempt_dir / "cold-cache.stdout").write_text(reset.stdout, encoding="utf-8")
                            (attempt_dir / "cold-cache.stderr").write_text(reset.stderr, encoding="utf-8")
                            if reset.returncode:
                                raise RuntimeError(f"cold-cache command failed for {query_id}, DOP={dop}")
                        sql_file = attempt_dir / "query.sql"
                        render_sql(source_sql, args.database, args.table, sql_file)
                        context = {**state_context, "query_id": query_id, "cache_mode": cache_mode,
                                   "iteration": iteration, "sql_file": str(sql_file), "attempt_dir": str(attempt_dir)}
                        completed = run_shell(args.query_command, context, attempt_dir)
                        metrics = extract_metrics(completed.stdout) if completed.returncode == 0 else None
                        record = write_attempt(
                            attempt_dir,
                            completed,
                            metrics,
                            {"timestamp_utc": utc_now(), "step": step, "query_id": query_id, "dop": dop,
                             "cache_mode": cache_mode, "iteration": iteration, "attempt_dir": str(attempt_dir)},
                        )
                        records.append(record)
    finally:
        if records:
            write_summaries(records, args.output)
        manifest["finished_at_utc"] = utc_now()
        manifest["completed_attempts"] = len(records)
        (args.output / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
