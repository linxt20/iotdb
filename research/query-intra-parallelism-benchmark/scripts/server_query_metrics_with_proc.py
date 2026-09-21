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

"""One-command, read-only benchmark adapter for server time and two-DataNode resources.

This script composes :mod:`server_current_query_metrics` and
:mod:`collect_datanode_proc` without using client wall-clock time as query latency. It is
safe for ``run_matrix.py``: every invocation prints exactly one JSON object on stdout, while
the child commands, server-history observations, and raw ``/proc`` samples are kept below the
given attempt directory.

It neither starts, stops, reconfigures, nor clears IoTDB. Exactly two explicit DataNode PIDs
are required. When explicit non-overlapping DataNode audit logs are supplied, the remote payload
extractor archives and verifies a query-scoped shuffle-byte value; otherwise ``shuffle_bytes`` is
JSON null, never a fabricated zero.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Optional, Tuple


HERE = Path(__file__).resolve().parent
DEFAULT_METRICS_SCRIPT = HERE / "server_current_query_metrics.py"
DEFAULT_PROC_SCRIPT = HERE / "collect_datanode_proc.py"
DEFAULT_SHUFFLE_AUDIT_SCRIPT = HERE / "extract_remote_shuffle_payload_bytes.py"


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"cannot read expected JSON evidence {path}: {error}") from error
    if not isinstance(value, dict):
        raise RuntimeError(f"expected JSON object in {path}")
    return value


def parse_two_pids(value: str) -> list[int]:
    try:
        pids = [int(part.strip()) for part in value.split(",") if part.strip()]
    except ValueError as error:
        raise ValueError("--datanode-pids must be two comma-separated positive integer PIDs") from error
    if len(pids) != 2 or len(set(pids)) != 2 or any(pid <= 0 for pid in pids):
        raise ValueError("--datanode-pids must name exactly two distinct positive DataNode PIDs")
    return pids


def pids_from_deployment_root(root: Path, deployment: str) -> list[int]:
    """Read only the two launcher-owned PID files immediately before a measurement.

    Fixed-DOP matrix steps restart DataNodes, so a PID copied into the query-command template is
    stale after the first transition.  The isolated launcher writes one small ``process.env`` for
    each named DataNode.  Parse only its literal ``pid=<positive integer>`` assignment rather than
    sourcing shell content from an experiment directory.
    """

    if deployment not in {"candidate", "control"}:
        raise ValueError("--deployment must be candidate or control")
    pids: list[int] = []
    for node in ("datanode-1", "datanode-2"):
        path = root / deployment / node / "process.env"
        try:
            matches = re.findall(r"(?m)^pid=([1-9][0-9]*)$", path.read_text(encoding="utf-8"))
        except OSError as error:
            raise ValueError(f"cannot read launcher DataNode PID manifest {path}: {error}") from error
        if len(matches) != 1:
            raise ValueError(f"expected exactly one literal pid assignment in {path}")
        pids.append(int(matches[0]))
    return parse_two_pids(",".join(map(str, pids)))


def non_negative_number(value: Any, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise RuntimeError(f"{field} is not numeric: {value!r}") from error
    if not math.isfinite(number) or number < 0:
        raise RuntimeError(f"{field} is not finite and non-negative: {value!r}")
    return number


def child_directories(raw_dir: Path) -> tuple[Path, Path, Path]:
    """Reserve dedicated evidence directories without overwriting a matrix attempt."""

    server_dir = raw_dir / "server-current-query"
    proc_dir = raw_dir / "datanode-proc"
    combined = raw_dir / "server-query-metrics-with-proc.json"
    collisions = [path for path in (server_dir, proc_dir, combined) if path.exists()]
    if collisions:
        raise ValueError("attempt evidence already exists and will not be overwritten: " + ", ".join(map(str, collisions)))
    return server_dir, proc_dir, combined


def metric_command(args: argparse.Namespace, server_dir: Path) -> list[str]:
    command = [
        sys.executable, str(args.metrics_script), "--cli", str(args.cli), "--host", args.host,
        "--port", str(args.port), "--username", args.username, "--password-env", args.password_env,
        "--sql-file", str(args.sql_file), "--raw-dir", str(server_dir),
        "--history-timeout-seconds", str(args.history_timeout_seconds),
        "--history-poll-seconds", str(args.history_poll_seconds),
    ]
    for cli_arg in args.cli_arg:
        command.extend(["--cli-arg", cli_arg])
    return command


def remote_shuffle_metrics(
    args: argparse.Namespace, raw_dir: Path, query_id: Optional[str]
) -> Tuple[Optional[int], str, Optional[str]]:
    """Archive the durable remote-payload audit only when the caller supplied all logs.

    The extractor rejects a direction mismatch by default. This protects the matrix from silently
    treating an incomplete two-DataNode log set as a query-scoped metric.
    """

    if not args.shuffle_audit_log:
        return None, "unmeasured_no_query_scoped_remote_payload_audit_logs", None
    if not query_id:
        raise RuntimeError("server history did not return query_id for remote shuffle audit")
    audit_dir = raw_dir / "remote-shuffle-audit"
    if audit_dir.exists():
        raise RuntimeError(f"remote shuffle audit directory already exists: {audit_dir}")
    command = [
        sys.executable, str(args.shuffle_audit_script), "--query-id", str(query_id),
        "--raw-dir", str(audit_dir),
    ]
    for log in args.shuffle_audit_log:
        command.extend(["--log", str(log)])
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    (raw_dir / "remote-shuffle-audit.stdout").write_text(completed.stdout, encoding="utf-8")
    (raw_dir / "remote-shuffle-audit.stderr").write_text(completed.stderr, encoding="utf-8")
    (raw_dir / "remote-shuffle-audit.command.json").write_text(
        json.dumps(command, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    if completed.returncode:
        raise RuntimeError(f"remote shuffle audit extractor failed: exit {completed.returncode}")
    try:
        payload = json.loads(completed.stdout)
        shuffle_bytes = int(payload["shuffle_bytes"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise RuntimeError("remote shuffle audit extractor did not return a valid byte count") from error
    if shuffle_bytes <= 0 or payload.get("direction_match") is not True:
        raise RuntimeError("remote shuffle audit is not a complete non-zero bidirectional measurement")
    return shuffle_bytes, str(payload.get("shuffle_bytes_source")), str(audit_dir / "remote-shuffle-payload-bytes.json")


def run(args: argparse.Namespace) -> int:
    pids = (
        parse_two_pids(args.datanode_pids)
        if args.datanode_pids
        else pids_from_deployment_root(args.deployment_root, args.deployment)
    )
    if not args.raw_dir.exists():
        args.raw_dir.mkdir(parents=True, exist_ok=False)
    if not args.raw_dir.is_dir():
        raise ValueError("--raw-dir must be a directory")
    server_dir, proc_dir, combined_path = child_directories(args.raw_dir)
    command = [
        sys.executable, str(args.proc_script), "--pids", ",".join(map(str, pids)),
        "--output", str(proc_dir), "--interval-seconds", str(args.proc_interval_seconds), "--",
        *metric_command(args, server_dir),
    ]
    # Child stdout contains a metrics object. Capture it so the public stdout remains exactly
    # one final combined object for run_matrix.py. The collector also retains it in proc_dir.
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    (args.raw_dir / "combined-wrapper.stdout").write_text(completed.stdout, encoding="utf-8")
    (args.raw_dir / "combined-wrapper.stderr").write_text(completed.stderr, encoding="utf-8")
    if completed.returncode:
        raise RuntimeError(f"DataNode collector/wrapped server metrics command failed: exit {completed.returncode}")

    server = read_json(server_dir / "metrics.json")
    proc = read_json(proc_dir / "summary.json")
    query_ms = non_negative_number(server.get("query_ms"), "server query_ms")
    cpu_pct = non_negative_number(proc.get("cpu_core_pct"), "DataNode cpu_core_pct")
    peak_rss = non_negative_number(proc.get("peak_rss_bytes"), "DataNode peak_rss_bytes")
    if proc.get("wrapped_command_exit_code") != 0:
        raise RuntimeError("collector summary reports a failed wrapped server metrics command")
    if proc.get("pid_starttime_verified") is not True:
        raise RuntimeError("collector did not verify DataNode PID identities")
    shuffle_bytes, shuffle_status, shuffle_evidence = remote_shuffle_metrics(
        args, args.raw_dir, server.get("query_id")
    )
    result = {
        "query_ms": query_ms,
        "cpu_pct": cpu_pct,
        "peak_rss_bytes": int(peak_rss),
        "shuffle_bytes": shuffle_bytes,
        "shuffle_bytes_status": shuffle_status,
        "shuffle_bytes_evidence": shuffle_evidence,
        "query_ms_source": "information_schema.current_queries.cost_time",
        "cpu_pct_source": "two_datanode_proc_cpu_core_pct",
        "peak_rss_bytes_source": "two_datanode_proc_combined_peak_rss",
        "datanode_pids": pids,
        "datanode_pid_source": (
            "explicit" if args.datanode_pids else f"launcher_process_env:{args.deployment_root}"
        ),
        "pid_starttime_verified": True,
        "server_query_id": server.get("query_id"),
    }
    write_json(combined_path, result)
    # Keep stdout intentionally one line: run_matrix.py scans it for the final JSON object.
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0


def self_test() -> int:
    if parse_two_pids("7,8") != [7, 8]:
        return 1
    for invalid in ("", "7", "7,7", "0,8", "x,8", "7,8,9"):
        try:
            parse_two_pids(invalid)
        except ValueError:
            continue
        return 1
    with tempfile.TemporaryDirectory() as temporary:
        raw = Path(temporary)
        server_dir, proc_dir, combined = child_directories(raw)
        if server_dir.parent != raw or proc_dir.parent != raw or combined.parent != raw:
            return 1
        server_dir.mkdir()
        try:
            child_directories(raw)
        except ValueError:
            pass
        else:
            return 1
        deployment = raw / "isolated"
        for node, pid in (("datanode-1", 7), ("datanode-2", 8)):
            path = deployment / "candidate" / node
            path.mkdir(parents=True)
            (path / "process.env").write_text(f"pid={pid}\nroot=/not-sourced\n", encoding="utf-8")
        if pids_from_deployment_root(deployment, "candidate") != [7, 8]:
            return 1
    print("server_query_metrics_with_proc self-test passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--metrics-script", type=Path, default=DEFAULT_METRICS_SCRIPT)
    parser.add_argument("--proc-script", type=Path, default=DEFAULT_PROC_SCRIPT)
    parser.add_argument("--shuffle-audit-script", type=Path, default=DEFAULT_SHUFFLE_AUDIT_SCRIPT)
    parser.add_argument(
        "--shuffle-audit-log",
        action="append",
        type=Path,
        default=[],
        help="non-overlapping DataNode log; repeat once per participating DataNode",
    )
    parser.add_argument("--cli", type=Path)
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--username", default="root")
    parser.add_argument("--password-env", default="IOTDB_PASSWORD")
    parser.add_argument("--cli-arg", action="append", default=[])
    parser.add_argument("--sql-file", type=Path)
    parser.add_argument("--raw-dir", type=Path)
    pid_source = parser.add_mutually_exclusive_group()
    pid_source.add_argument("--datanode-pids", help="two explicit current DataNode PIDs")
    pid_source.add_argument(
        "--deployment-root",
        type=Path,
        help="isolated launcher root; current DataNode PIDs are read before every query",
    )
    parser.add_argument("--deployment", default="candidate", choices=("candidate", "control"))
    parser.add_argument("--history-timeout-seconds", type=float, default=5.0)
    parser.add_argument("--history-poll-seconds", type=float, default=0.1)
    parser.add_argument("--proc-interval-seconds", type=float, default=0.1)
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if args.datanode_pids is None and args.deployment_root is None:
        parser.error("one of --datanode-pids or --deployment-root is required")
    required = ("cli", "host", "port", "sql_file", "raw_dir")
    if any(getattr(args, field) is None for field in required):
        parser.error("--cli, --host, --port, --sql-file, and --raw-dir are required")
    if not args.cli.is_file() or not args.sql_file.is_file():
        parser.error("--cli and --sql-file must name existing files")
    if not args.metrics_script.is_file() or not args.proc_script.is_file():
        parser.error("--metrics-script and --proc-script must name existing files")
    if args.shuffle_audit_log and not args.shuffle_audit_script.is_file():
        parser.error("--shuffle-audit-script must name an existing file when audit logs are supplied")
    if not 1 <= args.port <= 65535:
        parser.error("--port must be in 1..65535")
    if args.history_timeout_seconds <= 0 or args.history_poll_seconds <= 0 or args.proc_interval_seconds <= 0:
        parser.error("history and /proc intervals must be positive")
    try:
        return run(args)
    except (OSError, RuntimeError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
