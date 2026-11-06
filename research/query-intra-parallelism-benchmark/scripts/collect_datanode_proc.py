#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.

"""Archive Linux DataNode CPU/RSS samples around a command without changing the server.

It identifies each process by ``/proc/<pid>/stat`` starttime and aborts if a PID is reused.
``cpu_core_pct`` means one fully occupied CPU core is 100; it is not host-wide CPU percent.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_stat(pid: int) -> dict[str, int]:
    text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    right = text.rfind(")")
    if right < 0:
        raise ValueError(f"unexpected /proc/{pid}/stat format")
    fields = text[right + 2 :].split()  # index 0 is proc field 3 (state)
    if len(fields) <= 21:
        raise ValueError(f"truncated /proc/{pid}/stat")
    return {"utime_ticks": int(fields[11]), "stime_ticks": int(fields[12]), "starttime_ticks": int(fields[19])}


def parse_status(pid: int) -> dict[str, int]:
    values: dict[str, int] = {}
    for line in Path(f"/proc/{pid}/status").read_text(encoding="utf-8").splitlines():
        key, _, rest = line.partition(":")
        if key in {"VmRSS", "VmHWM"}:
            values[key] = int(rest.split()[0]) * 1024
    if "VmRSS" not in values:
        raise ValueError(f"/proc/{pid}/status has no VmRSS")
    return {"rss_bytes": values["VmRSS"], "hwm_bytes": values.get("VmHWM", values["VmRSS"])}


def sample(pid: int, expected_starttime: int) -> dict[str, Any]:
    stat = parse_stat(pid)
    if stat["starttime_ticks"] != expected_starttime:
        raise RuntimeError(f"PID {pid} was reused: expected starttime {expected_starttime}, got {stat['starttime_ticks']}")
    return {"pid": pid, "at_utc": utc_now(), "monotonic_ns": time.monotonic_ns(), **stat, **parse_status(pid)}


def parse_pids(value: str) -> list[int]:
    pids = [int(part.strip()) for part in value.split(",") if part.strip()]
    if not pids or len(pids) != len(set(pids)) or any(pid <= 0 for pid in pids):
        raise ValueError("--pids must be a nonempty comma-separated list of distinct positive PIDs")
    return pids


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> int:
    if os.name != "posix" or not Path("/proc").is_dir():
        raise RuntimeError("this collector requires Linux /proc")
    if args.output.exists() and any(args.output.iterdir()):
        raise ValueError("--output must be new or empty to preserve raw evidence")
    args.output.mkdir(parents=True, exist_ok=True)
    pids = parse_pids(args.pids)
    identities = {pid: parse_stat(pid)["starttime_ticks"] for pid in pids}
    write_json(args.output / "manifest.json", {
        "started_at_utc": utc_now(), "pids": pids, "starttime_ticks": identities,
        "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"), "page_size": os.sysconf("SC_PAGE_SIZE"),
        "host": platform.node(), "sample_interval_seconds": args.interval_seconds,
        "command": args.command, "cpu_definition": "sum(DN delta utime+stime)/CLK_TCK/wall_seconds*100; 100 = one CPU core",
    })
    raw = (args.output / "raw-samples.jsonl").open("w", encoding="utf-8")
    samples: list[dict[str, Any]] = []
    child = subprocess.Popen(args.command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) if args.command else None
    exit_code = 0
    try:
        while child is None or child.poll() is None:
            batch = [sample(pid, identities[pid]) for pid in pids]
            for item in batch:
                raw.write(json.dumps(item, ensure_ascii=False) + "\n")
            raw.flush()
            samples.extend(batch)
            time.sleep(args.interval_seconds)
        # One final snapshot makes the CPU interval end at or after the child completion.
        batch = [sample(pid, identities[pid]) for pid in pids]
        for item in batch:
            raw.write(json.dumps(item, ensure_ascii=False) + "\n")
        raw.flush()
        samples.extend(batch)
        if child is not None:
            stdout, stderr = child.communicate()
            (args.output / "wrapped-command.stdout").write_text(stdout, encoding="utf-8")
            (args.output / "wrapped-command.stderr").write_text(stderr, encoding="utf-8")
            sys.stdout.write(stdout)
            sys.stderr.write(stderr)
            exit_code = child.returncode
    finally:
        raw.close()
        if samples:
            first = {item["pid"]: item for item in samples[:len(pids)]}
            last = {item["pid"]: item for item in samples[-len(pids):]}
            elapsed = (max(item["monotonic_ns"] for item in last.values()) - min(item["monotonic_ns"] for item in first.values())) / 1e9
            ticks = sum(last[pid]["utime_ticks"] + last[pid]["stime_ticks"] - first[pid]["utime_ticks"] - first[pid]["stime_ticks"] for pid in pids)
            rss_by_batch = [sum(item["rss_bytes"] for item in samples[index:index + len(pids)]) for index in range(0, len(samples), len(pids))]
            write_json(args.output / "summary.json", {
                "finished_at_utc": utc_now(), "sample_count_per_datanode": len(samples) // len(pids),
                "wall_seconds": elapsed, "cpu_jiffies": ticks,
                "cpu_core_pct": ticks / os.sysconf("SC_CLK_TCK") / elapsed * 100 if elapsed > 0 else None,
                "peak_rss_bytes": max(rss_by_batch), "final_rss_bytes": rss_by_batch[-1],
                "pid_starttime_verified": True, "wrapped_command_exit_code": exit_code,
            })
    return exit_code


def self_test() -> int:
    if os.name != "posix" or not Path("/proc").is_dir():
        print("collect_datanode_proc self-test skipped: Linux /proc is unavailable")
        return 0
    me = os.getpid()
    identity = parse_stat(me)["starttime_ticks"]
    item = sample(me, identity)
    if item["pid"] != me or item["rss_bytes"] <= 0:
        return 1
    try:
        sample(me, identity + 1)
    except RuntimeError:
        print("collect_datanode_proc self-test passed")
        return 0
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--pids")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--interval-seconds", type=float, default=0.1)
    parser.add_argument("command", nargs=argparse.REMAINDER, help="optional wrapped command, after --")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if not args.pids or args.output is None:
        parser.error("--pids and --output are required")
    if args.interval_seconds <= 0:
        parser.error("--interval-seconds must be positive")
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    try:
        return run(args)
    except (OSError, ValueError, RuntimeError) as error:
        print(str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
