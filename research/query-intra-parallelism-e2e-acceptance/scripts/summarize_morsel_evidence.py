#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the License is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.

"""Summarize exported EXPLAIN ANALYZE morsel evidence without contacting an IoTDB server."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


PIPELINE = re.compile(r"\[([^\]]*-morsel-\d+)\]:")
FIELD = re.compile(
    r"(MORSEL_(?:SCHEDULING|TIME_PARTITIONS|DEVICE_COUNT|ESTIMATED_TSFILE_BYTES|"
    r"DRIVER_WALL_TIME_MS)):\s*(.+)"
)
OUTPUT_ROWS = re.compile(r"output:\s*(\d+)\s+rows")
CPU_TIME = re.compile(r"CPU Time:\s*([0-9]+(?:\.[0-9]+)?)\s+ms")
TS_BLOCKS = re.compile(r"TsBlock output count:\s*(\d+)")


def clean_line(line: str) -> str:
    """Remove CLI table borders while retaining the EXPLAIN ANALYZE payload."""
    return line.strip().strip("|").strip()


def parse_plan(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise ValueError(f"plan does not exist: {path}")
    pipelines: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = clean_line(raw_line)
        match = PIPELINE.search(line)
        if match:
            current = {"plan_node_id": match.group(1)}
            pipelines.append(current)
            continue
        if current is None:
            continue
        if match := FIELD.search(line):
            current[match.group(1).lower()] = match.group(2)
        elif match := OUTPUT_ROWS.search(line):
            current["actual_output_rows"] = int(match.group(1))
        elif match := CPU_TIME.search(line):
            current["actual_cpu_time_ms"] = float(match.group(1))
        elif match := TS_BLOCKS.search(line):
            current["actual_tsblock_output_count"] = int(match.group(1))
    if not pipelines:
        raise ValueError(f"no -morsel- pipeline entries found in {path}")
    for pipeline in pipelines:
        required = {"morsel_scheduling", "morsel_time_partitions", "morsel_driver_wall_time_ms"}
        missing = required.difference(pipeline)
        if missing:
            raise ValueError(f"incomplete morsel evidence in {path}: {sorted(missing)}")
    return pipelines


def numeric_max(pipelines: list[dict[str, Any]], key: str) -> float | None:
    values = [float(pipeline[key]) for pipeline in pipelines if key in pipeline]
    return max(values) if values else None


def summarize(label: str, pipelines: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "label": label,
        "pipeline_count": len(pipelines),
        "scheduling": sorted({pipeline["morsel_scheduling"] for pipeline in pipelines}),
        "max_driver_wall_time_ms": numeric_max(pipelines, "morsel_driver_wall_time_ms"),
        "max_cpu_time_ms": numeric_max(pipelines, "actual_cpu_time_ms"),
        "max_estimated_tsfile_bytes": numeric_max(
            pipelines, "morsel_estimated_tsfile_bytes"
        ),
        "pipelines": pipelines,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lpt-plan", required=True, type=Path)
    parser.add_argument("--equal-count-plan", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("--output must not already exist; evidence is append-only")
    try:
        lpt = parse_plan(args.lpt_plan)
        equal_count = parse_plan(args.equal_count_plan)
        report = {
            "lpt": summarize("LPT_TSFILE_BYTES", lpt),
            "equal_count": summarize("EQUAL_PARTITION_COUNT", equal_count),
            "interpretation": (
                "max_driver_wall_time_ms is the morsel scan-stage long-tail proxy, not whole-query "
                "latency; estimated TsFile bytes are a plan-time estimate, not observed work"
            ),
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    except (OSError, ValueError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
