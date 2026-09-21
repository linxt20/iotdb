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

"""Generate deterministic table-model CSV fixture shards for the P0 workload."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--devices", default=64, type=int)
    parser.add_argument("--partitions", default=10, type=int)
    parser.add_argument("--rows-per-partition", default=100_000, type=int)
    parser.add_argument("--start-time-ms", default=0, type=int)
    parser.add_argument("--partition-interval-ms", default=86_400_000, type=int)
    parser.add_argument("--row-interval-ms", default=1, type=int)
    args = parser.parse_args()
    if min(args.devices, args.partitions, args.rows_per_partition, args.partition_interval_ms, args.row_interval_ms) < 1:
        parser.error("all counts and intervals must be positive")
    if args.rows_per_partition * args.row_interval_ms > args.partition_interval_ms:
        parser.error("rows-per-partition * row-interval-ms must fit in one partition interval")
    args.output.mkdir(parents=True, exist_ok=True)
    total_rows = 0
    for partition in range(args.partitions):
        file_path = args.output / f"fixture-partition-{partition:02d}.csv"
        with file_path.open("w", newline="", encoding="utf-8") as target:
            writer = csv.writer(target)
            # The bundled import-data tool requires Time as the first header and device_id second.
            writer.writerow(("Time", "device_id", "s1", "s2"))
            partition_start = args.start_time_ms + partition * args.partition_interval_ms
            for device in range(args.devices):
                device_id = f"d{device:03d}"
                sequence_base = (device * args.partitions + partition) * args.rows_per_partition
                for row in range(args.rows_per_partition):
                    s1 = sequence_base + row
                    writer.writerow(
                        (partition_start + row * args.row_interval_ms, device_id, s1, f"{s1}.5")
                    )
                    total_rows += 1
    manifest = {
        "devices": args.devices,
        "partitions": args.partitions,
        "rows_per_partition": args.rows_per_partition,
        "total_rows": total_rows,
        "columns": ["Time", "device_id", "s1", "s2"],
        "s1": "(device * partitions + partition) * rows_per_partition + row",
        "s2": "s1 + 0.5",
    }
    (args.output / "fixture-manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
