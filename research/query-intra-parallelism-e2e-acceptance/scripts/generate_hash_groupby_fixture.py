#!/usr/bin/env python3
"""Generate a deterministic, cross-source table-model GROUP BY fixture.

Each group occurs on every device, so partial aggregates from different regions
must be co-located before final aggregation.  The generated output is CSV-ready
for IoTDB's table import tool.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--devices", type=int, default=4)
    parser.add_argument("--rows-per-device", type=int, default=250_000)
    parser.add_argument("--groups", type=int, default=25_000)
    args = parser.parse_args()
    if min(args.devices, args.rows_per_device, args.groups) < 1:
        parser.error("devices, rows-per-device and groups must be positive")

    args.output.mkdir(parents=True, exist_ok=True)
    for device in range(args.devices):
        path = args.output / f"device-{device:02d}.csv"
        with path.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream)
            writer.writerow(("time", "device_id", "g", "s1"))
            base = device * args.rows_per_device
            for row in range(args.rows_per_device):
                value = base + row
                writer.writerow((row, f"opd{device:02d}", value % args.groups, value))

    (args.output / "manifest.json").write_text(
        json.dumps(
            {
                "devices": args.devices,
                "rows_per_device": args.rows_per_device,
                "groups": args.groups,
                "total_rows": args.devices * args.rows_per_device,
                "schema": "time TIMESTAMP TIME, device_id STRING TAG, g INT32 FIELD, s1 INT64 FIELD",
                "cross_source_groups": True,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
