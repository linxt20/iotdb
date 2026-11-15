#!/usr/bin/env python3
"""Generate a deterministic, cross-source table-model GROUP BY fixture.

Each group occurs on every device, so partial aggregates from different regions
must be co-located before final aggregation. Legacy mode emits CSV for the
IoTDB import tool; runtime-sql-v1 emits bounded gzip-TSV partitions and typed
row-oracle manifests for the ordinary-SQL acceptance loader.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import re
from pathlib import Path


def legacy_fixture(args: argparse.Namespace) -> int:
    if min(args.devices, args.rows_per_device, args.groups) < 1:
        raise ValueError("devices, rows-per-device and groups must be positive")

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


RUNTIME_VERSION = "runtime_sql_fixture_v1"
RUNTIME_COLUMNS = [
    {"name": "time", "type": "TIMESTAMP", "category": "TIME"},
    {"name": "device_id", "type": "STRING", "category": "TAG"},
    {"name": "g", "type": "INT64", "category": "FIELD"},
    {"name": "s1", "type": "INT64", "category": "FIELD"},
    {"name": "s2", "type": "INT64", "category": "FIELD"},
]
RUNTIME_HEADER = b"time	device_id	g	s1	s2\n"
NULL_MARKER = r"\N"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_row(values: list[object]) -> bytes:
    return (json.dumps(values, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")


def runtime_config(args: argparse.Namespace) -> dict[str, object]:
    base = args.runtime_base_rows
    heavy = args.runtime_heavy_rows
    if base is None:
        base = 65_536 if args.runtime_preset == "scaled" else 32_768
    if heavy is None:
        heavy = 2_097_152 if args.runtime_preset == "scaled" else 1_048_576
    devices = args.devices if args.devices is not None else 64
    groups = args.groups if args.groups is not None else 257
    if groups != 257:
        raise ValueError("runtime-sql-v1 fixes exactly 257 grouping keys")
    if min(devices, base, heavy, args.time_partitions, args.time_partition_interval_ms) < 1:
        raise ValueError("runtime fixture dimensions must be positive")
    if devices > 1000:
        raise ValueError("runtime fixture uses d000..d999 identifiers")
    if max(base, heavy) > args.time_partition_interval_ms:
        raise ValueError("a device-partition cell must stay within its declared time partition")
    if heavy < base:
        raise ValueError("heavy cell rows must not be smaller than a base cell")
    for identifier in (args.database, args.table):
        if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", identifier):
            raise ValueError("database/table must be simple SQL identifiers")
    config = {
        "database": args.database,
        "table": args.table,
        "devices": devices,
        "timePartitions": args.time_partitions,
        "timePartitionIntervalMs": args.time_partition_interval_ms,
        "baseRowsPerDevicePartition": base,
        "groups": groups,
        "heavyCell": {
            "deviceIndex": devices - 1,
            "partitionIndex": args.time_partitions - 1,
            "rows": heavy,
        },
    }
    config["totalRows"] = devices * args.time_partitions * base + heavy - base
    # IoTDB SUM(INT64) returns DOUBLE. Keep every exact-once partial and merged sum
    # within the consecutive-integer binary64 range, so reduction order is immaterial.
    # 1007 bounds abs(s1 + 7) as well as abs(s1) and abs(s2).
    if config["totalRows"] * 1007 >= 2**53:
        raise ValueError("runtime fixture SUM oracle requires all partial sums below 2**53")
    # SQLite/Java/Iotdb all use signed 64-bit timestamps/fields for this fixture.
    if (args.time_partitions - 1) * args.time_partition_interval_ms + heavy - 1 >= 2**63:
        raise ValueError("runtime fixture timestamp exceeds signed INT64")
    return config


def cell_rows(config: dict[str, object], device: int, partition: int) -> int:
    heavy = config["heavyCell"]
    if device == heavy["deviceIndex"] and partition == heavy["partitionIndex"]:
        return heavy["rows"]
    return config["baseRowsPerDevicePartition"]


def values_for(config: dict[str, object], device: int, partition: int, row: int) -> list[object]:
    return [
        partition * config["timePartitionIntervalMs"] + row,
        f"d{device:03d}",
        (device * 17 + row) % 257,
        ((device * 1009 + row) % 2001) - 1000,
        None if (device + partition + row) % 17 == 0 else (row % 127) - 63,
    ]


def raw_predicate(s1: int) -> bool:
    # SQL/Java remainder retains the dividend's sign, unlike Python's % for negative values.
    return s1 >= 0 or s1 % 97 == 0



def full_scan_window_sql(full_table: str) -> str:
    # CASE keeps the time window behind a row-dependent, nonpushdown first WHEN.
    # All nested arguments are finite; sin(x)^2+cos(x)^2 remains far above0.5.
    argument = "CAST(s1 AS DOUBLE)"
    for _ in range(4):
        argument = f"SIN({argument})"
    return (
        f"SELECT time, device_id, g, s1, s2\nFROM {full_table}\nWHERE (CASE\n"
        f"  WHEN (SIN({argument}) * SIN({argument})\n"
        f"        + COS({argument}) * COS({argument})) > 0.5\n"
        "  THEN CASE WHEN time >= 0 AND time < 64 THEN s1 - s1 + 1 ELSE s1 - s1 END\n"
        "  ELSE s1 - s1\nEND) = 1\n"
    )



def runtime_early_limit_sql(full_table: str) -> str:
    return (
        f"SELECT CAST(1 AS INT64) AS c\nFROM {full_table}\n"
        "WHERE (SIN(CAST(s1 AS DOUBLE)) * SIN(CAST(s1 AS DOUBLE))\n"
        "       + COS(CAST(s1 AS DOUBLE)) * COS(CAST(s1 AS DOUBLE))) > 0.5\n"
        "LIMIT 257\n"
    )


def add_group(groups: list[list[object]], values: list[object]) -> None:
    _, _, g, s1, s2 = values
    record = groups[g]
    record[1] += 1
    record[3] += s1
    record[5] = s1 if record[5] is None else min(record[5], s1)
    record[6] = s1 if record[6] is None else max(record[6], s1)
    if s2 is not None:
        record[2] += 1
        record[4] = (record[4] or 0) + s2


def write_jsonl(path: Path, rows: list[list[object]]) -> dict[str, object]:
    with path.open("wb") as stream:
        for row in rows:
            stream.write(canonical_row(row))
    return {"path": path.as_posix(), "sha256": sha256_file(path), "rows": len(rows)}


def runtime_fixture(args: argparse.Namespace) -> int:
    config = runtime_config(args)
    fingerprint = hashlib.sha256(canonical_row([config])).hexdigest()
    standard = (
        config["devices"] == 64 and config["timePartitions"] == 4
        and config["baseRowsPerDevicePartition"] == 32_768
        and config["heavyCell"]["rows"] == 1_048_576
        and config["timePartitionIntervalMs"] == 604_800_000
        and config["database"] == "runtime_dop_e2e" and config["table"] == "telemetry"
    )
    version = RUNTIME_VERSION if standard else RUNTIME_VERSION + "-" + fingerprint[:12]
    if args.plan_only:
        print(json.dumps({
            "status": "PLAN_ONLY_NO_FILES_OR_ROWS_WRITTEN",
            "fixtureVersion": version, "configurationSha256": fingerprint, **config,
            "format": "gzip-tsv-v1", "columns": RUNTIME_COLUMNS,
        }, indent=2))
        return 0
    if args.output.is_symlink() or (args.output.exists() and any(args.output.iterdir())):
        raise ValueError("runtime fixture refuses a symlink or nonempty output directory")
    args.output.mkdir(parents=True, exist_ok=True)
    oracle_dir = args.output / "oracles"
    oracle_dir.mkdir()
    grouped = [[g, 0, 0, 0, None, None, None] for g in range(257)]
    projected = [[g + 1, 0, 0] for g in range(257)]
    partitions = []
    filtered_rows = 0
    ordered = []
    for partition in range(config["timePartitions"]):
        name = f"partition-{partition:03d}.tsv.gz"
        path = args.output / name
        raw_digest = hashlib.sha256()
        count = 0
        cells = []
        with path.open("xb") as backing:
            # Empty gzip filename and mtime=0 make the compressed byte identity reproducible.
            with gzip.GzipFile(filename="", fileobj=backing, mode="wb", compresslevel=1, mtime=0) as stream:
                stream.write(RUNTIME_HEADER)
                raw_digest.update(RUNTIME_HEADER)
                for device in range(config["devices"]):
                    rows = cell_rows(config, device, partition)
                    cells.append({
                        "deviceIndex": device, "deviceId": f"d{device:03d}", "rows": rows,
                        "minTime": partition * config["timePartitionIntervalMs"],
                        "maxTime": partition * config["timePartitionIntervalMs"] + rows - 1,
                    })
                    chunk = []
                    for row in range(rows):
                        values = values_for(config, device, partition, row)
                        add_group(grouped, values)
                        if values[3] >= 0:
                            projected[values[2]][1] += 1
                            projected[values[2]][2] += values[3] + 7
                        if raw_predicate(values[3]):
                            filtered_rows += 1
                        if values[0] < 64:
                            ordered.append(values)
                        chunk.append(
                            f"{values[0]}	{values[1]}	{values[2]}	{values[3]}	"
                            f"{NULL_MARKER if values[4] is None else values[4]}\n"
                        )
                        if len(chunk) == 4096:
                            data = "".join(chunk).encode("utf-8")
                            stream.write(data)
                            raw_digest.update(data)
                            chunk.clear()
                    if chunk:
                        data = "".join(chunk).encode("utf-8")
                        stream.write(data)
                        raw_digest.update(data)
                    count += rows
        partitions.append({
            "partitionIndex": partition, "path": name, "sha256": sha256_file(path),
            "bytes": path.stat().st_size, "uncompressedSha256": raw_digest.hexdigest(),
            "rows": count, "cells": cells, "flushAfterLoad": True,
        })
    if sum(partition["rows"] for partition in partitions) != config["totalRows"]:
        raise AssertionError("partition row counts disagree with immutable geometry")
    if sum(record[1] for record in grouped) != config["totalRows"]:
        raise AssertionError("group oracle row total mismatch")
    full_table = f"{config['database']}.{config['table']}"
    oracles = {}
    # Preserve exact integer accumulation while generating rows, then encode the
    # native IoTDB SUM output type. NULL remains NULL for all-NULL SUM groups.
    grouped_output = [
        [*row[:3], float(row[3]), None if row[4] is None else float(row[4]), *row[5:]]
        for row in grouped if row[1] > 0
    ]
    projected_output = [[*row[:2], float(row[2])] for row in projected if row[1] > 0]
    definitions = {
        "grouped": (
            grouped_output, ["INT64", "INT64", "INT64", "DOUBLE", "DOUBLE", "INT64", "INT64"], ["g"],
            f"SELECT g, COUNT(*) AS n, COUNT(s2) AS n2, SUM(s1) AS sum1, SUM(s2) AS sum2, "
            f"MIN(s1) AS lo, MAX(s1) AS hi FROM {full_table} GROUP BY g"),
        "filterProjectGrouped": (
            projected_output, ["INT64", "INT64", "DOUBLE"], ["k"],
            f"SELECT k, COUNT(*) AS n, SUM(v) AS total FROM "
            f"(SELECT g + 1 AS k, s1 + 7 AS v FROM {full_table} WHERE s1 >= 0) q GROUP BY k"),
        "runtimeEarlyLimitConstant": (
            [[1] for _ in range(min(config["totalRows"], 257))],
            ["INT64"], [], runtime_early_limit_sql(full_table)),
        "fullScanWindowFiltered": (
            sorted(ordered, key=lambda values: (values[1], values[0])),
            ["TIMESTAMP", "STRING", "INT64", "INT64", "INT64"], [],
            full_scan_window_sql(full_table)),
        "ordered": (
            sorted(ordered, key=lambda values: (values[1], values[0])),
            ["TIMESTAMP", "STRING", "INT64", "INT64", "INT64"], ["device_id", "time"],
            f"SELECT time, device_id, g, s1, s2 FROM {full_table} "
            f"WHERE time >= 0 AND time < 64 ORDER BY device_id ASC, time ASC"),
    }
    for name, (rows, column_types, order_by, sql) in definitions.items():
        path = oracle_dir / (name + ".jsonl")
        metadata = write_jsonl(path, rows)
        metadata["path"] = path.relative_to(args.output).as_posix()
        metadata.update({
            "columnTypes": column_types, "orderBy": order_by,
            "canonicalEncoding": "typed-json-array-lf", "sql": sql, "materialized": True,
        })
        if name == "runtimeEarlyLimitConstant":
            metadata.update({
                "selectionSemantics": "UNORDERED_COMPLETE_LIMIT_CONSTANT_V1",
                "columnNames": ["c"], "sqlLimit": 257,
            })
        if name == "fullScanWindowFiltered":
            metadata.update({
                "canonicalStorageOrder": ["device_id", "time"],
                "selectionSemantics": "FULL_SCAN_POST_FILTER_TIME_WINDOW_V1",
            })
        oracles[name] = metadata
    # Stream a second bounded-memory pass in canonical device/time order. No 9M-row Counter
    # or INSERT text is created. The acceptance runner externally sorts actual raw output by
    # these exact typed keys before comparing this SHA and full row count.
    raw_digest = hashlib.sha256()
    raw_count = 0
    for device in range(config["devices"]):
        for partition in range(config["timePartitions"]):
            for row in range(cell_rows(config, device, partition)):
                values = values_for(config, device, partition, row)
                if raw_predicate(values[3]):
                    raw_digest.update(canonical_row(values))
                    raw_count += 1
    if raw_count != filtered_rows:
        raise AssertionError("raw-filter oracle count mismatch")
    oracles["rawFiltered"] = {
        "materialized": False, "rows": raw_count, "sha256": raw_digest.hexdigest(),
        "columnTypes": ["TIMESTAMP", "STRING", "INT64", "INT64", "INT64"],
        "orderBy": ["device_id", "time"], "canonicalEncoding": "typed-json-array-lf",
        "recomputableFrom": "rowDefinition",
        "sql": f"SELECT time, device_id, g, s1, s2 FROM {full_table} WHERE ((s1 * 17) % 97) >= 0",
    }
    setup = [
        f"CREATE DATABASE {config['database']} WITH (max_data_region_group_num=2)",
        f"USE {config['database']}",
        f"CREATE TABLE {config['table']} "
        "(device_id STRING TAG, g INT64 FIELD, s1 INT64 FIELD, s2 INT64 FIELD)",
    ]
    schema_path = args.output / "schema.sql"
    schema_path.write_text(";\n".join(setup) + ";\n", encoding="utf-8")
    manifest = {
        "schemaVersion": 2, "fixtureVersion": version, "configurationSha256": fingerprint,
        "generatorSha256": sha256_file(Path(__file__).resolve()),
        **config, "format": "gzip-tsv-v1", "encoding": "UTF-8", "lineEnding": "LF",
        "compression": {"codec": "gzip", "level": 1, "mtime": 0, "filename": ""},
        "nullMarker": NULL_MARKER, "columns": RUNTIME_COLUMNS,
        "header": RUNTIME_HEADER.decode("ascii").rstrip("\n").split("	"),
        "setupSql": setup, "schemaSqlFile": {
            "path": "schema.sql", "sha256": sha256_file(schema_path)},
        "rowOrder": ["partitionIndex", "deviceIndex", "rowIndex"],
        "rowDefinition": {
            "device_id": "d + zero-padded decimal device index, width=3",
            "time": "partitionIndex*timePartitionIntervalMs + rowIndex",
            "g": "(deviceIndex*17+rowIndex)%257",
            "s1": "((deviceIndex*1009+rowIndex)%2001)-1000",
            "s2": "NULL iff (deviceIndex+partitionIndex+rowIndex)%17==0, otherwise (rowIndex%127)-63",
        },
        "partitions": partitions, "oracles": oracles,
        "loaderContract": {
            "preferred": "TableSession insertTablet, bounded batches",
            "recommendedBatchRows": 4096, "batchMustNotCrossDeviceCell": True,
            "flushEachPartition": True, "rejectExistingTableData": True,
            "requiredReceipt": [
                "actual deployment endpoints and fixture manifest SHA",
                "each input file compressed SHA, raw SHA and parsed row count",
                "each tablet RPC success and exact partition/device/batch row range",
                "each partition FLUSH success",
                "actual source DataRegion/DN file path,size,SHA roster after FLUSH",
            ],
        },
        "placementStatus": "NOT_LOADED_NOT_OBSERVED",
        "headroomStatus": "NOT_OBSERVED_REQUIRE_EACH_SOURCE_REGION_TSFILE_BYTES_LT_134217728",
    }
    (args.output / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({
        "manifest": str((args.output / "manifest.json").resolve()),
        "fixtureVersion": version, "rows": config["totalRows"],
        "manifestSha256": sha256_file(args.output / "manifest.json"),
    }, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("legacy", "runtime-sql-v1"), default="legacy")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--devices", type=int)
    parser.add_argument("--rows-per-device", type=int, default=250_000)
    parser.add_argument("--groups", type=int)
    parser.add_argument("--runtime-preset", choices=("standard", "scaled"), default="standard")
    parser.add_argument("--runtime-base-rows", type=int)
    parser.add_argument("--runtime-heavy-rows", type=int)
    parser.add_argument("--time-partitions", type=int, default=4)
    parser.add_argument("--time-partition-interval-ms", type=int, default=604_800_000)
    parser.add_argument("--database", default="runtime_dop_e2e")
    parser.add_argument("--table", default="telemetry")
    parser.add_argument("--plan-only", action="store_true")
    args = parser.parse_args()
    try:
        if args.mode == "runtime-sql-v1":
            return runtime_fixture(args)
        if args.plan_only:
            parser.error("--plan-only is a runtime-sql-v1 option")
        args.devices = args.devices if args.devices is not None else 4
        args.groups = args.groups if args.groups is not None else 25_000
        return legacy_fixture(args)
    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    raise SystemExit(main())
