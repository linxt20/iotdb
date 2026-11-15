#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.  You may obtain a copy of the
# License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.  See the License for the specific language governing permissions and limitations
# under the License.

"""Read-only acceptance for the experimental multi-source N x P hash GROUP BY path.

This program never creates data, changes a configuration, changes DOP, or starts/stops an IoTDB
process.  An operator must prepare two explicitly named, isolated deployments beforehand: an
enabled candidate and a hash-disabled control.  It archives the commands (with passwords
redacted), raw CLI output, copied configuration, canonical result CSVs, and an acceptance report.

The enabled EXPLAIN records the planning query's source/bucket IDs. EXPLAIN ANALYZE
is a separate query: its own fragment/operator IDs and hash sink downstream edges prove
that every source sends one channel to each unique bucket owner. Candidate and control
must return the same complete result multiset; this program makes no performance claim.

The --offline-typed-compare entry compares already drained CLI-v2 typed JSONL outputs
against each other and a frozen runtime fixture oracle. It validates the declared types,
NULLs and original result receipts, uses GNU sort with a fixed buffer and one thread,
then compares every row's full multiplicity. Optional --oracle-limit N selects only
an ordered oracle's deterministic first N rows after verifying the entire oracle.
The CLI result receipts must still describe completely drained SQL results.
--ordered-key-columns time explicitly accepts arbitrary order inside equal integral
ASC keys: it checks each original stream for monotonic keys and compares every tie
as a full typed multiset through bounded external sorting. Without this option,
ordered comparison retains the exact-sequence contract. Tie-aware LIMIT is rejected.
This entry never connects to IoTDB.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
from collections import Counter
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
TABLE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\Z")
TOPOLOGY = re.compile(
    r"sources\s*=\s*(?P<sources>\d+)\s+partitions\s*=\s*(?P<buckets>\d+)"
    r"\s*\(N\s*x\s*P experimental path\)",
    re.IGNORECASE,
)
TOPOLOGY_IDS = re.compile(
    r"hash-topology\s+sourceIds=\[(?P<sources>[^\]]*)\]"
    r"\s+bucketIds=\[(?P<buckets>[^\]]*)\]"
    r"\s+exchangeIds=(?P<exchanges>\[\[.*\]\])\s*\|?\s*$",
    re.IGNORECASE | re.MULTILINE,
)
FRAGMENT_COUNT = re.compile(r"Fragment Instances Count:\s*(?P<count>\d+)", re.IGNORECASE)
FRAGMENT_INSTANCE = re.compile(
    r"FRAGMENT-INSTANCE\[Id:\s*(?P<id>[^\]]+)\]\[IP:\s*(?P<ip>[^\]]+)\]",
    re.IGNORECASE,
)
OPERATOR = re.compile(
    r"\[PlanNodeId\s+(?P<id>[^\]]+)\]:\s*(?P<type>[A-Za-z0-9_]+)\(",
    re.IGNORECASE,
)
DATANODE_ID_HEADERS = {"nodeid", "node_id", "datanodeid", "data_node_id"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def command_output(command: list[str]) -> dict[str, Any]:
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    return {
        "command": command,
        "exit_code": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def find_git_root(start: Path) -> Path | None:
    for candidate in (start.resolve(), *start.resolve().parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def property_values(path: Path) -> dict[str, str]:
    if not path.is_file():
        raise ValueError(f"configuration file does not exist: {path}")
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def require_config_state(path: Path, enabled: bool) -> dict[str, str]:
    values = property_values(path)
    expected = "true" if enabled else "false"
    for key in ("enable_property_driven_planning", "enable_table_group_by_hash_repartition"):
        if values.get(key, "").lower() != expected:
            raise ValueError(
                f"{path} must set {key}={expected} for the selected {'candidate' if enabled else 'control'}"
            )
    if enabled:
        try:
            partition_count = int(values.get("table_group_by_hash_repartition_partition_count", "0"))
        except ValueError as error:
            raise ValueError(f"{path} has a non-integer hash partition count") from error
        if partition_count < 2:
            raise ValueError(f"{path} must configure at least two hash partitions")
    return values


def capture_context(output: Path, enabled_config: Path, baseline_config: Path) -> None:
    environment = output / "environment"
    configs = environment / "config"
    configs.mkdir(parents=True)
    copied: list[dict[str, str]] = []
    for role, config in (("enabled", enabled_config), ("baseline", baseline_config)):
        destination = configs / f"{role}-{config.name}"
        shutil.copy2(config, destination)
        copied.append({"role": role, "source": str(config), "copy": str(destination), "sha256": sha256(destination)})
    payload: dict[str, Any] = {
        "captured_at_utc": utc_now(),
        "platform": platform.platform(),
        "configs": copied,
    }
    git_root = find_git_root(ROOT)
    if git_root:
        payload["git"] = {
            "root": str(git_root),
            "head": command_output(["git", "-C", str(git_root), "rev-parse", "HEAD"]),
            "status": command_output(["git", "-C", str(git_root), "status", "--short"]),
        }
    (environment / "environment.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def endpoint(args: argparse.Namespace, name: str) -> tuple[Path, str, int]:
    cli = Path(getattr(args, f"{name}_cli"))
    if not cli.is_file():
        raise ValueError(f"{name} CLI executable does not exist: {cli}")
    return cli, getattr(args, f"{name}_host"), getattr(args, f"{name}_port")


def run_cli(
    args: argparse.Namespace, name: str, sql_file: Path, raw_dir: Path, password: str
) -> Path:
    cli, host, port = endpoint(args, name)
    statement = " ".join(sql_file.read_text(encoding="utf-8").splitlines())
    command = [
        str(cli), "-h", host, "-p", str(port), "-u", args.username, "-pw", password,
        "-sql_dialect", "table", "-e", statement,
    ]
    completed = subprocess.run(command, text=True, capture_output=True, check=False)
    label = f"{name}-{sql_file.stem}"
    stdout = raw_dir / f"{label}.stdout"
    stderr = raw_dir / f"{label}.stderr"
    stdout.write_text(completed.stdout, encoding="utf-8")
    stderr.write_text(completed.stderr, encoding="utf-8")
    redacted = command.copy()
    redacted[redacted.index("-pw") + 1] = "<redacted>"
    (raw_dir / f"{label}.command.json").write_text(
        json.dumps({"command": redacted, "exit_code": completed.returncode}, indent=2) + "\n",
        encoding="utf-8",
    )
    if completed.returncode:
        raise RuntimeError(f"CLI failed for {label}; inspect {stderr}")
    return stdout


def table_rows(path: Path) -> tuple[tuple[str, ...], list[tuple[str, ...]]]:
    rows: list[tuple[str, ...]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        clean = ANSI_ESCAPE.sub("", line).strip()
        if clean.startswith("|") and clean.endswith("|"):
            rows.append(tuple(cell.strip() for cell in clean[1:-1].split("|")))
    text = ANSI_ESCAPE.sub("", path.read_text(encoding="utf-8"))
    completed = re.search(r"^\s*(?:Total line number = (\d+)|Empty set\.)\s*$", text, re.MULTILINE)
    if not rows or completed is None:
        raise RuntimeError(f"CLI output lacks a result header or completion footer: {path}")
    reported_rows = int(completed.group(1) or "0")
    if reported_rows != len(rows) - 1:
        raise RuntimeError(f"CLI output result count disagrees with its completion footer: {path}")
    width = len(rows[0])
    if not width or any(len(row) != width for row in rows):
        raise RuntimeError(f"CLI output has an inconsistent table shape: {path}")
    return rows[0], rows[1:]


def write_canonical_csv(path: Path, table: tuple[tuple[str, ...], list[tuple[str, ...]]]) -> None:
    header, rows = table
    with path.open("w", newline="", encoding="utf-8") as target:
        writer = csv.writer(target)
        writer.writerow(header)
        writer.writerows(sorted(rows))


def digest(rows: list[tuple[str, ...]]) -> str:
    result = hashlib.sha256()
    for row in sorted(rows):
        result.update("\x1f".join(row).encode("utf-8"))
        result.update(b"\n")
    return result.hexdigest()


def compare_results(
    baseline: tuple[tuple[str, ...], list[tuple[str, ...]]],
    candidate: tuple[tuple[str, ...], list[tuple[str, ...]]],
    minimum_rows: int,
) -> dict[str, Any]:
    baseline_header, baseline_rows = baseline
    candidate_header, candidate_rows = candidate
    return {
        "same_header": baseline_header == candidate_header,
        "same_multiset": Counter(baseline_rows) == Counter(candidate_rows),
        "baseline_rows": len(baseline_rows),
        "candidate_rows": len(candidate_rows),
        "minimum_required_rows": minimum_rows,
        "baseline_multiset_sha256": digest(baseline_rows),
        "candidate_multiset_sha256": digest(candidate_rows),
        "matched": (
            baseline_header == candidate_header
            and Counter(baseline_rows) == Counter(candidate_rows)
            and len(candidate_rows) >= minimum_rows
        ),
    }


TYPED_ORACLE_NAMES = {
    "grouped": ["g", "n", "n2", "sum1", "sum2", "lo", "hi"],
    "filterProjectGrouped": ["k", "n", "total"],
    "ordered": ["time", "device_id", "g", "s1", "s2"],
    "fullScanWindowFiltered": ["time", "device_id", "g", "s1", "s2"],
    "runtimeEarlyLimitConstant": ["c"],
    "rawFiltered": ["time", "device_id", "g", "s1", "s2"],
}
TYPED_ENCODING = "typed-json-array-lf"
TYPED_MAX_LINE_BYTES = 1024 * 1024


def typed_file_sha256(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def typed_canonical(row: list[Any]) -> bytes:
    return (json.dumps(row, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")


def typed_row(line: bytes, column_types: list[str], label: str) -> list[Any]:
    if not line.endswith(b"\n") or line.endswith(b"\r\n"):
        raise ValueError(f"{label}: expected one UTF-8 JSON array with LF per row")
    row = json.loads(line.decode("utf-8"), parse_constant=lambda value: (_ for _ in ()).throw(ValueError(f"nonfinite JSON {value}")))
    if not isinstance(row, list) or len(row) != len(column_types):
        raise ValueError(f"{label}: row width differs from typed column schema")
    for index, (value, kind) in enumerate(zip(row, column_types)):
        if value is None:
            continue
        if kind in ("INT32", "INT64", "TIMESTAMP"):
            bits = 32 if kind == "INT32" else 64
            if type(value) is not int or not -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
                raise ValueError(f"{label}: column {index + 1} is not a lossless {kind} JSON integer")
        elif kind == "DOUBLE":
            # CLI-v2 emits native finite binary64 numbers. Reject JSON integers,
            # strings, booleans and nonfinite values; never coerce or use a tolerance.
            if type(value) is not float or not math.isfinite(value):
                raise ValueError(f"{label}: column {index + 1} is not a finite DOUBLE JSON number")
        elif kind in ("STRING", "TEXT"):
            if type(value) is not str:
                raise ValueError(f"{label}: column {index + 1} is not a JSON string")
        elif kind == "BOOLEAN":
            if type(value) is not bool:
                raise ValueError(f"{label}: column {index + 1} is not a JSON boolean")
        else:
            raise ValueError(f"{label}: unsupported offline fixture type {kind}")
    return row


def typed_sorted_stream(
    lines: Any, destination: Path, column_types: list[str], sort_exe: str,
    memory_mib: int, temporary: Path, ordered_key_indices: list[int] | None = None,
    key_prefix_indices: list[int] | None = None,
) -> dict[str, Any]:
    command = [
        sort_exe, "--buffer-size=" + str(memory_mib) + "M", "--parallel=1",
        "--temporary-directory=" + str(temporary), "--output=" + str(destination),
    ]
    if key_prefix_indices is not None:
        command.extend(["--field-separator=\t", *[
            f"--key={i + 1},{i + 1}n" for i in range(len(key_prefix_indices))
        ], f"--key={len(key_prefix_indices) + 1}"])
    environment = os.environ.copy()
    environment["LC_ALL"] = "C"
    count = 0
    previous_key = None
    key_groups = 0
    current_tie_rows = 0
    maximum_tie_rows = 0
    raw_digest = hashlib.sha256()
    sequence_digest = hashlib.sha256()
    error_log = destination.with_suffix(destination.suffix + ".sort-stderr")
    with error_log.open("wb") as errors:
        process = subprocess.Popen(command, stdin=subprocess.PIPE, stderr=errors, env=environment)
        try:
            assert process.stdin is not None
            for line in lines:
                if len(line) > TYPED_MAX_LINE_BYTES:
                    raise ValueError("Typed JSON row exceeds the bounded 1MiB line limit")
                row = typed_row(line, column_types, str(destination))
                canonical = typed_canonical(row)
                if ordered_key_indices is not None:
                    key = tuple(row[index] for index in ordered_key_indices)
                    if any(value is None for value in key):
                        raise ValueError("Tie-aware ASC integral keys must be non-NULL")
                    if previous_key is not None and key < previous_key:
                        raise ValueError("Original result stream has descending ordered keys")
                    if key != previous_key:
                        key_groups += 1
                        current_tie_rows = 0
                    current_tie_rows += 1
                    maximum_tie_rows = max(maximum_tie_rows, current_tie_rows)
                    previous_key = key
                record = canonical
                if key_prefix_indices is not None:
                    key = [row[index] for index in key_prefix_indices]
                    if any(type(value) is not int for value in key):
                        raise ValueError("External tie sort requires non-NULL integral keys")
                    record = ("\t".join(str(value) for value in key) + "\t").encode("ascii") + canonical
                process.stdin.write(record)
                raw_digest.update(line)
                sequence_digest.update(canonical)
                count += 1
            process.stdin.close()
            if process.wait() != 0:
                raise RuntimeError(f"GNU sort failed; inspect {error_log}")
        except BaseException:
            if process.stdin is not None and not process.stdin.closed:
                try:
                    process.stdin.close()
                except OSError:
                    pass
            if process.poll() is None:
                process.terminate()
            process.wait()
            raise
    return {
        "rows": count, "rawSha256": raw_digest.hexdigest(),
        "canonicalSequenceSha256": sequence_digest.hexdigest(),
        "sortedPath": str(destination), "sortCommand": command,
        "sortStderr": str(error_log),
        **({"orderedKeyValidation": {
            "direction": "ASC", "nondecreasing": True, "nullPolicy": "REJECT",
            "rowsChecked": count, "keyGroups": key_groups, "maximumTieRows": maximum_tie_rows,
        }} if ordered_key_indices is not None else {}),
    }


def typed_jsonl_lines(path: Path) -> Any:
    with path.open("rb") as stream:
        while True:
            line = stream.readline(TYPED_MAX_LINE_BYTES + 1)
            if not line:
                break
            yield line


def typed_expected_schema(manifest: dict[str, Any], oracle_name: str) -> tuple[list[str], list[str]]:
    oracle = manifest["oracles"][oracle_name]
    column_types = oracle["columnTypes"]
    names = oracle.get("columnNames", TYPED_ORACLE_NAMES[oracle_name])
    if (not isinstance(column_types, list) or not column_types
            or not isinstance(names, list) or len(names) != len(column_types)
            or any(not isinstance(value, str) or not value for value in names + column_types)):
        raise ValueError("Fixture oracle must provide a nonempty typed positional column schema")
    if any(kind not in ("INT32", "INT64", "TIMESTAMP", "DOUBLE", "STRING", "TEXT", "BOOLEAN") for kind in column_types):
        raise ValueError("Unsupported fixture type; no implicit coercion is permitted")
    if oracle.get("canonicalEncoding") != TYPED_ENCODING:
        raise ValueError("Unsupported fixture canonical encoding")
    return names, column_types


def typed_actual_result(
    directory: Path, role: str, manifest: dict[str, Any], oracle_name: str,
    destination: Path, sort_exe: str, memory_mib: int, temporary: Path,
    ordered_key_indices: list[int] | None = None,
) -> dict[str, Any]:
    schema_path = directory / "result.schema.json"
    receipt_path = directory / "result-receipt.json"
    data_path = directory / "result.rows.jsonl"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    if schema.get("encoding") != TYPED_ENCODING:
        raise ValueError(f"{role}: driver schema encoding is not {TYPED_ENCODING}")
    if receipt.get("status") != "FULL_TYPED_RESULT_DRAINED" or receipt.get("sequencePreserved") is not True:
        raise ValueError(f"{role}: require an explicitly complete ordinary-SQL result receipt")
    sql_sha = schema.get("sqlSha256")
    if not isinstance(sql_sha, str) or not re.fullmatch("[0-9a-f]{64}", sql_sha) or receipt.get("sqlSha256") != sql_sha:
        raise ValueError(f"{role}: result schema and receipt SQL SHA do not agree")
    names, column_types = typed_expected_schema(manifest, oracle_name)
    columns = schema.get("columns")
    if not isinstance(columns, list) or len(columns) != len(column_types):
        raise ValueError(f"{role}: result schema width differs from fixture oracle")
    for index, (column, name, kind) in enumerate(zip(columns, names, column_types)):
        if (not isinstance(column, dict) or type(column.get("index")) is not int
                or column["index"] != index + 1 or column.get("type") != kind
                or str(column.get("label", "")).casefold() != name.casefold()):
            raise ValueError(f"{role}: positional name/type mismatch at column {index + 1}")
    if "TIMESTAMP" in column_types and schema.get("timestampRepresentation") != "server-millisecond-integer":
        raise ValueError(f"{role}: TIMESTAMP must be the verified server millisecond integer")
    result = typed_sorted_stream(
        typed_jsonl_lines(data_path), destination, column_types, sort_exe, memory_mib, temporary,
        ordered_key_indices=ordered_key_indices)
    if type(receipt.get("rows")) is not int or receipt["rows"] != result["rows"]:
        raise ValueError(f"{role}: full typed row count disagrees with driver receipt")
    if receipt.get("sha256") != result["rawSha256"]:
        raise ValueError(f"{role}: full JSONL byte SHA disagrees with driver receipt")
    return {
        **result, "directory": str(directory.resolve()), "sqlSha256": sql_sha,
        "schema": {"path": str(schema_path.resolve()), "sha256": typed_file_sha256(schema_path)},
        "receipt": {"path": str(receipt_path.resolve()), "sha256": typed_file_sha256(receipt_path)},
        "input": {"path": str(data_path.resolve()), "sha256": result["rawSha256"]},
    }


def typed_raw_oracle_lines(manifest: dict[str, Any]) -> Any:
    expected_definition = {
        "device_id": "d + zero-padded decimal device index, width=3",
        "time": "partitionIndex*timePartitionIntervalMs + rowIndex",
        "g": "(deviceIndex*17+rowIndex)%257",
        "s1": "((deviceIndex*1009+rowIndex)%2001)-1000",
        "s2": "NULL iff (deviceIndex+partitionIndex+rowIndex)%17==0, otherwise (rowIndex%127)-63",
    }
    if (not manifest["fixtureVersion"].startswith("runtime_sql_fixture_v1")
            or manifest.get("rowDefinition") != expected_definition
            or manifest.get("groups") != 257):
        raise ValueError("Raw oracle reconstruction requires the exact runtime_sql_fixture_v1 row definition")
    devices = manifest["devices"]
    partitions = manifest["timePartitions"]
    base = manifest["baseRowsPerDevicePartition"]
    interval = manifest["timePartitionIntervalMs"]
    heavy = manifest["heavyCell"]
    dimensions = [devices, partitions, base, interval, heavy["rows"]]
    if (any(type(v) is not int or v < 1 for v in dimensions) or devices > 1000
            or heavy["deviceIndex"] != devices - 1 or heavy["partitionIndex"] != partitions - 1
            or not base <= heavy["rows"] <= interval
            or manifest["totalRows"] != devices * partitions * base + heavy["rows"] - base):
        raise ValueError("Raw oracle fixture geometry is invalid")
    for device in range(devices):
        for partition in range(partitions):
            count = heavy["rows"] if (device, partition) == (heavy["deviceIndex"], heavy["partitionIndex"]) else base
            for row in range(count):
                value = (device * 1009 + row) % 2001 - 1000
                signed = value * 17
                remainder = (abs(signed) % 97) * (1 if signed >= 0 else -1)
                if remainder >= 0:
                    yield typed_canonical([
                        partition * interval + row, f"d{device:03d}", (device * 17 + row) % 257,
                        value, None if (device + partition + row) % 17 == 0 else row % 127 - 63,
                    ])


def typed_oracle_result(
    manifest_path: Path, manifest: dict[str, Any], oracle_name: str, destination: Path,
    sort_exe: str, memory_mib: int, temporary: Path, oracle_limit: int | None = None,
) -> dict[str, Any]:
    oracle = manifest["oracles"][oracle_name]
    _, column_types = typed_expected_schema(manifest, oracle_name)
    source = None
    if oracle.get("materialized") is True:
        source = (manifest_path.parent / oracle["path"]).resolve()
        try:
            source.relative_to(manifest_path.parent.resolve())
        except ValueError as error:
            raise ValueError("Oracle path escapes the fixture manifest directory") from error
        lines = typed_jsonl_lines(source)
    elif oracle_name == "rawFiltered" and oracle.get("materialized") is False:
        if oracle.get("orderBy") != ["device_id", "time"]:
            raise ValueError("Raw oracle must declare canonical device/time order")
        lines = typed_raw_oracle_lines(manifest)
    else:
        raise ValueError("Unsupported nonmaterialized fixture oracle")
    full_destination = destination
    verified_sequence = None
    if oracle_limit is not None:
        if oracle_name != "ordered" or type(oracle_limit) is not int or oracle_limit <= 0:
            raise ValueError("--oracle-limit requires --oracle ordered and a strictly positive integer")
        if source is None:
            raise ValueError("Ordered LIMIT comparison requires a materialized full oracle")
        # Keep the exact full sequence whose SHA and row count are checked below. Selecting from
        # this private snapshot prevents a later fixture mutation from changing the chosen prefix.
        verified_sequence = destination.with_name("oracle.full.sequence.jsonl")
        with source.open("rb") as original, verified_sequence.open("xb") as snapshot:
            shutil.copyfileobj(original, snapshot, length=1024 * 1024)
        lines = typed_jsonl_lines(verified_sequence)
        full_destination = destination.with_name("oracle.full.sorted.jsonl")
    result = typed_sorted_stream(lines, full_destination, column_types, sort_exe, memory_mib, temporary)
    if result["rows"] != oracle["rows"] or result["rawSha256"] != oracle["sha256"]:
        raise ValueError("Full independent oracle rows/SHA differ from the immutable fixture manifest")
    full_result = {**result, "source": str(source) if source else "manifest-rowDefinition device/time stream",
                   "declaredRows": oracle["rows"], "declaredSha256": oracle["sha256"]}
    if oracle_limit is None:
        return full_result
    if oracle_limit > result["rows"]:
        raise ValueError("--oracle-limit exceeds the actual verified full oracle row count")
    assert verified_sequence is not None
    selected = typed_sorted_stream(
        islice(typed_jsonl_lines(verified_sequence), oracle_limit), destination,
        column_types, sort_exe, memory_mib, temporary)
    if selected["rows"] != oracle_limit:
        raise RuntimeError("Verified oracle snapshot did not provide the complete selected LIMIT result")
    return {**selected, "source": str(verified_sequence),
            "selection": {"kind": "DETERMINISTIC_ORDERED_SQL_LIMIT", "limit": oracle_limit,
                          "firstRowIndexInclusive": 0, "endRowIndexExclusive": oracle_limit},
            "fullOracleVerification": full_result}


def typed_multiset_comparison(
    paths: dict[str, Path], column_types: list[str], sample_limit: int = 20,
) -> dict[str, Any]:
    prefix = b"iotdb-typed-multiset-v1\0" + typed_canonical(column_types)
    states = {role: {"rows": 0, "distinctRows": 0, "hash": hashlib.sha256(prefix)} for role in paths}
    def counts(role: str, path: Path) -> Any:
        previous = None
        count = 0
        with path.open("rb") as stream:
            for line in stream:
                if previous is not None and line < previous:
                    raise RuntimeError("External sort output is not in bytewise C order")
                states[role]["hash"].update(line)
                states[role]["rows"] += 1
                if line == previous:
                    count += 1
                else:
                    if previous is not None:
                        states[role]["distinctRows"] += 1
                        yield previous, count
                    previous, count = line, 1
            if previous is not None:
                states[role]["distinctRows"] += 1
                yield previous, count
    iterators = {role: iter(counts(role, path)) for role, path in paths.items()}
    current = {role: next(iterator, None) for role, iterator in iterators.items()}
    differences = {"candidateVsControl": 0, "candidateVsOracle": 0, "controlVsOracle": 0}
    samples = []
    while any(value is not None for value in current.values()):
        key = min(value[0] for value in current.values() if value is not None)
        multiplicities = {role: value[1] if value is not None and value[0] == key else 0
                          for role, value in current.items()}
        mismatch = False
        for name, left, right in [
            ("candidateVsControl", "candidate", "control"),
            ("candidateVsOracle", "candidate", "oracle"),
            ("controlVsOracle", "control", "oracle"),
        ]:
            if multiplicities[left] != multiplicities[right]:
                differences[name] += 1
                mismatch = True
        if mismatch and len(samples) < sample_limit:
            samples.append({"row": json.loads(key), "multiplicities": multiplicities})
        for role, value in current.items():
            if value is not None and value[0] == key:
                current[role] = next(iterators[role], None)
    return {
        "algorithm": "Full bytewise merge of every typed canonical row and its exact occurrence count; SHA is supplementary.",
        "hashEncoding": "SHA256('iotdb-typed-multiset-v1\\0' + compact JSON columnTypes + LF + all bytewise-sorted canonical rows with duplicates)",
        "roles": {role: {"rows": state["rows"], "distinctRows": state["distinctRows"],
                          "typedMultisetSha256": state["hash"].hexdigest()} for role, state in states.items()},
        "comparisons": {name: {"sameMultiset": count == 0, "differingDistinctRows": count}
                        for name, count in differences.items()},
        "differenceSamples": samples, "differenceSampleLimit": sample_limit,
        "allTypedMultisetsEqual": all(count == 0 for count in differences.values()),
    }



def typed_ordered_tie_comparison(
    paths: dict[str, Path], column_types: list[str], key_indices: list[int],
    key_names: list[str], destination: Path, sort_exe: str, memory_mib: int,
    temporary: Path, sample_limit: int = 20,
) -> dict[str, Any]:
    # Inputs are private, completely validated canonical snapshots. Prefix each
    # row with exact integral keys; GNU sort -n compares them without float conversion.
    # Neither this producer nor the merge retains the contents of a tie group.
    destination.mkdir()
    sorted_inputs = {}
    for role, path in paths.items():
        sorted_inputs[role] = typed_sorted_stream(
            typed_jsonl_lines(path), destination / (role + ".key-sorted.tsv"),
            column_types, sort_exe, memory_mib, temporary, key_prefix_indices=key_indices)
    prefix = b"iotdb-typed-ordered-ties-v1\0" + typed_canonical([key_names, column_types])
    states = {role: {"rows": 0, "distinctRows": 0, "hash": hashlib.sha256(prefix)} for role in paths}
    def counts(role: str) -> Any:
        previous = None
        count = 0
        with Path(sorted_inputs[role]["sortedPath"]).open("rb") as stream:
            for line in stream:
                parts = line.split(b"\t", len(key_indices))
                if len(parts) != len(key_indices) + 1:
                    raise RuntimeError("Malformed private key-prefixed sort record")
                row = typed_row(parts[-1], column_types, role + " tie sort")
                key = tuple(row[index] for index in key_indices)
                if parts[:-1] != [str(value).encode("ascii") for value in key]:
                    raise RuntimeError("External tie sort prefix disagrees with typed row")
                canonical = typed_canonical(row)
                if canonical != parts[-1]:
                    raise RuntimeError("External tie sort changed canonical row bytes")
                current = (key, canonical)
                if previous is not None and current < previous:
                    raise RuntimeError("External tie sort violates numeric-key/bytewise-row order")
                states[role]["hash"].update(canonical)
                states[role]["rows"] += 1
                if current == previous:
                    count += 1
                else:
                    if previous is not None:
                        states[role]["distinctRows"] += 1
                        yield previous, count
                    previous, count = current, 1
            if previous is not None:
                states[role]["distinctRows"] += 1
                yield previous, count
    iterators = {role: iter(counts(role)) for role in paths}
    current = {role: next(iterator, None) for role, iterator in iterators.items()}
    group_key = None
    group_counts = {role: 0 for role in paths}
    group_differences = 0
    groups = 0
    mismatching_groups = 0
    differing_rows = 0
    samples = []
    group_path = destination / "per-key-exact-multisets.jsonl"
    group_digest = hashlib.sha256()
    with group_path.open("xb") as group_stream:
        def flush_group() -> None:
            nonlocal groups, mismatching_groups
            if group_key is None:
                return
            groups += 1
            mismatching_groups += int(group_differences > 0)
            record = typed_canonical([list(group_key), group_counts, group_differences])
            group_stream.write(record)
            group_digest.update(record)
        while any(value is not None for value in current.values()):
            key_row = min(value[0] for value in current.values() if value is not None)
            key, row_bytes = key_row
            if key != group_key:
                flush_group()
                group_key = key
                group_counts = {role: 0 for role in paths}
                group_differences = 0
            multiplicities = {
                role: value[1] if value is not None and value[0] == key_row else 0
                for role, value in current.items()
            }
            for role, count in multiplicities.items():
                group_counts[role] += count
            if len(set(multiplicities.values())) != 1:
                group_differences += 1
                differing_rows += 1
                if len(samples) < sample_limit:
                    samples.append({"key": list(key), "row": json.loads(row_bytes),
                                    "multiplicities": multiplicities})
            for role, value in current.items():
                if value is not None and value[0] == key_row:
                    current[role] = next(iterators[role], None)
        flush_group()
    return {
        "mode": "ASC_INTEGRAL_KEYS_EXACT_TYPED_TIE_MULTISETS",
        "keyColumns": key_names, "keyColumnIndicesOneBased": [index + 1 for index in key_indices],
        "algorithm": "Fixed-buffer GNU numeric-key sort followed by a complete streaming merge of every key/typed row occurrence count.",
        "boundedMemory": {"sortBufferMiBPerProcess": memory_mib, "sortParallelism": 1,
                          "retainedCompleteTieGroups": 0, "mergeRowCursorsPerRole": 2,
                          "retainedDifferenceSamples": sample_limit,
                          "canonicalRowLineLimitBytes": TYPED_MAX_LINE_BYTES},
        "keyGroups": groups, "mismatchingKeyGroups": mismatching_groups,
        "differingDistinctKeyRows": differing_rows, "differenceSamples": samples,
        "allKeyGroupsMatchOracle": mismatching_groups == 0,
        "roles": {role: {"rows": state["rows"], "distinctKeyRows": state["distinctRows"],
                          "canonicalOrderedTieSha256": state["hash"].hexdigest()}
                  for role, state in states.items()},
        "hashEncoding": "SHA256('iotdb-typed-ordered-ties-v1\\0' + compact JSON [keyColumns,columnTypes] + LF + every canonical row in ascending numeric-key then bytewise-row order, preserving duplicates)",
        "perKeyComparison": {"path": str(group_path), "sha256": group_digest.hexdigest(),
                             "rows": groups, "encoding": "[keyValues,roleRowCounts,differingDistinctRows] JSONL"},
        "sortInputs": sorted_inputs,
    }


def offline_typed_compare(args: argparse.Namespace) -> dict[str, Any]:
    output = args.output
    if output.is_symlink() or (output.exists() and (not output.is_dir() or any(output.iterdir()))):
        raise ValueError("Offline comparison --output must be new or empty; never overwrite evidence")
    manifest_path = args.fixture_manifest.resolve()
    expected_sha = args.expected_fixture_sha256
    actual_sha = typed_file_sha256(manifest_path)
    if not re.fullmatch("[0-9a-f]{64}", expected_sha or "") or actual_sha != expected_sha:
        raise ValueError("Fixture manifest SHA does not match the explicitly selected immutable fixture")
    if not 1 <= args.sort_memory_mib <= 1024:
        raise ValueError("--sort-memory-mib must be within 1..1024")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schemaVersion") != 2 or not manifest.get("fixtureVersion", "").startswith("runtime_sql_fixture_v1"):
        raise ValueError("Offline comparison requires a version2 runtime_sql_fixture_v1 manifest")
    names, column_types = typed_expected_schema(manifest, args.oracle)
    oracle_limit = getattr(args, "oracle_limit", None)
    ordered_key_columns = getattr(args, "ordered_key_columns", None)
    ordered_key_indices = None
    if ordered_key_columns is not None:
        folded = [value.casefold() for value in ordered_key_columns]
        if (args.oracle != "ordered" or not folded or len(folded) != len(set(folded))
                or oracle_limit is not None):
            raise ValueError("--ordered-key-columns requires a full ordered oracle without LIMIT and unique keys")
        if len(set(name.casefold() for name in names)) != len(names):
            raise ValueError("Tie-aware comparison requires unambiguous positional column labels")
        try:
            ordered_key_indices = [[name.casefold() for name in names].index(value) for value in folded]
        except ValueError as error:
            raise ValueError("Ordered key column is absent from the positional schema") from error
        if any(column_types[index] not in ("INT32", "INT64", "TIMESTAMP") for index in ordered_key_indices):
            raise ValueError("Tie-aware comparison currently supports only non-NULL ASC integral keys")
        ordered_key_columns = [names[index] for index in ordered_key_indices]
    if oracle_limit is not None and (
        args.oracle != "ordered" or type(oracle_limit) is not int or oracle_limit <= 0
    ):
        raise ValueError("--oracle-limit requires --oracle ordered and a strictly positive integer")
    if args.repository_root is not None and not (args.repository_root / ".git").exists():
        raise ValueError("--repository-root must identify the selected Git working tree")
    sort_found = shutil.which(args.sort_executable)
    if not sort_found:
        raise ValueError("GNU sort executable not found")
    sort_exe = str(Path(sort_found).resolve())
    version = command_output([sort_exe, "--version"])
    if version["exit_code"] != 0 or "GNU coreutils" not in version["stdout"]:
        raise ValueError("Offline bounded comparison currently requires GNU coreutils sort")
    output.mkdir(parents=True, exist_ok=True)
    canonical_dir = output / "canonical"
    temporary = output / "sort-tmp"
    canonical_dir.mkdir()
    temporary.mkdir()
    identity = {
        "schemaVersion": 1, "startedAtUtc": utc_now(), "mode": "offline-typed-compare",
        "entry": {"path": str(Path(__file__).resolve()), "sha256": typed_file_sha256(Path(__file__).resolve())},
        "fixtureManifest": {"path": str(manifest_path), "sha256": actual_sha},
        "oracleSelection": {"name": args.oracle, "limit": oracle_limit, "orderedKeyColumns": ordered_key_columns},
        "sort": {"path": sort_exe, "sha256": typed_file_sha256(Path(sort_exe)), "version": version,
                 "locale": "C", "parallel": 1, "sortBufferMiB": args.sort_memory_mib},
        "python": {"path": sys.executable, "realpath": str(Path(sys.executable).resolve()),
                   "sha256": typed_file_sha256(Path(sys.executable).resolve()), "version": sys.version},
        "scope": "Offline result comparison only; no IoTDB process or dynamic action is started.",
    }
    git_root = args.repository_root or find_git_root(Path(__file__).resolve().parent)
    if git_root:
        identity["git"] = {"root": str(git_root),
                           "branch": command_output(["git", "-C", str(git_root), "branch", "--show-current"]),
                           "commit": command_output(["git", "-C", str(git_root), "rev-parse", "HEAD"])}
    (output / "execution-identity.json").write_text(json.dumps(identity, indent=2) + "\n", encoding="utf-8")
    try:
        results = {}
        for role, directory in [("candidate", args.candidate_result_dir), ("control", args.control_result_dir)]:
            results[role] = typed_actual_result(directory, role, manifest, args.oracle,
                canonical_dir / (role + ".sorted.jsonl"), sort_exe, args.sort_memory_mib, temporary,
                ordered_key_indices=ordered_key_indices)
        results["oracle"] = typed_oracle_result(manifest_path, manifest, args.oracle,
            canonical_dir / "oracle.sorted.jsonl", sort_exe, args.sort_memory_mib, temporary, oracle_limit)
        multiset = typed_multiset_comparison(
            {role: Path(result["sortedPath"]) for role, result in results.items()}, column_types)
        ordering_required = args.oracle == "ordered"
        sequence_checks = {
            role: results[role]["canonicalSequenceSha256"] == results["oracle"]["canonicalSequenceSha256"]
            for role in ("candidate", "control")
        }
        tie_comparison = None
        if ordered_key_indices is not None:
            tie_comparison = typed_ordered_tie_comparison(
                {role: Path(result["sortedPath"]) for role, result in results.items()},
                column_types, ordered_key_indices, ordered_key_columns,
                canonical_dir / "ordered-ties", sort_exe, args.sort_memory_mib, temporary)
        accepted = multiset["allTypedMultisetsEqual"] and (
            tie_comparison["allKeyGroupsMatchOracle"] if tie_comparison is not None
            else not ordering_required or all(sequence_checks.values()))
        report = {
            "schemaVersion": 1, "completedAtUtc": utc_now(), "mode": "offline-typed-compare",
            "claim": (
                "Complete ordered SQL LIMIT result equivalence: every selected row and multiplicity "
                "matches the deterministic first N rows of the independently verified full oracle; "
                "no runtime or performance conclusion."
                if oracle_limit is not None else
                "Complete typed result equivalence with nondecreasing original ASC integral keys and exact per-key tie multisets; no tie-internal ordering, runtime or performance conclusion."
                if tie_comparison is not None else
                "Complete typed result equivalence with multiplicities and independently checked fixture oracle; no runtime or performance conclusion."
            ),
            "oracleName": args.oracle, "oracleLimit": oracle_limit,
            "columnNames": names, "columnTypes": column_types,
            "fixtureManifest": identity["fixtureManifest"], "inputs": results,
            "multiset": multiset, "orderingRequired": ordering_required,
            "orderedSequenceMatchesOracle": sequence_checks if ordering_required and tie_comparison is None else None,
            "orderedKeyComparison": tie_comparison,
            "accepted": accepted,
        }
        (output / "typed-result-comparison.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return report
    except BaseException as error:
        (output / "typed-comparison-failure.json").write_text(
            json.dumps({"failedAtUtc": utc_now(), "type": type(error).__name__, "message": str(error),
                        "status": "FAILED_EVIDENCE_RETAINED"}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        raise


def _id_list(value: str, label: str) -> list[str]:
    values = [item.strip() for item in value.split(",") if item.strip()]
    if not values or len(values) != len(set(values)):
        raise RuntimeError(f"enabled plan {label} IDs must be non-empty and unique")
    return values


def _exchange_matrix(value: str) -> list[list[str]]:
    rows = re.findall(r"\[([^\[\]]*)\]", value)
    if not rows:
        raise RuntimeError("enabled plan exchange ID matrix is empty")
    matrix = [_id_list(row, "exchange") for row in rows]
    flattened = [item for row in matrix for item in row]
    if len(flattened) != len(set(flattened)):
        raise RuntimeError("enabled plan exchange IDs must be globally unique")
    return matrix


def _topology_ids(text: str, expected_sources: int, expected_buckets: int) -> dict[str, Any]:
    matches = list(TOPOLOGY_IDS.finditer(text))
    if len(matches) != 1:
        raise RuntimeError(
            f"enabled plan must contain exactly one explicit source/bucket ID matrix, found {len(matches)}"
        )
    match = matches[0]
    source_ids = _id_list(match.group("sources"), "source")
    bucket_ids = _id_list(match.group("buckets"), "bucket")
    exchange_matrix = _exchange_matrix(match.group("exchanges"))
    if len(source_ids) != expected_sources or len(bucket_ids) != expected_buckets:
        raise RuntimeError(
            f"enabled plan ID counts do not match requested topology: sources={len(source_ids)} "
            f"buckets={len(bucket_ids)} expected={expected_sources}x{expected_buckets}"
        )
    if len(exchange_matrix) != expected_sources or any(
        len(row) != expected_buckets for row in exchange_matrix
    ):
        raise RuntimeError("enabled plan exchange ID matrix does not match source x bucket dimensions")
    return {
        "source_ids": source_ids,
        "bucket_ids": bucket_ids,
        "exchange_ids": exchange_matrix,
        "exchange_edges": expected_sources * expected_buckets,
    }


def validate_enabled_plan(path: Path, expected_sources: int, expected_buckets: int) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    for marker in ("Property enforcement:", "TableHashPartitioningShuffleSinkNode", "TABLE_HASH_V1"):
        if marker not in text:
            raise RuntimeError(f"enabled plan is missing required marker {marker!r}: {path}")
    matches = list(TOPOLOGY.finditer(text))
    if not matches:
        raise RuntimeError(f"enabled plan has no reviewed N x P source/bucket trace: {path}")
    reported_sources = {int(match.group("sources")) for match in matches}
    reported_buckets = {int(match.group("buckets")) for match in matches}
    if reported_sources != {expected_sources} or reported_buckets != {expected_buckets}:
        raise RuntimeError(
            "enabled plan source/bucket trace does not match requested topology: "
            f"expected {expected_sources} x {expected_buckets}, saw sources={sorted(reported_sources)} "
            f"partitions={sorted(reported_buckets)}"
        )
    topology = _topology_ids(text, expected_sources, expected_buckets)
    return {
        **topology,
        "source_count": len(topology["source_ids"]),
        "bucket_count": len(topology["bucket_ids"]),
    }


def validate_datanodes(path: Path, expected_datanodes: int) -> dict[str, Any]:
    header, rows = table_rows(path)
    normalized = {name.lower().replace(" ", "_"): index for index, name in enumerate(header)}
    node_index = next((normalized[name] for name in DATANODE_ID_HEADERS if name in normalized), None)
    if node_index is None:
        raise RuntimeError(f"SHOW DATANODES output lacks a node ID column: {path}")
    values = [row[node_index] for row in rows]
    if any(not value.isdigit() for value in values) or len(values) != len(set(values)):
        raise RuntimeError(f"SHOW DATANODES must contain unique numeric node IDs: {path}")
    status_index = normalized.get("status")
    if status_index is None or any(row[status_index].lower() != "running" for row in rows):
        raise RuntimeError(f"SHOW DATANODES must report every node as Running: {path}")
    node_ids = set(values)
    if len(node_ids) != expected_datanodes:
        raise RuntimeError(
            f"SHOW DATANODES reports {len(node_ids)} unique DataNode IDs, expected {expected_datanodes}: {path}"
        )
    return {"datanode_count": len(node_ids), "datanode_ids": sorted(node_ids)}


def validate_enabled_runtime(
    path: Path, topology: dict[str, Any], expected_datanodes: int, minimum_fragment_instances: int
) -> dict[str, Any]:
    """Validate concrete fragment and operator IDs, without counting rendered class strings."""
    text = path.read_text(encoding="utf-8")
    instances = list(FRAGMENT_INSTANCE.finditer(text))
    instance_ids = [match.group("id").strip() for match in instances]
    if not instance_ids or len(instance_ids) != len(set(instance_ids)):
        raise RuntimeError(f"enabled runtime must contain unique FRAGMENT-INSTANCE IDs: {path}")
    count_match = FRAGMENT_COUNT.search(text)
    if count_match is None or int(count_match.group("count")) != len(instance_ids):
        raise RuntimeError(f"enabled runtime fragment instance count disagrees with concrete IDs: {path}")
    if len(instance_ids) < minimum_fragment_instances:
        raise RuntimeError(
            f"enabled runtime has {len(instance_ids)} fragment instances, below required minimum "
            f"{minimum_fragment_instances}"
        )
    runtime_endpoints = {match.group("ip").strip() for match in instances}
    if len(runtime_endpoints) < expected_datanodes:
        raise RuntimeError(
            f"enabled runtime exposes {len(runtime_endpoints)} unique DataNode endpoints, "
            f"expected at least {expected_datanodes}"
        )
    # Keep runtime IDs independent from the IDs of the separate EXPLAIN query.
    fragments: dict[str, dict[str, Any]] = {}
    owners: dict[str, str] = {}
    current_fragment = None
    current_operator = None
    for raw in text.splitlines():
        line = ANSI_ESCAPE.sub("", raw).strip().strip("|").strip()
        instance = FRAGMENT_INSTANCE.search(line)
        if instance:
            current_fragment = instance.group("id").strip()
            current_operator = None
            if "[State: FINISHED]" not in line:
                raise RuntimeError(f"runtime fragment did not finish: {current_fragment}")
            fragments[current_fragment] = {"endpoint": instance.group("ip").strip(), "operators": {}}
            continue
        operator = OPERATOR.search(line)
        if operator:
            if current_fragment is None:
                raise RuntimeError("runtime operator has no fragment owner")
            current_operator = operator.group("id").strip()
            if current_operator in owners:
                raise RuntimeError(f"runtime operator ID has duplicate ownership: {current_operator}")
            owners[current_operator] = current_fragment
            fragments[current_fragment]["operators"][current_operator] = {
                "type": operator.group("type"), "downstream": []
            }
        elif "DownStreamPlanNodeId:" in line:
            if current_fragment is None or current_operator is None:
                raise RuntimeError("downstream list has no operator owner")
            value = line.split("DownStreamPlanNodeId:", 1)[1].strip()
            entry = fragments[current_fragment]["operators"][current_operator]
            if entry["downstream"]:
                raise RuntimeError("operator has repeated downstream lists")
            entry["downstream"] = _id_list(value, "runtime downstream")

    sources = []
    for fragment_id, fragment in fragments.items():
        sinks = [(key, value) for key, value in fragment["operators"].items()
                 if value["type"] == "TableHashPartitioningShuffleSinkNode"]
        if len(sinks) > 1:
            raise RuntimeError("multiple hash sources share one fragment instance")
        if sinks:
            key, value = sinks[0]
            sources.append({"sink_id": key, "fragment_id": fragment_id,
                            "endpoint": fragment["endpoint"], "exchange_ids": value["downstream"]})
    source_count = len(topology["source_ids"])
    bucket_count = len(topology["bucket_ids"])
    if len(sources) != source_count:
        raise RuntimeError(f"runtime hash source count {len(sources)} differs from {source_count}")
    source_fragments = {source["fragment_id"] for source in sources}
    if len({source["endpoint"] for source in sources}) != expected_datanodes:
        raise RuntimeError("runtime hash sources do not span the requested DataNodes")
    edges = [edge for source in sources for edge in source["exchange_ids"]]
    if any(len(source["exchange_ids"]) != bucket_count for source in sources):
        raise RuntimeError("runtime source channel count differs from bucket count")
    if len(edges) != len(set(edges)):
        raise RuntimeError("runtime sources reuse an exchange edge")
    buckets = []
    for column in range(bucket_count):
        column_edges = [source["exchange_ids"][column] for source in sources]
        column_owners = set()
        for edge in column_edges:
            if edge not in owners:
                raise RuntimeError(f"runtime edge has no exchange owner: {edge}")
            owner = owners[edge]
            if fragments[owner]["operators"][edge]["type"] != "ExchangeNode":
                raise RuntimeError(f"runtime edge does not target an exchange: {edge}")
            column_owners.add(owner)
        if len(column_owners) != 1:
            raise RuntimeError("one runtime bucket column targets multiple fragment instances")
        owner = next(iter(column_owners))
        if owner in source_fragments or any(bucket["fragment_id"] == owner for bucket in buckets):
            raise RuntimeError("runtime buckets must have unique owners separate from sources")
        bucket_ops = fragments[owner]["operators"]
        aggregations = [key for key, value in bucket_ops.items() if value["type"] == "AggregationNode"]
        exchanges = {key for key, value in bucket_ops.items() if value["type"] == "ExchangeNode"}
        if len(aggregations) != 1 or not any(value["type"] == "CollectNode" for value in bucket_ops.values()):
            raise RuntimeError("runtime bucket lacks a single final aggregation and collect")
        if exchanges != set(column_edges):
            raise RuntimeError("runtime bucket exchanges differ from source channels")
        buckets.append({"bucket_index": column, "fragment_id": owner,
                        "aggregation_id": aggregations[0], "exchange_ids": column_edges})
    return {
        "fragment_instances": len(instance_ids),
        "fragment_instance_ids": sorted(instance_ids),
        "runtime_ip_values": sorted(runtime_endpoints),
        "sources": sources,
        "buckets": buckets,
        "operator_ids": sorted(owners),
        "required_exchange_edges": topology["exchange_edges"],
        "verified_exchange_edges": len(edges),
        "ownership_verified": True,
        "id_scope": "EXPLAIN ANALYZE query only; planning EXPLAIN IDs are archived separately",
    }



def validate_baseline_plan(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    forbidden = ("TableHashPartitioningShuffleSinkNode", "TABLE_HASH_V1", "N x P experimental path")
    present = [marker for marker in forbidden if marker in text]
    if present:
        raise RuntimeError(f"hash-disabled control unexpectedly exposed hash-path markers {present}: {path}")

def write_queries(query_dir: Path, table: str) -> dict[str, Path]:
    core = f"SELECT s1, COUNT(*) AS row_count FROM {table} GROUP BY s1"
    statements = {
        "enabled-explain": f"EXPLAIN {core};\n",
        "enabled-explain-analyze": f"EXPLAIN ANALYZE {core};\n",
        "baseline-explain-analyze": f"EXPLAIN ANALYZE {core};\n",
        "enabled-result": f"{core} ORDER BY s1;\n",
        "baseline-result": f"{core} ORDER BY s1;\n",
    }
    query_dir.mkdir(parents=True)
    files: dict[str, Path] = {}
    for name, statement in statements.items():
        destination = query_dir / f"{name}.sql"
        destination.write_text(statement, encoding="utf-8")
        files[name] = destination
    return files


def execute(args: argparse.Namespace) -> dict[str, Any]:
    if not TABLE_NAME.fullmatch(args.table):
        raise ValueError("--table must be a simple unquoted database.table identifier")
    if args.expected_sources < 2 or args.expected_buckets < 2:
        raise ValueError("--expected-sources and --expected-buckets must both be at least 2")
    if args.minimum_result_rows < 1:
        raise ValueError("--minimum-result-rows must be positive")
    enabled_endpoint = (args.enabled_host, args.enabled_port)
    baseline_endpoint = (args.baseline_host, args.baseline_port)
    if enabled_endpoint == baseline_endpoint:
        raise ValueError("candidate and control endpoints must differ; do not toggle a live deployment in this runner")
    if args.enabled_isolation_id == args.baseline_isolation_id:
        raise ValueError("candidate and control must have distinct explicit isolation identifiers")
    enabled_values = require_config_state(args.enabled_config, enabled=True)
    baseline_values = require_config_state(args.baseline_config, enabled=False)
    configured_buckets = int(enabled_values["table_group_by_hash_repartition_partition_count"])
    if configured_buckets != args.expected_buckets:
        raise ValueError(
            f"--expected-buckets={args.expected_buckets} does not match enabled configuration "
            f"partition count {configured_buckets}"
        )
    if baseline_values.get("table_group_by_hash_repartition_partition_count") not in (None, str(configured_buckets)):
        raise ValueError("control and candidate must use the same configured hash partition count")
    password = os.environ.get(args.password_env)
    if not password:
        raise ValueError(f"password environment variable is unset or empty: {args.password_env}")
    if args.output.exists() and any(args.output.iterdir()):
        raise ValueError("--output must be new or empty so evidence is never overwritten")

    args.output.mkdir(parents=True, exist_ok=True)
    raw_dir = args.output / "raw"
    canonical_dir = args.output / "canonical"
    raw_dir.mkdir()
    canonical_dir.mkdir()
    capture_context(args.output, args.enabled_config, args.baseline_config)
    queries = write_queries(args.output / "queries", args.table)
    datanode_sql = args.output / "queries" / "show-datanodes.sql"
    datanode_sql.write_text("SHOW DATANODES;\n", encoding="utf-8")

    enabled_explain = run_cli(args, "enabled", queries["enabled-explain"], raw_dir, password)
    explain_shape = validate_enabled_plan(enabled_explain, args.expected_sources, args.expected_buckets)
    enabled_datanodes = run_cli(args, "enabled", datanode_sql, raw_dir, password)
    datanode_shape = validate_datanodes(enabled_datanodes, args.expected_datanodes)
    enabled_analyze = run_cli(args, "enabled", queries["enabled-explain-analyze"], raw_dir, password)
    analyze_shape = validate_enabled_runtime(
        enabled_analyze,
        explain_shape,
        args.expected_datanodes,
        args.minimum_fragment_instances
        if args.minimum_fragment_instances is not None
        else args.expected_sources + args.expected_buckets + 1,
    )
    baseline_analyze = run_cli(args, "baseline", queries["baseline-explain-analyze"], raw_dir, password)
    validate_baseline_plan(baseline_analyze)

    enabled_result = table_rows(run_cli(args, "enabled", queries["enabled-result"], raw_dir, password))
    baseline_result = table_rows(run_cli(args, "baseline", queries["baseline-result"], raw_dir, password))
    write_canonical_csv(canonical_dir / "enabled-result.csv", enabled_result)
    write_canonical_csv(canonical_dir / "baseline-result.csv", baseline_result)
    comparison = compare_results(baseline_result, enabled_result, args.minimum_result_rows)
    report = {
        "completed_at_utc": utc_now(),
        "claim": "multi-source N x P GROUP BY hash topology correctness only; no performance conclusion",
        "table": args.table,
        "candidate": {"isolation_id": args.enabled_isolation_id, "endpoint": f"{args.enabled_host}:{args.enabled_port}"},
        "control": {"isolation_id": args.baseline_isolation_id, "endpoint": f"{args.baseline_host}:{args.baseline_port}"},
        "expected_topology": {"sources": args.expected_sources, "buckets": args.expected_buckets},
        "enabled_explain": explain_shape,
        "enabled_datanodes": datanode_shape,
        "enabled_explain_analyze": analyze_shape,
        "control_hash_path": "absent",
        "result_equivalence": comparison,
        "accepted": comparison["matched"],
    }
    (args.output / "multisource-hash-groupby-acceptance.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return report


def typed_self_test(root: Path) -> int:
    root.mkdir()
    checks = 0
    number = 0
    group_types = ["INT64"] * 7
    group_rows = [
        [1, 2, 0, 3, None, -1, 4],
        [2, 1, 1, 9007199254740993, 6, 5, 5],
        [1, 2, 0, 3, None, -1, 4],
    ]
    sql_sha = hashlib.sha256(b"SELF_TEST_SYNTHETIC_ROWS_NOT_SERVICE_EVIDENCE").hexdigest()
    def write_json(path: Path, value: Any) -> None:
        path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    def fixture(directory: Path, rows: Any, kind: str, column_types: list[str]) -> Path:
        directory.mkdir()
        oracle_path = directory / "oracle.jsonl"
        count = 0
        with oracle_path.open("wb") as stream:
            for row in rows:
                stream.write(typed_canonical(row))
                count += 1
        manifest = {
            "schemaVersion": 2, "fixtureVersion": "runtime_sql_fixture_v1-self-test",
            "oracles": {kind: {"materialized": True, "path": "oracle.jsonl",
                "rows": count, "sha256": typed_file_sha256(oracle_path),
                "columnTypes": column_types, "canonicalEncoding": TYPED_ENCODING}},
        }
        manifest_path = directory / "manifest.json"
        write_json(manifest_path, manifest)
        return manifest_path
    def actual(directory: Path, rows: Any, kind: str, column_types: list[str]) -> None:
        directory.mkdir()
        data = directory / "result.rows.jsonl"
        count = 0
        with data.open("wb") as stream:
            for row in rows:
                stream.write(typed_canonical(row))
                count += 1
        write_json(directory / "result.schema.json", {
            "encoding": TYPED_ENCODING, "timestampRepresentation": "server-millisecond-integer",
            "sqlSha256": sql_sha, "columns": [
                {"index": i + 1, "name": name, "label": name, "type": column_type}
                for i, (name, column_type) in enumerate(zip(TYPED_ORACLE_NAMES[kind], column_types))],
        })
        write_json(directory / "result-receipt.json", {
            "status": "FULL_TYPED_RESULT_DRAINED", "rows": count,
            "sha256": typed_file_sha256(data), "sequencePreserved": True, "sqlSha256": sql_sha,
        })
    def run(candidate: list[list[Any]], control: list[list[Any]], expected: list[list[Any]],
            kind: str = "grouped", column_types: list[str] | None = None, mutate: Any = None,
            should_reject: bool = False, oracle_limit: Any = None,
            ordered_key_columns: list[str] | None = None) -> dict[str, Any] | None:
        nonlocal number, checks
        number += 1
        directory = root / str(number)
        directory.mkdir()
        types = column_types or group_types
        manifest = fixture(directory / "fixture", expected, kind, types)
        actual(directory / "candidate", candidate, kind, types)
        actual(directory / "control", control, kind, types)
        if mutate:
            mutate(directory)
        args = argparse.Namespace(
            output=directory / "comparison", fixture_manifest=manifest,
            expected_fixture_sha256=typed_file_sha256(manifest), oracle=kind,
            candidate_result_dir=directory / "candidate", control_result_dir=directory / "control",
            sort_memory_mib=1, sort_executable="sort", repository_root=None,
            oracle_limit=oracle_limit, ordered_key_columns=ordered_key_columns,
        )
        try:
            result = offline_typed_compare(args)
        except (ValueError, RuntimeError):
            if not should_reject:
                raise
            checks += 1
            return None
        if should_reject:
            raise AssertionError("Invalid typed evidence was accepted")
        checks += 1
        return result
    def copied(rows: list[list[Any]]) -> list[list[Any]]:
        return json.loads(json.dumps(rows))
    assert run(list(reversed(group_rows)), group_rows, group_rows)["accepted"]
    projected = [[1, 2, 14], [2, 1, 9007199254740993], [1, 2, 14]]
    assert run(projected[::-1], projected, projected, "filterProjectGrouped", ["INT64"] * 3)["accepted"]
    # SUM(INT64) has native DOUBLE output; the positional schema and JSON scalar
    # type remain strict. These rows cover exact finite sums, NULL and duplicates.
    sum_types = ["INT64", "INT64", "INT64", "DOUBLE", "DOUBLE", "INT64", "INT64"]
    sum_rows = [[1, 2, 0, 3.0, None, -1, 4], [2, 1, 1, 9007199254740992.0, 6.0, 5, 5],
                [1, 2, 0, 3.0, None, -1, 4]]
    assert run(sum_rows[::-1], sum_rows, sum_rows, column_types=sum_types)["accepted"]
    projected_types = ["INT64", "INT64", "DOUBLE"]
    projected_sums = [[1, 2, 14.0], [2, 1, 9007199254740992.0], [1, 2, 14.0]]
    assert run(projected_sums[::-1], projected_sums, projected_sums,
               "filterProjectGrouped", projected_types)["accepted"]
    for value in (3, True, "3.0", float("inf"), float("-inf"), float("nan")):
        # Nonfinite JSON is rejected by parsing as well as by the native type check.
        changed_sum = copied(sum_rows)
        changed_sum[0][3] = value
        if isinstance(value, float) and not math.isfinite(value):
            try:
                typed_row((json.dumps(changed_sum[0]) + "\n").encode(), sum_types, "self-test")
            except ValueError:
                checks += 1
            else:
                raise AssertionError("Nonfinite DOUBLE accepted")
        else:
            run(changed_sum, sum_rows, sum_rows, column_types=sum_types, should_reject=True)
    changed_sum = copied(sum_rows)
    changed_sum[0][3] = math.nextafter(3.0, math.inf)
    assert not run(changed_sum, sum_rows, sum_rows, column_types=sum_types)["accepted"]
    changed_sum = copied(sum_rows)
    changed_sum[0][4] = 0.0
    assert not run(changed_sum, sum_rows, sum_rows, column_types=sum_types)["accepted"]
    def sum_schema_as_int(directory: Path) -> None:
        path = directory / "candidate/result.schema.json"
        schema = json.loads(path.read_text())
        schema["columns"][3]["type"] = "INT64"
        write_json(path, schema)
    run(sum_rows, sum_rows, sum_rows, column_types=sum_types,
        mutate=sum_schema_as_int, should_reject=True)
    # Binary64 JSON exponents canonicalize to the same exact float value.
    assert typed_canonical(typed_row(b"[3e0]\n", ["DOUBLE"], "self-test")) == b"[3.0]\n"
    checks += 1
    same_count_and_set = [group_rows[0], group_rows[1], group_rows[1]]
    result = run(same_count_and_set, group_rows, group_rows)
    assert not result["accepted"] and result["multiset"]["comparisons"]["candidateVsControl"]["differingDistinctRows"] == 2
    result = run(group_rows[:-1], group_rows, group_rows)
    assert not result["accepted"]
    assert result["multiset"]["comparisons"]["candidateVsControl"]["differingDistinctRows"] == 1
    changed = copied(group_rows)
    changed[0][4] = 0
    assert not run(changed, group_rows, group_rows)["accepted"]
    changed = copied(group_rows)
    changed[1][3] -= 1
    assert not run(changed, group_rows, group_rows)["accepted"]
    for value in (True, "1", 1.0, 2**63):
        changed = copied(group_rows)
        changed[0][0] = value
        run(changed, group_rows, group_rows, should_reject=True)
    def modify(directory: Path, filename: str, field: str, value: Any) -> None:
        path = directory / "candidate" / filename
        payload = json.loads(path.read_text())
        payload[field] = value
        write_json(path, payload)
    run(group_rows, group_rows, group_rows,
        mutate=lambda d: modify(d, "result-receipt.json", "sha256", "0" * 64), should_reject=True)
    run(group_rows, group_rows, group_rows,
        mutate=lambda d: modify(d, "result-receipt.json", "rows", len(group_rows) + 1), should_reject=True)
    run(group_rows, group_rows, group_rows,
        mutate=lambda d: modify(d, "result-receipt.json", "status", "PARTIAL_RESULT"), should_reject=True)
    def modify_column(directory: Path, field: str, value: Any) -> None:
        path = directory / "candidate/result.schema.json"
        schema = json.loads(path.read_text())
        schema["columns"][0][field] = value
        write_json(path, schema)
    run(group_rows, group_rows, group_rows,
        mutate=lambda d: modify_column(d, "type", "STRING"), should_reject=True)
    run(group_rows, group_rows, group_rows,
        mutate=lambda d: modify_column(d, "label", "other"), should_reject=True)
    run(group_rows, group_rows, group_rows,
        mutate=lambda d: modify_column(d, "index", True), should_reject=True)
    def change_encoding(directory: Path) -> None:
        path = directory / "candidate/result.rows.jsonl"
        path.write_bytes(path.read_text(encoding="utf-8").encode("utf-16-be"))
        modify(directory, "result-receipt.json", "sha256", typed_file_sha256(path))
    run(group_rows, group_rows, group_rows, mutate=change_encoding, should_reject=True)
    def tamper_oracle(directory: Path) -> None:
        path = directory / "fixture/oracle.jsonl"
        path.write_bytes(path.read_bytes() + typed_canonical(group_rows[0]))
    run(group_rows, group_rows, group_rows, mutate=tamper_oracle, should_reject=True)
    types = ["TIMESTAMP", "STRING", "INT64", "INT64", "INT64"]
    ordered = [[0, "d000", 1, 5, None], [1, "null", 2, 6, 0], [2, "x\u001f\n☃", 3, 7, 1]]
    result = run(list(reversed(ordered)), ordered, ordered, "ordered", types)
    assert not result["accepted"] and result["multiset"]["allTypedMultisetsEqual"]
    assert run(ordered, ordered, ordered, "ordered", types)["accepted"]
    changed = copied(ordered)
    changed[1][1] = None
    assert not run(changed, ordered, ordered, "ordered", types)["accepted"]
    run(ordered, ordered, ordered, "ordered", types,
        mutate=lambda d: modify(d, "result.schema.json", "timestampRepresentation", "text"), should_reject=True)
    # Time-only SQL permits arbitrary order within ties. The independent fixture
    # oracle may use device/time order; only the two original SQL streams must
    # have nondecreasing time. Integral sorting must stay exact beyond 2**53.
    tie_times = [-10, -2, 0, 2, 10, 9007199254740993, 2**63 - 1]
    tie_oracle = [[time, device, 1, index, None] for device in ("d000", "d001", "d002")
                  for index, time in enumerate(tie_times)]
    tie_oracle.extend([tie_oracle[0], tie_oracle[0]])
    tie_control = sorted(tie_oracle, key=lambda row: (row[0], row[1]))
    tie_candidate = sorted(tie_oracle, key=lambda row: (row[0], -int(row[1][1:])))
    tie_result = run(tie_candidate, tie_control, tie_oracle, "ordered", types,
                     ordered_key_columns=["time"])
    assert tie_result["accepted"] and tie_result["orderedSequenceMatchesOracle"] is None
    tie_report = tie_result["orderedKeyComparison"]
    assert tie_report["keyGroups"] == len(tie_times) and tie_report["mismatchingKeyGroups"] == 0
    assert tie_report["allKeyGroupsMatchOracle"] and tie_report["differingDistinctKeyRows"] == 0
    assert len({role["canonicalOrderedTieSha256"] for role in tie_report["roles"].values()}) == 1
    assert all(tie_result["inputs"][role]["orderedKeyValidation"]["nondecreasing"]
               for role in ("candidate", "control"))
    assert not run(tie_candidate, tie_control, tie_oracle, "ordered", types)["accepted"]
    assert run(tie_candidate, tie_control, tie_oracle, "ordered", types,
               ordered_key_columns=["TiMe"])["accepted"]
    assert run(tie_candidate, tie_control, tie_oracle, "ordered", types,
               ordered_key_columns=["time", "g"])["accepted"]
    inverse = copied(tie_candidate)
    inverse[0], inverse[-1] = inverse[-1], inverse[0]
    run(inverse, tie_control, tie_oracle, "ordered", types,
        ordered_key_columns=["time"], should_reject=True)
    run(tie_candidate, inverse, tie_oracle, "ordered", types,
        ordered_key_columns=["time"], should_reject=True)
    assert not run(tie_candidate[:-1], tie_control, tie_oracle, "ordered", types,
                   ordered_key_columns=["time"])["accepted"]
    repeated = copied(tie_candidate)
    # Keep the same row count and time key; replace a distinct device row by a duplicate.
    repeated[0] = copied([repeated[1]])[0]
    assert not run(repeated, tie_control, tie_oracle, "ordered", types,
                   ordered_key_columns=["time"])["accepted"]
    bad_type = copied(tie_candidate)
    bad_type[0][0] = float(bad_type[0][0])
    run(bad_type, tie_control, tie_oracle, "ordered", types,
        ordered_key_columns=["time"], should_reject=True)
    null_key = copied(tie_candidate)
    null_key[0][0] = None
    run(null_key, tie_control, tie_oracle, "ordered", types,
        ordered_key_columns=["time"], should_reject=True)
    wrong_null = copied(tie_candidate)
    wrong_null[0][-1] = 0
    assert not run(wrong_null, tie_control, tie_oracle, "ordered", types,
                   ordered_key_columns=["time"])["accepted"]
    run(tie_candidate, tie_control, tie_oracle, "ordered", types,
        ordered_key_columns=["time"],
        mutate=lambda d: modify_column(d, "type", "INT64"), should_reject=True)
    for keys in ([], ["absent"], ["time", "TIME"], ["device_id"]):
        run(tie_candidate, tie_control, tie_oracle, "ordered", types,
            ordered_key_columns=keys, should_reject=True)
    run(tie_candidate, tie_control, tie_oracle, "ordered", types,
        ordered_key_columns=["time"], oracle_limit=1, should_reject=True)
    run(group_rows, group_rows, group_rows, ordered_key_columns=["g"], should_reject=True)
    # A single tie exceeds the external sort's 1MiB buffer and spans many input
    # read/write blocks. Fixture and driver rows are streamed; no tie-row list exists.
    number += 1
    large_dir = root / str(number)
    large_dir.mkdir()
    large_rows = 12288
    def large_row(index: int) -> list[Any]:
        return [7, f"d{index:06d}" + "x" * 256, index % 257, index - 7000,
                None if index % 17 == 0 else index % 127 - 63]
    large_manifest = fixture(large_dir / "fixture",
        (large_row(index) for index in range(large_rows)), "ordered", types)
    actual(large_dir / "candidate", (large_row(index) for index in reversed(range(large_rows))),
           "ordered", types)
    actual(large_dir / "control", (large_row(index) for index in range(large_rows)),
           "ordered", types)
    assert (large_dir / "fixture/oracle.jsonl").stat().st_size > 2 * 1024 * 1024
    large_result = offline_typed_compare(argparse.Namespace(
        output=large_dir / "comparison", fixture_manifest=large_manifest,
        expected_fixture_sha256=typed_file_sha256(large_manifest), oracle="ordered",
        candidate_result_dir=large_dir / "candidate", control_result_dir=large_dir / "control",
        sort_memory_mib=1, sort_executable="sort", repository_root=None,
        oracle_limit=None, ordered_key_columns=["time"],
    ))
    assert large_result["accepted"]
    large_ties = large_result["orderedKeyComparison"]
    assert large_ties["keyGroups"] == 1 and large_ties["perKeyComparison"]["rows"] == 1
    assert large_ties["boundedMemory"]["retainedCompleteTieGroups"] == 0
    assert all(value["rows"] == large_rows for value in large_ties["roles"].values())
    assert all("--buffer-size=1M" in value["sortCommand"]
               for value in large_ties["sortInputs"].values())
    assert all(large_result["inputs"][role]["orderedKeyValidation"]["maximumTieRows"] == large_rows
               for role in ("candidate", "control"))
    checks += 1
    # The full-scan compute predicate has the same selected rows as the window
    # oracle, but SQL has no ORDER BY. Verify its entire bag without imposing
    # the oracle file's deterministic storage sequence.
    full_scan_result = run(ordered[::-1], ordered, ordered, "fullScanWindowFiltered", types)
    assert full_scan_result["accepted"] and not full_scan_result["orderingRequired"]
    assert full_scan_result["orderedKeyComparison"] is None
    assert full_scan_result["orderedSequenceMatchesOracle"] is None
    assert not run(ordered[:-1], ordered, ordered, "fullScanWindowFiltered", types)["accepted"]
    duplicated_window = [ordered[0], ordered[0], ordered[2]]
    assert not run(duplicated_window, ordered, ordered, "fullScanWindowFiltered", types)["accepted"]
    invalid_window_type = copied(ordered)
    invalid_window_type[0][0] = "0"
    run(invalid_window_type, ordered, ordered, "fullScanWindowFiltered", types, should_reject=True)
    changed_window_null = copied(ordered)
    changed_window_null[0][-1] = 0
    assert not run(changed_window_null, ordered, ordered, "fullScanWindowFiltered", types)["accepted"]
    run(ordered, ordered, ordered, "fullScanWindowFiltered", types,
        oracle_limit=1, should_reject=True)
    run(ordered, ordered, ordered, "fullScanWindowFiltered", types,
        ordered_key_columns=["time"], should_reject=True)
    run(ordered, ordered, ordered, "fullScanWindowFiltered", types,
        mutate=lambda d: modify(d, "result-receipt.json", "status", "PARTIAL_RESULT"), should_reject=True)
    # A no-ORDER LIMIT over a constant projection has a deterministic complete
    # multiset despite arbitrary source selection. Duplicate multiplicity is the proof.
    constant_limit = [[1] for _ in range(257)]
    limit_result = run(constant_limit, constant_limit, constant_limit,
                       "runtimeEarlyLimitConstant", ["INT64"])
    assert limit_result["accepted"] and not limit_result["orderingRequired"]
    assert all(role["rows"] == 257 and role["distinctRows"] == 1
               for role in limit_result["multiset"]["roles"].values())
    assert not run(constant_limit[:-1], constant_limit, constant_limit,
                   "runtimeEarlyLimitConstant", ["INT64"])["accepted"]
    assert not run(constant_limit + [[1]], constant_limit, constant_limit,
                   "runtimeEarlyLimitConstant", ["INT64"])["accepted"]
    assert not run([[0]] + constant_limit[1:], constant_limit, constant_limit,
                   "runtimeEarlyLimitConstant", ["INT64"])["accepted"]
    assert not run([[None]] + constant_limit[1:], constant_limit, constant_limit,
                   "runtimeEarlyLimitConstant", ["INT64"])["accepted"]
    for invalid_constant in (1.0, True, "1"):
        run([[invalid_constant]] + constant_limit[1:], constant_limit, constant_limit,
            "runtimeEarlyLimitConstant", ["INT64"], should_reject=True)
    run(constant_limit, constant_limit, constant_limit, "runtimeEarlyLimitConstant", ["INT64"],
        mutate=lambda d: modify(d, "result-receipt.json", "status", "PARTIAL_RESULT"), should_reject=True)
    run(constant_limit, constant_limit, constant_limit, "runtimeEarlyLimitConstant", ["INT64"],
        mutate=lambda d: modify(d, "result-receipt.json", "rows", 0), should_reject=True)
    run(constant_limit, constant_limit, constant_limit, "runtimeEarlyLimitConstant", ["INT64"],
        mutate=lambda d: modify_column(d, "label", "other"), should_reject=True)
    run(constant_limit, constant_limit, constant_limit, "runtimeEarlyLimitConstant", ["INT64"],
        oracle_limit=257, should_reject=True)
    run(constant_limit, constant_limit, constant_limit, "runtimeEarlyLimitConstant", ["INT64"],
        ordered_key_columns=["c"], should_reject=True)
    # A LIMIT oracle is an explicit complete SQL result, selected only after validating the
    # entire independent ordered oracle. Arbitrary partial prefixes never become full results.
    for limit in (1, 2, len(ordered)):
        result = run(ordered[:limit], ordered[:limit], ordered, "ordered", types, oracle_limit=limit)
        assert result["accepted"] and result["oracleLimit"] == limit
        assert result["inputs"]["oracle"]["rows"] == limit
        full = result["inputs"]["oracle"]["fullOracleVerification"]
        assert full["rows"] == len(ordered) and full["rawSha256"] == full["declaredSha256"]
        assert result["orderingRequired"] and all(result["orderedSequenceMatchesOracle"].values())
    for limit in (0, -1, True, 1.0, "1", len(ordered) + 1):
        run(ordered[:1], ordered[:1], ordered, "ordered", types,
            oracle_limit=limit, should_reject=True)
    run(group_rows[:1], group_rows[:1], group_rows, oracle_limit=1, should_reject=True)
    run(ordered[:1], ordered[:1], ordered, "rawFiltered", types,
        oracle_limit=1, should_reject=True)
    assert not run(ordered[:2], ordered[:2], ordered, "ordered", types)["accepted"]
    assert not run(ordered[1:], ordered[1:], ordered, "ordered", types, oracle_limit=2)["accepted"]
    assert not run(ordered[:1], ordered[:1], ordered, "ordered", types, oracle_limit=2)["accepted"]
    assert not run(ordered, ordered, ordered, "ordered", types, oracle_limit=2)["accepted"]
    result = run(ordered[:2][::-1], ordered[:2], ordered, "ordered", types, oracle_limit=2)
    assert not result["accepted"] and result["multiset"]["allTypedMultisetsEqual"]
    def tamper_ordered_suffix(directory: Path) -> None:
        path = directory / "fixture/oracle.jsonl"
        changed = copied(ordered)
        changed[-1][-1] += 1
        path.write_bytes(b"".join(typed_canonical(row) for row in changed))
    run(ordered[:1], ordered[:1], ordered, "ordered", types, oracle_limit=1,
        mutate=tamper_ordered_suffix, should_reject=True)
    def invalid_typed_suffix_with_matching_sha(directory: Path) -> None:
        path = directory / "fixture/oracle.jsonl"
        changed = copied(ordered)
        changed[-1][0] = "invalid timestamp after the selected prefix"
        path.write_bytes(b"".join(typed_canonical(row) for row in changed))
        manifest_path = directory / "fixture/manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["oracles"]["ordered"]["sha256"] = typed_file_sha256(path)
        write_json(manifest_path, manifest)
    run(ordered[:1], ordered[:1], ordered, "ordered", types, oracle_limit=1,
        mutate=invalid_typed_suffix_with_matching_sha, should_reject=True)
    run(ordered[:1], ordered[:1], ordered, "ordered", types, oracle_limit=1,
        mutate=lambda d: modify(d, "result-receipt.json", "status", "PARTIAL_RESULT"), should_reject=True)
    # Exercise the unmaterialized oracle through the same public comparison path.
    number += 1
    directory = root / str(number)
    directory.mkdir()
    raw_manifest = {
        "schemaVersion": 2, "fixtureVersion": "runtime_sql_fixture_v1-self-test", "groups": 257,
        "devices": 2, "timePartitions": 1, "baseRowsPerDevicePartition": 33,
        "timePartitionIntervalMs": 64, "heavyCell": {"deviceIndex": 1, "partitionIndex": 0, "rows": 34},
        "totalRows": 67,
        "rowDefinition": {
            "device_id": "d + zero-padded decimal device index, width=3",
            "time": "partitionIndex*timePartitionIntervalMs + rowIndex",
            "g": "(deviceIndex*17+rowIndex)%257",
            "s1": "((deviceIndex*1009+rowIndex)%2001)-1000",
            "s2": "NULL iff (deviceIndex+partitionIndex+rowIndex)%17==0, otherwise (rowIndex%127)-63",
        },
    }
    raw_rows = []
    for device, count in [(0, 33), (1, 34)]:
        for index in range(count):
            value = (1009 * device + index) % 2001 - 1000
            if value >= 0 or value % 97 == 0:
                raw_rows.append([index, f"d{device:03d}", (17 * device + index) % 257, value,
                                 None if (device + index) % 17 == 0 else index % 127 - 63])
    assert any(row[3] < 0 for row in raw_rows), "Must include negative exact remainder zero"
    raw_manifest["oracles"] = {"rawFiltered": {
        "materialized": False, "rows": len(raw_rows),
        "sha256": hashlib.sha256(b"".join(typed_canonical(row) for row in raw_rows)).hexdigest(),
        "columnTypes": types, "canonicalEncoding": TYPED_ENCODING, "orderBy": ["device_id", "time"],
    }}
    manifest_path = directory / "manifest.json"
    write_json(manifest_path, raw_manifest)
    actual(directory / "candidate", list(reversed(raw_rows)), "rawFiltered", types)
    actual(directory / "control", raw_rows, "rawFiltered", types)
    result = offline_typed_compare(argparse.Namespace(
        output=directory / "comparison", fixture_manifest=manifest_path,
        expected_fixture_sha256=typed_file_sha256(manifest_path), oracle="rawFiltered",
        candidate_result_dir=directory / "candidate", control_result_dir=directory / "control",
        sort_memory_mib=1, sort_executable="sort", repository_root=None,
    ))
    assert result["accepted"]
    checks += 1
    # Hash domain includes the schema, even for the same literal JSON row.
    literal = directory / "literal.jsonl"
    literal.write_bytes(b"[1]\n")
    paths = {"candidate": literal, "control": literal, "oracle": literal}
    int_hash = typed_multiset_comparison(paths, ["INT64"])["roles"]["candidate"]["typedMultisetSha256"]
    time_hash = typed_multiset_comparison(paths, ["TIMESTAMP"])["roles"]["candidate"]["typedMultisetSha256"]
    assert int_hash != time_hash
    checks += 1
    return checks


def self_test() -> int:
    checks = 0
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        def rejects(call, text):
            nonlocal checks
            try:
                call(text)
            except RuntimeError:
                checks += 1
            else:
                raise AssertionError("invalid evidence accepted: " + text[:160])
        plan = root / "plan.stdout"
        plan.write_text(
            "Property enforcement:\nTableHashPartitioningShuffleSinkNode TABLE_HASH_V1 "
            "sources=2 partitions=2 (N x P experimental path) "
            "hash-topology sourceIds=[ps1, ps2] bucketIds=[pb1, pb2] "
            "exchangeIds=[[pe11, pe12], [pe21, pe22]]\n", encoding="utf-8")
        shape = validate_enabled_plan(plan, 2, 2)
        checks += 1
        runtime = root / "runtime.stdout"
        text = "\n".join([
            "Fragment Instances Count: 5",
            "FRAGMENT-INSTANCE[Id: q.0.0][IP: dn-a][State: FINISHED]",
            "[PlanNodeId root]: CollectNode(CollectOperator)",
            "FRAGMENT-INSTANCE[Id: q.1.0][IP: dn-a][State: FINISHED]",
            "[PlanNodeId s1]: TableHashPartitioningShuffleSinkNode(HashPartitioningSinkOperator)",
            "DownStreamPlanNodeId: e11, e12",
            "FRAGMENT-INSTANCE[Id: q.2.0][IP: dn-b][State: FINISHED]",
            "[PlanNodeId s2]: TableHashPartitioningShuffleSinkNode(HashPartitioningSinkOperator)",
            "DownStreamPlanNodeId: e21, e22",
            "FRAGMENT-INSTANCE[Id: q.3.0][IP: dn-a][State: FINISHED]",
            "[PlanNodeId b1]: AggregationNode(HashAggregationOperator)",
            "[PlanNodeId c1]: CollectNode(CollectOperator)",
            "[PlanNodeId e11]: ExchangeNode(ExchangeOperator)",
            "[PlanNodeId e21]: ExchangeNode(ExchangeOperator)",
            "FRAGMENT-INSTANCE[Id: q.4.0][IP: dn-b][State: FINISHED]",
            "[PlanNodeId b2]: AggregationNode(HashAggregationOperator)",
            "[PlanNodeId c2]: CollectNode(CollectOperator)",
            "[PlanNodeId e12]: ExchangeNode(ExchangeOperator)",
            "[PlanNodeId e22]: ExchangeNode(ExchangeOperator)",
        ])
        def validate(value):
            runtime.write_text(value, encoding="utf-8")
            return validate_enabled_runtime(runtime, shape, 2, 5)
        assert validate(text)["verified_exchange_edges"] == 4
        checks += 1
        for invalid in [
            text.replace("dn-b", "dn-a"),
            text.replace("Count: 5", "Count: 6"),
            text.replace("q.4.0", "q.3.0"),
            text.replace("DownStreamPlanNodeId: e21, e22", "DownStreamPlanNodeId: e21"),
            text.replace("DownStreamPlanNodeId: e21, e22", "DownStreamPlanNodeId: e21, e12"),
            text.replace("DownStreamPlanNodeId: e21, e22", "DownStreamPlanNodeId: e22, e21"),
            text.replace("DownStreamPlanNodeId: e21, e22", "DownStreamPlanNodeId: e21, missing"),
            text.replace("[PlanNodeId e22]", "[PlanNodeId e21]"),
            text.replace("[PlanNodeId b2]: AggregationNode", "[PlanNodeId b2]: ProjectNode"),
            text.replace("[State: FINISHED]", "[State: RUNNING]", 1),
        ]:
            rejects(validate, invalid)
        datanodes = root / "datanodes.stdout"
        def nodes(value):
            datanodes.write_text(value, encoding="utf-8")
            return validate_datanodes(datanodes, 2)
        node_text = "| NodeID | Status |\n| 1 | Running |\n| 2 | Running |\nTotal line number = 2\n"
        assert nodes(node_text)["datanode_count"] == 2
        checks += 1
        rejects(nodes, node_text.replace("| 2 |", "| 1 |"))
        rejects(nodes, node_text.replace("Running", "Unknown", 1))
        table = root / "table.stdout"
        table.write_text("| s1 | row_count |\n| null | 4 |\n| 2 | 8 |\nTotal line number = 2\n", encoding="utf-8")
        parsed = table_rows(table)
        assert compare_results(parsed, parsed, 2)["matched"]
        assert not compare_results(parsed, (parsed[0], parsed[1] + parsed[1][:1]), 2)["matched"]
        checks += 2
        table.write_text("| s1 | row_count |\nEmpty set.\n", encoding="utf-8")
        assert table_rows(table)[1] == []
        checks += 1
        def result(value):
            table.write_text(value, encoding="utf-8")
            return table_rows(table)
        rejects(result, "")
        rejects(result, "| s1 | row_count |\n")
        rejects(result, "| s1 | row_count |\n| 1 | 4 |\nEmpty set.\n")
        typed_checks = typed_self_test(root / "typed-results")
        checks += typed_checks
    print(json.dumps({"self_test": "PASS", "checks": checks, "typed_result_checks": typed_checks}))
    return 0

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--offline-typed-compare", action="store_true")
    parser.add_argument("--candidate-result-dir", type=Path)
    parser.add_argument("--control-result-dir", type=Path)
    parser.add_argument("--fixture-manifest", type=Path)
    parser.add_argument("--expected-fixture-sha256")
    parser.add_argument("--oracle", choices=tuple(TYPED_ORACLE_NAMES))
    parser.add_argument("--ordered-key-columns", nargs="+",
                        help="explicit non-NULL ASC integral keys; verify original key order and exact tie multisets")
    parser.add_argument("--oracle-limit", type=int,
                        help="complete ordered SQL LIMIT result: verify the full oracle, then select its first N rows")
    parser.add_argument("--sort-memory-mib", type=int, default=64)
    parser.add_argument("--sort-executable", default="sort")
    parser.add_argument("--repository-root", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--table", help="unquoted database.table fixture name")
    parser.add_argument("--username", default="root")
    parser.add_argument("--password-env", default="IOTDB_E2E_PASSWORD")
    parser.add_argument("--expected-sources", type=int, default=2)
    parser.add_argument("--expected-buckets", type=int, default=2)
    parser.add_argument("--minimum-result-rows", type=int, default=2)
    parser.add_argument("--expected-datanodes", type=int, default=2)
    parser.add_argument("--minimum-fragment-instances", type=int)
    for endpoint_name in ("enabled", "baseline"):
        parser.add_argument(f"--{endpoint_name}-cli")
        parser.add_argument(f"--{endpoint_name}-host")
        parser.add_argument(f"--{endpoint_name}-port", type=int)
        parser.add_argument(f"--{endpoint_name}-config", type=Path)
        parser.add_argument(f"--{endpoint_name}-isolation-id")
    args = parser.parse_args()
    if args.ordered_key_columns is not None and not args.offline_typed_compare:
        parser.error("--ordered-key-columns is only valid with --offline-typed-compare")
    if args.oracle_limit is not None and not args.offline_typed_compare:
        parser.error("--oracle-limit is only valid with --offline-typed-compare")
    if args.self_test:
        return self_test()
    if args.offline_typed_compare:
        required = ["output", "candidate_result_dir", "control_result_dir", "fixture_manifest",
                    "expected_fixture_sha256", "oracle"]
        missing = [name.replace("_", "-") for name in required if getattr(args, name) is None]
        if missing:
            parser.error("required for offline typed comparison: " + ", ".join("--" + name for name in missing))
        try:
            report = offline_typed_compare(args)
        except (OSError, RuntimeError, ValueError, KeyError, TypeError) as error:
            print(f"offline typed result comparison failed: {error}", file=sys.stderr)
            return 1
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report["accepted"] else 1
    required = ["output", "table"]
    for endpoint_name in ("enabled", "baseline"):
        required.extend(
            [f"{endpoint_name}_cli", f"{endpoint_name}_host", f"{endpoint_name}_port", f"{endpoint_name}_config", f"{endpoint_name}_isolation_id"]
        )
    missing = [name.replace("_", "-") for name in required if getattr(args, name) is None]
    if missing:
        parser.error("required for a server run: " + ", ".join(f"--{name}" for name in missing))
    try:
        report = execute(args)
    except (OSError, RuntimeError, ValueError) as error:
        if args.output is not None:
            args.output.mkdir(parents=True, exist_ok=True)
            (args.output / "failure.txt").write_text(f"{error}\n", encoding="utf-8")
        print(f"multi-source hash GROUP BY acceptance failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
