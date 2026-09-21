#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with this
# work for additional information regarding copyright ownership.  The ASF
# licenses this file to You under the Apache License, Version 2.0.

"""Read and archive query-scoped remote TsBlock payload audit events.

The DataNode instrumentation writes one logfmt event only at the remote exchange RPC payload
boundary.  This utility is deliberately read-only with respect to the server: it reads explicit,
non-overlapping DataNode logs, copies matching lines into an attempt directory, and emits one JSON
object.  It never treats SinkChannel count, buffer retained bytes, process-wide metrics, or local
channels as shuffle bytes.

``sent_payload_bytes`` counts application payload bytes offered by each source DataNode to its
Thrift response. ``received_payload_bytes`` counts payload returned to each remote SourceHandle.
Retries are separate transfer attempts in both fields. A complete, quiet two-DataNode archive must
make them equal; otherwise the default is to fail instead of inventing ``shuffle_bytes`` from an
incomplete log set.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import tempfile
from pathlib import Path
from typing import Any


MARKER = "REMOTE_SHUFFLE_PAYLOAD_BYTES"
QUERY_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")
FIELD_RE = re.compile(r"([A-Za-z_]+)=([^\s]+)")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def parse_event(line: str) -> dict[str, str] | None:
    marker = line.find(MARKER)
    if marker < 0:
        return None
    fields = dict(FIELD_RE.findall(line[marker + len(MARKER):]))
    if fields.get("version") != "1" or fields.get("direction") not in {"sent", "received"}:
        return None
    required = ("query_id", "producer_fragment", "consumer_fragment", "channel", "payload_bytes", "payload_blocks")
    if any(field not in fields for field in required):
        raise ValueError(f"malformed {MARKER} event: {line.rstrip()}")
    try:
        payload_bytes = int(fields["payload_bytes"])
        payload_blocks = int(fields["payload_blocks"])
        channel = int(fields["channel"])
    except ValueError as error:
        raise ValueError(f"non-integer payload field in audit event: {line.rstrip()}") from error
    if payload_bytes <= 0 or payload_blocks <= 0 or channel < 0:
        raise ValueError(f"invalid payload values in audit event: {line.rstrip()}")
    return fields


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def collect(query_id: str, logs: list[Path], raw_dir: Path, allow_direction_mismatch: bool) -> dict[str, Any]:
    if not QUERY_ID_RE.fullmatch(query_id):
        raise ValueError("--query-id contains characters that cannot occur in the version-1 audit format")
    if len({path.resolve() for path in logs}) != len(logs):
        raise ValueError("--log paths must be distinct; overlapping/copy logs would double count events")
    if raw_dir.exists() and any(raw_dir.iterdir()):
        raise ValueError("--raw-dir must be new or empty to preserve evidence")
    raw_dir.mkdir(parents=True, exist_ok=True)

    totals = {"sent": {"bytes": 0, "blocks": 0, "events": 0}, "received": {"bytes": 0, "blocks": 0, "events": 0}}
    inputs: list[dict[str, str]] = []
    for index, path in enumerate(logs, start=1):
        try:
            original = path.read_bytes()
        except OSError as error:
            raise ValueError(f"cannot read audit log {path}: {error}") from error
        try:
            lines = original.decode("utf-8").splitlines(keepends=True)
        except UnicodeDecodeError as error:
            raise ValueError(f"audit log is not UTF-8 text: {path}") from error
        matching: list[str] = []
        for line in lines:
            event = parse_event(line)
            if event is None or event["query_id"] != query_id:
                continue
            direction = event["direction"]
            totals[direction]["bytes"] += int(event["payload_bytes"])
            totals[direction]["blocks"] += int(event["payload_blocks"])
            totals[direction]["events"] += 1
            matching.append(line)
        archive = raw_dir / f"input-{index:02d}.matching-events.log"
        archive.write_text("".join(matching), encoding="utf-8")
        inputs.append({
            "path": str(path), "sha256": sha256_bytes(original), "matching_event_file": archive.name,
            "matching_events": str(len(matching)),
        })

    if totals["sent"]["bytes"] != totals["received"]["bytes"] and not allow_direction_mismatch:
        raise RuntimeError(
            "sent/received payload bytes differ; archive all non-overlapping DataNode audit logs "
            "for this query or explicitly inspect a failed/retried transfer before using a metric"
        )
    if totals["sent"]["bytes"] == 0 and totals["received"]["bytes"] == 0:
        raise RuntimeError("no remote payload audit event matches --query-id; zero is not accepted as measured evidence")
    result = {
        "query_id": query_id,
        "shuffle_bytes": totals["sent"]["bytes"],
        "shuffle_bytes_source": "sum(version=1 REMOTE_SHUFFLE_PAYLOAD_BYTES direction=sent payload_bytes)",
        "received_payload_bytes": totals["received"]["bytes"],
        "sent_payload_blocks": totals["sent"]["blocks"],
        "received_payload_blocks": totals["received"]["blocks"],
        "sent_events": totals["sent"]["events"],
        "received_events": totals["received"]["events"],
        "direction_match": totals["sent"]["bytes"] == totals["received"]["bytes"],
        "retry_semantics": "each RPC payload attempt is counted; no sequence-id deduplication",
        "local_channel_bytes_included": False,
        "thrift_framing_or_transport_overhead_included": False,
        "inputs": inputs,
    }
    write_json(raw_dir / "remote-shuffle-payload-bytes.json", result)
    return result


def self_test() -> int:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        one = root / "dn-1.log"
        two = root / "dn-2.log"
        one.write_text(
            "x REMOTE_SHUFFLE_PAYLOAD_BYTES version=1 direction=sent query_id=q.1 producer_fragment=q.1:0:0 consumer_fragment=- channel=0 payload_bytes=5 payload_blocks=1\n",
            encoding="utf-8",
        )
        two.write_text(
            "x REMOTE_SHUFFLE_PAYLOAD_BYTES version=1 direction=received query_id=q.1 producer_fragment=q.1:0:0 consumer_fragment=q.1:1:0 channel=0 payload_bytes=5 payload_blocks=1\n",
            encoding="utf-8",
        )
        result = collect("q.1", [one, two], root / "archive", False)
        if result["shuffle_bytes"] != 5 or not result["direction_match"]:
            return 1
    print("extract_remote_shuffle_payload_bytes self-test passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--query-id")
    parser.add_argument("--log", action="append", type=Path, default=[])
    parser.add_argument("--raw-dir", type=Path)
    parser.add_argument("--allow-direction-mismatch", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if not args.query_id or not args.log or args.raw_dir is None:
        parser.error("--query-id, one or more --log, and --raw-dir are required")
    try:
        print(json.dumps(collect(args.query_id, args.log, args.raw_dir, args.allow_direction_mismatch), ensure_ascii=False))
        return 0
    except (OSError, RuntimeError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
