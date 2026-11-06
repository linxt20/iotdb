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

"""Compare two CSV result sets, preserving duplicate rows in the comparison."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from pathlib import Path


def read_rows(path: Path, has_header: bool) -> list[tuple[str, ...]]:
    with path.open("r", newline="", encoding="utf-8") as source:
        reader = csv.reader(source)
        if has_header:
            next(reader, None)
        return [tuple(row) for row in reader]


def digest(rows: list[tuple[str, ...]]) -> str:
    hasher = hashlib.sha256()
    for row in rows:
        hasher.update("\x1f".join(row).encode("utf-8"))
        hasher.update(b"\n")
    return hasher.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", required=True, type=Path)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--ordered", action="store_true", help="also require identical row order")
    parser.add_argument("--no-header", action="store_true")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()

    baseline = read_rows(args.baseline, not args.no_header)
    candidate = read_rows(args.candidate, not args.no_header)
    baseline_bag = Counter(baseline)
    candidate_bag = Counter(candidate)
    same_bag = baseline_bag == candidate_bag
    same_order = baseline == candidate
    matched = same_bag and (same_order if args.ordered else True)
    report = {
        "baseline": str(args.baseline),
        "candidate": str(args.candidate),
        "ordered": args.ordered,
        "baseline_rows": len(baseline),
        "candidate_rows": len(candidate),
        "baseline_multiset_sha256": digest(sorted(baseline)),
        "candidate_multiset_sha256": digest(sorted(candidate)),
        "same_multiset": same_bag,
        "same_order": same_order,
        "matched": matched,
    }
    rendered = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if matched else 1


if __name__ == "__main__":
    raise SystemExit(main())
