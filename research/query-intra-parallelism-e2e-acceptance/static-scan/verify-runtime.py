#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to You under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.
# See the License for the specific language governing permissions
# and limitations under the License.

"""Verify complete single-source 32-bucket evidence; no server operations."""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path


def canonical(rows):
    return "\n".join(sorted(json.dumps(row, separators=(",", ":"), ensure_ascii=False)
                            for row in rows)) + "\n"


def require(value, message):
    if not value:
        raise ValueError(message)


def verify(side: Path, expected: dict, candidate: bool) -> dict:
    read = lambda name: json.loads((side / name).read_text())
    receipt, plan = read("receipt.json"), read("frozen-plan.json")
    rows, buckets, snapshots = read("result.json"), read("bucket-results.json"), read("terminal-snapshots.json")
    require(receipt["status"] == "PASS", "Driver did not finish successfully")
    require((plan["sourceCount"], plan["bucketCount"], plan["hashChannelCount"],
             plan["fragmentInstanceCount"], plan["scanDop"]) == (1, 32, 32, 34, 1), "Frozen topology differs")
    require(plan["outputTypes"] == expected["types"], "Analyzed output types differ")
    require(len(plan["buckets"]) == len(buckets) == 32, "Missing bucket")
    require({bucket["bucket"] for bucket in buckets} == set(range(32)), "Duplicate/missing logical bucket")
    for key in ("fragmentInstanceId", "sourceExchangeId", "gatherExchangeId", "bucketSinkId"):
        require(len({bucket[key] for bucket in buckets}) == 32, "Physical identity reused: " + key)
    require(len({item["fragmentInstanceId"] for item in snapshots}) == 34, "Runtime FI identity reused")
    require({item["fragmentInstanceId"] for item in snapshots}
            == {item["fragmentInstanceId"] for item in plan["frozenFragments"]}, "Runtime/frozen FI set differs")
    require(len({item["endpoint"] for item in snapshots}) == 2, "Actual runtime did not use two DataNodes")
    require(len({item["dataNodeId"] for item in buckets}) == 2, "Receive buckets did not span two nodes")
    require(all(bucket["sourceMemoryKey"] == plan["sourceMemoryKey"] for bucket in buckets), "Source account split")
    counts = {bucket["bucket"]: bucket["outputRows"] for bucket in buckets}
    require(all(bucket["state"] == "FINISHED" for bucket in buckets), "A real bucket did not terminate")
    require([counts[i] for i in range(32)] == expected["bucketRowCounts"], "Actual bucket routing differs")
    require(sum(counts[i] == 0 for i in range(32)) == expected["emptyBucketCount"] > 0, "No completed empty bucket")
    actual = canonical(rows)
    require(Counter(map(json.dumps, rows)) == Counter(map(json.dumps, expected["rows"])), "Independent full multiset differs")
    digest = hashlib.sha256(actual.encode()).hexdigest()
    require(digest == expected["multisetSha256"] == receipt["resultSha256"], "Full result SHA differs")
    require((side / "result.canonical.jsonl").read_text() == actual, "Saved canonical bytes differ")
    require(len(rows) == expected["rowCount"] == receipt["rows"], "Complete result row count differs")
    source = next(item for item in snapshots if item["fragmentInstanceId"] == plan["sourceFragmentInstanceId"])
    require(any(operator["planNodeId"] == plan["sourceSinkId"]
                and "HashPartition" in operator["operatorType"]
                for operator in source["actualOperators"]), "Missing actual production hash sink")
    if candidate:
        require(source["plannedDop"] == source["actualPipelineCount"] == 1, "Candidate source created extra drivers")
        require(receipt["resourceProtocol"] == "B_REQUIRED_AND_CHECKED", "Candidate resource checks skipped")
        resource = read("resource-terminal.json")
        require(len(resource) == 34, "Missing independent per-fragment resource receipt")
        by_id = {item["fragmentInstanceId"]: item for item in resource}
        require(set(by_id) == {item["fragmentInstanceId"] for item in snapshots}, "Resource/runtime FI identity differs")
        for record in resource:
            require(record["complete"] and record["released"] and not record["fragmentMapPresent"],
                    "Fragment cleanup is incomplete")
            for key in ("liveDrivers", "liveQueues", "unconsumedFactories", "liveEndpoints",
                        "pendingFutures", "fragmentReservedBytes", "queryReservedBytes", "deregisterFailures"):
                require(record[key] == 0, "Nonzero terminal resource: " + key)
            require(record["deregisterReceipt"]["success"]
                    and record["deregisterReceipt"]["bytesAfter"] == 0
                    and not record["deregisterReceipt"]["presentAfter"], "Missing successful zero deregistration")
            require(all(item["terminal"] for item in record["objects"]), "Physical object remains open")
            require(record["createdDrivers"] == record["closedDrivers"], "Actual drivers did not all close")
        before = {pool["dataNodeId"]: pool for pool in read("resource-idle-before.json")}
        after = {pool["dataNodeId"]: pool for pool in read("resource-idle-after.json")}
        require(len(before) == len(after) == 2 and before.keys() == after.keys(), "Missing pool comparison")
        for node, pool in after.items():
            require(all(pool[key] == before[node][key]
                        for key in ("exchangeRemainingBytes", "operatorsFreeBytes")), "Independent pools did not restore")
        objects = by_id[plan["sourceFragmentInstanceId"]]["objects"]
        channels = [item for item in objects if item["kind"] == "SINK_CHANNEL"]
        require(len(channels) == len({item["token"] for item in channels}) == 32, "Actual physical source channels differ")
        require(len({(item["memoryQueryId"], item["memoryFragmentInstanceId"], item["memoryPlanNodeId"])
                     for item in channels}) == 1, "Actual producer has multiple memory keys")
    else:
        require(receipt["resourceProtocol"] == "UNAVAILABLE_IN_FROZEN_A", "A resource observation scope hidden")
    return {"status": "PASS", "rows": len(rows), "sha256": digest, "actualFragmentInstances": 34,
            "actualReceiveBuckets": 32, "completedEmptyBuckets": expected["emptyBucketCount"],
            "candidateResourceChecks": candidate}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--expected", type=Path, default=Path(__file__).with_name("expected-results.json"))
    parser.add_argument("--candidate", action="store_true")
    args = parser.parse_args()
    print(json.dumps(verify(args.run, json.loads(args.expected.read_text()), args.candidate), indent=2))


if __name__ == "__main__":
    main()
