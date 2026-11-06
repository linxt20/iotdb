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

"""Independent observed source32 pressure acceptance. Never starts/attaches/stops a DataNode.

Runtime requires pre-attached, explicitly owned fresh services and a frozen complete-PASS
small-case helper. --check-binding validates files only. --run is an explicit runtime action.
"""
from __future__ import annotations

import argparse
import hashlib
import heapq
import json
import os
from pathlib import Path
import selectors
import shutil
import signal
import subprocess
import sys
import tarfile
import time
from urllib.parse import unquote, urlparse

ROOT = Path(__file__).resolve().parent
JAVA_MAIN = "StaticScanAcceptanceDriver"
GATE_MAIN = "StaticScanPressureGate"
ROWS = 1_048_576


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1_048_576), b""):
            digest.update(block)
    return digest.hexdigest()


def save(path, value):
    with Path(path).open("x") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write("\n")


def read(path):
    return json.loads(Path(path).read_text())


def binding(value):
    path = Path(value["path"])
    require(path.is_absolute() and path.is_file(), "Missing absolute file binding: " + str(path))
    require(sha(path) == value["sha256"], "File binding differs: " + str(path))
    return path


def row_at(index):
    return [1 if (index // 4) % 2 == 0 else 2,
            None if index % 97 == 0 else index + 0.5]


def canonical_row(row):
    require(isinstance(row, list) and len(row) == 2, "Output row width differs")
    require(type(row[0]) is int and row[0] in (1, 2), "INT64 key type/value differs")
    require(row[1] is None or (type(row[1]) is float and row[1] >= 0.5
                             and row[1] <= ROWS - 0.5), "DOUBLE type/value differs")
    return json.dumps(row, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n"


def external_sort(rows, output, prefix):
    """Keep at most 65536 canonical rows in memory; preserve sorted chunks as evidence."""
    paths, chunk, count = [], [], 0
    for row in rows:
        chunk.append(canonical_row(row))
        count += 1
        if len(chunk) == 65536:
            path = output / (prefix + ".chunk-%04d.jsonl" % len(paths))
            with path.open("x") as stream:
                stream.writelines(sorted(chunk))
            paths.append(path)
            chunk = []
    if chunk:
        path = output / (prefix + ".chunk-%04d.jsonl" % len(paths))
        with path.open("x") as stream:
            stream.writelines(sorted(chunk))
        paths.append(path)
    streams = [path.open() for path in paths]
    result = output / (prefix + ".canonical.jsonl")
    digest = hashlib.sha256()
    try:
        with result.open("xb") as stream:
            for line in heapq.merge(*streams):
                data = line.encode()
                stream.write(data)
                digest.update(data)
    finally:
        for stream in streams:
            stream.close()
    return {"rows": count, "path": str(result), "sha256": digest.hexdigest(),
            "chunks": [{"path": str(path), "sha256": sha(path)} for path in paths]}


def stream_array(path):
    """Incrementally parse the helper's JSON array, including its pretty-printed form."""
    decoder = json.JSONDecoder()
    with Path(path).open() as stream:
        buffer, pos, eof, state = "", 0, False, "START"
        while True:
            if not eof and (len(buffer) - pos < 4096):
                block = stream.read(65536)
                buffer = buffer[pos:] + block
                pos = 0
                eof = not block
            while pos < len(buffer) and buffer[pos].isspace():
                pos += 1
            if state == "START":
                require(pos < len(buffer) and buffer[pos] == "[", "Result is not a JSON array")
                pos += 1
                state = "FIRST_OR_END"
                continue
            if pos == len(buffer):
                require(not eof, "Truncated result array")
                continue
            if buffer[pos] == "]":
                require(state in ("FIRST_OR_END", "DELIMITER"), "Trailing row separator")
                require(not (buffer[pos + 1:] + stream.read()).strip(), "Trailing result data")
                return
            if state == "DELIMITER":
                require(buffer[pos] == ",", "Missing row separator")
                pos += 1
                state = "VALUE"
                continue
            try:
                value, next_pos = decoder.raw_decode(buffer, pos)
            except json.JSONDecodeError:
                require(not eof, "Malformed or truncated result JSON")
                block = stream.read(65536)
                buffer = buffer[pos:] + block
                pos = 0
                eof = not block
                continue
            pos = next_pos
            state = "DELIMITER"
            yield value


def fixture(output, database):
    require(database.replace("_", "").isalnum() and database[0].isalpha(), "Unsafe fixture database")
    path = output / "pressure-fixture.sql"
    with path.open("x") as stream:
        stream.write("CREATE DATABASE " + database + " WITH (max_data_region_group_num=1);\n")
        stream.write("USE " + database + ";\n")
        stream.write("CREATE TABLE fixture (device STRING TAG, s1 INT64 FIELD, s2 DOUBLE FIELD);\n")
        for start in range(0, ROWS, 1024):
            values = []
            for index in range(start, min(start + 1024, ROWS)):
                s1, s2 = row_at(index)
                values.append("(%d,'d%d',%d,%s)" % (
                    index // 4 + 1, index % 4, s1, "NULL" if s2 is None else str(s2)))
            stream.write("INSERT INTO fixture(time,device,s1,s2) VALUES " + ",".join(values) + ";\n")
        stream.write("FLUSH;\n")
    expected = external_sort((row_at(index) for index in range(ROWS)), output, "expected")
    expected.update(types=["INT64", "DOUBLE"], bucketRowCounts=[ROWS // 2, ROWS // 2] + [0] * 30,
                    emptyBucketCount=30, fixturePath=str(path), fixtureSha256=sha(path),
                    generator="i=0..1048575; device=i%4; time=i//4+1; s1=1+(i//4)%2; s2=null if i%97=0 else i+.5")
    save(output / "independent-expected.json", expected)
    return path, expected


def file_identity(command, output, label, timeout):
    save(output / (label + ".before.json"), {"command": command, "cwd": str(output),
         "python": sys.executable, "sysPath": sys.path,
         "java": str(Path(command[0]).resolve()), "javaSha256": sha(Path(command[0]).resolve())})
    with (output / (label + ".log")).open("x") as stream:
        result = subprocess.run(command, cwd=output, stdout=stream, stderr=subprocess.STDOUT,
                                timeout=timeout, check=False)
    save(output / (label + ".exit.json"), {"exitCode": result.returncode})
    require(result.returncode == 0, label + " failed; see preserved log")


def proc_identity(process, command):
    """Caller supplies its Popen object, never a PID read from a manifest or observer."""
    require(process.poll() is None, "Owned helper has already exited")
    proc = Path("/proc") / str(process.pid)
    args = proc.joinpath("cmdline").read_bytes().decode().split("\0")[:-1]
    require(args == command, "Owned helper actual command differs")
    require(JAVA_MAIN in args and "org.apache.iotdb.db.service.DataNode" not in args,
            "Refusing to signal a non-helper process")
    stat = proc.joinpath("stat").read_text()
    java = proc.joinpath("exe").resolve()
    require(java == Path(command[0]).resolve(), "Owned helper Java executable differs")
    return {"pid": process.pid, "startTicks": stat[stat.rfind(")") + 2:].split()[19],
            "java": str(java), "javaSha256": sha(java), "command": args}


def owned_signal(process, command, identity, sig):
    if process.poll() is not None:
        return False
    actual = proc_identity(process, command)
    require(actual == identity, "Owned helper PID/start/executable changed; refusing signal")
    os.kill(process.pid, sig)
    return True


def check_completed_small_acceptance(accepted, manifest):
    """A generic result/cleanup PASS cannot stand in for strict B4 mixed-locality PASS."""
    require(accepted.get("status") == "PASS", "Small-shape acceptance has not fully passed")
    require(accepted.get("acceptedHelperSourceSha256") == manifest["acceptedHelperSource"]["sha256"],
            "Complete small-shape receipt does not bind the accepted helper source SHA")
    for receipt_field, candidate_field in (
        ("candidateSourceManifest", "sourceManifest"), ("candidatePackage", "package")
    ):
        require(accepted.get(receipt_field) == manifest["candidate"][candidate_field],
                "Small-shape acceptance belongs to a different candidate " + candidate_field)
    require(isinstance(accepted.get("strictSourceB4Receipt"), dict),
            "Generic small-shape PASS is missing strict source B4 acceptance")
    strict = read(binding(accepted["strictSourceB4Receipt"]))
    require(strict.get("status") == "PASS", "Strict source B4 acceptance has not passed")
    expected = {"38": 32, "39": 1, "40": 125703}
    require(strict.get("observedNumbers") == expected, "Strict source B4 canonical counts differ")
    raw = strict.get("observedRawThrift", {})
    for key, tag in (("38", "i32"), ("39", "i32"), ("40", "i64")):
        require(isinstance(raw.get(key), dict) and type(raw[key].get(tag)) is int
                and raw[key][tag] == expected[key],
                "Strict source B4 original typed Thrift value differs: " + key)


def check_manifest(manifest):
    require(manifest["schemaVersion"] == 1 and manifest["rows"] == ROWS, "Unsupported pressure shape")
    require(manifest["bindingState"] == "BOUND_AFTER_COMPLETE_SMALL_HELPER_AND_CLEAN_A_PASS",
            "Pressure template is not bound to completed small-case acceptance")
    persistent = Path(manifest["persistentEvidenceRoot"])
    require(persistent.is_absolute() and not persistent.exists(), "Persistent evidence root must be absolute and new")
    helper_source = binding(manifest["acceptedHelperSource"])
    accepted = read(binding(manifest["completeSmallAcceptanceReceipt"]))
    check_completed_small_acceptance(accepted, manifest)
    clean = read(binding(manifest["cleanACorrectnessReceipt"]))
    require(clean["status"] == "PASS" and clean["observerAttached"] is False,
            "True clean frozen-A acceptance is missing or was observed")
    require(manifest["ownership"]["freshEnvironment"] is True,
            "Pressure must use an explicitly owned fresh service environment")
    owned = read(binding(manifest["ownership"]["receipt"]))
    require(owned["ownerRunId"] == manifest["ownerRunId"],
            "Service ownership receipt differs")
    require(manifest["candidate"]["baselineKind"] == "B_CANDIDATE"
            and manifest["control"]["baselineKind"] == "FROZEN_A", "Explicit A/B roles required")
    require(manifest["candidate"]["sourceManifest"]["sha256"]
            != manifest["control"]["sourceManifest"]["sha256"], "Control and candidate are the same source")
    records = {"helperSource": {"path": str(helper_source), "sha256": sha(helper_source)}}
    for role in ("control", "candidate"):
        side = manifest[role]
        java = binding(side["java"])
        require(java.is_file(), "Missing bound Java")
        bindings = [side["sourceManifest"], side["package"], side["sideManifest"], side["helperConfig"]]
        for item in bindings:
            binding(item)
        command = side["helperCommand"]
        require(command[0] == str(java) and command[-1] == JAVA_MAIN,
                "Helper command must end at the exact accepted Java main")
        require("-Xmx4g" in command and "-cp" in command, "Pressure helper requires an explicit 4 GiB heap/classpath")
        require("-DIOTDB_CONF=" + str(Path(side["helperConfig"]["path"]).parent) in command,
                "Helper config command binding differs")
        cp = command[command.index("-cp") + 1]
        require("*" not in cp and set(cp.split(os.pathsep))
                == {side["helperClasses"]} | {item["path"] for item in side["libraries"]},
                "Exact helper classpath/library binding differs")
        for item in side["libraries"]:
            binding(item)
        require(sha(Path(side["helperClasses"]) / (JAVA_MAIN + ".class")) == side["helperClassSha256"],
                "Compiled accepted helper class differs")
        for directory_key, list_key, prefix in (("helperClasses", "helperClassFiles", JAVA_MAIN),
                                                ("gateClasses", "gateClassFiles", GATE_MAIN)):
            classes = sorted(Path(side[directory_key]).rglob("*.class"))
            require(classes and {str(path.resolve()) for path in classes}
                    == {str(binding(item).resolve()) for item in side[list_key]},
                    "Complete compiled class-file set differs: " + directory_key)
            require(all(path.name.startswith(prefix) for path in classes),
                    "Unrelated class can shadow the bound helper/gate entry")
        require(side["compiledHelperSourceSha256"] == manifest["acceptedHelperSource"]["sha256"],
                "Compiled helper does not match fully passing accepted source")
        m = read(Path(side["sideManifest"]["path"]))
        require(m["requireNewMetrics"] == (role == "candidate") and m["baselineKind"] == side["baselineKind"],
                "Original helper resource protocol was bypassed")
        require(m["database"] == manifest["database"] and m["table"] == "fixture", "Wrong fixture binding")
        require(m["sourceManifest"] == side["sourceManifest"] and m["package"] == side["package"],
                "Helper side-manifest source/package differs from parent binding")
        server_library = next(item for item in side["libraries"]
                              if Path(item["path"]).name == "iotdb-server-2.0.11-SNAPSHOT.jar")
        require(server_library["sha256"] == m["serverJarSha256"]
                and Path(server_library["path"]).resolve().parent.parent == Path(m["distributionRoot"]).resolve(),
                "Actual helper server JAR/distribution identity differs")
        require(len(side["observers"]) == 2, "Exactly two bound observers required")
        observed = [read(binding(item)) for item in side["observers"]]
        require(len({node["pid"] for node in observed}) == 2, "Duplicate actual observer PID")
        for node in observed:
            require(node["serverJarSha256"] == server_library["sha256"]
                    and Path(node["iotdbHome"]).resolve() == Path(m["distributionRoot"]).resolve(),
                    "Observed DataNode package differs from helper's exact package")
            for path_field, sha_field in (("serverJar", "serverJarSha256"), ("observerJar", "observerJarSha256")):
                binding({"path": node[path_field], "sha256": node[sha_field]})
        gate = side["gateCommand"]
        require(gate[0] == str(java) and gate[-1] == GATE_MAIN, "Exact gate Java/main binding required")
        require(sha(Path(side["gateClasses"]) / (GATE_MAIN + ".class")) == side["gateClassSha256"],
                "Gate class differs")
        binding(side["gateSource"])
        require(side["gateSource"]["sha256"] == sha(ROOT / "StaticScanPressureGate.java"), "Wrong gate source")
        require(gate[gate.index("-cp") + 1] == side["gateClasses"] + os.pathsep + cp,
                "Gate exact classpath differs")
        records[role] = {"sideManifest": side["sideManifest"], "java": side["java"],
                         "sourceManifest": side["sourceManifest"], "package": side["package"],
                         "helperClassSha256": side["helperClassSha256"], "gateClassSha256": side["gateClassSha256"],
                         "libraries": side["libraries"], "observers": side["observers"]}
    cleanup = manifest["ownership"]["cleanup"]
    binding(cleanup["script"])
    require(cleanup["command"][:2] == ["python3", cleanup["script"]["path"]]
            and manifest["ownerRunId"] in cleanup["command"], "Cleanup must bind the exact owned run")
    owned_dn_pids = {item["pid"] for item in owned["processes"] if item["kind"] == "DataNode"}
    observed_pids = {read(Path(item["path"]))["pid"] for role in ("control", "candidate")
                     for item in manifest[role]["observers"]}
    require(len(owned_dn_pids) == len(observed_pids) == 4 and owned_dn_pids == observed_pids,
            "Observed DataNodes differ from the explicitly owned service environment")
    require(Path(cleanup["receiptPath"]).is_absolute() and not Path(cleanup["receiptPath"]).exists(),
            "Owned cleanup output must be an absolute new receipt path")
    return records


def persist_evidence(output, manifest):
    """Lossless complete evidence archive; validate every member without expanding it on disk."""
    persistent = Path(manifest["persistentEvidenceRoot"])
    persistent.mkdir()
    members = [{"path": str(path.relative_to(output)), "bytes": path.stat().st_size, "sha256": sha(path)}
               for path in sorted(output.rglob("*")) if path.is_file()]
    save(persistent / "archive-members.json", members)
    archive_path = persistent / "full-evidence.tar.gz"
    budget = manifest["persistentArchiveBudgetBytes"]

    class BoundedArchiveOutput:
        def __init__(self, stream):
            self.stream, self.bytes = stream, 0

        def write(self, data):
            require(self.bytes + len(data) <= budget, "Compressed evidence exceeded explicit archive budget")
            result = self.stream.write(data)
            self.bytes += result
            return result

        def flush(self):
            self.stream.flush()

    with archive_path.open("xb") as raw:
        with tarfile.open(fileobj=BoundedArchiveOutput(raw), mode="w|gz") as archive:
            for item in members:
                archive.add(output / item["path"], arcname=item["path"], recursive=False)
    actual = {}
    with tarfile.open(archive_path, mode="r|gz") as archive:
        for item in archive:
            require(item.isfile() and item.name not in actual, "Unexpected/duplicate archive member")
            stream = archive.extractfile(item)
            digest = hashlib.sha256()
            length = 0
            for block in iter(lambda: stream.read(1_048_576), b""):
                length += len(block)
                digest.update(block)
            actual[item.name] = (length, digest.hexdigest())
    require(actual == {item["path"]: (item["bytes"], item["sha256"]) for item in members},
            "Compressed complete evidence failed round-trip SHA verification")
    save(persistent / "archive-receipt.json", {"status": "PASS", "archive": str(archive_path),
         "archiveSha256": sha(archive_path), "compressedBytes": archive_path.stat().st_size,
         "originalBytes": sum(item["bytes"] for item in members), "members": len(members),
         "allMembersDecompressedAndShaVerified": True, "ramOriginalsPreserved": True,
         "pressureRuntimeStatus": read(output / "pressure-acceptance.json")["status"]})


def verify_execution(output, expected, candidate, side):
    """Independent complete result and original B/C terminal evidence; observer does not replace it."""
    load = lambda name: read(output / name)
    receipt, plan = load("receipt.json"), load("frozen-plan.json")
    require(receipt["status"] == "PASS", "Original helper did not fully pass")
    loaded = load("actual-loaded-code.json")
    for class_name, location in loaded.items():
        source = Path(unquote(urlparse(location).path)).resolve()
        if class_name == JAVA_MAIN:
            require(source == Path(side["helperClasses"]).resolve(), "Actual helper loaded from unexpected classes")
        else:
            server = next(item for item in side["libraries"]
                          if Path(item["path"]).name == "iotdb-server-2.0.11-SNAPSHOT.jar")
            require(source == Path(server["path"]).resolve(), "Actual production class loaded outside bound server JAR")
    require(JAVA_MAIN in loaded, "Helper did not record actual loaded code identity")
    require([plan[key] for key in ("sourceCount", "bucketCount", "hashChannelCount", "fragmentInstanceCount", "scanDop")]
            == [1, 32, 32, 34, 1] and plan["outputTypes"] == ["INT64", "DOUBLE"], "Wrong actual execution shape")
    frozen_ids = {item["fragmentInstanceId"] for item in plan["frozenFragments"]}
    require(len(plan["frozenFragments"]) == len(frozen_ids) == 34, "Missing/duplicate frozen FI")
    for item in plan["frozenFragments"]:
        binding(item)
    snapshots, buckets = load("terminal-snapshots.json"), load("bucket-results.json")
    require(len(snapshots) == 34 and {item["fragmentInstanceId"] for item in snapshots} == frozen_ids
            and all(item["state"] == "FINISHED" for item in snapshots), "A real FI did not finish")
    require(len({item["endpoint"] for item in snapshots}) == 2, "Actual execution did not use two DataNodes")
    require(len(buckets) == len(plan["buckets"]) == 32
            and {item["bucket"] for item in buckets} == set(range(32)), "Missing/duplicate bucket")
    by_bucket = {item["bucket"]: item for item in buckets}
    for key in ("fragmentInstanceId", "sourceExchangeId", "gatherExchangeId", "bucketSinkId"):
        require(len({item[key] for item in buckets}) == 32, "Actual bucket identity reused: " + key)
    for frozen in plan["buckets"]:
        actual = by_bucket[frozen["bucket"]]
        require(all(actual[key] == value for key, value in frozen.items()), "Actual/frozen bucket edge differs")
        require(actual["state"] == "FINISHED"
                and actual["outputRows"] == expected["bucketRowCounts"][actual["bucket"]], "Bucket routing/termination differs")
    actual = external_sort(stream_array(output / "result.json"), output, "independent-actual")
    require(actual["rows"] == expected["rows"] == receipt["rows"] == ROWS, "Full result row count differs")
    require(actual["sha256"] == expected["sha256"] == receipt["resultSha256"]
            == sha(output / "result.canonical.jsonl"), "Full typed multiset/SHA differs")
    with Path(actual["path"]).open("rb") as left, Path(expected["path"]).open("rb") as right:
        while True:
            a, b = left.read(1_048_576), right.read(1_048_576)
            require(a == b, "Complete canonical result bytes differ")
            if not a:
                break
    source = next(item for item in snapshots if item["fragmentInstanceId"] == plan["sourceFragmentInstanceId"])
    require(any(item["planNodeId"] == plan["sourceSinkId"] and "HashPartition" in item["operatorType"]
                for item in source["actualOperators"]), "Missing real production source hash sink")
    if candidate:
        require(source["plannedDop"] == source["actualPipelineCount"] == 1, "Source created extra drivers")
        require(receipt["resourceProtocol"] == "B_REQUIRED_AND_CHECKED", "Original B/C protocol skipped")
        resources = load("resource-terminal.json")
        require(len(resources) == 34 and {item["fragmentInstanceId"] for item in resources} == frozen_ids,
                "Missing candidate original per-FI resource receipt")
        for item in resources:
            require(item["complete"] and item["released"] and not item["fragmentMapPresent"], "Incomplete FI cleanup")
            require(all(item[key] == 0 for key in ("liveDrivers", "liveQueues", "unconsumedFactories", "liveEndpoints",
                "pendingFutures", "fragmentReservedBytes", "queryReservedBytes", "deregisterFailures")), "Nonzero final resource")
            deregister = item["deregisterReceipt"]
            require(deregister["success"] and deregister["bytesAfter"] == 0 and not deregister["presentAfter"],
                    "Missing zero deregistration receipt")
            require(all(obj["terminal"] for obj in item["objects"])
                    and item["createdDrivers"] == item["closedDrivers"], "Unclosed actual resource")
        producer = next(item for item in resources if item["fragmentInstanceId"] == plan["sourceFragmentInstanceId"])
        channels = [item for item in producer["objects"] if item["kind"] == "SINK_CHANNEL"]
        require(len(channels) == len({item["token"] for item in channels}) == 32, "Missing real 32 physical channels")
        require(len({(item["memoryQueryId"], item["memoryFragmentInstanceId"], item["memoryPlanNodeId"])
                     for item in channels}) == 1, "Source memory account split")
        require(producer["createdDrivers"] == producer["closedDrivers"] == 1, "Source driver count differs")
        before = {item["dataNodeId"]: item for item in load("resource-idle-before.json")}
        after = {item["dataNodeId"]: item for item in load("resource-idle-after.json")}
        require(len(before) == len(after) == 2 and before.keys() == after.keys(), "Missing two-node pool receipts")
        for node in before:
            require(all(before[node][key] == after[node][key]
                        for key in ("exchangeRemainingBytes", "operatorsFreeBytes")), "Original node pools did not restore")
    else:
        require(receipt["resourceProtocol"] == "UNAVAILABLE_IN_FROZEN_A", "A original B/C protocol gap hidden")
    return {"status": "PASS", "rows": ROWS, "fullMultisetSha256": actual["sha256"],
            "originalCandidateBCProtocolChecked": candidate, "originalAResourceProtocolAvailable": False,
            "frozenPlanSha256": sha(output / "frozen-plan.json")}


def one_side(side, role, root, fixture_path, expected):
    output = root / role
    output.mkdir()
    execution = output / "execution"
    helper_command = side["helperCommand"] + [side["sideManifest"]["path"], str(execution)]
    gate_manifest = {"sideManifest": side["sideManifest"], "observers": side["observers"],
                     "durationMillis": 60000, "frozenPlanPath": str(execution / "frozen-plan.json")}
    save(output / "gate-manifest.json", gate_manifest)
    file_identity(side["helperCommand"] + ["--fixture", side["sideManifest"]["path"],
                  str(fixture_path), str(output / "fixture-loaded.json")], output, "load-fixture", 900)
    require(read(output / "fixture-loaded.json")["fixtureSha256"] == sha(fixture_path), "Wrong loaded fixture")
    gate_command = side["gateCommand"] + [str(output / "gate-manifest.json"), str(output / "gate")]
    save(output / "processes.before.json", {"helperCommand": helper_command, "gateCommand": gate_command,
         "helperSourceSha256": side["compiledHelperSourceSha256"], "helperClassSha256": side["helperClassSha256"],
         "gateSource": side["gateSource"], "gateClassSha256": side["gateClassSha256"],
         "runtimeLibraries": side["libraries"], "sysPath": sys.path, "python": sys.executable})
    gate = helper = helper_identity = None
    stopped = False
    pressure = False
    failure = None
    cleanup_errors = []
    windows = []
    event_log = (output / "gate-events.jsonl").open("x")
    helper_log = (output / "helper.log").open("x")
    gate_error = (output / "gate.stderr.log").open("x")
    selector = selectors.DefaultSelector()
    try:
        gate = subprocess.Popen(gate_command, cwd=output, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=gate_error, text=True, bufsize=1)
        selector.register(gate.stdout, selectors.EVENT_READ)
        deadline = time.monotonic() + 65
        while not pressure and time.monotonic() < deadline:
            require(gate.poll() is None, "Pressure gate exited before confirmation")
            for key, _ in selector.select(0.05):
                line = key.fileobj.readline()
                require(line, "Gate event stream closed")
                event_log.write(line)
                event_log.flush()
                event = json.loads(line)
                if event["event"] == "WINDOWS_STARTED":
                    require(helper is None, "Gate started duplicate windows")
                    windows = event["detail"]["windows"]
                    helper = subprocess.Popen(helper_command, cwd=output, stdout=helper_log, stderr=subprocess.STDOUT)
                    # JVM exec may briefly expose Popen's pre-exec Python identity; wait for exact Java args.
                    identity_deadline = time.monotonic() + 2
                    while True:
                        try:
                            helper_identity = proc_identity(helper, helper_command)
                            break
                        except ValueError:
                            require(time.monotonic() < identity_deadline, "Owned helper failed exact identity binding")
                            time.sleep(0.002)
                    save(output / "owned-helper.identity.json", helper_identity)
                elif event["event"] == "SOURCE_RESERVED":
                    require(helper is not None and helper_identity is not None and not stopped,
                            "Positive source did not belong to this runner's live helper")
                    require(event["detail"]["reservedBytes"] > 0, "Source has no actual reservation")
                    require(owned_signal(helper, helper_command, helper_identity, signal.SIGSTOP), "Helper exited before hold")
                    stopped = True
                    stop_deadline = time.monotonic() + 1
                    while True:
                        require(helper.poll() is None, "Owned helper exited during SIGSTOP")
                        stat = (Path("/proc") / str(helper.pid) / "stat").read_text()
                        if stat[stat.rfind(")") + 2:].split()[0] == "T":
                            break
                        require(time.monotonic() < stop_deadline, "Owned helper never entered stopped state")
                        time.sleep(0.001)
                    save(output / "hold.json", {"signal": "SIGSTOP", "ownedHelper": helper_identity,
                         "sourceObservation": event["detail"], "monotonicNs": time.monotonic_ns()})
                    gate.stdin.write("HOLD_ACK\n")
                    gate.stdin.flush()
                elif event["event"] == "PRESSURE_CONFIRMED":
                    require(stopped, "Pressure was not observed under this owned helper hold")
                    pressure = True
                    owned_signal(helper, helper_command, helper_identity, signal.SIGCONT)
                    stopped = False
                    save(output / "resume.json", {"signal": "SIGCONT", "ownedHelper": helper_identity,
                                                 "monotonicNs": time.monotonic_ns()})
                else:
                    raise ValueError("Unknown gate event")
        require(pressure, "No real source32 pressure before bounded timeout")
        require(helper.wait(timeout=900) == 0, "Helper failed complete consumption after resume")
        require(gate.wait(timeout=30) == 0, "Gate cleanup failed")
        gate_receipt = read(output / "gate/gate-receipt.json")
        require(gate_receipt["status"] == "PASS", "Observer gate did not pass")
        result = verify_execution(execution, expected, role == "candidate", side)
        result["observedSource32Pressure"] = True
        result["bucketCaps"] = gate_receipt["secondBinding"]["bucketCaps"]
        save(output / "side-result.json", result)
        return result
    except BaseException as error:
        failure = error
        raise
    finally:
        # Resume before terminating/reaping our helper; NEVER signal any observed DataNode PID.
        if helper is not None and helper_identity is not None:
            try:
                if stopped:
                    owned_signal(helper, helper_command, helper_identity, signal.SIGCONT)
                    stopped = False
                if helper.poll() is None:
                    owned_signal(helper, helper_command, helper_identity, signal.SIGTERM)
                    try:
                        helper.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        owned_signal(helper, helper_command, helper_identity, signal.SIGKILL)
                        helper.wait(timeout=10)
            except BaseException as error:
                cleanup_errors.append("owned helper: " + repr(error))
        if gate is not None:
            try:
                if gate.poll() is None:
                    try:
                        gate.stdin.write("ABORT\n")
                        gate.stdin.flush()
                    except BrokenPipeError:
                        pass
                    try:
                        gate.wait(timeout=25)
                    except subprocess.TimeoutExpired:
                        gate.kill()
                        gate.wait(timeout=10)
                # Also stop every known owned window if gate finalization was incomplete.
                for index, window in enumerate(windows):
                    final_path = output / ("gate/observer-final-%d.json" % index)
                    if final_path.exists():
                        final = read(final_path)
                        if final.get("referencesReleased"):
                            continue
                    observer_binding = next(item for item in side["observers"]
                                            if read(Path(item["path"]))["pid"] == window["pid"])
                    observer = read(Path(observer_binding["path"]))
                    cp = observer["observerJar"] + os.pathsep + side["helperCommand"][side["helperCommand"].index("-cp") + 1]
                    file_identity([side["java"]["path"], "--add-modules", "jdk.attach", "-cp", cp,
                         "StaticScanReadOnlyObserver", "stop", observer_binding["path"], window["windowId"],
                         str(output / ("fallback-observer-final-%d.json" % index))], output,
                         "fallback-observer-stop-%d" % index, 30)
            except BaseException as error:
                cleanup_errors.append("observer/gate: " + repr(error))
        selector.close()
        event_log.close()
        helper_log.close()
        gate_error.close()
        save(output / "lifecycle-finally.json", {"failure": repr(failure) if failure else None,
             "helperReaped": helper is None or helper.poll() is not None, "helperStillStopped": stopped,
             "gateReaped": gate is None or gate.poll() is not None,
             "cleanupErrors": cleanup_errors, "helperSignalScope": "ONLY_THIS_RUNNERS_POPEN_HELPER"})
        require(not cleanup_errors, "Pressure runner cleanup failed: " + repr(cleanup_errors))


def run(manifest, output, records):
    require(not output.exists(), "Preserve old evidence: output must be new")
    filesystem = subprocess.check_output(["stat", "-f", "-c", "%T", str(output.parent)], text=True).strip()
    require(filesystem in ("tmpfs", "ramfs"), "Large pressure fixture/results must live on tmpfs/ramfs")
    require(shutil.disk_usage(output.parent).free >= manifest["ramMinimumFreeBytes"], "Insufficient explicit RAM work space")
    require(shutil.disk_usage(Path(manifest["persistentEvidenceRoot"]).parent).free
            >= manifest["persistentMinimumFreeBytes"], "Insufficient space for lossless persistent evidence")
    output.mkdir()
    save(output / "execution-before.json", {"manifest": manifest, "identity": records,
         "runnerPath": str(Path(__file__).resolve()), "runnerSha256": sha(__file__),
         "python": sys.executable, "sysPath": sys.path, "scope": "OBSERVED_RESOURCE_RUN_NOT_PERFORMANCE"})
    reports = {}
    failure = None
    cleanup_error = None
    archive_error = None
    try:
        fixture_path, expected = fixture(output, manifest["database"])
        for role in ("control", "candidate"):
            reports[role] = one_side(manifest[role], role, output, fixture_path, expected)
        a = {item["bucket"]: item for item in reports["control"]["bucketCaps"]}
        b = {item["bucket"]: item for item in reports["candidate"]["bucketCaps"]}
        require(a.keys() == b.keys() == set(range(32)), "Missing cap comparison bucket")
        for bucket in range(32):
            require(a[bucket]["localToProducer"] == b[bucket]["localToProducer"],
                    "A/B actual locality differs; cap comparison is non-comparable")
            require(a[bucket]["maxBytesCanReserve"] == b[bucket]["maxBytesCanReserve"],
                    "A/B actual per-channel cap differs for bucket " + str(bucket))
        require(reports["control"]["fullMultisetSha256"] == reports["candidate"]["fullMultisetSha256"],
                "A/B complete result SHA differs")
    except BaseException as error:
        failure = error
    finally:
        cleanup = manifest["ownership"]["cleanup"]
        try:
            binding(cleanup["script"])
            with (output / "owned-service-cleanup.log").open("x") as stream:
                result = subprocess.run(cleanup["command"], stdout=stream, stderr=subprocess.STDOUT,
                                        timeout=120, check=False)
            require(result.returncode == 0, "Owned service cleanup failed")
            receipt_path = Path(cleanup["receiptPath"])
            receipt = read(receipt_path)
            require(receipt["ownerRunId"] == manifest["ownerRunId"] and receipt["status"] == "PASS"
                    and receipt["allOwnedProcessesExited"] and receipt["allOwnedPortsClosed"]
                    and receipt["allOriginalConfigurationsRestored"], "Owned environment cleanup receipt incomplete")
            save(output / "owned-cleanup-receipt-binding.json", {"path": str(receipt_path), "sha256": sha(receipt_path)})
        except BaseException as error:
            cleanup_error = error
        runtime_status = "RUNTIME_PASS_PENDING_PERSISTENT_ARCHIVE" if not failure and not cleanup_error else "FAIL"
        save(output / "pressure-acceptance.json", {"status": runtime_status,
             "scope": "INDEPENDENT_OBSERVED_SOURCE32_RESOURCE_ACCEPTANCE; NOT PERFORMANCE", "sides": reports,
             "failure": repr(failure) if failure else None, "cleanupFailure": repr(cleanup_error) if cleanup_error else None,
             "originalAHasBCResourceProtocol": False, "candidateOriginalBCChecksRequired": True})
        try:
            persist_evidence(output, manifest)
        except BaseException as error:
            archive_error = error
            save(output / "archive-failure.json", {"status": "FAIL", "failure": repr(error),
                 "ramOriginalsPreserved": True, "persistentEvidenceRoot": manifest["persistentEvidenceRoot"]})
            # A runtime pass without lossless persistent evidence is not an overall acceptance.
            path = Path(manifest["persistentEvidenceRoot"])
            if path.exists():
                save(path / "archive-failure.json", {"status": "FAIL", "failure": repr(error),
                     "ramOriginals": str(output), "runtimeReceipt": str(output / "pressure-acceptance.json")})
        persistent = Path(manifest["persistentEvidenceRoot"])
        if persistent.exists():
            save(persistent / "acceptance-final.json", {"status": "PASS" if not failure and not cleanup_error and not archive_error else "FAIL",
                 "scope": "INDEPENDENT_OBSERVED_SOURCE32_RESOURCE_ACCEPTANCE; NOT PERFORMANCE",
                 "runtimeStatus": runtime_status, "archiveVerified": archive_error is None,
                 "runtimeReceiptSha256": sha(output / "pressure-acceptance.json"),
                 "archiveReceiptPath": str(persistent / "archive-receipt.json"),
                 "failure": repr(failure) if failure else None,
                 "cleanupFailure": repr(cleanup_error) if cleanup_error else None,
                 "archiveFailure": repr(archive_error) if archive_error else None})
    if failure:
        raise failure
    if cleanup_error:
        raise cleanup_error
    if archive_error:
        raise archive_error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check-binding", action="store_true")
    mode.add_argument("--run", action="store_true")
    args = parser.parse_args()
    manifest = read(args.manifest)
    records = check_manifest(manifest)
    if args.check_binding:
        save(args.output, {"status": "FILES_BOUND_ONLY_NOT_RUNTIME_ACCEPTED", "identity": records,
                           "servicesStarted": False, "observerAttached": False, "pressureExecuted": False})
    else:
        run(manifest, args.output.resolve(), records)


if __name__ == "__main__":
    main()
