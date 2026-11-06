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

"""Compile/run the same frozen-graph driver on an explicit A baseline and B candidate.

Deployments must already be running in isolated directories. This entry never starts/stops a
DataNode, changes node configuration, substitutes an operator, or copies a distribution.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from urllib.parse import urlparse, unquote
from pathlib import Path

ROOT = Path(__file__).resolve().parent
JAVA = Path("/usr/lib/jvm/java-17-openjdk-amd64/bin/java")
JAVAC = JAVA.with_name("javac")
REPOSITORY = Path("/root/iotdb")


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1048576), b""):
            h.update(block)
    return h.hexdigest()


def save(path, value):
    require(not path.exists(), "Evidence would be overwritten: " + str(path))
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")


def properties(path):
    values = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def invoke(command, output, label, cwd, timeout=240):
    before = {"command": [str(arg) for arg in command], "cwd": str(cwd),
              "java": str(JAVA.resolve()), "javaSha256": sha(JAVA.resolve()),
              "javac": str(JAVAC.resolve()), "javacSha256": sha(JAVAC.resolve()),
              "javaVersion": subprocess.run([str(JAVA), "-version"], text=True, capture_output=True).stderr,
              "python": sys.executable, "importPath": sys.path}
    save(output / (label + ".before.json"), before)
    with (output / (label + ".log")).open("w") as stream:
        result = subprocess.run(command, cwd=cwd, stdout=stream, stderr=subprocess.STDOUT,
                                timeout=timeout, check=False)
    save(output / (label + ".exit.json"), {"exitCode": result.returncode})
    require(result.returncode == 0, label + " failed; retained log: " + str(output / (label + ".log")))


def identity(side, role, output, live):
    require(side["baselineKind"] == ("B_CANDIDATE" if role == "candidate" else "FROZEN_A"),
            "Explicit frozen A / B identity is required")
    for item in (side["sourceManifest"], side["package"]):
        require(Path(item["path"]).is_absolute() and Path(item["path"]).is_file(), "Missing absolute binding")
        require(sha(item["path"]) == item["sha256"], "Frozen input identity differs: " + item["path"])
    dist = Path(side["distributionRoot"])
    require(dist.is_absolute() and dist.is_dir(), "Missing explicit distribution")
    jars = sorted((dist / "lib").glob("*.jar"))
    require(jars, "Distribution has no runtime libraries")
    server = dist / "lib/iotdb-server-2.0.11-SNAPSHOT.jar"
    require(sha(server) == side["serverJarSha256"], "Actual distribution server JAR differs")
    require(len(side["nodes"]) == 2, "Two explicit DataNode records are required")
    records, internal_ports = [], set()
    for index, node in enumerate(side["nodes"]):
        config = Path(node["config"]).resolve()
        values = properties(config)
        for key, expected in side["expectedFlags"].items():
            require(values.get(key) == str(expected), "Configuration differs: " + str(config) + " " + key)
        require(values.get("degree_of_query_parallelism") == "1", "Both roles must configure scan DOP=1")
        require(values["dn_internal_address"] == "127.0.0.1", "Acceptance requires isolated loopback endpoints")
        internal_ports.add(int(values["dn_internal_port"]))
        copy = output / ("node-%d.properties" % index)
        shutil.copy2(config, copy)
        record = {"config": str(config), "configSha256": sha(config), "mode": config.stat().st_mode,
                  "flags": values, "copy": str(copy)}
        if live:
            pid = int(node["pid"])
            cmdline = (Path("/proc") / str(pid) / "cmdline").read_bytes().replace(b"\0", b" ").decode()
            require("org.apache.iotdb.db.service.DataNode" in cmdline, "PID is not a real DataNode")
            require(str(config.parent) in cmdline, "Actual DataNode process is not bound to the named config")
            require(str(dist) in cmdline, "Actual DataNode process is not bound to the named distribution")
            record.update(pid=pid, cmdline=cmdline,
                          maps=(Path("/proc") / str(pid) / "maps").read_text())
        records.append(record)
    require(len(internal_ports) == 2, "Duplicate DataNode internal endpoints")
    helper = output / "helper-conf"
    helper.mkdir()
    first = properties(side["nodes"][0]["config"])
    for key in ("collectorPort", "helperInternalPort"):
        require(side[key] not in internal_ports, "Helper port overlaps a DataNode")
        if live:
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", side[key]))
    require(side["collectorPort"] != side["helperInternalPort"], "Helper endpoints overlap")
    helper_text = "\n".join([
        "dn_internal_address=127.0.0.1",
        "dn_internal_port=" + str(side["helperInternalPort"]),
        "dn_mpp_data_exchange_port=" + str(side["collectorPort"]),
        "dn_rpc_address=127.0.0.1", "dn_rpc_port=" + str(side["helperInternalPort"] + 1),
        "degree_of_query_parallelism=1",
        "enable_property_driven_planning=" + first.get("enable_property_driven_planning", "false"),
        "enable_table_group_by_hash_repartition=false",
        "dn_seed_config_node=" + ",".join(n["host"] + ":" + str(n["port"]) for n in side["configNodes"]),
        "dn_system_dir=" + str(output / "helper-data/system"),
        "dn_data_dirs=" + str(output / "helper-data/data"),
        "dn_wal_dirs=" + str(output / "helper-data/wal"),
        "dn_consensus_dir=" + str(output / "helper-data/consensus"),
    ]) + "\n"
    (helper / "iotdb-system.properties").write_text(helper_text)
    command_manifest = dict(side, requireNewMetrics=role == "candidate", database="static_scan32",
                            table="fixture", firstDataNodeInternalPort=int(first["dn_internal_port"]),
                            queryId="ss32_%d_%s_0" % (time.time_ns(), role))
    save(output / "side-manifest.json", command_manifest)
    record = {"role": role, "baselineKind": side["baselineKind"], "sourceManifest": side["sourceManifest"],
              "package": side["package"], "distributionRoot": str(dist),
              "serverJarSha256": sha(server), "nodes": records,
              "driver": {"path": str(ROOT / "StaticScanAcceptanceDriver.java"),
                         "sha256": sha(ROOT / "StaticScanAcceptanceDriver.java")},
              "libraries": [{"path": str(p), "sha256": sha(p)} for p in jars],
              "helperConfig": {"path": str(helper / "iotdb-system.properties"),
                               "sha256": sha(helper / "iotdb-system.properties")}}
    save(output / "execution-identity.json", record)
    return helper, jars


def run(args):
    manifest = json.loads(args.manifest.read_text())
    require(manifest["schemaVersion"] == 1, "Unsupported manifest schema")
    require(not args.output.exists(), "Output must be a new directory; previous evidence is preserved")
    candidate, control = manifest["candidate"], manifest["control"]
    require(candidate["sourceManifest"]["sha256"] != control["sourceManifest"]["sha256"],
            "Same-source DOP=1 comparison cannot be labeled frozen A vs B")
    require(candidate["serverJarSha256"] != control["serverJarSha256"], "A/B server identities must differ")
    require(candidate["jdbcUrl"] != control["jdbcUrl"], "Candidate/control deployments must be isolated")
    args.output.mkdir(parents=True)
    frozen = args.output / "frozen-inputs"
    frozen.mkdir()
    tool_inputs = []
    for name in ("run-two-datanode.py", "verify-runtime.py", "StaticScanAcceptanceDriver.java",
                 "fixture.sql", "expected-results.json"):
        original, snapshot = ROOT / name, frozen / name
        shutil.copy2(original, snapshot)
        require(sha(original) == sha(snapshot), "Input changed while freezing: " + name)
        tool_inputs.append({"original": str(original), "frozen": str(snapshot), "sha256": sha(snapshot)})
    save(args.output / "frozen-inputs.json", tool_inputs)
    save(args.output / "manifest.json", manifest)
    save(args.output / "before.json", {
        "repository": str(REPOSITORY),
        "branch": subprocess.check_output(["git", "-C", str(REPOSITORY), "branch", "--show-current"], text=True).strip(),
        "head": subprocess.check_output(["git", "-C", str(REPOSITORY), "rev-parse", "HEAD"], text=True).strip(),
        "entry": str(Path(__file__).resolve()), "entrySha256": sha(__file__),
        "manifest": str(args.manifest.resolve()), "manifestSha256": sha(args.manifest),
        "fixtureSha256": sha(ROOT / "fixture.sql"),
        "expectedSha256": sha(ROOT / "expected-results.json"),
        "compileOnly": args.compile_only,
    })
    reports = {}
    spec = importlib.util.spec_from_file_location("static_scan32_runtime_verifier", frozen / "verify-runtime.py")
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    expected = json.loads((frozen / "expected-results.json").read_text())
    for role in ("control", "candidate"):
        side = manifest[role]
        output = args.output / role
        output.mkdir()
        helper, jars = identity(side, role, output, not args.compile_only)
        classes = output / "classes"
        classes.mkdir()
        cp = os.pathsep.join(str(p) for p in jars)
        invoke([str(JAVAC), "-encoding", "UTF-8", "-cp", cp, "-d", str(classes),
                str(frozen / "StaticScanAcceptanceDriver.java")], output, "compile-driver", output)
        compiled = classes / "StaticScanAcceptanceDriver.class"
        save(output / "compiled-driver.json", {"path": str(compiled), "sha256": sha(compiled),
                                              "sourceSha256": sha(ROOT / "StaticScanAcceptanceDriver.java")})
        if args.compile_only:
            reports[role] = {"status": "COMPILED_ONLY", "runtimeAccepted": False}
            continue
        helper_home = output / "helper-home"
        helper_home.mkdir()
        java_command = [str(JAVA), "--add-opens=java.base/java.lang=ALL-UNNAMED",
                        "-Xms128m", "-Xmx512m", "-DIOTDB_CONF=" + str(helper),
                        "-DIOTDB_HOME=" + str(helper_home),
                        "-cp", str(classes) + os.pathsep + cp, "StaticScanAcceptanceDriver"]
        side_manifest = output / "side-manifest.json"
        invoke([*java_command, "--fixture", str(side_manifest), str(frozen / "fixture.sql"),
                str(output / "fixture-loaded.json")], output, "load-fixture", output)
        invoke([*java_command, str(side_manifest), str(output / "execution")], output, "execute-frozen-case", output)
        helper_loaded = json.loads((output / "execution/actual-loaded-code.json").read_text())
        helper_server = (Path(side["distributionRoot"]) / "lib/iotdb-server-2.0.11-SNAPSHOT.jar").resolve()
        for class_name, source_url in helper_loaded.items():
            if class_name == "StaticScanAcceptanceDriver":
                continue
            parsed = urlparse(source_url)
            require(parsed.scheme == "file" and Path(unquote(parsed.path)).resolve() == helper_server,
                    "Standalone planner loaded a different server JAR: " + class_name)
        loaded = []
        for node in side["nodes"]:
            log = Path(node["classLoadLog"])
            text = log.read_text()
            expected_source = (Path(side["distributionRoot"]) / "lib/iotdb-server-2.0.11-SNAPSHOT.jar").resolve()
            lines = [line for line in text.splitlines() if
                     "org.apache.iotdb.db.queryengine.plan.planner.DataNodeTableOperatorGenerator source:" in line]
            require(lines, "No actual server operator loading evidence")
            for line in lines:
                source_url = line.split(" source:", 1)[1].strip().split()[0]
                parsed = urlparse(source_url)
                require(parsed.scheme == "file" and Path(unquote(parsed.path)).resolve() == expected_source,
                        "Actual server operator JAR loading differs")
            loaded.append({"pid": node["pid"], "log": str(log), "sha256": sha(log), "loadedOperatorLines": lines})
        save(output / "actual-server-loaded-code.json", loaded)
        reports[role] = verifier.verify(output / "execution", expected, role == "candidate")
    if not args.compile_only:
        require(reports["candidate"]["sha256"] == reports["control"]["sha256"], "Complete A/B result differs")
    save(args.output / "acceptance.json", {"status": "COMPILED_ONLY" if args.compile_only else "PASS",
         "scope": "frozen A and B candidate, identical single-source 32-bucket production graph",
         "roles": reports, "scanDop": 1, "logicalBuckets": 32,
         "performanceClaim": False, "serviceLifecycleOwnedByExternalOrchestrator": True})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--compile-only", action="store_true")
    args = parser.parse_args()
    try:
        run(args)
    except Exception as failure:
        if args.output.exists() and not (args.output / "failure.json").exists():
            save(args.output / "failure.json", {"status": "FAIL", "error": repr(failure)})
        raise


if __name__ == "__main__":
    main()
