# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Preserve the closeout observations and recheck every member of the portable evidence."""
import argparse
from collections import Counter
import fcntl
import hashlib
import io
import json
import math
from pathlib import Path
import struct
import subprocess
import tarfile

S = Path("/tmp/Dev_0")
R = Path("/root/rivermind-data/iotdb-paper-experiments-20261117")
WARM_RELEASE = "340f52e9696bb3a2ffb40a60f9e488c0c685e47a16674c7885ba09ef4eaedb62"
EXCLUDE = {"data", "tmp", "__pycache__"}
ORACLE_SHA = "766e7e9396105f11cd7b95f86ed01b401fc11788d8136db77c39898ab89b5106"
QUAL_PASS = "PARTITION_MAJOR_IDENTICAL_PHYSICAL_FIXTURE_AND_NATIVE_LAYOUT"
CONTRAST_PASS = "ACTUAL_WARM_STARTUP_CAUSAL_CONTRAST_VERIFIED"


def sha(path):
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def read(path):
    return json.loads(path.read_bytes())


def encode(record):
    return (json.dumps(record, ensure_ascii=False, indent=2) + "\n").encode("utf-8")


def write(path, value):
    with path.open("xb") as f:
        f.write(encode(value))


def warm_bag(path):
    def typed(r):
        timestamp, device, value, ordinal, present = json.loads(r)
        assert type(timestamp) is int and type(device) is str and type(ordinal) is int and type(present) is bool
        assert value is None or (type(value) in (int, float) and math.isfinite(value))
        return (timestamp, device, None if value is None else struct.pack(">d", value), ordinal, present)
    with path.open("rb") as stream:
        return Counter(map(typed, stream))


def warm_trial(path, required, oracle):
    p = path / "evidence/summary.json"
    if not p.is_file():
        return {"status": "NO_COMPLETED_TRIAL", "path": str(path)}
    d = read(p)
    assert d["releaseSha256"] == WARM_RELEASE
    arms = {}
    for arm, row in d["arms"].items():
        scans = row["queries"]
        counts = Counter()
        for key, x in scans.items():
            f = path / "evidence" / arm / ("contrast-" + key) / "ordinary-sql/result.rows.jsonl"
            rec = x["actualResultReceipt"]
            digest = sha(f)
            assert digest == rec["sha256"]
            assert rec["rows"] == oracle.total() == 740740
            assert warm_bag(f) == oracle
            counts[digest] += rec["rows"]
        q = scans["grown-first"]
        conf = path / arm / "cluster"
        logs = {}
        for node in ("datanode-1", "datanode-2"):
            log = conf / node / "logs/stdout.log"
            text = log.read_text(errors="replace")
            logs[node] = {"sha256": sha(log), "walFlushes": [l for l in text.splitlines() if
                          "WALNode" in l and "flushes memTable" in l],
                          "publisherWarnings": [l for l in text.splitlines() if "WARN" in l and
                          ("Failed to publish independent dynamic scan feedback" in l or
                           "dynamic snapshot" in l.lower() or "runtime scan history" in l.lower())]}
        snapshots = {n: [v for v in source["sameSourceHistoryLinks"]
                         if any(row0["queryId"] == v["queryId"] and
                                row0["sourceByDataNode"][n]["decision"]["action"] == "REBALANCE_NEXT_FRAGMENT"
                                for row0 in scans.values())] for n, source in q["sourceByDataNode"].items()}
        arms[arm] = {
            "rawDigestCounts": dict(counts), "typedRowsVerified": sum(counts.values()),
            "readBackOfRebalancedSameSourceSnapshots": snapshots,
            "physical": {k: {"addedFiles": len(v["rawAdded"]), "totalBytes": v["totalBytes"]} for k, v in row["physical"].items()},
            "nodes": q["actualColdEstimateAndStartup"], "logs": logs,
        }
    flags = {k: d[k] for k in ("noWalDrivenFlush", "marginAndNaturalWidthGate", "exactMatchedPhysicalInventory",
                               "matchedPredictedPartitionClassMatrix", "samePostGrowthLayout", "identicalInputProjection")}
    left, right = tuple(d["arms"].values())
    physical = all(left["physical"][n]["addedNormalized"] == right["physical"][n]["addedNormalized"] and
                   left["physical"][n]["totalBytes"] == right["physical"][n]["totalBytes"] for n in left["physical"])
    assert physical == flags["exactMatchedPhysicalInventory"]
    assert flags["noWalDrivenFlush"] == all(not l["walFlushes"] for a in arms.values() for l in a["logs"].values())
    accepted = d["status"] == required and all(flags.values())
    has_rejections = any(l["publisherWarnings"] for a in arms.values() for l in a["logs"].values())
    return {"status": d["status"], "path": str(path), "nativeSummarySha256": sha(p), "flags": flags,
            "matchedGatePass": accepted, "publisherWarningsObserved": has_rejections, "arms": arms,
            "scope": "Equal row values and matched inputs are required; no performance claim from this run."}


def enumerate_files(directory):
    for path in sorted(directory.rglob("*")):
        relative = path.relative_to(directory)
        if EXCLUDE.intersection(relative.parts) or path.is_symlink() or not path.is_file():
            continue
        assert path.resolve() == path
        yield path, relative


def verify(archive):
    with tarfile.open(archive, "r|gz") as tar:
        first = tar.next()
        assert first.isfile() and first.name == "evidence-manifest.json"
        payload = tar.extractfile(first).read()
        expected = json.loads(payload)["files"]
        seen = set()
        while (member := tar.next()) is not None:
            assert member.isfile() and member.name in expected and member.name not in seen
            row = expected[member.name]
            seen.add(member.name)
            assert member.size == row["bytes"] and hashlib.file_digest(tar.extractfile(member), "sha256").hexdigest() == row["sha256"], member.name
        assert set(expected) == seen
        return {"archiveSha256": sha(archive), "archiveBytes": archive.stat().st_size,
                "manifestSha256": hashlib.sha256(payload).hexdigest(), "membersVerified": len(seen)}


def seal(out):
    warm_driver = S / "sxing-iotdb-warm-partition-driver-20261120-03-start"
    selector_driver = S / "sxing-iotdb-selector-holdout-driver-20261120-01"
    selector_audit = S / "sxing-iotdb-selector-holdout-audit-20261120-01"
    assert all((p / "workflow.exit").is_file() for p in (warm_driver, selector_driver, selector_audit))
    qualification = S / "sxing-iotdb-warm-partition-qualification-20261120-03"
    contrast = S / "sxing-iotdb-warm-partition-contrast-20261120-03"
    oracledir = S / "sxing-iotdb-warm-matched-week-oracle-20261119-02"
    oraclefile = oracledir / "complete-typed-oracle.jsonl"
    assert sha(oraclefile) == ORACLE_SHA
    oracle = warm_bag(oraclefile)
    warm = {"oracleSha256": ORACLE_SHA, "qualification": warm_trial(qualification, QUAL_PASS, oracle),
            "contrast": warm_trial(contrast, CONTRAST_PASS, oracle)}
    write(out / "warm-adjudication.json", warm)
    root = S / "sxing-iotdb-warm-tie-publish-fix-20261119-01"
    release = root / "packaged-01/release.json"
    assert sha(release) == WARM_RELEASE
    build = read(release)
    src = Path(build["repository"])
    diff = subprocess.check_output(["git", "-C", str(src), "diff", "HEAD", "--full-index", "--binary"])
    assert hashlib.sha256(diff).hexdigest() == build["sourceDiffSha256"]
    with (out / "warm-candidate.patch").open("xb") as f:
        f.write(diff)
    entries = {"warm/warm-adjudication.json": out / "warm-adjudication.json",
               "candidate/warm-candidate.patch": out / "warm-candidate.patch",
               "candidate/release.json": release,
               "candidate/full-test-summary.json": root / "verify-full/summary.json",
               "candidate/scoped-test-summary.json": root / "verify-focused/summary.json",
               "candidate/full-tested-classes.json": root / "packaged-01/full-tested-classes.json"}
    assert sha(entries["candidate/full-test-summary.json"]) == build["reactorReceiptSha256"]
    for row in build["sourceFiles"]:
        p = src / row["path"]
        assert sha(p) == row["sha256"]
        entries["candidate/source/" + row["path"]] = p
    inputs = {
        "warm/oracle": oracledir,
        "warm/driver": warm_driver,
        "warm/preflight-storage-failure": S / "sxing-iotdb-warm-partition-driver-20261120-03",
        "warm/qualification-01": S / "sxing-iotdb-warm-partition-qualification-20261119-01",
        "warm/qualification-02": S / "sxing-iotdb-warm-partition-qualification-20261119-02",
        "warm/qualification-03": qualification,
        "warm/contrast": contrast,
        "london/frozen": S / "sxing-iotdb-selector-nonaliased-frozen-20261119-01",
        "london/calibration": S / "sxing-iotdb-london-delayed-selector-background-gate-20261119-01",
        "london/driver": selector_driver,
        "london/trials": S / "sxing-iotdb-london-delayed-selector-holdout-20261120-01",
        "london/audit": selector_audit,
    }
    for key, directory in inputs.items():
        if directory.is_dir():
            for path, rel in enumerate_files(directory):
                entries[key + "/" + rel.as_posix()] = path
    entries["seal/" + Path(__file__).name] = Path(__file__).resolve()
    files = {k: {"sha256": sha(p), "bytes": p.stat().st_size} for k, p in sorted(entries.items())}
    payload = encode({"scope": "Single sc01 closeout. Full available SQL results, receipts, logs, drivers and "
                     "verified changed sources. Excludes database state, build caches, released JARs and toolchain.",
                      "files": files})
    archive = out / "iotdb-closeout-experiment-evidence.tar.gz"
    with tarfile.open(archive, "x:gz", compresslevel=2, format=tarfile.PAX_FORMAT) as tar:
        m = tarfile.TarInfo("evidence-manifest.json")
        m.size = len(payload)
        tar.addfile(m, io.BytesIO(payload))
        for key, path in sorted(entries.items()):
            tar.add(path, arcname=key, recursive=False)
    record = verify(archive)
    record["warmAdjudicationSha256"] = sha(out / "warm-adjudication.json")
    p = selector_audit / "adjudication.json"
    record["londonAdjudicationSha256"] = sha(p) if p.is_file() else None
    write(out / "archive-receipt.json", record)
    print(json.dumps(record, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    app = argparse.ArgumentParser()
    group = app.add_mutually_exclusive_group(required=True)
    group.add_argument("--seal", type=Path)
    group.add_argument("--verify", type=Path)
    group.add_argument("--warm-replay", type=Path)
    opt = app.parse_args()
    if opt.verify:
        print(json.dumps(verify(opt.verify), ensure_ascii=False))
    elif opt.warm_replay:
        root = opt.warm_replay.resolve()
        oraclefile = root / "oracle/complete-typed-oracle.jsonl"
        assert sha(oraclefile) == ORACLE_SHA
        oracle = warm_bag(oraclefile)
        print(json.dumps({"oracleSha256": ORACLE_SHA,
                          "qualification": warm_trial(root / "qualification-03", QUAL_PASS, oracle),
                          "contrast": warm_trial(root / "contrast", CONTRAST_PASS, oracle)}, ensure_ascii=False, indent=2))
    else:
        with (R / "shared-pressure-feedback-candidate-01/local-timed-experiments.lock").open("rb") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            seal(opt.seal.resolve())
