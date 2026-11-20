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

"""Replay the eight assigned London cells from raw rows, events and receipts."""
import argparse
from contextlib import nullcontext
import fcntl
import hashlib
import importlib.util
import json
from pathlib import Path
import statistics
import traceback

ROOT = Path("/tmp/Dev_0")
DURABLE = Path("/root/rivermind-data/iotdb-paper-experiments-20261117")
N = 10**9
GOOD = "VERIFIED_DELAYED_SELECTOR_TRIAL"
PINS = {
    "new-arrival-selector-prereg.json": "63e60bfd23ab5a8b1cfbc2b0f236bef57a8e270a468ab7b9857d320afef22f9d",
    "selector-freeze.json": "d8d5e3e952036e568779bbb15805bcb189ab03b8611c5320b838a3f8f5983e5e",
    "background-only-gate.json": "38ae4f28e0c3c84201b13df41de6132553154bc3ac6c36411b0f02999e9c8da6",
    "london_scalar_two_pulse_prereg_20261119.json": "d0b7b2080720b092565ffc8586cd5ebfbbe6049e024ea47be3c6a4d31e39a7ac",
    "adjudicate_london_scalar_two_pulse_20261119.py": "12ce189ede8bed26d33978842387f318185c622f44c4316997914f4d42b038e6",
    "adjudicate_london_two_pulse_matched_feedback_wire_v2_20261119.py": "b88fa61dfb2cd2b7b4f0e64891486951757b338bf3d3d4e6a7774e74c7d6e1e5",
    "london_early_wave_phase_proof_20261119.py": "469611e4e48a04562041fe43485517ab633e66185c38970d3140fa271b83b59b",
    "replay_london_quarter_kwh_histogram.py": "e0dcda40d5b0cc024ea507b82079c5bc77ba8fbc40c0e4b15e60eb8d936b7d36",
    "oracle/manifest.json": "5e388512277c84dd67506ae64a09bddf1c4aaebf9423cf5242346b422661eaac",
    "foreground-oracle.jsonl": "9a7fd84ff1db22d3aa5b46d0671ac9ecdfd54bc665060941b5640a599c9e6801",
}


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def read(path):
    return json.loads(path.read_bytes())


def load(path, name):
    assert sha(path) == PINS[path.name], path
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert Path(module.__file__).resolve() == path
    return module


def cell_audit(leaf, declaration, arm, limits, exact, wire, bg, plan, frozen_oracle):
    raw = read(leaf / "trial-receipt.json")
    assert sha(leaf / "trial-receipt.json") == declaration["receiptSha256"]
    assert raw["status"] == declaration["status"]
    assignment = read(leaf / "assignment.json")
    assert raw["profile"] == declaration["profile"] == assignment["profile"] == arm["profile"]
    if raw["status"] != GOOD:
        if "failureSha256" in raw:
            assert sha(leaf / "runner-failure.txt") == raw["failureSha256"]
        return {"status": raw["status"], "profile": arm["profile"], "nativeReceiptSha256": declaration["receiptSha256"]}
    width, on = arm["actualStartupWorkersPerSource"], arm["sharedPressureFeedback"]
    run = leaf / "loaded"
    loaded = read(run / "summary.json")
    assert all(raw[k] == v for k, v in loaded.items())
    assert raw["configuredStartup"] == width and raw["actualConfiguredFeedback"] is on
    mf = read(leaf / "effective.json")
    assert mf["runtimeProfile"]["initialWorkers"] == width
    assert mf["runtimeProfile"]["sharedPressureFeedback"] is on
    for node in mf["nodes"]:
        original = Path(node["configPath"])
        orig_leaf = Path(node["nodeRoot"]).parents[1]
        assert orig_leaf.name == leaf.name
        cfg = leaf / original.relative_to(orig_leaf)
        assert sha(cfg) == node["configSha256"]
        lines = cfg.read_text().splitlines()
        for k, v in {
            "runtime_scan_parallelism_fixed_workers": "0",
            "runtime_scan_parallelism_initial_workers": str(width),
            "enable_runtime_scan_shared_pressure_feedback": str(on).lower(),
        }.items():
            assert [s for s in lines if s.startswith(k + "=")] == [k + "=" + v]
    fg = run / "foreground/ordinary-sql"
    foreground = read(fg / "result-receipt.json")
    rows = fg / "result.rows.jsonl"
    digest = sha(rows)
    assert foreground["rows"] == 2898043 and foreground["fullResultDrained"] and foreground["sha256"] == digest
    assert digest == raw["foregroundProof"]["resultSha256"]
    # A canonical raw digest was already checked against the full typed independent oracle
    # in the previous London experiments. For any new layout of the same rows, compare typed values.
    if digest != "696f8ee269a19070a7ca882832bbfd09f1be3da0e1e0e170398b098f56a0d920":
        assert exact.typed_equal(frozen_oracle, rows) == (2898043, 167932474)
    query = exact.recompute(fg)
    assert query["startupByNode"] == {1: width, 2: width} and sum(query["sourceRowsByNode"].values()) == 167932474
    assert query["verifiedTopology"] == {"sources": 2, "buckets": 2, "edges": 4}
    t = read(run / "origin.json")
    start = t["originMonotonicNanos"]
    count, failures, last, tail = bg.validate_background(run, plan, start, 4)
    assert sum(count.values()) == 120 and len(failures) == count["failed"]
    assert (count["completed"], count["rejected"], count["failed"], tail) == (
        raw["backgroundCompleted"], raw["backgroundRejected"], raw["backgroundErrors"], raw["backgroundP95Seconds"],
    )
    good = count["completed"] == 120 and last <= start + 26_500_000_000 and tail <= limits["gateP95Seconds"]
    assert good == raw["meetsStandaloneGate"] == declaration["gateMet"]
    p = wire.phase
    phases = p.verify(fg, t, last, width, on)
    assert json.loads(json.dumps(phases)) == raw["observedPressureAndWorkers"]
    live = {}
    for e in p.lines(fg / "runtime-live-timeline.jsonl"):
        got = p.live(e, width)
        if got:
            live.setdefault(got[1], set()).add(got[2] - start)
    overlap = {n: (any(6.6*N <= x <= 8.1*N for x in ticks) and
                   any(8.1*N <= x <= 9.6*N for x in ticks)) for n, ticks in live.items()}
    overlaps_both = overlap == {1: True, 2: True}
    qualified = phases["fullPhaseCoverage"] and count["completed"] == 120 and query["commonActiveSeconds"] >= 37
    assert qualified == raw["qualifiedCoverageAndCorrectness"]
    actuation = wire.measure(fg, {**raw, "status": "VERIFIED_MATCHED_FEEDBACK_TRIAL"})
    return {
        "status": GOOD, "profile": arm["profile"], "actualStartupByNode": query["startupByNode"],
        "foregroundRows": foreground["rows"], "foregroundSeconds": foreground["elapsedNanos"]/N,
        "foregroundSha256": digest, "backgroundCompleted": count["completed"],
        "backgroundRejected": count["rejected"], "backgroundErrors": count["failed"],
        "backgroundP95Seconds": tail, "lastBackgroundDrainOffsetSeconds": (last-start)/N if last else None,
        "gatePass": good, "commonActiveSeconds": query["commonActiveSeconds"],
        "phaseCoverage": phases["fullPhaseCoverage"], "twoSourcesObservedAcrossSecondPulse": overlaps_both,
        "qualifiedCoverageAndCorrectness": bool(qualified and overlaps_both),
        "actuation": actuation, "nativeReceiptSha256": declaration["receiptSha256"],
    }


def audit(source, assets):
    assert all(sha(assets / name) == h for name, h in PINS.items())
    native = read(source / "summary.json")
    assert native["preexecutionSha256"] == sha(source / "preexecution.json")
    for key, file in (("selectorFreezeSha256", "selector-freeze.json"), ("arrivalPlanSha256", "new-arrival-selector-prereg.json"),
                      ("thresholdSha256", "background-only-gate.json")):
        assert native[key] == PINS[file]
    freeze = read(assets / "selector-freeze.json")
    arrival = read(assets / "new-arrival-selector-prereg.json")
    gate = read(assets / "background-only-gate.json")
    assert native["p95GateSeconds"] == gate["gateP95Seconds"]
    exact = load(assets / "replay_london_quarter_kwh_histogram.py", "independent_london_foreground")
    wire = load(assets / "adjudicate_london_two_pulse_matched_feedback_wire_v2_20261119.py", "independent_london_wire")
    bg = load(assets / "adjudicate_london_scalar_two_pulse_20261119.py", "independent_london_background")
    bg.T, bg.ORACLE = assets, assets / "oracle/manifest.json"
    plan = read(assets / "london_scalar_two_pulse_prereg_20261119.json")
    plan["protocol"]["arrivalOffsetsNanosByClient"] = arrival["offeredTimesNanosByClient"]
    pairs = {}
    for i, order in enumerate(arrival["blockOrders"], 1):
        block = f"pair-{i:02d}"
        declared = native["blocks"][block]
        assert list(declared) == order
        entries = {}
        for position, which in enumerate(order, 1):
            arm = freeze["candidateRows"][which]
            e = declared[which]
            leaf = source / block / f"{position:02d}-{which}"
            assert leaf.name == e["role"]
            try:
                value = cell_audit(leaf, e, arm, gate, exact, wire, bg, plan, assets / "foreground-oracle.jsonl")
            except Exception:
                value = {"status": "INDEPENDENT_AUDIT_REJECTED", "profile": arm["profile"], "auditTrace": traceback.format_exc()}
            entries[which] = value
        j, s = entries["joint"], entries["sequential"]
        complete = all(x["status"] == GOOD and x["backgroundCompleted"] == 120 for x in (j, s))
        qualified = complete and all(x["qualifiedCoverageAndCorrectness"] for x in (j, s))
        if complete:
            assert j["actuation"]["queryForegroundLayoutSha256"] == s["actuation"]["queryForegroundLayoutSha256"]
        pairs[block] = {"arms": entries, "bothComplete": complete, "bothQualified": qualified,
                        "jointMinusSequentialForCompletePair":
                            {k: j[k]-s[k] for k in ("foregroundSeconds", "backgroundP95Seconds")} if complete else None}
    assert len(native["blocks"]) == len(pairs) == 4
    arms = {a: [row["arms"][a] for row in pairs.values()] for a in ("joint", "sequential")}
    qualified = sum(r["bothQualified"] for r in pairs.values())
    deltas = [r["jointMinusSequentialForCompletePair"] for r in pairs.values() if r["bothComplete"]]
    return {
        "status": "LONDON_FOUR_FROZEN_PAIRS_INDEPENDENTLY_RECOMPUTED",
        "nativeSummarySha256": sha(source / "summary.json"), "p95GateSeconds": gate["gateP95Seconds"],
        "backgroundGatePassBySelector": {a: sum(r.get("gatePass", False) for r in rows) for a, rows in arms.items()},
        "completePairs": len(deltas), "qualifiedPairs": qualified,
        "medianJointMinusSequentialForCompletePairs":
            {k: statistics.median(d[k] for d in deltas) for k in deltas[0]} if deltas else None,
        "pairs": pairs,
        "scope": "One delayed-burst real London workload on two DataNodes sharing one host. Width and feedback differ. "
                 "Every pair is shown, including failures. A tail-threshold pass is not a production SLO.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("assets", type=Path)
    parser.add_argument("output", type=Path)
    options = parser.parse_args()
    lock_path = DURABLE / "shared-pressure-feedback-candidate-01/local-timed-experiments.lock"
    with (lock_path.open("rb") if lock_path.is_file() else nullcontext()) as lock:
        if lock is not None:
            fcntl.flock(lock, fcntl.LOCK_EX)
        result = audit(options.source.resolve(), options.assets.resolve())
        with options.output.open("x") as stream:
            json.dump(result, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
        print(sha(options.output), result["status"], result["completePairs"], result["qualifiedPairs"])
