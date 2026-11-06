#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Validate property contracts against explicit JUnit assertion polarity."""

import argparse
import copy
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

EXPECTED_OPERATORS = {
    "scan", "filter", "project", "sort", "top-k", "window", "row-number",
    "aggregation", "join", "union", "collect", "merge-sort", "output",
}
REQUIRED_FIELDS = {
    "id", "sql", "required", "provided", "enforcer", "scan_parallelism",
    "forbidden_parallelism_reason", "plan_test", "fallback_reason", "execution_marker",
}
VALID_PARALLELISM = {"allowed", "forbidden", "conditional"}
HELPERS = {
    ("PropertyDrivenDistributionTest.java", "assertAllScansForbidParallelism"):
        (False, "scan.isAllowParallelScan()"),
}
DISTRIBUTE = Path(
    "iotdb-core/datanode/src/main/java/org/apache/iotdb/db/queryengine/"
    "plan/relational/planner/distribute"
)


class ContractError(ValueError):
    pass


@dataclass(frozen=True)
class Token:
    text: str
    kind: str
    start: int
    end: int


@lru_cache(maxsize=256)
def java_tokens(source):
    """Keep literals atomic and discard comments before examining Java structure."""
    if re.search(r"\\u+[0-9a-fA-F]{4}", source):
        raise ContractError("unsupported Java Unicode escape in linked source")
    tokens = []
    i = 0
    while i < len(source):
        c = source[i]
        if c.isspace():
            i += 1
            continue
        if source.startswith("//", i):
            end = source.find("\n", i + 2)
            i = len(source) if end < 0 else end + 1
            continue
        if source.startswith("/*", i):
            end = source.find("*/", i + 2)
            if end < 0:
                raise ContractError("unterminated Java block comment")
            i = end + 2
            continue
        start = i
        if c in "\"'":
            text_block = source.startswith('"""', i)
            delimiter = '"""' if text_block else c
            i += len(delimiter)
            while i < len(source):
                if source[i] == "\\":
                    i += 2
                elif source.startswith(delimiter, i):
                    i += len(delimiter)
                    break
                elif source[i] in "\r\n" and not text_block:
                    raise ContractError("newline in a Java string/character literal")
                else:
                    i += 1
            else:
                raise ContractError("unterminated Java literal")
            tokens.append(Token(source[start:i], "literal", start, i))
            continue
        match = re.match(r"[A-Za-z_$][A-Za-z0-9_$]*|[0-9]+", source[i:])
        if match:
            i += len(match.group(0))
            tokens.append(Token(source[start:i], "word", start, i))
        else:
            i += 1
            tokens.append(Token(c, "symbol", start, i))
    return tuple(tokens)


def closing(tokens, opening):
    pairs = {"(": ")", "[": "]", "{": "}"}
    if tokens[opening].text not in pairs:
        raise ContractError("expected opening delimiter")
    stack = []
    for index in range(opening, len(tokens)):
        text = tokens[index].text
        if text in pairs:
            stack.append(pairs[text])
        elif text in pairs.values():
            if not stack or stack.pop() != text:
                raise ContractError("unbalanced Java delimiters")
            if not stack:
                return index
    raise ContractError("unterminated Java delimiter")


def canonical(tokens):
    tokens = list(tokens)
    while tokens and tokens[0].text == "(" and closing(tokens, 0) == len(tokens) - 1:
        tokens = tokens[1:-1]
    return "".join(token.text for token in tokens)


@lru_cache(maxsize=128)
def method_info(source, method_name, require_test=False):
    tokens = java_tokens(source)
    matches = []
    for i in range(len(tokens) - 2):
        if tokens[i].text != "void" or tokens[i + 1].text != method_name or tokens[i + 2].text != "(":
            continue
        end_parameters = closing(tokens, i + 2)
        opening = end_parameters + 1
        while opening < len(tokens) and tokens[opening].text not in {"{", ";"}:
            opening += 1
        if opening == len(tokens) or tokens[opening].text != "{":
            continue
        end = closing(tokens, opening)
        boundary = i - 1
        while boundary >= 0 and tokens[boundary].text not in {"{", "}", ";"}:
            boundary -= 1
        prefix = "".join(token.text for token in tokens[boundary + 1:i])
        if require_test and not re.search(r"@(?:org[.]junit[.])?Test(?:\(|public|protected|private|static|final|$)", prefix):
            raise ContractError(f"{method_name} is not annotated with JUnit @Test")
        matches.append({
            "tokens": tokens[opening + 1:end],
            "opening": tokens[opening].start, "end": tokens[end].end,
            "name_start": tokens[i + 1].start, "name_end": tokens[i + 1].end,
        })
    if len(matches) != 1:
        raise ContractError(f"expected one method {method_name}, found {len(matches)}")
    return matches[0]


def method_body(source, method_name):
    """Compatibility accessor used by external tooling."""
    try:
        info = method_info(source, method_name)
        return source[info["opening"]:info["end"]]
    except ContractError:
        return None


def split_arguments(tokens):
    if not tokens:
        return []
    result, start, i = [], 0, 0
    while i < len(tokens):
        if tokens[i].text in {"(", "[", "{"}:
            i = closing(tokens, i) + 1
        elif tokens[i].text == ",":
            result.append(tokens[start:i])
            start = i + 1
            i += 1
        else:
            i += 1
    result.append(tokens[start:])
    return result


def assertions(source, info):
    source_tokens = java_tokens(source)
    imports = set()
    for i, token in enumerate(source_tokens):
        if token.kind == "word" and token.text == "import":
            end = i
            while end < len(source_tokens) and source_tokens[end].text != ";":
                end += 1
            imports.add("".join(t.text for t in source_tokens[i:end + 1]))
        if i + 2 < len(source_tokens) and token.text in {"void", "boolean"} and source_tokens[i + 1].text in {"assertTrue", "assertFalse"} and source_tokens[i + 2].text == "(":
            raise ContractError("unsupported locally declared assertion method")
    found = []
    tokens = info["tokens"]
    for i, token in enumerate(tokens[:-1]):
        if token.kind != "word" or token.text not in {"assertTrue", "assertFalse"} or tokens[i + 1].text != "(":
            continue
        qualifier = ""
        if i > 0 and tokens[i - 1].text == ".":
            start = i - 1
            while start > 0 and (tokens[start - 1].kind == "word" or tokens[start - 1].text == "."):
                start -= 1
            qualifier = "".join(t.text for t in tokens[start:i])
        if qualifier:
            bound = qualifier == "org.junit.Assert." or (
                qualifier == "Assert." and "importorg.junit.Assert;" in imports)
        else:
            bound = ("importstaticorg.junit.Assert.*;" in imports or
                     "importstaticorg.junit.Assert." + token.text + ";" in imports)
        if not bound:
            raise ContractError("assertion is not bound to an imported/qualified JUnit Assert")
        end = closing(tokens, i + 1)
        args = split_arguments(tokens[i + 2:end])
        if len(args) not in {1, 2} or not args[-1]:
            raise ContractError(f"unsupported {token.text} argument count")
        found.append({
            "polarity": token.text == "assertTrue",
            "expression": canonical(args[-1]),
            "expression_tokens": args[-1],
            "callee_start": token.start, "callee_end": token.end,
            "call_end": tokens[end].end,
        })
    return found


def direct_proof(source, info, polarity, expression):
    wanted = canonical(java_tokens(expression))
    if not wanted:
        raise ContractError("empty assertion expression")
    candidates = [a for a in assertions(source, info) if a["expression"] == wanted]
    if len({a["polarity"] for a in candidates}) > 1:
        raise ContractError("ambiguous assertion polarity for the same expression")
    matching = [a for a in candidates if a["polarity"] == polarity]
    if not matching:
        raise ContractError(f"no JUnit {'assertTrue' if polarity else 'assertFalse'} proves {expression!r}")
    return matching[0]


def resolve_marker(marker, source, info, class_name):
    parts = marker.split(":", 2)
    if len(parts) >= 2 and parts[0] in {"true", "false"}:
        return direct_proof(source, info, parts[0] == "true", marker.split(":", 1)[1])
    if len(parts) == 3 and parts[0] == "helper" and parts[1] in {"true", "false"}:
        name = parts[2]
        registered = HELPERS.get((class_name, name))
        if registered is None:
            raise ContractError(f"unsupported helper {name!r}")
        requested = parts[1] == "true"
        if requested != registered[0]:
            raise ContractError(f"helper {name} has a different registered polarity")
        tokens = info["tokens"]
        calls = [i for i in range(len(tokens) - 1)
                 if tokens[i].kind == "word" and tokens[i].text == name and tokens[i + 1].text == "("]
        if not calls:
            raise ContractError(f"test does not call helper {name}")
        for i in calls:
            end = closing(tokens, i + 1)
            if len(split_arguments(tokens[i + 2:end])) != 2:
                raise ContractError(f"unsupported helper call form for {name}")
        helper = method_info(source, name)
        return direct_proof(source, helper, registered[0], registered[1])
    raise ContractError(f"unsupported assertion marker {marker!r}; use true:, false:, or a registered helper")


def scan_truth(proof):
    """Resolve assertion polarity and leading negation, rejecting composite scan predicates."""
    tokens = list(java_tokens(proof["expression"]))
    while tokens and tokens[0].text == "(" and closing(tokens, 0) == len(tokens) - 1:
        tokens = tokens[1:-1]
    negations = 0
    while tokens and tokens[0].text == "!":
        negations += 1
        tokens = tokens[1:]
        while tokens and tokens[0].text == "(" and closing(tokens, 0) == len(tokens) - 1:
            tokens = tokens[1:-1]
    if len(tokens) < 5 or [t.text for t in tokens[-4:]] != [".", "isAllowParallelScan", "(", ")"]:
        raise ContractError("scan_permission requires a direct isAllowParallelScan() predicate")
    receiver = tokens[:-4]
    if not receiver or receiver[0].kind != "word":
        raise ContractError("unsupported scan receiver")
    i = 0
    while i < len(receiver):
        text = receiver[i].text
        if text in {"(", "["}:
            i = closing(receiver, i) + 1
        elif receiver[i].kind == "word" or text == ".":
            i += 1
        else:
            raise ContractError("unsupported composite scan predicate")
    return proof["polarity"] ^ bool(negations % 2)


@lru_cache(maxsize=64)
def test_source(repo_root, class_name):
    if not re.fullmatch(r"[A-Za-z_$][A-Za-z0-9_$]*[.]java", class_name):
        return None
    matches = list((repo_root / "iotdb-core/datanode/src/test/java").rglob(class_name))
    return matches[0] if len(matches) == 1 else None


@lru_cache(maxsize=16)
def enum_values(source, name):
    tokens = java_tokens(source)
    for i in range(len(tokens) - 2):
        if tokens[i].text == "enum" and tokens[i + 1].text == name and tokens[i + 2].text == "{":
            end = closing(tokens, i + 2)
            values = []
            for token in tokens[i + 3:end]:
                if token.text == ";":
                    break
                if token.text == ",":
                    continue
                if not re.fullmatch(r"[A-Z][A-Z0-9_]*", token.text):
                    raise ContractError(f"unsupported enum declaration for {name}")
                values.append(token.text)
            return set(values)
    raise ContractError(f"missing enum {name}")


def validate(matrix, repo_root, source_overrides=None):
    errors = []
    source_overrides = source_overrides or {}
    if not isinstance(matrix, dict):
        return ["matrix must be an object"]
    if matrix.get("schema_version") != 2:
        errors.append("schema_version must be 2")
    if not isinstance(matrix.get("purpose"), str) or not matrix["purpose"].strip():
        errors.append("purpose must be a non-empty string")
    operators = matrix.get("operators")
    if not isinstance(operators, list):
        return ["operators must be an array"]
    ids = [entry.get("id") for entry in operators if isinstance(entry, dict)]
    if set(ids) != EXPECTED_OPERATORS:
        errors.append("operator ids must be exactly: " + ", ".join(sorted(EXPECTED_OPERATORS)))
    if len(ids) != len(set(ids)):
        errors.append("operator ids must be unique")
    try:
        reason_domains = {
            "FALLBACK": enum_values((repo_root / DISTRIBUTE / "FallbackReason.java").read_text(), "FallbackReason"),
            "GROUP_BY_ADMISSION": enum_values((repo_root / DISTRIBUTE / "TableDistributedPlanGenerator.java").read_text(), "RejectionReason"),
        }
    except (OSError, ContractError) as error:
        return errors + [f"reason enum binding failed: {error}"]
    for entry in operators:
        if not isinstance(entry, dict):
            errors.append("every operator entry must be an object")
            continue
        label = entry.get("id", "<missing id>")
        missing = REQUIRED_FIELDS - set(entry)
        if missing:
            errors.append(f"{label}: missing fields: {', '.join(sorted(missing))}")
            continue
        for field in ("sql", "required", "provided", "enforcer"):
            if not isinstance(entry[field], str) or not entry[field].strip():
                errors.append(f"{label}: {field} must be a non-empty string")
        if not isinstance(entry["sql"], str) or not entry["sql"].lstrip().upper().startswith("SELECT"):
            errors.append(f"{label}: sql must be a SELECT statement")
        parallelism = entry["scan_parallelism"]
        if parallelism not in VALID_PARALLELISM:
            errors.append(f"{label}: invalid scan_parallelism")
        reason = entry["forbidden_parallelism_reason"]
        if parallelism == "allowed" and reason is not None:
            errors.append(f"{label}: allowed scan path requires a null forbidden reason")
        if parallelism in {"forbidden", "conditional"} and (not isinstance(reason, str) or not reason.strip()):
            errors.append(f"{label}: {parallelism} scan path requires a reason")
        fallback = entry["fallback_reason"]
        if fallback is not None:
            if not isinstance(fallback, str) or ":" not in fallback:
                errors.append(f"{label}: fallback_reason must be null or a reason-domain code")
                continue
            else:
                domain, code = fallback.split(":", 1)
                if code not in reason_domains.get(domain, set()):
                    errors.append(f"{label}: unknown fallback reason {fallback!r}")
        plan_test = entry["plan_test"]
        if not isinstance(plan_test, dict):
            errors.append(f"{label}: plan_test must be an object")
            continue
        class_name, method_name = plan_test.get("class"), plan_test.get("method")
        if not isinstance(class_name, str) or not isinstance(method_name, str) or not re.fullmatch(r"[A-Za-z_$][A-Za-z0-9_$]*", method_name):
            errors.append(f"{label}: invalid plan_test source or method")
            continue
        markers = plan_test.get("assertions")
        if not isinstance(markers, list) or not markers or not all(isinstance(m, str) and m.strip() for m in markers):
            errors.append(f"{label}: assertions must be a non-empty marker list")
            continue
        path = test_source(repo_root, class_name)
        if path is None:
            errors.append(f"{label}: cannot resolve one JUnit source named {class_name}")
            continue
        try:
            source = source_overrides.get(class_name, path.read_text(encoding="utf-8"))
            info = method_info(source, method_name, require_test=True)
            proved = {marker: resolve_marker(marker, source, info, class_name) for marker in markers}
            if fallback is not None:
                domain, code = fallback.split(":", 1)
                if not any(fallback in marker or (domain + ":" in marker and code in marker) for marker in markers):
                    raise ContractError("fallback reason needs an explicit linked reason assertion")
            execution = entry["execution_marker"]
            if not isinstance(execution, dict):
                raise ContractError("execution_marker must be an object")
            kind, value, assertion = execution.get("kind"), execution.get("value"), execution.get("assertion")
            if assertion not in proved:
                raise ContractError("execution marker assertion must be listed and proved")
            if parallelism in {"allowed", "forbidden"} and kind != "scan_permission":
                raise ContractError("unconditional scan eligibility requires a scan_permission marker")
            if kind == "scan_permission":
                if not isinstance(value, bool):
                    raise ContractError("scan_permission value must be boolean")
                actual = scan_truth(proved[assertion])
                if actual != value:
                    raise ContractError("scan_permission assertion polarity disagrees with its value")
                if parallelism in {"allowed", "forbidden"} and value != (parallelism == "allowed"):
                    raise ContractError("scan_permission polarity disagrees with scan_parallelism")
            elif kind == "plan_node":
                if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_$][A-Za-z0-9_$]*", value):
                    raise ContractError("plan_node value must name a node or topology")
                if parallelism != "conditional":
                    raise ContractError("a plan_node marker alone cannot prove unconditional scan eligibility")
            elif kind == "runtime_evidence":
                artifact, digest = execution.get("artifact"), execution.get("sha256")
                if not isinstance(artifact, str) or not Path(artifact).is_absolute() or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
                    raise ContractError("runtime evidence needs an absolute artifact path and SHA256")
                if hashlib.sha256(Path(artifact).read_bytes()).hexdigest() != digest:
                    raise ContractError("runtime evidence artifact SHA256 mismatch")
            else:
                raise ContractError(f"unsupported execution marker kind {kind!r}")
        except (OSError, ContractError, TypeError) as error:
            errors.append(f"{label}: {error}")
    return errors


def self_tests(matrix, repo_root):
    originals = {}
    for entry in matrix["operators"]:
        name = entry["plan_test"]["class"]
        originals[name] = test_source(repo_root, name).read_text(encoding="utf-8")
    cases = []

    def run(name, mutation=None, should_fail=True):
        candidate, sources = copy.deepcopy(matrix), dict(originals)
        if mutation:
            mutation(candidate, sources)
        failures = validate(candidate, repo_root, sources)
        passed = bool(failures) if should_fail else not failures
        cases.append({"case": name, "passed": passed, "errors": failures})

    def entry(m, name="project"):
        return next(e for e in m["operators"] if e["id"] == name)

    def change_assert(sources, method_name, expression, polarity):
        name = "PropertyDrivenDistributionTest.java"
        source = sources[name]
        info = method_info(source, method_name)
        proof = next(p for p in assertions(source, info) if p["expression"] == canonical(java_tokens(expression)))
        replacement = "assertTrue" if polarity else "assertFalse"
        sources[name] = source[:proof["callee_start"]] + replacement + source[proof["callee_end"]:]

    run("all_current_contracts", should_fail=False)

    def flip_contract(m, s):
        e = entry(m)
        e["scan_parallelism"] = "forbidden"
        e["forbidden_parallelism_reason"] = "Injected negative contract"
        e["execution_marker"]["value"] = False
    run("flip_projection_contract_only", flip_contract)

    def flip_assert(m, s):
        change_assert(s, entry(m)["plan_test"]["method"], "scans.get(0).isAllowParallelScan()", False)
    run("flip_projection_assertion", flip_assert)

    def delete_method(m, s):
        e = entry(m); name = e["plan_test"]["class"]
        source = s[name]; info = method_info(source, e["plan_test"]["method"])
        s[name] = source[:info["name_start"]] + "removedProjectionMethod" + source[info["name_end"]:]
    run("delete_linked_method", delete_method)

    def comment_assert(m, s):
        e = entry(m); name = e["plan_test"]["class"]; source = s[name]
        info = method_info(source, e["plan_test"]["method"])
        proof = next(p for p in assertions(source, info) if p["expression"] == "scans.get(0).isAllowParallelScan()")
        s[name] = source[:proof["callee_start"]] + "/*" + source[proof["callee_start"]:proof["call_end"]] + "*/" + source[proof["call_end"]:]
    run("assertion_in_comment_is_not_proof", comment_assert)

    def literal_decoy(m, s):
        e = entry(m); name = e["plan_test"]["class"]; source = s[name]
        info = method_info(source, e["plan_test"]["method"])
        body = '{ String decoy = "assertTrue(scans.get(0).isAllowParallelScan()); } /*"; }'
        s[name] = source[:info["opening"]] + body + source[info["end"]:]
    run("assertion_in_string_is_not_proof", literal_decoy)

    def helper_flip(m, s):
        change_assert(s, "assertAllScansForbidParallelism", "scan.isAllowParallelScan()", True)
    run("helper_assertion_polarity", helper_flip)

    def unregistered_helper(m, s):
        e = entry(m, "window")
        e["plan_test"]["assertions"] = ["helper:false:unregisteredHelper"]
        e["execution_marker"]["assertion"] = "helper:false:unregisteredHelper"
    run("unregistered_helper_rejected", unregistered_helper)

    def unsupported_marker(m, s):
        e = entry(m); e["plan_test"]["assertions"] = ["isAllowParallelScan()"]
        e["execution_marker"]["assertion"] = "isAllowParallelScan()"
    run("bare_substring_marker_rejected", unsupported_marker)

    def negated_polarity(m, s):
        e = entry(m); name = e["plan_test"]["class"]; source = s[name]
        info = method_info(source, e["plan_test"]["method"])
        proof = next(p for p in assertions(source, info) if p["expression"] == "scans.get(0).isAllowParallelScan()")
        expression = proof["expression_tokens"]
        source = source[:expression[0].start] + "!" + source[expression[0].start:]
        info = method_info(source, e["plan_test"]["method"])
        proof = next(p for p in assertions(source, info) if p["expression"] == "!scans.get(0).isAllowParallelScan()")
        source = source[:proof["callee_start"]] + "assertFalse" + source[proof["callee_end"]:]
        s[name] = source
        marker = "false:!scans.get(0).isAllowParallelScan()"
        e["plan_test"]["assertions"] = [marker]; e["execution_marker"]["assertion"] = marker
        e["execution_marker"]["value"] = False
        e["scan_parallelism"] = "forbidden"; e["forbidden_parallelism_reason"] = "Injected negation"
    run("negation_does_not_reverse_the_contract_silently", negated_polarity)

    def punctuation_in_literals(m, s):
        e = entry(m); name = e["plan_test"]["class"]; source = s[name]
        info = method_info(source, e["plan_test"]["method"])
        injected = '\n String punctuation = "},) // assertFalse(ignored)"; /* { ) , } */\n'
        s[name] = source[:info["opening"] + 1] + injected + source[info["opening"] + 1:]
    run("comment_and_literal_delimiters", punctuation_in_literals, should_fail=False)

    def nested_message(m, s):
        e = entry(m); name = e["plan_test"]["class"]; source = s[name]
        info = method_info(source, e["plan_test"]["method"])
        proof = next(p for p in assertions(source, info) if p["expression"] == "scans.get(0).isAllowParallelScan()")
        expression = source[proof["expression_tokens"][0].start:proof["expression_tokens"][-1].end]
        replacement = 'assertTrue(String.format("a,b (%s)", "})"), ((' + expression + ')))'
        s[name] = source[:proof["callee_start"]] + replacement + source[proof["call_end"]:]
    run("nested_message_and_parentheses", nested_message, should_fail=False)

    def unknown_kind(m, s):
        entry(m)["execution_marker"]["kind"] = "unknown"
    run("unknown_execution_marker_rejected", unknown_kind)

    def unknown_reason(m, s):
        entry(m, "join")["fallback_reason"] = "FALLBACK:UNKNOWN_REASON"
    run("unknown_fallback_reason_rejected", unknown_reason)

    def duplicate_operator(m, s):
        m["operators"].append(copy.deepcopy(entry(m)))
    run("duplicate_operator_rejected", duplicate_operator)

    def commented_method(m, s):
        delete_method(m, s)
        name = entry(m)["plan_test"]["class"]
        s[name] += '\n// void projectQueryAllowsParallelScan() { assertTrue(scans.get(0).isAllowParallelScan()); }\n'
    run("method_in_comment_is_not_a_test", commented_method)

    def remove_annotation(m, s):
        e = entry(m); name = e["plan_test"]["class"]; source = s[name]
        info = method_info(source, e["plan_test"]["method"])
        annotation = source.rfind("@Test", 0, info["name_start"])
        s[name] = source[:annotation] + source[annotation + len("@Test"):]
    run("missing_junit_annotation_rejected", remove_annotation)

    def malformed_literal(m, s):
        name = entry(m)["plan_test"]["class"]
        s[name] += '\nString unterminated = "missing close'
    run("malformed_java_literal_rejected", malformed_literal)

    def conflicting_assertions(m, s):
        e = entry(m); name = e["plan_test"]["class"]; source = s[name]
        info = method_info(source, e["plan_test"]["method"])
        s[name] = source[:info["end"] - 1] + 'assertFalse(scans.get(0).isAllowParallelScan());' + source[info["end"] - 1:]
    run("conflicting_polarities_rejected", conflicting_assertions)
    def runtime_cannot_prove_scan(m, s):
        e = entry(m)
        e["scan_parallelism"] = "forbidden"
        e["forbidden_parallelism_reason"] = "Injected opposite scan contract"
        artifact = repo_root / "research/query-intra-parallelism-e2e-acceptance/manifests/property-enforcement-matrix.json"
        e["execution_marker"].update(kind="runtime_evidence", value="result",
            artifact=str(artifact.resolve()), sha256=hashlib.sha256(artifact.read_bytes()).hexdigest())
    run("runtime_artifact_cannot_replace_scan_polarity", runtime_cannot_prove_scan)

    def local_assertion(m, s):
        name = entry(m)["plan_test"]["class"]; source = s[name]; end = source.rfind("}")
        s[name] = source[:end] + "private static void assertTrue(Object... ignored) {}" + source[end:]
    run("local_assertion_shadowing_rejected", local_assertion)

    def literal_import(m, s):
        name = entry(m)["plan_test"]["class"]
        source = s[name].replace("import static org.junit.Assert.assertTrue;", "")
        end = source.rfind("}")
        s[name] = source[:end] + 'String decoyImport = "importstaticorg.junit.Assert.assertTrue;";' + source[end:]
    run("literal_cannot_supply_a_junit_import", literal_import)

    def qualified_junit(m, s):
        e = entry(m); name = e["plan_test"]["class"]; source = s[name]
        info = method_info(source, e["plan_test"]["method"])
        proof = next(p for p in assertions(source, info) if p["expression"] == "scans.get(0).isAllowParallelScan()")
        s[name] = source[:proof["callee_start"]] + "org.junit.Assert.assertTrue" + source[proof["callee_end"]:]
    run("qualified_junit_assertion", qualified_junit, should_fail=False)
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--matrix", type=Path)
    parser.add_argument("--repo-root", type=Path)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--report", type=Path, help="write a new JSON report; existing files are preserved")
    args = parser.parse_args()
    repo_root = (args.repo_root or Path(__file__).resolve().parents[3]).resolve()
    path = args.matrix or repo_root / "research/query-intra-parallelism-e2e-acceptance/manifests/property-enforcement-matrix.json"
    try:
        matrix = json.loads(path.read_text(encoding="utf-8"))
        errors = validate(matrix, repo_root)
        cases = self_tests(matrix, repo_root) if args.self_test and not errors else []
        errors += [f"self-test failed: {case['case']}" for case in cases if not case["passed"]]
        report = {
            "status": "FAIL" if errors else "PASS",
            "scope": "static contract/assertion links and parser self-tests; Java and service execution are separate",
            "repo_root": str(repo_root), "matrix": str(path.resolve()),
            "matrix_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "contracts": len(matrix.get("operators", [])),
            "errors": errors, "self_tests": cases,
            "source_bindings": [],
        }
        if not errors:
            for name in sorted({e["plan_test"]["class"] for e in matrix["operators"]}):
                source = test_source(repo_root, name)
                report["source_bindings"].append({"path": str(source), "sha256": hashlib.sha256(source.read_bytes()).hexdigest()})
        if args.report:
            with args.report.open("x", encoding="utf-8") as output:
                json.dump(report, output, ensure_ascii=False, indent=2)
                output.write("\n")
    except (OSError, ValueError, ContractError) as error:
        print(f"property enforcement matrix: invalid input: {error}", file=sys.stderr)
        return 2
    if errors:
        print("property enforcement matrix: FAILED", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1
    print(f"property enforcement matrix: OK ({len(matrix['operators'])} contracts, {len(cases)} parser self-tests)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
