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

"""Offline contract check for manifests/property-enforcement-matrix.json."""

import argparse
import json
import re
import sys
from pathlib import Path

EXPECTED_OPERATORS = {"scan", "filter", "project", "sort", "top-k", "window", "row-number", "aggregation", "join", "union"}
REQUIRED_FIELDS = {"id", "sql", "required", "provided", "enforcer", "scan_parallelism", "forbidden_parallelism_reason", "plan_test"}
VALID_PARALLELISM = {"allowed", "forbidden", "conditional"}


def test_source(repo_root, class_name):
  matches = list((repo_root / "iotdb-core" / "datanode" / "src" / "test" / "java").rglob(class_name))
  return matches[0] if len(matches) == 1 else None


def method_body(source, method_name):
  match = re.search(r"\bvoid\s+" + re.escape(method_name) + r"\s*\(", source)
  if match is None:
    return None
  opening_brace = source.find("{", match.end())
  if opening_brace == -1:
    return None
  depth = 0
  for index in range(opening_brace, len(source)):
    if source[index] == "{":
      depth += 1
    elif source[index] == "}":
      depth -= 1
      if depth == 0:
        return source[opening_brace : index + 1]
  return None


def validate(matrix, repo_root):
  errors = []
  if matrix.get("schema_version") != 1:
    errors.append("schema_version must be 1")
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
      errors.append(f"{label}: scan_parallelism must be one of {sorted(VALID_PARALLELISM)}")
    reason = entry["forbidden_parallelism_reason"]
    if parallelism == "allowed" and reason is not None:
      errors.append(f"{label}: an allowed path must not claim a forbidden-parallelism reason")
    if parallelism in {"forbidden", "conditional"} and (not isinstance(reason, str) or not reason.strip()):
      errors.append(f"{label}: a {parallelism} path requires a non-empty fallback reason")
    plan_test = entry["plan_test"]
    if not isinstance(plan_test, dict):
      errors.append(f"{label}: plan_test must be an object")
      continue
    class_name, method_name = plan_test.get("class"), plan_test.get("method")
    assertions = plan_test.get("assertions")
    if not all(isinstance(value, str) and value.strip() for value in (class_name, method_name)):
      errors.append(f"{label}: plan_test.class and plan_test.method must be non-empty strings")
      continue
    if not isinstance(assertions, list) or not assertions or not all(isinstance(item, str) and item.strip() for item in assertions):
      errors.append(f"{label}: plan_test.assertions must be a non-empty string array")
      assertions = []
    source = test_source(repo_root, class_name)
    if source is None:
      errors.append(f"{label}: cannot resolve one JUnit source named {class_name}")
    else:
      body = method_body(source.read_text(encoding="utf-8"), method_name)
      if body is None:
        errors.append(f"{label}: {class_name} has no test method {method_name}")
      else:
        for marker in assertions:
          if marker not in body:
            errors.append(
                f"{label}: {class_name}.{method_name} does not contain assertion marker {marker!r}"
            )
  return errors


def main():
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument("--matrix", type=Path, help="override the checked matrix path")
  args = parser.parse_args()
  repo_root = Path(__file__).resolve().parents[3]
  matrix_path = args.matrix or repo_root / "research" / "query-intra-parallelism-e2e-acceptance" / "manifests" / "property-enforcement-matrix.json"
  try:
    matrix = json.loads(matrix_path.read_text(encoding="utf-8"))
  except (OSError, json.JSONDecodeError) as error:
    print(f"property enforcement matrix: invalid input: {error}", file=sys.stderr)
    return 2
  errors = validate(matrix, repo_root)
  if errors:
    print("property enforcement matrix: FAILED", file=sys.stderr)
    print("\n".join(f"- {error}" for error in errors), file=sys.stderr)
    return 1
  print(f"property enforcement matrix: OK ({len(matrix['operators'])} operator contracts, linked JUnit plan assertions)")
  return 0


if __name__ == "__main__":
  sys.exit(main())
