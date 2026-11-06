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

"""Capture the immutable context required to interpret a benchmark run."""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def command_output(command: list[str]) -> dict[str, Any]:
    try:
        completed = subprocess.run(command, capture_output=True, text=True, check=False)
    except OSError as error:
        return {"command": command, "error": str(error)}
    return {
        "command": command,
        "exit_code": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def find_git_root(start: Path) -> Path | None:
    current = start.resolve()
    for candidate in (current, *current.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def git_context(git_root: Path | None) -> dict[str, Any]:
    if git_root is None:
        return {"available": False}
    return {
        "available": True,
        "root": str(git_root),
        "head": command_output(["git", "-C", str(git_root), "rev-parse", "HEAD"]),
        "branch": command_output(["git", "-C", str(git_root), "branch", "--show-current"]),
        "status": command_output(["git", "-C", str(git_root), "status", "--short"]),
        "diff_stat": command_output(["git", "-C", str(git_root), "diff", "--stat"]),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--config", action="append", default=[], type=Path)
    parser.add_argument("--git-root", type=Path)
    args = parser.parse_args()

    args.output.mkdir(parents=True, exist_ok=True)
    config_dir = args.output / "config"
    config_dir.mkdir(exist_ok=True)
    copied_configs = []
    for config in args.config:
        destination = config_dir / config.name
        if config.is_file():
            shutil.copy2(config, destination)
            copied_configs.append({"source": str(config), "copy": str(destination)})
        else:
            copied_configs.append({"source": str(config), "missing": True})

    git_root = args.git_root or find_git_root(Path.cwd())
    payload = {
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "python": sys.version,
        "uname": command_output(["uname", "-a"]),
        "cpu": command_output(["lscpu"]),
        "memory": command_output(["free", "-b"]),
        "storage": command_output(["df", "-B1"]),
        "git": git_context(git_root),
        "configs": copied_configs,
    }
    (args.output / "environment.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
