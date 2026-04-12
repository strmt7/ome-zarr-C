#!/usr/bin/env python3
"""Block new embedded-Python execution patterns from entering cpp/."""

from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path

FORBIDDEN_PATTERNS = {
    "py::exec": re.compile(r"\bpy::exec\s*\("),
    "py::eval": re.compile(r"\bpy::eval\s*\("),
    "pybind11/eval.h": re.compile(r"<pybind11/eval\.h>"),
    "raw-python-string": re.compile(r'R"PY\('),
    "python-c-api-exec": re.compile(r"\bPy(?:Run_|CompileString)"),
}
CPP_SUFFIXES = {".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".hxx"}


def current_tree_violations(root: Path) -> list[tuple[str, int, str, str]]:
    violations: list[tuple[str, int, str, str]] = []
    for path in sorted((root / "cpp").rglob("*")):
        if not path.is_file() or path.suffix not in CPP_SUFFIXES:
            continue
        for index, line in enumerate(path.read_text().splitlines(), start=1):
            for name, pattern in FORBIDDEN_PATTERNS.items():
                if pattern.search(line):
                    violations.append((name, index, str(path), line.strip()))
    return violations


def diff_violations(base: str, head: str) -> list[tuple[str, int, str, str]]:
    cmd = [
        "git",
        "diff",
        "--unified=0",
        "--no-color",
        base,
        head,
        "--",
        "cpp",
    ]
    diff = subprocess.run(
        cmd, capture_output=True, text=True, check=True
    ).stdout.splitlines()

    violations: list[tuple[str, int, str, str]] = []
    current_file = ""
    current_line = 0
    hunk_line = 0

    for line in diff:
        if line.startswith("+++ b/"):
            current_file = line[6:]
            continue
        if line.startswith("@@"):
            match = re.search(r"\+(\d+)", line)
            if match is not None:
                hunk_line = int(match.group(1))
                current_line = hunk_line
            continue
        if not current_file:
            continue
        if line.startswith("+") and not line.startswith("+++"):
            content = line[1:]
            for name, pattern in FORBIDDEN_PATTERNS.items():
                if pattern.search(content):
                    violations.append(
                        (name, current_line, current_file, content.strip())
                    )
            current_line += 1
        elif line.startswith("-") and not line.startswith("---"):
            continue
        else:
            current_line += 1

    return violations


def print_violations(violations: list[tuple[str, int, str, str]]) -> None:
    for name, line_number, path, line in violations:
        print(f"{path}:{line_number}: {name}: {line}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--all",
        action="store_true",
        help="scan the current cpp/ tree instead of a git diff",
    )
    parser.add_argument("--base", help="git base ref for diff mode")
    parser.add_argument("--head", help="git head ref for diff mode")
    args = parser.parse_args()

    if args.all:
        violations = current_tree_violations(Path("."))
    else:
        if not args.base or not args.head:
            parser.error("diff mode requires --base and --head")
        violations = diff_violations(args.base, args.head)

    if not violations:
        print("No forbidden pseudo-C++ patterns found.")
        return 0

    print_violations(violations)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
