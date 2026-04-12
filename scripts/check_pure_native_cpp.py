#!/usr/bin/env python3
"""Enforce a pure-native C++ layout and flag mixed binding/semantic debt."""

from __future__ import annotations

import argparse
import re
from collections import Counter
from pathlib import Path

CPP_SUFFIXES = {".c", ".cc", ".cpp", ".cxx", ".h", ".hh", ".hpp", ".hxx"}

PYTHON_INTEGRATION_PATTERNS = {
    "pybind-include": re.compile(r"pybind11/"),
    "py-namespace": re.compile(r"\bpy::"),
    "python-c-api": re.compile(r"\bPy[A-Z][A-Za-z_]*\b"),
    "python-attr-dispatch": re.compile(r"\.attr\s*\("),
}


def iter_cpp_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix in CPP_SUFFIXES
    )


def scan_paths(
    paths: list[Path], patterns: dict[str, re.Pattern[str]]
) -> list[tuple[str, str, int, str]]:
    violations: list[tuple[str, str, int, str]] = []
    for path in paths:
        for line_number, line in enumerate(path.read_text().splitlines(), start=1):
            for name, pattern in patterns.items():
                if pattern.search(line):
                    violations.append((name, str(path), line_number, line.strip()))
    return violations


def print_violations(title: str, violations: list[tuple[str, str, int, str]]) -> None:
    print(title)
    for name, path, line_number, line in violations:
        print(f"{path}:{line_number}: {name}: {line}")


def debt_paths(
    cpp_root: Path,
    native_root: Path,
    binding_root: Path,
) -> list[Path]:
    debt: list[Path] = []
    for path in iter_cpp_files(cpp_root):
        if path.is_relative_to(native_root) or path.is_relative_to(binding_root):
            continue
        debt.append(path)
    return debt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cpp-root", default="cpp")
    parser.add_argument("--native-root", default="cpp/native")
    parser.add_argument("--binding-root", default="cpp/bindings")
    parser.add_argument(
        "--enforce-pure-native-subtree",
        action="store_true",
        help="fail if cpp/native contains Python integration patterns",
    )
    parser.add_argument(
        "--report-existing-debt",
        action="store_true",
        help="report mixed binding/semantic debt outside cpp/native and cpp/bindings",
    )
    parser.add_argument(
        "--fail-on-existing-debt",
        action="store_true",
        help="return non-zero when mixed debt is found outside the allowed roots",
    )
    args = parser.parse_args()

    if not args.enforce_pure_native_subtree and not args.report_existing_debt:
        parser.error(
            "enable at least one of --enforce-pure-native-subtree "
            "or --report-existing-debt"
        )

    cpp_root = Path(args.cpp_root)
    native_root = Path(args.native_root)
    binding_root = Path(args.binding_root)

    exit_code = 0

    if args.enforce_pure_native_subtree:
        native_files = iter_cpp_files(native_root)
        if not native_files:
            print(f"No pure-native files found under {native_root}")
        else:
            native_violations = scan_paths(native_files, PYTHON_INTEGRATION_PATTERNS)
            if native_violations:
                print_violations("Pure-native subtree violations:", native_violations)
                exit_code = 1
            else:
                print(f"Pure-native subtree clean: {native_root}")

    if args.report_existing_debt:
        debt_files = debt_paths(cpp_root, native_root, binding_root)
        debt_violations = scan_paths(debt_files, PYTHON_INTEGRATION_PATTERNS)
        if debt_violations:
            print_violations(
                "Mixed binding/semantic debt outside allowed roots:",
                debt_violations,
            )
            counts = Counter(name for name, _path, _line, _text in debt_violations)
            print("Debt summary:")
            for name, count in sorted(counts.items()):
                print(f"{name}: {count}")
            if args.fail_on_existing_debt:
                exit_code = 1
        else:
            print("No mixed binding/semantic debt found outside allowed roots.")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
