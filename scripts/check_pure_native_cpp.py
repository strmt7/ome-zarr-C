#!/usr/bin/env python3
"""Enforce a pure-native C++ layout and flag Python-integration debt."""

from __future__ import annotations

import argparse
import json
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


def load_baseline(path: Path) -> dict:
    return json.loads(path.read_text())


def validate_against_baseline(
    violations: list[tuple[str, str, int, str]],
    baseline: dict,
) -> list[str]:
    errors: list[str] = []
    counts = Counter(name for name, _path, _line, _text in violations)
    observed_files = sorted({path for _name, path, _line, _text in violations})
    allowed_files = sorted(baseline.get("allowed_files", []))
    max_counts = baseline.get("max_counts", {})

    unexpected_files = [path for path in observed_files if path not in allowed_files]
    if unexpected_files:
        errors.append(
            "unexpected debt files outside baseline: " + ", ".join(unexpected_files)
        )

    for name, count in sorted(counts.items()):
        allowed = max_counts.get(name)
        if allowed is None:
            errors.append(f"new debt category outside baseline: {name}={count}")
        elif count > allowed:
            errors.append(f"debt count regression for {name}: {count} > {allowed}")

    return errors


def debt_paths(
    cpp_root: Path,
    native_root: Path,
) -> list[Path]:
    debt: list[Path] = []
    for path in iter_cpp_files(cpp_root):
        if path.is_relative_to(native_root):
            continue
        debt.append(path)
    return debt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cpp-root", default="cpp")
    parser.add_argument("--native-root", default="cpp/native")
    parser.add_argument(
        "--enforce-pure-native-subtree",
        action="store_true",
        help="fail if cpp/native contains Python integration patterns",
    )
    parser.add_argument(
        "--report-existing-debt",
        action="store_true",
        help="report Python-integration debt outside cpp/native",
    )
    parser.add_argument(
        "--fail-on-existing-debt",
        action="store_true",
        help="return non-zero when Python-integration debt is found under cpp",
    )
    parser.add_argument(
        "--baseline-json",
        help=(
            "JSON baseline describing allowed existing mixed-debt files and max counts"
        ),
    )
    parser.add_argument(
        "--fail-on-baseline-regression",
        action="store_true",
        help="return non-zero if existing mixed debt exceeds the recorded baseline",
    )
    args = parser.parse_args()

    if not args.enforce_pure_native_subtree and not args.report_existing_debt:
        parser.error(
            "enable at least one of --enforce-pure-native-subtree "
            "or --report-existing-debt"
        )
    if args.fail_on_baseline_regression and not args.baseline_json:
        parser.error("--fail-on-baseline-regression requires --baseline-json")

    cpp_root = Path(args.cpp_root)
    native_root = Path(args.native_root)

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
        debt_files = debt_paths(cpp_root, native_root)
        debt_violations = scan_paths(debt_files, PYTHON_INTEGRATION_PATTERNS)
        if debt_violations:
            print_violations(
                "Python-integration debt outside pure-native subtree:",
                debt_violations,
            )
            counts = Counter(name for name, _path, _line, _text in debt_violations)
            print("Debt summary:")
            for name, count in sorted(counts.items()):
                print(f"{name}: {count}")
            if args.baseline_json:
                baseline_errors = validate_against_baseline(
                    debt_violations,
                    load_baseline(Path(args.baseline_json)),
                )
                if baseline_errors:
                    print("Baseline regression:")
                    for error in baseline_errors:
                        print(error)
                    if args.fail_on_baseline_regression:
                        exit_code = 1
            if args.fail_on_existing_debt:
                exit_code = 1
        else:
            print("No Python-integration debt found outside pure-native subtree.")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
