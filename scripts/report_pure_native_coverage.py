#!/usr/bin/env python3
"""Report pure-native coverage against the frozen upstream snapshot."""

from __future__ import annotations

import argparse
import ast
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Span:
    start: int
    end: int

    @property
    def line_count(self) -> int:
        return self.end - self.start + 1


def build_qualname_spans(source: str) -> dict[str, Span]:
    tree = ast.parse(source)
    spans: dict[str, Span] = {}

    def walk(node: ast.AST, prefix: str = "") -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                spans[prefix + child.name] = Span(child.lineno, child.end_lineno)
            elif isinstance(child, ast.ClassDef):
                walk(child, prefix=f"{prefix}{child.name}.")

    walk(tree)
    return spans


def callable_line_numbers(spans: dict[str, Span]) -> set[int]:
    line_numbers: set[int] = set()
    for span in spans.values():
        line_numbers.update(range(span.start, span.end + 1))
    return line_numbers


def upstream_total_lines(upstream_root: Path) -> int:
    return sum(
        len(path.read_text().splitlines())
        for path in sorted(upstream_root.rglob("*.py"))
    )


def load_manifest(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--manifest",
        default="docs/reference/pure-native-coverage-manifest.json",
    )
    parser.add_argument(
        "--fail-under",
        type=float,
        help="fail if the computed percentage is below this threshold",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit the computed report as JSON",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    manifest_path = repo_root / args.manifest
    manifest = load_manifest(manifest_path)
    upstream_root = repo_root / manifest["upstream_root"]
    entries = manifest["entries"]

    cache: dict[str, dict[str, Span]] = {}
    resolved_entries: list[dict[str, Any]] = []
    total_converted = 0

    for entry in entries:
        entry_type = entry.get("entry_type", "qualname")
        upstream_file = upstream_root / entry["upstream_file"]
        if not upstream_file.exists():
            raise SystemExit(f"missing upstream file: {upstream_file}")

        for relative in entry.get("boundary_files", []) + entry["native_files"]:
            resolved = repo_root / relative
            if not resolved.exists():
                raise SystemExit(f"missing implementation file: {resolved}")

        spans = cache.setdefault(
            entry["upstream_file"],
            build_qualname_spans(upstream_file.read_text()),
        )
        if entry_type == "qualname":
            qualname = entry["qualname"]
            if qualname not in spans:
                upstream_file_name = entry["upstream_file"]
                raise SystemExit(
                    "missing qualname "
                    f"{qualname!r} in upstream file {upstream_file_name}"
                )
            line_count = spans[qualname].line_count
        elif entry_type == "non_callable_scaffold":
            total_lines = len(upstream_file.read_text().splitlines())
            line_count = total_lines - len(callable_line_numbers(spans))
            if line_count < 0:
                raise SystemExit(
                    f"negative non-callable scaffold count for {entry['upstream_file']}"
                )
        else:
            raise SystemExit(f"unsupported manifest entry type: {entry_type}")

        total_converted += line_count
        resolved_entry = {
            "entry_type": entry_type,
            "upstream_file": entry["upstream_file"],
            "line_count": line_count,
            "boundary_files": entry.get("boundary_files", []),
            "native_files": entry["native_files"],
        }
        if entry_type == "qualname":
            resolved_entry["qualname"] = entry["qualname"]
        resolved_entries.append(resolved_entry)

    total_upstream = upstream_total_lines(upstream_root)
    percentage = (total_converted / total_upstream) * 100 if total_upstream else 0.0

    report = {
        "metric_name": manifest["metric_name"],
        "manifest": str(manifest_path.relative_to(repo_root)),
        "upstream_root": str(upstream_root.relative_to(repo_root)),
        "total_upstream_lines": total_upstream,
        "counted_lines": total_converted,
        "percentage": percentage,
        "entry_count": len(resolved_entries),
        "entries": resolved_entries,
    }

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(f"metric: {report['metric_name']}")
        print(f"manifest: {report['manifest']}")
        print(f"upstream_root: {report['upstream_root']}")
        print(f"counted_lines: {total_converted}")
        print(f"total_upstream_lines: {total_upstream}")
        print(f"percentage: {percentage:.6f}")

    if args.fail_under is not None and percentage < args.fail_under:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
