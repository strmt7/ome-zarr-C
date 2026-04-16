#!/usr/bin/env python3
"""Report documented upstream callables with standalone-native benchmark coverage."""

from __future__ import annotations

import argparse
import importlib
import inspect
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM_ROOT = ROOT / "source_code_v.0.15.0"
if str(UPSTREAM_ROOT) not in sys.path:
    sys.path.insert(0, str(UPSTREAM_ROOT))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

MODULES = (
    "ome_zarr.conversions",
    "ome_zarr.csv",
    "ome_zarr.data",
    "ome_zarr.format",
    "ome_zarr.utils",
)

NATIVE_BENCHMARK_COVERAGE = {
    "ome_zarr.conversions.int_to_rgba": ["conversions.int_to_rgba"],
    "ome_zarr.conversions.rgba_to_int": ["conversions.rgba_to_int"],
    "ome_zarr.csv.parse_csv_value": ["csv.parse_csv_value"],
    "ome_zarr.data.make_circle": ["data.make_circle_batch"],
    "ome_zarr.data.rgb_to_5d": ["data.rgb_to_5d_batch"],
    "ome_zarr.format.detect_format": ["format.dispatch"],
    "ome_zarr.format.format_from_version": ["format.dispatch"],
    "ome_zarr.format.format_implementations": ["format.dispatch"],
    "ome_zarr.format.FormatV01.init_store": ["format.v01_init_store"],
    "ome_zarr.format.FormatV01.generate_coordinate_transformations": [
        "format.well_and_coord"
    ],
    "ome_zarr.format.FormatV01.generate_well_dict": ["format.well_and_coord"],
    "ome_zarr.format.FormatV01.matches": ["format.matches"],
    "ome_zarr.format.FormatV01.validate_coordinate_transformations": [
        "format.well_and_coord"
    ],
    "ome_zarr.format.FormatV01.validate_well_dict": ["format.well_and_coord"],
    "ome_zarr.format.FormatV02.matches": ["format.matches"],
    "ome_zarr.format.FormatV03.matches": ["format.matches"],
    "ome_zarr.format.FormatV04.generate_coordinate_transformations": [
        "format.well_and_coord"
    ],
    "ome_zarr.format.FormatV04.generate_well_dict": ["format.well_and_coord"],
    "ome_zarr.format.FormatV04.matches": ["format.matches"],
    "ome_zarr.format.FormatV04.validate_coordinate_transformations": [
        "format.well_and_coord"
    ],
    "ome_zarr.format.FormatV04.validate_well_dict": ["format.well_and_coord"],
    "ome_zarr.format.FormatV05.matches": ["format.matches"],
    "ome_zarr.utils.splitall": ["utils.path_helpers"],
    "ome_zarr.utils.strip_common_prefix": ["utils.path_helpers"],
}

EXCLUDED_REASON = (
    "No standalone-native benchmark entrypoint is currently registered for this "
    "callable. Do not claim native C++ performance for it until one exists."
)


def iter_documented_callables() -> list[str]:
    documented: list[str] = []
    for module_name in MODULES:
        module = importlib.import_module(module_name)
        for name, obj in sorted(vars(module).items()):
            if name.startswith("_"):
                continue
            if inspect.isfunction(obj) and obj.__module__ == module.__name__:
                documented.append(f"{module_name}.{name}")
            elif inspect.isclass(obj) and obj.__module__ == module.__name__:
                for member_name, raw in sorted(vars(obj).items()):
                    if member_name.startswith("_"):
                        continue
                    if isinstance(
                        raw, (staticmethod, classmethod)
                    ) or inspect.isfunction(raw):
                        documented.append(f"{module_name}.{obj.__name__}.{member_name}")
    return documented


def case_inventory() -> set[str]:
    from benchmarks.catalog import iter_cases

    return {case.benchmark_base_name for case in iter_cases(suites=["public-api"])}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="emit a JSON report")
    args = parser.parse_args()

    documented = sorted(iter_documented_callables())
    inventory = case_inventory()

    missing_cases = {
        callable_id: [
            benchmark_name
            for benchmark_name in benchmark_names
            if benchmark_name not in inventory
        ]
        for callable_id, benchmark_names in NATIVE_BENCHMARK_COVERAGE.items()
        if any(benchmark_name not in inventory for benchmark_name in benchmark_names)
    }
    if missing_cases:
        raise SystemExit(
            "coverage manifest references missing benchmark case(s): "
            + json.dumps(missing_cases, indent=2, sort_keys=True)
        )

    excluded = {
        callable_id: EXCLUDED_REASON
        for callable_id in documented
        if callable_id not in NATIVE_BENCHMARK_COVERAGE
    }

    report = {
        "documented_callable_count": len(documented),
        "native_benchmarked_callable_count": len(NATIVE_BENCHMARK_COVERAGE),
        "excluded_callable_count": len(excluded),
        "inventory_case_count": len(inventory),
        "excluded": excluded,
    }

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(f"documented_callable_count: {report['documented_callable_count']}")
        print(
            "native_benchmarked_callable_count: "
            f"{report['native_benchmarked_callable_count']}"
        )
        print(f"excluded_callable_count: {report['excluded_callable_count']}")
        print(f"inventory_case_count: {report['inventory_case_count']}")
        if excluded:
            print("excluded:")
            for callable_id, reason in excluded.items():
                print(f"- {callable_id}: {reason}")
        else:
            print("excluded: none")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
