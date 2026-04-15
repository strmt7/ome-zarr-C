#!/usr/bin/env python3
"""Enforce benchmark coverage for the documented ome_zarr public API."""

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
    "ome_zarr.cli",
    "ome_zarr.csv",
    "ome_zarr.data",
    "ome_zarr.format",
    "ome_zarr.io",
    "ome_zarr.reader",
    "ome_zarr.scale",
    "ome_zarr.utils",
    "ome_zarr.writer",
)

EXCLUDED = {
    "ome_zarr.cli.config_logging": (
        "Python-only logging bootstrap; standalone product uses native CLI runtime"
    ),
    "ome_zarr.format.Format.generate_coordinate_transformations": (
        "abstract base contract; benchmark concrete implementations instead"
    ),
    "ome_zarr.format.Format.generate_well_dict": (
        "abstract base contract; benchmark concrete implementations instead"
    ),
    "ome_zarr.format.Format.init_channels": (
        "abstract base contract on an abstract class; no concrete direct entrypoint"
    ),
    "ome_zarr.format.Format.init_store": (
        "abstract base contract; benchmark concrete implementations instead"
    ),
    "ome_zarr.format.Format.matches": (
        "abstract base contract; benchmark concrete implementations instead"
    ),
    "ome_zarr.format.Format.validate_coordinate_transformations": (
        "abstract base contract; benchmark concrete implementations instead"
    ),
    "ome_zarr.format.Format.validate_well_dict": (
        "abstract base contract; benchmark concrete implementations instead"
    ),
    "ome_zarr.reader.Spec.matches": (
        "abstract static contract; benchmark concrete spec predicates instead"
    ),
}

COVERAGE = {
    "ome_zarr.cli.create": ["cli.create_wrapper"],
    "ome_zarr.cli.csv_to_labels": ["csv.csv_to_zarr"],
    "ome_zarr.cli.download": ["cli.download_wrapper"],
    "ome_zarr.cli.finder": ["utils.finder"],
    "ome_zarr.cli.info": ["cli.info_wrapper"],
    "ome_zarr.cli.main": ["runtime.cli.create_info_v05", "runtime.cli.download_v05"],
    "ome_zarr.cli.scale": ["cli.scale_wrapper"],
    "ome_zarr.cli.view": ["utils.view"],
    "ome_zarr.csv.csv_to_zarr": ["csv.csv_to_zarr"],
    "ome_zarr.csv.dict_to_zarr": ["csv.dict_to_zarr"],
    "ome_zarr.csv.parse_csv_value": ["csv.parse_csv_value"],
    "ome_zarr.data.astronaut": ["runtime.data.create_zarr_astronaut_v05"],
    "ome_zarr.data.coins": ["runtime.data.create_zarr_coins_v05"],
    "ome_zarr.data.create_zarr": [
        "runtime.data.create_zarr_coins_v05",
        "runtime.data.create_zarr_astronaut_v05",
    ],
    "ome_zarr.data.make_circle": ["data.make_circle", "micro.data.make_circle_batch"],
    "ome_zarr.data.rgb_to_5d": ["data.rgb_to_5d", "micro.data.rgb_to_5d_batch"],
    "ome_zarr.format.detect_format": [
        "format.dispatch",
        "micro.format.detect_format_batch",
    ],
    "ome_zarr.format.format_from_version": ["format.dispatch"],
    "ome_zarr.format.format_implementations": ["format.dispatch"],
    "ome_zarr.format.FormatV01.generate_coordinate_transformations": [
        "format.well_and_coord"
    ],
    "ome_zarr.format.FormatV01.generate_well_dict": ["format.well_and_coord"],
    "ome_zarr.format.FormatV01.init_store": ["format.v01_init_store"],
    "ome_zarr.format.FormatV01.matches": ["format.matches"],
    "ome_zarr.format.FormatV01.validate_coordinate_transformations": [
        "format.well_and_coord"
    ],
    "ome_zarr.format.FormatV01.validate_well_dict": ["format.well_and_coord"],
    "ome_zarr.format.FormatV04.generate_coordinate_transformations": [
        "format.well_and_coord",
        "micro.format.coordinate_transformations_batch",
    ],
    "ome_zarr.format.FormatV04.generate_well_dict": ["format.well_and_coord"],
    "ome_zarr.format.FormatV04.matches": ["format.matches"],
    "ome_zarr.format.FormatV04.validate_coordinate_transformations": [
        "format.well_and_coord",
        "micro.format.coordinate_transformations_batch",
    ],
    "ome_zarr.format.FormatV04.validate_well_dict": ["format.well_and_coord"],
    "ome_zarr.format.FormatV02.matches": ["format.matches"],
    "ome_zarr.format.FormatV03.matches": ["format.matches"],
    "ome_zarr.format.FormatV05.matches": ["format.matches"],
    "ome_zarr.io.parse_url": [
        "runtime.io.parse_url_v2_image",
        "runtime.io.parse_url_v3_image",
    ],
    "ome_zarr.io.ZarrLocation.basename": ["io.location_methods"],
    "ome_zarr.io.ZarrLocation.create": ["io.create_load"],
    "ome_zarr.io.ZarrLocation.exists": ["io.location_methods"],
    "ome_zarr.io.ZarrLocation.load": ["io.create_load"],
    "ome_zarr.io.ZarrLocation.parts": ["io.location_methods"],
    "ome_zarr.io.ZarrLocation.subpath": ["io.location_methods"],
    "ome_zarr.reader.Label.matches": ["reader.matches"],
    "ome_zarr.reader.Labels.matches": ["reader.matches"],
    "ome_zarr.reader.Multiscales.array": ["reader.image_surface"],
    "ome_zarr.reader.Multiscales.matches": ["reader.matches"],
    "ome_zarr.reader.Node.add": ["reader.node_ops"],
    "ome_zarr.reader.Node.first": ["reader.node_ops"],
    "ome_zarr.reader.Node.load": ["reader.node_ops"],
    "ome_zarr.reader.Node.write_metadata": ["reader.node_ops"],
    "ome_zarr.reader.OMERO.matches": ["reader.matches"],
    "ome_zarr.reader.Plate.get_numpy_type": ["reader.plate_surface"],
    "ome_zarr.reader.Plate.get_pyramid_lazy": ["reader.plate_surface"],
    "ome_zarr.reader.Plate.get_stitched_grid": ["reader.plate_surface"],
    "ome_zarr.reader.Plate.get_tile_path": ["reader.plate_surface"],
    "ome_zarr.reader.Plate.matches": ["reader.matches"],
    "ome_zarr.reader.Reader.descend": ["reader.image_surface"],
    "ome_zarr.reader.Spec.lookup": ["reader.image_surface"],
    "ome_zarr.reader.Well.matches": ["reader.matches"],
    "ome_zarr.scale.Scaler.gaussian": ["scale.scaler_gaussian"],
    "ome_zarr.scale.Scaler.laplacian": ["scale.scaler_laplacian"],
    "ome_zarr.scale.Scaler.local_mean": ["meso.scaler.local_mean_rgb"],
    "ome_zarr.scale.Scaler.methods": ["scale.scaler_methods"],
    "ome_zarr.scale.Scaler.nearest": ["meso.scaler.nearest_rgb"],
    "ome_zarr.scale.Scaler.resize_image": ["scale.scaler_resize_image"],
    "ome_zarr.scale.Scaler.scale": ["cli.scale_wrapper"],
    "ome_zarr.scale.Scaler.zoom": ["scale.scaler_zoom"],
    "ome_zarr.utils.download": ["utils.download"],
    "ome_zarr.utils.find_multiscales": ["utils.find_multiscales"],
    "ome_zarr.utils.finder": ["utils.finder"],
    "ome_zarr.utils.info": [
        "runtime.utils.info_v2_image",
        "runtime.utils.info_v3_image_with_stats",
    ],
    "ome_zarr.utils.splitall": ["utils.path_helpers"],
    "ome_zarr.utils.strip_common_prefix": ["utils.path_helpers"],
    "ome_zarr.utils.view": ["utils.view"],
    "ome_zarr.writer.add_metadata": ["writer.metadata_helpers"],
    "ome_zarr.writer.check_format": ["writer.group_helpers"],
    "ome_zarr.writer.check_group_fmt": ["writer.group_helpers"],
    "ome_zarr.writer.get_metadata": ["writer.metadata_helpers"],
    "ome_zarr.writer.write_image": [
        "runtime.writer.write_image_v05_numpy",
        "runtime.writer.write_image_v05_delayed",
    ],
    "ome_zarr.writer.write_label_metadata": ["writer.metadata_writers"],
    "ome_zarr.writer.write_labels": ["writer.write_labels"],
    "ome_zarr.writer.write_multiscale": ["writer.write_multiscale"],
    "ome_zarr.writer.write_multiscale_labels": ["writer.write_multiscale_labels"],
    "ome_zarr.writer.write_multiscales_metadata": ["writer.metadata_writers"],
    "ome_zarr.writer.write_plate_metadata": ["writer.metadata_writers"],
    "ome_zarr.writer.write_well_metadata": ["writer.metadata_writers"],
}


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
                        raw,
                        (staticmethod, classmethod),
                    ) or inspect.isfunction(raw):
                        documented.append(f"{module_name}.{obj.__name__}.{member_name}")
    return documented


def case_inventory() -> set[str]:
    from benchmarks.catalog import iter_cases

    return {case.benchmark_base_name for case in iter_cases()}


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
        for callable_id, benchmark_names in COVERAGE.items()
        if any(benchmark_name not in inventory for benchmark_name in benchmark_names)
    }
    if missing_cases:
        raise SystemExit(
            "coverage manifest references missing benchmark case(s): "
            + json.dumps(missing_cases, indent=2, sort_keys=True)
        )

    uncovered = [
        callable_id
        for callable_id in documented
        if callable_id not in EXCLUDED and callable_id not in COVERAGE
    ]

    report = {
        "documented_callable_count": len(documented),
        "excluded_callable_count": len(EXCLUDED),
        "covered_callable_count": len(COVERAGE),
        "inventory_case_count": len(inventory),
        "uncovered": uncovered,
        "excluded": EXCLUDED,
    }

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(f"documented_callable_count: {report['documented_callable_count']}")
        print(f"excluded_callable_count: {report['excluded_callable_count']}")
        print(f"covered_callable_count: {report['covered_callable_count']}")
        print(f"inventory_case_count: {report['inventory_case_count']}")
        if uncovered:
            print("uncovered:")
            for callable_id in uncovered:
                print(f"- {callable_id}")
        else:
            print("uncovered: none")

    return 1 if uncovered else 0


if __name__ == "__main__":
    raise SystemExit(main())
