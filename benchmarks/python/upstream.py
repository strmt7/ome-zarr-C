from __future__ import annotations

import importlib
import json
import sys
from itertools import cycle
from pathlib import Path
from typing import Any

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
UPSTREAM_ROOT = ROOT / "source_code_v.0.15.0"
if str(UPSTREAM_ROOT) not in sys.path:
    sys.path.insert(0, str(UPSTREAM_ROOT))

axes = importlib.import_module("ome_zarr.axes")
conversions = importlib.import_module("ome_zarr.conversions")
csv = importlib.import_module("ome_zarr.csv")
data = importlib.import_module("ome_zarr.data")
fmt = importlib.import_module("ome_zarr.format")
utils = importlib.import_module("ome_zarr.utils")


def touch(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return float(len(value))
    if isinstance(value, dict):
        return float(len(value)) + sum(
            touch(key) + touch(item) for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return float(len(value)) + sum(touch(item) for item in value)
    if isinstance(value, np.ndarray):
        return float(value.size) + float(np.asarray(value).sum())
    return float(len(repr(value)))


def bench_axes_constructor() -> float:
    formats = [
        fmt.FormatV01(),
        fmt.FormatV02(),
        fmt.FormatV03(),
        fmt.FormatV04(),
        fmt.FormatV05(),
    ]
    axis_cases = [
        None,
        ["y", "x"],
        ["z", "y", "x"],
        ["t", "c", "z", "y", "x"],
        [
            {"name": "t", "type": "time"},
            {"name": "c", "type": "channel"},
            {"name": "y", "type": "space"},
            {"name": "x", "type": "space"},
        ],
    ]
    total = 0.0
    for format_obj, axis_input in zip(cycle(formats), axis_cases * 4, strict=False):
        try:
            instance = axes.Axes(axis_input, format_obj)
            total += touch(instance.to_list(format_obj))
        except Exception as exc:  # noqa: BLE001
            total += touch(type(exc).__name__) + touch(str(exc))
    return total


def bench_conversions_int_to_rgba() -> float:
    values = [0, 1, -1, 100100, -2_147_483_648, 2_147_483_647]
    return sum(touch(conversions.int_to_rgba(value)) for value in values)


def bench_conversions_rgba_to_int() -> float:
    values = [(0, 0, 0, 0), (255, 0, 0, 255), (17, 34, 51, 68), (255, 255, 255, 255)]
    return sum(touch(conversions.rgba_to_int(*rgba)) for rgba in values)


def bench_csv_parse_value() -> float:
    cases = [
        ("", "s"),
        ("0", "l"),
        ("1.5", "d"),
        ("abc", "s"),
        ("True", "b"),
        ("nan", "d"),
        ("inf", "d"),
        ("-inf", "d"),
    ]
    return sum(touch(csv.parse_csv_value(value, kind)) for value, kind in cases)


def bench_data_make_circle() -> float:
    total = 0.0
    for height, width, value, dtype in [
        (8, 8, 1, np.uint16),
        (9, 5, 7, np.int16),
        (12, 16, 2, np.uint8),
    ]:
        target = np.zeros((height, width), dtype=dtype)
        data.make_circle(height, width, value, target)
        total += touch(target)
    return total


def bench_data_rgb_to_5d() -> float:
    arrays = [
        np.arange(16, dtype=np.uint8).reshape(4, 4),
        np.arange(4 * 5 * 3, dtype=np.uint8).reshape(4, 5, 3),
        np.arange(3 * 4 * 3, dtype=np.uint8).reshape(3, 4, 3),
    ]
    return sum(touch(data.rgb_to_5d(array)) for array in arrays)


def bench_format_dispatch() -> float:
    total = 0.0
    for version in ["0.1", "0.2", "0.3", "0.4", "0.5"]:
        total += touch(fmt.format_from_version(version).version)
    total += touch([item.version for item in fmt.format_implementations()])
    return total


def bench_format_matches() -> float:
    metadata_cases = [
        {"multiscales": [{"version": "0.4"}]},
        {"ome": {"version": "0.5", "multiscales": []}},
        {"plate": {"version": "0.4"}},
        {"well": {"version": "0.4"}},
    ]
    formats = [
        fmt.FormatV01(),
        fmt.FormatV02(),
        fmt.FormatV03(),
        fmt.FormatV04(),
        fmt.FormatV05(),
    ]
    return sum(
        touch(format_obj.matches(json.loads(json.dumps(metadata))))
        for format_obj in formats
        for metadata in metadata_cases
    )


def bench_format_v01_init_store() -> float:
    plan_inputs = [
        ("https://example.org/demo.zarr", "r"),
        ("s3://bucket/demo.zarr", "r"),
        ("/tmp/demo.zarr", "w"),
    ]
    total = 0.0
    for path, mode in plan_inputs:
        try:
            store = fmt.FormatV01().init_store(path, mode=mode)
            total += touch(type(store).__name__)
        except Exception as exc:  # noqa: BLE001
            total += touch(type(exc).__name__) + touch(str(exc))
    return total


def bench_format_well_and_coord() -> float:
    format_v04 = fmt.FormatV04()
    transformations = format_v04.generate_coordinate_transformations(
        [[256, 256], [128, 128], [64, 64]]
    )
    well = format_v04.generate_well_dict("B/3", ["A", "B", "C"], ["1", "2", "3"])
    format_v04.validate_coordinate_transformations(2, 3, transformations)
    format_v04.validate_well_dict(well, ["A", "B", "C"], ["1", "2", "3"])
    return touch(transformations) + touch(well)


def bench_utils_path_helpers() -> float:
    parts = [["root", "a", "b"], ["root", "a", "c"], ["root", "a", "d"]]
    return touch(utils.strip_common_prefix(parts)) + touch(
        [utils.splitall(path) for path in ["a/b/c", "/tmp/demo.zarr", "s3://bucket/a"]]
    )
