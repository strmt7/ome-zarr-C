from __future__ import annotations

import hashlib
import importlib
import json
import math
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
import warnings
from collections.abc import Callable
from contextlib import ExitStack, nullcontext, redirect_stderr, redirect_stdout
from dataclasses import dataclass, fields, is_dataclass, replace
from functools import lru_cache
from io import StringIO
from pathlib import Path
from typing import Any

import dask
import dask.array as da
import numpy as np

from benchmarks.runtime_support import (
    rewrite_snapshot_prefix,
    run_cli_main,
    run_create_zarr,
    run_info,
    run_parse_url,
    run_write_image,
    snapshot_tree,
    write_minimal_v2_image,
    write_minimal_v3_image,
)
from tests import test_axes_equivalence as axes_eq
from tests import test_cli_equivalence as cli_eq
from tests import test_data_equivalence as data_eq
from tests import test_data_runtime_equivalence as data_rt
from tests import test_utils_equivalence as utils_eq

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM_ROOT = ROOT / "source_code_v.0.15.0"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(UPSTREAM_ROOT) not in sys.path:
    sys.path.insert(0, str(UPSTREAM_ROOT))

_py_axes = importlib.import_module("ome_zarr.axes")
_py_conversions = importlib.import_module("ome_zarr.conversions")
_py_data = importlib.import_module("ome_zarr.data")
_py_dask_utils = importlib.import_module("ome_zarr.dask_utils")
_py_format = importlib.import_module("ome_zarr.format")
_py_io = importlib.import_module("ome_zarr.io")
_py_cli = importlib.import_module("ome_zarr.cli")
_py_utils = importlib.import_module("ome_zarr.utils")
_py_scale = importlib.import_module("ome_zarr.scale")
_py_writer = importlib.import_module("ome_zarr.writer")

_cpp_dask_utils = importlib.import_module("ome_zarr_c.dask_utils")
_cpp_format = importlib.import_module("ome_zarr_c.format")
_cpp_io = importlib.import_module("ome_zarr_c.io")
_cpp_scale = importlib.import_module("ome_zarr_c.scale")
_cpp_writer = importlib.import_module("ome_zarr_c.writer")


@dataclass(frozen=True)
class BenchmarkCase:
    group: str
    name: str
    description: str
    verify: Callable[[], None]
    python_timer: Callable[[int], float]
    cpp_timer: Callable[[int], float]

    @property
    def benchmark_base_name(self) -> str:
        return f"{self.group}.{self.name}"


_VERIFIED_CASES: set[str] = set()
_RNG = random.Random(20260412)
_FLOAT_CASE_MULTIPLIER = 10_000.0


def _normalize_snapshot_tree(tree):
    normalized = []
    for kind, rel_path, payload in tree:
        if kind == "json" and isinstance(payload, str):
            normalized.append((kind, rel_path, json.loads(payload)))
        else:
            normalized.append((kind, rel_path, payload))
    return normalized


def benchmark_environment_metadata() -> dict[str, str]:
    thread_settings = ",".join(
        f"{key}={os.environ.get(key, '')}"
        for key in (
            "OMP_NUM_THREADS",
            "OPENBLAS_NUM_THREADS",
            "MKL_NUM_THREADS",
            "NUMEXPR_NUM_THREADS",
        )
    )
    return {
        "benchmark_scope": "verified-native-backed-kernels-and-runtime-local-tempdir",
        "dask_scheduler": "single-threaded",
        "thread_env": thread_settings,
        "upstream_snapshot": "ome-zarr-py v0.15.0",
    }


def _make_case(
    group: str,
    name: str,
    description: str,
    verify: Callable[[], None],
    python_func: Callable[[], object],
    cpp_func: Callable[[], object],
) -> BenchmarkCase:
    case_id = f"{group}.{name}"
    return BenchmarkCase(
        group=group,
        name=name,
        description=description,
        verify=verify,
        python_timer=_make_timer(case_id, verify, python_func),
        cpp_timer=_make_timer(case_id, verify, cpp_func),
    )


def _make_timer(
    case_id: str,
    verify: Callable[[], None],
    func: Callable[[], object],
) -> Callable[[int], float]:
    def timer(loops: int) -> float:
        _verify_once(case_id, verify)
        start = time.perf_counter()
        with _bench_stdio_context():
            for _ in range(loops):
                func()
        return time.perf_counter() - start

    return timer


def _native_bench_timer(
    case_id: str,
    verify: Callable[[], None],
    native_match: str,
) -> Callable[[int], float]:
    def timer(loops: int) -> float:
        _verify_once(case_id, verify)
        bench_path = utils_eq._native_cli_path().with_name("ome_zarr_native_bench_core")
        if not bench_path.exists():
            bench_path = bench_path.with_suffix(".exe")
        with tempfile.TemporaryDirectory(prefix="ome-zarr-c-native-bench-") as temp_dir:
            json_path = Path(temp_dir) / "native.json"
            completed = subprocess.run(
                [
                    str(bench_path),
                    "--match",
                    native_match,
                    "--rounds",
                    "1",
                    "--iterations",
                    str(max(1, loops)),
                    "--json-output",
                    str(json_path),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            del completed
            payload = json.loads(json_path.read_text(encoding="utf-8"))
        results = payload.get("results", [])
        if len(results) != 1:
            raise AssertionError(
                "Expected exactly one native benchmark result for "
                f"{native_match}, got {len(results)}"
            )
        item = results[0]
        return float(item["median_us_per_op"]) * float(item["iterations"]) / 1_000_000.0

    return timer


def _verify_once(case_id: str, verify: Callable[[], None]) -> None:
    if case_id in _VERIFIED_CASES:
        return
    with _bench_stdio_context():
        verify()
    _VERIFIED_CASES.add(case_id)


def _bench_stdio_context():
    if os.environ.get("OME_ZARR_BENCH_SUPPRESS_STDIO", "1").lower() in {
        "0",
        "false",
        "no",
    }:
        return nullcontext()
    sink = StringIO()
    stack = ExitStack()
    stack.enter_context(redirect_stdout(sink))
    stack.enter_context(redirect_stderr(sink))
    return stack


def _compute_dask(array: da.Array) -> np.ndarray:
    with dask.config.set(scheduler="single-threaded"):
        return np.asarray(array.compute())


def _format_signature(fmt: Any) -> tuple[str, int, dict[str, str], str]:
    return (
        str(fmt.version),
        int(fmt.zarr_format),
        dict(fmt.chunk_key_encoding),
        repr(fmt),
    )


def _digest_array(array: np.ndarray) -> tuple[tuple[int, ...], str, str]:
    contiguous = np.ascontiguousarray(array)
    return (
        tuple(int(dim) for dim in contiguous.shape),
        str(contiguous.dtype),
        hashlib.sha256(contiguous.tobytes()).hexdigest(),
    )


def _canonicalize(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        return ("bytes", len(raw), hashlib.sha256(raw).hexdigest())
    if isinstance(value, da.Array):
        return ("dask", _digest_array(_compute_dask(value)))
    if isinstance(value, np.ndarray):
        return ("ndarray", _digest_array(np.asarray(value)))
    if isinstance(value, np.generic):
        return _canonicalize(value.item())
    if isinstance(value, bool):
        return value
    if isinstance(value, float):
        if math.isnan(value):
            return ("float", "nan")
        if math.isinf(value):
            return ("float", "inf" if value > 0 else "-inf")
        return ("float", value)
    if isinstance(value, type):
        return ("type", value.__module__, value.__qualname__)
    if isinstance(value, (int, str, type(None))):
        return value
    if isinstance(value, dict):
        return {
            key: _canonicalize(value[key])
            for key in sorted(value, key=lambda item: repr(item))
        }
    if is_dataclass(value):
        return {
            field.name: _canonicalize(getattr(value, field.name))
            for field in fields(value)
        }
    if isinstance(value, list):
        return [_canonicalize(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_canonicalize(item) for item in value)
    if hasattr(value, "version") and hasattr(value, "zarr_format"):
        return ("format", _format_signature(value))
    raise TypeError(f"Unsupported benchmark value: {type(value)!r}")


def _assert_parity(case_name: str, py_value: object, cpp_value: object) -> None:
    left = _canonicalize(py_value)
    right = _canonicalize(cpp_value)
    if left != right:
        raise AssertionError(
            f"Benchmark case {case_name} lost parity.\npython={left!r}\ncpp={right!r}"
        )


def _touch(value: Any) -> float:
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        if not raw:
            return 0.0
        return float(len(raw)) + float(raw[0]) + float(raw[-1])
    if isinstance(value, da.Array):
        return _touch(_compute_dask(value))
    if isinstance(value, np.ndarray):
        array = np.asarray(value)
        if array.size == 0:
            return 0.0
        flat = array.reshape(-1)
        return float(array.size) + float(flat[0]) + float(flat[-1])
    if isinstance(value, np.generic):
        return _touch(value.item())
    if value is None:
        return 0.0
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, int):
        return float(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return 0.0
        return value
    if isinstance(value, str):
        return float(len(value))
    if isinstance(value, type):
        return float(len(value.__module__) + len(value.__qualname__))
    if isinstance(value, dict):
        return sum(
            _touch(value[key]) for key in sorted(value, key=lambda item: repr(item))
        )
    if is_dataclass(value):
        return sum(_touch(getattr(value, field.name)) for field in fields(value))
    if isinstance(value, (list, tuple)):
        return sum(_touch(item) for item in value)
    if hasattr(value, "version") and hasattr(value, "zarr_format"):
        version, zarr_format, chunk_key_encoding, repr_value = _format_signature(value)
        return (
            float(len(version))
            + float(zarr_format)
            + float(len(chunk_key_encoding))
            + float(len(repr_value))
        )
    raise TypeError(f"Unsupported benchmark touch value: {type(value)!r}")


def _touch_outcome(outcome: Any) -> float:
    total = 0.0
    total += _touch(outcome.value)
    total += _touch(outcome.payload)
    total += _touch(outcome.stdout)
    total += _touch(outcome.records)
    total += _touch(outcome.tree)
    return total


def _float_payload(value: float) -> float:
    return round(value * _FLOAT_CASE_MULTIPLIER) / _FLOAT_CASE_MULTIPLIER


_INT_RGBA_VALUES = tuple(_RNG.randint(-(2**31), 2**31 - 1) for _ in range(8_192))
_RGBA_VALUES = tuple(
    tuple(_RNG.randint(0, 255) for _ in range(4)) for _ in range(8_192)
)

_AXES_CASES = (
    (None, _py_format.FormatV01(), _cpp_format.FormatV01()),
    (None, _py_format.FormatV02(), _cpp_format.FormatV02()),
    (["y", "x"], _py_format.FormatV03(), _cpp_format.FormatV03()),
    (["y", "x"], _py_format.FormatV04(), _cpp_format.FormatV04()),
    (["y", "x"], _py_format.FormatV05(), _cpp_format.FormatV05()),
    (["z", "y", "x"], _py_format.FormatV03(), _cpp_format.FormatV03()),
    (["z", "y", "x"], _py_format.FormatV04(), _cpp_format.FormatV04()),
    (
        ["t", "c", "z", "y", "x"],
        _py_format.FormatV05(),
        _cpp_format.FormatV05(),
    ),
    (
        [
            {"name": "z", "type": "space"},
            {"name": "y", "type": "space"},
            {"name": "x", "type": "space"},
        ],
        _py_format.FormatV04(),
        _cpp_format.FormatV04(),
    ),
    (
        [
            {"name": "t", "type": "time"},
            {"name": "c", "type": "channel"},
            {"name": "y", "type": "space"},
            {"name": "x", "type": "space"},
        ],
        _py_format.FormatV05(),
        _cpp_format.FormatV05(),
    ),
)

_FORMAT_METADATA_CASES = tuple(
    (
        {},
        {"multiscales": [{"version": "0.5"}]},
        {"multiscales": [{"version": 0.5}]},
        {"multiscales": [{"version": "0.4"}]},
        {"plate": {"version": "0.4"}},
        {"well": {"version": "0.3"}},
        {"image-label": {"version": "0.2"}},
        {"plate": {}},
    )
    * 1_024
)

_COORDINATE_SHAPE_CASES = tuple(
    (
        [(256, 256), (128, 128), (64, 64)],
        [(1, 3, 512, 512), (1, 3, 256, 256), (1, 3, 128, 128)],
        [(1, 1, 16, 32, 64), (1, 1, 8, 16, 32)],
    )
    * 256
)

_WRITER_AXES_CASES = (
    (2, ["y", "x"], _py_format.FormatV04(), _cpp_format.FormatV04()),
    (3, ["z", "y", "x"], _py_format.FormatV04(), _cpp_format.FormatV04()),
    (4, "czyx", _py_format.FormatV04(), _cpp_format.FormatV04()),
    (5, None, _py_format.FormatV01(), _cpp_format.FormatV01()),
    (2, None, _py_format.FormatV03(), _cpp_format.FormatV03()),
) * 256

_VALID_TRANSFORMATIONS = (
    [{"type": "scale", "scale": [1, 1]}],
    [{"type": "scale", "scale": [2, 2]}],
)
_WRITER_DATASET_CASES = (
    (
        [{"path": "0"}],
        2,
        _py_format.FormatV03(),
        _cpp_format.FormatV03(),
    ),
    (
        [
            {"path": "0", "coordinateTransformations": _VALID_TRANSFORMATIONS[0]},
            {"path": "1", "coordinateTransformations": _VALID_TRANSFORMATIONS[1]},
        ],
        2,
        _py_format.FormatV04(),
        _cpp_format.FormatV04(),
    ),
) * 256

_STORAGE_OPTIONS_CASES = (
    ({"chunks": (64, 64)}, 0),
    ({"chunks": (32, 32), "compressor": None}, 0),
    ([{"chunks": (128, 128)}, {"chunks": (64, 64)}], 1),
    ([{"chunks": (256, 256), "compressor": None}], 0),
) * 1_024

_MAKE_CIRCLE_CASES = (
    (8, 8, 1, np.float64),
    (9, 5, 7, np.int16),
    (12, 16, 2, np.uint8),
    (33, 21, 5, np.uint16),
) * 64

_RGB_TO_5D_CASES = (
    np.arange(16, dtype=np.uint8).reshape(4, 4),
    np.arange(4 * 5 * 3, dtype=np.uint8).reshape(4, 5, 3),
    np.arange(6 * 7 * 3, dtype=np.uint8).reshape(6, 7, 3),
) * 256


@lru_cache(maxsize=1)
def _resize_setup() -> tuple[da.Array, tuple[int, int], dict[str, Any]]:
    data = np.arange(512 * 768, dtype=np.uint16).reshape(512, 768)
    image = da.from_array(data, chunks=(192, 256))
    kwargs = {
        "order": 1,
        "mode": "reflect",
        "preserve_range": True,
        "anti_aliasing": False,
    }
    return image, (256, 384), kwargs


@lru_cache(maxsize=1)
def _local_mean_setup() -> tuple[da.Array, tuple[int, int, int]]:
    data = np.arange(8 * 192 * 192, dtype=np.float32).reshape(8, 192, 192)
    image = da.from_array(data, chunks=(4, 96, 96))
    return image, (4, 96, 96)


@lru_cache(maxsize=1)
def _zoom_setup() -> tuple[da.Array, tuple[int, int, int]]:
    data = np.arange(6 * 192 * 160, dtype=np.float32).reshape(6, 192, 160)
    image = da.from_array(data, chunks=(3, 96, 80))
    return image, (3, 96, 80)


@lru_cache(maxsize=1)
def _build_pyramid_setup() -> tuple[np.ndarray, list[dict[str, int]], tuple[str, ...]]:
    image = np.arange(2 * 3 * 128 * 128, dtype=np.float32).reshape(2, 3, 128, 128)
    scale_factors = [
        {"y": 2, "x": 2},
        {"y": 4, "x": 4},
    ]
    dims = ("c", "z", "y", "x")
    return image, scale_factors, dims


@lru_cache(maxsize=1)
def _scaler_image() -> np.ndarray:
    return np.arange(3 * 256 * 256, dtype=np.uint8).reshape(3, 256, 256)


@lru_cache(maxsize=1)
def _py_scalers() -> dict[str, object]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return {
            "nearest": _py_scale.Scaler(method="nearest", downscale=2, max_layer=3),
            "local_mean": _py_scale.Scaler(
                method="local_mean",
                downscale=2,
                max_layer=3,
            ),
        }


@lru_cache(maxsize=1)
def _cpp_scalers() -> dict[str, object]:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return {
            "nearest": _cpp_scale.Scaler(method="nearest", downscale=2, max_layer=3),
            "local_mean": _cpp_scale.Scaler(
                method="local_mean",
                downscale=2,
                max_layer=3,
            ),
        }


@lru_cache(maxsize=1)
def _runtime_source_dir() -> Path:
    return Path(tempfile.mkdtemp(prefix="ome-zarr-c-bench-src-"))


@lru_cache(maxsize=1)
def _runtime_v2_source() -> Path:
    root = _runtime_source_dir() / "image-v2.zarr"
    write_minimal_v2_image(root)
    return root


@lru_cache(maxsize=1)
def _runtime_v3_source() -> Path:
    root = _runtime_source_dir() / "image-v3.zarr"
    write_minimal_v3_image(root)
    return root


@lru_cache(maxsize=1)
def _runtime_cli_download_source() -> Path:
    root = _runtime_source_dir() / "cli-download-source.zarr"
    replacements = {str(root): "<SOURCE>"}
    random.seed(0)
    outcome = run_cli_main(
        _py_cli.main,
        ["create", "--method=astronaut", str(root), "--format", "0.5"],
        replacements,
    )
    _assert_outcome_ok("runtime.cli.download_v05.source", outcome)
    return root


def _fresh_runtime_dir(prefix: str) -> Path:
    return Path(tempfile.mkdtemp(prefix=f"ome-zarr-c-bench-{prefix}-"))


def _assert_outcome_ok(case_name: str, outcome: Any) -> None:
    if outcome.status != "ok":
        raise AssertionError(
            f"Benchmark case {case_name} failed unexpectedly: "
            f"{outcome.error_type} {outcome.error_message}"
        )


def _runtime_fmt_pair(version: str):
    if version == "0.4":
        return _py_format.FormatV04(), _cpp_format.FormatV04()
    return _py_format.FormatV05(), _cpp_format.FormatV05()


def _verify_axes_constructor() -> None:
    _assert_parity(
        "micro.axes.constructor_batch",
        [
            {
                "axes": getattr(_py_axes.Axes(axes_input, py_fmt), "axes", None),
                "to_list_03": _py_axes.Axes(axes_input, py_fmt).to_list(
                    _py_format.FormatV03()
                ),
                "to_list_04": _py_axes.Axes(axes_input, py_fmt).to_list(
                    _py_format.FormatV04()
                ),
            }
            for axes_input, py_fmt, _cpp_fmt in _AXES_CASES
        ],
        [
            axes_eq._run_native_axes(axes_input, cpp_fmt)
            for axes_input, _py_fmt, cpp_fmt in _AXES_CASES
        ],
    )


def _bench_axes_constructor_python() -> float:
    total = 0.0
    for axes_input, py_fmt, _cpp_fmt in _AXES_CASES:
        instance = _py_axes.Axes(axes_input, py_fmt)
        fmt03 = _py_format.FormatV03()
        fmt04 = _py_format.FormatV04()
        total += _touch(instance.to_list(fmt03))
        total += _touch(instance.to_list(fmt04))
    return total


def _verify_detect_format() -> None:
    _assert_parity(
        "micro.format.detect_format_batch",
        [
            _format_signature(
                _py_format.detect_format(metadata, _py_format.FormatV03())
            )
            for metadata in _FORMAT_METADATA_CASES
        ],
        [
            _format_signature(
                _cpp_format.detect_format(metadata, _cpp_format.FormatV03())
            )
            for metadata in _FORMAT_METADATA_CASES
        ],
    )


def _bench_detect_format(
    module: object, default_factory: Callable[[], object]
) -> float:
    total = 0.0
    default = default_factory()
    for metadata in _FORMAT_METADATA_CASES:
        total += _touch(module.detect_format(metadata, default))
    return total


def _verify_coordinate_transformations() -> None:
    py_fmt = _py_format.FormatV04()
    cpp_fmt = _cpp_format.FormatV04()
    _assert_parity(
        "micro.format.coordinate_transformations_batch",
        [
            (
                py_fmt.generate_coordinate_transformations(shapes),
                py_fmt.validate_coordinate_transformations(
                    len(shapes[0]),
                    len(shapes),
                    py_fmt.generate_coordinate_transformations(shapes),
                ),
            )
            for shapes in _COORDINATE_SHAPE_CASES
        ],
        [
            (
                cpp_fmt.generate_coordinate_transformations(shapes),
                cpp_fmt.validate_coordinate_transformations(
                    len(shapes[0]),
                    len(shapes),
                    cpp_fmt.generate_coordinate_transformations(shapes),
                ),
            )
            for shapes in _COORDINATE_SHAPE_CASES
        ],
    )


def _bench_coordinate_transformations(fmt: object) -> float:
    total = 0.0
    for shapes in _COORDINATE_SHAPE_CASES:
        generated = fmt.generate_coordinate_transformations(shapes)
        total += _touch(generated)
        total += _touch(
            fmt.validate_coordinate_transformations(
                len(shapes[0]),
                len(shapes),
                generated,
            )
        )
    return total


def _verify_get_valid_axes() -> None:
    _assert_parity(
        "micro.writer.get_valid_axes_batch",
        [
            _py_writer._get_valid_axes(ndim, axes, py_fmt)
            for ndim, axes, py_fmt, _cpp_fmt in _WRITER_AXES_CASES
        ],
        [
            _cpp_writer._get_valid_axes(ndim, axes, cpp_fmt)
            for ndim, axes, _py_fmt, cpp_fmt in _WRITER_AXES_CASES
        ],
    )


def _bench_get_valid_axes(py_like: bool) -> float:
    total = 0.0
    if py_like:
        for ndim, axes, py_fmt, _cpp_fmt in _WRITER_AXES_CASES:
            total += _touch(_py_writer._get_valid_axes(ndim, axes, py_fmt))
    else:
        for ndim, axes, _py_fmt, cpp_fmt in _WRITER_AXES_CASES:
            total += _touch(_cpp_writer._get_valid_axes(ndim, axes, cpp_fmt))
    return total


def _verify_validate_datasets() -> None:
    _assert_parity(
        "micro.writer.validate_datasets_batch",
        [
            _py_writer._validate_datasets(datasets, dims, py_fmt)
            for datasets, dims, py_fmt, _cpp_fmt in _WRITER_DATASET_CASES
        ],
        [
            _cpp_writer._validate_datasets(datasets, dims, cpp_fmt)
            for datasets, dims, _py_fmt, cpp_fmt in _WRITER_DATASET_CASES
        ],
    )


def _bench_validate_datasets(py_like: bool) -> float:
    total = 0.0
    if py_like:
        for datasets, dims, py_fmt, _cpp_fmt in _WRITER_DATASET_CASES:
            total += _touch(_py_writer._validate_datasets(datasets, dims, py_fmt))
    else:
        for datasets, dims, _py_fmt, cpp_fmt in _WRITER_DATASET_CASES:
            total += _touch(_cpp_writer._validate_datasets(datasets, dims, cpp_fmt))
    return total


def _verify_resolve_storage_options() -> None:
    _assert_parity(
        "micro.writer.resolve_storage_options_batch",
        [
            _py_writer._resolve_storage_options(storage_options, path)
            for storage_options, path in _STORAGE_OPTIONS_CASES
        ],
        [
            _cpp_writer._resolve_storage_options(storage_options, path)
            for storage_options, path in _STORAGE_OPTIONS_CASES
        ],
    )


def _bench_resolve_storage_options(module: object) -> float:
    total = 0.0
    for storage_options, path in _STORAGE_OPTIONS_CASES:
        total += _touch(module._resolve_storage_options(storage_options, path))
    return total


def _verify_make_circle() -> None:
    py_results = []
    native_results = []
    for height, width, value, dtype in _MAKE_CIRCLE_CASES:
        py_results.append(
            data_eq._run_make_circle(_py_data.make_circle, height, width, value, dtype)
        )
        native_results.append(
            data_eq._run_native_make_circle(
                target_shape=(height, width),
                circle_shape=(height, width),
                offset=(0, 0),
                value=value,
                dtype=dtype,
            )
        )
    _assert_parity("micro.data.make_circle_batch", py_results, native_results)


def _bench_make_circle_python() -> float:
    total = 0.0
    for height, width, value, dtype in _MAKE_CIRCLE_CASES:
        total += _touch_outcome(
            data_eq._run_make_circle(_py_data.make_circle, height, width, value, dtype)
        )
    return total


def _verify_rgb_to_5d() -> None:
    _assert_parity(
        "micro.data.rgb_to_5d_batch",
        [
            data_eq._run_rgb_to_5d(_py_data.rgb_to_5d, pixels)
            for pixels in _RGB_TO_5D_CASES
        ],
        [data_eq._run_native_rgb_to_5d(pixels) for pixels in _RGB_TO_5D_CASES],
    )


def _bench_rgb_to_5d_python() -> float:
    total = 0.0
    for pixels in _RGB_TO_5D_CASES:
        total += _touch_outcome(data_eq._run_rgb_to_5d(_py_data.rgb_to_5d, pixels))
    return total


def _verify_resize() -> None:
    image, output_shape, kwargs = _resize_setup()
    _assert_parity(
        "meso.dask_utils.resize_2d",
        _py_dask_utils.resize(image, output_shape, **kwargs),
        _cpp_dask_utils.resize(image, output_shape, **kwargs),
    )


def _bench_resize(module: object) -> float:
    image, output_shape, kwargs = _resize_setup()
    return _touch(module.resize(image, output_shape, **kwargs))


def _verify_local_mean() -> None:
    image, output_shape = _local_mean_setup()
    _assert_parity(
        "meso.dask_utils.local_mean_3d",
        _py_dask_utils.local_mean(image, output_shape),
        _cpp_dask_utils.local_mean(image, output_shape),
    )


def _bench_local_mean(module: object) -> float:
    image, output_shape = _local_mean_setup()
    return _touch(module.local_mean(image, output_shape))


def _verify_zoom() -> None:
    image, output_shape = _zoom_setup()
    _assert_parity(
        "meso.dask_utils.zoom_3d",
        _py_dask_utils.zoom(image, output_shape),
        _cpp_dask_utils.zoom(image, output_shape),
    )


def _bench_zoom(module: object) -> float:
    image, output_shape = _zoom_setup()
    return _touch(module.zoom(image, output_shape))


def _verify_build_pyramid() -> None:
    image, scale_factors, dims = _build_pyramid_setup()
    _assert_parity(
        "meso.scale.build_pyramid_local_mean",
        _py_scale._build_pyramid(
            image,
            scale_factors,
            dims=dims,
            method=_py_scale.Methods.LOCAL_MEAN,
        ),
        _cpp_scale._build_pyramid(
            image,
            scale_factors,
            dims=dims,
            method=_cpp_scale.Methods.LOCAL_MEAN,
        ),
    )


def _bench_build_pyramid(py_like: bool) -> float:
    image, scale_factors, dims = _build_pyramid_setup()
    if py_like:
        return _touch(
            _py_scale._build_pyramid(
                image,
                scale_factors,
                dims=dims,
                method=_py_scale.Methods.LOCAL_MEAN,
            )
        )
    return _touch(
        _cpp_scale._build_pyramid(
            image,
            scale_factors,
            dims=dims,
            method=_cpp_scale.Methods.LOCAL_MEAN,
        )
    )


def _verify_scaler_nearest() -> None:
    image = _scaler_image()
    _assert_parity(
        "meso.scaler.nearest_rgb",
        _py_scalers()["nearest"].nearest(image),
        _cpp_scalers()["nearest"].nearest(image),
    )


def _bench_scaler_nearest(py_like: bool) -> float:
    image = _scaler_image()
    scalers = _py_scalers() if py_like else _cpp_scalers()
    return _touch(scalers["nearest"].nearest(image))


def _verify_scaler_local_mean() -> None:
    image = _scaler_image()
    _assert_parity(
        "meso.scaler.local_mean_rgb",
        _py_scalers()["local_mean"].local_mean(image),
        _cpp_scalers()["local_mean"].local_mean(image),
    )


def _bench_scaler_local_mean(py_like: bool) -> float:
    image = _scaler_image()
    scalers = _py_scalers() if py_like else _cpp_scalers()
    return _touch(scalers["local_mean"].local_mean(image))


def _verify_parse_url_v2() -> None:
    assert run_parse_url(_py_io.parse_url, _runtime_v2_source()) == run_parse_url(
        _cpp_io.parse_url, _runtime_v2_source()
    )


def _verify_parse_url_v3() -> None:
    assert run_parse_url(_py_io.parse_url, _runtime_v3_source()) == run_parse_url(
        _cpp_io.parse_url, _runtime_v3_source()
    )


def _bench_parse_url(module: object, source: Path, case_name: str) -> float:
    outcome = run_parse_url(module.parse_url, source)
    _assert_outcome_ok(case_name, outcome)
    return _touch_outcome(outcome)


def _verify_info_v2() -> None:
    source = _runtime_v2_source()
    expected = run_info(_py_utils.info, source, stats=False)
    actual = cli_eq._run_native_cli(["info", str(source)], {})
    assert expected.status == actual.status == "ok"
    assert actual.payload["stderr"] == ""
    assert expected.stdout == actual.stdout


def _verify_info_v3_with_stats() -> None:
    source = _runtime_v3_source()
    expected = run_info(_py_utils.info, source, stats=True)
    actual = cli_eq._run_native_cli(["info", str(source), "--stats"], {})
    assert expected.status == actual.status == "ok"
    assert actual.payload["stderr"] == ""
    assert expected.stdout == actual.stdout


def _bench_info(module: object, source: Path, *, stats: bool, case_name: str) -> float:
    if module is _py_utils:
        outcome = run_info(module.info, source, stats=stats)
    else:
        args = ["info", str(source)]
        if stats:
            args.append("--stats")
        outcome = cli_eq._run_native_cli(args, {})
    _assert_outcome_ok(case_name, outcome)
    return _touch_outcome(outcome)


def _verify_cli_download_v05() -> None:
    py_source = _fresh_runtime_dir("py-cli-download") / "source.zarr"
    cpp_source = _fresh_runtime_dir("cpp-cli-download") / "source.zarr"
    py_output_root = _fresh_runtime_dir("py-cli-download-out")
    cpp_output_root = _fresh_runtime_dir("cpp-cli-download-out")
    replacements = {
        str(py_source): "<SOURCE>",
        str(cpp_source): "<SOURCE>",
        str(py_output_root): "<OUT>",
        str(cpp_output_root): "<OUT>",
    }
    try:
        random.seed(0)
        py_create = run_cli_main(
            _py_cli.main,
            ["create", "--method=astronaut", str(py_source), "--format", "0.5"],
            replacements,
        )
        random.seed(0)
        cpp_create = cli_eq._run_native_cli(
            ["create", "--method=astronaut", str(cpp_source), "--format", "0.5"],
            replacements,
        )
        assert py_create.status == cpp_create.status == "ok"
        assert py_create.stdout == cpp_create.stdout
        assert cpp_create.payload["stderr"] == ""
        expected = run_cli_main(
            _py_cli.main,
            ["download", str(py_source), f"--output={py_output_root}"],
            replacements,
        )
        actual = cli_eq._run_native_cli(
            ["download", str(cpp_source), f"--output={cpp_output_root}"],
            replacements,
        )
        assert expected.status == actual.status == "ok"
        assert utils_eq._normalize_download_stdout(
            expected.stdout, replacements
        ) == utils_eq._normalize_download_stdout(actual.stdout, replacements)
        assert actual.payload["stderr"] == ""
        assert rewrite_snapshot_prefix(
            snapshot_tree(py_output_root),
            py_source.name,
            "<SOURCE_ROOT>",
        ) == rewrite_snapshot_prefix(
            snapshot_tree(cpp_output_root),
            cpp_source.name,
            "<SOURCE_ROOT>",
        )
    finally:
        shutil.rmtree(py_source.parent, ignore_errors=True)
        shutil.rmtree(cpp_source.parent, ignore_errors=True)
        shutil.rmtree(py_output_root, ignore_errors=True)
        shutil.rmtree(cpp_output_root, ignore_errors=True)


def _bench_cli_download(py_like: bool, case_name: str) -> float:
    output_root = _fresh_runtime_dir("utils-download")
    source = _runtime_cli_download_source()
    replacements = {
        str(source): "<SOURCE>",
        str(output_root): "<OUT>",
    }
    try:
        if py_like:
            outcome = run_cli_main(
                _py_cli.main,
                ["download", str(source), f"--output={output_root}"],
                replacements,
            )
        else:
            outcome = cli_eq._run_native_cli(
                ["download", str(source), f"--output={output_root}"],
                replacements,
            )
        _assert_outcome_ok(case_name, outcome)
        return _touch_outcome(outcome) + _touch(snapshot_tree(output_root))
    finally:
        shutil.rmtree(output_root, ignore_errors=True)


def _verify_write_image_numpy_v05() -> None:
    image = np.arange(3 * 32 * 32, dtype=np.uint8).reshape(3, 32, 32)
    py_root = _fresh_runtime_dir("py-write-image") / "image.zarr"
    cpp_root = _fresh_runtime_dir("cpp-write-image") / "image.zarr"
    py_fmt, cpp_fmt = _runtime_fmt_pair("0.5")
    try:
        expected = run_write_image(
            _py_writer.write_image,
            py_root,
            image,
            str(py_root),
            fmt=py_fmt,
            axes="cyx",
            scale_factors=(2, 4),
        )
        actual = run_write_image(
            _cpp_writer.write_image,
            cpp_root,
            image,
            str(cpp_root),
            fmt=cpp_fmt,
            axes="cyx",
            scale_factors=(2, 4),
        )
        assert expected == actual
    finally:
        shutil.rmtree(py_root.parent, ignore_errors=True)
        shutil.rmtree(cpp_root.parent, ignore_errors=True)


def _bench_write_image_numpy(module: object, case_name: str) -> float:
    image = np.arange(3 * 32 * 32, dtype=np.uint8).reshape(3, 32, 32)
    root = _fresh_runtime_dir("write-image") / "image.zarr"
    fmt = _py_format.FormatV05() if module is _py_writer else _cpp_format.FormatV05()
    try:
        outcome = run_write_image(
            module.write_image,
            root,
            image,
            str(root),
            fmt=fmt,
            axes="cyx",
            scale_factors=(2, 4),
        )
        _assert_outcome_ok(case_name, outcome)
        return _touch_outcome(outcome)
    finally:
        shutil.rmtree(root.parent, ignore_errors=True)


def _verify_write_image_delayed_v05() -> None:
    image = da.from_array(
        np.arange(16, dtype=np.uint16).reshape(4, 4),
        chunks=(2, 2),
    )
    py_root = _fresh_runtime_dir("py-write-image-delayed") / "image.zarr"
    cpp_root = _fresh_runtime_dir("cpp-write-image-delayed") / "image.zarr"
    try:
        expected = run_write_image(
            _py_writer.write_image,
            py_root,
            image,
            str(py_root),
            fmt=_py_format.FormatV05(),
            axes=["y", "x"],
            scale_factors=(2,),
            compute=False,
        )
        actual = run_write_image(
            _cpp_writer.write_image,
            cpp_root,
            image,
            str(cpp_root),
            fmt=_cpp_format.FormatV05(),
            axes=["y", "x"],
            scale_factors=(2,),
            compute=False,
        )
        assert expected == actual
    finally:
        shutil.rmtree(py_root.parent, ignore_errors=True)
        shutil.rmtree(cpp_root.parent, ignore_errors=True)


def _bench_write_image_delayed(module: object, case_name: str) -> float:
    image = da.from_array(
        np.arange(16, dtype=np.uint16).reshape(4, 4),
        chunks=(2, 2),
    )
    root = _fresh_runtime_dir("write-image-delayed") / "image.zarr"
    fmt = _py_format.FormatV05() if module is _py_writer else _cpp_format.FormatV05()
    try:
        outcome = run_write_image(
            module.write_image,
            root,
            image,
            str(root),
            fmt=fmt,
            axes=["y", "x"],
            scale_factors=(2,),
            compute=False,
        )
        _assert_outcome_ok(case_name, outcome)
        return _touch_outcome(outcome)
    finally:
        shutil.rmtree(root.parent, ignore_errors=True)


def _verify_create_zarr_coins_v05() -> None:
    py_root = _fresh_runtime_dir("py-create-zarr") / "coins.zarr"
    cpp_root = _fresh_runtime_dir("cpp-create-zarr") / "coins.zarr"
    try:
        expected = run_create_zarr(
            _py_data.create_zarr,
            py_root,
            method=_py_data.coins,
            label_name="coins",
            fmt=_py_format.FormatV05(),
            seed=0,
        )
        actual = data_rt._run_native_create(
            cpp_root,
            method_name="coins",
            version="0.5",
            seed=0,
        )
        assert expected.status == actual.status == "ok"
        assert actual.payload["stderr"] == ""
        assert expected.tree == _normalize_snapshot_tree(actual.tree)
    finally:
        shutil.rmtree(py_root.parent, ignore_errors=True)
        shutil.rmtree(cpp_root.parent, ignore_errors=True)


def _bench_create_zarr_coins(py_like: bool, case_name: str) -> float:
    root = _fresh_runtime_dir("create-zarr-coins") / "coins.zarr"
    try:
        if py_like:
            outcome = run_create_zarr(
                _py_data.create_zarr,
                root,
                method=_py_data.coins,
                label_name="coins",
                fmt=_py_format.FormatV05(),
                seed=0,
            )
        else:
            outcome = data_rt._run_native_create(
                root,
                method_name="coins",
                version="0.5",
                seed=0,
            )
        _assert_outcome_ok(case_name, outcome)
        if not py_like:
            outcome = replace(outcome, tree=_normalize_snapshot_tree(outcome.tree))
        return _touch_outcome(outcome)
    finally:
        shutil.rmtree(root.parent, ignore_errors=True)


def _verify_create_zarr_astronaut_v05() -> None:
    py_root = _fresh_runtime_dir("py-create-zarr-astronaut") / "astronaut.zarr"
    cpp_root = _fresh_runtime_dir("cpp-create-zarr-astronaut") / "astronaut.zarr"
    try:
        expected = run_create_zarr(
            _py_data.create_zarr,
            py_root,
            method=_py_data.astronaut,
            label_name="circles",
            fmt=_py_format.FormatV05(),
            seed=0,
        )
        actual = data_rt._run_native_create(
            cpp_root,
            method_name="astronaut",
            version="0.5",
            seed=0,
        )
        assert expected.status == actual.status == "ok"
        assert actual.payload["stderr"] == ""
        assert expected.tree == _normalize_snapshot_tree(actual.tree)
    finally:
        shutil.rmtree(py_root.parent, ignore_errors=True)
        shutil.rmtree(cpp_root.parent, ignore_errors=True)


def _bench_create_zarr_astronaut(py_like: bool, case_name: str) -> float:
    root = _fresh_runtime_dir("create-zarr-astronaut") / "astronaut.zarr"
    try:
        if py_like:
            outcome = run_create_zarr(
                _py_data.create_zarr,
                root,
                method=_py_data.astronaut,
                label_name="circles",
                fmt=_py_format.FormatV05(),
                seed=0,
            )
        else:
            outcome = data_rt._run_native_create(
                root,
                method_name="astronaut",
                version="0.5",
                seed=0,
            )
        _assert_outcome_ok(case_name, outcome)
        if not py_like:
            outcome = replace(outcome, tree=_normalize_snapshot_tree(outcome.tree))
        return _touch_outcome(outcome)
    finally:
        shutil.rmtree(root.parent, ignore_errors=True)


def _verify_cli_create_info_v05() -> None:
    py_root = _fresh_runtime_dir("py-cli") / "cli-image.zarr"
    cpp_root = _fresh_runtime_dir("cpp-cli") / "cli-image.zarr"
    replacements = {
        str(py_root): "<ROOT>",
        str(cpp_root): "<ROOT>",
    }
    try:
        random.seed(0)
        py_create = run_cli_main(
            _py_cli.main,
            ["create", "--method=coins", str(py_root), "--format", "0.5"],
            replacements,
        )
        random.seed(0)
        cpp_create = cli_eq._run_native_cli(
            ["create", "--method=coins", str(cpp_root), "--format", "0.5"],
            replacements,
        )
        assert py_create.status == cpp_create.status == "ok"
        assert py_create.stdout == cpp_create.stdout
        assert cpp_create.payload["stderr"] == ""
        assert snapshot_tree(py_root) == snapshot_tree(cpp_root)
        expected_info = run_cli_main(
            _py_cli.main,
            ["info", str(py_root)],
            replacements,
        )
        actual_info = cli_eq._run_native_cli(
            ["info", str(cpp_root)],
            replacements,
        )
        assert expected_info.status == actual_info.status == "ok"
        assert expected_info.stdout == actual_info.stdout
        assert actual_info.payload["stderr"] == ""
    finally:
        shutil.rmtree(py_root.parent, ignore_errors=True)
        shutil.rmtree(cpp_root.parent, ignore_errors=True)


def _bench_cli_create_info(py_like: bool, case_name: str) -> float:
    root = _fresh_runtime_dir("cli-create-info") / "cli-image.zarr"
    replacements = {str(root): "<ROOT>"}
    try:
        random.seed(0)
        if py_like:
            create = run_cli_main(
                _py_cli.main,
                ["create", "--method=coins", str(root), "--format", "0.5"],
                replacements,
            )
            info = run_cli_main(
                _py_cli.main,
                ["info", str(root)],
                replacements,
            )
        else:
            create = cli_eq._run_native_cli(
                ["create", "--method=coins", str(root), "--format", "0.5"],
                replacements,
            )
            info = cli_eq._run_native_cli(
                ["info", str(root)],
                replacements,
            )
        _assert_outcome_ok(case_name, create)
        _assert_outcome_ok(case_name, info)
        return (
            _touch_outcome(create) + _touch_outcome(info) + _touch(snapshot_tree(root))
        )
    finally:
        shutil.rmtree(root.parent, ignore_errors=True)


ALL_CASES = (
    BenchmarkCase(
        group="micro",
        name="axes.constructor_batch",
        description="Axes normalization plus to_list conversion across valid formats.",
        verify=_verify_axes_constructor,
        python_timer=_make_timer(
            "micro.axes.constructor_batch",
            _verify_axes_constructor,
            _bench_axes_constructor_python,
        ),
        cpp_timer=_native_bench_timer(
            "micro.axes.constructor_batch",
            _verify_axes_constructor,
            "axes.constructor_batch",
        ),
    ),
    _make_case(
        "micro",
        "format.detect_format_batch",
        "Metadata format detection on mixed OME-Zarr metadata payloads.",
        _verify_detect_format,
        lambda: _bench_detect_format(_py_format, _py_format.FormatV03),
        lambda: _bench_detect_format(_cpp_format, _cpp_format.FormatV03),
    ),
    _make_case(
        "micro",
        "format.coordinate_transformations_batch",
        "Coordinate transformation generation plus validation roundtrip.",
        _verify_coordinate_transformations,
        lambda: _bench_coordinate_transformations(_py_format.FormatV04()),
        lambda: _bench_coordinate_transformations(_cpp_format.FormatV04()),
    ),
    _make_case(
        "micro",
        "writer.get_valid_axes_batch",
        "Writer axis validation on representative valid axis combinations.",
        _verify_get_valid_axes,
        lambda: _bench_get_valid_axes(True),
        lambda: _bench_get_valid_axes(False),
    ),
    _make_case(
        "micro",
        "writer.validate_datasets_batch",
        "Writer dataset validation with valid coordinate transformations.",
        _verify_validate_datasets,
        lambda: _bench_validate_datasets(True),
        lambda: _bench_validate_datasets(False),
    ),
    _make_case(
        "micro",
        "writer.resolve_storage_options_batch",
        "Writer storage option resolution for dict and per-level list inputs.",
        _verify_resolve_storage_options,
        lambda: _bench_resolve_storage_options(_py_writer),
        lambda: _bench_resolve_storage_options(_cpp_writer),
    ),
    BenchmarkCase(
        group="micro",
        name="data.make_circle_batch",
        description="Synthetic circle mask generation over mixed shapes and dtypes.",
        verify=_verify_make_circle,
        python_timer=_make_timer(
            "micro.data.make_circle_batch",
            _verify_make_circle,
            _bench_make_circle_python,
        ),
        cpp_timer=_native_bench_timer(
            "micro.data.make_circle_batch",
            _verify_make_circle,
            "data.make_circle_batch",
        ),
    ),
    BenchmarkCase(
        group="micro",
        name="data.rgb_to_5d_batch",
        description="RGB/greyscale normalization into 5D OME-Zarr video layout.",
        verify=_verify_rgb_to_5d,
        python_timer=_make_timer(
            "micro.data.rgb_to_5d_batch",
            _verify_rgb_to_5d,
            _bench_rgb_to_5d_python,
        ),
        cpp_timer=_native_bench_timer(
            "micro.data.rgb_to_5d_batch",
            _verify_rgb_to_5d,
            "data.rgb_to_5d_batch",
        ),
    ),
    _make_case(
        "meso",
        "dask_utils.resize_2d",
        "Chunk-aware 2D resize with explicit output shape and deterministic scheduler.",
        _verify_resize,
        lambda: _bench_resize(_py_dask_utils),
        lambda: _bench_resize(_cpp_dask_utils),
    ),
    _make_case(
        "meso",
        "dask_utils.local_mean_3d",
        "Chunk-aware 3D local-mean downsampling.",
        _verify_local_mean,
        lambda: _bench_local_mean(_py_dask_utils),
        lambda: _bench_local_mean(_cpp_dask_utils),
    ),
    _make_case(
        "meso",
        "dask_utils.zoom_3d",
        "Chunk-aware 3D scipy zoom downsampling.",
        _verify_zoom,
        lambda: _bench_zoom(_py_dask_utils),
        lambda: _bench_zoom(_cpp_dask_utils),
    ),
    _make_case(
        "meso",
        "scale.build_pyramid_local_mean",
        "High-level multiscale pyramid construction with local-mean method.",
        _verify_build_pyramid,
        lambda: _bench_build_pyramid(True),
        lambda: _bench_build_pyramid(False),
    ),
    _make_case(
        "meso",
        "scaler.nearest_rgb",
        "Deprecated Scaler.nearest on RGB-like in-memory data.",
        _verify_scaler_nearest,
        lambda: _bench_scaler_nearest(True),
        lambda: _bench_scaler_nearest(False),
    ),
    _make_case(
        "meso",
        "scaler.local_mean_rgb",
        "Deprecated Scaler.local_mean on RGB-like in-memory data.",
        _verify_scaler_local_mean,
        lambda: _bench_scaler_local_mean(True),
        lambda: _bench_scaler_local_mean(False),
    ),
    _make_case(
        "runtime",
        "io.parse_url_v2_image",
        (
            "Local-store parse_url and ZarrLocation state extraction for a "
            "minimal v2 image."
        ),
        _verify_parse_url_v2,
        lambda: _bench_parse_url(
            _py_io, _runtime_v2_source(), "runtime.io.parse_url_v2_image"
        ),
        lambda: _bench_parse_url(
            _cpp_io, _runtime_v2_source(), "runtime.io.parse_url_v2_image"
        ),
    ),
    _make_case(
        "runtime",
        "io.parse_url_v3_image",
        (
            "Local-store parse_url and ZarrLocation state extraction for a "
            "minimal v3 image."
        ),
        _verify_parse_url_v3,
        lambda: _bench_parse_url(
            _py_io, _runtime_v3_source(), "runtime.io.parse_url_v3_image"
        ),
        lambda: _bench_parse_url(
            _cpp_io, _runtime_v3_source(), "runtime.io.parse_url_v3_image"
        ),
    ),
    _make_case(
        "runtime",
        "utils.info_v2_image",
        "Recursive info traversal for a minimal v2 OME-Zarr image.",
        _verify_info_v2,
        lambda: _bench_info(
            _py_utils,
            _runtime_v2_source(),
            stats=False,
            case_name="runtime.utils.info_v2_image",
        ),
        lambda: _bench_info(
            object(),
            _runtime_v2_source(),
            stats=False,
            case_name="runtime.utils.info_v2_image",
        ),
    ),
    _make_case(
        "runtime",
        "utils.info_v3_image_with_stats",
        "Recursive info traversal plus stats for a minimal v3 OME-Zarr image.",
        _verify_info_v3_with_stats,
        lambda: _bench_info(
            _py_utils,
            _runtime_v3_source(),
            stats=True,
            case_name="runtime.utils.info_v3_image_with_stats",
        ),
        lambda: _bench_info(
            object(),
            _runtime_v3_source(),
            stats=True,
            case_name="runtime.utils.info_v3_image_with_stats",
        ),
    ),
    _make_case(
        "runtime",
        "cli.download_v05",
        "CLI download roundtrip for a format 0.5 source created on the same stack.",
        _verify_cli_download_v05,
        lambda: _bench_cli_download(True, "runtime.cli.download_v05"),
        lambda: _bench_cli_download(False, "runtime.cli.download_v05"),
    ),
    _make_case(
        "runtime",
        "writer.write_image_v05_numpy",
        "Filesystem-backed write_image for a NumPy RGB-like image in format 0.5.",
        _verify_write_image_numpy_v05,
        lambda: _bench_write_image_numpy(
            _py_writer, "runtime.writer.write_image_v05_numpy"
        ),
        lambda: _bench_write_image_numpy(
            _cpp_writer, "runtime.writer.write_image_v05_numpy"
        ),
    ),
    _make_case(
        "runtime",
        "writer.write_image_v05_delayed",
        "Filesystem-backed write_image for a delayed Dask image in format 0.5.",
        _verify_write_image_delayed_v05,
        lambda: _bench_write_image_delayed(
            _py_writer, "runtime.writer.write_image_v05_delayed"
        ),
        lambda: _bench_write_image_delayed(
            _cpp_writer, "runtime.writer.write_image_v05_delayed"
        ),
    ),
    _make_case(
        "runtime",
        "data.create_zarr_coins_v05",
        "End-to-end create_zarr on the synthetic coins dataset in format 0.5.",
        _verify_create_zarr_coins_v05,
        lambda: _bench_create_zarr_coins(True, "runtime.data.create_zarr_coins_v05"),
        lambda: _bench_create_zarr_coins(False, "runtime.data.create_zarr_coins_v05"),
    ),
    _make_case(
        "runtime",
        "data.create_zarr_astronaut_v05",
        "End-to-end create_zarr on the synthetic astronaut dataset in format 0.5.",
        _verify_create_zarr_astronaut_v05,
        lambda: _bench_create_zarr_astronaut(
            True, "runtime.data.create_zarr_astronaut_v05"
        ),
        lambda: _bench_create_zarr_astronaut(
            False, "runtime.data.create_zarr_astronaut_v05"
        ),
    ),
    _make_case(
        "runtime",
        "cli.create_info_v05",
        "CLI create plus info roundtrip for the synthetic coins dataset in format 0.5.",
        _verify_cli_create_info_v05,
        lambda: _bench_cli_create_info(True, "runtime.cli.create_info_v05"),
        lambda: _bench_cli_create_info(False, "runtime.cli.create_info_v05"),
    ),
)


def iter_cases(
    *,
    match: str | None = None,
    groups: list[str] | None = None,
) -> list[BenchmarkCase]:
    selected = list(ALL_CASES)
    if groups:
        allowed = set(groups)
        selected = [case for case in selected if case.group in allowed]
    if match:
        lowered = match.lower()
        selected = [
            case
            for case in selected
            if lowered in case.benchmark_base_name.lower()
            or lowered in case.description.lower()
        ]
    return selected


__all__ = ["BenchmarkCase", "ALL_CASES", "benchmark_environment_metadata", "iter_cases"]
