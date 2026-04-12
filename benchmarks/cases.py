from __future__ import annotations

import hashlib
import importlib
import math
import os
import random
import sys
import time
import warnings
from collections.abc import Callable
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import dask
import dask.array as da
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM_ROOT = ROOT / "source_code_v.0.15.0"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(UPSTREAM_ROOT) not in sys.path:
    sys.path.insert(0, str(UPSTREAM_ROOT))

_py_axes = importlib.import_module("ome_zarr.axes")
_py_conversions = importlib.import_module("ome_zarr.conversions")
_py_csv = importlib.import_module("ome_zarr.csv")
_py_data = importlib.import_module("ome_zarr.data")
_py_dask_utils = importlib.import_module("ome_zarr.dask_utils")
_py_format = importlib.import_module("ome_zarr.format")
_py_scale = importlib.import_module("ome_zarr.scale")
_py_writer = importlib.import_module("ome_zarr.writer")

_cpp_axes = importlib.import_module("ome_zarr_c.axes")
_cpp_conversions = importlib.import_module("ome_zarr_c.conversions")
_cpp_csv = importlib.import_module("ome_zarr_c.csv")
_cpp_data = importlib.import_module("ome_zarr_c.data")
_cpp_dask_utils = importlib.import_module("ome_zarr_c.dask_utils")
_cpp_format = importlib.import_module("ome_zarr_c.format")
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
        "benchmark_scope": "verified-native-backed-in-memory-only",
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
        for _ in range(loops):
            func()
        return time.perf_counter() - start

    return timer


def _verify_once(case_id: str, verify: Callable[[], None]) -> None:
    if case_id in _VERIFIED_CASES:
        return
    verify()
    _VERIFIED_CASES.add(case_id)


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
    if isinstance(value, (int, str, type(None))):
        return value
    if isinstance(value, dict):
        return {
            key: _canonicalize(value[key])
            for key in sorted(value, key=lambda item: repr(item))
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
    if isinstance(value, dict):
        return sum(
            _touch(value[key]) for key in sorted(value, key=lambda item: repr(item))
        )
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


def _float_payload(value: float) -> float:
    return round(value * _FLOAT_CASE_MULTIPLIER) / _FLOAT_CASE_MULTIPLIER


_INT_RGBA_VALUES = tuple(_RNG.randint(-(2**31), 2**31 - 1) for _ in range(8_192))
_RGBA_VALUES = tuple(
    tuple(_RNG.randint(0, 255) for _ in range(4)) for _ in range(8_192)
)

_CSV_PARSE_CASES: list[tuple[str, str]] = []
for literal in (
    "",
    "0",
    "1",
    "-1",
    "1.5",
    "2.5",
    "-2.5",
    "abc",
    "True",
    "False",
    " 3 ",
    "1e3",
    "nan",
):
    for column_type in ("d", "l", "s", "b", "x"):
        _CSV_PARSE_CASES.append((literal, column_type))
for _ in range(768):
    whole = _RNG.randint(-1_000_000, 1_000_000)
    frac = _RNG.randint(0, 9_999)
    exponent = _RNG.randint(-5, 5)
    value = f"{whole}.{frac:04d}e{exponent:+d}"
    for column_type in ("d", "l"):
        _CSV_PARSE_CASES.append((value, column_type))
for literal in ("inf", "-inf"):
    for column_type in ("d", "s", "b", "x"):
        _CSV_PARSE_CASES.append((literal, column_type))
_CSV_PARSE_CASES = tuple(_CSV_PARSE_CASES)

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


def _verify_int_to_rgba() -> None:
    _assert_parity(
        "micro.conversions.int_to_rgba_batch",
        [_py_conversions.int_to_rgba(value) for value in _INT_RGBA_VALUES],
        [_cpp_conversions.int_to_rgba(value) for value in _INT_RGBA_VALUES],
    )


def _bench_int_to_rgba(module: object) -> float:
    total = 0.0
    for value in _INT_RGBA_VALUES:
        rgba = module.int_to_rgba(value)
        total += rgba[0] + rgba[3]
    return _float_payload(total)


def _verify_rgba_to_int() -> None:
    _assert_parity(
        "micro.conversions.rgba_to_int_batch",
        [_py_conversions.rgba_to_int(*rgba) for rgba in _RGBA_VALUES],
        [_cpp_conversions.rgba_to_int(*rgba) for rgba in _RGBA_VALUES],
    )


def _bench_rgba_to_int(module: object) -> float:
    total = 0
    for rgba in _RGBA_VALUES:
        total += module.rgba_to_int(*rgba)
    return float(total)


def _verify_parse_csv_value() -> None:
    _assert_parity(
        "micro.csv.parse_csv_value_batch",
        [
            _py_csv.parse_csv_value(value, column_type)
            for value, column_type in _CSV_PARSE_CASES
        ],
        [
            _cpp_csv.parse_csv_value(value, column_type)
            for value, column_type in _CSV_PARSE_CASES
        ],
    )


def _bench_parse_csv_value(module: object) -> float:
    total = 0.0
    for value, column_type in _CSV_PARSE_CASES:
        parsed = module.parse_csv_value(value, column_type)
        total += _touch(parsed)
    return _float_payload(total)


def _verify_axes_constructor() -> None:
    _assert_parity(
        "micro.axes.constructor_batch",
        [
            (
                getattr(_py_axes.Axes(axes_input, py_fmt), "axes", None),
                _py_axes.Axes(axes_input, py_fmt).to_list(_py_format.FormatV03()),
                _py_axes.Axes(axes_input, py_fmt).to_list(_py_format.FormatV04()),
            )
            for axes_input, py_fmt, _cpp_fmt in _AXES_CASES
        ],
        [
            (
                getattr(_cpp_axes.Axes(axes_input, cpp_fmt), "axes", None),
                _cpp_axes.Axes(axes_input, cpp_fmt).to_list(_cpp_format.FormatV03()),
                _cpp_axes.Axes(axes_input, cpp_fmt).to_list(_cpp_format.FormatV04()),
            )
            for axes_input, _py_fmt, cpp_fmt in _AXES_CASES
        ],
    )


def _bench_axes_constructor(module: object) -> float:
    total = 0.0
    for axes_input, py_fmt, cpp_fmt in _AXES_CASES:
        fmt = py_fmt if module is _py_axes else cpp_fmt
        instance = module.Axes(axes_input, fmt)
        fmt03 = (
            _py_format.FormatV03() if module is _py_axes else _cpp_format.FormatV03()
        )
        fmt04 = (
            _py_format.FormatV04() if module is _py_axes else _cpp_format.FormatV04()
        )
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
    cpp_results = []
    for height, width, value, dtype in _MAKE_CIRCLE_CASES:
        py_target = np.zeros((height, width), dtype=dtype)
        cpp_target = np.zeros((height, width), dtype=dtype)
        _py_data.make_circle(height, width, value, py_target)
        _cpp_data.make_circle(height, width, value, cpp_target)
        py_results.append(py_target)
        cpp_results.append(cpp_target)
    _assert_parity("micro.data.make_circle_batch", py_results, cpp_results)


def _bench_make_circle(module: object) -> float:
    total = 0.0
    for height, width, value, dtype in _MAKE_CIRCLE_CASES:
        target = np.zeros((height, width), dtype=dtype)
        module.make_circle(height, width, value, target)
        total += _touch(target)
    return total


def _verify_rgb_to_5d() -> None:
    _assert_parity(
        "micro.data.rgb_to_5d_batch",
        [_py_data.rgb_to_5d(pixels) for pixels in _RGB_TO_5D_CASES],
        [_cpp_data.rgb_to_5d(pixels) for pixels in _RGB_TO_5D_CASES],
    )


def _bench_rgb_to_5d(module: object) -> float:
    total = 0.0
    for pixels in _RGB_TO_5D_CASES:
        total += _touch(module.rgb_to_5d(pixels))
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


def _verify_coins() -> None:
    _assert_parity("macro.data.coins", _py_data.coins(), _cpp_data.coins())


def _bench_coins(module: object) -> float:
    return _touch(module.coins())


def _verify_astronaut() -> None:
    _assert_parity("macro.data.astronaut", _py_data.astronaut(), _cpp_data.astronaut())


def _bench_astronaut(module: object) -> float:
    return _touch(module.astronaut())


ALL_CASES = (
    _make_case(
        "micro",
        "conversions.int_to_rgba_batch",
        "Signed 32-bit integer to RGBA batch conversion.",
        _verify_int_to_rgba,
        lambda: _bench_int_to_rgba(_py_conversions),
        lambda: _bench_int_to_rgba(_cpp_conversions),
    ),
    _make_case(
        "micro",
        "conversions.rgba_to_int_batch",
        "RGBA tuple to signed integer batch conversion.",
        _verify_rgba_to_int,
        lambda: _bench_rgba_to_int(_py_conversions),
        lambda: _bench_rgba_to_int(_cpp_conversions),
    ),
    _make_case(
        "micro",
        "csv.parse_csv_value_batch",
        "CSV scalar parsing across mixed numeric and string inputs.",
        _verify_parse_csv_value,
        lambda: _bench_parse_csv_value(_py_csv),
        lambda: _bench_parse_csv_value(_cpp_csv),
    ),
    _make_case(
        "micro",
        "axes.constructor_batch",
        "Axes normalization plus to_list conversion across valid formats.",
        _verify_axes_constructor,
        lambda: _bench_axes_constructor(_py_axes),
        lambda: _bench_axes_constructor(_cpp_axes),
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
    _make_case(
        "micro",
        "data.make_circle_batch",
        "Synthetic circle mask generation over mixed shapes and dtypes.",
        _verify_make_circle,
        lambda: _bench_make_circle(_py_data),
        lambda: _bench_make_circle(_cpp_data),
    ),
    _make_case(
        "micro",
        "data.rgb_to_5d_batch",
        "RGB/greyscale normalization into 5D OME-Zarr video layout.",
        _verify_rgb_to_5d,
        lambda: _bench_rgb_to_5d(_py_data),
        lambda: _bench_rgb_to_5d(_cpp_data),
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
        "macro",
        "data.coins",
        "Synthetic coins pyramid and labels generation.",
        _verify_coins,
        lambda: _bench_coins(_py_data),
        lambda: _bench_coins(_cpp_data),
    ),
    _make_case(
        "macro",
        "data.astronaut",
        "Synthetic astronaut pyramid and label generation.",
        _verify_astronaut,
        lambda: _bench_astronaut(_py_data),
        lambda: _bench_astronaut(_cpp_data),
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
