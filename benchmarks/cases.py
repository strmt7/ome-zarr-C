from __future__ import annotations

import math
import os
import time
from collections.abc import Callable
from contextlib import ExitStack, nullcontext, redirect_stderr, redirect_stdout
from dataclasses import dataclass
from io import StringIO
from typing import Any, Literal

import numpy as np

from benchmarks.native.standalone import assert_selftest_section, native_bench_timer
from benchmarks.python import upstream


@dataclass(frozen=True)
class BenchmarkCase:
    group: str
    name: str
    description: str
    verify: Callable[[], None]
    python_timer: Callable[[int], float]
    converted_timer: Callable[[int], float]
    converted_variant: Literal["native"] = "native"

    @property
    def benchmark_base_name(self) -> str:
        return f"{self.group}.{self.name}"


_VERIFIED_CASES: set[str] = set()


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
        "benchmark_scope": "frozen-upstream-python-vs-standalone-native-cpp",
        "thread_env": thread_settings,
        "upstream_snapshot": "ome-zarr-py v0.15.0",
    }


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


def _verify_once(case_id: str, verify: Callable[[], None]) -> None:
    if case_id in _VERIFIED_CASES:
        return
    with _bench_stdio_context():
        verify()
    _VERIFIED_CASES.add(case_id)


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


def _make_case(
    group: str,
    name: str,
    description: str,
    verify: Callable[[], None],
    python_func: Callable[[], object],
    converted_func: Callable[[], object],
) -> BenchmarkCase:
    case_id = f"{group}.{name}"
    return BenchmarkCase(
        group=group,
        name=name,
        description=description,
        verify=verify,
        python_timer=_make_timer(case_id, verify, python_func),
        converted_timer=_make_timer(case_id, verify, converted_func),
    )


def _make_native_case(
    group: str,
    name: str,
    description: str,
    selftest_section: str,
    python_func: Callable[[], object],
    native_match: str,
) -> BenchmarkCase:
    case_id = f"{group}.{name}"

    def verify() -> None:
        python_func()
        assert_selftest_section(selftest_section)

    return BenchmarkCase(
        group=group,
        name=name,
        description=description,
        verify=verify,
        python_timer=_make_timer(case_id, verify, python_func),
        converted_timer=native_bench_timer(
            case_id=case_id,
            verify=lambda: _verify_once(case_id, verify),
            native_match=native_match,
        ),
    )


def _values_equal(left: Any, right: Any) -> bool:
    if isinstance(left, float) and isinstance(right, float):
        if math.isnan(left) and math.isnan(right):
            return True
    if isinstance(left, np.ndarray) and isinstance(right, np.ndarray):
        return np.array_equal(left, right)
    if isinstance(left, dict) and isinstance(right, dict):
        if left.keys() != right.keys():
            return False
        return all(_values_equal(left[key], right[key]) for key in left)
    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        if len(left) != len(right):
            return False
        return all(
            _values_equal(item_left, item_right)
            for item_left, item_right in zip(left, right, strict=True)
        )
    return left == right


def _assert_parity(case_name: str, py_value: object, cpp_value: object) -> None:
    if not _values_equal(py_value, cpp_value):
        raise AssertionError(
            f"Benchmark case {case_name} lost parity.\n"
            f"python={py_value!r}\nnative={cpp_value!r}"
        )


def _touch(value: Any) -> float:
    return upstream.touch(value)


def _touch_outcome(outcome: Any) -> float:
    return _touch(
        (
            getattr(outcome, "status", None),
            getattr(outcome, "value", None),
            getattr(outcome, "stdout", None),
            getattr(outcome, "records", None),
            getattr(outcome, "payload", None),
        )
    )


ALL_CASES: tuple[BenchmarkCase, ...] = (
    _make_native_case(
        "axes",
        "constructor_batch",
        "Axes normalization and validation on representative upstream inputs.",
        "axes",
        upstream.bench_axes_constructor,
        "axes.constructor_batch",
    ),
    _make_native_case(
        "conversions",
        "int_to_rgba",
        "Integer-to-RGBA conversion over signed 32-bit edge cases.",
        "conversions_and_csv",
        upstream.bench_conversions_int_to_rgba,
        "conversions.int_to_rgba",
    ),
    _make_native_case(
        "conversions",
        "rgba_to_int",
        "RGBA-to-integer conversion over byte edge cases.",
        "conversions_and_csv",
        upstream.bench_conversions_rgba_to_int,
        "conversions.rgba_to_int",
    ),
    _make_native_case(
        "csv",
        "parse_csv_value",
        "CSV typed value parsing for string, integer, float, bool, and NaN cases.",
        "conversions_and_csv",
        upstream.bench_csv_parse_value,
        "csv.parse_csv_value",
    ),
    _make_native_case(
        "data",
        "make_circle_batch",
        "Synthetic circle painting into representative NumPy arrays.",
        "dask_scale_and_data",
        upstream.bench_data_make_circle,
        "data.make_circle_batch",
    ),
    _make_native_case(
        "data",
        "rgb_to_5d_batch",
        "RGB and greyscale normalization into the upstream 5D layout.",
        "dask_scale_and_data",
        upstream.bench_data_rgb_to_5d,
        "data.rgb_to_5d_batch",
    ),
    _make_native_case(
        "format",
        "dispatch",
        "Format dispatch helpers and implementation enumeration.",
        "format",
        upstream.bench_format_dispatch,
        "format.dispatch",
    ),
    _make_native_case(
        "format",
        "matches",
        "Concrete format metadata match checks.",
        "format",
        upstream.bench_format_matches,
        "format.matches",
    ),
    _make_native_case(
        "format",
        "v01_init_store",
        "FormatV01 store initialization planning and behavior.",
        "format",
        upstream.bench_format_v01_init_store,
        "format.v01_init_store",
    ),
    _make_native_case(
        "format",
        "well_and_coord",
        "Well metadata and coordinate transformation helpers.",
        "format",
        upstream.bench_format_well_and_coord,
        "format.well_and_coord",
    ),
    _make_native_case(
        "utils",
        "path_helpers",
        "Path splitting and common-prefix stripping helpers.",
        "io_and_utils",
        upstream.bench_utils_path_helpers,
        "utils.path_helpers",
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
