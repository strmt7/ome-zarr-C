from __future__ import annotations

import copy
import importlib
import random
import sys
import warnings
from pathlib import Path

import dask.array as da
import numpy as np

from tests._outcomes import err, ok

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_scale = importlib.import_module("ome_zarr.scale")
_cpp_scale = importlib.import_module("ome_zarr_c.scale")


def _warning_snapshot(caught: list[warnings.WarningMessage]) -> list[tuple[str, str]]:
    return [(type(w.message).__name__, str(w.message)) for w in caught]


def _array_signature(result: da.Array) -> dict:
    computed = result.compute()
    return {
        "shape": tuple(int(dim) for dim in result.shape),
        "dtype": str(result.dtype),
        "chunks": tuple(tuple(int(v) for v in axis) for axis in result.chunks),
        "chunksize": tuple(int(v) for v in result.chunksize),
        "values": computed.tolist(),
    }


def _run_build_pyramid(func, image, scale_factors, dims, method, chunks=None):
    scale_factors_payload = copy.deepcopy(scale_factors)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            result = func(
                image,
                scale_factors_payload,
                dims=dims,
                method=method,
                chunks=chunks,
            )
            return ok(
                value={
                    "levels": [_array_signature(level) for level in result],
                    "warnings": _warning_snapshot(caught),
                },
                payload=scale_factors_payload,
            )
        except Exception as exc:  # noqa: BLE001
            return err(
                exc,
                payload=scale_factors_payload,
                records=_warning_snapshot(caught),
            )


def _assert_build_pyramid_match(
    image,
    scale_factors,
    dims,
    py_method,
    cpp_method,
    *,
    chunks=None,
) -> None:
    expected = _run_build_pyramid(
        _py_scale._build_pyramid,
        image,
        scale_factors,
        dims,
        py_method,
        chunks=chunks,
    )
    actual = _run_build_pyramid(
        _cpp_scale._build_pyramid,
        image,
        scale_factors,
        dims,
        cpp_method,
        chunks=chunks,
    )
    assert expected == actual


def test_build_pyramid_matches_upstream_for_integer_scale_factors() -> None:
    rng = random.Random(20260412)
    base_cases = [
        (
            np.arange(24, dtype=np.uint16).reshape(1, 1, 4, 6),
            [2, 4],
            ("t", "c", "y", "x"),
            _py_scale.Methods.NEAREST,
            _cpp_scale.Methods.NEAREST,
        ),
        (
            np.arange(24, dtype=np.float32).reshape(2, 3, 4),
            [2, 4],
            ("z", "y", "x"),
            _py_scale.Methods.LOCAL_MEAN,
            _cpp_scale.Methods.LOCAL_MEAN,
        ),
        (
            np.arange(36, dtype=np.uint8).reshape(6, 6),
            [2, 4],
            ("y", "x"),
            "resize",
            "resize",
        ),
    ]

    for image, scale_factors, dims, py_method, cpp_method in base_cases:
        _assert_build_pyramid_match(image, scale_factors, dims, py_method, cpp_method)

    for _ in range(4):
        y = rng.randint(4, 8)
        x = rng.randint(4, 8)
        image = np.arange(y * x, dtype=np.uint16).reshape(y, x)
        _assert_build_pyramid_match(
            image,
            [2, 4],
            ("y", "x"),
            _py_scale.Methods.NEAREST,
            _cpp_scale.Methods.NEAREST,
            chunks=(rng.randint(2, y), rng.randint(2, x)),
        )


def test_build_pyramid_matches_upstream_for_dict_scale_factors() -> None:
    cases = [
        (
            np.arange(96, dtype=np.float32).reshape(2, 3, 4, 4),
            [
                {"z": 1, "y": 2, "x": 2},
                {"z": 1, "y": 4, "x": 4},
            ],
            ("c", "z", "y", "x"),
            _py_scale.Methods.LOCAL_MEAN,
            _cpp_scale.Methods.LOCAL_MEAN,
        ),
        (
            da.from_array(
                np.arange(48, dtype=np.uint16).reshape(2, 4, 6),
                chunks=(1, 3, 4),
            ),
            [
                {"y": 2, "x": 2},
                {"y": 4, "x": 4},
            ],
            ("z", "y", "x"),
            _py_scale.Methods.ZOOM,
            _cpp_scale.Methods.ZOOM,
        ),
    ]

    for image, scale_factors, dims, py_method, cpp_method in cases:
        _assert_build_pyramid_match(image, scale_factors, dims, py_method, cpp_method)


def test_build_pyramid_matches_upstream_for_warning_cases() -> None:
    image = np.arange(9, dtype=np.uint16).reshape(3, 3)
    _assert_build_pyramid_match(
        image,
        [2, 4],
        ("y", "x"),
        _py_scale.Methods.NEAREST,
        _cpp_scale.Methods.NEAREST,
    )


def test_build_pyramid_matches_upstream_for_errors() -> None:
    image = np.arange(4, dtype=np.uint16).reshape(2, 2)
    unknown_method = object()

    _assert_build_pyramid_match(
        image,
        [{"y": 2, "x": 2}],
        ("y", "x"),
        unknown_method,
        unknown_method,
    )
    _assert_build_pyramid_match(
        image,
        ({"y": 2, "x": 2},),
        ("y", "x"),
        _py_scale.Methods.NEAREST,
        _cpp_scale.Methods.NEAREST,
    )
    _assert_build_pyramid_match(
        image,
        [2],
        ("y", "x"),
        "not-a-method",
        "not-a-method",
    )
