from __future__ import annotations

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

_py_dask_utils = importlib.import_module("ome_zarr.dask_utils")
_cpp_dask_utils = importlib.import_module("ome_zarr_c.dask_utils")


def _warning_snapshot(caught: list[warnings.WarningMessage]) -> list[tuple[str, str]]:
    return [(type(w.message).__name__, str(w.message)) for w in caught]


def _chunksize_signature(
    result,
) -> tuple[tuple[tuple[str, int], ...], tuple[tuple[str, int], ...]]:
    better_chunks, block_output_shape = result
    return (
        tuple((type(value).__name__, int(value)) for value in better_chunks),
        tuple((type(value).__name__, int(value)) for value in block_output_shape),
    )


def _array_signature(result: da.Array) -> dict:
    computed = result.compute()
    return {
        "type": f"{type(result).__module__}.{type(result).__qualname__}",
        "shape": tuple(int(dim) for dim in result.shape),
        "dtype": str(result.dtype),
        "chunks": tuple(tuple(int(v) for v in axis) for axis in result.chunks),
        "chunksize": tuple(int(v) for v in result.chunksize),
        "values": computed.tolist(),
    }


def _run_chunksize(func, image, factors):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            return ok(
                value=_chunksize_signature(func(image, factors)),
                payload=_warning_snapshot(caught),
            )
        except Exception as exc:  # noqa: BLE001
            return err(exc, payload=_warning_snapshot(caught))


def _run_array(func, image, *args, **kwargs):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            return ok(
                value=_array_signature(func(image, *args, **kwargs)),
                payload=_warning_snapshot(caught),
            )
        except Exception as exc:  # noqa: BLE001
            return err(exc, payload=_warning_snapshot(caught))


def _assert_chunksize_match(image, factors) -> None:
    expected = _run_chunksize(_py_dask_utils._better_chunksize, image, factors)
    actual = _run_chunksize(_cpp_dask_utils._better_chunksize, image, factors)
    assert expected == actual


def _assert_array_match(name: str, image, *args, **kwargs) -> None:
    expected = _run_array(getattr(_py_dask_utils, name), image, *args, **kwargs)
    actual = _run_array(getattr(_cpp_dask_utils, name), image, *args, **kwargs)
    assert expected == actual


def test_better_chunksize_matches_upstream() -> None:
    rng = random.Random(20260412)
    base_cases = [
        (np.arange(24, dtype=np.uint16).reshape(4, 6), (3, 4), np.array([0.5, 0.5])),
        (
            np.arange(60, dtype=np.float32).reshape(3, 4, 5),
            (2, 3, 4),
            np.array([0.5, 0.75, 0.5]),
        ),
        (np.arange(35, dtype=np.int16).reshape(5, 7), (4, 5), np.array([2 / 5, 3 / 7])),
    ]

    for data, chunks, factors in base_cases:
        image = da.from_array(data, chunks=chunks)
        _assert_chunksize_match(image, factors)

    for _ in range(5):
        y = rng.randint(4, 8)
        x = rng.randint(5, 10)
        data = np.arange(y * x, dtype=np.uint16).reshape(y, x)
        chunks = (rng.randint(2, y), rng.randint(2, x))
        output_shape = (rng.randint(1, y), rng.randint(1, x))
        factors = np.array(output_shape) / np.array(data.shape).astype(float)
        image = da.from_array(data, chunks=chunks)
        _assert_chunksize_match(image, factors)


def test_downscale_nearest_matches_upstream() -> None:
    valid_cases = [
        (np.arange(24, dtype=np.uint16).reshape(4, 6), (3, 4), (2, 3)),
        (np.arange(60, dtype=np.uint16).reshape(3, 4, 5), (2, 3, 4), (1, 2, 5)),
        (np.arange(24, dtype=np.uint16).reshape(4, 6), (3, 4), (True, 2)),
    ]

    for data, chunks, factors in valid_cases:
        image = da.from_array(data, chunks=chunks)
        _assert_array_match("downscale_nearest", image, factors)

    invalid_cases = [
        ((2, 3), (4, 6), (3, 7)),
        ((2, 3), (4, 6), (0, 2)),
        ((2, 3), (4, 6), (1.5, 2)),
        ((2, 3), (4, 6), (2,)),
    ]

    for chunks, shape, factors in invalid_cases:
        data = np.arange(shape[0] * shape[1], dtype=np.uint16).reshape(shape)
        image = da.from_array(data, chunks=chunks)
        _assert_array_match("downscale_nearest", image, factors)


def test_resize_matches_upstream() -> None:
    cases = [
        (np.arange(24, dtype=np.uint16).reshape(4, 6), (3, 4), (2, 3), (), {}),
        (
            np.arange(60, dtype=np.float32).reshape(3, 4, 5),
            (2, 3, 4),
            (2, 3, 4),
            (),
            {"order": 0, "preserve_range": True, "anti_aliasing": False},
        ),
        (
            np.arange(36, dtype=np.uint8).reshape(6, 6),
            (4, 4),
            (3, 5),
            (),
            {"mode": "reflect", "preserve_range": True},
        ),
    ]

    for data, chunks, output_shape, args, kwargs in cases:
        image = da.from_array(data, chunks=chunks)
        _assert_array_match("resize", image, output_shape, *args, **kwargs)


def test_local_mean_matches_upstream() -> None:
    cases = [
        (np.arange(24, dtype=np.uint16).reshape(4, 6), (3, 4), (2, 3)),
        (np.arange(96, dtype=np.int16).reshape(4, 4, 6), (3, 3, 4), (2, 2, 3)),
        (np.arange(144, dtype=np.float32).reshape(6, 6, 4), (4, 4, 3), (3, 3, 2)),
    ]

    for data, chunks, output_shape in cases:
        image = da.from_array(data, chunks=chunks)
        _assert_array_match("local_mean", image, output_shape)


def test_zoom_matches_upstream() -> None:
    cases = [
        (np.arange(24, dtype=np.uint16).reshape(4, 6), (3, 4), (2, 3)),
        (np.arange(60, dtype=np.uint16).reshape(3, 4, 5), (2, 3, 4), (1, 2, 5)),
        (np.arange(96, dtype=np.float32).reshape(4, 6, 4), (3, 4, 3), (2, 3, 2)),
    ]

    for data, chunks, output_shape in cases:
        image = da.from_array(data, chunks=chunks)
        _assert_array_match("zoom", image, output_shape)
