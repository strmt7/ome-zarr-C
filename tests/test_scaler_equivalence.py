from __future__ import annotations

import importlib
import io
import sys
import warnings
from contextlib import redirect_stdout
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


def _array_signature(array) -> dict:
    if isinstance(array, da.Array):
        value = array.compute()
        kind = f"{type(array).__module__}.{type(array).__qualname__}"
        chunks = tuple(tuple(int(v) for v in axis) for axis in array.chunks)
        chunksize = tuple(int(v) for v in array.chunksize)
    else:
        value = array
        kind = f"{type(array).__module__}.{type(array).__qualname__}"
        chunks = None
        chunksize = None

    return {
        "type": kind,
        "shape": tuple(int(dim) for dim in value.shape),
        "dtype": str(value.dtype),
        "chunks": chunks,
        "chunksize": chunksize,
        "values": value.tolist(),
    }


def _result_signature(result):
    if isinstance(result, list):
        return [_array_signature(item) for item in result]
    return _array_signature(result)


def _run_scaler_method(cls, method_name: str, data, **scaler_kwargs):
    stream = io.StringIO()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            with redirect_stdout(stream):
                scaler = cls(**scaler_kwargs)
                result = getattr(scaler, method_name)(data)
            return ok(
                value=_result_signature(result),
                stdout=stream.getvalue(),
                records=_warning_snapshot(caught),
            )
        except Exception as exc:  # noqa: BLE001
            return err(exc, stdout=stream.getvalue(), records=_warning_snapshot(caught))


def _run_scaler_func(cls, method_name: str):
    stream = io.StringIO()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            with redirect_stdout(stream):
                scaler = cls(method=method_name)
                func = scaler.func
            return ok(
                value=(func.__name__, func.__qualname__.split(".")[-1]),
                stdout=stream.getvalue(),
                records=_warning_snapshot(caught),
            )
        except Exception as exc:  # noqa: BLE001
            return err(exc, stdout=stream.getvalue(), records=_warning_snapshot(caught))


def _assert_method_match(method_name: str, data, **scaler_kwargs) -> None:
    expected = _run_scaler_method(_py_scale.Scaler, method_name, data, **scaler_kwargs)
    actual = _run_scaler_method(_cpp_scale.Scaler, method_name, data, **scaler_kwargs)
    assert expected == actual


def test_scaler_methods_matches_upstream() -> None:
    assert list(_py_scale.Scaler.methods()) == list(_cpp_scale.Scaler.methods())


def test_scaler_func_property_matches_upstream() -> None:
    for method_name in [
        "nearest",
        "gaussian",
        "laplacian",
        "local_mean",
        "resize_image",
        "zoom",
        "missing",
    ]:
        expected = _run_scaler_func(_py_scale.Scaler, method_name)
        actual = _run_scaler_func(_cpp_scale.Scaler, method_name)
        assert expected == actual


def test_scaler_resize_image_matches_upstream() -> None:
    cases = [
        np.arange(64, dtype=np.uint16).reshape(8, 8),
        np.arange(3 * 32 * 32, dtype=np.uint8).reshape(3, 32, 32),
        da.from_array(np.arange(64, dtype=np.uint16).reshape(8, 8), chunks=(5, 5)),
        da.from_array(
            np.arange(3 * 16 * 16, dtype=np.float32).reshape(3, 16, 16),
            chunks=(1, 7, 7),
        ),
    ]

    for data in cases:
        _assert_method_match("resize_image", data)


def test_scaler_nearest_matches_upstream() -> None:
    cases = [
        np.arange(64, dtype=np.uint16).reshape(8, 8),
        np.arange(3 * 32 * 32, dtype=np.uint8).reshape(3, 32, 32),
        np.arange(1 * 2 * 1 * 16 * 16, dtype=np.uint8).reshape(1, 2, 1, 16, 16),
    ]

    for data in cases:
        _assert_method_match("nearest", data)


def test_scaler_gaussian_matches_upstream() -> None:
    cases = [
        np.arange(64, dtype=np.uint16).reshape(8, 8),
        np.arange(3 * 32 * 32, dtype=np.uint8).reshape(3, 32, 32),
        np.arange(1 * 2 * 1 * 16 * 16, dtype=np.uint8).reshape(1, 2, 1, 16, 16),
    ]

    for data in cases:
        _assert_method_match("gaussian", data)


def test_scaler_laplacian_matches_upstream() -> None:
    cases = [
        np.arange(64, dtype=np.uint16).reshape(8, 8),
        np.arange(3 * 32 * 32, dtype=np.uint8).reshape(3, 32, 32),
        np.arange(1 * 2 * 1 * 16 * 16, dtype=np.uint8).reshape(1, 2, 1, 16, 16),
    ]

    for data in cases:
        _assert_method_match("laplacian", data)


def test_scaler_local_mean_matches_upstream() -> None:
    cases = [
        np.arange(64, dtype=np.uint16).reshape(8, 8),
        np.arange(3 * 32 * 32, dtype=np.uint8).reshape(3, 32, 32),
        np.arange(1 * 2 * 1 * 16 * 16, dtype=np.uint8).reshape(1, 2, 1, 16, 16),
    ]

    for data in cases:
        _assert_method_match("local_mean", data)


def test_scaler_zoom_matches_upstream() -> None:
    data = np.arange(64, dtype=np.uint16).reshape(8, 8)
    _assert_method_match("zoom", data)
