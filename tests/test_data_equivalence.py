from __future__ import annotations

import hashlib
import importlib
import sys
from pathlib import Path

import numpy as np

from tests._outcomes import err, ok

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_data = importlib.import_module("ome_zarr.data")
_cpp_data = importlib.import_module("ome_zarr_c.data")


def _array_digest(array: np.ndarray) -> dict:
    return {
        "shape": tuple(int(dim) for dim in array.shape),
        "dtype": str(array.dtype),
        "sha256": hashlib.sha256(array.tobytes()).hexdigest(),
    }


def _result_signature(result):
    if (
        isinstance(result, tuple)
        and len(result) == 2
        and all(isinstance(part, list) for part in result)
    ):
        return [
            [_array_digest(np.asarray(item)) for item in result[0]],
            [_array_digest(np.asarray(item)) for item in result[1]],
        ]
    return _array_digest(np.asarray(result))


def _run_simple(func):
    try:
        return ok(value=_result_signature(func()))
    except Exception as exc:  # noqa: BLE001
        return err(exc)


def _run_make_circle(func, h, w, value, dtype):
    target = np.zeros((h, w), dtype=dtype)
    try:
        func(h, w, value, target)
        return ok(value=_array_digest(target))
    except Exception as exc:  # noqa: BLE001
        return err(exc)


def _run_rgb_to_5d(func, pixels):
    try:
        return ok(value=_result_signature(func(pixels)))
    except Exception as exc:  # noqa: BLE001
        return err(exc)


def test_make_circle_matches_upstream() -> None:
    for h, w, value, dtype in [
        (8, 8, 1, np.float64),
        (9, 5, 7, np.int16),
        (12, 16, 2, np.uint8),
    ]:
        expected = _run_make_circle(_py_data.make_circle, h, w, value, dtype)
        actual = _run_make_circle(_cpp_data.make_circle, h, w, value, dtype)
        assert expected == actual

    py_target = np.zeros((16, 16), dtype=np.int16)
    cpp_target = np.zeros((16, 16), dtype=np.int16)
    py_view = py_target[3:11, 4:12]
    cpp_view = cpp_target[3:11, 4:12]
    _py_data.make_circle(8, 8, 5, py_view)
    _cpp_data.make_circle(8, 8, 5, cpp_view)
    assert _array_digest(py_target) == _array_digest(cpp_target)


def test_rgb_to_5d_matches_upstream() -> None:
    cases = [
        np.arange(16, dtype=np.uint8).reshape(4, 4),
        np.arange(4 * 5 * 3, dtype=np.uint8).reshape(4, 5, 3),
        np.arange(8 * 10 * 3, dtype=np.uint16).reshape(8, 10, 3)[::2, ::2, :],
        np.arange(2 * 3 * 4 * 5, dtype=np.uint8).reshape(2, 3, 4, 5),
    ]

    for pixels in cases:
        expected = _run_rgb_to_5d(_py_data.rgb_to_5d, pixels)
        actual = _run_rgb_to_5d(_cpp_data.rgb_to_5d, pixels)
        assert expected == actual


def test_coins_matches_upstream() -> None:
    expected = _run_simple(_py_data.coins)
    actual = _run_simple(_cpp_data.coins)
    assert expected == actual


def test_astronaut_matches_upstream() -> None:
    expected = _run_simple(_py_data.astronaut)
    actual = _run_simple(_cpp_data.astronaut)
    assert expected == actual
