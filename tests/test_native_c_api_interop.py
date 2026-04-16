from __future__ import annotations

import ctypes
import importlib
import json
import math
import sys
import warnings
from functools import lru_cache
from pathlib import Path

import numpy as np
import pytest

from benchmarks.runtime_support import location_signature, write_minimal_v2_image
from tests.test_utils_equivalence import ROOT, _native_cli_path

sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_conversions = importlib.import_module("ome_zarr.conversions")
_py_csv = importlib.import_module("ome_zarr.csv")
_py_data = importlib.import_module("ome_zarr.data")
_py_format = importlib.import_module("ome_zarr.format")
_py_io = importlib.import_module("ome_zarr.io")
_py_scale = importlib.import_module("ome_zarr.scale")


class CApiResult(ctypes.Structure):
    _fields_ = [
        ("ok", ctypes.c_int),
        ("json", ctypes.c_void_p),
        ("error_type", ctypes.c_void_p),
        ("error_message", ctypes.c_void_p),
    ]


class CApiU8ArrayResult(ctypes.Structure):
    _fields_ = [
        ("ok", ctypes.c_int),
        ("data", ctypes.POINTER(ctypes.c_uint8)),
        ("data_len", ctypes.c_size_t),
        ("shape", ctypes.POINTER(ctypes.c_size_t)),
        ("ndim", ctypes.c_size_t),
        ("error_type", ctypes.c_void_p),
        ("error_message", ctypes.c_void_p),
    ]


def _read_c_string(pointer: int | None) -> str:
    if not pointer:
        return ""
    return ctypes.string_at(pointer).decode("utf-8")


@lru_cache(maxsize=1)
def _native_api_lib() -> ctypes.CDLL:
    build_dir = _native_cli_path().parent
    candidates = [
        build_dir / "libome_zarr_native_api.so",
        build_dir / "libome_zarr_native_api.dylib",
        build_dir / "ome_zarr_native_api.dll",
    ]
    library_path = next((path for path in candidates if path.exists()), None)
    if library_path is None:
        pytest.fail("native C ABI shared library was not built")

    lib = ctypes.CDLL(str(library_path))
    lib.ome_zarr_native_api_abi_version.restype = ctypes.c_char_p

    lib.ome_zarr_native_api_project_metadata.restype = CApiResult
    lib.ome_zarr_native_api_call_json.argtypes = [
        ctypes.c_char_p,
        ctypes.c_char_p,
    ]
    lib.ome_zarr_native_api_call_json.restype = CApiResult
    lib.ome_zarr_native_api_int_to_rgba.argtypes = [ctypes.c_int32]
    lib.ome_zarr_native_api_int_to_rgba.restype = CApiResult
    lib.ome_zarr_native_api_int_to_rgba_255.argtypes = [ctypes.c_int32]
    lib.ome_zarr_native_api_int_to_rgba_255.restype = CApiResult
    lib.ome_zarr_native_api_rgba_to_int.argtypes = [
        ctypes.c_uint8,
        ctypes.c_uint8,
        ctypes.c_uint8,
        ctypes.c_uint8,
    ]
    lib.ome_zarr_native_api_rgba_to_int.restype = CApiResult
    lib.ome_zarr_native_api_parse_csv_value.argtypes = [
        ctypes.c_char_p,
        ctypes.c_char_p,
    ]
    lib.ome_zarr_native_api_parse_csv_value.restype = CApiResult
    lib.ome_zarr_native_api_rgb_to_5d_u8.argtypes = [
        ctypes.POINTER(ctypes.c_uint8),
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_size_t),
    ]
    lib.ome_zarr_native_api_rgb_to_5d_u8.restype = CApiU8ArrayResult
    lib.ome_zarr_native_api_free_result.argtypes = [CApiResult]
    lib.ome_zarr_native_api_free_result.restype = None
    lib.ome_zarr_native_api_free_u8_array_result.argtypes = [CApiU8ArrayResult]
    lib.ome_zarr_native_api_free_u8_array_result.restype = None
    return lib


def _decode_result(lib: ctypes.CDLL, result: CApiResult):
    try:
        if result.ok == 0:
            raise AssertionError(
                f"{_read_c_string(result.error_type)}: "
                f"{_read_c_string(result.error_message)}"
            )
        return json.loads(_read_c_string(result.json))
    finally:
        lib.ome_zarr_native_api_free_result(result)


def _decode_error(lib: ctypes.CDLL, result: CApiResult) -> tuple[str, str]:
    try:
        assert result.ok == 0
        return (
            _read_c_string(result.error_type),
            _read_c_string(result.error_message),
        )
    finally:
        lib.ome_zarr_native_api_free_result(result)


def _decode_array(
    lib: ctypes.CDLL,
    result: CApiU8ArrayResult,
) -> tuple[np.ndarray, tuple[int, ...]]:
    try:
        if result.ok == 0:
            raise AssertionError(
                f"{_read_c_string(result.error_type)}: "
                f"{_read_c_string(result.error_message)}"
            )
        shape = tuple(int(result.shape[index]) for index in range(result.ndim))
        if result.data_len == 0:
            flat = np.empty((0,), dtype=np.uint8)
        else:
            flat = np.ctypeslib.as_array(
                result.data,
                shape=(result.data_len,),
            ).copy()
        return flat.reshape(shape), shape
    finally:
        lib.ome_zarr_native_api_free_u8_array_result(result)


def _decode_array_error(
    lib: ctypes.CDLL,
    result: CApiU8ArrayResult,
) -> tuple[str, str]:
    try:
        assert result.ok == 0
        return (
            _read_c_string(result.error_type),
            _read_c_string(result.error_message),
        )
    finally:
        lib.ome_zarr_native_api_free_u8_array_result(result)


def _call_json(lib: ctypes.CDLL, operation: str, request: dict):
    return _decode_result(
        lib,
        lib.ome_zarr_native_api_call_json(
            operation.encode(),
            json.dumps(request).encode(),
        ),
    )


def _csv_payload_from_python(value: str, col_type: str) -> dict:
    parsed = _py_csv.parse_csv_value(value, col_type)
    if isinstance(parsed, bool):
        return {"type": "bool", "value": parsed}
    if isinstance(parsed, int):
        return {"type": "int", "value": parsed}
    if isinstance(parsed, float):
        if math.isnan(parsed):
            return {"type": "float", "repr": "nan"}
        if math.isinf(parsed):
            return {"type": "float", "repr": "inf" if parsed > 0 else "-inf"}
        return {"type": "float", "value": parsed}
    return {"type": "str", "value": parsed}


def _u8_pointer(array: np.ndarray):
    return array.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8))


def _shape_pointer(shape: tuple[int, ...] | list[int]):
    shape_array = (ctypes.c_size_t * len(shape))(*shape)
    return shape_array, ctypes.cast(shape_array, ctypes.POINTER(ctypes.c_size_t))


def test_c_api_metadata_exposes_stable_ffi_contract() -> None:
    lib = _native_api_lib()
    assert lib.ome_zarr_native_api_abi_version() == b"1"
    metadata = _decode_result(lib, lib.ome_zarr_native_api_project_metadata())

    assert metadata["api_style"] == "c_abi"
    assert "data.rgb_to_5d_u8" in metadata["buffer_operations"]
    assert "conversions.int_to_rgba" in metadata["operations"]
    assert "csv.parse_csv_value" in metadata["operations"]
    assert "io.local_io_signature" in metadata["operations"]
    assert "scale.scaler_methods" in metadata["operations"]


@pytest.mark.parametrize("value", [-(2**31), -1, 0, 1, 255, 2**31 - 1])
def test_c_api_conversions_match_frozen_upstream(value: int) -> None:
    lib = _native_api_lib()

    assert _decode_result(
        lib,
        lib.ome_zarr_native_api_int_to_rgba(value),
    ) == pytest.approx(_py_conversions.int_to_rgba(value))
    assert _decode_result(
        lib,
        lib.ome_zarr_native_api_int_to_rgba_255(value),
    ) == _py_conversions.int_to_rgba_255(value)


@pytest.mark.parametrize(
    "rgba",
    [(0, 0, 0, 0), (0, 0, 0, 255), (255, 255, 255, 255), (128, 64, 32, 16)],
)
def test_c_api_rgba_to_int_matches_frozen_upstream(
    rgba: tuple[int, int, int, int],
) -> None:
    lib = _native_api_lib()
    assert _decode_result(
        lib,
        lib.ome_zarr_native_api_rgba_to_int(*rgba),
    ) == _py_conversions.rgba_to_int(*rgba)


@pytest.mark.parametrize(
    ("value", "col_type"),
    [
        ("1.25", "d"),
        ("nan", "d"),
        ("inf", "d"),
        ("2.5", "l"),
        ("nan", "l"),
        ("False", "b"),
        ("", "b"),
        ("unchanged", "s"),
        ("bad-float", "d"),
    ],
)
def test_c_api_csv_parse_matches_frozen_upstream(value: str, col_type: str) -> None:
    lib = _native_api_lib()
    assert _decode_result(
        lib,
        lib.ome_zarr_native_api_parse_csv_value(value.encode(), col_type.encode()),
    ) == _csv_payload_from_python(value, col_type)


def test_c_api_json_dispatch_matches_native_direct_calls() -> None:
    lib = _native_api_lib()

    assert _call_json(lib, "conversions.int_to_rgba_255", {"value": -1}) == [
        255,
        255,
        255,
        255,
    ]
    assert _call_json(lib, "conversions.rgba_to_int", {"rgba": [255, 0, 0, 0]}) == (
        _py_conversions.rgba_to_int(255, 0, 0, 0)
    )

    format_v05 = _py_format.FormatV05()
    native_format = _call_json(lib, "format.format_from_version", {"version": "0.5"})
    assert native_format == {
        "version": "0.5",
        "class_name": "FormatV05",
        "zarr_format": format_v05.zarr_format,
        "chunk_key_encoding": format_v05.chunk_key_encoding,
    }


@pytest.mark.parametrize(
    "array",
    [
        np.arange(12, dtype=np.uint8).reshape(3, 4),
        np.arange(24, dtype=np.uint8).reshape(2, 4, 3),
        np.zeros((0, 3), dtype=np.uint8),
    ],
)
def test_c_api_numpy_uint8_rgb_to_5d_matches_frozen_upstream(
    array: np.ndarray,
) -> None:
    lib = _native_api_lib()
    contiguous = np.ascontiguousarray(array)
    shape_array, shape_pointer = _shape_pointer(list(contiguous.shape))

    actual, actual_shape = _decode_array(
        lib,
        lib.ome_zarr_native_api_rgb_to_5d_u8(
            _u8_pointer(contiguous),
            contiguous.ndim,
            shape_pointer,
        ),
    )
    expected = np.asarray(_py_data.rgb_to_5d(contiguous), dtype=np.uint8)

    assert actual_shape == expected.shape
    assert actual.dtype == np.uint8
    assert np.array_equal(actual, expected)
    assert shape_array[0] == contiguous.shape[0]


def test_c_api_numpy_input_requires_supported_dimension_count() -> None:
    lib = _native_api_lib()
    array = np.zeros((1, 2, 3, 4), dtype=np.uint8)
    shape_array, shape_pointer = _shape_pointer(list(array.shape))

    error_type, error_message = _decode_array_error(
        lib,
        lib.ome_zarr_native_api_rgb_to_5d_u8(
            _u8_pointer(array),
            array.ndim,
            shape_pointer,
        ),
    )

    with pytest.raises(AssertionError) as upstream_error:
        _py_data.rgb_to_5d(array)

    assert error_type == type(upstream_error.value).__name__
    assert error_message == str(upstream_error.value)
    assert shape_array[0] == array.shape[0]


def test_c_api_local_zarr_signature_matches_frozen_upstream_zarr_store(
    tmp_path: Path,
) -> None:
    lib = _native_api_lib()
    root = tmp_path / "image-v2.zarr"
    write_minimal_v2_image(root)
    upstream_location = _py_io.parse_url(root)
    assert upstream_location is not None

    actual = _call_json(
        lib,
        "io.local_io_signature",
        {"path": str(root), "mode": "r"},
    )

    assert actual == location_signature(upstream_location)


def test_c_api_local_zarr_signature_matches_missing_read_semantics(
    tmp_path: Path,
) -> None:
    lib = _native_api_lib()

    assert _py_io.parse_url(tmp_path / "missing.zarr") is None
    assert (
        _call_json(
            lib,
            "io.local_io_signature",
            {"path": str(tmp_path / "missing.zarr"), "mode": "r"},
        )
        is None
    )


def test_c_api_scale_metadata_matches_frozen_upstream() -> None:
    lib = _native_api_lib()

    assert _call_json(lib, "scale.scaler_methods", {}) == list(
        _py_scale.Scaler.methods()
    )

    image = np.zeros((1, 1, 1, 64, 64), dtype=np.uint8)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        expected_shape = list(_py_scale.Scaler(downscale=2).resize_image(image).shape)

    assert (
        _call_json(
            lib,
            "scale.resize_image_shape",
            {"shape": list(image.shape), "downscale": 2},
        )
        == expected_shape
    )


def test_c_api_reports_json_dispatch_errors_without_raising_across_ffi() -> None:
    lib = _native_api_lib()
    error_type, error_message = _decode_error(
        lib,
        lib.ome_zarr_native_api_call_json(
            b"conversions.int_to_rgba",
            b'{"value": 2147483648}',
        ),
    )

    assert error_type == "OverflowError"
    assert "Integer out of int32 range" in error_message
