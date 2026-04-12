from __future__ import annotations

import copy
import importlib
import logging
import sys
from pathlib import Path

from tests._outcomes import err, ok

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_format = importlib.import_module("ome_zarr.format")
_py_writer = importlib.import_module("ome_zarr.writer")
_cpp_format = importlib.import_module("ome_zarr_c.format")
_cpp_writer = importlib.import_module("ome_zarr_c.writer")


def _compressor_signature(codec) -> dict:
    return {
        "type": type(codec).__name__,
        "config": codec.get_config(),
    }


def _call_with_logs(func, *args, **kwargs):
    records = []
    logger = logging.getLogger("ome_zarr.writer")
    previous_level = logger.level
    previous_propagate = logger.propagate

    class _Capture(logging.Handler):
        def emit(self, record) -> None:
            records.append((record.levelno, record.getMessage()))

    handler = _Capture()
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    try:
        try:
            return ok(value=func(*args, **kwargs), records=records)
        except Exception as exc:  # noqa: BLE001
            return err(exc, records=records)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
        logger.propagate = previous_propagate


def _assert_result_match(py_func, cpp_func, py_args=(), cpp_args=()) -> None:
    expected = _call_with_logs(py_func, *py_args)
    actual = _call_with_logs(cpp_func, *cpp_args)
    assert expected == actual


def test_get_valid_axes_matches_upstream() -> None:
    strange_axes = [
        {"name": "duration", "type": "time"},
        {"name": "rotation", "type": "angle"},
        {"name": "dz", "type": "space"},
        {"name": "WIDTH", "type": "space"},
    ]

    cases = [
        (_py_format.FormatV01(), _cpp_format.FormatV01(), 2, ["y", "x"]),
        (_py_format.FormatV02(), _cpp_format.FormatV02(), None, None),
        (_py_format.FormatV03(), _cpp_format.FormatV03(), 2, None),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), 5, None),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), 3, ["z", "y", "x"]),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), 4, "czyx"),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), 3, ["foo", "y", "x"]),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), 3, ["x", "z", "y"]),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), 4, strange_axes),
        (_py_format.FormatV05(), _cpp_format.FormatV05(), 2, None),
    ]

    for py_fmt, cpp_fmt, ndim, axes in cases:
        _assert_result_match(
            _py_writer._get_valid_axes,
            _cpp_writer._get_valid_axes,
            py_args=(ndim, copy.deepcopy(axes), py_fmt),
            cpp_args=(ndim, copy.deepcopy(axes), cpp_fmt),
        )

    error_cases = [
        (_py_format.FormatV04(), _cpp_format.FormatV04(), 2, [{"name": "y"}, {}]),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), 3, None),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), 4, ["z", "y", "x"]),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), 3, ["y", "c", "x"]),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), 4, "ctyx"),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), 4, ["foo", "bar", "y", "x"]),
    ]

    for py_fmt, cpp_fmt, ndim, axes in error_cases:
        _assert_result_match(
            _py_writer._get_valid_axes,
            _cpp_writer._get_valid_axes,
            py_args=(ndim, copy.deepcopy(axes), py_fmt),
            cpp_args=(ndim, copy.deepcopy(axes), cpp_fmt),
        )


def test_extract_dims_from_axes_matches_upstream() -> None:
    cases = [
        None,
        [],
        ["t", "c", "z", "y", "x"],
        [{"name": "c", "type": "channel"}, {"name": "y", "type": "space"}],
        {"x": 1, "y": 2},
        [{"name": "y"}, {"type": "space"}],
        ["x", {"name": "y"}],
    ]

    for axes in cases:
        _assert_result_match(
            _py_writer._extract_dims_from_axes,
            _cpp_writer._extract_dims_from_axes,
            py_args=(copy.deepcopy(axes),),
            cpp_args=(copy.deepcopy(axes),),
        )


def test_retuple_matches_upstream() -> None:
    cases = [
        (64, (3, 4, 5, 1028, 1028)),
        (True, (2, 3)),
        ((), (1, 2)),
        ((64, 64), (3, 4, 5, 1028, 1028)),
        ([64, 64], (3, 4, 5, 1028, 1028)),
        ((3, 4, 5, 64, 64), (3, 4, 5, 1028, 1028)),
        ((1, 2, 3), (4, 5)),
    ]

    for chunks, shape in cases:
        _assert_result_match(
            _py_writer._retuple,
            _cpp_writer._retuple,
            py_args=(copy.deepcopy(chunks), shape),
            cpp_args=(copy.deepcopy(chunks), shape),
        )


def test_validate_well_images_matches_upstream() -> None:
    cases = [
        [],
        ["A/1", {"path": "B/2"}, {"path": "C/3", "acquisition": 1}],
        [{"path": "A/1", "extra": "alpha"}],
        [{"path": 3}],
        [{"path": "A/1", "acquisition": "1"}],
        [{}],
        [1],
    ]

    for images in cases:
        _assert_result_match(
            _py_writer._validate_well_images,
            _cpp_writer._validate_well_images,
            py_args=(copy.deepcopy(images),),
            cpp_args=(copy.deepcopy(images),),
        )


def test_validate_plate_acquisitions_matches_upstream() -> None:
    cases = [
        [],
        [{"id": 0}],
        [{"id": True}],
        [{"id": 0, "extra": "alpha"}],
        [{"name": "scan"}],
        [{"id": "0"}],
        [1],
    ]

    for acquisitions in cases:
        _assert_result_match(
            _py_writer._validate_plate_acquisitions,
            _cpp_writer._validate_plate_acquisitions,
            py_args=(copy.deepcopy(acquisitions),),
            cpp_args=(copy.deepcopy(acquisitions),),
        )


def test_validate_plate_rows_columns_matches_upstream() -> None:
    cases = [
        ["A", "B"],
        ["1", "2"],
        ["A", "B", "B"],
        ["A", "&"],
        [1],
    ]

    for rows_or_columns in cases:
        _assert_result_match(
            _py_writer._validate_plate_rows_columns,
            _cpp_writer._validate_plate_rows_columns,
            py_args=(copy.deepcopy(rows_or_columns),),
            cpp_args=(copy.deepcopy(rows_or_columns),),
        )


def test_validate_datasets_matches_upstream() -> None:
    valid_transformations = [
        [{"type": "scale", "scale": [1, 1]}],
        [{"type": "scale", "scale": [2, 2]}],
    ]

    cases = [
        (_py_format.FormatV03(), _cpp_format.FormatV03(), [{"path": "0"}], 2),
        (
            _py_format.FormatV04(),
            _cpp_format.FormatV04(),
            [
                {"path": "0", "coordinateTransformations": valid_transformations[0]},
                {"path": "1", "coordinateTransformations": valid_transformations[1]},
            ],
            2,
        ),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), None, 2),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), [], 2),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), [{"path": ""}], 2),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), [{"foo": "bar"}], 2),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), [1], 2),
    ]

    for py_fmt, cpp_fmt, datasets, dims in cases:
        _assert_result_match(
            _py_writer._validate_datasets,
            _cpp_writer._validate_datasets,
            py_args=(copy.deepcopy(datasets), dims, py_fmt),
            cpp_args=(copy.deepcopy(datasets), dims, cpp_fmt),
        )


def test_validate_plate_wells_matches_upstream() -> None:
    rows = ["A", "B"]
    columns = ["1", "2"]
    cases = [
        (_py_format.FormatV01(), _cpp_format.FormatV01(), ["A/1", "B/2"]),
        (
            _py_format.FormatV04(),
            _cpp_format.FormatV04(),
            [
                {"path": "A/1", "rowIndex": 0, "columnIndex": 0},
                {"path": "B/2", "rowIndex": 1, "columnIndex": 1},
            ],
        ),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), []),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), None),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), [1]),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), ["C/1"]),
        (_py_format.FormatV04(), _cpp_format.FormatV04(), [{"path": "A/1"}]),
    ]

    for py_fmt, cpp_fmt, wells in cases:
        _assert_result_match(
            _py_writer._validate_plate_wells,
            _cpp_writer._validate_plate_wells,
            py_args=(copy.deepcopy(wells), rows, columns, py_fmt),
            cpp_args=(copy.deepcopy(wells), rows, columns, cpp_fmt),
        )


def test_blosc_compressor_matches_upstream() -> None:
    expected = _compressor_signature(_py_writer._blosc_compressor())
    actual = _compressor_signature(_cpp_writer._blosc_compressor())
    assert expected == actual


def test_resolve_storage_options_matches_upstream() -> None:
    cases = [
        (None, 0),
        ({}, 0),
        ({"compressor": "zlib", "foo": 1}, 0),
        ([{"compressor": "zlib"}, {"compressor": "blosc"}], 1),
        ([{"compressor": "zlib"}], 1),
    ]

    for storage_options, path in cases:
        _assert_result_match(
            _py_writer._resolve_storage_options,
            _cpp_writer._resolve_storage_options,
            py_args=(copy.deepcopy(storage_options), path),
            cpp_args=(copy.deepcopy(storage_options), path),
        )
