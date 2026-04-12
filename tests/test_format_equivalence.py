from __future__ import annotations

import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_format = importlib.import_module("ome_zarr.format")
_cpp_format = importlib.import_module("ome_zarr_c.format")

PY_FORMAT_TYPES = [
    _py_format.FormatV01,
    _py_format.FormatV02,
    _py_format.FormatV03,
    _py_format.FormatV04,
    _py_format.FormatV05,
]
CPP_FORMAT_TYPES = [
    _cpp_format.FormatV01,
    _cpp_format.FormatV02,
    _cpp_format.FormatV03,
    _cpp_format.FormatV04,
    _cpp_format.FormatV05,
]


def _call(func, *args, **kwargs):
    try:
        return ("ok", func(*args, **kwargs))
    except Exception as exc:  # noqa: BLE001
        return ("err", type(exc), str(exc))


def _format_signature(fmt) -> tuple[str, int, dict[str, str], str]:
    return (fmt.version, fmt.zarr_format, fmt.chunk_key_encoding, repr(fmt))


def _assert_result_match(py_func, cpp_func, *args, **kwargs) -> None:
    expected = _call(py_func, *args, **kwargs)
    actual = _call(cpp_func, *args, **kwargs)

    if expected[0] == "err" or actual[0] == "err":
        assert expected[0] == actual[0]
        assert expected[1] is actual[1]
        assert expected[2] == actual[2]
        return

    assert expected == actual


def test_format_from_version_matches_upstream() -> None:
    for version in ("0.1", "0.2", "0.3", "0.4", "0.5", 0.2, 0.5):
        py_result = _call(_py_format.format_from_version, version)
        cpp_result = _call(_cpp_format.format_from_version, version)

        assert py_result[0] == cpp_result[0] == "ok"
        assert _format_signature(py_result[1]) == _format_signature(cpp_result[1])

    _assert_result_match(
        _py_format.format_from_version,
        _cpp_format.format_from_version,
        "1.0",
    )


def test_detect_format_matches_upstream() -> None:
    default_py = _py_format.FormatV03()
    default_cpp = _cpp_format.FormatV03()
    metadata_cases = [
        {},
        {"multiscales": [{"version": "0.5"}]},
        {"plate": {"version": "0.4"}},
        {"well": {"version": "0.3"}},
        {"image-label": {"version": "0.2"}},
    ]

    for metadata in metadata_cases:
        py_result = _py_format.detect_format(metadata, default_py)
        cpp_result = _cpp_format.detect_format(metadata, default_cpp)
        assert _format_signature(py_result) == _format_signature(cpp_result)


def test_format_properties_and_matches_align() -> None:
    metadata_cases = [
        {"multiscales": [{"version": "0.1"}]},
        {"multiscales": [{"version": "0.2"}]},
        {"multiscales": [{"version": "0.3"}]},
        {"multiscales": [{"version": "0.4"}]},
        {"multiscales": [{"version": "0.5"}]},
        {"plate": {"version": "0.4"}},
        {"well": {"version": "0.3"}},
        {"image-label": {"version": "0.2"}},
        {"plate": {}},
    ]

    for py_cls, cpp_cls in zip(PY_FORMAT_TYPES, CPP_FORMAT_TYPES, strict=True):
        py_fmt = py_cls()
        cpp_fmt = cpp_cls()
        assert _format_signature(py_fmt) == _format_signature(cpp_fmt)
        for metadata in metadata_cases:
            assert py_fmt.matches(metadata) == cpp_fmt.matches(metadata)


def test_generate_well_dict_matches_upstream() -> None:
    py_fmt = _py_format.FormatV04()
    cpp_fmt = _cpp_format.FormatV04()
    rows = ["A", "B", "C"]
    columns = ["1", "2", "3"]

    for well in ("A/1", "B/3", "D/1", "A/9", "A", "A/1/2"):
        _assert_result_match(
            py_fmt.generate_well_dict,
            cpp_fmt.generate_well_dict,
            well,
            rows,
            columns,
        )


def test_validate_well_dict_matches_upstream() -> None:
    rows = ["A", "B", "C"]
    columns = ["1", "2", "3"]
    v01_cases = [
        {"path": "A/1"},
        {"path": "A/1", "extra": 1},
        {},
        {"path": 3},
    ]
    v04_cases = [
        {"path": "A/1", "rowIndex": 0, "columnIndex": 0},
        {"path": "A/1", "rowIndex": True, "columnIndex": False},
        {"path": "A/1"},
        {"path": "A/1", "rowIndex": 0},
        {"path": "A/1", "columnIndex": 0},
        {"path": 3, "rowIndex": 0, "columnIndex": 0},
        {"path": "A/1/2", "rowIndex": 0, "columnIndex": 0},
        {"path": "D/1", "rowIndex": 0, "columnIndex": 0},
        {"path": "A/9", "rowIndex": 0, "columnIndex": 0},
        {"path": "A/1", "rowIndex": 1, "columnIndex": 0},
        {"path": "A/1", "rowIndex": 0, "columnIndex": 1},
    ]

    py_v01 = _py_format.FormatV01()
    cpp_v01 = _cpp_format.FormatV01()
    for well in v01_cases:
        _assert_result_match(
            py_v01.validate_well_dict,
            cpp_v01.validate_well_dict,
            dict(well),
            rows,
            columns,
        )

    py_v04 = _py_format.FormatV04()
    cpp_v04 = _cpp_format.FormatV04()
    for well in v04_cases:
        _assert_result_match(
            py_v04.validate_well_dict,
            cpp_v04.validate_well_dict,
            dict(well),
            rows,
            columns,
        )


def test_generate_coordinate_transformations_match_upstream() -> None:
    py_fmt = _py_format.FormatV04()
    cpp_fmt = _cpp_format.FormatV04()

    for shapes in (
        [(256, 256), (128, 128), (64, 64)],
        [(1, 3, 512, 512), (1, 3, 256, 256), (1, 3, 128, 128)],
        [(1, 1, 16, 32, 64), (1, 1, 8, 16, 32)],
    ):
        assert py_fmt.generate_coordinate_transformations(
            shapes
        ) == cpp_fmt.generate_coordinate_transformations(shapes)


def test_validate_coordinate_transformations_match_upstream() -> None:
    transformations = [
        [{"type": "scale", "scale": (1, 1)}],
        [{"type": "scale", "scale": (0.5, 0.5)}],
    ]
    translate = [{"type": "translation", "translation": (1, 1)}]
    scale_then_trans = [item + translate for item in transformations]
    double_translate = [item + translate for item in scale_then_trans]

    cases = [
        (2, 2, transformations),
        (2, 1, transformations),
        (2, 1, [[{"type": "scale", "scale": ("1", 1)}]]),
        (2, 1, [[{"type": "foo", "scale": (1, 1)}]]),
        (3, 1, [[{"type": "scale", "scale": (1, 1)}]]),
        (2, 2, scale_then_trans),
        (2, 2, [translate + item for item in transformations]),
        (2, 2, double_translate),
        (2, 1, [[{"scale": (1, 1)}]]),
        (2, 1, [[{"type": "scale"}]]),
        (2, 1, [[{"type": "scale", "scale": (1, 1)}, {"type": "translation"}]]),
        (
            2,
            1,
            [
                [
                    {"type": "scale", "scale": (1, 1)},
                    {"type": "translation", "translation": ("1", 1)},
                ]
            ],
        ),
    ]

    for py_cls, cpp_cls in (
        (_py_format.FormatV04, _cpp_format.FormatV04),
        (_py_format.FormatV05, _cpp_format.FormatV05),
    ):
        py_fmt = py_cls()
        cpp_fmt = cpp_cls()
        for ndim, nlevels, payload in cases:
            _assert_result_match(
                py_fmt.validate_coordinate_transformations,
                cpp_fmt.validate_coordinate_transformations,
                ndim,
                nlevels,
                payload,
            )
