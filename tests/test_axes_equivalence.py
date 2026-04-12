from __future__ import annotations

import importlib
import sys
from itertools import product
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_axes = importlib.import_module("ome_zarr.axes")
_py_format = importlib.import_module("ome_zarr.format")
_cpp_axes = importlib.import_module("omero_zarr_c.axes")

PythonAxes = _py_axes.Axes
FormatV01 = _py_format.FormatV01
FormatV02 = _py_format.FormatV02
FormatV03 = _py_format.FormatV03
FormatV04 = _py_format.FormatV04
FormatV05 = _py_format.FormatV05
CppAxes = _cpp_axes.Axes

FORMATS = [FormatV01(), FormatV02(), FormatV03(), FormatV04(), FormatV05()]
AXIS_NAMES = ["x", "y", "z", "c", "t", "foo", "bar"]
AXIS_DICTS = [
    {"name": "x", "type": "space"},
    {"name": "y", "type": "space"},
    {"name": "z", "type": "space"},
    {"name": "c", "type": "channel"},
    {"name": "t", "type": "time"},
    {"name": "foo"},
    {"name": "foo", "type": "foo"},
    {"name": "bar", "type": "bar"},
]


def _run_axes(cls, axes_input, fmt):
    instance = cls(axes_input, fmt)
    return {
        "axes": getattr(instance, "axes", None),
        "to_list_03": instance.to_list(FormatV03()),
        "to_list_04": instance.to_list(FormatV04()),
    }


def _compare_case(axes_input, fmt):
    py_result = None
    cpp_result = None
    py_exc = None
    cpp_exc = None

    try:
        py_result = _run_axes(PythonAxes, axes_input, fmt)
    except Exception as exc:  # noqa: BLE001
        py_exc = exc

    try:
        cpp_result = _run_axes(CppAxes, axes_input, fmt)
    except Exception as exc:  # noqa: BLE001
        cpp_exc = exc

    if py_exc or cpp_exc:
        assert type(py_exc) is type(cpp_exc)
        assert str(py_exc) == str(cpp_exc)
        return

    assert py_result == cpp_result


def test_axes_none_behavior_matches_upstream() -> None:
    for fmt in FORMATS:
        _compare_case(None, fmt)


def test_axes_string_combinations_match_upstream() -> None:
    for fmt in FORMATS:
        for length in range(2, 6):
            for axes_input in product(AXIS_NAMES, repeat=length):
                _compare_case(list(axes_input), fmt)


def test_axes_dict_combinations_match_upstream() -> None:
    for fmt in (FormatV04(), FormatV05()):
        for length in range(2, 6):
            for axes_input in product(AXIS_DICTS, repeat=length):
                _compare_case([dict(item) for item in axes_input], fmt)
