from __future__ import annotations

import importlib
import json
import subprocess
import sys
from itertools import product
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

from tests import test_utils_equivalence as utils_eq  # noqa: E402

_py_axes = importlib.import_module("ome_zarr.axes")
_py_format = importlib.import_module("ome_zarr.format")

PythonAxes = _py_axes.Axes
FormatV01 = _py_format.FormatV01
FormatV02 = _py_format.FormatV02
FormatV03 = _py_format.FormatV03
FormatV04 = _py_format.FormatV04
FormatV05 = _py_format.FormatV05

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


def _native_probe_path() -> Path:
    return utils_eq._native_cli_path().with_name("ome_zarr_native_probe")


def _run_axes(cls, axes_input, fmt):
    instance = cls(axes_input, fmt)
    return {
        "axes": getattr(instance, "axes", None),
        "to_list_03": instance.to_list(FormatV03()),
        "to_list_04": instance.to_list(FormatV04()),
    }


def _raise_native_error(error_type: str, error_message: str) -> None:
    error_classes = {
        "AttributeError": AttributeError,
        "TypeError": TypeError,
        "ValueError": ValueError,
    }
    error_class = error_classes.get(error_type, RuntimeError)
    raise error_class(error_message)


def _run_native_axes(axes_input, fmt):
    completed = subprocess.run(
        [
            str(_native_probe_path()),
            "axes",
            "--axes-json",
            json.dumps(axes_input),
            "--format-version",
            str(fmt.version),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout)
    if payload["status"] != "ok":
        _raise_native_error(payload["error_type"], payload["error_message"])
    return payload["value"]


def _python_outcome(axes_input, fmt):
    try:
        return ("ok", _run_axes(PythonAxes, axes_input, fmt))
    except Exception as exc:  # noqa: BLE001
        return ("err", type(exc).__name__, str(exc))


def _native_outcomes(cases):
    completed = subprocess.run(
        [
            str(_native_probe_path()),
            "axes-batch",
            "--cases-json",
            json.dumps(cases),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout)
    if payload["status"] != "ok":
        raise RuntimeError(payload)
    outcomes = []
    for item in payload["value"]:
        if item["status"] == "ok":
            outcomes.append(("ok", item["value"]))
        else:
            outcomes.append(("err", item["error_type"], item["error_message"]))
    return outcomes


def _compare_cases(cases):
    chunk_size = 256
    for offset in range(0, len(cases), chunk_size):
        chunk = cases[offset : offset + chunk_size]
        expected = [_python_outcome(case["axes"], case["format"]) for case in chunk]
        actual = _native_outcomes(
            [
                {"axes": case["axes"], "format_version": str(case["format"].version)}
                for case in chunk
            ]
        )
        assert expected == actual


def test_axes_none_behavior_matches_upstream() -> None:
    _compare_cases([{"axes": None, "format": fmt} for fmt in FORMATS])


def test_axes_string_combinations_match_upstream() -> None:
    cases = []
    for fmt in FORMATS:
        for length in range(2, 6):
            for axes_input in product(AXIS_NAMES, repeat=length):
                cases.append({"axes": list(axes_input), "format": fmt})
    _compare_cases(cases)


def test_axes_dict_combinations_match_upstream() -> None:
    cases = []
    for fmt in (FormatV04(), FormatV05()):
        for length in range(2, 6):
            for axes_input in product(AXIS_DICTS, repeat=length):
                cases.append(
                    {
                        "axes": [dict(item) for item in axes_input],
                        "format": fmt,
                    }
                )
    _compare_cases(cases)
