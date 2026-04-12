from __future__ import annotations

import importlib
import math
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_csv = importlib.import_module("ome_zarr.csv")
_cpp_csv = importlib.import_module("ome_zarr_c.csv")


def _assert_same_value(left, right) -> None:
    assert type(left) is type(right)
    if isinstance(left, float) and math.isnan(left):
        assert math.isnan(right)
        return
    assert left == right


def _assert_parse_case(value: str, col_type: str) -> None:
    expected_value = None
    actual_value = None
    expected_exc = None
    actual_exc = None

    try:
        expected_value = _py_csv.parse_csv_value(value, col_type)
    except Exception as exc:  # noqa: BLE001
        expected_exc = exc

    try:
        actual_value = _cpp_csv.parse_csv_value(value, col_type)
    except Exception as exc:  # noqa: BLE001
        actual_exc = exc

    if expected_exc or actual_exc:
        assert type(expected_exc) is type(actual_exc)
        assert str(expected_exc) == str(actual_exc)
        return

    _assert_same_value(expected_value, actual_value)


def test_column_types_match_upstream() -> None:
    assert _cpp_csv.COLUMN_TYPES == _py_csv.COLUMN_TYPES


def test_parse_csv_value_examples_match_upstream() -> None:
    values = [
        "",
        "0",
        "1",
        "-1",
        "1.5",
        "2.5",
        "-2.5",
        "abc",
        "True",
        "False",
        " 3 ",
        "1e3",
        "nan",
        "inf",
        "-inf",
    ]
    col_types = ["d", "l", "s", "b", "x"]

    for value in values:
        for col_type in col_types:
            _assert_parse_case(value, col_type)


def test_parse_csv_value_random_numeric_strings_match_upstream() -> None:
    rng = random.Random(0)
    for _ in range(5000):
        whole = rng.randint(-1_000_000, 1_000_000)
        frac = rng.randint(0, 9999)
        exponent = rng.randint(-5, 5)
        value = f"{whole}.{frac:04d}e{exponent:+d}"
        for col_type in ("d", "l"):
            _assert_parse_case(value, col_type)
