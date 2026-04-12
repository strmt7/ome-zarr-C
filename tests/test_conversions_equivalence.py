from __future__ import annotations

import importlib
import random
import sys
from itertools import product
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_conversions = importlib.import_module("ome_zarr.conversions")
_cpp_conversions = importlib.import_module("ome_zarr_c.conversions")

py_int_to_rgba = _py_conversions.int_to_rgba
py_int_to_rgba_255 = _py_conversions.int_to_rgba_255
py_rgba_to_int = _py_conversions.rgba_to_int
cpp_int_to_rgba = _cpp_conversions.int_to_rgba
cpp_int_to_rgba_255 = _cpp_conversions.int_to_rgba_255
cpp_rgba_to_int = _cpp_conversions.rgba_to_int


def test_known_examples_match_upstream() -> None:
    for value in (0, 100100, -1, 1, -2_147_483_648, 2_147_483_647):
        assert cpp_int_to_rgba(value) == py_int_to_rgba(value)
        assert cpp_int_to_rgba_255(value) == py_int_to_rgba_255(value)


def test_rgba_roundtrip_matches_upstream() -> None:
    sample_bytes = [0, 1, 127, 128, 254, 255]
    for rgba in product(sample_bytes, repeat=4):
        assert cpp_rgba_to_int(*rgba) == py_rgba_to_int(*rgba)
        roundtrip = cpp_int_to_rgba_255(cpp_rgba_to_int(*rgba))
        assert roundtrip == list(rgba)


def test_random_signed_32bit_values_match_upstream() -> None:
    rng = random.Random(0)
    for _ in range(5000):
        value = rng.randint(-(2**31), 2**31 - 1)
        assert cpp_int_to_rgba(value) == py_int_to_rgba(value)
        assert cpp_int_to_rgba_255(value) == py_int_to_rgba_255(value)
