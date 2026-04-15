from __future__ import annotations

import importlib
import json
import random
import sys
from itertools import product
from pathlib import Path

from tests import test_utils_equivalence as utils_eq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_conversions = importlib.import_module("ome_zarr.conversions")


def _run_native_int_to_rgba(value: int) -> list[float]:
    outcome = utils_eq._run_native_probe(["int-to-rgba", "--value", str(value)])
    assert outcome.status == "ok", outcome
    return [float(component) for component in outcome.value]


def _run_native_int_to_rgba_255(value: int) -> list[int]:
    outcome = utils_eq._run_native_probe(["int-to-rgba-255", "--value", str(value)])
    assert outcome.status == "ok", outcome
    return [int(component) for component in outcome.value]


def _run_native_rgba_to_int(rgba: tuple[int, int, int, int]) -> int:
    outcome = utils_eq._run_native_probe(
        ["rgba-to-int", "--rgba-json", json.dumps(list(rgba))]
    )
    assert outcome.status == "ok", outcome
    return int(outcome.value)


def test_known_examples_match_upstream() -> None:
    for value in (0, 100100, -1, 1, -2_147_483_648, 2_147_483_647):
        assert _run_native_int_to_rgba(value) == _py_conversions.int_to_rgba(value)
        assert _run_native_int_to_rgba_255(value) == _py_conversions.int_to_rgba_255(
            value
        )


def test_rgba_roundtrip_matches_upstream() -> None:
    sample_bytes = [0, 1, 127, 128, 254, 255]
    for rgba in product(sample_bytes, repeat=4):
        assert _run_native_rgba_to_int(rgba) == _py_conversions.rgba_to_int(*rgba)
        roundtrip = _run_native_int_to_rgba_255(_run_native_rgba_to_int(rgba))
        assert roundtrip == list(rgba)


def test_random_signed_32bit_values_match_upstream() -> None:
    rng = random.Random(0)
    for _ in range(5000):
        value = rng.randint(-(2**31), 2**31 - 1)
        assert _run_native_int_to_rgba(value) == _py_conversions.int_to_rgba(value)
        assert _run_native_int_to_rgba_255(value) == _py_conversions.int_to_rgba_255(
            value
        )
