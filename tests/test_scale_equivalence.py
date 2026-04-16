from __future__ import annotations

from tests import test_utils_equivalence as utils_eq


def test_scale_native_selftest_section_passes() -> None:
    utils_eq.assert_native_selftest_section("dask_scale_and_data")
