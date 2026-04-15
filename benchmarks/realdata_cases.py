from __future__ import annotations

import os

from benchmarks import cases as core_cases
from benchmarks.public_fixtures import ensure_fixture
from benchmarks.runtime_support import run_info, run_parse_url
from tests import test_io_equivalence as io_eq
from tests import test_utils_equivalence as utils_eq


def _include_large_fixture() -> bool:
    value = os.environ.get("OME_ZARR_BENCH_INCLUDE_LARGE", "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _native_info_stdout(path) -> str:
    outcome = utils_eq._run_native_cli(["info", str(path)])
    if outcome.status != "ok":
        raise AssertionError(
            f"native info failed for {path}: "
            f"{outcome.error_type} {outcome.error_message}"
        )
    if outcome.payload["stderr"] != "":
        raise AssertionError(f"native info emitted stderr for {path}")
    return outcome.stdout


def _python_info_stdout(path) -> str:
    outcome = run_info(utils_eq._py_utils.info, path, stats=False)
    if outcome.status != "ok":
        raise AssertionError(
            f"python info failed for {path}: "
            f"{outcome.error_type} {outcome.error_message}"
        )
    return outcome.stdout


def _verify_examples_image_surface() -> None:
    path = ensure_fixture("examples_image")
    core_cases._assert_parity(
        "realdata.examples_image_surface",
        {
            "parse_url": run_parse_url(io_eq._py_io.parse_url, str(path)),
            "info": _python_info_stdout(path),
        },
        {
            "parse_url": io_eq._run_native_io_signature(str(path)),
            "info": _native_info_stdout(path),
        },
    )


def _bench_examples_image_surface(
    utils_module,
    *,
    native_parse_url: bool,
) -> float:
    path = ensure_fixture("examples_image")
    info_value = (
        _python_info_stdout(path)
        if utils_module == "python"
        else _native_info_stdout(path)
    )
    return core_cases._touch(
        {
            "parse_url": (
                io_eq._run_native_io_signature(str(path))
                if native_parse_url
                else run_parse_url(io_eq._py_io.parse_url, str(path))
            ),
            "info": info_value,
        }
    )


def _verify_examples_plate_surface() -> None:
    path = ensure_fixture("examples_plate")
    core_cases._assert_parity(
        "realdata.examples_plate_surface",
        {
            "parse_url": run_parse_url(io_eq._py_io.parse_url, str(path)),
            "info": _python_info_stdout(path),
        },
        {
            "parse_url": io_eq._run_native_io_signature(str(path)),
            "info": _native_info_stdout(path),
        },
    )


def _bench_examples_plate_surface(
    utils_module,
    *,
    native_parse_url: bool,
) -> float:
    path = ensure_fixture("examples_plate")
    info_value = (
        _python_info_stdout(path)
        if utils_module == "python"
        else _native_info_stdout(path)
    )
    return core_cases._touch(
        {
            "parse_url": (
                io_eq._run_native_io_signature(str(path))
                if native_parse_url
                else run_parse_url(io_eq._py_io.parse_url, str(path))
            ),
            "info": info_value,
        }
    )


def _verify_bia_tonsil3_surface() -> None:
    path = ensure_fixture("bia_tonsil3")
    core_cases._assert_parity(
        "realdata.bia_tonsil3_surface",
        {
            "parse_url": run_parse_url(io_eq._py_io.parse_url, str(path)),
            "info": _python_info_stdout(path),
        },
        {
            "parse_url": io_eq._run_native_io_signature(str(path)),
            "info": _native_info_stdout(path),
        },
    )


def _bench_bia_tonsil3_surface(
    utils_module,
    *,
    native_parse_url: bool,
) -> float:
    path = ensure_fixture("bia_tonsil3")
    info_value = (
        _python_info_stdout(path)
        if utils_module == "python"
        else _native_info_stdout(path)
    )
    return core_cases._touch(
        {
            "parse_url": (
                io_eq._run_native_io_signature(str(path))
                if native_parse_url
                else run_parse_url(io_eq._py_io.parse_url, str(path))
            ),
            "info": info_value,
        }
    )


def _verify_bia_156_42_surface() -> None:
    path = ensure_fixture("bia_156_42")
    core_cases._assert_parity(
        "realdata.bia_156_42_surface",
        {
            "parse_url": run_parse_url(io_eq._py_io.parse_url, str(path)),
            "info": _python_info_stdout(path),
        },
        {
            "parse_url": io_eq._run_native_io_signature(str(path)),
            "info": _native_info_stdout(path),
        },
    )


def _bench_bia_156_42_surface(
    utils_module,
    *,
    native_parse_url: bool,
) -> float:
    path = ensure_fixture("bia_156_42")
    info_value = (
        _python_info_stdout(path)
        if utils_module == "python"
        else _native_info_stdout(path)
    )
    return core_cases._touch(
        {
            "parse_url": (
                io_eq._run_native_io_signature(str(path))
                if native_parse_url
                else run_parse_url(io_eq._py_io.parse_url, str(path))
            ),
            "info": info_value,
        }
    )


REALDATA_CASES = [
    core_cases._make_case(
        "realdata",
        "examples_image.surface",
        "Real-data parse_url/info benchmark on a small example OME-Zarr image.",
        _verify_examples_image_surface,
        lambda: _bench_examples_image_surface("python", native_parse_url=False),
        lambda: _bench_examples_image_surface("native", native_parse_url=True),
    ),
    core_cases._make_case(
        "realdata",
        "examples_plate.surface",
        "Real-data parse_url/info benchmark on a small example OME-Zarr plate.",
        _verify_examples_plate_surface,
        lambda: _bench_examples_plate_surface("python", native_parse_url=False),
        lambda: _bench_examples_plate_surface("native", native_parse_url=True),
    ),
    core_cases._make_case(
        "realdata",
        "bia_tonsil3.surface",
        (
            "Real-data parse_url/info benchmark on the 108.8 MiB "
            "BIA tonsil OME-NGFF image."
        ),
        _verify_bia_tonsil3_surface,
        lambda: _bench_bia_tonsil3_surface("python", native_parse_url=False),
        lambda: _bench_bia_tonsil3_surface("native", native_parse_url=True),
    ),
]

if _include_large_fixture():
    REALDATA_CASES.append(
        core_cases._make_case(
            "realdata",
            "bia_156_42.surface",
            (
                "Real-data parse_url/info benchmark on the 455.3 MiB "
                "BIA high-content screening OME-NGFF image."
            ),
            _verify_bia_156_42_surface,
            lambda: _bench_bia_156_42_surface("python", native_parse_url=False),
            lambda: _bench_bia_156_42_surface("native", native_parse_url=True),
        )
    )


def iter_realdata_cases() -> tuple[core_cases.BenchmarkCase, ...]:
    return tuple(REALDATA_CASES)


__all__ = ["REALDATA_CASES", "iter_realdata_cases"]
