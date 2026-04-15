from __future__ import annotations

import os

from benchmarks import cases as core_cases
from benchmarks.public_fixtures import ensure_fixture
from benchmarks.runtime_support import run_info, run_parse_url
from tests import test_io_equivalence as io_eq
from tests import test_reader_equivalence as reader_eq
from tests import test_utils_equivalence as utils_eq


def _include_large_fixture() -> bool:
    value = os.environ.get("OME_ZARR_BENCH_INCLUDE_LARGE", "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _reader_summary(io_module, reader_module, path):
    location = io_module.parse_url(str(path))
    if location is None:
        raise AssertionError(f"realdata path is not parseable as zarr: {path}")
    return [
        reader_eq._node_signature(node, reader_module)
        for node in reader_module.Reader(location)()
    ]


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
            "reader": _reader_summary(io_eq._py_io, reader_eq._py_reader, path),
        },
        {
            "parse_url": run_parse_url(io_eq._cpp_io.parse_url, str(path)),
            "info": _native_info_stdout(path),
            "reader": _reader_summary(io_eq._cpp_io, reader_eq._cpp_reader, path),
        },
    )


def _bench_examples_image_surface(io_module, utils_module, reader_module) -> float:
    path = ensure_fixture("examples_image")
    info_value = (
        _python_info_stdout(path)
        if utils_module is utils_eq._py_utils
        else _native_info_stdout(path)
    )
    return core_cases._touch(
        {
            "parse_url": run_parse_url(io_module.parse_url, str(path)),
            "info": info_value,
            "reader": _reader_summary(io_module, reader_module, path),
        }
    )


def _verify_examples_plate_surface() -> None:
    path = ensure_fixture("examples_plate")
    core_cases._assert_parity(
        "realdata.examples_plate_surface",
        {
            "parse_url": run_parse_url(io_eq._py_io.parse_url, str(path)),
            "info": _python_info_stdout(path),
            "reader": _reader_summary(io_eq._py_io, reader_eq._py_reader, path),
        },
        {
            "parse_url": run_parse_url(io_eq._cpp_io.parse_url, str(path)),
            "info": _native_info_stdout(path),
            "reader": _reader_summary(io_eq._cpp_io, reader_eq._cpp_reader, path),
        },
    )


def _bench_examples_plate_surface(io_module, utils_module, reader_module) -> float:
    path = ensure_fixture("examples_plate")
    info_value = (
        _python_info_stdout(path)
        if utils_module is utils_eq._py_utils
        else _native_info_stdout(path)
    )
    return core_cases._touch(
        {
            "parse_url": run_parse_url(io_module.parse_url, str(path)),
            "info": info_value,
            "reader": _reader_summary(io_module, reader_module, path),
        }
    )


def _verify_bia_tonsil3_surface() -> None:
    path = ensure_fixture("bia_tonsil3")
    core_cases._assert_parity(
        "realdata.bia_tonsil3_surface",
        {
            "parse_url": run_parse_url(io_eq._py_io.parse_url, str(path)),
            "info": _python_info_stdout(path),
            "reader": _reader_summary(io_eq._py_io, reader_eq._py_reader, path),
        },
        {
            "parse_url": run_parse_url(io_eq._cpp_io.parse_url, str(path)),
            "info": _native_info_stdout(path),
            "reader": _reader_summary(io_eq._cpp_io, reader_eq._cpp_reader, path),
        },
    )


def _bench_bia_tonsil3_surface(io_module, utils_module, reader_module) -> float:
    path = ensure_fixture("bia_tonsil3")
    info_value = (
        _python_info_stdout(path)
        if utils_module is utils_eq._py_utils
        else _native_info_stdout(path)
    )
    return core_cases._touch(
        {
            "parse_url": run_parse_url(io_module.parse_url, str(path)),
            "info": info_value,
            "reader": _reader_summary(io_module, reader_module, path),
        }
    )


def _verify_bia_156_42_surface() -> None:
    path = ensure_fixture("bia_156_42")
    core_cases._assert_parity(
        "realdata.bia_156_42_surface",
        {
            "parse_url": run_parse_url(io_eq._py_io.parse_url, str(path)),
            "info": _python_info_stdout(path),
            "reader": _reader_summary(io_eq._py_io, reader_eq._py_reader, path),
        },
        {
            "parse_url": run_parse_url(io_eq._cpp_io.parse_url, str(path)),
            "info": _native_info_stdout(path),
            "reader": _reader_summary(io_eq._cpp_io, reader_eq._cpp_reader, path),
        },
    )


def _bench_bia_156_42_surface(io_module, utils_module, reader_module) -> float:
    path = ensure_fixture("bia_156_42")
    info_value = (
        _python_info_stdout(path)
        if utils_module is utils_eq._py_utils
        else _native_info_stdout(path)
    )
    return core_cases._touch(
        {
            "parse_url": run_parse_url(io_module.parse_url, str(path)),
            "info": info_value,
            "reader": _reader_summary(io_module, reader_module, path),
        }
    )


REALDATA_CASES = [
    core_cases._make_case(
        "realdata",
        "examples_image.surface",
        "Real-data parse_url/info/reader benchmark on a small example OME-Zarr image.",
        _verify_examples_image_surface,
        lambda: _bench_examples_image_surface(
            io_eq._py_io, utils_eq._py_utils, reader_eq._py_reader
        ),
        lambda: _bench_examples_image_surface(
            io_eq._cpp_io, utils_eq._cpp_utils, reader_eq._cpp_reader
        ),
    ),
    core_cases._make_case(
        "realdata",
        "examples_plate.surface",
        "Real-data parse_url/info/reader benchmark on a small example OME-Zarr plate.",
        _verify_examples_plate_surface,
        lambda: _bench_examples_plate_surface(
            io_eq._py_io, utils_eq._py_utils, reader_eq._py_reader
        ),
        lambda: _bench_examples_plate_surface(
            io_eq._cpp_io, utils_eq._cpp_utils, reader_eq._cpp_reader
        ),
    ),
    core_cases._make_case(
        "realdata",
        "bia_tonsil3.surface",
        (
            "Real-data parse_url/info/reader benchmark on the 108.8 MiB "
            "BIA tonsil OME-NGFF image."
        ),
        _verify_bia_tonsil3_surface,
        lambda: _bench_bia_tonsil3_surface(
            io_eq._py_io, utils_eq._py_utils, reader_eq._py_reader
        ),
        lambda: _bench_bia_tonsil3_surface(
            io_eq._cpp_io, utils_eq._cpp_utils, reader_eq._cpp_reader
        ),
    ),
]

if _include_large_fixture():
    REALDATA_CASES.append(
        core_cases._make_case(
            "realdata",
            "bia_156_42.surface",
            (
                "Real-data parse_url/info/reader benchmark on the 455.3 MiB "
                "BIA high-content screening OME-NGFF image."
            ),
            _verify_bia_156_42_surface,
            lambda: _bench_bia_156_42_surface(
                io_eq._py_io, utils_eq._py_utils, reader_eq._py_reader
            ),
            lambda: _bench_bia_156_42_surface(
                io_eq._cpp_io, utils_eq._cpp_utils, reader_eq._cpp_reader
            ),
        )
    )


def iter_realdata_cases() -> tuple[core_cases.BenchmarkCase, ...]:
    return tuple(REALDATA_CASES)


__all__ = ["REALDATA_CASES", "iter_realdata_cases"]
