from __future__ import annotations

import importlib

import pytest


def _report_module():
    pytest.importorskip("pyperf")
    return importlib.import_module("benchmarks.report")


def test_markdown_report_uses_native_cpp_time_convention() -> None:
    report = _report_module()

    markdown = report._render_markdown(
        [
            {
                "group": "format",
                "name": "dispatch",
                "variant": "native",
                "python_median": 4.0,
                "native_cpp_median": 1.0,
                "relative_ratio": 4.0,
                "status": report._status(4.0, "native"),
            }
        ]
    )

    assert (
        "| Case | Variant | Python time | native C++ time | time saved | "
        "native C++ time reduction | native C++ speedup over Python "
        "(`python_time / native_cpp_time`) | Status |"
    ) in markdown
    assert (
        "- Pairing rule: Python and native C++ results are paired by benchmark "
        "base name after stripping `.python` and `.native`; incomplete pairs "
        "stop the report."
    ) in markdown
    assert (
        "- Derived fields: `time_saved = python_time - native_cpp_time`; "
        "native C++ time reduction is `time_saved / python_time`."
    ) in markdown
    assert (
        "- format native C++ speedup over Python "
        "(`python_time / native_cpp_time`): 4.000x"
    ) in markdown
    assert (
        "| dispatch | native C++ | 4.000 s | 1.000 s | 3.000 s | "
        "75.0% | 4.000x | native C++ faster than Python |"
    ) in markdown


def test_markdown_report_keeps_regression_signs_visible() -> None:
    report = _report_module()

    markdown = report._render_markdown(
        [
            {
                "group": "utils",
                "name": "path_helpers",
                "variant": "native",
                "python_median": 1.0,
                "native_cpp_median": 1.5,
                "relative_ratio": 1.0 / 1.5,
                "status": report._status(1.0 / 1.5, "native"),
            }
        ]
    )

    assert (
        "| path_helpers | native C++ | 1.000 s | 1.500 s | -500.00 ms | "
        "-50.0% | 0.667x | native C++ slower than Python |"
    ) in markdown
