from __future__ import annotations

from benchmarks import cases as core_cases
from benchmarks.public_api_cases import PUBLIC_API_CASES
from benchmarks.realdata_cases import REALDATA_CASES

SUITE_CASES = {
    "core": tuple(core_cases.ALL_CASES),
    "public-api": tuple(PUBLIC_API_CASES),
    "realdata": tuple(REALDATA_CASES),
}


def available_suites() -> tuple[str, ...]:
    return tuple(SUITE_CASES)


def benchmark_environment_metadata() -> dict[str, str]:
    metadata = dict(core_cases.benchmark_environment_metadata())
    metadata["available_benchmark_suites"] = ",".join(available_suites())
    return metadata


def iter_cases(
    *,
    match: str | None = None,
    groups: list[str] | None = None,
    suites: list[str] | None = None,
):
    selected_suites = suites or list(available_suites())
    unknown = [suite for suite in selected_suites if suite not in SUITE_CASES]
    if unknown:
        raise SystemExit(f"Unknown benchmark suite(s): {', '.join(unknown)}")

    selected = []
    for suite in selected_suites:
        selected.extend(SUITE_CASES[suite])

    if groups:
        allowed = set(groups)
        selected = [case for case in selected if case.group in allowed]
    if match:
        lowered = match.lower()
        selected = [
            case
            for case in selected
            if lowered in case.benchmark_base_name.lower()
            or lowered in case.description.lower()
        ]
    return selected


__all__ = [
    "SUITE_CASES",
    "available_suites",
    "benchmark_environment_metadata",
    "iter_cases",
]
