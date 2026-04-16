from __future__ import annotations

from benchmarks import cases as core_cases

# Public-API benchmark coverage is restricted to cases with a standalone native
# C++ timing path. Do not add Python package-path converted timings here.
PUBLIC_API_CASES: tuple[core_cases.BenchmarkCase, ...] = tuple(core_cases.ALL_CASES)


def iter_public_api_cases() -> tuple[core_cases.BenchmarkCase, ...]:
    return PUBLIC_API_CASES


__all__ = ["PUBLIC_API_CASES", "iter_public_api_cases"]
