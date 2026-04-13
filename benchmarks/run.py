from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pyperf

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

for env_var in (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
):
    os.environ.setdefault(env_var, "1")


def _parse_args(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--group", action="append")
    parser.add_argument("--suite", action="append")
    parser.add_argument("--match")
    parser.add_argument("--verify-only", action="store_true")
    return parser.parse_known_args(argv)


def _resolve_selection(
    parsed: argparse.Namespace,
) -> tuple[list[str] | None, str | None, list[str] | None]:
    groups = parsed.group
    suites = parsed.suite
    match = parsed.match

    if groups is not None:
        os.environ["OME_ZARR_BENCH_GROUPS"] = ",".join(groups)
    else:
        env_groups = os.environ.get("OME_ZARR_BENCH_GROUPS", "")
        groups = [group for group in env_groups.split(",") if group] or None

    if match is not None:
        os.environ["OME_ZARR_BENCH_MATCH"] = match
    else:
        match = os.environ.get("OME_ZARR_BENCH_MATCH") or None

    if suites is not None:
        os.environ["OME_ZARR_BENCH_SUITES"] = ",".join(suites)
    else:
        env_suites = os.environ.get("OME_ZARR_BENCH_SUITES", "")
        suites = [suite for suite in env_suites.split(",") if suite] or None

    return groups, match, suites


def main(argv: list[str] | None = None) -> int:
    from benchmarks.catalog import (
        available_suites,
        benchmark_environment_metadata,
        iter_cases,
    )

    parsed, pyperf_argv = _parse_args(argv or sys.argv[1:])
    sys.argv = [sys.argv[0], *pyperf_argv]
    groups, match, suites = _resolve_selection(parsed)

    cases = iter_cases(match=match, groups=groups, suites=suites)
    if not cases:
        raise SystemExit("No benchmark cases matched the requested filters.")

    if parsed.list:
        if suites:
            print(f"suites: {', '.join(suites)}")
        else:
            print(f"suites: {', '.join(available_suites())}")
        for case in cases:
            print(f"{case.benchmark_base_name}: {case.description}")
        return 0

    if parsed.verify_only:
        for case in cases:
            case.verify()
            print(f"verified {case.benchmark_base_name}")
        return 0

    def add_cmdline_args(cmd: list[str], _args: argparse.Namespace) -> None:
        for suite in suites or []:
            cmd.extend(["--suite", suite])
        for group in groups or []:
            cmd.extend(["--group", group])
        if match:
            cmd.extend(["--match", match])

    runner = pyperf.Runner(add_cmdline_args=add_cmdline_args)
    runner.metadata.update(benchmark_environment_metadata())
    runner.metadata["case_count"] = str(len(cases))
    runner.metadata["selected_suites"] = ",".join(suites or available_suites())

    for case in cases:
        runner.bench_time_func(f"{case.benchmark_base_name}.python", case.python_timer)
        runner.bench_time_func(f"{case.benchmark_base_name}.cpp", case.cpp_timer)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
