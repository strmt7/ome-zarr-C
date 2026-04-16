from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import tempfile
from pathlib import Path

import pyperf

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BUILD_DIR = ROOT / "build-cpp"
DEFAULT_PYTHON = ROOT / ".venv" / "bin" / "python"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a bounded iteration benchmark comparison for one touched "
            "surface using both the Python-visible parity harness and the "
            "standalone native bench."
        )
    )
    parser.add_argument("--suite", default="public-api")
    parser.add_argument("--match", required=True)
    parser.add_argument("--python-match")
    parser.add_argument("--native-match")
    parser.add_argument("--build-dir", type=Path, default=DEFAULT_BUILD_DIR)
    parser.add_argument("--python", type=Path, default=DEFAULT_PYTHON)
    parser.add_argument("--python-timeout", type=int, default=180)
    parser.add_argument("--native-timeout", type=int, default=120)
    parser.add_argument("--processes", type=int, default=3)
    parser.add_argument("--values", type=int, default=2)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--min-time", type=float, default=0.01)
    parser.add_argument("--markdown-out", type=Path)
    parser.add_argument("--json-out", type=Path)
    parser.add_argument(
        "--paired-case",
        action="append",
        default=[],
        help=(
            "Explicit Python-to-native case pairing in the form "
            "'python_case=native_case'. May be passed multiple times."
        ),
    )
    return parser.parse_args()


def _run(cmd: list[str], *, timeout: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=True,
    )


def _load_pyperf_pairs(path: Path) -> dict[str, dict[str, float]]:
    suite = pyperf.BenchmarkSuite.load(str(path))
    pairs: dict[str, dict[str, float]] = {}
    for bench in suite.get_benchmarks():
        name = bench.get_name()
        if not (name.endswith(".python") or name.endswith(".cpp")):
            continue
        base, variant = name.rsplit(".", 1)
        pairs.setdefault(base, {})[variant] = (
            statistics.median(bench.get_values()) * 1_000_000.0
        )
    return pairs


def _load_native_results(path: Path) -> dict[str, float]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        item["name"]: float(item["median_us_per_op"]) for item in payload["results"]
    }


def _format_cpp_result(ratio: float | None) -> str:
    if ratio is None:
        return "-"
    return f"{ratio:.3f}x"


def _build_markdown(
    *,
    suite: str,
    python_match: str,
    native_match: str,
    pyperf_pairs: dict[str, dict[str, float]],
    native_results: dict[str, float],
    paired_cases: list[tuple[str, str]],
) -> str:
    lines = [
        "# Iteration benchmark comparison: "
        f"`{suite}` / `{python_match}` vs native `{native_match}`",
        "",
        "Python-visible harness timings come from the bounded `pyperf` suite.",
        "Standalone native timings come from `ome_zarr_native_bench_core` "
        "and measure pure-native semantic cost.",
        "",
        "| case | C++ harness us/op | python us/op | "
        "C++ harness relative speed vs Python | native us/op | "
        "native C++ relative speed vs Python |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]

    for case_name in sorted(set(pyperf_pairs) | set(native_results)):
        py_us = pyperf_pairs.get(case_name, {}).get("python")
        cpp_us = pyperf_pairs.get(case_name, {}).get("cpp")
        native_us = native_results.get(case_name)

        def _fmt(value: float | None) -> str:
            return "-" if value is None else f"{value:.3f}"

        py_cpp_ratio = (
            None if py_us is None or cpp_us is None or cpp_us == 0.0 else py_us / cpp_us
        )
        py_native_ratio = (
            None
            if py_us is None or native_us is None or native_us == 0.0
            else py_us / native_us
        )

        lines.append(
            "| "
            + case_name
            + " | "
            + _fmt(cpp_us)
            + " | "
            + _fmt(py_us)
            + " | "
            + _format_cpp_result(py_cpp_ratio)
            + " | "
            + _fmt(native_us)
            + " | "
            + _format_cpp_result(py_native_ratio)
            + " |"
        )

    if paired_cases:
        lines.extend(
            [
                "",
                "## Explicit paired comparisons",
                "",
                "| python case | native case | native us/op | python us/op | "
                "native C++ relative speed vs Python |",
                "| --- | --- | ---: | ---: | ---: |",
            ]
        )
        for python_case, native_case in paired_cases:
            py_us = pyperf_pairs.get(python_case, {}).get("python")
            native_us = native_results.get(native_case)
            native_ratio = (
                None
                if py_us is None or native_us is None or native_us == 0.0
                else py_us / native_us
            )
            lines.append(
                "| "
                + python_case
                + " | "
                + native_case
                + " | "
                + ("-" if native_us is None else f"{native_us:.3f}")
                + " | "
                + ("-" if py_us is None else f"{py_us:.3f}")
                + " | "
                + _format_cpp_result(native_ratio)
                + " |"
            )

    return "\n".join(lines) + "\n"


def _parse_paired_cases(items: list[str]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for item in items:
        if "=" not in item:
            raise SystemExit(
                "Invalid --paired-case value "
                f"{item!r}; expected 'python_case=native_case'"
            )
        python_case, native_case = item.split("=", 1)
        python_case = python_case.strip()
        native_case = native_case.strip()
        if not python_case or not native_case:
            raise SystemExit(
                f"Invalid --paired-case value {item!r}; expected non-empty case names"
            )
        pairs.append((python_case, native_case))
    return pairs


def main() -> int:
    args = _parse_args()
    paired_cases = _parse_paired_cases(args.paired_case)
    python_match = args.python_match or args.match
    native_match = args.native_match or args.match
    native_bench = args.build_dir / "ome_zarr_native_bench_core"
    if not args.python.exists():
        raise SystemExit(f"Python executable not found: {args.python}")
    if not native_bench.exists():
        raise SystemExit(
            "Native benchmark executable not found. Build the standalone "
            "targets first: "
            f"{native_bench}"
        )

    with tempfile.TemporaryDirectory(prefix="ome-zarr-c-iter-bench-") as temp_dir:
        temp_root = Path(temp_dir)
        pyperf_json = temp_root / "pyperf.json"
        native_json = temp_root / "native.json"

        verify_cmd = [
            str(args.python),
            "-m",
            "benchmarks.run",
            "--suite",
            args.suite,
            "--match",
            python_match,
            "--verify-only",
        ]
        _run(verify_cmd, timeout=args.python_timeout)

        pyperf_cmd = [
            str(args.python),
            "-m",
            "benchmarks.run",
            "--suite",
            args.suite,
            "--match",
            python_match,
            "--fast",
            "--quiet",
            "--processes",
            str(args.processes),
            "--values",
            str(args.values),
            "--warmups",
            str(args.warmups),
            "--min-time",
            str(args.min_time),
            "--timeout",
            str(args.python_timeout),
            "--output",
            str(pyperf_json),
        ]
        _run(pyperf_cmd, timeout=args.python_timeout)

        native_cmd = [
            str(native_bench),
            "--match",
            native_match,
            "--quick",
            "--json-output",
            str(native_json),
        ]
        native_run = _run(native_cmd, timeout=args.native_timeout)

        pyperf_pairs = _load_pyperf_pairs(pyperf_json)
        native_results = _load_native_results(native_json)

        markdown = _build_markdown(
            suite=args.suite,
            python_match=python_match,
            native_match=native_match,
            pyperf_pairs=pyperf_pairs,
            native_results=native_results,
            paired_cases=paired_cases,
        )
        print(markdown, end="")

        if args.markdown_out is not None:
            args.markdown_out.write_text(markdown, encoding="utf-8")

        if args.json_out is not None:
            args.json_out.write_text(
                json.dumps(
                    {
                        "suite": args.suite,
                        "match": args.match,
                        "python_match": python_match,
                        "native_match": native_match,
                        "pyperf_us_per_op": pyperf_pairs,
                        "native_us_per_op": native_results,
                        "paired_cases": paired_cases,
                        "native_stdout": native_run.stdout,
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
