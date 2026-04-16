from __future__ import annotations

import argparse
import math
import sys
from collections import defaultdict
from pathlib import Path

import pyperf


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize paired python/cpp pyperf benchmark results."
    )
    parser.add_argument("input", help="Path to the pyperf JSON results file.")
    parser.add_argument(
        "--markdown-out",
        help="Optional path to write the markdown report.",
    )
    return parser.parse_args(argv)


def _split_name(name: str) -> tuple[str, str]:
    if name.endswith(".python"):
        return name[: -len(".python")], "python"
    if name.endswith(".cpp"):
        return name[: -len(".cpp")], "cpp"
    raise ValueError(f"Unexpected benchmark name: {name}")


def _format_seconds(seconds: float) -> str:
    if seconds < 1e-6:
        return f"{seconds * 1e9:.2f} ns"
    if seconds < 1e-3:
        return f"{seconds * 1e6:.2f} us"
    if seconds < 1:
        return f"{seconds * 1e3:.2f} ms"
    return f"{seconds:.3f} s"


def _status(cpp_ratio: float) -> str:
    if cpp_ratio >= 1.05:
        return "C++ faster"
    if cpp_ratio <= 0.95:
        return "C++ slower"
    return "roughly equal"


def _geometric_mean(values: list[float]) -> float:
    return math.exp(sum(math.log(value) for value in values) / len(values))


def _format_cpp_result(cpp_ratio: float) -> str:
    return f"{cpp_ratio:.3f}x"


def _render_markdown(paired_rows: list[dict[str, object]]) -> str:
    cpp_ratios = [row["cpp_ratio"] for row in paired_rows]
    faster = sum(1 for row in paired_rows if row["status"] == "C++ faster")
    slower = sum(1 for row in paired_rows if row["status"] == "C++ slower")
    equal = sum(1 for row in paired_rows if row["status"] == "roughly equal")

    grouped: dict[str, list[float]] = defaultdict(list)
    for row in paired_rows:
        grouped[row["group"]].append(row["cpp_ratio"])

    lines = [
        "# Benchmark Summary",
        "",
        f"- Paired cases: {len(paired_rows)}",
        (
            "- Geometric-mean C++ relative speed vs Python: "
            f"{_format_cpp_result(_geometric_mean(cpp_ratios))}"
        ),
        (
            "- Case classification: "
            f"{faster} C++ faster, {equal} roughly equal, {slower} C++ slower"
        ),
        "",
        "## Group Geometric Means",
        "",
    ]

    for group in sorted(grouped):
        lines.append(
            f"- {group}: {_format_cpp_result(_geometric_mean(grouped[group]))}"
        )

    lines.extend(
        [
            "",
            "## Cases",
            "",
            "| Case | C++ median | Python median | "
            "C++ relative speed vs Python | Status |",
            "| --- | ---: | ---: | ---: | --- |",
        ]
    )

    for row in paired_rows:
        lines.append(
            "| "
            f"{row['name']} | "
            f"{_format_seconds(row['cpp_median'])} | "
            f"{_format_seconds(row['python_median'])} | "
            f"{_format_cpp_result(row['cpp_ratio'])} | "
            f"{row['status']} |"
        )

    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    suite = pyperf.BenchmarkSuite.load(args.input)

    paired: dict[str, dict[str, pyperf.Benchmark]] = defaultdict(dict)
    for benchmark in suite.get_benchmarks():
        base_name, implementation = _split_name(benchmark.get_name())
        paired[base_name][implementation] = benchmark

    rows = []
    for base_name in sorted(paired):
        pair = paired[base_name]
        if "python" not in pair or "cpp" not in pair:
            raise SystemExit(f"Incomplete benchmark pair for {base_name}")

        python_median = pair["python"].median()
        cpp_median = pair["cpp"].median()
        cpp_ratio = python_median / cpp_median
        group, _, short_name = base_name.partition(".")
        rows.append(
            {
                "group": group,
                "name": short_name,
                "python_median": python_median,
                "cpp_median": cpp_median,
                "cpp_ratio": cpp_ratio,
                "status": _status(cpp_ratio),
            }
        )

    markdown = _render_markdown(rows)
    print(markdown, end="")

    if args.markdown_out:
        output_path = Path(args.markdown_out)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(markdown)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
