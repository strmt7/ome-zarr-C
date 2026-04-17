from __future__ import annotations

import argparse
import math
import sys
from collections import defaultdict
from pathlib import Path

import pyperf


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Summarize paired frozen-upstream Python vs standalone native C++ "
            "pyperf benchmark results."
        )
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
    if name.endswith(".native"):
        return name[: -len(".native")], "native"
    raise ValueError(f"Unexpected benchmark name: {name}")


def _format_seconds(seconds: float) -> str:
    sign = "-" if seconds < 0 else ""
    magnitude = abs(seconds)
    if magnitude < 1e-6:
        return f"{sign}{magnitude * 1e9:.2f} ns"
    if magnitude < 1e-3:
        return f"{sign}{magnitude * 1e6:.2f} us"
    if magnitude < 1:
        return f"{sign}{magnitude * 1e3:.2f} ms"
    return f"{sign}{magnitude:.3f} s"


def _variant_label(variant: str) -> str:
    if variant != "native":
        raise ValueError(f"Unexpected benchmark variant: {variant}")
    return "native C++"


def _status(relative_ratio: float, variant: str) -> str:
    label = _variant_label(variant)
    if relative_ratio >= 1.05:
        return f"{label} faster than Python"
    if relative_ratio <= 0.95:
        return f"{label} slower than Python"
    return "roughly equal"


def _geometric_mean(values: list[float]) -> float:
    return math.exp(sum(math.log(value) for value in values) / len(values))


def _format_relative_result(relative_ratio: float) -> str:
    return f"{relative_ratio:.3f}x"


def _format_percent(value: float) -> str:
    return f"{value:.1f}%"


def _render_markdown(paired_rows: list[dict[str, object]]) -> str:
    relative_ratios = [row["relative_ratio"] for row in paired_rows]
    faster = sum(
        1 for row in paired_rows if str(row["status"]).endswith("faster than Python")
    )
    slower = sum(
        1 for row in paired_rows if str(row["status"]).endswith("slower than Python")
    )
    equal = sum(1 for row in paired_rows if row["status"] == "roughly equal")
    variants = sorted({str(row["variant"]) for row in paired_rows})
    if variants == ["native"]:
        summary_label = "native C++"
    else:
        raise ValueError(f"Unexpected benchmark variants: {variants}")

    grouped: dict[str, list[float]] = defaultdict(list)
    for row in paired_rows:
        grouped[row["group"]].append(row["relative_ratio"])

    lines = [
        "# Benchmark Summary",
        "",
        f"- Paired cases: {len(paired_rows)}",
        (
            f"- Geometric-mean {summary_label} speedup over Python "
            f"(`python_time / native_cpp_time`): "
            f"{_format_relative_result(_geometric_mean(relative_ratios))}"
        ),
        (
            "- Case classification: "
            f"{faster} {summary_label} faster, {equal} roughly equal, "
            f"{slower} {summary_label} slower"
        ),
        "",
        "## Group Geometric Means",
        "",
    ]

    for group in sorted(grouped):
        lines.append(
            f"- {group}: {_format_relative_result(_geometric_mean(grouped[group]))}"
        )

    lines.extend(
        [
            "",
            "## Cases",
            "",
            "| Case | Variant | Python time | native C++ time | time saved per op | "
            "native C++ time reduction | native C++ speedup over Python "
            "(`python_time / native_cpp_time`) | Status |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )

    for row in paired_rows:
        python_median = row["python_median"]
        converted_median = row["converted_median"]
        time_saved = python_median - converted_median
        time_reduction = 1.0 - (converted_median / python_median)
        lines.append(
            "| "
            f"{row['name']} | "
            f"{_variant_label(str(row['variant']))} | "
            f"{_format_seconds(python_median)} | "
            f"{_format_seconds(converted_median)} | "
            f"{_format_seconds(time_saved)} | "
            f"{_format_percent(time_reduction * 100.0)} | "
            f"{_format_relative_result(row['relative_ratio'])} | "
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
        if "python" not in pair:
            raise SystemExit(f"Incomplete benchmark pair for {base_name}")
        if "native" in pair:
            converted_variant = "native"
        else:
            raise SystemExit(f"Incomplete benchmark pair for {base_name}")

        python_median = pair["python"].median()
        converted_median = pair[converted_variant].median()
        relative_ratio = python_median / converted_median
        group, _, short_name = base_name.partition(".")
        rows.append(
            {
                "group": group,
                "name": short_name,
                "variant": converted_variant,
                "python_median": python_median,
                "converted_median": converted_median,
                "relative_ratio": relative_ratio,
                "status": _status(relative_ratio, converted_variant),
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
