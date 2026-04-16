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
            "Summarize paired Python/converted-path pyperf benchmark results. "
            "Only .native benchmark names are reported as native C++; "
            ".compat names are reported as compatibility/oracle data."
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
    if name.endswith(".compat"):
        return name[: -len(".compat")], "compat"
    if name.endswith(".native"):
        return name[: -len(".native")], "native"
    raise ValueError(f"Unexpected benchmark name: {name}")


def _format_seconds(seconds: float) -> str:
    if seconds < 1e-6:
        return f"{seconds * 1e9:.2f} ns"
    if seconds < 1e-3:
        return f"{seconds * 1e6:.2f} us"
    if seconds < 1:
        return f"{seconds * 1e3:.2f} ms"
    return f"{seconds:.3f} s"


def _variant_label(variant: str) -> str:
    if variant == "native":
        return "native C++"
    return "compat/oracle"


def _status(relative_ratio: float, variant: str) -> str:
    label = _variant_label(variant)
    if relative_ratio >= 1.05:
        return f"{label} above Python"
    if relative_ratio <= 0.95:
        return f"{label} below Python"
    return "roughly equal"


def _geometric_mean(values: list[float]) -> float:
    return math.exp(sum(math.log(value) for value in values) / len(values))


def _format_relative_result(relative_ratio: float) -> str:
    return f"{relative_ratio:.3f}x"


def _render_markdown(paired_rows: list[dict[str, object]]) -> str:
    relative_ratios = [row["relative_ratio"] for row in paired_rows]
    above = sum(1 for row in paired_rows if str(row["status"]).endswith("above Python"))
    below = sum(1 for row in paired_rows if str(row["status"]).endswith("below Python"))
    equal = sum(1 for row in paired_rows if row["status"] == "roughly equal")
    variants = sorted({str(row["variant"]) for row in paired_rows})
    if variants == ["native"]:
        summary_label = "native C++"
    elif variants == ["compat"]:
        summary_label = "compat/oracle"
    else:
        summary_label = "converted path (native C++ plus compat/oracle)"

    grouped: dict[str, list[float]] = defaultdict(list)
    for row in paired_rows:
        grouped[row["group"]].append(row["relative_ratio"])

    lines = [
        "# Benchmark Summary",
        "",
        f"- Paired cases: {len(paired_rows)}",
        (
            f"- Geometric-mean {summary_label} relative speed vs Python: "
            f"{_format_relative_result(_geometric_mean(relative_ratios))}"
        ),
        (
            "- Case classification: "
            f"{above} above Python, {equal} roughly equal, "
            f"{below} below Python"
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
            "| Case | Variant | Python median | converted median | "
            "converted relative speed vs Python | Status |",
            "| --- | --- | ---: | ---: | ---: | --- |",
        ]
    )

    for row in paired_rows:
        lines.append(
            "| "
            f"{row['name']} | "
            f"{_variant_label(str(row['variant']))} | "
            f"{_format_seconds(row['python_median'])} | "
            f"{_format_seconds(row['converted_median'])} | "
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
        elif "compat" in pair:
            converted_variant = "compat"
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
