from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

TEXT_FILES = [
    ROOT / "README.md",
    ROOT / "AGENTS.md",
    *sorted((ROOT / "docs").rglob("*.md")),
    *sorted((ROOT / ".github" / "instructions").glob("*.md")),
]

RUFF_CPP_TARGET_PATTERN = re.compile(
    r"\bruff\b.*\b(?:cpp/|\.cpp\b|\.hpp\b|\.cc\b|\.cxx\b|\.h\b)",
    re.IGNORECASE,
)
STALE_NATIVE_CLI_PATTERN = re.compile(
    r"ome_zarr_native_cli\s+(?:cli\s+|data\s+create-plan|format\s+|io\s+subpath|utils\s+(?:view-plan|finder-plan)|writer\s+image-plan)",
    re.IGNORECASE,
)
REMOVED_RUNTIME_PATTERN = re.compile(
    r"\b(?:ome_zarr_c\.cli|cpp/bindings/cli_bindings\.cpp|"
    r"tests/test_scaler_runtime_equivalence\.py|"
    r"tests/test_utils_download_runtime\.py|"
    r"tests/test_utils_info_equivalence\.py)\b"
)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def main() -> int:
    issues: list[str] = []

    standalone_doc = ROOT / "docs/reference/standalone-cpp-target.md"
    native_dev_doc = ROOT / "docs/reference/native-build-and-selftest.md"
    native_bench = ROOT / "cpp/tools/native_bench_format.cpp"
    native_bench_core = ROOT / "cpp/tools/native_bench_core.cpp"
    native_cli = ROOT / "cpp/tools/native_cli.cpp"
    native_selftest = ROOT / "cpp/tools/native_selftest.cpp"
    cmake_file = ROOT / "CMakeLists.txt"
    iteration_compare_script = ROOT / "scripts/compare_iteration_benchmarks.py"
    setup_py = ROOT / "setup.py"
    data_bindings = ROOT / "cpp/bindings/data_bindings.cpp"

    for required in (
        standalone_doc,
        native_dev_doc,
        native_bench,
        native_bench_core,
        native_cli,
        native_selftest,
        cmake_file,
        iteration_compare_script,
    ):
        if not required.exists():
            issues.append(f"Missing required file: {required.relative_to(ROOT)}")

    agents_text = read_text(ROOT / "AGENTS.md")
    docs_index_text = read_text(ROOT / "docs/index.md")
    readme_text = read_text(ROOT / "README.md")
    setup_text = read_text(setup_py)
    data_bindings_text = read_text(data_bindings)

    if "docs/reference/standalone-cpp-target.md" not in agents_text:
        issues.append(
            "AGENTS.md is missing the standalone C++ target doc in fast-load guidance."
        )
    if "docs/reference/native-build-and-selftest.md" not in agents_text:
        issues.append(
            "AGENTS.md is missing the native build/self-test doc in fast-load guidance."
        )
    if "docs/reference/standalone-cpp-target.md" not in docs_index_text:
        issues.append("docs/index.md is missing the standalone C++ target doc.")
    if "docs/reference/native-build-and-selftest.md" not in docs_index_text:
        issues.append("docs/index.md is missing the native build/self-test doc.")
    if "transitional parity workspace" not in readme_text:
        issues.append(
            "README.md no longer states the current transitional workspace status."
        )
    if "standalone C++ only" not in readme_text:
        issues.append("README.md no longer states the standalone C++ target.")
    if "ome_zarr_native_selftest" not in readme_text:
        issues.append("README.md is missing the native self-test command.")
    if "ome_zarr_native_cli" not in readme_text:
        issues.append("README.md is missing the native CLI command.")
    if "ome_zarr_native_cli info " not in readme_text:
        issues.append("README.md is missing the native CLI info example.")
    if "ome_zarr_native_cli info /tmp/demo/image.zarr --stats" not in readme_text:
        issues.append("README.md is missing the native CLI info --stats example.")
    if "ome_zarr_native_cli create " not in readme_text:
        issues.append("README.md is missing the native CLI create example.")
    if "ome_zarr_native_cli finder " not in readme_text:
        issues.append("README.md is missing the native CLI finder example.")
    if "ome_zarr_native_cli download " not in readme_text:
        issues.append("README.md is missing the native CLI download example.")
    if "ome_zarr_native_cli view " not in readme_text:
        issues.append("README.md is missing the native CLI view example.")
    if "ome_zarr_native_cli scale " not in readme_text:
        issues.append("README.md is missing the native CLI scale example.")
    if "ome_zarr_native_cli csv_to_labels " not in readme_text:
        issues.append("README.md is missing the native CLI csv_to_labels example.")
    if "ome_zarr_native_bench_core" not in readme_text:
        issues.append("README.md is missing the native core benchmark command.")
    if "scripts/compare_iteration_benchmarks.py" not in readme_text:
        issues.append("README.md is missing the iteration benchmark helper command.")
    if "--paired-case utils.download=local.download" not in readme_text:
        issues.append(
            "README.md is missing the native download iteration benchmark example."
        )
    if "--paired-case utils.view=local.view_prepare" not in readme_text:
        issues.append(
            "README.md is missing the native view iteration benchmark example."
        )
    if (
        "--paired-case runtime.utils.info_v3_image_with_stats=local.info_stats"
        not in readme_text
    ):
        issues.append(
            "README.md is missing the native info --stats iteration benchmark example."
        )
    if (
        "--paired-case runtime.data.create_zarr_coins_v05=local.create_coins"
        not in readme_text
    ):
        issues.append(
            "README.md is missing the native create iteration benchmark example."
        )
    if "--paired-case csv.csv_to_zarr=local.csv_to_labels" not in readme_text:
        issues.append(
            "README.md is missing the native csv_to_labels iteration benchmark example."
        )
    if "--paired-case cli.scale_wrapper=local.scale_nearest" not in readme_text:
        issues.append(
            "README.md is missing the native scale iteration benchmark example."
        )
    if "ome_zarr_native_bench_format" in readme_text and not native_bench.exists():
        issues.append(
            "README.md references ome_zarr_native_bench_format, "
            "but the source file is missing."
        )
    if "ome_zarr_native_bench_core" in readme_text and not native_bench_core.exists():
        issues.append(
            "README.md references ome_zarr_native_bench_core, "
            "but the source file is missing."
        )
    if "ome_zarr_native_selftest" in readme_text and not native_selftest.exists():
        issues.append(
            "README.md references ome_zarr_native_selftest, "
            "but the source file is missing."
        )
    if "ome_zarr_native_cli" in readme_text and not native_cli.exists():
        issues.append(
            "README.md references ome_zarr_native_cli, but the source file is missing."
        )
    if "cpp/native/create_runtime.cpp" in setup_text:
        issues.append(
            "setup.py still builds standalone-native create runtime into the "
            "Python extension."
        )
    if "cpp/native/create_assets.cpp" in setup_text:
        issues.append(
            "setup.py still builds standalone-native create assets into the "
            "Python extension."
        )
    if "cpp/native/python_random.cpp" in setup_text:
        issues.append(
            "setup.py still builds standalone-native Python-random emulation "
            "into the Python extension."
        )
    if '#include "../native/create_runtime.hpp"' in data_bindings_text:
        issues.append(
            "cpp/bindings/data_bindings.cpp still depends on standalone-native "
            "create runtime."
        )

    for path in TEXT_FILES:
        text = read_text(path)
        if STALE_NATIVE_CLI_PATTERN.search(text) is not None:
            issues.append(
                "Stale plan-only native CLI reference remains in "
                f"{path.relative_to(ROOT)}"
            )
        if REMOVED_RUNTIME_PATTERN.search(text) is not None:
            issues.append(
                "Removed transitional runtime surface is still referenced in "
                f"{path.relative_to(ROOT)}"
            )
        for line in text.splitlines():
            if "ruff" not in line.lower():
                continue
            stripped = line.strip()
            if stripped.startswith("- Do not point Ruff at"):
                continue
            if stripped.startswith("- Ruff is for Python files only"):
                continue
            if (
                "Never point Ruff at" in stripped
                or "Never run Ruff against" in stripped
            ):
                continue
            if RUFF_CPP_TARGET_PATTERN.search(stripped) is not None:
                issues.append(
                    f"Ruff/C++ conflict in {path.relative_to(ROOT)}: {stripped!r}"
                )

    if issues:
        for issue in issues:
            print(f"ERROR: {issue}")
        return 1

    print("Repo consistency checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
