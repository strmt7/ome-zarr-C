from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

TEXT_FILES = [
    ROOT / "README.md",
    ROOT / "AGENTS.md",
    ROOT / ".github" / "copilot-instructions.md",
    ROOT / "benchmarks" / "__init__.py",
    ROOT / "benchmarks" / "cases.py",
    ROOT / "benchmarks" / "report.py",
    ROOT / "benchmarks" / "run.py",
    ROOT / "scripts" / "compare_iteration_benchmarks.py",
    *sorted((ROOT / "docs").rglob("*.md")),
    *sorted((ROOT / ".github" / "instructions").glob("*.md")),
    *sorted((ROOT / "benchmarks" / "results").rglob("*.md")),
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
    r"\b(?:ome_zarr_c|cpp/bindings/cli_bindings\.cpp|"
    r"cpp/bindings/utils_bindings\.cpp|"
    r"cpp/bindings/csv_bindings\.cpp|"
    r"cpp/bindings/data_bindings\.cpp|"
    r"cpp/bindings/reader_bindings\.cpp|"
    r"tests/test_scaler_runtime_equivalence\.py|"
    r"tests/test_utils_download_runtime\.py|"
    r"tests/test_utils_info_equivalence\.py)\b"
)
STALE_DISTRO_NATIVE_DEPS_PATTERN = re.compile(
    r"\b(?:libblosc-dev|libzstd-dev)\b",
    re.IGNORECASE,
)
MISLEADING_BENCHMARK_LABEL_PATTERNS = (
    re.compile(r"\b(?:C\+\+|cpp)\s+harness\b", re.IGNORECASE),
    re.compile(r"\bC\+\+\s+median\b"),
    re.compile(r"\bGeometric-mean\s+C\+\+\s+relative speed vs Python\b"),
    re.compile(r"\bCase classification:.*\bC\+\+\s+(?:faster|slower)\b"),
    re.compile(r"\bcompat/oracle\b", re.IGNORECASE),
)
STALE_NATIVE_BACKED_PATTERN = re.compile(r"\bnative[- ]backed\b", re.IGNORECASE)


def _read_define_int(text: str, macro_name: str) -> int:
    match = re.search(
        rf"^#define\s+{re.escape(macro_name)}\s+(\d+)\b", text, re.MULTILINE
    )
    if match is None:
        raise ValueError(f"Missing macro {macro_name}")
    return int(match.group(1))


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
    native_dependency_manifest = ROOT / "docs/reference/native-dependency-manifest.json"
    native_toolchain_installer = ROOT / "scripts/install_latest_native_toolchain.sh"
    setup_py = ROOT / "setup.py"
    pyproject_toml = ROOT / "pyproject.toml"
    removed_python_package = ROOT / "ome_zarr_c"
    benchmark_python_dir = ROOT / "benchmarks/python"
    benchmark_native_dir = ROOT / "benchmarks/native"
    removed_binding_files = (
        ROOT / "cpp/bindings/common.hpp",
        ROOT / "cpp/bindings/format_bindings.cpp",
        ROOT / "cpp/bindings/module.cpp",
    )

    for required in (
        standalone_doc,
        native_dev_doc,
        native_bench,
        native_bench_core,
        native_cli,
        native_selftest,
        cmake_file,
        iteration_compare_script,
        native_dependency_manifest,
        native_toolchain_installer,
    ):
        if not required.exists():
            issues.append(f"Missing required file: {required.relative_to(ROOT)}")

    for removed in removed_binding_files:
        if removed.exists():
            issues.append(
                f"Removed binding-layer file still exists: {removed.relative_to(ROOT)}"
            )
    if removed_python_package.exists():
        issues.append("Removed Python compatibility package directory still exists.")
    if not benchmark_python_dir.exists():
        issues.append("benchmarks/python is missing.")
    if not benchmark_native_dir.exists():
        issues.append("benchmarks/native is missing.")

    agents_text = read_text(ROOT / "AGENTS.md")
    docs_index_text = read_text(ROOT / "docs/index.md")
    readme_text = read_text(ROOT / "README.md")
    setup_text = read_text(setup_py)
    pyproject_text = read_text(pyproject_toml)
    cmake_text = read_text(cmake_file)
    native_dev_text = read_text(native_dev_doc)
    manifest = json.loads(read_text(native_dependency_manifest))

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
    if "docs/reference/native-dependency-manifest.json" not in docs_index_text:
        issues.append("docs/index.md is missing the native dependency manifest.")
    if "standalone C++ product" not in readme_text:
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
    if "docs/reference/native-dependency-manifest.json" not in readme_text:
        issues.append("README.md is missing the native dependency manifest reference.")
    if "install_latest_native_toolchain.sh" not in readme_text:
        issues.append("README.md is missing the native toolchain installer reference.")
    if "benchmarks/python/" not in readme_text:
        issues.append("README.md is missing the benchmarks/python layout.")
    if "benchmarks/native/" not in readme_text:
        issues.append("README.md is missing the benchmarks/native layout.")
    if "scripts/compare_iteration_benchmarks.py" not in readme_text:
        issues.append("README.md is missing the iteration benchmark helper command.")
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
    if "cpp/bindings/data_bindings.cpp" in setup_text:
        issues.append("setup.py still builds the removed transitional data bindings.")
    if any(
        token in setup_text
        for token in (
            "Pybind11Extension",
            "build_ext",
            "pybind11",
            "ome_zarr_c._core",
            "cpp/bindings/",
            "find_packages",
        )
    ):
        issues.append("setup.py still references removed pybind binding build inputs.")
    if "packages=[]" not in setup_text.replace(" ", ""):
        issues.append("setup.py must remain metadata-only with packages=[].")
    if "pybind11" in pyproject_text:
        issues.append("pyproject.toml still depends on removed pybind tooling.")
    if "install_latest_native_toolchain.sh" not in native_dev_text:
        issues.append(
            "native-build-and-selftest.md is missing the native toolchain installer."
        )

    expected_cmake_minimum = (
        "cmake_minimum_required(VERSION "
        f"{manifest['native_build']['cmake_minimum_version']})"
    )
    if expected_cmake_minimum not in cmake_text:
        issues.append(
            "CMakeLists.txt does not use the manifest-pinned minimum CMake version."
        )
    if (
        f"set(CMAKE_CXX_STANDARD {manifest['native_build']['cxx_standard']})"
        not in cmake_text
    ):
        issues.append("CMakeLists.txt does not use the manifest-pinned C++ standard.")

    tinyxml2_text = read_text(ROOT / "third_party/tinyxml2/tinyxml2.h")
    cpp_httplib_text = read_text(ROOT / "third_party/cpp-httplib/httplib.h")
    nlohmann_text = read_text(ROOT / "third_party/nlohmann/json.hpp")

    manifest_tinyxml2 = tuple(
        int(part)
        for part in manifest["vendored_headers"]["tinyxml2"]["version"].split(".")
    )
    repo_tinyxml2 = (
        _read_define_int(tinyxml2_text, "TINYXML2_MAJOR_VERSION"),
        _read_define_int(tinyxml2_text, "TINYXML2_MINOR_VERSION"),
        _read_define_int(tinyxml2_text, "TINYXML2_PATCH_VERSION"),
    )
    if repo_tinyxml2 != manifest_tinyxml2:
        issues.append(
            "third_party/tinyxml2 version does not match "
            "the native dependency manifest."
        )

    httplib_match = re.search(
        r'^#define\s+CPPHTTPLIB_VERSION\s+"([^"]+)"',
        cpp_httplib_text,
        re.MULTILINE,
    )
    if (
        httplib_match is None
        or httplib_match.group(1)
        != manifest["vendored_headers"]["cpp-httplib"]["version"]
    ):
        issues.append(
            "third_party/cpp-httplib version does not match "
            "the native dependency manifest."
        )

    repo_nlohmann = (
        _read_define_int(nlohmann_text, "NLOHMANN_JSON_VERSION_MAJOR"),
        _read_define_int(nlohmann_text, "NLOHMANN_JSON_VERSION_MINOR"),
        _read_define_int(nlohmann_text, "NLOHMANN_JSON_VERSION_PATCH"),
    )
    manifest_nlohmann = tuple(
        int(part)
        for part in manifest["vendored_headers"]["nlohmann/json"]["version"].split(".")
    )
    if repo_nlohmann != manifest_nlohmann:
        issues.append(
            "third_party/nlohmann/json version does not match "
            "the native dependency manifest."
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
        if "cpp/bindings/format_bindings.cpp" in text:
            issues.append(
                "Removed format binding file is still referenced in "
                f"{path.relative_to(ROOT)}"
            )
        if STALE_DISTRO_NATIVE_DEPS_PATTERN.search(text) is not None:
            issues.append(
                "Stale distro-native dependency reference remains in "
                f"{path.relative_to(ROOT)}"
            )
        if STALE_NATIVE_BACKED_PATTERN.search(text) is not None:
            issues.append(
                "Stale compiled-extension-backed terminology remains in "
                f"{path.relative_to(ROOT)}"
            )
        for pattern in MISLEADING_BENCHMARK_LABEL_PATTERNS:
            if pattern.search(text) is not None:
                issues.append(
                    f"Misleading benchmark label remains in {path.relative_to(ROOT)}"
                )
                break
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
