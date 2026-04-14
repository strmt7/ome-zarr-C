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


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def main() -> int:
    issues: list[str] = []

    standalone_doc = ROOT / "docs/reference/standalone-cpp-target.md"
    native_bench = ROOT / "cpp/tools/native_bench_format.cpp"
    cmake_file = ROOT / "CMakeLists.txt"

    for required in (standalone_doc, native_bench, cmake_file):
        if not required.exists():
            issues.append(f"Missing required file: {required.relative_to(ROOT)}")

    agents_text = read_text(ROOT / "AGENTS.md")
    docs_index_text = read_text(ROOT / "docs/index.md")
    readme_text = read_text(ROOT / "README.md")

    if "docs/reference/standalone-cpp-target.md" not in agents_text:
        issues.append(
            "AGENTS.md is missing the standalone C++ target doc in fast-load guidance."
        )
    if "docs/reference/standalone-cpp-target.md" not in docs_index_text:
        issues.append("docs/index.md is missing the standalone C++ target doc.")
    if "transitional parity workspace" not in readme_text:
        issues.append(
            "README.md no longer states the current transitional workspace status."
        )
    if "standalone C++ only" not in readme_text:
        issues.append("README.md no longer states the standalone C++ target.")
    if "ome_zarr_native_bench_format" in readme_text and not native_bench.exists():
        issues.append(
            "README.md references ome_zarr_native_bench_format, "
            "but the source file is missing."
        )

    for path in TEXT_FILES:
        text = read_text(path)
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
