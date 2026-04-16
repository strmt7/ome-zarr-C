from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from collections.abc import Callable
from functools import lru_cache
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _build_dir() -> Path:
    return Path(os.environ.get("OME_ZARR_NATIVE_BUILD_DIR", ROOT / "build-cpp"))


@lru_cache(maxsize=1)
def ensure_native_bench_core() -> Path:
    cmake = shutil.which("cmake")
    if cmake is None:
        raise RuntimeError("cmake is required for standalone native benchmarks")

    build_dir = _build_dir()
    configure_cmd = [
        cmake,
        "-S",
        str(ROOT),
        "-B",
        str(build_dir),
        "-DCMAKE_BUILD_TYPE=Release",
    ]
    if shutil.which("ninja") is not None:
        configure_cmd[1:1] = ["-G", "Ninja"]

    subprocess.run(configure_cmd, check=True, capture_output=True, text=True)
    subprocess.run(
        [cmake, "--build", str(build_dir), "-j2"],
        check=True,
        capture_output=True,
        text=True,
    )

    bench_path = build_dir / "ome_zarr_native_bench_core"
    if not bench_path.exists():
        bench_path = bench_path.with_suffix(".exe")
    if not bench_path.exists():
        raise RuntimeError("standalone native benchmark binary was not built")
    return bench_path


@lru_cache(maxsize=1)
def run_native_selftest_stdout() -> str:
    ensure_native_bench_core()
    selftest_path = _build_dir() / "ome_zarr_native_selftest"
    if not selftest_path.exists():
        selftest_path = selftest_path.with_suffix(".exe")
    if not selftest_path.exists():
        raise RuntimeError("standalone native self-test binary was not built")

    completed = subprocess.run(
        [str(selftest_path)],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout


def assert_selftest_section(section: str) -> None:
    stdout = run_native_selftest_stdout()
    if f"[PASS] {section}" not in stdout:
        raise AssertionError(f"native self-test section did not pass: {section}")


def native_bench_timer(
    *,
    case_id: str,
    verify: Callable[[], None],
    native_match: str,
) -> Callable[[int], float]:
    def timer(loops: int) -> float:
        verify()
        bench_path = ensure_native_bench_core()
        with tempfile.TemporaryDirectory(prefix="ome-zarr-c-native-bench-") as temp_dir:
            json_path = Path(temp_dir) / "native.json"
            subprocess.run(
                [
                    str(bench_path),
                    "--match",
                    native_match,
                    "--rounds",
                    "1",
                    "--iterations",
                    str(max(1, loops)),
                    "--json-output",
                    str(json_path),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            payload = json.loads(json_path.read_text(encoding="utf-8"))

        results = payload.get("results", [])
        if len(results) != 1:
            raise AssertionError(
                f"Expected exactly one native benchmark result for {case_id}, "
                f"got {len(results)}"
            )
        item = results[0]
        return float(item["median_us_per_op"]) * float(item["iterations"]) / 1_000_000.0

    return timer
