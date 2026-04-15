# Benchmark Suite

This repository uses a dedicated `pyperf`-based benchmark suite for performance
work on parity-proven native-backed surfaces. The benchmark tree is split into
three suites:

- `core`: converted kernels plus deterministic tempdir runtime flows
- `public-api`: benchmark coverage for the documented upstream public API
- `realdata`: public OME-Zarr fixtures used for end-to-end reader/info timing

The repository also carries a native-only CMake benchmark target for pure
`cpp/native/` measurements. Use the two layers together:

- `pyperf` suites for Python-visible parity and end-to-end user-facing timing
- native CMake benchmarks for pure semantic cost without Python boundary
  overhead

## Why `pyperf`

- The official `pyperf` documentation describes a benchmark architecture with
  calibration, warmups, multiple worker processes, and structured result files.
- The `pyperf` benchmarking guide also recommends tuning the system, analyzing
  stability, and adjusting runs, values, and loops rather than trusting a
  single wall clock reading.
- The `pyperf` system guide documents CPU affinity and other system-noise
  controls that can improve repeatability on Linux.
- The `pyperf` analysis guide documents result comparison and post-run
  inspection instead of relying on raw means or a single fastest sample.

References:

- <https://pyperf.readthedocs.io/en/latest/run_benchmark.html>
- <https://pyperf.readthedocs.io/en/latest/system.html>
- <https://pyperf.readthedocs.io/en/latest/analyze.html>

## Why Not `asv` First

`asv` was considered and is still a good future option for historical tracking.
Its official documentation positions it as a suite for benchmarking a single
project over its lifetime, with benchmark data stored alongside the suite and
growing over time. That is useful once this repository has a broader converted
surface and a stable long-running performance baseline, but it is heavier than
needed for the current first-pass question: "is the verified native-backed code
already faster than the frozen upstream implementation on the same machine?"

Reference:

- <https://asv.readthedocs.io/en/stable/using.html>

## Scope Rules

The benchmark suite intentionally measures only surfaces that satisfy all of the
following:

- parity is already proven against the frozen `v0.15.0` upstream snapshot
- the benchmark input is reproducible, either fully in-memory or on a
  deterministic local tempdir fixture
- the workload does not depend on network access or remote object stores
- the workload does not depend on network access, browser launching, or live
  filesystem traversal noise

Included in the current suites:

- in-memory helper kernels such as conversions, Dask resizing, and pyramid
  construction
- deterministic local-store runtime flows such as `parse_url`, `info`,
  `write_image`, `create_zarr`, and CLI create/info/download
- real public OME-Zarr fixtures with stable provenance and non-restrictive
  licenses

Excluded from direct coverage claims:

- network-backed stores, browser launches, and other non-deterministic flows
- abstract base-class contracts such as `ome_zarr.format.Format.*` methods and
  `ome_zarr.reader.Spec.matches`

In simple terms, those abstract members are excluded because there is no direct
implementation to time. The suite benchmarks the concrete methods that users
actually execute instead.

## Fast Iteration Policy

Use short, focused runs while iterating on a specific hotspot:

- restrict the run with `--suite`, `--group`, and/or `--match`
- use `--fast` for exploratory measurement
- wrap exploratory local runs in an explicit timeout such as `timeout 180s`
- only rerun a broad suite when you need a final repo artifact or a broader
  claim than the touched surface justifies

This keeps routine performance debugging from turning into repeated multi-minute
waits while still preserving a real `pyperf` measurement path.

## Benchmark Layout

- `benchmarks/catalog.py`: suite registry for `core`, `public-api`, and `realdata`
- `benchmarks/run.py`: executes paired upstream-vs-native `pyperf` timings
- `benchmarks/cases.py`: benchmark registry, parity guards, and deterministic
  inputs for the `core` suite
- `benchmarks/public_api_cases.py`: public-API benchmark cases
- `benchmarks/realdata_cases.py`: public-fixture end-to-end cases
- `benchmarks/public_fixtures.py`: public fixture download, cache, and
  provenance helpers
- `benchmarks/runtime_support.py`: deterministic tempdir fixtures and shared
  runtime parity helpers reused by the benchmark registry
- `benchmarks/report.py`: turns a `pyperf` JSON file into a markdown summary
- `scripts/compare_iteration_benchmarks.py`: bounded per-iteration comparison
  helper for one touched surface using both the Python-visible suite and the
  standalone native benchmark
- `scripts/check_public_api_benchmark_coverage.py`: enforces benchmark
  coverage for documented upstream public callables
- `CMakeLists.txt` plus `cpp/tools/native_bench_format.cpp` and
  `cpp/tools/native_bench_core.cpp`: native-only build and benchmark entrypoints
  for direct `cpp/native` timing

Each paired case benchmarks the Python upstream function and the converted
native-backed function on the same exact benchmark input. Before timing begins,
the case verifies parity on that benchmark input and aborts if parity fails.

The benchmark runner disables package logging and Python warnings by default via
`OME_ZARR_BENCH_DISABLE_LOGGING=1` and
`OME_ZARR_BENCH_DISABLE_WARNINGS=1`. This keeps benchmark results focused on
code execution instead of warning/info I/O. Override with
`OME_ZARR_BENCH_DISABLE_LOGGING=0` and/or `OME_ZARR_BENCH_DISABLE_WARNINGS=0`
if you intentionally want those side effects included in a measurement.
Timed runs also suppress raw stdout/stderr by default via
`OME_ZARR_BENCH_SUPPRESS_STDIO=1` so command output does not pollute the timing
process. Set `OME_ZARR_BENCH_SUPPRESS_STDIO=0` only when output cost is the
measurement target.

## Fixture Provenance

The `realdata` suite downloads public fixtures on demand into the repo-local
cache directory `.benchmarks-fixtures/`. Override that location with
`OME_ZARR_BENCH_FIXTURE_ROOT=/abs/path/to/cache`.

Default fixture set:

- `examples_image`: small valid image fixture from
  `BioImageTools/ome-zarr-examples` at commit
  `8c10c88fbb77c3dcc206d9b234431f243beee576` (`BSD-3-Clause`)
- `examples_plate`: small valid plate fixture from the same repository and
  commit (`BSD-3-Clause`)
- `bia_tonsil3`: public BioImage Archive / IDR OME-NGFF image,
  `108.8 MiB` (`CC BY 4.0`)

Optional large fixture:

- `bia_156_42`: public BioImage Archive / IDR OME-NGFF image,
  `455.3 MiB` (`CC BY 4.0`)

Enable the large fixture only when explicitly requested:

```bash
OME_ZARR_BENCH_INCLUDE_LARGE=1 .venv/bin/python -m benchmarks.run \
  --suite realdata \
  --verify-only
```

See `docs/reference/public-benchmark-fixtures.md` for the source URLs and
license details.

## Running The Suite

Install the benchmark dependency:

```bash
.venv/bin/python -m pip install -e '.[dev,benchmark]' --no-build-isolation
```

Build profile notes:

- the repository now appends an explicit portable release profile by default
  when building the extension:
  - Unix-like hosts: `-O3` and hidden symbol visibility
  - Windows hosts: `/O2`
- unsafe math flags are intentionally excluded because parity comes first
- optional tuning knobs:
  - `OME_ZARR_C_ENABLE_LTO=1` enables link-time optimization
  - `OME_ZARR_C_MARCH_NATIVE=1` enables host-specific CPU tuning on Unix-like
    hosts

Example host-tuned rebuild for local benchmark experiments:

```bash
OME_ZARR_C_ENABLE_LTO=1 OME_ZARR_C_MARCH_NATIVE=1 \
  .venv/bin/python -m pip install -e '.[dev,benchmark]' --no-build-isolation
```

When you change either build-profile environment variable, rebuild the editable
install before benchmarking so the timed extension binary actually reflects the
requested profile.

For standalone native benchmarking without Python boundary overhead:

```bash
cmake -S . -B build-cpp -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-cpp -j2
./build-cpp/ome_zarr_native_selftest
./build-cpp/ome_zarr_native_bench_format --quick
./build-cpp/ome_zarr_native_bench_core --quick
```

Optional host-tuned native build:

```bash
cmake -S . -B build-cpp -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DOME_ZARR_C_NATIVE_ENABLE_LTO=ON \
  -DOME_ZARR_C_NATIVE_MARCH_NATIVE=ON
cmake --build build-cpp -j2
./build-cpp/ome_zarr_native_selftest
./build-cpp/ome_zarr_native_bench_format --quick
./build-cpp/ome_zarr_native_bench_core --quick
```

The native benchmark layer is intentionally bounded. Use it during local
optimization loops when the broader Python-visible suite would be too slow for
the question at hand, and use `--match` to focus on a hotspot:

```bash
timeout 120s ./build-cpp/ome_zarr_native_bench_core --match format --quick
timeout 120s ./build-cpp/ome_zarr_native_bench_core --match writer --quick
```

For bounded iteration comparisons on the touched hotspot, use the helper
script. It verifies parity on the selected Python-visible cases, runs a short
`pyperf` pass, runs the matching native benchmark slice, and prints one table
with both layers:

```bash
timeout 180s .venv/bin/python scripts/compare_iteration_benchmarks.py --match format
```

The current `format` pairing is the strongest apples-to-apples iteration lane:

- Python-visible side: `public-api` `format.*` cases through the parity harness
- native side: `ome_zarr_native_bench_core --match format`

The two layers are intentionally reported together but not conflated:

- the `pyperf` row measures user-visible Python/runtime overhead on the parity
  harness
- the native row measures pure `cpp/native` semantic cost

For the `utils` hotspot, use:

```bash
timeout 180s .venv/bin/python scripts/compare_iteration_benchmarks.py \
  --match utils \
  --native-match utils
```

List the available cases across all suites:

```bash
.venv/bin/python -m benchmarks.run --list
```

List the available suites and only the public-API cases:

```bash
.venv/bin/python -m benchmarks.run --suite public-api --list
```

List only the compute-heavy kernel cases:

```bash
.venv/bin/python -m benchmarks.run --suite core --group meso --list
```

Verify benchmark parity inputs without timing:

```bash
.venv/bin/python -m benchmarks.run --suite core --verify-only
.venv/bin/python -m benchmarks.run --suite public-api --verify-only
.venv/bin/python -m benchmarks.run --suite realdata --verify-only
```

Include logging side effects in a benchmark run only when you explicitly want
to study them:

```bash
OME_ZARR_BENCH_DISABLE_LOGGING=0 \
  .venv/bin/python -m benchmarks.run --suite core --fast
```

Include warnings as well only when that is the measurement target:

```bash
OME_ZARR_BENCH_DISABLE_LOGGING=0 \
OME_ZARR_BENCH_DISABLE_WARNINGS=0 \
  .venv/bin/python -m benchmarks.run --suite core --fast
```

Include raw stdout/stderr only when that is the measurement target:

```bash
OME_ZARR_BENCH_SUPPRESS_STDIO=0 \
  .venv/bin/python -m benchmarks.run --suite public-api --fast
```

Enforce benchmark coverage for documented upstream public callables:

```bash
.venv/bin/python scripts/check_public_api_benchmark_coverage.py
```

Run a full `core` snapshot:

```bash
.venv/bin/python -m benchmarks.run \
  --suite core \
  --processes 6 \
  --values 10 \
  --warmups 1 \
  --min-time 0.02 \
  --output /tmp/ome-zarr-c-bench.json
```

Run only the kernel tier with tighter sampling:

```bash
.venv/bin/python -m benchmarks.run \
  --suite core \
  --group meso \
  --processes 10 \
  --values 20 \
  --warmups 2 \
  --min-time 0.05 \
  --output /tmp/ome-zarr-c-bench-meso.json
```

Run the public-API suite:

```bash
.venv/bin/python -m benchmarks.run \
  --suite public-api \
  --processes 2 \
  --values 2 \
  --warmups 1 \
  --min-time 0.01 \
  --quiet \
  --output /tmp/ome-zarr-c-bench-public-api.json
```

Run only the format cases in the public API suite during iteration:

```bash
timeout 180s .venv/bin/python -m benchmarks.run \
  --suite public-api \
  --match format \
  --fast \
  --output /tmp/ome-zarr-c-bench-public-api-format.json
```

Run the default real-data suite:

```bash
.venv/bin/python -m benchmarks.run \
  --suite realdata \
  --processes 2 \
  --values 2 \
  --warmups 1 \
  --min-time 0.01 \
  --quiet \
  --output /tmp/ome-zarr-c-bench-realdata.json
```

Render the paired summary:

```bash
.venv/bin/python -m benchmarks.report \
  /tmp/ome-zarr-c-bench.json \
  --markdown-out /tmp/ome-zarr-c-bench.md
```

Generated JSON and markdown benchmark outputs are typically written under
`benchmarks/results/`. This repository keeps selected benchmark snapshots there
as tracked reference artifacts, while temporary exploratory outputs may still
live under `/tmp/` or another local scratch path.

## Current Snapshot

Latest completed broad snapshot on `2026-04-14` on the current portable `-O3`
build:

- `core`: `29` paired cases, geometric-mean speedup `1.139x` (`python / cpp`)
- `public-api`: `38` paired cases, geometric-mean speedup `1.032x`
- `realdata`: `3` paired cases, geometric-mean speedup `1.005x`
- public API benchmark coverage:
  - `89` documented callables discovered from upstream modules
  - `8` abstract exclusions
  - `0` uncovered callable entrypoints

Notable wins in that completed snapshot:

- `conversions.rgba_to_int_batch`: `2.198x`
- `conversions.int_to_rgba_batch`: `1.904x`
- `data.rgb_to_5d_batch`: `2.467x`
- `data.make_circle_batch`: `2.087x`
- `format.detect_format_batch`: `1.413x`
- `scale.scaler_methods`: `4.533x`

Still-slower paths in the same completed snapshot:

- `format.matches`: `0.824x`
- `format.well_and_coord`: `0.705x`
- `utils.find_multiscales`: `0.879x`
- `writer.resolve_storage_options_batch`: `0.901x`
- `scaler.nearest_rgb`: `0.944x`

Interpretation:

- The strongest gains are in arithmetic-heavy conversion and array-shaping
  kernels.
- The meso compute slice is modestly faster overall on the current machine.

Latest focused format-only fast slice on `2026-04-14`:

- artifact: `benchmarks/results/format-slice-v4-report.md`
- `4` paired cases, geometric-mean speedup `0.887x`
- `dispatch`: `1.059x`
- `matches`: `0.876x`
- `v01_init_store`: `0.970x`
- `well_and_coord`: `0.688x`

That focused slice is better than the earlier committed format slices, but the
format path is still not a net win overall on this machine and remains the
highest-value optimization target.
- The broad suite-level picture was still near-flat in the last completed
  all-suite run, so targeted wins should not be overstated as package-wide wins.
- Boundary-heavy helper and format paths are still the main performance drag.
- Filesystem-heavy runtime paths are noisier than in-memory kernels, so the
  summaries use `pyperf` medians and geometric means instead of raw
  means.

## Benchmark Hygiene

- Keep BLAS/OpenMP thread counts pinned to `1` unless there is an explicit
  reason to benchmark multi-threaded kernels.
- Prefer the single-threaded Dask scheduler for reproducibility.
- Treat the first numbers as a baseline, not a marketing claim.
- Re-run unstable cases with more `--processes`, `--values`, or `--min-time`.
- If `pyperf` reports instability, do not ignore it; adjust the run, inspect
  the results again, and prefer medians or geometric means over a single raw
  sample.
