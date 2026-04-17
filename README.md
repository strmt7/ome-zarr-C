# ome-zarr-C

> **Early alpha test:** this repository is an experimental porting workspace.
> Many or most features may be incomplete, broken, inaccurate, unstable, or
> behave differently from what users expect. It is provided as-is for testing
> only, with no responsibility accepted for failures, data loss, incorrect
> results, or other consequences of using it.

`ome-zarr-C` is a release-anchored C++ porting workspace for
[`ome/ome-zarr-py`](https://github.com/ome/ome-zarr-py).

The project preserves the exact upstream `v0.15.0` release snapshot under
`source_code_v.0.15.0/` and implements converted functionality outside that
snapshot in native C++ modules. Under the stricter current architecture,
`cpp/native/` holds C++ semantics, `cpp/tools/` exposes standalone native
entrypoints, and no active binding layer remains. The working rule for
converted code is behavioral parity first, performance claims second.

Current `main` uses Python only as a development oracle for tests, fixture
generation, and benchmark comparison against the frozen upstream snapshot. The
active converted implementation and runtime entrypoints are standalone native
C++; repo-maintained Python compatibility packages are not part of the
architecture.

## Goals

- port upstream Python surfaces incrementally to C++
- preserve upstream public behavior, including edge cases and observable quirks
- prove parity with differential tests against the frozen release snapshot
- benchmark converted surfaces with representative data before claiming gains
- migrate toward a standalone C++ product while keeping the Python oracle only
  as development-time proof infrastructure

## Upstream Provenance

- Original repository: `ome/ome-zarr-py`
- Imported release: `v0.15.0`
- Imported release commit: `cade24e`
- Upstream license: BSD 2-Clause

The upstream snapshot is kept as immutable reference material in
`source_code_v.0.15.0/`. All new implementation work happens outside that
directory.

## Repository Layout

- `source_code_v.0.15.0/`: frozen upstream release snapshot
- `cpp/`: C++ implementations
- `cpp/native/`: pure-native semantic implementation
- `cpp/api/`: optional C ABI interoperability boundary for external callers
- `cpp/tools/`: standalone native self-test and benchmark executables
- `tests/`: differential and regression tests
- `benchmarks/`: Python-upstream and native-C++ timing suites
- `docs/`: project references, design notes, and benchmark material

Start with `docs/reference/cpp-user-guide.md` when you need practical native
C++ command, concept, and API usage guidance.

## Verified Parity Surfaces

Verified surfaces are proven through root-level tests under `tests/` and the
standalone native self-test. Python is used only to execute the frozen upstream
oracle; no repo-maintained Python compatibility package is shipped.

Current verified native areas include:

- `cpp/native/axes.*`
- `cpp/native/conversions.*`
- `cpp/native/csv.*`
- `cpp/native/data.*`
- `cpp/native/dask_utils.*`
- `cpp/native/format.*`
- `cpp/native/io.*`
- `cpp/native/reader.*`
- `cpp/native/scale.*`
- `cpp/native/utils.*`
- `cpp/native/writer.*`

## Split-Native Coverage

Use the committed coverage manifest and report script to measure the current
architecture-first conversion floor:

```bash
.venv/bin/python scripts/report_split_native_coverage.py
.venv/bin/python scripts/report_split_native_coverage.py --fail-under 25
```

This legacy `split-native` metric is retained for historical reporting. Current
manifest entries no longer require binding files; native semantic ownership is
tracked by the stricter pure-native manifest below.

## Pure-Native Coverage

Use the stricter report when you need the percentage of upstream behavior whose
semantics already live in `cpp/native/`:

```bash
.venv/bin/python scripts/report_pure_native_coverage.py
```

This metric is intentionally stricter than `split-native` and should be used
for claims about real native semantic ownership.

Current committed manifest:

- `4180 / 4180 = 100.000000%` pure-native coverage

## Dependency-Sensitive Surfaces

Store-backed reader, writer, data, CLI, CSV label-ingestion, and
`utils.download()` paths are now
covered by differential tests and benchmark lanes on the currently qualified
dependency window shipped in `pyproject.toml`.

The verified parity and benchmark stack for this repository currently depends
on the project-managed `dask` window:

- `dask>=2025.12.0,<=2026.1.1`

Changing that window is possible, but it requires rerunning parity and
benchmark qualification before making any performance or compatibility claim.

## Deployment

## Python Oracle Dev Harness

The current parity and benchmark harness still uses Python because it compares
native C++ behavior directly against the frozen upstream package on the same
machine. That harness is for development-time proof and benchmark comparison,
not the runtime product shape.

Python dev-harness prerequisites:

- Python `3.12`

Typical Linux package examples:

- Debian or Ubuntu: `python3.12-dev`
- Fedora: `python3.12-devel`

Create a local environment and install the metadata-only editable project for
parity and benchmark dependencies:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e '.[dev]' --no-build-isolation
```

Install the benchmark dependency when you want to quantify upstream-vs-native
performance on the verified kernel and runtime slices:

```bash
.venv/bin/python -m pip install -e '.[dev,benchmark]' --no-build-isolation
```

Benchmark runs disable package logging by default so timing is not polluted by
warning and info I/O. Set `OME_ZARR_BENCH_DISABLE_LOGGING=0` when you explicitly
want benchmark runs to include logging side effects.
Benchmark runs also suppress Python warnings by default for the same reason.
Set `OME_ZARR_BENCH_DISABLE_WARNINGS=0` when you intentionally want warning
emission included in a measurement.
Timed benchmark runs also suppress raw stdout and stderr by default.
Set `OME_ZARR_BENCH_SUPPRESS_STDIO=0` only when output cost is intentionally
part of the measurement.

The host-tuned profile is intentionally opt-in because it is not portable across
different CPU targets. The default profile is the one intended for reproducible
CI and portable deployments.

## Standalone Native Build

The repository now also carries a native-only CMake build for `cpp/native/`
plus standalone native validation and benchmark tools. This is the direct path
for measuring pure-native semantic cost without Python boundary overhead.

Pinned latest native stack as of `2026-04-15`:

- CMake `4.3.1`
- Ninja `1.13.2`
- C++ standard `23`
- Zstd `1.5.7`
- zlib `1.3.2`
- LZ4 `1.10.0`
- c-blosc `1.21.6`
- vendored `cpp-httplib` `0.42.0`
- vendored `nlohmann/json` `3.12.0`
- vendored `tinyxml2` `11.0.0`

The source-of-truth manifest for those versions is
`docs/reference/native-dependency-manifest.json`.

Minimal host prerequisites on Ubuntu or Debian:

```bash
sudo apt-get update
sudo apt-get install -y build-essential curl unzip
./scripts/install_latest_native_toolchain.sh /usr/local
```

Configure and build:

```bash
/usr/local/bin/cmake -S . -B build-cpp -G Ninja -DCMAKE_BUILD_TYPE=Release
/usr/local/bin/cmake --build build-cpp -j2
```

Or, once `/usr/local/bin` is first on `PATH`:

```bash
cmake -S . -B build-cpp -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-cpp -j2
```

Optional host-tuned native build:

```bash
cmake -S . -B build-cpp -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DOME_ZARR_C_NATIVE_ENABLE_LTO=ON \
  -DOME_ZARR_C_NATIVE_MARCH_NATIVE=ON
cmake --build build-cpp -j2
```

Run the standalone native verification path:

```bash
./build-cpp/ome_zarr_native_selftest
./build-cpp/ome_zarr_native_api_selftest
./build-cpp/ome_zarr_native_cli --help
ctest --test-dir build-cpp --output-on-failure
```

The build also produces `libome_zarr_native_api.so` on Linux, or the platform
equivalent shared library, for tools that need an FFI boundary instead of a
CLI process. That C ABI is optional and does not embed Python, pybind, or a
repo-maintained Python package. It exposes JSON calls for metadata-shaped
results and typed contiguous buffers for array consumers such as NumPy,
CuPy-compatible host buffers, CFFI, ctypes, Rust, Julia, or C/C++ callers.
See `docs/reference/native-c-api-interop.md` for the ABI contract and tested
NumPy/Zarr interop examples.

Run the bounded native benchmarks:

```bash
./build-cpp/ome_zarr_native_bench_format --quick
./build-cpp/ome_zarr_native_bench_core --quick
./build-cpp/ome_zarr_native_bench_core --match format --quick
```

These native tools are intentionally bounded and do not run for many minutes.
Use them to separate core semantic cost from Python-boundary overhead before
deciding where further optimization work belongs. See
`docs/reference/native-build-and-selftest.md` for the standalone-native
workflow details and focused `--match` examples.

For a bounded iteration comparison on a touched hotspot, run the comparison
script:

```bash
timeout 180s .venv/bin/python scripts/compare_iteration_benchmarks.py --match format
```

That script emits one table with frozen-upstream Python `pyperf` numbers,
matching standalone-native benchmark numbers for the same touched surface,
time saved, native C++ time reduction, and native C++ speedup over Python using
`python_time / native_cpp_time`. Keep that convention for all new timing
summaries: show the two measured times first, then the saved time, then
reduction, then the direct speedup ratio.

For direct time comparison on the real standalone runtime paths touched in the
current iterations:

```bash
timeout 180s .venv/bin/python scripts/compare_iteration_benchmarks.py \
  --suite public-api \
  --match format \
  --native-match format
```

Native CLI quick examples:

```bash
./build-cpp/ome_zarr_native_cli info /tmp/demo/image.zarr
./build-cpp/ome_zarr_native_cli info /tmp/demo/image.zarr --stats
./build-cpp/ome_zarr_native_cli create --method coins --format 0.5 /tmp/demo/coins.zarr
./build-cpp/ome_zarr_native_cli finder /tmp/demo/images --port 8012
./build-cpp/ome_zarr_native_cli download /tmp/demo/image.zarr --output /tmp/out
./build-cpp/ome_zarr_native_cli view /tmp/demo/image.zarr --port 8013
./build-cpp/ome_zarr_native_cli scale /tmp/demo/input.zarr /tmp/demo/output.zarr yx --copy-metadata --method nearest --max_layer 2
./build-cpp/ome_zarr_native_cli csv_to_labels /tmp/demo/props.csv cell_id score#d /tmp/demo/image.zarr cell_id
```

The documented CLI command set now has standalone-native replacements.

Current optional native C ABI entrypoints:

```text
ome_zarr_native_api_project_metadata
ome_zarr_native_api_call_json
ome_zarr_native_api_rgb_to_5d_u8
```

`ome_zarr_native_api_call_json` currently covers native conversions, CSV value
parsing, format metadata, local Zarr location signatures, and scale metadata.
`ome_zarr_native_api_rgb_to_5d_u8` accepts a caller-owned contiguous `uint8`
buffer plus shape metadata and returns an owned 5D buffer matching the frozen
upstream `ome_zarr.data.rgb_to_5d` layout. The caller frees returned memory
with the matching API free function.

For heavier data-kernel iteration comparisons, lower the pyperf worker count
explicitly so the bounded run stays practical:

```bash
timeout 180s .venv/bin/python scripts/compare_iteration_benchmarks.py \
  --suite public-api \
  --match data \
  --native-match data \
  --processes 1 \
  --values 1 \
  --warmups 1 \
  --min-time 0.005
```

Run the current proven-safe local verification lane:

```bash
timeout 180s .venv/bin/python -m pytest -q \
  tests/test_axes_equivalence.py \
  tests/test_conversions_equivalence.py \
  tests/test_dask_utils_equivalence.py \
  tests/test_data_equivalence.py \
  tests/test_format_equivalence.py \
  tests/test_scale_equivalence.py \
  tests/test_scaler_equivalence.py \
  tests/test_utils_equivalence.py \
  tests/test_writer_equivalence.py
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
```

Current full-suite pytest warnings, if any, must be investigated. They should
not be dismissed as performance or native-toolchain issues without evidence.

## Porting Approach

Converted code is added in small, reviewable slices:

1. Read the exact upstream implementation from the frozen snapshot.
2. Port the smallest self-contained surface.
3. Add differential tests against the upstream implementation.
4. Verify local parity before widening the converted area.
5. Benchmark only after parity is established.

## Benchmarking

The repository ships a paired `pyperf` benchmark suite under `benchmarks/`.
It compares the frozen upstream Python implementation with standalone native
C++ benchmark entrypoints on the same benchmark inputs. Python timing helpers
live under `benchmarks/python/`; native C++ timing helpers live under
`benchmarks/native/`. Benchmark orchestration stays in `benchmarks/`; tests
stay in the root `tests/` folder.

For standalone-C++ performance work, also use the native-only CMake benchmark
tooling. The `pyperf` suite is for paired upstream-vs-native comparison, while
the native benchmark executable measures pure-native semantic cost directly.

Benchmark summaries should be interpreted in time terms first. Use the same
unit for Python time and native C++ time, then compute
`time_saved = python_time - native_cpp_time`, native C++ time reduction as
`time_saved / python_time`, and native C++ speedup over Python as
`python_time / native_cpp_time`. A ratio greater than `1.0x` means the native
C++ path used less time for that case; a ratio below `1.0x` means it used more
time. Negative saved time and negative reduction are valid regression signals
and should stay visible.

Native CLI `create` examples are functional parity examples, not create
performance evidence. Do not claim native C++ `create` performance until a
registered native benchmark entrypoint is paired with frozen-upstream Python
timing.

Common benchmark commands:

```bash
.venv/bin/python -m benchmarks.run --list
.venv/bin/python -m benchmarks.run --suite core --verify-only
.venv/bin/python -m benchmarks.run --suite public-api --verify-only
.venv/bin/python -m benchmarks.run --suite realdata --verify-only
.venv/bin/python scripts/check_public_api_benchmark_coverage.py
```

The suite now has three layers:

- `core`: standalone-native-qualified kernels and deterministic helpers
- `public-api`: native-qualified documented public API timing subset
- `realdata`: paired `parse_url`/`info` timings on public OME-Zarr data

The real-data suite downloads public benchmark fixtures into
`.benchmarks-fixtures/` by default, or into `OME_ZARR_BENCH_FIXTURE_ROOT` when
that environment variable is set. An additional `455.3 MiB` BIA fixture is
available only when `OME_ZARR_BENCH_INCLUDE_LARGE=1`.

See `docs/reference/benchmark-suite.md` for the methodology,
`docs/reference/public-api-benchmark-results.md` for the latest bounded local
public-API timing results, and `docs/reference/public-benchmark-fixtures.md`
for fixture provenance.

Refresh the tracked public-API benchmark result snapshot before making a new
performance claim:

```bash
BENCH_ARTIFACT_DIR="${BENCH_ARTIFACT_DIR:-benchmark-artifacts/public-api}"
mkdir -p "$BENCH_ARTIFACT_DIR"
.venv/bin/python -m benchmarks.run \
  --suite public-api \
  --fast \
  --quiet \
  --processes 1 \
  --values 1 \
  --warmups 1 \
  --min-time 0.005 \
  --output "$BENCH_ARTIFACT_DIR/public-api.pyperf.json"
.venv/bin/python -m benchmarks.report \
  "$BENCH_ARTIFACT_DIR/public-api.pyperf.json" \
  --markdown-out "$BENCH_ARTIFACT_DIR/public-api.md"
```

For read-only surfaces that print absolute paths, parity tests should run the
upstream and converted implementations against the same fixture path so the
comparison measures behavior rather than path differences.

Any remaining local test warning must be investigated on its own evidence; do
not treat warnings as acceptable just because they come from an upstream-facing
test path.

## Security and Scan Scope

Repository-maintained code is scanned and tested. The frozen upstream snapshot
under `source_code_v.0.15.0/` is excluded from security scanning so alerts stay
focused on converted and maintained code in this repository.

## License

This repository contains upstream BSD 2-Clause licensed code preserved from
`ome/ome-zarr-py` together with new conversion work. See
`source_code_v.0.15.0/LICENSE` for the upstream license text.
