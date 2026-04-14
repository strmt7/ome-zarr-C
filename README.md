# ome-zarr-C

`ome-zarr-C` is a release-anchored C++ porting workspace for
[`ome/ome-zarr-py`](https://github.com/ome/ome-zarr-py).

The project preserves the exact upstream `v0.15.0` release snapshot under
`source_code_v.0.15.0/` and implements converted functionality outside that
snapshot in native-backed modules. Under the stricter current architecture,
`cpp/native/` holds semantics, `cpp/bindings/` holds boundary glue, and mixed
files outside those roots remain remediation debt. The working rule for
converted code is behavioral parity first, performance claims second.

Current `main` is still a transitional parity workspace with Python-oracle
support. The target end-state product is standalone C++ only: Python remains
in this repo today for parity tests, fixture generation, and benchmark
comparison, but it is not the intended runtime delivery shape.

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
- `cpp/bindings/`: transitional Python boundary glue for parity-proof workflows
- `cpp/tools/`: standalone native self-test and benchmark executables
- `ome_zarr_c/`: transitional Python compatibility/oracle layer
- `tests/`: differential and regression tests
- `docs/`: project references, design notes, and benchmark material

## Verified Native-Backed Surfaces

These are parity-proven native-backed surfaces. This is not the same as
pure-native coverage under the stricter `cpp/native/` plus `cpp/bindings/`
policy.

The following upstream behaviors are native-backed and currently proven by
differential tests on this runtime:

- `ome_zarr_c.conversions`
  - `int_to_rgba`
  - `int_to_rgba_255`
  - `rgba_to_int`
- `ome_zarr_c.axes`
  - axes normalization
  - axis-name extraction
  - OME-Zarr axis validation logic
- `ome_zarr_c.format`
  - format implementation ordering
  - `format_from_version`
  - `detect_format`
  - per-format matching
  - per-format Zarr/chunk-key properties
  - metadata version lookup
  - well-dict validation
  - coordinate-transformation generation and validation
- `ome_zarr_c.csv`
  - `parse_csv_value`
- `ome_zarr_c.utils`
  - `strip_common_prefix`
  - `splitall`
  - `find_multiscales`
- `ome_zarr_c.writer`
  - `_blosc_compressor`
  - `_get_valid_axes`
  - `_extract_dims_from_axes`
  - `_retuple`
  - `_resolve_storage_options`
  - `_validate_well_images`
  - `_validate_plate_acquisitions`
  - `_validate_plate_rows_columns`
  - `_validate_datasets`
  - `_validate_plate_wells`
- `ome_zarr_c.dask_utils`
  - `_better_chunksize`
  - `downscale_nearest`
  - `local_mean`
  - `resize`
  - `zoom`
- `ome_zarr_c.scale`
  - `_build_pyramid`
  - `Scaler.resize_image`
  - `Scaler.nearest`
  - `Scaler.gaussian`
  - `Scaler.laplacian`
  - `Scaler.local_mean`
  - `Scaler.zoom`
- `ome_zarr_c.data`
  - `coins`
  - `astronaut`
  - `make_circle`
  - `rgb_to_5d`

Each converted surface is validated against the frozen upstream release with
parity tests under `tests/`.

## Split-Native Coverage

Use the committed coverage manifest and report script to measure the current
architecture-first conversion floor:

```bash
.venv/bin/python scripts/report_split_native_coverage.py
.venv/bin/python scripts/report_split_native_coverage.py --fail-under 25
```

This `split-native` metric counts only upstream surfaces that are routed
through dedicated `cpp/bindings/` entrypoints with corresponding native files,
and excludes any residual mixed routing that has not yet been isolated to a
dedicated binding-plus-native path.

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

Store-backed reader, writer, data, CLI, and `utils.download()` paths are now
covered by differential tests and benchmark lanes on the currently qualified
dependency window shipped in `pyproject.toml`.

The verified parity and benchmark stack for this repository currently depends
on the project-managed `dask` window:

- `dask>=2025.12.0,<=2026.1.1`

Changing that window is possible, but it requires rerunning parity and
benchmark qualification before making any performance or compatibility claim.

## Deployment

## Transitional Python-Oriented Dev Harness

The current parity and benchmark harness still uses Python because it compares
the converted behavior directly against the frozen upstream package on the same
machine. That harness is for development-time proof, not the intended final
product shape.

Python dev-harness prerequisites:

- Python `3.12`
- a working C++17 toolchain for building the `pybind11` extension
- Python headers for the selected interpreter

Typical Linux package examples:

- Debian or Ubuntu: `build-essential python3.12-dev`
- Fedora: `gcc-c++ python3.12-devel`

Create a local environment and install the editable package for parity work:

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

The extension now builds with an explicit portable release profile by default:

- Unix-like hosts: `-O3` and hidden symbol visibility
- Windows hosts: `/O2`
- no unsafe math flags such as `-ffast-math`

Optional build-time tuning knobs:

- `OME_ZARR_C_ENABLE_LTO=1`: enable link-time optimization
- `OME_ZARR_C_MARCH_NATIVE=1`: enable host-specific CPU tuning on Unix-like hosts

Example host-tuned rebuild for local benchmarking only:

```bash
OME_ZARR_C_ENABLE_LTO=1 OME_ZARR_C_MARCH_NATIVE=1 \
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

Typical host prerequisites on Ubuntu or Debian:

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake ninja-build
```

Configure and build:

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
ctest --test-dir build-cpp --output-on-failure
```

Run the bounded native benchmarks:

```bash
./build-cpp/ome_zarr_native_bench_format --quick
./build-cpp/ome_zarr_native_bench_core --quick
```

These native tools are intentionally bounded and do not run for many minutes.
Use them to separate core semantic cost from Python-boundary overhead before
deciding where further optimization work belongs. See
`docs/reference/native-build-and-selftest.md` for the standalone-native
workflow details and focused `--match` examples.

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

After editing native code under `cpp/`, rebuild the editable install before
re-running tests so the suite exercises the current extension binary:

```bash
.venv/bin/python -m pip install -e . --no-build-isolation
```

## Porting Approach

Converted code is added in small, reviewable slices:

1. Read the exact upstream implementation from the frozen snapshot.
2. Port the smallest self-contained surface.
3. Add differential tests against the upstream implementation.
4. Verify local parity before widening the converted area.
5. Benchmark only after parity is established.

## Benchmarking

The repository ships a paired `pyperf` benchmark suite under `benchmarks/`.
It compares the frozen upstream Python implementation and the native-backed
implementation on the same benchmark inputs and aborts if the timed input loses
parity.

For standalone-C++ performance work, also use the native-only CMake benchmark
tooling. The `pyperf` suite measures Python-visible public behavior, while the
native benchmark measures pure-native semantic cost without pybind/Python
boundary overhead.

Common benchmark commands:

```bash
.venv/bin/python -m benchmarks.run --list
.venv/bin/python -m benchmarks.run --suite core --verify-only
.venv/bin/python -m benchmarks.run --suite public-api --verify-only
.venv/bin/python -m benchmarks.run --suite realdata --verify-only
.venv/bin/python scripts/check_public_api_benchmark_coverage.py
```

The suite now has three layers:

- `core`: converted kernels plus deterministic runtime flows
- `public-api`: coverage-checked timings for the documented upstream public API
- `realdata`: paired `parse_url`/`info`/reader timings on public OME-Zarr data

The real-data suite downloads public benchmark fixtures into
`.benchmarks-fixtures/` by default, or into `OME_ZARR_BENCH_FIXTURE_ROOT` when
that environment variable is set. An additional `455.3 MiB` BIA fixture is
available only when `OME_ZARR_BENCH_INCLUDE_LARGE=1`.

See `docs/reference/benchmark-suite.md` for the methodology and
`docs/reference/public-benchmark-fixtures.md` for fixture provenance.

Latest completed broad snapshot on `2026-04-14` on the current portable `-O3`
build:

- `core`: `29` cases, geometric mean `1.139x` (`python / cpp`)
- `public-api`: `38` cases, geometric mean `1.032x`
- `realdata`: `3` default public fixtures, geometric mean `1.005x`
- public API coverage checker: `89` documented callables, `8` abstract
  exclusions, `0` uncovered callables

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

For read-only surfaces that print absolute paths, parity tests should run the
upstream and converted implementations against the same fixture path so the
comparison measures behavior rather than path differences.

The remaining local test warnings are upstream-compatibility warnings, not
evidence of stale C++ dependencies. They come from deprecated upstream
`Scaler` APIs that remain intentionally covered because the converted package
must keep matching `ome-zarr-py v0.15.0`.

## Security and Scan Scope

Repository-maintained code is scanned and tested. The frozen upstream snapshot
under `source_code_v.0.15.0/` is excluded from security scanning so alerts stay
focused on converted and maintained code in this repository.

## License

This repository contains upstream BSD 2-Clause licensed code preserved from
`ome/ome-zarr-py` together with new conversion work. See
`source_code_v.0.15.0/LICENSE` for the upstream license text.
