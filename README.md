# ome-zarr-C

`ome-zarr-C` is a release-anchored C++/pybind11 porting workspace for
[`ome/ome-zarr-py`](https://github.com/ome/ome-zarr-py).

The project preserves the exact upstream `v0.15.0` release snapshot under
`source_code_v.0.15.0/` and implements converted functionality outside that
snapshot in native-backed modules. Under the stricter current architecture,
`cpp/native/` holds semantics, `cpp/bindings/` holds boundary glue, and mixed
files outside those roots remain remediation debt. The working rule for
converted code is behavioral parity first, performance claims second.

## Goals

- port upstream Python surfaces incrementally to C++ with `pybind11`
- preserve upstream public behavior, including edge cases and observable quirks
- prove parity with differential tests against the frozen release snapshot
- benchmark converted surfaces with representative data before claiming gains

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
- `cpp/`: C++ implementations exposed through `pybind11`
- `ome_zarr_c/`: Python compatibility layer for converted surfaces
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
and excludes mixed exports still left in `cpp/core.cpp`.

## Pure-Native Coverage

Use the stricter report when you need the percentage of upstream behavior whose
semantics already live in `cpp/native/`:

```bash
.venv/bin/python scripts/report_pure_native_coverage.py
```

This metric is intentionally stricter than `split-native` and should be used
for claims about real native semantic ownership.

Current committed manifest:

- `3856 / 4180 = 92.248804%` pure-native coverage

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

Host prerequisites:

- Python `3.12`
- a working C++17 toolchain for building the `pybind11` extension
- Python headers for the selected interpreter

Typical Linux package examples:

- Debian or Ubuntu: `build-essential python3.12-dev`
- Fedora: `gcc-c++ python3.12-devel`

Create a local environment and install the editable package:

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

The host-tuned profile is intentionally opt-in because it is not portable across
different CPU targets. The default profile is the one intended for reproducible
CI and portable deployments.

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

Latest completed broad snapshot on `2026-04-13` before the current `-O3`
retime:

- `core`: `29` cases, geometric mean `0.982x` (`python / cpp`)
- `public-api`: `38` cases, geometric mean `0.993x`
- `realdata`: `3` default public fixtures, geometric mean `0.995x`
- public API coverage checker: `89` documented callables, `8` abstract
  exclusions, `0` uncovered callables

Latest targeted local reruns on the current portable `-O3` build:

- `core` conversions micro-slice: geometric mean `2.032x`
- `core` data-oriented micro-slice: geometric mean `1.503x`
- `core` meso compute slice: geometric mean `1.059x`
- strongest measured wins in those reruns:
  - `conversions.rgba_to_int_batch`: `2.167x`
  - `conversions.int_to_rgba_batch`: `1.906x`
  - `data.rgb_to_5d_batch`: `2.451x`
  - `data.make_circle_batch`: `2.100x`
  - `dask_utils.resize_2d`: `1.321x`
  - `scale.build_pyramid_local_mean`: `1.108x`
- still-slower paths in the same reruns:
  - `writer.validate_datasets_batch`: `0.960x`
  - `writer.get_valid_axes_batch`: `0.843x`
  - `format.matches`: `0.798x`
  - `format.well_and_coord`: `0.631x`

For read-only surfaces that print absolute paths, parity tests should run the
upstream and converted implementations against the same fixture path so the
comparison measures behavior rather than path differences.

## Security and Scan Scope

Repository-maintained code is scanned and tested. The frozen upstream snapshot
under `source_code_v.0.15.0/` is excluded from security scanning so alerts stay
focused on converted and maintained code in this repository.

## License

This repository contains upstream BSD 2-Clause licensed code preserved from
`ome/ome-zarr-py` together with new conversion work. See
`source_code_v.0.15.0/LICENSE` for the upstream license text.
