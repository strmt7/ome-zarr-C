# Benchmark Suite

This repository keeps all timing code under `benchmarks/`. Correctness tests
stay in the root `tests/` folder.

## Layout

- `benchmarks/python/`: frozen-upstream Python timing helpers.
- `benchmarks/native/`: standalone native C++ timing helpers.
- `benchmarks/cases.py`: benchmark case registry and shared timing model.
- `benchmarks/public_api_cases.py`: native-qualified public API timing subset.
- `benchmarks/realdata_cases.py`: public-fixture end-to-end cases.
- `benchmarks/catalog.py`: suite registry.
- `benchmarks/run.py`: `pyperf` runner.
- `benchmarks/report.py`: `pyperf` JSON to markdown report.
- `cpp/tools/native_bench_core.cpp`: standalone native benchmark executable.
- `cpp/tools/native_bench_format.cpp`: focused native format benchmark.

No benchmark may import a repo-maintained converted Python package. Native C++
performance claims require standalone native C++ executable/library timing.

## Suites

- `core`: standalone-native-qualified kernels and deterministic helpers.
- `public-api`: documented upstream public API subset with native benchmark
  entrypoints.
- `realdata`: paired `parse_url`/`info` timings on public OME-Zarr data.

When no suite is specified, `benchmarks.run` executes `core` and `realdata`.
Run `public-api` explicitly when checking public API coverage.

## Method

Each benchmark case has:

- a frozen-upstream Python timer,
- a standalone native C++ timer,
- a verification hook that runs before timing.

The native timer calls `ome_zarr_native_bench_core`; the Python timer imports
only `source_code_v.0.15.0/ome_zarr`. Results must be reported in time terms:
Python time, native C++ time, time saved per operation, and native C++ time
reduction. If a ratio is included, label it as native C++ speedup over Python
using `python_time / native_cpp_time`; never use a shorthand label that omits
the ratio direction.

## Commands

Install benchmark dependencies:

```bash
.venv/bin/python -m pip install -e '.[dev,benchmark]' --no-build-isolation
```

Build native benchmarks:

```bash
./scripts/install_latest_native_toolchain.sh /usr/local
/usr/local/bin/cmake -S . -B build-cpp -G Ninja -DCMAKE_BUILD_TYPE=Release
/usr/local/bin/cmake --build build-cpp -j2
./build-cpp/ome_zarr_native_selftest
./build-cpp/ome_zarr_native_bench_core --quick
```

List cases:

```bash
.venv/bin/python -m benchmarks.run --list
.venv/bin/python -m benchmarks.run --suite public-api --list
```

Verify benchmark inputs without timing:

```bash
.venv/bin/python -m benchmarks.run --suite core --verify-only
.venv/bin/python -m benchmarks.run --suite public-api --verify-only
.venv/bin/python -m benchmarks.run --suite realdata --verify-only
.venv/bin/python scripts/check_public_api_benchmark_coverage.py
```

Run bounded timing:

```bash
.venv/bin/python -m benchmarks.run \
  --suite public-api \
  --fast \
  --quiet \
  --processes 1 \
  --values 1 \
  --warmups 1 \
  --min-time 0.005 \
  --output /tmp/ome-zarr-c-native-public-api.json
.venv/bin/python -m benchmarks.report \
  /tmp/ome-zarr-c-native-public-api.json \
  --markdown-out /tmp/ome-zarr-c-native-public-api.md
```

Use `--implementation python`, `--implementation native`, or
`--implementation both` to keep focused runs bounded. The iteration helper uses
Python-only pyperf collection and the standalone native benchmark binary
separately, so it does not duplicate native work through pyperf.

Run a focused iteration comparison:

```bash
timeout 180s .venv/bin/python scripts/compare_iteration_benchmarks.py \
  --suite public-api \
  --match format \
  --native-match format
```

## Result Artifacts

Tracked result artifacts under `benchmarks/results/` must reflect the current
benchmark architecture. Old result files from deleted benchmark paths must be
removed instead of left as historical-looking current data.

The current tracked public-API result summary lives in
`docs/reference/public-api-benchmark-results.md`. Refresh that summary before
making a new public-API performance claim.

## Fixture Provenance

The `realdata` suite downloads public fixtures on demand into
`.benchmarks-fixtures/`. Override that location with
`OME_ZARR_BENCH_FIXTURE_ROOT=/abs/path/to/cache`.

Default fixture set:

- `examples_image`: small valid image fixture from
  `BioImageTools/ome-zarr-examples` at commit
  `8c10c88fbb77c3dcc206d9b234431f243beee576` (`BSD-3-Clause`).
- `examples_plate`: small valid plate fixture from the same repository and
  commit (`BSD-3-Clause`).
- `bia_tonsil3`: public BioImage Archive / IDR OME-NGFF image,
  `108.8 MiB` (`CC BY 4.0`).

Optional large fixture:

- `bia_156_42`: public BioImage Archive / IDR OME-NGFF image,
  `455.3 MiB` (`CC BY 4.0`).

Enable the large fixture only when explicitly requested:

```bash
OME_ZARR_BENCH_INCLUDE_LARGE=1 .venv/bin/python -m benchmarks.run \
  --suite realdata \
  --verify-only
```
