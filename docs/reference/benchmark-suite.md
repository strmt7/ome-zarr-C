# Benchmark Suite

This repository uses a dedicated `pyperf`-based benchmark suite for performance
work on parity-proven native-backed surfaces. The benchmark tree is split into
three suites:

- `core`: converted kernels plus deterministic tempdir runtime flows
- `public-api`: benchmark coverage for the documented upstream public API
- `realdata`: public OME-Zarr fixtures used for end-to-end reader/info timing

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
- `scripts/check_public_api_benchmark_coverage.py`: enforces benchmark
  coverage for documented upstream public callables

Each paired case benchmarks the Python upstream function and the converted
native-backed function on the same exact benchmark input. Before timing begins,
the case verifies parity on that benchmark input and aborts if parity fails.

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

Generated JSON and markdown benchmark outputs are local artifacts under
`benchmarks/results/`. That directory stays gitignored because the numbers are
machine-dependent.

## Current Snapshot

Latest local snapshot on `2026-04-13`:

- `core`: `29` paired cases, geometric-mean speedup `0.982x` (`python / cpp`)
- `public-api`: `38` paired cases, geometric-mean speedup `0.993x`
- `realdata`: `3` paired cases, geometric-mean speedup `0.995x`
- public API benchmark coverage:
  - `89` documented callables discovered from upstream modules
  - `8` abstract exclusions
  - `0` uncovered callable entrypoints

High-signal cases from the same snapshot:

- `conversions.rgba_to_int_batch`: `2.142x` faster in C++
- `conversions.int_to_rgba_batch`: `1.816x` faster in C++
- `scale.scaler_methods`: `4.591x` faster in C++
- `dask_utils.resize_2d`: `1.307x` faster in C++
- `reader.image_surface`: `1.193x` faster in C++
- `format.matches`: `0.337x`, currently slower in C++

Interpretation:

- The strongest gains are in conversion kernels, scaler method dispatch, and
  selected read/write runtime flows.
- The broad suite-level picture is still near-flat overall because several
  boundary-heavy helper and format paths remain slower.
- The public real-data suite is also near-flat overall on the current machine.
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
