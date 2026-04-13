# Benchmark Suite

This repository uses a dedicated `pyperf`-based benchmark suite for performance
work on parity-proven native-backed surfaces.

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

Included in the current suite:

- in-memory helper kernels such as conversions, Dask resizing, and pyramid
  construction
- deterministic local-store runtime flows such as `parse_url`, `info`,
  `write_image`, `create_zarr`, and CLI create/info/download

Excluded from the paired suite on this dependency stack:

- direct upstream `ome_zarr.utils.download()` because it still raises here when
  its lower-level `zarr` call path passes `zarr_array_kwargs` into the current
  `zarr` API
- network-backed stores, browser launches, and other non-deterministic flows

## Benchmark Layout

- `benchmarks/run.py`: executes paired upstream-vs-native `pyperf` timings
- `benchmarks/cases.py`: benchmark registry, parity guards, and deterministic
  benchmark inputs
- `benchmarks/runtime_support.py`: deterministic tempdir fixtures and shared
  runtime parity helpers reused by the benchmark registry
- `benchmarks/report.py`: turns a `pyperf` JSON file into a markdown summary

Each paired case benchmarks the Python upstream function and the converted
native-backed function on the same exact benchmark input. Before timing begins,
the case verifies parity on that benchmark input and aborts if parity fails.

## Running The Suite

Install the benchmark dependency:

```bash
.venv/bin/python -m pip install -e .[benchmark] --no-build-isolation
```

List the available cases:

```bash
.venv/bin/python -m benchmarks.run --list
```

List only the compute-heavy kernel cases:

```bash
.venv/bin/python -m benchmarks.run --group meso --list
```

Verify benchmark parity inputs without timing:

```bash
.venv/bin/python -m benchmarks.run --verify-only
```

Run a first full benchmark snapshot:

```bash
.venv/bin/python -m benchmarks.run \
  --processes 6 \
  --values 10 \
  --warmups 1 \
  --min-time 0.02 \
  --output /tmp/ome-zarr-c-bench.json
```

Run only the kernel tier with tighter sampling:

```bash
.venv/bin/python -m benchmarks.run \
  --group meso \
  --processes 10 \
  --values 20 \
  --warmups 2 \
  --min-time 0.05 \
  --output /tmp/ome-zarr-c-bench-meso.json
```

Run only the deterministic runtime tier:

```bash
.venv/bin/python -m benchmarks.run \
  --group runtime \
  --processes 6 \
  --values 10 \
  --warmups 1 \
  --min-time 0.03 \
  --output /tmp/ome-zarr-c-bench-runtime.json
```

Render the paired summary:

```bash
.venv/bin/python -m benchmarks.report \
  /tmp/ome-zarr-c-bench.json \
  --markdown-out /tmp/ome-zarr-c-bench.md
```

## Current Snapshot

Committed benchmark snapshot on `2026-04-13`:

- Full paired suite: `29` cases, geometric-mean speedup `0.977x`
  (`python / cpp`)
- Group geometric means:
  - `micro`: `0.912x`
  - `meso`: `1.090x`
  - `macro`: `0.981x`
  - `runtime`: `0.985x`
- Runtime-focused rerun: `10` cases, geometric-mean speedup `1.024x`

High-signal cases from the same snapshot:

- `conversions.rgba_to_int_batch`: `2.171x` faster in C++
- `conversions.int_to_rgba_batch`: `1.809x` faster in C++
- `dask_utils.resize_2d`: `1.303x` faster in C++
- `writer.write_image_v05_delayed` runtime rerun: `1.278x` faster in C++
- `data.create_zarr_astronaut_v05` runtime rerun: `1.074x` faster in C++
- `utils.info_v3_image_with_stats` runtime rerun: `0.889x`, currently slower

Interpretation:

- The strongest gains are in converted compute kernels and some delayed write
  paths.
- The full-suite geometric mean is still slightly below break-even because
  several boundary-heavy micro cases remain slower.
- Filesystem-heavy runtime paths are noisier than in-memory kernels, so the
  committed summaries use `pyperf` medians and geometric means instead of raw
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
