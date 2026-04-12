# Benchmark Suite

This repository uses a dedicated `pyperf`-based benchmark suite for performance
work on parity-proven native-backed surfaces.

## Why `pyperf`

- The official `pyperf` documentation describes a benchmark architecture with
  calibration, warmups, multiple worker processes, and structured result files.
- The `pyperf` benchmarking guide also recommends tuning the system, analyzing
  stability, and adjusting runs/values/loops rather than trusting a single wall
  clock reading.
- The `pyperf` system guide documents CPU affinity and other system-noise
  controls that can improve repeatability on Linux.

References:

- <https://pyperf.readthedocs.io/en/latest/run_benchmark.html>
- <https://pyperf.readthedocs.io/en/latest/system.html>

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
- the benchmark input is in-memory and reproducible
- the workload does not depend on the currently blocked store-backed Zarr paths
- the workload does not depend on network access, browser launching, or live
  filesystem traversal noise

Excluded from the first suite:

- `ome_zarr_c.csv.dict_to_zarr`
- `ome_zarr_c.csv.csv_to_zarr`
- `ome_zarr_c.utils.info`
- `ome_zarr_c.data.create_zarr`
- writer metadata/path/store-writing functions
- reader/io/store-backed paths
- side-effect-heavy stdout-oriented flows such as `Scaler.zoom`

## Benchmark Layout

- `benchmarks/run.py`: executes paired upstream-vs-native `pyperf` timings
- `benchmarks/cases.py`: benchmark registry, parity guards, and deterministic
  benchmark inputs
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

Render the paired summary:

```bash
.venv/bin/python -m benchmarks.report \
  /tmp/ome-zarr-c-bench.json \
  --markdown-out /tmp/ome-zarr-c-bench.md
```

## Benchmark Hygiene

- Keep BLAS/OpenMP thread counts pinned to `1` unless there is an explicit
  reason to benchmark multi-threaded kernels.
- Prefer the single-threaded Dask scheduler for reproducibility.
- Treat the first numbers as a baseline, not a marketing claim.
- Re-run unstable cases with more `--processes`, `--values`, or `--min-time`.
- If `pyperf` reports instability, do not ignore it; adjust the run and inspect
  the results again.
