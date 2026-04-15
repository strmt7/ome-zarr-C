# Native Build And Self-Test

This document describes the standalone native C++ build path that runs without
the Python runtime.

## Scope

This path exercises `cpp/native/` directly:

- native static library: `ome_zarr_native`
- native CLI executable: `ome_zarr_native_cli`
- native self-test executable: `ome_zarr_native_selftest`
- native benchmark executables:
  - `ome_zarr_native_bench_format`
  - `ome_zarr_native_bench_core`

Python still exists elsewhere in the repository for parity proofs against the
frozen upstream release, but it is not required to compile or run these native
targets.

## Host prerequisites

Install a modern C++ toolchain plus CMake and Ninja.

Typical Ubuntu or Debian example:

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake ninja-build libblosc-dev libzstd-dev
```

## Configure And Build

```bash
cd /opt/omeroconvertedc/ome-zarr-C
cmake -S . -B build-cpp -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-cpp -j2
```

Optional local host tuning:

```bash
cmake -S . -B build-cpp -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DOME_ZARR_C_NATIVE_ENABLE_LTO=ON \
  -DOME_ZARR_C_NATIVE_MARCH_NATIVE=ON
cmake --build build-cpp -j2
```

These tuning flags are for local benchmarking or deployment on a known host.
Do not use them for portability claims.

## Run Native Verification

Run the standalone native self-test directly:

```bash
./build-cpp/ome_zarr_native_selftest
./build-cpp/ome_zarr_native_cli --help
```

Or through CTest:

```bash
ctest --test-dir build-cpp --output-on-failure
```

## Run Native Benchmarks

The native benchmarks are intentionally bounded so they are useful during local
optimization work instead of taking tens of minutes.

Quick sanity pass:

```bash
timeout 120s ./build-cpp/ome_zarr_native_bench_format --quick
timeout 120s ./build-cpp/ome_zarr_native_bench_core --quick
timeout 120s ./build-cpp/ome_zarr_native_bench_core --match format --quick
```

Focused run for a specific hotspot:

```bash
timeout 120s ./build-cpp/ome_zarr_native_bench_core --match writer --quick
timeout 120s ./build-cpp/ome_zarr_native_bench_core --match format --rounds 6 --iterations 5000
```

## Native CLI

The standalone native CLI now exposes real runtime commands rather than
plan-only helper surfaces.

Current commands:

```bash
./build-cpp/ome_zarr_native_cli info /tmp/demo/image.zarr
./build-cpp/ome_zarr_native_cli info /tmp/demo/image.zarr --stats
./build-cpp/ome_zarr_native_cli finder /tmp/demo/images --port 8012
./build-cpp/ome_zarr_native_cli download /tmp/demo/image.zarr --output /tmp/out
./build-cpp/ome_zarr_native_cli view /tmp/demo/image.zarr --port 8013
```

Current scope:

- `info`: standalone local metadata traversal for OME-Zarr image roots
- `info --stats`: standalone local metadata traversal plus dataset min/max reporting
- `finder`: standalone local OME-Zarr discovery plus BioFile Finder CSV output
- `download`: standalone local OME-Zarr export with real v2/v3 metadata and chunk rewriting
- `view`: standalone local validator-serving runtime with real browser launch and CORS-enabled HTTP serving

This still does not replace the full historical Python CLI. The native product
path is being expanded command by command, and every new standalone command
must be parity-checked against the frozen Python oracle before it is treated as
acceptable.

The native benchmark layer measures pure C++ semantic cost. Use the Python
benchmark suite separately when you need end-to-end parity-harness timing or
upstream-versus-port comparison on the same machine.

For a fast iteration comparison on the touched `format` hotspot:

```bash
timeout 180s .venv/bin/python scripts/compare_iteration_benchmarks.py --match format
```

For direct Python-vs-native comparison on the current standalone runtime
commands:

```bash
timeout 180s .venv/bin/python scripts/compare_iteration_benchmarks.py \
  --suite core \
  --match info_v2_image \
  --python-match info_v2_image \
  --native-match local.info \
  --paired-case runtime.utils.info_v2_image=local.info

timeout 180s .venv/bin/python scripts/compare_iteration_benchmarks.py \
  --suite public-api \
  --match finder \
  --python-match finder \
  --native-match local.finder \
  --paired-case utils.finder=local.finder

timeout 180s .venv/bin/python scripts/compare_iteration_benchmarks.py \
  --suite public-api \
  --match download \
  --python-match utils.download \
  --native-match local.download \
  --paired-case utils.download=local.download

timeout 180s .venv/bin/python scripts/compare_iteration_benchmarks.py \
  --suite public-api \
  --match view \
  --python-match utils.view \
  --native-match local.view_prepare \
  --paired-case utils.view=local.view_prepare

timeout 180s .venv/bin/python scripts/compare_iteration_benchmarks.py \
  --suite core \
  --match info_v3_image_with_stats \
  --python-match info_v3_image_with_stats \
  --native-match local.info_stats \
  --paired-case runtime.utils.info_v3_image_with_stats=local.info_stats
```

The standalone `view` pairing intentionally compares Python `utils.view` against
native `local.view_prepare`, because the long-lived HTTP server itself is not a
bounded benchmark target. The comparison measures validator URL and serving
setup cost, while the subprocess CLI tests cover the real server behavior.
