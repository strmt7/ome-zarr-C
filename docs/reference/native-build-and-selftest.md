# Native Build And Self-Test

This document describes the standalone native C++ build path that runs without
the Python runtime.

## Scope

This path exercises `cpp/native/` directly:

- native static library: `ome_zarr_native`
- optional native shared C ABI library: `ome_zarr_native_api`
- native CLI executable: `ome_zarr_native_cli`
- native oracle probe executable: `ome_zarr_native_probe`
- native self-test executable: `ome_zarr_native_selftest`
- native C ABI self-test executable: `ome_zarr_native_api_selftest`
- native benchmark executables:
  - `ome_zarr_native_bench_format`
  - `ome_zarr_native_bench_core`

Python still exists elsewhere in the repository for parity proofs against the
frozen upstream release, but it is not required to compile or run these native
targets.

## Pinned Native Stack

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

The repo source of truth for those versions is
`docs/reference/native-dependency-manifest.json`.

## Host prerequisites

Install a working compiler plus the small bootstrap prerequisites, then use the
repo installer to fetch and install the pinned latest native toolchain.

Typical Ubuntu or Debian example:

```bash
sudo apt-get update
sudo apt-get install -y build-essential curl unzip
./scripts/install_latest_native_toolchain.sh /usr/local
```

## Configure And Build

```bash
cd /opt/omeroconvertedc/ome-zarr-C
/usr/local/bin/cmake -S . -B build-cpp -G Ninja -DCMAKE_BUILD_TYPE=Release
/usr/local/bin/cmake --build build-cpp -j2
```

Optional local host tuning:

```bash
/usr/local/bin/cmake -S . -B build-cpp -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DOME_ZARR_C_NATIVE_ENABLE_LTO=ON \
  -DOME_ZARR_C_NATIVE_MARCH_NATIVE=ON
/usr/local/bin/cmake --build build-cpp -j2
```

These tuning flags are for local benchmarking or deployment on a known host.
Do not use them for portability claims.

## Run Native Verification

Run the standalone native self-test directly:

```bash
./build-cpp/ome_zarr_native_selftest
./build-cpp/ome_zarr_native_api_selftest
./build-cpp/ome_zarr_native_cli --help
./build-cpp/ome_zarr_native_probe --help
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
./build-cpp/ome_zarr_native_cli create --method coins --format 0.5 /tmp/demo/coins.zarr
./build-cpp/ome_zarr_native_cli finder /tmp/demo/images --port 8012
./build-cpp/ome_zarr_native_cli download /tmp/demo/image.zarr --output /tmp/out
./build-cpp/ome_zarr_native_cli view /tmp/demo/image.zarr --port 8013
./build-cpp/ome_zarr_native_cli scale /tmp/demo/input.zarr /tmp/demo/output.zarr yx --copy-metadata --method nearest --max_layer 2
./build-cpp/ome_zarr_native_cli csv_to_labels /tmp/demo/props.csv cell_id score#d /tmp/demo/image.zarr cell_id
```

Current scope:

- `info`: standalone local metadata traversal for OME-Zarr image roots
- `info --stats`: standalone local metadata traversal plus dataset min/max reporting
- `create`: standalone synthetic dataset creation for `coins` and `astronaut`
  in formats `0.4` and `0.5`
- `finder`: standalone local OME-Zarr discovery plus BioFile Finder CSV output
- `download`: standalone local OME-Zarr export with real v2/v3 metadata and chunk rewriting
- `view`: standalone local validator-serving runtime with real browser launch and CORS-enabled HTTP serving
- `csv_to_labels`: standalone local CSV-to-label-properties mutation for image and plate roots

This still does not expose every historical upstream library surface as a
stable native library API. The native runtime path is already expanded command
by command, and every
standalone command must be parity-checked against the frozen Python oracle
before it is treated as acceptable.

The native benchmark layer measures pure C++ semantic cost. Use the Python
benchmark suite separately when you need end-to-end parity-harness timing or
upstream-versus-port comparison on the same machine.

Python-vs-native reports must show time terms first: Python time, native C++
time, time saved per operation, and native C++ time reduction. If a ratio is
included, label it as native C++ speedup over Python
(`python_time / native_cpp_time`).

## Native C ABI Interop

The build produces `ome_zarr_native_api` as a shared library. This target is a
thin C ABI over native semantics for external FFI consumers. It is tested with
native CTest coverage and with `tests/test_native_c_api_interop.py`, which
uses `ctypes`, NumPy arrays, and Zarr-created local stores as external
callers/producers.

The ABI intentionally accepts raw C pointers, shape metadata, and JSON strings
instead of Python objects. See `docs/reference/native-c-api-interop.md` before
adding or changing an ABI entrypoint.

For a fast iteration comparison on the touched `format` hotspot:

```bash
timeout 180s .venv/bin/python scripts/compare_iteration_benchmarks.py --match format
```

For direct Python-vs-native comparison on the current standalone-native
benchmark cases:

```bash
timeout 180s .venv/bin/python scripts/compare_iteration_benchmarks.py \
  --suite public-api \
  --match format \
  --native-match format
```

For heavier cases, keep the bounded helper aggressively small instead of
waiting on a broad pyperf run:

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
