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
sudo apt-get install -y build-essential cmake ninja-build
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

The standalone native CLI currently exposes already-native helper and planning
surfaces directly from `cpp/native/`.

Examples:

```bash
./build-cpp/ome_zarr_native_cli cli create-plan --method astronaut
./build-cpp/ome_zarr_native_cli cli scale-factors --downscale 2 --max-layer 4
./build-cpp/ome_zarr_native_cli data create-plan \
  --version 0.5 --base-shape 1,3,512,512 --smallest-shape 1,3,64,64
./build-cpp/ome_zarr_native_cli format detect --multiscales-version 0.5
./build-cpp/ome_zarr_native_cli format matches --version 0.5 --multiscales-version 0.5
./build-cpp/ome_zarr_native_cli format zarr-format --version 0.5
./build-cpp/ome_zarr_native_cli format chunk-key-encoding --version 0.5
./build-cpp/ome_zarr_native_cli format class-name --version 0.4
./build-cpp/ome_zarr_native_cli format generate-well \
  --path B/3 --rows A,B,C --columns 1,2,3
./build-cpp/ome_zarr_native_cli format validate-well \
  --path B/3 --row-index 1 --column-index 2 --rows A,B,C --columns 1,2,3
./build-cpp/ome_zarr_native_cli io subpath --path /tmp/demo.zarr --subpath labels/0 --file
./build-cpp/ome_zarr_native_cli utils view-plan \
  --path /tmp/demo/image.zarr --port 8013 --discovered-count 1
./build-cpp/ome_zarr_native_cli utils finder-plan \
  --path /tmp/demo/images --port 8012
./build-cpp/ome_zarr_native_cli writer image-plan \
  --axes t,c,y,x --scaler-present --scaler-max-layer 4 --scaler-method local_mean
```

This does not yet replace the full historical Python CLI. It is the first real
native runtime entrypoint for already-native surfaces, and it exists so the
product path can keep moving away from the Python/pybind runtime.

The native benchmark layer measures pure C++ semantic cost. Use the Python
benchmark suite separately when you need end-to-end parity-harness timing or
upstream-versus-port comparison on the same machine.

For a fast iteration comparison on the touched `format` hotspot:

```bash
timeout 180s .venv/bin/python scripts/compare_iteration_benchmarks.py --match format
```

For a fast iteration comparison on the touched `utils` hotspot:

```bash
timeout 180s .venv/bin/python scripts/compare_iteration_benchmarks.py \
  --match utils \
  --native-match utils
```
