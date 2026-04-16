# Standalone C++ Target

This document defines the intended product shape for `ome-zarr-C`.

## Product goal

The final delivered product is a standalone C++ implementation derived from the
frozen `ome-zarr-py` `v0.15.0` logic.

That means:

- shipped runtime code should be C++ only
- Python is allowed in this repository for parity proofs, fixture generation,
  benchmark comparison, and other development-time oracle work
- Python adapters, pybind glue, and repo-maintained Python compatibility
  packages are not the target product
- optional C ABI entrypoints may exist for external FFI consumers, provided
  they do not embed Python, use pybind, or move semantics out of `cpp/native/`

## Current state vs target state

Current `main` is a standalone-native workspace with a Python oracle:

- `cpp/native/` contains pure-native semantic code
- `cpp/api/` exposes an optional C ABI over selected native surfaces
- no active binding layer remains
- `tests/` and `benchmarks/` compare the converted behavior against the frozen
  Python upstream

Target end state:

- a native C++ library built directly from `cpp/native/`
- native C++ executables for CLI, testing, and benchmarking
- no Python dependency in the shipped runtime path
- Python kept only as a non-shipping oracle toolchain until parity is fully
  proven on the standalone C++ product

## Architectural rules

1. Do not reintroduce Python-object C++ glue unless the boundary is explicitly
   approved as temporarily unavoidable for parity proof.
2. Do not treat the Python package shape as the product contract.
3. Prefer moving reusable logic into `cpp/native/` even if a temporary Python
   harness still consumes it.
4. When a pure-native benchmark and an upstream-Python benchmark disagree, use
   that difference to identify remaining semantic or boundary overhead.
5. Keep the current state and target state documented separately so repo docs
   stay truthful while the migration is in progress.
6. Keep the standalone-native toolchain on the latest pinned stable versions
   recorded in `docs/reference/native-dependency-manifest.json` instead of
   accepting older distro package versions by accident.
7. Keep `cpp/api/` as a thin C ABI over native semantics. It may expose raw
   buffers and JSON for external packages, but it must not include Python
   object dispatch or package-specific adapter logic.

## Immediate migration priorities

1. Keep `cpp/native/` buildable without Python headers or pybind.
2. Grow native build, self-test, and benchmark tooling around `cpp/native/`.
3. Keep the Python development oracle restricted to the frozen upstream
   snapshot and benchmark/test harness code.
4. Move user-facing runtime claims away from the Python package shape and
   toward the standalone C++ target as native entrypoints become available.

## Current native-only entrypoints

Current `main` now ships these standalone-native CMake targets:

- `ome_zarr_native`: static library built from `cpp/native/`
- `ome_zarr_native_api`: optional shared C ABI library for external FFI
  callers
- `ome_zarr_native_cli`: native executable entrypoint for real standalone
  runtime commands such as `info`, `create`, `finder`, `download`, `view`,
  `scale`, and `csv_to_labels`
- `ome_zarr_native_selftest`: native smoke and edge-case regression checks
- `ome_zarr_native_api_selftest`: native C ABI smoke and edge-case regression
  checks
- `ome_zarr_native_bench_format`: focused format hotspot benchmark
- `ome_zarr_native_bench_core`: broader bounded native benchmark suite

These targets run without importing the Python runtime. Python remains in the
repository separately for parity-proof workflows against the frozen upstream
release.

The optional C ABI currently covers JSON metadata calls and a typed contiguous
`uint8` buffer transform for `rgb_to_5d`. Root tests prove that NumPy can pass
real array memory through the ABI and that Zarr-created local stores can be
read through native path metadata signatures without adding a Python adapter.

The largest remaining migration work is broadening native probes and runtime
coverage without reintroducing Python package scaffolding.
