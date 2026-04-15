# Standalone C++ Target

This document defines the intended product shape for `ome-zarr-C`.

## Product goal

The final delivered product is a standalone C++ implementation derived from the
frozen `ome-zarr-py` `v0.15.0` logic.

That means:

- shipped runtime code should be C++ only
- Python is allowed in this repository for parity proofs, fixture generation,
  benchmark comparison, and other development-time oracle work
- Python adapters, pybind glue, and Python package metadata are transitional
  scaffolding, not the target product

## Current state vs target state

Current `main` is still a transitional workspace:

- `cpp/native/` contains pure-native semantic code
- `cpp/bindings/` plus `ome_zarr_c/` provide the Python-visible parity harness
- `tests/` and `benchmarks/` compare the converted behavior against the frozen
  Python upstream

Target end state:

- a native C++ library built directly from `cpp/native/`
- native C++ executables for CLI, testing, and benchmarking
- no Python dependency in the shipped runtime path
- Python kept only as a non-shipping oracle toolchain until parity is fully
  proven on the standalone C++ product

## Architectural rules

1. Do not add new semantic logic to `cpp/bindings/` unless the boundary is
   temporarily unavoidable for parity proof.
2. Do not treat the Python package shape as the product contract.
3. Prefer moving reusable logic into `cpp/native/` even if a temporary Python
   harness still consumes it.
4. When a pure-native benchmark and a Python-visible benchmark disagree, use
   that difference to identify boundary overhead rather than guessing.
5. Keep the current state and target state documented separately so repo docs
   stay truthful while the migration is in progress.

## Immediate migration priorities

1. Keep `cpp/native/` buildable without Python headers or pybind.
2. Grow native build, self-test, and benchmark tooling around `cpp/native/`.
3. Reduce boundary-heavy slow paths in `cpp/bindings/` only when they still
   matter for parity-proof workflows.
4. Move user-facing runtime claims away from the Python package shape and
   toward the standalone C++ target as native entrypoints become available.

## Current native-only entrypoints

Current `main` now ships these standalone-native CMake targets:

- `ome_zarr_native`: static library built from `cpp/native/`
- `ome_zarr_native_cli`: native executable entrypoint for real standalone
  runtime commands such as `info` and `finder`
- `ome_zarr_native_selftest`: native smoke and edge-case regression checks
- `ome_zarr_native_bench_format`: focused format hotspot benchmark
- `ome_zarr_native_bench_core`: broader bounded native benchmark suite

These targets run without importing the Python runtime. Python remains in the
repository separately for parity-proof workflows against the frozen upstream
release.
