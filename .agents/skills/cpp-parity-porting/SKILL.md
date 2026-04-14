---
name: cpp-parity-porting
description: Port one upstream ome-zarr-py surface to C++/pybind11 without touching the frozen snapshot.
origin: repo-local
---

# C++ Parity Porting

Use this skill when converting an upstream Python module or helper.

## Workflow

1. Read the upstream implementation from `source_code_v.0.15.0/`.
2. Identify whether the surface can live in `cpp/native/` with only a minimal
   `cpp/bindings/` boundary. Do not expand mixed legacy files outside those
   roots.
3. Port behavior first, including exception types and messages when they are
   observable.
4. If the port crosses Python runtime behavior such as iterators, truthiness,
   Python callbacks, or Python exception state, load
   `pybind11-runtime-parity` before finalizing the binding.
5. Load `immutable-parity-proof` before making any parity or native-coverage
   claim.
6. Add differential tests against the upstream snapshot.
7. If the observable output includes absolute paths and the surface is
   read-only, run the upstream and converted implementations against the same
   fixture path to avoid false stdout mismatches.
8. Do not keep Python-object semantics in C++ implementation code. If a Python
   boundary is unavoidable, keep it minimal and local to `cpp/bindings/`.
9. After parity holds, load `benchmark-first` and
   `cpp-performance-optimization` before making performance changes.
10. Do not describe a surface as pure-native unless the semantics live in
   `cpp/native/`.
