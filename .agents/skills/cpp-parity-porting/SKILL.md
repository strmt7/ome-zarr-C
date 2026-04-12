---
name: cpp-parity-porting
description: Port one upstream ome-zarr-py surface to C++/pybind11 without touching the frozen snapshot.
origin: repo-local
---

# C++ Parity Porting

Use this skill when converting an upstream Python module or helper.

## Workflow

1. Read the upstream implementation from `source_code_v.0.15.0/`.
2. Identify whether the surface should live entirely in C++ or needs a thin
   Python wrapper for compatibility.
3. Port behavior first, including exception types and messages when they are
   observable.
4. Add differential tests against the upstream snapshot.
5. Benchmark only after parity holds.
