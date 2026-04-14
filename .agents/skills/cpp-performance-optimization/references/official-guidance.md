# Official Guidance

- `pybind11` STL conversion docs:
  automatic STL conversions copy data on Python/C++ transitions and can hurt
  both semantics and performance in hot paths. Prefer opaque or more direct
  representations when the boundary is performance-critical.
- `pybind11` NumPy docs:
  `py::array_t<T>` and buffer-oriented access allow typed array entrypoints,
  while unchecked proxies avoid repeated bounds and dimension checks in hot
  loops when indices are already known valid.
- `pybind11` GIL docs:
  pybind11 never releases the GIL implicitly; `gil_scoped_release` is only safe
  when the code cannot access Python state until the GIL is reacquired.
- `pyperf` system docs:
  benchmark tuning should reduce machine jitter and use stable CPU-frequency
  settings before trusting fine-grained timing comparisons.

Repo-validated heuristics that also align with the reviewed external guide:

- remove repeated scans and heap churn before chasing micro-arithmetic wins
- prefer contiguous data layouts in hot iteration paths
- precompute reused loop invariants outside the hottest loop
- prefer move-friendly construction and reserved capacity over repeated copies
- consider compile-time constants only for truly static data
- use zero-copy views only when object lifetime is proven safe
- simplify hot-loop control flow only when parity and error timing stay exact
- parallelize only when ordering, numeric behavior, and exception timing remain
  parity-clean under test

Primary references:

- https://pybind11.readthedocs.io/en/stable/advanced/cast/stl.html
- https://pybind11.readthedocs.io/en/stable/advanced/pycpp/numpy.html
- https://pybind11.readthedocs.io/en/stable/advanced/misc.html
- https://pyperf.readthedocs.io/en/latest/system.html
