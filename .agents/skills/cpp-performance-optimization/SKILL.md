---
name: cpp-performance-optimization
description: Optimize a parity-proven ome-zarr C++ surface for measurable speedups by removing Python-object overhead, copies, and unnecessary boundary crossings without changing behavior.
origin: repo-local, grounded in official pybind11 and pyperf docs
---

# C++ Performance Optimization

Use this skill only after parity is already proven for the target surface.

Read `references/official-guidance.md` before changing hot bindings or array
paths.

## Optimization order

1. Better data structures and data layout.
2. Fewer allocations, copies, and repeated scans.
3. Fewer Python/C++ boundary crossings.
4. Typed buffer entrypoints and native structs.
5. Only then smaller inner-loop or compile-time refinements.

## Workflow

1. Start from measured benchmark output, not intuition.
2. Rank cases by both absolute runtime and `python / cpp` slowdown.
3. Fix the biggest structural cost first:
   - choose the cheapest correct container or native layout for the access
     pattern
   - avoid node-heavy or copy-heavy structures in hot paths unless required
   - reduce repeated parsing or scanning of the same metadata
4. Remove Python-object churn from hot paths:
   - repeated `attr()` calls
   - repeated dict/list materialization
   - repeated `repr()` or string building on the success path
   - repeated Python/C++ boundary crossings inside loops
5. Move semantics toward typed native structs and enums in `cpp/native/`.
6. Remove repeated work inside tight loops:
   - precompute reused invariants
   - hoist repeated scans or lookups
   - avoid rebuilding the same derived metadata on every iteration
7. Prefer contiguous native layouts and pre-sized containers in hot iteration
   paths. Use node-based or allocation-heavy structures only when the contract
   truly requires them.
8. In hot array paths, prefer typed buffer or `py::array_t<T>` entrypoints over
   generic Python-object or implicit STL-conversion paths when the public
   contract allows it.
9. Use non-owning views such as `std::string_view` or spans only when the
   source lifetime is guaranteed for the full use-site and the Python-visible
   behavior stays identical.
10. Prefer move-friendly construction, `reserve`, and in-place population when
   they remove copies without changing the public semantics.
11. Push fixed lookup tables or deterministic shape metadata to compile time
   when the values are truly static and parity does not depend on runtime
   construction.
12. Release the GIL only around long-running native-only code that cannot touch
   Python objects, Python callbacks, or pybind11-owned members.
13. Preserve exact output, exception behavior, ordering, dtype handling, and
   serialization semantics. Performance does not justify divergence.
14. Rebuild, rerun the narrow parity lane, then rerun the same benchmark case on
   the same machine class before claiming a speedup.

## Guardrails

- Do not introduce unsafe math flags, approximate algorithms, or reordered
  semantics that change observable results.
- Do not apply a zero-copy view optimization across a Python boundary unless the
  ownership and lifetime model is explicitly proven safe.
- Do not parallelize code whose ordering, floating-point accumulation, or
  exception timing is part of the public behavior unless parity is re-proven on
  that exact path.
- Do not apply PGO, host-specific flags, or parallel execution settings as a
  blanket repo claim unless the exact build profile and benchmark scope are
  stated.
- Do not replace a branch with branchless logic if that changes exception
  timing, overflow behavior, or short-circuit semantics.
- Do not trade away pure-native structure by moving semantics back into Python
  objects just because a short-term benchmark looks better.
