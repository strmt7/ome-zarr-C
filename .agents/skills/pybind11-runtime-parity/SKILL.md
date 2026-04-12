---
name: pybind11-runtime-parity
description: Use when a pybind11 port crosses Python runtime behavior such as exceptions, truthiness, iterators, imports, or Python object callbacks and must remain parity-safe.
origin: repo-local, grounded in official pybind11 docs
---

# pybind11 Runtime Parity

Use this skill when a port is no longer just arithmetic or struct marshaling
and instead interacts with Python objects, imports, generators, iterators, or
exception state.

## Rules

- Preserve Python-visible behavior before simplifying the C++.
- Catch `py::error_already_set` for exceptions that originate in Python code or
  Python C API calls; do not assume pybind11 C++ exception wrappers catch them.
- Use Python truth-value evaluation for generic Python objects instead of
  forcing `py::cast<bool>`.
- When implementing iterator behavior, use `pybind11::stop_iteration` or
  equivalent Python iterator semantics rather than ad-hoc sentinels.
- If the binding depends on imports from partially ported modules, make that
  dependency explicit and local to the wrapper instead of hiding it.

Read `references/official-guidance.md` before changing iterator semantics,
exception handling, or Python-object lifetimes.
