# AI Agent Do's and Don'ts

This file records the operating rules that were reinforced by the current
porting work.

## Priority order

1. Preserve complete parity and identical user-visible functionality with the
   frozen Python upstream.
2. After parity is proven, maximize real performance gains from the native C++
   implementation.

## Do

- Read `docs/reference/architecture-first-porting.md` before widening a port.
- Read the exact upstream implementation before touching the wrapper or C++.
- Port the smallest self-contained surface first.
- Write differential tests first or alongside the native change.
- Rebuild the editable install after every native edit.
- Compare upstream and converted behavior on the same runtime.
- Use the same fixture path when path text is part of the observable output.
- Compare serialized side effects for file- or store-mutating surfaces.
- Patch external boundaries instead of triggering real browsers or servers in
  parity tests.
- Document blockers explicitly when the runtime cannot prove a live surface.
- Keep README and reference docs synchronized with what is actually verified.
- Prefer `--no-build-isolation` in offline or sandboxed rebuilds once local
  build dependencies are already installed.
- Treat existing mixed C++ files as debt. New work should move semantics toward
  `cpp/native/` and keep Python glue in `cpp/bindings/` only.
- Prove a Python-object boundary is unavoidable before letting any `py::object`
  or similar Python runtime type participate in C++ code, and confine that
  boundary to `cpp/bindings/`.

## Don't

- Do not edit `source_code_v.0.15.0/`.
- Do not normalize, simplify, or "improve" upstream behavior during a parity
  port.
- Do not put Python objects, Python attribute dispatch, or Python C-API-driven
  semantics in C++ implementation code unless no practical native alternative
  can be demonstrated.
- Do not present mixed pybind-heavy code as pure-native C++.
- Do not add new semantic logic to a binding-heavy file just because the file
  already exists.
- Do not count a surface as complete if the public path is still blocked by the
  runtime.
- Do not treat mocked store interactions as proof of real store parity.
- Do not preserve an accidental wrapper bug just because tests were missing.
- Do not trust a stale extension binary after editing `cpp/`.
- Do not claim full-suite green if the verified lane excluded known blockers.
- Do not leave repo docs overstating coverage or verification.
