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
- Keep the current transitional Python-harness state and the target standalone
  C++ product state clearly separated in docs.
- Treat GitHub `main` as the shared source of truth and reconcile local state
  against `origin/main` before trusting it.
- Prefer `--no-build-isolation` in offline or sandboxed rebuilds once local
  build dependencies are already installed.
- Treat existing mixed C++ files as debt. New work should move semantics toward
  `cpp/native/` and keep Python glue in `cpp/bindings/` only.
- Prove a Python-object boundary is unavoidable before letting any `py::object`
  or similar Python runtime type participate in C++ code, and confine that
  boundary to `cpp/bindings/`.
- Prefer newer stable dependency versions when they enable better native
  implementation techniques or measurable speedups, but only keep the upgrade
  after rerunning parity and benchmark validation on the upgraded stack.
- Use the standalone native build and native benchmark tools when you need to
  measure pure-native semantic cost separately from Python-visible boundary
  overhead.
- Keep standalone native CLI surfaces limited to real runtime commands or
  durable product APIs.
- Once a standalone-native runtime surface exists and parity is proven, treat
  the corresponding binding/runtime path as shrink-only debt and reduce it in
  later slices instead of adding fresh semantics there. Remove matching
  `setup.py` sources and direct `cpp/bindings/` dependencies on standalone
  runtime modules once they are no longer needed for oracle work.
- When a standalone-native runtime replacement already exists, delete the
  matching wrapper exports, pybind registrations, obsolete runtime tests, and
  stale benchmark references in the same slice unless a remaining oracle-only
  dependency is still active and can be stated precisely.
- If a replaced surface can be verified and benchmarked through the Python
  oracle plus standalone-native probe/bench tools, remove the old pybind or
  Python-wrapper surface instead of leaving it behind as compatibility glue.
- Use Ruff only for Python or Markdown-like files. If linting a subset, pass
  those paths explicitly and keep `cpp/` out of the Ruff target list.
- Start with the smallest sufficient context slice and broaden only when the
  evidence says the first pass is not enough.
- Do run a smart consistency sweep before pushing so docs, file names, function
  names, benchmark claims, and structural references still match the actual
  repo state.

## Don't

- Do not edit `source_code_v.0.15.0/`.
- Do not normalize, simplify, or "improve" upstream behavior during a parity
  port.
- Do not put Python objects, Python attribute dispatch, or Python C-API-driven
  semantics in C++ implementation code unless no practical native alternative
  can be demonstrated.
- Do not present mixed pybind-heavy code as pure-native C++.
- Do not present the transitional Python package layout as the intended final
  product. The target delivery shape is standalone C++.
- Do not add new semantic logic to a binding-heavy file just because the file
  already exists.
- Do not ship plan-only or helper-only standalone CLI commands as if they were
  part of the final product.
- Do not count a surface as complete if the public path is still blocked by the
  runtime.
- Do not treat mocked store interactions as proof of real store parity.
- Do not preserve an accidental wrapper bug just because tests were missing.
- Do not trust a stale extension binary after editing `cpp/`.
- Do not claim full-suite green if the verified lane excluded known blockers.
- Do not leave repo docs overstating coverage or verification.
- Do not freeze old dependency versions out of convenience if a newer stable
  version would help parity-safe performance work and can be fully requalified.
- Do not point Ruff at `cpp/` or any C/C++ file extension. Ruff is not a C++
  linter in this repo.
- Do not assume passing tests are enough to prove the repo is internally
  consistent. Scan for stale names, removed files still mentioned in docs, and
  other contract drift before pushing.
