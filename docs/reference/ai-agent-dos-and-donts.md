# Maintainer Operating Rules

Critical: do not spawn, delegate to, or coordinate with multiple AI agents,
subagents, or separate agent sessions. Work in one session only unless the user
explicitly revokes this rule in a later instruction. If any AI-agent work is
already in progress, let it finish fully, then harvest and merge its outputs
without losing changes before continuing in one session.

Keep public-facing repository material professional. Do not expose internal
reasoning, private discussions, transient process notes.

This file records the operating rules that were reinforced by the current
porting work.

## Priority order

1. Preserve complete parity and identical user-visible functionality with the
   frozen Python upstream.
2. After parity is proven, maximize real performance gains from the native C++
   implementation.

## Do

- Read `docs/reference/architecture-first-porting.md` before widening a port.
- Read the exact upstream implementation before touching C++ or tests.
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
- Keep the frozen Python oracle and the standalone C++ product path clearly
  separated in docs.
- Treat GitHub `main` as the shared source of truth and reconcile local state
  against `origin/main` before trusting it.
- Prefer `--no-build-isolation` in offline or sandboxed rebuilds once local
  build dependencies are already installed.
- Treat existing mixed C++ files as debt. New work should move semantics toward
  `cpp/native/` and standalone native tools only.
- Prove a Python-object boundary is unavoidable before letting any `py::object`
  or similar Python runtime type participate in C++ code. No active
  Python-object C++ boundary is allowed in current `main`.
- Prefer newer stable dependency versions when they enable better native
  implementation techniques or measurable C++ performance gains, but only keep
  the upgrade after rerunning parity and benchmark validation on the upgraded
  stack.
- Use the standalone native build and native benchmark tools when you need to
  measure pure-native semantic cost.
- Keep standalone native CLI surfaces limited to real runtime commands or
  durable product APIs.
- Put optional external interop in `cpp/api/` as a thin C ABI over native
  semantics. Use raw buffers and JSON strings, not Python objects or pybind.
- Test ABI array surfaces with real contiguous NumPy memory through `ctypes`
  or equivalent FFI, and test store/path surfaces against real Zarr-created
  stores when possible.
- Once a standalone-native runtime surface exists and parity is proven, remove
  any corresponding binding/runtime scaffolding instead of adding fresh
  semantics there.
- When a standalone-native runtime replacement already exists, delete pybind
  registrations, obsolete runtime tests, and stale benchmark references in the
  same slice unless a remaining oracle-only dependency is still active and can
  be stated precisely.
- If a replaced surface can be verified and benchmarked through the frozen
  Python oracle plus standalone-native probe/bench tools, remove old pybind or
  Python package surfaces instead of leaving them behind as glue.
- Use Ruff only for Python or Markdown-like files. If linting a subset, pass
  those paths explicitly and keep `cpp/` out of the Ruff target list.
- Start with the smallest sufficient context slice and broaden only when the
  evidence says the first pass is not enough.
- Do run a smart consistency sweep before pushing so docs, file names, function
  names, benchmark claims, and structural references still match the actual
  repo state.
- Report pure-native benchmark results in time terms first: Python time,
  native C++ time, time saved, native C++ time reduction, and native C++
  speedup over Python (`python_time / native_cpp_time`). A ratio above `1.0x`
  means native C++ is faster; below `1.0x` means native C++ is slower. Do not
  use shorthand labels that omit the ratio direction, and do not invert slower
  cases into larger "slower" multipliers; report the direct speedup ratio, for
  example `0.748x`.
- Label only standalone native C++ executable/library timings as native C++.
  Do not publish Python package-path converted timings.

## Don't

- Do not edit `source_code_v.0.15.0/`.
- Do not normalize, simplify, or "improve" upstream behavior during a parity
  port.
- Do not put Python objects, Python attribute dispatch, or Python C-API-driven
  semantics in C++ implementation code unless no practical native alternative
  can be demonstrated.
- Do not confuse a C ABI with a Python adapter. The ABI may be loaded by
  Python libraries, but it must remain object-free and native on the C++ side.
- Do not present mixed pybind-heavy code as pure-native C++.
- Do not present any Python package layout as the intended final product. The
  target delivery shape is standalone C++.
- Do not add new semantic logic to a binding-heavy file just because the file
  already exists.
- Do not ship plan-only or helper-only standalone CLI commands as if they were
  part of the final product.
- Do not count a surface as complete if the public path is still blocked by the
  runtime.
- Do not treat mocked store interactions as proof of real store parity.
- Do not preserve an accidental helper bug just because tests were missing.
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
- Do not describe a slower C++ result as a performance gain or invert it into
  a larger "slower" multiplier. Keep the direct speedup ratio and show the
  actual Python and native C++ times.
- Do not call a Python package-path timing a C++ timing just because it lives
  next to C++ parity work.
- Do not spawn, delegate to, or coordinate with multiple AI agents, subagents,
  or separate agent sessions unless the critical single-session rule is
  explicitly revoked by the user in a later instruction.
- Do not expose internal reasoning, private discussions, transient process
  notes in
  user-facing docs, release notes, examples, or PR/commit text.
