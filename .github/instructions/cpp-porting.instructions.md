# C++ Porting Instructions

- Priority order:
  1. Preserve exact parity with the frozen Python upstream.
  2. Maximize measured performance only after parity is already proven.
- Do not modify the frozen upstream snapshot under `source_code_v.0.15.0/`.
- Port one small upstream surface at a time.
- Keep Python wrappers as thin transitional compatibility/oracle layers only.
- Keep real semantics in `cpp/native/` and standalone entrypoints in
  `cpp/tools/`.
- The target shipped product is standalone C++, not a Python adapter. No active
  binding layer remains in current `main`; do not reintroduce one without
  explicit approval and a documented unavoidable parity need.
- Once a standalone-native runtime surface exists and parity is proven, treat
  the matching Python-harness path as shrink-only debt. Do not add new
  semantics there; reduce or delete it in later slices.
- When a standalone-native runtime replacement already exists, prefer deleting
  the matching wrapper/binding/runtime scaffolding in the same slice rather
  than leaving dormant compatibility code behind. Keep only the smallest
  oracle-only residue that is still actively verified and explain it
  concretely if it cannot be removed yet.
- If tests or benchmarks for a replaced surface can run through the Python
  oracle plus standalone-native probe/bench tooling, rewire them and delete
  the pybind/Python wrapper path instead of preserving it as benchmark glue.
- Do not ship plan-only or helper-only standalone CLI commands. Public native
  CLI surfaces should correspond to real runtime commands or durable product
  APIs that are intended to survive the migration.
- Prefer newer stable dependency versions when they unlock stronger native
  techniques or performance gains, but treat the upgrade as provisional until
  parity and benchmark validation are rerun successfully on that exact stack.
- For the standalone native path, treat
  `docs/reference/native-dependency-manifest.json` as the source of truth for
  the latest pinned tool and library versions. Do not quietly rely on older
  host packages when the manifest specifies newer native releases.
- Do not put Python objects, Python attribute dispatch, or Python C-API-driven
  semantics into C++ implementation code unless the boundary can be proven
  unavoidable for the Python-visible contract.
- Verify the frozen snapshot manifest before claiming upstream immutability.
- Preserve upstream exceptions and edge-case behavior unless an intentional
  divergence is explicitly documented.
- If upstream behavior depends on interpreter-generated exception text, use the
  live Python runtime to produce that behavior instead of freezing a message
  literal in C++.
- Do not use pybind truthiness shortcuts such as `py::cast<bool>` to replace
  real native parity work. Match Python `if obj:` semantics natively or keep
  the surface in the development oracle until it can be ported cleanly.
- For functions that mutate files or stores, compare the resulting serialized
  on-disk state against upstream instead of only checking return values.
- For functions that cross external side-effect boundaries, patch the boundary
  and compare the outbound call payloads against upstream instead of triggering
  the real side effect.
- Do not normalize or "improve" upstream behavior for convenience. Parity means
  preserving the same behavior unless a divergence is explicitly documented.
- Do not introduce embedded-Python execution patterns such as `py::exec`,
  `py::eval`, or raw Python source blocks into `cpp/`. If such debt already
  exists, do not count the affected surface as fully native-converted coverage.
- Do not add new Python-integrated semantics to C++ files. Existing mixed files
  are debt and should be deleted or replaced, not expanded.
- If historical embedded-Python debt is encountered, remove it or quarantine it
  to the smallest possible boundary instead of extending it.
- After changing native code or package build surfaces, rebuild the native
  targets or editable install before rerunning parity tests.
- When working on pure-native performance, also use the standalone native build
  and native benchmark tooling so Python-boundary overhead does not get
  confused with core semantic cost.
- When a change affects `cpp/native/`, `cpp/tools/`, or `CMakeLists.txt`,
  rebuild the standalone native targets and rerun the native self-test before
  claiming the native path is healthy.
- Ruff is for Python files only in this repo. Never run Ruff against `cpp/` or
  any C/C++ file extension. If linting a subset, pass only Python or
  Markdown-like paths explicitly.
- Before pushing, scan the touched implementation, tests, scripts, and docs for
  stale or conflicting references such as removed file names, mismatched
  function names, outdated benchmark/coverage claims, and contract drift
  between code and documentation.
- Add differential tests against the frozen snapshot before claiming parity.
- Benchmark before claiming any performance improvement.
