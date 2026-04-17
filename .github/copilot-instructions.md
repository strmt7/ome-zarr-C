# GitHub Copilot Instructions

Use [AGENTS.md](../AGENTS.md) as the universal project contract.

## Core rules

- Never edit `source_code_v.0.15.0/`.
- Keep ports incremental and file-by-file.
- Priority order:
  1. Match the frozen Python upstream exactly.
  2. Maximize measured performance only after parity is proven.
- Newer stable dependency versions are allowed when they help performance or
  implementation quality, but they must be requalified with parity tests and
  benchmark validation before they are accepted.
- For the standalone native path, use
  `docs/reference/native-dependency-manifest.json` as the source of truth for
  pinned latest tool and library versions instead of whatever older host
  packages happen to be installed.
- Treat `origin/main` as the shared source of truth for repo state.
- Python objects are not allowed in C++ semantic code. No active binding layer
  remains in current `main`; do not reintroduce one without explicit approval
  and a documented unavoidable parity need.
- Optional external interoperability belongs in `cpp/api/` as a C ABI over
  native semantics. It may expose raw buffers and JSON strings for external
  FFI consumers, but it must not use Python objects, CPython headers, pybind,
  embedded interpreter calls, or package-specific Python adapter code.
- Do not expose plan-only or helper-only commands through the shipped
  standalone native CLI. Public native entrypoints must be real runtime
  commands or durable product APIs.
- Once a standalone-native runtime surface exists and parity is proven, delete
  corresponding repo-maintained Python package scaffolding instead of keeping
  duplicate product paths alive.
- For runtime surfaces already replaced by standalone native code, remove
  pybind registrations and stale test/benchmark references in the same slice
  unless a remaining frozen-upstream oracle dependency is explicitly justified
  and still exercised.
- If parity and benchmark coverage can be preserved with the frozen Python
  oracle plus standalone-native probe/bench tooling, do not retain a pybind or
  Python package layer for that surface.
- Ruff is for Python files only. Never target `cpp/` or C/C++ files with Ruff; use
  native checks, compiler/test validation, and C++-appropriate tooling
  instead.
- Before pushing, perform a repo-consistency sweep over the touched code,
  tests, scripts, and docs so stale names, outdated references, and contract
  drift do not get published just because tests passed.
- Load context selectively: start with the smallest sufficient set of docs,
  code, tests, and skills, then widen only when evidence says it is necessary.
- Differential tests are required for ported surfaces.
- C ABI array interop must be tested with real contiguous NumPy memory through
  `ctypes` or an equivalent external FFI consumer, while keeping the C++ side
  object-free and native.
- Benchmarks are required before claiming that native C++ is faster. Report
  pure-native outcomes in time terms first: Python time, native C++ time, time
  saved per operation, and native C++ time reduction. If a ratio is included,
  label it as native C++ speedup over Python
  (`python_time / native_cpp_time`). Do not use shorthand labels that omit the
  ratio direction, and do not invert slower cases into larger "slower"
  multipliers; report the direct speedup ratio, for example `0.748x`.
- Never label Python package-path timings as C++ or native C++. Pure-native
  performance claims require standalone native C++ executable/library timing.
- Use repo-local skills under `.agents/skills/` when they fit the task.
- Keep frozen snapshots under `source_code_v*/` excluded from security scanning,
  and update the exclusion whenever a new snapshot is added.
- Treat `tests/` as first-class repo code; fix findings there instead of
  suppressing them because they are test-only.
- Keep `README.md` user-facing. Put AI-agent operating rules in `AGENTS.md`,
  `.github/instructions/`, and repo-local skills.

## Load order

1. `AGENTS.md`
2. `docs/reference/ai-agent-context-routing.md`
3. `docs/reference/porting-contract.md`
4. `docs/reference/ai-agent-skills.md`
5. the nearest implementation and test files
