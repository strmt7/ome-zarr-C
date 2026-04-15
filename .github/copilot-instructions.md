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
- Treat `origin/main` as the shared source of truth for repo state.
- Python objects are not allowed in C++ semantic code unless the boundary is
  proven unavoidable and isolated to minimal binding glue.
- Do not expose plan-only or helper-only commands through the shipped
  standalone native CLI. Public native entrypoints must be real runtime
  commands or durable product APIs.
- Once a standalone-native runtime surface exists and parity is proven, treat
  the corresponding Python-visible binding path as shrink-only debt. Remove
  redundant `setup.py` sources and direct `cpp/bindings/` dependencies on
  standalone runtime modules instead of keeping duplicate product paths alive.
- For runtime surfaces already replaced by standalone native code, remove the
  matching wrapper exports, pybind registrations, and stale test/benchmark
  references in the same slice unless a remaining oracle-only dependency is
  explicitly justified and still exercised.
- If parity and benchmark coverage can be preserved with the Python oracle plus
  standalone-native probe/bench tooling, do not retain a pybind or Python
  wrapper layer for that surface.
- Ruff is Python-only. Never target `cpp/` or C/C++ files with Ruff; use
  native checks, compiler/test validation, and C++-appropriate tooling
  instead.
- Before pushing, perform a repo-consistency sweep over the touched code,
  tests, scripts, and docs so stale names, outdated references, and contract
  drift do not get published just because tests passed.
- Load context selectively: start with the smallest sufficient set of docs,
  code, tests, and skills, then widen only when evidence says it is necessary.
- Differential tests are required for ported surfaces.
- Benchmarks are required before claiming speedups.
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
