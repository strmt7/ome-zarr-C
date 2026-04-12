# AI Agent Skills

Repo-local reusable skills live under `.agents/skills/`.

## Available skills

- `context-budget`: keep context narrow and high-signal
- `search-first`: search repo and upstream sources before adding code or tools
- `verification-loop`: choose the smallest correct verification lane and report
  it accurately
- `ai-regression-testing`: add narrow differential or contract tests that catch
  common incomplete-agent regressions
- `python-testing`: pick Python-side parity and regression tests
- `tdd-workflow`: drive features and fixes with tests first or tests alongside
- `cpp-parity-porting`: port a single upstream surface to C++/pybind11
- `benchmark-first`: benchmark before claiming speedups

Use the nearest matching skill before inventing a workflow from scratch. For
pushes made by an AI agent, `verification-loop` includes waiting for the remote
workflow set on the pushed commit.
