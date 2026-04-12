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
- `pybind11-runtime-parity`: preserve Python-visible behavior when C++ bindings
  call back into Python objects or expose iterator-style behavior
- `workflow-supply-chain-maintenance`: audit GitHub Actions, CodeQL, and
  Dependabot configuration against official current sources before changing
  automation or version pins

Use the nearest matching skill before inventing a workflow from scratch. For
pushes made by an AI agent, `verification-loop` includes waiting for the remote
workflow set on the pushed commit, and it assumes exact failing run logs are
inspected before a CI fix is attempted.
