# AI Agent Skills

Repo-local reusable skills live under `.agents/skills/`.

## Available skills

- `context-budget`: keep context narrow and high-signal
- `search-first`: search repo and upstream sources before adding code or tools
- `web-discovery`: run bounded current public-web discovery before relying on memory
- `site-extract`: extract structured facts from a known public page with minimal noise
- `browser-fallback`: use a deterministic browser workflow only when direct fetch is not enough
- `source-audit`: separate confirmed evidence from inference before answering
- `compliance-and-rate-limit`: keep web collection bounded, cache-aware, and non-evasive
- `verification-loop`: choose the smallest correct verification lane and report
  it accurately
- `ai-regression-testing`: add narrow differential or contract tests that catch
  common incomplete-agent regressions
- `python-testing`: pick Python-side parity and regression tests
- `tdd-workflow`: drive features and fixes with tests first or tests alongside
- `cpp-parity-porting`: port a single upstream surface to standalone C++
- `cpp-performance-optimization`: optimize parity-proven native hot
  paths for real C++ performance gains without changing behavior
- `benchmark-first`: benchmark before claiming native C++ is faster; report
  Python time, native C++ time, time saved, and time reduction first; any
  ratio must be labeled as native C++ speedup over Python
  (`python_time / native_cpp_time`); native C++ claims require standalone
  native executable/library timing
- `immutable-parity-proof`: verify the frozen snapshot manifest, use
  content-addressed parity evidence, and keep embedded-Python debt out of
  native-conversion counts
- `workflow-supply-chain-maintenance`: audit GitHub Actions, CodeQL, and
  Dependabot configuration against official current sources before changing
  automation or version pins

Use the nearest matching skill before inventing a workflow from scratch. For
pushes made by an AI agent, `verification-loop` includes waiting for the remote
workflow set on the pushed commit, and it assumes exact failing run logs are
inspected before a CI fix is attempted.

The expected balance is:

- use `context-budget` to avoid bulk-loading irrelevant files
- use `search-first` and `source-audit` to replace guessing with targeted evidence
- use `verification-loop` before claiming completion

Selective loading is required, but it must never be used as an excuse to make
assumptions that a slightly broader targeted read would have resolved.
