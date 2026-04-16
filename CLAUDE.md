# Claude Code instructions

Use `AGENTS.md` as the universal baseline. This file is only a thin adapter.

## Load order

1. `AGENTS.md`
2. `docs/reference/ai-agent-context-routing.md`
3. `docs/reference/porting-contract.md`
4. `docs/reference/ai-agent-skills.md`
5. the nearest implementation and test files

## Core rules

- Never edit `source_code_v.0.15.0/`.
- Keep C ABI wrappers thin and push converted logic into `cpp/native/`.
- Do not add Python-object, pybind, or embedded-interpreter semantics to C++.
- Match upstream behavior before attempting improvements.
- Differential tests are required for every ported module.
- Benchmark before claiming speedups.
- Use `.agents/skills/` when they match the task.

## Verification

Run the smallest correct subset from `AGENTS.md` and the verification skill, and
state the exact level achieved.
