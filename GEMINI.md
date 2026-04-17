# Repository Instructions

Critical: do not spawn, delegate to, or coordinate with multiple AI agents,
subagents, or separate agent sessions. Work in one session only unless the user
explicitly revokes this rule in a later instruction.

Keep public-facing repository material professional. Do not expose internal
reasoning, private discussions, transient process notes.

Use `AGENTS.md` as the universal project contract and
`docs/reference/ai-agent-context-routing.md` to keep context narrow.

## Core rules

- Never modify `source_code_v.0.15.0/`.
- Prefer small, file-by-file ports with differential parity tests.
- Keep C ABI wrappers thin and C++ cores behavior-focused.
- Do not add Python-object, pybind, or embedded-interpreter semantics to C++.
- Use `.agents/skills/` when they fit the task.
- Do not claim speed or parity beyond what tests and benchmarks prove.

## Verification

Use the verification flow in `AGENTS.md` and
`docs/reference/ai-agent-skills.md`.
