# GitHub Copilot Instructions

Use [AGENTS.md](../AGENTS.md) as the universal project contract.

## Core rules

- Never edit `source_code_v.0.15.0/`.
- Keep ports incremental and file-by-file.
- Match upstream behavior before optimizing.
- Differential tests are required for ported surfaces.
- Benchmarks are required before claiming speedups.
- Use repo-local skills under `.agents/skills/` when they fit the task.

## Load order

1. `AGENTS.md`
2. `docs/reference/ai-agent-context-routing.md`
3. `docs/reference/porting-contract.md`
4. `docs/reference/ai-agent-skills.md`
5. the nearest implementation and test files
