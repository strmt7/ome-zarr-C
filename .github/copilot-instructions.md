# GitHub Copilot Instructions

Use [AGENTS.md](../AGENTS.md) as the universal project contract.

## Core rules

- Never edit `source_code_v.0.15.0/`.
- Keep ports incremental and file-by-file.
- Match upstream behavior before optimizing.
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
