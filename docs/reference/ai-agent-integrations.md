# Automation Adapter Map

`AGENTS.md` is the universal baseline for this repository. Its pinned
Karpathy agent baseline is centralized there so adapters inherit the same
four-principle behavior without duplicating prompt text. It is adapted from
`forrestchang/andrej-karpathy-skills` at
`2c606141936f1eeef17fa3043a72095b4765b9c2`.

## Adapter map

- Tool-specific adapter: `CLAUDE.md`
- Tool-specific adapter: `GEMINI.md`
- Editor-assist adapter: `.github/copilot-instructions.md`
- Cursor: `.cursor/rules/00-porting-core.mdc`
- Repo-local reusable workflows: `.agents/skills/`

## Rule

Adapters may add harness-specific reminders, but they must not contradict
`AGENTS.md` or the frozen snapshot policy. Adapter content is operational
configuration, not user-facing product documentation.
