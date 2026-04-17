# Automation Adapter Map

`AGENTS.md` is the universal baseline for this repository.

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
