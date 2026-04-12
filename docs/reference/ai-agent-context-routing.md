# AI Agent Context Routing

Use the smallest correct context for the task. Do not load the whole repo by
default.

## First-pass cap

- Open at most 1 doc, 1 upstream implementation file, 1 local implementation
  file, 1 nearest test file, and 1 matching skill before broadening scope.
- If the edit target and verification lane are still unclear after that first
  pass, summarize and add at most 3 more files.

## Task routes

### Porting one upstream module

1. Read `docs/reference/porting-contract.md`.
2. Read the upstream file under `source_code_v.0.15.0/`.
3. Read the matching local wrapper or `cpp/` file.
4. Read the nearest parity test.
5. Run only the relevant differential test lanes.

### Workflow or tooling changes

1. Read `AGENTS.md`.
2. Read the touched workflow or instruction file.
3. Read the nearest verification skill.
4. Run the exact workflow-relevant local checks.
5. If the task mentions branches, tags, default branch, or repo settings,
   verify both local Git state and the remote GitHub page or API before stating
   facts.

### Documentation-only changes

1. Read `AGENTS.md`.
2. Read the touched doc.
3. Read `docs/reference/ai-agent-integrations.md` if the change affects agent
   surfaces.

### Benchmarking

1. Read `docs/reference/porting-contract.md`.
2. Read `.agents/skills/benchmark-first/SKILL.md`.
3. Compare upstream and converted code on identical inputs and environment.

### External research

1. Search this repository first.
2. Check the frozen upstream snapshot and upstream release/tag docs.
3. Check official upstream or project repositories.
4. Use broader web/forum sources only after that.
