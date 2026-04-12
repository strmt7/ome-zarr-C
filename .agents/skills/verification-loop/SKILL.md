---
name: verification-loop
description: Run the smallest correct verification lane and report it precisely.
origin: adapted from ZMB-UZH/omero-docker-extended
---

# Verification Loop

Use this skill after any non-trivial change.

## Verification order

1. Relevant parity suites under `tests/`
2. `ruff check .`
3. `ruff format --check .`
4. any module-specific build or benchmark check required by the touched files
   and, after native-code changes, rebuild the editable install first
5. after an AI-agent push, wait for the GitHub workflows on that pushed commit
   and fix failures before considering the change finished
6. if workflows, CodeQL scope, or pinned tool versions changed, load
   `workflow-supply-chain-maintenance` and verify the exact scanner config or
   version source that justified the edit
7. when tests add shared helpers, ensure they import cleanly in both focused
   runs and whole-suite collection before treating the lane as green

## Reporting rule

State exactly which suites ran. Do not imply whole-repo parity when only one
ported surface was checked. After an AI-agent push, state whether the remote
workflow set is green for that exact commit. If remote CI fails, inspect the
exact failing run logs for that repo before changing code.
