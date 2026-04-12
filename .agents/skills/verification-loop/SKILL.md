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

## Reporting rule

State exactly which suites ran. Do not imply whole-repo parity when only one
ported surface was checked.
