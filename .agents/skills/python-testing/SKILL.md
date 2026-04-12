---
name: python-testing
description: Choose Python-side parity tests, wrapper tests, and syntax checks without weakening coverage.
origin: adapted from ZMB-UZH/omero-docker-extended
---

# Python Testing

Use this skill when the touched surface includes Python wrappers or Python-only
compatibility logic.

## Rules

- Prefer narrow parity tests against the frozen upstream implementation.
- Keep suites separated by converted surface.
- Never weaken assertions just to make the port look green.
