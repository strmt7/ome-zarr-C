---
name: benchmark-first
description: Benchmark converted code against the frozen upstream implementation before making performance claims.
origin: repo-local
---

# Benchmark First

Use this skill after parity has been demonstrated for a converted surface.

## Rules

- Compare the upstream implementation and the converted implementation on the
  same inputs and same machine class.
- Prefer representative real data. If real data is unavailable, document the
  synthetic fallback clearly.
- Run repeated measurements and report medians.
- Report version and environment details with the results.
