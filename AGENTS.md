# AGENTS guide

This repository incrementally ports the frozen `ome/ome-zarr-py` `v0.15.0`
snapshot to C++ without modifying the snapshot itself.

## Mandatory rules

1. Never edit `source_code_v.0.15.0/`.
2. Every converted surface must be checked against the frozen upstream code.
3. Preserve upstream exceptions, edge cases, and quirks unless an intentional
   divergence is explicitly documented.
4. Do not claim performance gains without benchmark data.
5. Use repo-local tooling in `.venv/` unless there is a hard blocker.
6. Use latest stable tool versions unless the repo pins something stricter.
7. Do not use background agents or subagents unless the user explicitly asks.
8. Before making any claim about branches, default branch, remotes, tags, or
   GitHub repo state, verify both local Git state and remote GitHub state.

## Fast load order

1. `docs/reference/ai-agent-context-routing.md`
2. `docs/reference/porting-contract.md`
3. `docs/reference/ai-agent-skills.md`
4. the touched upstream implementation file under `source_code_v.0.15.0/`
5. the matching wrapper, C++ file, and nearest test module

## Repository map

- `source_code_v.0.15.0/`: immutable upstream reference snapshot
- `cpp/`: C++ implementations
- `omero_zarr_c/`: Python compatibility wrappers
- `tests/`: parity and regression tests
- `docs/`: repo rules, routing, and benchmarks
- `.agents/skills/`: reusable repo-local workflows

## Verification minimum

```bash
.venv/bin/python -m pip install -e .[dev]
.venv/bin/python -m pytest tests/test_conversions_equivalence.py -q
.venv/bin/python -m pytest tests/test_axes_equivalence.py -q
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
```

Run only the narrowest relevant suites while iterating, but report exactly what
was verified.
