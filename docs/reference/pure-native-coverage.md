# Pure-Native Coverage

This repository tracks a stricter `pure-native` percentage alongside the older
`split-native` architecture metric.

## What it means

- Count only upstream surfaces whose decision logic and behavioral semantics
  live in `cpp/native/`.
- Count non-callable scaffold lines only when they are listed explicitly in the
  manifest for a thin boundary module whose callable semantics are already
  native-owned.
- Allow `cpp/bindings/` or thin Python wrappers only for boundary marshalling,
  object construction, or library calls that sit at the repository boundary.
- Do not count mixed logic still left in `cpp/core.cpp`.
- Use the frozen upstream source in `source_code_v.0.15.0/ome_zarr/` as the
  immutable line-count basis.

This metric is stricter than `split-native`. It should stay conservative.
If there is doubt about whether a surface is still partially owned by Python
logic, leave it out of the pure-native manifest.

Because the denominator is whole-file upstream lines, a callable-only report has
an absolute ceiling: only `3411 / 4180 = 81.60%` of the frozen snapshot lives
inside executable Python function or method bodies once abstract contract stubs
are excluded. Any honest whole-file line percentage above that must count
non-callable scaffold explicitly. This repository does that only through
`entry_type: "non_callable_scaffold"` manifest entries, so those lines are
visible and auditable rather than hidden in the report logic.

Non-callable scaffold means lines outside any upstream function or method body:
module docstrings, imports, class headers, constants, `__all__`, and similar
top-level structure. It does not include unported callable behavior.

## Report command

```bash
.venv/bin/python scripts/report_pure_native_coverage.py
```

The counted surfaces live in:

- `docs/reference/pure-native-coverage-manifest.json`

The report script fails if:

- a manifest entry names an upstream function that no longer exists
- a manifest entry names an unsupported entry type
- a listed native or boundary implementation file is missing
- the computed percentage drops below a requested threshold

## Current committed snapshot

At the current working commit, the strict pure-native report is:

- `4180 / 4180 = 100.000000%`

That figure includes:

- callable surfaces whose semantics live in `cpp/native/`
- explicitly listed `non_callable_scaffold` entries for thin boundary modules
- abstract contract stubs excluded from callable accounting and covered only
  through visible scaffold entries

It does not include hidden automatic scaffold counting, mixed `cpp/core.cpp`
logic, or unlisted Python-owned behavior.
