# Pure-Native Coverage

This repository tracks a stricter `pure-native` percentage alongside the older
`split-native` architecture metric.

## What it means

- Count only upstream surfaces whose decision logic and behavioral semantics
  live in `cpp/native/`.
- Allow `cpp/bindings/` or thin Python wrappers only for boundary marshalling,
  object construction, or library calls that sit at the repository boundary.
- Do not count mixed logic still left in `cpp/core.cpp`.
- Use the frozen upstream source in `source_code_v.0.15.0/ome_zarr/` as the
  immutable line-count basis.

This metric is stricter than `split-native`. It should stay conservative.
If there is doubt about whether a surface is still partially owned by Python
logic, leave it out of the pure-native manifest.

## Report command

```bash
.venv/bin/python scripts/report_pure_native_coverage.py
```

The counted surfaces live in:

- `docs/reference/pure-native-coverage-manifest.json`

The report script fails if:

- a manifest entry names an upstream function that no longer exists
- a listed native or boundary implementation file is missing
- the computed percentage drops below a requested threshold
