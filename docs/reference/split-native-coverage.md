# Split-Native Coverage

This repository tracks a conservative `split-native` percentage for the frozen
upstream Python snapshot.

## What it means

- Count only upstream surfaces that are routed through dedicated
  `cpp/bindings/` entrypoints instead of mixed exports left in `cpp/core.cpp`.
- Require a corresponding native implementation file under `cpp/native/`.
- Use the frozen upstream source in `source_code_v.0.15.0/ome_zarr/` as the
  immutable line-count basis.

This metric is stricter than the old `native-backed` percentage, but still not
the final `pure-native` end state the repository is targeting. It is the
current architecture-first floor for "real" conversion progress.

## Report command

```bash
.venv/bin/python scripts/report_split_native_coverage.py
.venv/bin/python scripts/report_split_native_coverage.py --fail-under 25
```

The counted surfaces live in:

- `docs/reference/split-native-coverage-manifest.json`

The report script fails if:

- a manifest entry names an upstream function that no longer exists
- a listed binding/native implementation file is missing
- the computed percentage drops below a requested threshold
