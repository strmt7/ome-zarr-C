# Split-Native Coverage

This repository retains a conservative `split-native` percentage for historical
comparison against the frozen upstream Python snapshot.

## What it means

- Count only upstream surfaces that have been split away from legacy mixed
  exports and have corresponding native implementation files.
- Current manifest entries no longer require binding files. Empty
  `binding_files` arrays mean the active binding layer has been removed.
- Require corresponding implementation files under `cpp/native/`.
- Use the frozen upstream source in `source_code_v.0.15.0/ome_zarr/` as the
  immutable line-count basis.

This metric is stricter than the old `native-backed` percentage, but still not
the primary `pure-native` claim. Use the pure-native report for current C++
semantic ownership.

## Report command

```bash
.venv/bin/python scripts/report_split_native_coverage.py
.venv/bin/python scripts/report_split_native_coverage.py --fail-under 25
```

The counted surfaces live in:

- `docs/reference/split-native-coverage-manifest.json`

The report script fails if:

- a manifest entry names an upstream function that no longer exists
- a listed native implementation file is missing
- the computed percentage drops below a requested threshold
