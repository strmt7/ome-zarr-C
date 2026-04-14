# Split-Native Coverage

This repository tracks a conservative `split-native` percentage for the frozen
upstream Python snapshot.

## What it means

- Count only upstream surfaces that are routed through dedicated
  `cpp/bindings/` entrypoints, or thin Python orchestration that depends only
  on split binding/native surfaces, instead of mixed exports left in legacy
  files outside those roots.
- Require corresponding implementation files under `cpp/bindings/` and
  `cpp/native/`.
- Use the frozen upstream source in `source_code_v.0.15.0/ome_zarr/` as the
  immutable line-count basis.

This metric is stricter than the old `native-backed` percentage, but still not
the final `pure-native` end state the repository is targeting. It is the
current architecture-first floor for measurable progress after a surface has
been split away from mixed-core debt.

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
