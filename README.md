# ome-zarr-C

`ome-zarr-C` is a release-anchored C++/pybind11 conversion workspace for
[`ome/ome-zarr-py`](https://github.com/ome/ome-zarr-py).

The exact upstream `v0.15.0` release snapshot is preserved under
`source_code_v.0.15.0/`. That directory is immutable reference code and must
never be edited directly.

## Scope

- port selected upstream Python modules to C++ incrementally
- preserve upstream behavior first, including documented quirks
- prove parity with differential tests against the frozen upstream snapshot
- benchmark converted components before claiming any performance win
- keep all new code outside `source_code_v.0.15.0/`

## Upstream Provenance

- Original repository: `ome/ome-zarr-py`
- Imported release: `v0.15.0`
- Imported release commit: `cade24e`
- Upstream license: BSD 2-Clause, preserved under `source_code_v.0.15.0/LICENSE`

## Repository Layout

- `source_code_v.0.15.0/`: frozen upstream release snapshot
- `cpp/`: C++ implementation files exposed via `pybind11`
- `ome_zarr_c/`: thin Python wrappers around native code
- `tests/`: differential and regression tests against the frozen snapshot
- `docs/`: project rules, routing, and porting contract
- `.agents/skills/`: reusable repo-local agent workflows

## Frozen Snapshot Policy

- Never edit files inside `source_code_v.0.15.0/`
- All new work must happen outside the frozen snapshot
- CI rejects direct modifications to the frozen snapshot on the protected branch
- If upstream behavior needs to change, change the wrapper/port and document the
  intentional divergence

## Current Status

The current native parity slices are:

- `ome_zarr_c.conversions` for integer/RGBA conversion helpers
- `ome_zarr_c.axes` for axes normalization and validation
- `ome_zarr_c.csv` for `parse_csv_value`, `dict_to_zarr`, and `csv_to_zarr`
- `ome_zarr_c.utils` for `strip_common_prefix`, `splitall`, and `find_multiscales`
- `ome_zarr_c.format` for version dispatch, metadata version lookup, well
  validation, and coordinate transformation generation/validation

Each converted surface is checked with differential tests against the frozen
upstream implementation.

## Local Development

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e .[dev]
.venv/bin/python -m pytest -q
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
```

## Rules For New Ports

1. Read the upstream implementation from `source_code_v.0.15.0/`.
2. Port the smallest self-contained unit first.
3. Keep the Python wrapper thin unless Python-level compatibility requires
   otherwise.
4. Add differential tests before claiming parity.
5. Benchmark with representative data before claiming speedups.

See `AGENTS.md` and `docs/reference/porting-contract.md` for the full working
contract.
