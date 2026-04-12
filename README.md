# ome-zarr-C

`ome-zarr-C` is a release-anchored C++/pybind11 porting workspace for
[`ome/ome-zarr-py`](https://github.com/ome/ome-zarr-py).

The project preserves the exact upstream `v0.15.0` release snapshot under
`source_code_v.0.15.0/` and implements converted functionality outside that
snapshot in native-backed modules. The working rule for converted code is
behavioral parity first, performance claims second.

## Goals

- port upstream Python surfaces incrementally to C++ with `pybind11`
- preserve upstream public behavior, including edge cases and observable quirks
- prove parity with differential tests against the frozen release snapshot
- benchmark converted surfaces with representative data before claiming gains

## Upstream Provenance

- Original repository: `ome/ome-zarr-py`
- Imported release: `v0.15.0`
- Imported release commit: `cade24e`
- Upstream license: BSD 2-Clause

The upstream snapshot is kept as immutable reference material in
`source_code_v.0.15.0/`. All new implementation work happens outside that
directory.

## Repository Layout

- `source_code_v.0.15.0/`: frozen upstream release snapshot
- `cpp/`: C++ implementations exposed through `pybind11`
- `ome_zarr_c/`: Python compatibility layer for converted surfaces
- `tests/`: differential and regression tests
- `docs/`: project references, design notes, and benchmark material

## Converted Surfaces

The current native-backed parity slices are:

- `ome_zarr_c.conversions`
  - `int_to_rgba`
  - `int_to_rgba_255`
  - `rgba_to_int`
- `ome_zarr_c.axes`
  - axes normalization
  - axis-name extraction
  - OME-Zarr axis validation logic
- `ome_zarr_c.format`
  - format dispatch
  - metadata version lookup
  - well-dict generation and validation
  - coordinate-transformation generation and validation
- `ome_zarr_c.csv`
  - `parse_csv_value`
  - `dict_to_zarr`
  - `csv_to_zarr`
- `ome_zarr_c.utils`
  - `strip_common_prefix`
  - `splitall`
  - `find_multiscales`
  - `finder`
  - `view`
  - `info`

Each converted surface is validated against the frozen upstream release with
parity tests under `tests/`.

## Local Development

Create a local environment and install the editable package:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e .[dev]
```

Run the full local verification lane:

```bash
.venv/bin/python -m pytest -q
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
```

After editing native code under `cpp/`, rebuild the editable install before
re-running tests so the suite exercises the current extension binary:

```bash
.venv/bin/python -m pip install -e .[dev]
```

## Porting Approach

Converted code is added in small, reviewable slices:

1. Read the exact upstream implementation from the frozen snapshot.
2. Port the smallest self-contained surface.
3. Add differential tests against the upstream implementation.
4. Verify local parity before widening the converted area.
5. Benchmark only after parity is established.

For read-only surfaces that print absolute paths, parity tests should run the
upstream and converted implementations against the same fixture path so the
comparison measures behavior rather than path differences.

## Security and Scan Scope

Repository-maintained code is scanned and tested. The frozen upstream snapshot
under `source_code_v.0.15.0/` is excluded from security scanning so alerts stay
focused on converted and maintained code in this repository.

## License

This repository contains upstream BSD 2-Clause licensed code preserved from
`ome/ome-zarr-py` together with new conversion work. See
`source_code_v.0.15.0/LICENSE` for the upstream license text.
