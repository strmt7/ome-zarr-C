# Public Benchmark Fixtures

The `realdata` benchmark suite uses only public fixtures with non-restrictive
licenses. The fixture downloader caches archives and extracted datasets under
`.benchmarks-fixtures/` by default. Override that location with
`OME_ZARR_BENCH_FIXTURE_ROOT=/abs/path/to/cache`.

These downloads are benchmark inputs, not repository source files, so the cache
directory stays gitignored.

## Validation Use

Use fixture verification before timing or real-data claims:

```bash
.venv/bin/python -m benchmarks.run --suite realdata --verify-only
```

The verification hook downloads the listed public fixtures when needed, writes
a local `fixture.json` manifest, and runs the frozen Python oracle and native
path against the same extracted dataset path. Current real-data checks cover the
documented `parse_url` / `info` surfaces only.

Real-data evidence must name the dataset ID, source URL, license, command, and
whether the optional large fixture was enabled. A successful fixture check does
not prove unrelated reader, writer, mutation, network, or GPU behavior. GPU
claims also require separate device evidence from `scripts/gpu_capability_probe.py`
and measured GPU-path timings.

## Default Fixtures

- `examples_image`
  - source repository: <https://github.com/BioImageTools/ome-zarr-examples>
  - fixed commit:
    `8c10c88fbb77c3dcc206d9b234431f243beee576`
  - archive URL:
    <https://codeload.github.com/BioImageTools/ome-zarr-examples/zip/8c10c88fbb77c3dcc206d9b234431f243beee576>
  - extracted dataset path:
    `data/valid/image-03.zarr`
  - license: `BSD-3-Clause`
  - purpose: small valid image fixture for end-to-end `parse_url` / `info` /
    reader timings

- `examples_plate`
  - source repository: <https://github.com/BioImageTools/ome-zarr-examples>
  - fixed commit:
    `8c10c88fbb77c3dcc206d9b234431f243beee576`
  - archive URL:
    <https://codeload.github.com/BioImageTools/ome-zarr-examples/zip/8c10c88fbb77c3dcc206d9b234431f243beee576>
  - extracted dataset path:
    `data/valid/plate-01.zarr`
  - license: `BSD-3-Clause`
  - purpose: small valid plate fixture for end-to-end `parse_url` / `info` /
    reader timings

- `bia_tonsil3`
  - source page:
    <https://uk1s3.embassy.ebi.ac.uk/bia-integrator-data/pages/S-BIAD800/f49ada41-43bf-47ff-99b9-bdf8cc311ce3.html>
  - archive URL:
    <https://ftp.ebi.ac.uk/biostudies/fire/S-BIAD/800/S-BIAD800/Files/idr0054/Tonsil%203.ome.zarr.zip>
  - license: `CC BY 4.0`
  - approximate size: `108.8 MiB`
  - purpose: medium-size public OME-NGFF fixture from BioImage Archive / IDR

## Optional Large Fixture

- `bia_156_42`
  - source page:
    <https://uk1s3.embassy.ebi.ac.uk/bia-integrator-data/pages/S-BIAD885/a1cfd35d-81c5-448a-a368-2350eca76c6f.html>
  - archive URL:
    <https://ftp.ebi.ac.uk/biostudies/fire/S-BIAD/885/S-BIAD885/Files/idr0010/156-42.ome.zarr.zip>
  - license: `CC BY 4.0`
  - approximate size: `455.3 MiB`
  - purpose: larger real-data benchmark for explicit opt-in runs

The large fixture is excluded from the default suite so routine verification
does not silently download a multi-hundred-megabyte archive. Enable it only
when you want that additional coverage:

```bash
OME_ZARR_BENCH_INCLUDE_LARGE=1 .venv/bin/python -m benchmarks.run \
  --suite realdata \
  --verify-only
```
