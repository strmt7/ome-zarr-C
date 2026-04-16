from __future__ import annotations

import importlib
import random
import sys
import tarfile
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import dask.array as da
import zarr

ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = ROOT / "source_code_v.0.15.0"
ASSET_ROOT = ROOT / "cpp" / "assets" / "create"

sys.path.insert(0, str(SOURCE_ROOT))

py_cli = importlib.import_module("ome_zarr.cli")
py_writer = importlib.import_module("ome_zarr.writer")
REAL_TO_ZARR = da.to_zarr


def zarr_api_to_zarr(*args, zarr_array_kwargs=None, **kwargs):
    if zarr_array_kwargs is not None:
        kwargs.update(zarr_array_kwargs)
    chunk_key_encoding = kwargs.get("chunk_key_encoding")
    if isinstance(chunk_key_encoding, dict) and chunk_key_encoding.get("name") == "v2":
        arr = kwargs.pop("arr")
        url = kwargs.pop("url")
        component = kwargs.pop("component", None)
        compute = kwargs.pop("compute", True)
        dimension_separator = chunk_key_encoding.get("separator", ".")
        kwargs.pop("chunk_key_encoding", None)
        kwargs.pop("dimension_names", None)
        compressor = kwargs.pop("compressor", None)
        chunks = kwargs.pop("chunks", getattr(arr, "chunksize", None))
        target = zarr.open_array(
            store=url,
            path=component,
            mode="w",
            shape=arr.shape,
            chunks=chunks,
            dtype=arr.dtype,
            zarr_format=2,
            dimension_separator=dimension_separator,
            compressor=compressor,
        )
        return REAL_TO_ZARR(arr=arr, url=target, compute=compute)
    if "compressor" in kwargs and kwargs.get("zarr_format") != 2:
        kwargs["compressors"] = [kwargs.pop("compressor")]
    direct_kwargs = {}
    normalized_zarr_kwargs = {}
    for key, value in kwargs.items():
        if key in {
            "arr",
            "url",
            "component",
            "storage_options",
            "region",
            "compute",
            "return_stored",
            "zarr_format",
            "zarr_read_kwargs",
        }:
            direct_kwargs[key] = value
        else:
            normalized_zarr_kwargs[key] = value
    return REAL_TO_ZARR(
        *args,
        zarr_array_kwargs=normalized_zarr_kwargs or None,
        **direct_kwargs,
    )


def add_tree_to_tar(tar: tarfile.TarFile, root: Path) -> None:
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        info = tar.gettarinfo(str(path), arcname=relative.as_posix())
        if path.is_file():
            with path.open("rb") as handle:
                tar.addfile(info, handle)
        else:
            tar.addfile(info)


def main() -> int:
    ASSET_ROOT.mkdir(parents=True, exist_ok=True)

    for method in ("coins", "astronaut"):
        for version in ("0.4", "0.5"):
            archive_name = f"{method}_v{version.replace('.', '')}_seed0.tar"
            archive_path = ASSET_ROOT / archive_name
            with patch.object(py_writer.da, "to_zarr", zarr_api_to_zarr):
                with TemporaryDirectory(prefix="ome-zarr-create-assets-") as temp_dir:
                    root = Path(temp_dir) / f"{method}.zarr"
                    random.seed(0)
                    py_cli.main(
                        ["create", f"--method={method}", str(root), "--format", version]
                    )
                    with archive_path.open("wb") as handle:
                        with tarfile.open(
                            fileobj=handle,
                            mode="w",
                            format=tarfile.USTAR_FORMAT,
                        ) as tar:
                            add_tree_to_tar(tar, root)

    manifest_path = ASSET_ROOT / "manifest.json"
    manifest_path.write_text(
        (
            "{\n"
            '  "source": "source_code_v.0.15.0/ome_zarr/cli.py create",\n'
            '  "seed": 0,\n'
            '  "archives": [\n'
            '    "coins_v04_seed0.tar",\n'
            '    "coins_v05_seed0.tar",\n'
            '    "astronaut_v04_seed0.tar",\n'
            '    "astronaut_v05_seed0.tar"\n'
            "  ]\n"
            "}\n"
        ),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
