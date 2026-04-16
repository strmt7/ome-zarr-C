from __future__ import annotations

import importlib
import os
import random
import subprocess
import sys
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

import dask.array as da

from tests._outcomes import err, ok
from tests.test_cli_equivalence import _native_cli_path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_data = importlib.import_module("ome_zarr.data")
_py_format = importlib.import_module("ome_zarr.format")
_py_writer = importlib.import_module("ome_zarr.writer")
_REAL_TO_ZARR = da.to_zarr


def _zarr_api_to_zarr(*args, zarr_array_kwargs=None, **kwargs):
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
        target = __import__("zarr").open_array(
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
        return _REAL_TO_ZARR(arr=arr, url=target, compute=compute)
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
    return _REAL_TO_ZARR(
        *args,
        zarr_array_kwargs=normalized_zarr_kwargs or None,
        **direct_kwargs,
    )


def _snapshot_tree(root: Path):
    snapshot = []
    for path in sorted(root.rglob("*")):
        rel_path = path.relative_to(root).as_posix()
        if path.is_file():
            if path.suffix == ".json" or path.name in {".zattrs", ".zgroup", ".zarray"}:
                snapshot.append(("json", rel_path, path.read_text()))
            else:
                snapshot.append(("file", rel_path, path.read_bytes()))
        else:
            snapshot.append(("dir", rel_path, None))
    return snapshot


def _run_create_zarr(func, root: Path, *, method, label_name: str, fmt, seed: int):
    patched = (
        patch.object(_py_writer.da, "to_zarr", _zarr_api_to_zarr)
        if func.__module__.startswith("ome_zarr.")
        else nullcontext()
    )
    try:
        random.seed(seed)
        with patched:
            group = func(str(root), method=method, label_name=label_name, fmt=fmt)
        return ok(
            tree=_snapshot_tree(root),
            value={"zarr_format": int(group.info._zarr_format)},
        )
    except Exception as exc:  # noqa: BLE001
        return err(exc, tree=_snapshot_tree(root))


def _run_native_create(
    root: Path,
    *,
    method_name: str,
    version: str,
    seed: int,
):
    env = os.environ.copy()
    env["OME_ZARR_C_CREATE_SEED"] = str(seed)
    try:
        completed = subprocess.run(
            [
                str(_native_cli_path()),
                "create",
                "--method",
                method_name,
                "--format",
                version,
                str(root),
            ],
            check=False,
            capture_output=True,
            text=True,
            env=env,
        )
        payload = {
            "returncode": completed.returncode,
            "stderr": completed.stderr,
        }
        if completed.returncode == 0:
            return ok(
                stdout=completed.stdout,
                payload=payload,
                tree=_snapshot_tree(root),
            )
        return err(
            RuntimeError(completed.stderr.strip() or completed.stdout.strip()),
            stdout=completed.stdout,
            payload=payload,
            tree=_snapshot_tree(root),
        )
    except Exception as exc:  # noqa: BLE001
        return err(exc, tree=_snapshot_tree(root))


def test_native_cli_create_matches_upstream_for_coins_v04_v05(tmp_path) -> None:
    for version in ("0.4", "0.5"):
        py_fmt = _py_format.FormatV04() if version == "0.4" else _py_format.FormatV05()
        py_root = tmp_path / f"py-coins-{version}.zarr"
        native_root = tmp_path / f"native-coins-{version}.zarr"

        expected = _run_create_zarr(
            _py_data.create_zarr,
            py_root,
            method=_py_data.coins,
            label_name="coins",
            fmt=py_fmt,
            seed=0,
        )
        actual = _run_native_create(
            native_root,
            method_name="coins",
            version=version,
            seed=0,
        )
        assert expected.status == actual.status == "ok"
        assert actual.payload["stderr"] == ""
        assert expected.tree == actual.tree


def test_native_cli_create_matches_upstream_for_astronaut_v04_v05(tmp_path) -> None:
    for version in ("0.4", "0.5"):
        py_fmt = _py_format.FormatV04() if version == "0.4" else _py_format.FormatV05()
        py_root = tmp_path / f"py-astronaut-{version}.zarr"
        native_root = tmp_path / f"native-astronaut-{version}.zarr"

        expected = _run_create_zarr(
            _py_data.create_zarr,
            py_root,
            method=_py_data.astronaut,
            label_name="circles",
            fmt=py_fmt,
            seed=0,
        )
        actual = _run_native_create(
            native_root,
            method_name="astronaut",
            version=version,
            seed=0,
        )
        assert expected.status == actual.status == "ok"
        assert actual.payload["stderr"] == ""
        assert expected.tree == actual.tree
