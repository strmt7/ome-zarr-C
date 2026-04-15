from __future__ import annotations

import importlib
import json
import os
import random
import shutil
import subprocess
import sys
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

import dask.array as da
import pytest
import zarr

from tests._outcomes import err, ok

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_data = importlib.import_module("ome_zarr.data")
_py_format = importlib.import_module("ome_zarr.format")
_py_writer = importlib.import_module("ome_zarr.writer")
_cpp_data = importlib.import_module("ome_zarr_c.data")
_cpp_format = importlib.import_module("ome_zarr_c.format")
_REAL_TO_ZARR = da.to_zarr


def _compat_to_zarr(*args, zarr_array_kwargs=None, **kwargs):
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
                snapshot.append(("json", rel_path, json.loads(path.read_text())))
            else:
                snapshot.append(("file", rel_path, path.read_bytes()))
        else:
            snapshot.append(("dir", rel_path, None))
    return snapshot


def _run_create_zarr(func, root: Path, *, method, label_name: str, fmt, seed: int):
    patched = (
        patch.object(_py_writer.da, "to_zarr", _compat_to_zarr)
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


def _native_cli_path() -> Path:
    cmake = shutil.which("cmake")
    if cmake is None:
        pytest.skip("cmake is required for standalone native CLI tests")

    build_dir = ROOT / "build-cpp-tests"
    configure_cmd = [
        cmake,
        "-S",
        str(ROOT),
        "-B",
        str(build_dir),
        "-DCMAKE_BUILD_TYPE=Release",
    ]
    if shutil.which("ninja") is not None:
        configure_cmd[1:1] = ["-G", "Ninja"]

    try:
        subprocess.run(configure_cmd, check=True, capture_output=True, text=True)
        subprocess.run(
            [cmake, "--build", str(build_dir), "-j2"],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        failure_text = f"{exc.stdout}\n{exc.stderr}"
        if (
            "Could not find BLOSC_LIBRARY" in failure_text
            or "Could not find ZSTD_LIBRARY" in failure_text
            or "blosc.h" in failure_text
            or "zstd.h" in failure_text
        ):
            pytest.skip(
                "standalone native CLI tests require libblosc-dev and libzstd-dev"
            )
        raise

    cli_path = build_dir / "ome_zarr_native_cli"
    if not cli_path.exists():
        cli_path = cli_path.with_suffix(".exe")
    assert cli_path.exists(), "standalone native CLI binary was not built"
    return cli_path


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


def test_create_zarr_matches_upstream_for_coins_v04_v05(tmp_path) -> None:
    for version in ("0.4", "0.5"):
        py_fmt = _py_format.FormatV04() if version == "0.4" else _py_format.FormatV05()
        cpp_fmt = (
            _cpp_format.FormatV04() if version == "0.4" else _cpp_format.FormatV05()
        )
        py_root = tmp_path / f"py-coins-{version}.zarr"
        cpp_root = tmp_path / f"cpp-coins-{version}.zarr"

        expected = _run_create_zarr(
            _py_data.create_zarr,
            py_root,
            method=_py_data.coins,
            label_name="coins",
            fmt=py_fmt,
            seed=0,
        )
        actual = _run_create_zarr(
            _cpp_data.create_zarr,
            cpp_root,
            method=_cpp_data.coins,
            label_name="coins",
            fmt=cpp_fmt,
            seed=0,
        )
        assert expected == actual


def test_create_zarr_matches_upstream_for_astronaut_v04_v05(tmp_path) -> None:
    for version in ("0.4", "0.5"):
        py_fmt = _py_format.FormatV04() if version == "0.4" else _py_format.FormatV05()
        cpp_fmt = (
            _cpp_format.FormatV04() if version == "0.4" else _cpp_format.FormatV05()
        )
        py_root = tmp_path / f"py-astronaut-{version}.zarr"
        cpp_root = tmp_path / f"cpp-astronaut-{version}.zarr"

        expected = _run_create_zarr(
            _py_data.create_zarr,
            py_root,
            method=_py_data.astronaut,
            label_name="circles",
            fmt=py_fmt,
            seed=0,
        )
        actual = _run_create_zarr(
            _cpp_data.create_zarr,
            cpp_root,
            method=_cpp_data.astronaut,
            label_name="circles",
            fmt=cpp_fmt,
            seed=0,
        )
        assert expected == actual


def test_native_cli_create_matches_upstream_for_coins_v04_v05(tmp_path) -> None:
    for version in ("0.4", "0.5"):
        py_fmt = _py_format.FormatV04() if version == "0.4" else _py_format.FormatV05()
        py_root = tmp_path / f"py-native-coins-{version}.zarr"
        cpp_root = tmp_path / f"cpp-native-coins-{version}.zarr"

        expected = _run_create_zarr(
            _py_data.create_zarr,
            py_root,
            method=_py_data.coins,
            label_name="coins",
            fmt=py_fmt,
            seed=0,
        )
        actual = _run_native_create(
            cpp_root,
            method_name="coins",
            version=version,
            seed=0,
        )
        assert expected.status == actual.status == "ok"
        assert actual.stdout == ""
        assert actual.payload["stderr"] == ""
        assert expected.tree == actual.tree


def test_native_cli_create_matches_upstream_for_astronaut_v04_v05(tmp_path) -> None:
    for version in ("0.4", "0.5"):
        py_fmt = _py_format.FormatV04() if version == "0.4" else _py_format.FormatV05()
        py_root = tmp_path / f"py-native-astronaut-{version}.zarr"
        cpp_root = tmp_path / f"cpp-native-astronaut-{version}.zarr"

        expected = _run_create_zarr(
            _py_data.create_zarr,
            py_root,
            method=_py_data.astronaut,
            label_name="circles",
            fmt=py_fmt,
            seed=0,
        )
        actual = _run_native_create(
            cpp_root,
            method_name="astronaut",
            version=version,
            seed=0,
        )
        assert expected.status == actual.status == "ok"
        assert actual.stdout == ""
        assert actual.payload["stderr"] == ""
        assert expected.tree == actual.tree
