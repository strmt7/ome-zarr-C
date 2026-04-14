from __future__ import annotations

import importlib
import io
import json
import sys
import warnings
from contextlib import nullcontext, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import dask
import dask.array as da
import numpy as np
import zarr

from tests._outcomes import err, ok

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_format = importlib.import_module("ome_zarr.format")
_py_scale = importlib.import_module("ome_zarr.scale")
_py_writer = importlib.import_module("ome_zarr.writer")
_cpp_format = importlib.import_module("ome_zarr_c.format")
_cpp_scale = importlib.import_module("ome_zarr_c.scale")
_cpp_writer = importlib.import_module("ome_zarr_c.writer")
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


def _warning_snapshot(caught: list[warnings.WarningMessage]) -> list[tuple[str, str]]:
    return [(type(w.message).__name__, str(w.message)) for w in caught]


def _run_write_image(func, root: Path, *args, **kwargs):
    stream = io.StringIO()
    patched = (
        patch.object(_py_writer.da, "to_zarr", _compat_to_zarr)
        if func.__module__.startswith("ome_zarr.")
        else nullcontext()
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            with patched, redirect_stdout(stream):
                result = func(*args, **kwargs)
                if kwargs.get("compute") is False:
                    dask.compute(*result)
                return ok(
                    tree=_snapshot_tree(root),
                    stdout=stream.getvalue(),
                    records=_warning_snapshot(caught),
                    value={"delayed_count": len(result)},
                )
        except Exception as exc:  # noqa: BLE001
            return err(
                exc,
                tree=_snapshot_tree(root),
                stdout=stream.getvalue(),
                records=_warning_snapshot(caught),
            )


def _fmt_pair(version: str):
    if version == "0.4":
        return _py_format.FormatV04(), _cpp_format.FormatV04()
    return _py_format.FormatV05(), _cpp_format.FormatV05()


def test_write_image_matches_upstream_for_numpy_inputs(tmp_path) -> None:
    image = np.arange(3 * 32 * 32, dtype=np.uint8).reshape(3, 32, 32)
    for version in ("0.4", "0.5"):
        py_fmt, cpp_fmt = _fmt_pair(version)
        py_root = tmp_path / f"py-write-image-{version}.zarr"
        cpp_root = tmp_path / f"cpp-write-image-{version}.zarr"

        expected = _run_write_image(
            _py_writer.write_image,
            py_root,
            image,
            str(py_root),
            fmt=py_fmt,
            axes="cyx",
            scale_factors=(2, 4),
        )
        actual = _run_write_image(
            _cpp_writer.write_image,
            cpp_root,
            image,
            str(cpp_root),
            fmt=cpp_fmt,
            axes="cyx",
            scale_factors=(2, 4),
        )
        assert expected == actual


def test_write_image_matches_upstream_for_compute_false(tmp_path) -> None:
    image = da.from_array(
        np.arange(16, dtype=np.uint16).reshape(4, 4),
        chunks=(2, 2),
    )
    py_root = tmp_path / "py-write-image-delayed.zarr"
    cpp_root = tmp_path / "cpp-write-image-delayed.zarr"

    expected = _run_write_image(
        _py_writer.write_image,
        py_root,
        image,
        str(py_root),
        fmt=_py_format.FormatV05(),
        axes=["y", "x"],
        scale_factors=(2,),
        compute=False,
    )
    actual = _run_write_image(
        _cpp_writer.write_image,
        cpp_root,
        image,
        str(cpp_root),
        fmt=_cpp_format.FormatV05(),
        axes=["y", "x"],
        scale_factors=(2,),
        compute=False,
    )
    assert expected == actual


def test_write_image_matches_upstream_for_scaler_fallback(tmp_path) -> None:
    image = np.arange(64, dtype=np.uint16).reshape(8, 8)
    py_root = tmp_path / "py-write-image-scaler.zarr"
    cpp_root = tmp_path / "cpp-write-image-scaler.zarr"
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        py_scaler = _py_scale.Scaler(method="laplacian", max_layer=2)
        cpp_scaler = _cpp_scale.Scaler(method="laplacian", max_layer=2)

    expected = _run_write_image(
        _py_writer.write_image,
        py_root,
        image,
        str(py_root),
        fmt=_py_format.FormatV05(),
        axes=["y", "x"],
        scaler=py_scaler,
    )
    actual = _run_write_image(
        _cpp_writer.write_image,
        cpp_root,
        image,
        str(cpp_root),
        fmt=_cpp_format.FormatV05(),
        axes=["y", "x"],
        scaler=cpp_scaler,
    )
    assert expected == actual
