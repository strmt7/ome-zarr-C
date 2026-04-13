from __future__ import annotations

import importlib
import io
import json
import random
import sys
import warnings
from contextlib import nullcontext, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import dask
import dask.array as da
import zarr

from tests._outcomes import err, ok

ROOT = Path(__file__).resolve().parents[1]
UPSTREAM_ROOT = ROOT / "source_code_v.0.15.0"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(UPSTREAM_ROOT) not in sys.path:
    sys.path.insert(0, str(UPSTREAM_ROOT))

_py_io = importlib.import_module("ome_zarr.io")
_py_utils = importlib.import_module("ome_zarr.utils")
_py_writer = importlib.import_module("ome_zarr.writer")
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
    return _REAL_TO_ZARR(*args, **kwargs)


def snapshot_tree(root: Path):
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


def rewrite_snapshot_prefix(snapshot, old_prefix: str, new_prefix: str):
    rewritten = []
    for kind, rel_path, payload in snapshot:
        if rel_path == old_prefix or rel_path.startswith(f"{old_prefix}/"):
            rel_path = new_prefix + rel_path[len(old_prefix) :]
        rewritten.append((kind, rel_path, payload))
    return rewritten


def normalize_output(text: str, replacements: dict[str, str]) -> str:
    normalized = text
    for original, replacement in replacements.items():
        normalized = normalized.replace(original, replacement)
    return normalized


def write_minimal_v2_image(root: Path) -> None:
    group = zarr.open_group(str(root), mode="w", zarr_format=2)
    group.attrs.update(
        {
            "multiscales": [
                {
                    "version": "0.4",
                    "axes": ["y", "x"],
                    "datasets": [{"path": "0"}],
                }
            ]
        }
    )
    array = zarr.open_array(
        str(root / "0"),
        mode="w",
        shape=(2, 2),
        chunks=(2, 2),
        dtype="i4",
    )
    array[:] = [[1, 2], [3, 4]]


def write_minimal_v3_image(root: Path) -> None:
    group = zarr.open_group(str(root), mode="w", zarr_format=3)
    group.attrs.update(
        {
            "ome": {
                "version": "0.5",
                "multiscales": [
                    {
                        "axes": [
                            {"name": "y", "type": "space"},
                            {"name": "x", "type": "space"},
                        ],
                        "datasets": [
                            {
                                "path": "s0",
                                "coordinateTransformations": [
                                    {"type": "scale", "scale": [1, 1]}
                                ],
                            }
                        ],
                    }
                ],
            }
        }
    )
    array = group.create_array(
        name="s0",
        shape=(2, 2),
        chunks=(2, 2),
        dtype="i4",
    )
    array[:] = [[5, 6], [7, 8]]


def write_plain_zarr_group(root: Path) -> None:
    group = zarr.open_group(str(root), mode="w", zarr_format=2)
    array = group.create_array(
        name="plain",
        shape=(2, 2),
        chunks=(2, 2),
        dtype="i4",
    )
    array[:] = [[9, 10], [11, 12]]


def node_signature(node) -> dict:
    return {
        "basename": Path(str(node.zarr.path)).name,
        "visible": node.visible,
        "version": node.zarr.version,
        "metadata": node.metadata,
        "specs": [type(spec).__name__ for spec in node.specs],
        "data": [
            {
                "shape": array.shape,
                "dtype": str(array.dtype),
                "chunks": array.chunks,
            }
            for array in node.data
        ],
    }


def run_info(func, path: Path, *, stats: bool = False):
    stream = io.StringIO()
    try:
        with redirect_stdout(stream):
            nodes = list(func(str(path), stats=stats))
        return ok(
            value=[node_signature(node) for node in nodes],
            stdout=stream.getvalue(),
        )
    except Exception as exc:  # noqa: BLE001
        return err(exc, stdout=stream.getvalue())


def location_signature(location) -> dict:
    return {
        "type": type(location).__name__,
        "exists": location.exists(),
        "fmt_version": location.fmt.version,
        "mode": location.mode,
        "version": location.version,
        "path": location.path,
        "basename": location.basename(),
        "parts": location.parts(),
        "subpath_empty": location.subpath(""),
        "subpath_nested": location.subpath("nested/data"),
        "repr": repr(location),
        "root_attrs": location.root_attrs,
    }


def run_parse_url(parse_url, path, *, mode="r", fmt=None):
    try:
        kwargs = {"mode": mode}
        if fmt is not None:
            kwargs["fmt"] = fmt
        location = parse_url(path, **kwargs)
        if location is None:
            return ok(value=None)
        return ok(value=location_signature(location))
    except Exception as exc:  # noqa: BLE001
        return err(exc)


def warning_snapshot(caught: list[warnings.WarningMessage]) -> list[tuple[str, str]]:
    return [(type(w.message).__name__, str(w.message)) for w in caught]


def run_write_image(func, root: Path, *args, **kwargs):
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
                    tree=snapshot_tree(root),
                    stdout=stream.getvalue(),
                    records=warning_snapshot(caught),
                    value={"delayed_count": len(result)},
                )
        except Exception as exc:  # noqa: BLE001
            return err(
                exc,
                tree=snapshot_tree(root),
                stdout=stream.getvalue(),
                records=warning_snapshot(caught),
            )


def run_create_zarr(func, root: Path, *, method, label_name: str, fmt, seed: int):
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
            tree=snapshot_tree(root),
            value={"zarr_format": int(group.info._zarr_format)},
        )
    except Exception as exc:  # noqa: BLE001
        return err(exc, tree=snapshot_tree(root))


def run_cli_main(main_func, args: list[str], replacements: dict[str, str]):
    stream = io.StringIO()
    patched = (
        patch.object(_py_writer.da, "to_zarr", _compat_to_zarr)
        if main_func.__module__.startswith("ome_zarr.")
        else nullcontext()
    )
    try:
        with patched, redirect_stdout(stream):
            main_func(args)
        return ok(stdout=normalize_output(stream.getvalue(), replacements))
    except Exception as exc:  # noqa: BLE001
        return err(exc, stdout=normalize_output(stream.getvalue(), replacements))


def run_download(func, source: Path, output: Path):
    stream = io.StringIO()
    try:
        with redirect_stdout(stream):
            func(str(source), str(output))
        copied_root = output / source.name
        return ok(tree=snapshot_tree(copied_root), stdout=stream.getvalue())
    except Exception as exc:  # noqa: BLE001
        copied_root = output / source.name
        tree = snapshot_tree(copied_root) if copied_root.exists() else []
        return err(exc, tree=tree, stdout=stream.getvalue())


__all__ = [
    "location_signature",
    "node_signature",
    "normalize_output",
    "rewrite_snapshot_prefix",
    "run_cli_main",
    "run_create_zarr",
    "run_download",
    "run_info",
    "run_parse_url",
    "run_write_image",
    "snapshot_tree",
    "warning_snapshot",
    "write_minimal_v2_image",
    "write_minimal_v3_image",
    "write_plain_zarr_group",
]
