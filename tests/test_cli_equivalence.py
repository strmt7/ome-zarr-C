from __future__ import annotations

import argparse
import importlib
import io
import json
import logging
import random
import sys
from contextlib import nullcontext, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import dask.array as da
import numpy as np
import zarr

from tests._outcomes import err, ok

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_cli = importlib.import_module("ome_zarr.cli")
_py_writer = importlib.import_module("ome_zarr.writer")
_cpp_cli = importlib.import_module("ome_zarr_c.cli")
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


def _rewrite_snapshot_prefix(snapshot, old_prefix: str, new_prefix: str):
    rewritten = []
    for kind, rel_path, payload in snapshot:
        if rel_path == old_prefix or rel_path.startswith(f"{old_prefix}/"):
            rel_path = new_prefix + rel_path[len(old_prefix) :]
        rewritten.append((kind, rel_path, payload))
    return rewritten


def _normalize_output(text: str, replacements: dict[str, str]) -> str:
    normalized = text
    for original, replacement in replacements.items():
        normalized = normalized.replace(original, replacement)
    return normalized


def _run_main(main_func, args: list[str], replacements: dict[str, str]):
    stream = io.StringIO()
    patched = (
        patch.object(_py_writer.da, "to_zarr", _compat_to_zarr)
        if main_func is _py_cli.main and args and args[0] in {"create", "scale"}
        else nullcontext()
    )
    try:
        with patched, redirect_stdout(stream):
            main_func(args)
        return ok(stdout=_normalize_output(stream.getvalue(), replacements))
    except Exception as exc:  # noqa: BLE001
        return err(exc, stdout=_normalize_output(stream.getvalue(), replacements))


def _run_config_logging(
    module, *, loglevel: int, verbose: int, quiet: int, monkeypatch
):
    records: dict[str, object] = {}
    s3fs_logger = logging.getLogger("s3fs")
    previous_level = s3fs_logger.level

    def fake_basic_config(*, level):
        records["level"] = level

    monkeypatch.setattr(module.logging, "basicConfig", fake_basic_config)
    try:
        module.config_logging(
            loglevel,
            argparse.Namespace(verbose=verbose, quiet=quiet),
        )
        records["s3fs_level"] = s3fs_logger.level
    finally:
        s3fs_logger.setLevel(previous_level)
    return records


def _run_wrapper_call(module, func_name: str, attr_name: str, args, monkeypatch):
    records: dict[str, object] = {}

    monkeypatch.setattr(
        module,
        attr_name,
        lambda *call_args, **call_kwargs: records.update(
            {"call_args": call_args, "call_kwargs": call_kwargs}
        ),
    )
    monkeypatch.setattr(module.logging, "basicConfig", lambda **kwargs: None)
    getattr(module, func_name)(args)
    return records


def test_cli_config_logging_matches_upstream(monkeypatch) -> None:
    assert _run_config_logging(
        _py_cli,
        loglevel=logging.WARNING,
        verbose=2,
        quiet=1,
        monkeypatch=monkeypatch,
    ) == _run_config_logging(
        _cpp_cli,
        loglevel=logging.WARNING,
        verbose=2,
        quiet=1,
        monkeypatch=monkeypatch,
    )


def test_cli_view_finder_and_csv_wrappers_match_upstream(monkeypatch) -> None:
    view_args = argparse.Namespace(
        path="image.zarr",
        port=8001,
        force=True,
        verbose=0,
        quiet=0,
    )
    finder_args = argparse.Namespace(path="images", port=9000, verbose=1, quiet=0)
    csv_args = argparse.Namespace(
        csv_path="props.csv",
        csv_id="cell_id",
        csv_keys="score#d",
        zarr_path="image.zarr",
        zarr_id="cell_id",
        verbose=0,
        quiet=0,
    )

    assert _run_wrapper_call(
        _py_cli, "view", "zarr_view", view_args, monkeypatch
    ) == _run_wrapper_call(
        _cpp_cli,
        "view",
        "zarr_view",
        view_args,
        monkeypatch,
    )
    assert _run_wrapper_call(
        _py_cli,
        "finder",
        "bff_finder",
        finder_args,
        monkeypatch,
    ) == _run_wrapper_call(
        _cpp_cli,
        "finder",
        "bff_finder",
        finder_args,
        monkeypatch,
    )
    assert _run_wrapper_call(
        _py_cli,
        "csv_to_labels",
        "csv_to_zarr",
        csv_args,
        monkeypatch,
    ) == _run_wrapper_call(
        _cpp_cli,
        "csv_to_labels",
        "csv_to_zarr",
        csv_args,
        monkeypatch,
    )


def test_cli_create_and_info_match_upstream_v04_v05(tmp_path) -> None:
    for version in ("0.4", "0.5"):
        py_root = tmp_path / f"py-cli-{version}.zarr"
        cpp_root = tmp_path / f"cpp-cli-{version}.zarr"
        replacements = {
            str(py_root): "<ROOT>",
            str(cpp_root): "<ROOT>",
        }

        random.seed(0)
        py_create = _run_main(
            _py_cli.main,
            ["create", "--method=coins", str(py_root), "--format", version],
            replacements,
        )
        random.seed(0)
        cpp_create = _run_main(
            _cpp_cli.main,
            ["create", "--method=coins", str(cpp_root), "--format", version],
            replacements,
        )
        assert py_create == cpp_create
        assert _snapshot_tree(py_root) == _snapshot_tree(cpp_root)

        py_info = _run_main(_py_cli.main, ["info", str(py_root)], replacements)
        cpp_info = _run_main(_cpp_cli.main, ["info", str(cpp_root)], replacements)
        assert py_info == cpp_info


def test_cli_download_matches_upstream(tmp_path) -> None:
    py_source = tmp_path / "py-source.zarr"
    cpp_source = tmp_path / "cpp-source.zarr"
    py_output = tmp_path / "py-downloads"
    cpp_output = tmp_path / "cpp-downloads"
    replacements = {
        str(py_source): "<SOURCE>",
        str(cpp_source): "<SOURCE>",
        str(py_output): "<OUT>",
        str(cpp_output): "<OUT>",
    }

    random.seed(0)
    py_create = _run_main(
        _py_cli.main,
        ["create", "--method=astronaut", str(py_source), "--format", "0.5"],
        replacements,
    )
    random.seed(0)
    cpp_create = _run_main(
        _cpp_cli.main,
        ["create", "--method=astronaut", str(cpp_source), "--format", "0.5"],
        replacements,
    )
    assert py_create == cpp_create

    expected = _run_main(
        _py_cli.main,
        ["download", str(py_source), f"--output={py_output}"],
        replacements,
    )
    actual = _run_main(
        _cpp_cli.main,
        ["download", str(cpp_source), f"--output={cpp_output}"],
        replacements,
    )
    assert expected.status == actual.status == "ok"
    assert "downloading..." in expected.stdout
    assert "downloading..." in actual.stdout
    assert _rewrite_snapshot_prefix(
        _snapshot_tree(py_output), py_source.name, "<SOURCE_ROOT>"
    ) == _rewrite_snapshot_prefix(
        _snapshot_tree(cpp_output), cpp_source.name, "<SOURCE_ROOT>"
    )


def test_cli_scale_matches_upstream(tmp_path) -> None:
    data = np.arange(64, dtype=np.uint16).reshape(8, 8)
    py_input = tmp_path / "py-input.zarr"
    cpp_input = tmp_path / "cpp-input.zarr"
    py_output = tmp_path / "py-output.zarr"
    cpp_output = tmp_path / "cpp-output.zarr"
    replacements = {
        str(py_input): "<INPUT>",
        str(cpp_input): "<INPUT>",
        str(py_output): "<OUTPUT>",
        str(cpp_output): "<OUTPUT>",
    }

    py_array = zarr.open_array(
        str(py_input),
        mode="w",
        shape=data.shape,
        chunks=data.shape,
        dtype=data.dtype,
    )
    cpp_array = zarr.open_array(
        str(cpp_input),
        mode="w",
        shape=data.shape,
        chunks=data.shape,
        dtype=data.dtype,
    )
    py_array[:] = data
    cpp_array[:] = data
    py_array.attrs.update({"alpha": 1})
    cpp_array.attrs.update({"alpha": 1})

    expected = _run_main(
        _py_cli.main,
        [
            "scale",
            str(py_input),
            str(py_output),
            "yx",
            "--copy-metadata",
            "--method=nearest",
            "--max_layer=2",
        ],
        replacements,
    )
    actual = _run_main(
        _cpp_cli.main,
        [
            "scale",
            str(cpp_input),
            str(cpp_output),
            "yx",
            "--copy-metadata",
            "--method=nearest",
            "--max_layer=2",
        ],
        replacements,
    )
    assert expected == actual
    assert _snapshot_tree(py_output) == _snapshot_tree(cpp_output)
