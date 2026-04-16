from __future__ import annotations

import io
import json
import os
import random
import shutil
import subprocess
import sys
from contextlib import nullcontext, redirect_stdout
from functools import lru_cache
from pathlib import Path
from unittest.mock import patch

import dask.array as da
import numpy as np
import pytest
import zarr

from tests import test_utils_equivalence as utils_eq
from tests._outcomes import err, ok

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_cli = __import__("ome_zarr.cli", fromlist=["dummy"])
_py_writer = __import__("ome_zarr.writer", fromlist=["dummy"])
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


@lru_cache(maxsize=1)
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
            or "ome-zarr-C native builds require" in failure_text
            or "CMake 4.3 or higher is required" in failure_text
        ):
            pytest.skip(
                "standalone native CLI tests require the latest pinned "
                "native host toolchain"
            )
        raise

    cli_path = build_dir / "ome_zarr_native_cli"
    if not cli_path.exists():
        cli_path = cli_path.with_suffix(".exe")
    assert cli_path.exists(), "standalone native CLI binary was not built"
    return cli_path


def _rewrite_snapshot_prefix(snapshot, old_prefix: str, new_prefix: str):
    rewritten = []
    for kind, rel_path, payload in snapshot:
        if rel_path == old_prefix or rel_path.startswith(f"{old_prefix}/"):
            rel_path = new_prefix + rel_path[len(old_prefix) :]
        rewritten.append((kind, rel_path, payload))
    return rewritten


def _normalize_output(text: str, replacements: dict[str, str]):
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


def _run_native_cli(
    args: list[str],
    replacements: dict[str, str],
    *,
    env: dict[str, str] | None = None,
):
    completed = subprocess.run(
        [str(_native_cli_path()), *args],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    stdout = _normalize_output(completed.stdout, replacements)
    payload = {
        "returncode": completed.returncode,
        "stderr": completed.stderr,
    }
    if completed.returncode == 0:
        return ok(stdout=stdout, payload=payload)
    return err(
        RuntimeError(completed.stderr.strip() or completed.stdout.strip()),
        stdout=stdout,
        payload=payload,
    )


def test_native_cli_scale_matches_upstream(tmp_path) -> None:
    data = np.arange(64, dtype=np.uint16).reshape(8, 8)
    py_input = tmp_path / "py-native-scale-input.zarr"
    native_input = tmp_path / "native-scale-input.zarr"
    py_output = tmp_path / "py-native-scale-output.zarr"
    native_output = tmp_path / "native-scale-output.zarr"
    replacements = {
        str(py_input): "<INPUT>",
        str(native_input): "<INPUT>",
        str(py_output): "<OUTPUT>",
        str(native_output): "<OUTPUT>",
    }

    for input_path in (py_input, native_input):
        arr = zarr.open_array(
            str(input_path),
            mode="w",
            shape=data.shape,
            chunks=(2, 2),
            dtype=data.dtype,
        )
        arr[:] = data
        arr.attrs.update({"alpha": 1})

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
    actual = _run_native_cli(
        [
            "scale",
            str(native_input),
            str(native_output),
            "yx",
            "--copy-metadata",
            "--method=nearest",
            "--max_layer=2",
        ],
        replacements,
    )
    assert expected.status == actual.status == "ok"
    assert expected.stdout == actual.stdout
    assert _snapshot_tree(py_output) == _snapshot_tree(native_output)


def test_native_cli_scale_invalid_method_matches_upstream(tmp_path) -> None:
    data = np.arange(16, dtype=np.uint16).reshape(4, 4)
    py_input = tmp_path / "py-bad-scale-input.zarr"
    native_input = tmp_path / "native-bad-scale-input.zarr"
    replacements = {
        str(py_input): "<INPUT>",
        str(native_input): "<INPUT>",
    }

    for input_path in (py_input, native_input):
        arr = zarr.open_array(
            str(input_path),
            mode="w",
            shape=data.shape,
            chunks=(2, 2),
            dtype=data.dtype,
        )
        arr[:] = data

    expected = _run_main(
        _py_cli.main,
        [
            "scale",
            str(py_input),
            str(tmp_path / "py-bad-scale-output.zarr"),
            "yx",
            "--method=laplacian",
        ],
        replacements,
    )
    actual = _run_native_cli(
        [
            "scale",
            str(native_input),
            str(tmp_path / "native-bad-scale-output.zarr"),
            "yx",
            "--method=laplacian",
        ],
        replacements,
    )

    assert expected.status == actual.status == "err"
    assert actual.payload["returncode"] != 0
    assert actual.payload["stderr"].strip() == expected.error_message


def test_native_cli_create_info_matches_upstream_for_seeded_labels_tree(
    tmp_path,
) -> None:
    py_root = tmp_path / "py-create-info.zarr"
    native_root = tmp_path / "native-create-info.zarr"
    replacements = {
        str(py_root): "<ROOT>",
        str(native_root): "<ROOT>",
    }
    native_env = os.environ.copy()
    native_env["OME_ZARR_C_CREATE_SEED"] = "0"

    random.seed(0)
    expected_create = _run_main(
        _py_cli.main,
        ["create", "--method=coins", str(py_root), "--format", "0.5"],
        replacements,
    )
    random.seed(0)
    actual_create = _run_native_cli(
        ["create", "--method=coins", str(native_root), "--format", "0.5"],
        replacements,
        env=native_env,
    )

    assert expected_create.status == actual_create.status == "ok"
    assert expected_create.stdout == actual_create.stdout
    assert _snapshot_tree(py_root) == _snapshot_tree(native_root)

    expected_info = _run_main(_py_cli.main, ["info", str(py_root)], replacements)
    actual_info = _run_native_cli(["info", str(native_root)], replacements)

    assert expected_info.status == actual_info.status == "ok"
    assert actual_info.payload["stderr"] == ""
    assert expected_info.stdout == actual_info.stdout


def test_native_cli_download_matches_upstream_for_seeded_labels_tree(tmp_path) -> None:
    py_source = tmp_path / "py" / "source.zarr"
    native_source = tmp_path / "native" / "source.zarr"
    py_output = tmp_path / "py-downloads"
    native_output = tmp_path / "native-downloads"
    replacements = {
        str(py_source): "<SOURCE>",
        str(native_source): "<SOURCE>",
        str(py_output): "<OUT>",
        str(native_output): "<OUT>",
    }
    native_env = os.environ.copy()
    native_env["OME_ZARR_C_CREATE_SEED"] = "0"

    random.seed(0)
    expected_create = _run_main(
        _py_cli.main,
        ["create", "--method=astronaut", str(py_source), "--format", "0.5"],
        replacements,
    )
    random.seed(0)
    actual_create = _run_native_cli(
        ["create", "--method=astronaut", str(native_source), "--format", "0.5"],
        replacements,
        env=native_env,
    )
    assert expected_create.status == actual_create.status == "ok"

    expected_download = _run_main(
        _py_cli.main,
        ["download", str(py_source), f"--output={py_output}"],
        replacements,
    )
    actual_download = _run_native_cli(
        ["download", str(native_source), f"--output={native_output}"],
        replacements,
    )

    assert expected_download.status == actual_download.status == "ok"
    assert actual_download.payload["stderr"] == ""
    assert utils_eq._normalize_download_stdout(
        expected_download.stdout,
        replacements,
    ) == utils_eq._normalize_download_stdout(actual_download.stdout, replacements)
    assert _snapshot_tree(py_output) == _snapshot_tree(native_output)
