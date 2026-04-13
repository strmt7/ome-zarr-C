from __future__ import annotations

import importlib
import io
import sys
import warnings
from contextlib import nullcontext, redirect_stdout
from pathlib import Path
from unittest.mock import patch

import numpy as np
import zarr

from tests._outcomes import err, ok

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_scale = importlib.import_module("ome_zarr.scale")
_cpp_scale = importlib.import_module("ome_zarr_c.scale")


def _compat_create_group(self, dir_path: str, base, pyramid):  # noqa: ANN001
    grp = zarr.open_group(dir_path, mode="w")
    grp.create_array(name="base", data=base)
    for index in range(1, len(pyramid)):
        grp.create_array(name=str(index), data=pyramid[index])
    return grp


def _snapshot_tree(root: Path):
    snapshot = []
    for path in sorted(root.rglob("*")):
        rel_path = path.relative_to(root).as_posix()
        if path.is_file():
            snapshot.append(("file", rel_path, path.read_bytes()))
        else:
            snapshot.append(("dir", rel_path, None))
    return snapshot


def _warning_snapshot(caught: list[warnings.WarningMessage]) -> list[tuple[str, str]]:
    return [(type(w.message).__name__, str(w.message)) for w in caught]


def _write_input_array(path: Path, data: np.ndarray, attrs: dict | None = None) -> None:
    array = zarr.open_array(
        str(path),
        mode="w",
        shape=data.shape,
        chunks=data.shape,
        dtype=data.dtype,
    )
    array[:] = data
    if attrs:
        array.attrs.update(attrs)


def _run_scaler_scale(scale_module, input_path: Path, output_path: Path, **kwargs):
    stream = io.StringIO()
    patched = (
        patch.object(scale_module.Scaler, "_Scaler__create_group", _compat_create_group)
        if scale_module is _py_scale
        else nullcontext()
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            with patched, redirect_stdout(stream):
                scaler = scale_module.Scaler(**kwargs)
                scaler.scale(str(input_path), str(output_path))
            return ok(
                tree=_snapshot_tree(output_path),
                stdout=stream.getvalue(),
                records=_warning_snapshot(caught),
            )
        except Exception as exc:  # noqa: BLE001
            tree = _snapshot_tree(output_path) if output_path.exists() else []
            return err(
                exc,
                tree=tree,
                stdout=stream.getvalue(),
                records=_warning_snapshot(caught),
            )


def test_scaler_scale_matches_upstream_with_metadata_copy(tmp_path) -> None:
    data = np.arange(64, dtype=np.uint16).reshape(8, 8)
    py_input = tmp_path / "py-input.zarr"
    cpp_input = tmp_path / "cpp-input.zarr"
    _write_input_array(py_input, data, attrs={"alpha": 1})
    _write_input_array(cpp_input, data, attrs={"alpha": 1})

    expected = _run_scaler_scale(
        _py_scale,
        py_input,
        tmp_path / "py-output.zarr",
        copy_metadata=True,
        max_layer=2,
        method="nearest",
    )
    actual = _run_scaler_scale(
        _cpp_scale,
        cpp_input,
        tmp_path / "cpp-output.zarr",
        copy_metadata=True,
        max_layer=2,
        method="nearest",
    )
    assert expected == actual


def test_scaler_scale_matches_upstream_for_labeled_arrays(tmp_path) -> None:
    data = np.array(
        [
            [0, 0, 1, 1, 2, 2, 3, 3],
            [0, 0, 1, 1, 2, 2, 3, 3],
            [4, 4, 5, 5, 6, 6, 7, 7],
            [4, 4, 5, 5, 6, 6, 7, 7],
            [0, 0, 1, 1, 2, 2, 3, 3],
            [0, 0, 1, 1, 2, 2, 3, 3],
            [4, 4, 5, 5, 6, 6, 7, 7],
            [4, 4, 5, 5, 6, 6, 7, 7],
        ],
        dtype=np.uint8,
    )
    py_input = tmp_path / "py-labeled-input.zarr"
    cpp_input = tmp_path / "cpp-labeled-input.zarr"
    _write_input_array(py_input, data)
    _write_input_array(cpp_input, data)

    expected = _run_scaler_scale(
        _py_scale,
        py_input,
        tmp_path / "py-labeled-output.zarr",
        labeled=True,
        max_layer=2,
        method="nearest",
    )
    actual = _run_scaler_scale(
        _cpp_scale,
        cpp_input,
        tmp_path / "cpp-labeled-output.zarr",
        labeled=True,
        max_layer=2,
        method="nearest",
    )
    assert expected == actual
