from __future__ import annotations

import importlib
import json
import os
import struct
import sys
from pathlib import Path

from tests import test_utils_equivalence as utils_eq
from tests._outcomes import err, ok

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_format = importlib.import_module("ome_zarr.format")
_py_io = importlib.import_module("ome_zarr.io")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, separators=(",", ":")))


def _write_v2_group(root: Path, attrs: dict | None = None) -> None:
    root.mkdir(parents=True, exist_ok=True)
    _write_json(root / ".zgroup", {"zarr_format": 2})
    _write_json(root / ".zattrs", attrs or {})


def _write_v2_i4_array(root: Path, values: tuple[int, int, int, int]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    _write_json(
        root / ".zarray",
        {
            "chunks": [2, 2],
            "compressor": None,
            "dimension_separator": ".",
            "dtype": "<i4",
            "fill_value": 0,
            "filters": None,
            "order": "C",
            "shape": [2, 2],
            "zarr_format": 2,
        },
    )
    (root / "0.0").write_bytes(struct.pack("<4i", *values))


def _write_v3_i4_array(root: Path, values: tuple[int, int, int, int]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    _write_json(
        root / "zarr.json",
        {
            "attributes": {},
            "chunk_grid": {
                "configuration": {"chunk_shape": [2, 2]},
                "name": "regular",
            },
            "chunk_key_encoding": {
                "configuration": {"separator": "/"},
                "name": "default",
            },
            "codecs": [{"configuration": {"endian": "little"}, "name": "bytes"}],
            "data_type": "int32",
            "fill_value": 0,
            "node_type": "array",
            "shape": [2, 2],
            "storage_transformers": [],
            "zarr_format": 3,
        },
    )
    chunk = root / "c" / "0" / "0"
    chunk.parent.mkdir(parents=True, exist_ok=True)
    chunk.write_bytes(struct.pack("<4i", *values))


def _write_minimal_v2_image(root: Path) -> None:
    _write_v2_group(
        root,
        {
            "multiscales": [
                {
                    "version": "0.4",
                    "axes": ["y", "x"],
                    "datasets": [{"path": "0"}],
                }
            ]
        },
    )
    _write_v2_i4_array(root / "0", (1, 2, 3, 4))


def _write_minimal_v3_image(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    _write_json(
        root / "zarr.json",
        {
            "attributes": {
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
            },
            "node_type": "group",
            "zarr_format": 3,
        },
    )
    _write_v3_i4_array(root / "s0", (5, 6, 7, 8))


def _location_signature(location) -> dict:
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


def _run_parse_url(parse_url, path, *, mode="r", fmt=None):
    try:
        kwargs = {"mode": mode}
        if fmt is not None:
            kwargs["fmt"] = fmt
        location = parse_url(path, **kwargs)
        if location is None:
            return ok(value=None)
        return ok(value=_location_signature(location))
    except Exception as exc:  # noqa: BLE001
        return err(exc)


def _run_native_io_signature(path, *, mode="r", fmt_version: str | None = None):
    args = ["io-signature", "--path", os.fspath(path), "--mode", mode]
    if fmt_version is not None:
        args.extend(["--format-version", fmt_version])
    outcome = utils_eq._run_native_probe(args)
    if outcome.status == "ok":
        return ok(value=outcome.value)
    return err(outcome.error_type(outcome.error_message))


def _run_native_io_create_signature(
    path,
    *,
    mode="r",
    fmt_version: str | None = None,
    subpath: str = "",
):
    args = [
        "io-signature",
        "--path",
        os.fspath(path),
        "--mode",
        mode,
        "--create-subpath",
        subpath,
    ]
    if fmt_version is not None:
        args.extend(["--format-version", fmt_version])
    outcome = utils_eq._run_native_probe(args)
    if outcome.status == "ok":
        return ok(value=outcome.value)
    return err(outcome.error_type(outcome.error_message))


def test_parse_url_matches_upstream_for_missing_path(tmp_path) -> None:
    expected = _run_parse_url(_py_io.parse_url, tmp_path / "missing.zarr")
    actual = _run_native_io_signature(tmp_path / "missing.zarr")
    assert expected == actual


def test_parse_url_matches_upstream_for_write_mode_creation(tmp_path) -> None:
    py_fmt = _py_format.FormatV05()
    path = tmp_path / "created.zarr"

    expected = _run_parse_url(_py_io.parse_url, path, mode="w", fmt=py_fmt)
    actual = _run_native_io_signature(path, mode="w", fmt_version=py_fmt.version)
    assert expected == actual


def test_zarr_location_matches_upstream_for_minimal_v2_image(tmp_path) -> None:
    root = tmp_path / "image-v2.zarr"
    _write_minimal_v2_image(root)

    expected = _run_parse_url(_py_io.parse_url, root)
    actual = _run_native_io_signature(root)
    assert expected == actual


def test_zarr_location_matches_upstream_for_minimal_v3_image(tmp_path) -> None:
    root = tmp_path / "image-v3.zarr"
    _write_minimal_v3_image(root)

    expected = _run_parse_url(_py_io.parse_url, root)
    actual = _run_native_io_signature(root)
    assert expected == actual


def test_zarr_location_matches_upstream_for_plate_root(tmp_path) -> None:
    root = tmp_path / "plate.zarr"
    field = root / "A" / "1" / "0"
    _write_minimal_v2_image(field)
    _write_v2_group(
        root / "A" / "1",
        {"well": {"version": "0.4", "images": [{"path": "0", "acquisition": 1}]}},
    )
    _write_v2_group(
        root,
        {
            "plate": {
                "version": "0.4",
                "rows": [{"name": "A"}],
                "columns": [{"name": "1"}],
                "wells": [{"path": "A/1", "rowIndex": 0, "columnIndex": 0}],
            }
        },
    )

    expected = _run_parse_url(_py_io.parse_url, root)
    actual = _run_native_io_signature(root)
    assert expected == actual


def test_zarr_location_create_and_equality_match_upstream(tmp_path) -> None:
    root = tmp_path / "root.zarr"
    _write_minimal_v2_image(root)

    py_loc = _py_io.parse_url(root)
    assert py_loc is not None

    py_child = py_loc.create("labels")
    native_child = _run_native_io_create_signature(root, subpath="labels")
    assert native_child == ok(value=_location_signature(py_child))

    native_same = _run_native_io_create_signature(root, subpath="")
    assert native_same == ok(value=_location_signature(py_loc.create("")))
