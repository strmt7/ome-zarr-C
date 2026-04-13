from __future__ import annotations

import importlib
import sys
from pathlib import Path

import zarr

from tests._outcomes import err, ok

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_format = importlib.import_module("ome_zarr.format")
_py_io = importlib.import_module("ome_zarr.io")
_cpp_format = importlib.import_module("ome_zarr_c.format")
_cpp_io = importlib.import_module("ome_zarr_c.io")


def _write_minimal_v2_image(root: Path) -> None:
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


def _write_minimal_v3_image(root: Path) -> None:
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


def test_parse_url_matches_upstream_for_missing_path(tmp_path) -> None:
    expected = _run_parse_url(_py_io.parse_url, tmp_path / "missing.zarr")
    actual = _run_parse_url(_cpp_io.parse_url, tmp_path / "missing.zarr")
    assert expected == actual


def test_parse_url_matches_upstream_for_write_mode_creation(tmp_path) -> None:
    py_fmt = _py_format.FormatV05()
    cpp_fmt = _cpp_format.FormatV05()
    path = tmp_path / "created.zarr"

    expected = _run_parse_url(_py_io.parse_url, path, mode="w", fmt=py_fmt)
    actual = _run_parse_url(_cpp_io.parse_url, path, mode="w", fmt=cpp_fmt)
    assert expected == actual


def test_zarr_location_matches_upstream_for_minimal_v2_image(tmp_path) -> None:
    root = tmp_path / "image-v2.zarr"
    _write_minimal_v2_image(root)

    expected = _run_parse_url(_py_io.parse_url, root)
    actual = _run_parse_url(_cpp_io.parse_url, root)
    assert expected == actual


def test_zarr_location_matches_upstream_for_minimal_v3_image(tmp_path) -> None:
    root = tmp_path / "image-v3.zarr"
    _write_minimal_v3_image(root)

    expected = _run_parse_url(_py_io.parse_url, root)
    actual = _run_parse_url(_cpp_io.parse_url, root)
    assert expected == actual


def test_zarr_location_create_and_equality_match_upstream(tmp_path) -> None:
    root = tmp_path / "root.zarr"
    _write_minimal_v2_image(root)

    py_loc = _py_io.parse_url(root)
    cpp_loc = _cpp_io.parse_url(root)
    assert py_loc is not None
    assert cpp_loc is not None

    py_child = py_loc.create("labels")
    cpp_child = cpp_loc.create("labels")
    assert _location_signature(py_child) == _location_signature(cpp_child)

    assert (py_loc == py_loc.create("")) == (cpp_loc == cpp_loc.create(""))
