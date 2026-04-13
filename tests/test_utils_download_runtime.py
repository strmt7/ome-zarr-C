from __future__ import annotations

import importlib
import io
from contextlib import redirect_stdout
from pathlib import Path

import zarr

_cpp_utils = importlib.import_module("ome_zarr_c.utils")


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


def test_download_is_exported() -> None:
    assert hasattr(_cpp_utils, "download")


def test_download_roundtrip_writes_minimal_v2_image(tmp_path) -> None:
    source = tmp_path / "source-v2.zarr"
    output = tmp_path / "downloads"
    _write_minimal_v2_image(source)

    stream = io.StringIO()
    with redirect_stdout(stream):
        _cpp_utils.download(str(source), str(output))

    copied = zarr.open_group(str(output / source.name), mode="r")
    assert copied.attrs.asdict() == {
        "multiscales": [
            {
                "version": "0.4",
                "axes": ["y", "x"],
                "datasets": [{"path": "0"}],
            }
        ]
    }
    assert copied["0"][:].tolist() == [[1, 2], [3, 4]]
    assert "downloading..." in stream.getvalue()


def test_download_roundtrip_writes_minimal_v3_image(tmp_path) -> None:
    source = tmp_path / "source-v3.zarr"
    output = tmp_path / "downloads"
    _write_minimal_v3_image(source)

    stream = io.StringIO()
    with redirect_stdout(stream):
        _cpp_utils.download(str(source), str(output))

    copied = zarr.open_group(str(output / source.name), mode="r")
    assert copied.attrs.asdict() == {
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
    assert copied["s0"][:].tolist() == [[5, 6], [7, 8]]
    assert "downloading..." in stream.getvalue()
