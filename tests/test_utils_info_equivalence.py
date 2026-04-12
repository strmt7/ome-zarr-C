from __future__ import annotations

import importlib
import io
import sys
from contextlib import redirect_stdout
from pathlib import Path

import zarr

from tests._outcomes import err, ok

ROOT = Path(__file__).resolve().parents[1]

_cpp_utils = importlib.import_module("ome_zarr_c.utils")

sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))
_py_utils = importlib.import_module("ome_zarr.utils")


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


def _write_plain_zarr_group(root: Path) -> None:
    group = zarr.open_group(str(root), mode="w", zarr_format=2)
    array = group.create_array(
        name="plain",
        shape=(2, 2),
        chunks=(2, 2),
        dtype="i4",
    )
    array[:] = [[9, 10], [11, 12]]


def _node_signature(node) -> dict:
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


def _run_info(func, path: Path, *, stats: bool = False):
    stream = io.StringIO()
    try:
        with redirect_stdout(stream):
            nodes = list(func(str(path), stats=stats))
        return ok(
            value=[_node_signature(node) for node in nodes],
            stdout=stream.getvalue(),
        )
    except Exception as exc:  # noqa: BLE001
        return err(exc, stdout=stream.getvalue())


def test_cpp_utils_is_importable_before_upstream_sys_path_injection() -> None:
    assert hasattr(_cpp_utils, "info")


def test_info_matches_upstream_for_missing_input_path(tmp_path) -> None:
    expected = _run_info(_py_utils.info, tmp_path / "missing.zarr")
    actual = _run_info(_cpp_utils.info, tmp_path / "missing.zarr")
    assert expected == actual


def test_info_matches_upstream_for_minimal_v2_image(tmp_path) -> None:
    root = tmp_path / "image.zarr"
    _write_minimal_v2_image(root)

    expected = _run_info(_py_utils.info, root, stats=False)
    actual = _run_info(_cpp_utils.info, root, stats=False)
    assert expected == actual


def test_info_matches_upstream_for_minimal_v2_image_with_stats(tmp_path) -> None:
    root = tmp_path / "image.zarr"
    _write_minimal_v2_image(root)

    expected = _run_info(_py_utils.info, root, stats=True)
    actual = _run_info(_cpp_utils.info, root, stats=True)
    assert expected == actual


def test_info_matches_upstream_for_minimal_v3_image(tmp_path) -> None:
    root = tmp_path / "image-v3.zarr"
    _write_minimal_v3_image(root)

    expected = _run_info(_py_utils.info, root, stats=False)
    actual = _run_info(_cpp_utils.info, root, stats=False)
    assert expected == actual


def test_info_matches_upstream_for_non_ome_zarr_group(tmp_path) -> None:
    root = tmp_path / "plain.zarr"
    _write_plain_zarr_group(root)

    expected = _run_info(_py_utils.info, root, stats=False)
    actual = _run_info(_cpp_utils.info, root, stats=False)
    assert expected == actual
