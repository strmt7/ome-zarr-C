from __future__ import annotations

import copy
import importlib
import sys
from pathlib import Path

import dask
import dask.array as da
import numpy as np
import zarr

from tests._outcomes import err, ok

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_format = importlib.import_module("ome_zarr.format")
_py_writer = importlib.import_module("ome_zarr.writer")
_cpp_format = importlib.import_module("ome_zarr_c.format")
_cpp_writer = importlib.import_module("ome_zarr_c.writer")


def _snapshot_tree(root: Path):
    snapshot = []
    for path in sorted(root.rglob("*")):
        rel_path = path.relative_to(root).as_posix()
        if path.is_file():
            snapshot.append(("file", rel_path, path.read_bytes()))
        else:
            snapshot.append(("dir", rel_path, None))
    return snapshot


def _run_tree_call(func, root: Path, *args, signature=None, **kwargs):
    try:
        result = func(*args, **kwargs)
        payload = {"tree": _snapshot_tree(root)}
        if signature is not None:
            payload["value"] = signature(result)
        return ok(**payload)
    except Exception as exc:  # noqa: BLE001
        return err(exc, tree=_snapshot_tree(root))


def _fmt_pair(version: str):
    if version == "0.4":
        return _py_format.FormatV04(), _cpp_format.FormatV04(), 2
    return _py_format.FormatV05(), _cpp_format.FormatV05(), 3


def _signature_group_fmt(result):
    group, fmt = result
    return {
        "group_name": group.name,
        "group_zarr_format": int(group.info._zarr_format),
        "fmt_version": fmt.version,
        "fmt_zarr_format": int(fmt.zarr_format),
    }


def test_check_format_matches_upstream(tmp_path) -> None:
    for version in ("0.4", "0.5"):
        py_fmt, cpp_fmt, zarr_format = _fmt_pair(version)
        root = zarr.open_group(
            str(tmp_path / f"writer-check-format-{version}.zarr"),
            mode="w",
            zarr_format=zarr_format,
        )
        assert (
            _py_writer.check_format(root, py_fmt).version
            == _cpp_writer.check_format(root, cpp_fmt).version
        )


def test_check_group_fmt_matches_upstream_for_path_inputs(tmp_path) -> None:
    for version in ("0.4", "0.5"):
        py_fmt, cpp_fmt, _ = _fmt_pair(version)
        py_root = tmp_path / f"py-{version}.zarr"
        cpp_root = tmp_path / f"cpp-{version}.zarr"

        expected = _run_tree_call(
            _py_writer.check_group_fmt,
            py_root,
            str(py_root),
            py_fmt,
            signature=_signature_group_fmt,
        )
        actual = _run_tree_call(
            _cpp_writer.check_group_fmt,
            cpp_root,
            str(cpp_root),
            cpp_fmt,
            signature=_signature_group_fmt,
        )
        assert expected == actual


def test_get_and_add_metadata_match_upstream(tmp_path) -> None:
    for version in ("0.4", "0.5"):
        py_fmt, cpp_fmt, zarr_format = _fmt_pair(version)
        py_root = tmp_path / f"py-meta-{version}.zarr"
        cpp_root = tmp_path / f"cpp-meta-{version}.zarr"
        py_group = zarr.open_group(str(py_root), mode="w", zarr_format=zarr_format)
        cpp_group = zarr.open_group(str(cpp_root), mode="w", zarr_format=zarr_format)

        metadata = {
            "alpha": {"one": 1},
            "beta": ["x", "y"],
        }
        _py_writer.add_metadata(py_group, copy.deepcopy(metadata), py_fmt)
        _cpp_writer.add_metadata(cpp_group, copy.deepcopy(metadata), cpp_fmt)

        assert _py_writer.get_metadata(py_group) == _cpp_writer.get_metadata(cpp_group)
        assert _snapshot_tree(py_root) == _snapshot_tree(cpp_root)


def test_write_multiscales_metadata_matches_upstream(tmp_path) -> None:
    datasets = [
        {
            "path": "s0",
            "coordinateTransformations": [{"type": "scale", "scale": [1, 1]}],
        },
        {
            "path": "s1",
            "coordinateTransformations": [{"type": "scale", "scale": [2, 2]}],
        },
    ]
    metadata = {
        "metadata": {
            "omero": {
                "channels": [
                    {
                        "color": "FF0000",
                        "window": {"start": 0, "end": 255, "min": 0, "max": 255},
                    }
                ]
            }
        }
    }
    for version in ("0.4", "0.5"):
        py_fmt, cpp_fmt, zarr_format = _fmt_pair(version)
        py_root = tmp_path / f"py-ms-{version}.zarr"
        cpp_root = tmp_path / f"cpp-ms-{version}.zarr"
        py_group = zarr.open_group(str(py_root), mode="w", zarr_format=zarr_format)
        cpp_group = zarr.open_group(str(cpp_root), mode="w", zarr_format=zarr_format)

        expected = _run_tree_call(
            _py_writer.write_multiscales_metadata,
            py_root,
            py_group,
            copy.deepcopy(datasets),
            py_fmt,
            ["y", "x"],
            "sample",
            **copy.deepcopy(metadata),
        )
        actual = _run_tree_call(
            _cpp_writer.write_multiscales_metadata,
            cpp_root,
            cpp_group,
            copy.deepcopy(datasets),
            cpp_fmt,
            ["y", "x"],
            "sample",
            **copy.deepcopy(metadata),
        )
        assert expected == actual


def test_write_plate_metadata_matches_upstream(tmp_path) -> None:
    rows = ["A", "B"]
    columns = ["1", "2"]
    wells = ["A/1", "B/2"]
    acquisitions = [{"id": 0, "name": "scan"}]

    for version in ("0.4", "0.5"):
        py_fmt, cpp_fmt, zarr_format = _fmt_pair(version)
        py_root = tmp_path / f"py-plate-{version}.zarr"
        cpp_root = tmp_path / f"cpp-plate-{version}.zarr"
        py_group = zarr.open_group(str(py_root), mode="w", zarr_format=zarr_format)
        cpp_group = zarr.open_group(str(cpp_root), mode="w", zarr_format=zarr_format)

        expected = _run_tree_call(
            _py_writer.write_plate_metadata,
            py_root,
            py_group,
            rows,
            columns,
            copy.deepcopy(wells),
            py_fmt,
            copy.deepcopy(acquisitions),
            3,
            "plate-a",
        )
        actual = _run_tree_call(
            _cpp_writer.write_plate_metadata,
            cpp_root,
            cpp_group,
            rows,
            columns,
            copy.deepcopy(wells),
            cpp_fmt,
            copy.deepcopy(acquisitions),
            3,
            "plate-a",
        )
        assert expected == actual


def test_write_well_metadata_matches_upstream(tmp_path) -> None:
    images = [{"path": "0"}, {"path": "1", "acquisition": 3}]

    for version in ("0.4", "0.5"):
        py_fmt, cpp_fmt, zarr_format = _fmt_pair(version)
        py_root = tmp_path / f"py-well-{version}.zarr"
        cpp_root = tmp_path / f"cpp-well-{version}.zarr"
        py_group = zarr.open_group(str(py_root), mode="w", zarr_format=zarr_format)
        cpp_group = zarr.open_group(str(cpp_root), mode="w", zarr_format=zarr_format)

        expected = _run_tree_call(
            _py_writer.write_well_metadata,
            py_root,
            py_group,
            copy.deepcopy(images),
            py_fmt,
        )
        actual = _run_tree_call(
            _cpp_writer.write_well_metadata,
            cpp_root,
            cpp_group,
            copy.deepcopy(images),
            cpp_fmt,
        )
        assert expected == actual


def test_write_label_metadata_matches_upstream(tmp_path) -> None:
    colors = [{"label-value": 1, "rgba": [255, 0, 0, 255]}]
    properties = [{"label-value": 1, "class": "cell"}]

    for version in ("0.4", "0.5"):
        py_fmt, cpp_fmt, zarr_format = _fmt_pair(version)
        py_root = tmp_path / f"py-label-{version}.zarr"
        cpp_root = tmp_path / f"cpp-label-{version}.zarr"
        py_group = zarr.open_group(str(py_root), mode="w", zarr_format=zarr_format)
        cpp_group = zarr.open_group(str(cpp_root), mode="w", zarr_format=zarr_format)
        py_labels = py_group.require_group("labels")
        cpp_labels = cpp_group.require_group("labels")
        py_labels.require_group("cells")
        cpp_labels.require_group("cells")

        expected = _run_tree_call(
            _py_writer.write_label_metadata,
            py_root,
            py_labels,
            "cells",
            copy.deepcopy(colors),
            copy.deepcopy(properties),
            py_fmt,
        )
        actual = _run_tree_call(
            _cpp_writer.write_label_metadata,
            cpp_root,
            cpp_labels,
            "cells",
            copy.deepcopy(colors),
            copy.deepcopy(properties),
            cpp_fmt,
        )
        assert expected == actual


def test_write_multiscale_roundtrip_v4_v5(tmp_path) -> None:
    for version in ("0.4", "0.5"):
        _, cpp_fmt, zarr_format = _fmt_pair(version)
        root = tmp_path / f"write-ms-{version}.zarr"
        group = zarr.open_group(str(root), mode="w", zarr_format=zarr_format)
        pyramid = [
            np.arange(16, dtype=np.uint16).reshape(4, 4),
            np.arange(4, dtype=np.uint16).reshape(2, 2),
        ]

        delayed = _cpp_writer.write_multiscale(
            copy.deepcopy(pyramid),
            group,
            fmt=cpp_fmt,
            axes=["y", "x"],
            coordinate_transformations=[
                [{"type": "scale", "scale": [1, 1]}],
                [{"type": "scale", "scale": [2, 2]}],
            ],
            compute=True,
        )
        assert delayed == []

        metadata = _cpp_writer.get_metadata(group)
        assert metadata["multiscales"][0]["datasets"] == [
            {
                "path": "s0",
                "coordinateTransformations": [{"type": "scale", "scale": [1, 1]}],
            },
            {
                "path": "s1",
                "coordinateTransformations": [{"type": "scale", "scale": [2, 2]}],
            },
        ]
        assert group["s0"][:].tolist() == pyramid[0].tolist()
        assert group["s1"][:].tolist() == pyramid[1].tolist()


def test_write_pyramid_to_zarr_compute_false_writes_after_dask_compute(
    tmp_path,
) -> None:
    root = tmp_path / "write-pyramid-delayed.zarr"
    group = zarr.open_group(str(root), mode="w", zarr_format=3)
    pyramid = [
        da.from_array(np.arange(16, dtype=np.uint16).reshape(4, 4), chunks=(2, 2)),
        da.from_array(np.arange(4, dtype=np.uint16).reshape(2, 2), chunks=(2, 2)),
    ]

    delayed = _cpp_writer._write_pyramid_to_zarr(
        pyramid,
        group,
        fmt=_cpp_format.FormatV05(),
        axes=["y", "x"],
        coordinate_transformations=[
            [{"type": "scale", "scale": [1, 1]}],
            [{"type": "scale", "scale": [2, 2]}],
        ],
        compute=False,
    )
    assert len(delayed) == 3
    dask.compute(*delayed)

    metadata = _cpp_writer.get_metadata(group)
    assert len(metadata["multiscales"][0]["datasets"]) == 2
    assert (
        group["s0"][:].tolist() == np.arange(16, dtype=np.uint16).reshape(4, 4).tolist()
    )
    assert (
        group["s1"][:].tolist() == np.arange(4, dtype=np.uint16).reshape(2, 2).tolist()
    )


def test_write_multiscale_labels_roundtrip_v5(tmp_path) -> None:
    root = tmp_path / "write-label-pyramid.zarr"
    group = zarr.open_group(str(root), mode="w", zarr_format=3)
    pyramid = [
        np.array([[0, 1], [2, 3]], dtype=np.uint8),
        np.array([[3]], dtype=np.uint8),
    ]

    _cpp_writer.write_multiscale_labels(
        copy.deepcopy(pyramid),
        group,
        "cells",
        fmt=_cpp_format.FormatV05(),
        axes=["y", "x"],
        coordinate_transformations=[
            [{"type": "scale", "scale": [1, 1]}],
            [{"type": "scale", "scale": [2, 2]}],
        ],
        label_metadata={"colors": [{"label-value": 1, "rgba": [255, 0, 0, 255]}]},
        compute=True,
    )

    labels_meta = _cpp_writer.get_metadata(group["labels"])
    assert labels_meta["labels"] == ["cells"]
    assert group["labels/cells/s0"][:].tolist() == pyramid[0].tolist()
    assert group["labels/cells/s1"][:].tolist() == pyramid[1].tolist()


def test_write_labels_roundtrip_v5(tmp_path) -> None:
    root = tmp_path / "write-labels.zarr"
    group = zarr.open_group(str(root), mode="w", zarr_format=3)
    labels = np.arange(64, dtype=np.uint8).reshape(8, 8)

    _cpp_writer.write_labels(
        labels,
        group,
        "cells",
        scaler=None,
        scale_factors=(2,),
        method=_cpp_writer.Methods.NEAREST,
        fmt=_cpp_format.FormatV05(),
        axes=["y", "x"],
        label_metadata={"properties": [{"label-value": 1, "class": "cell"}]},
        compute=True,
    )

    labels_meta = _cpp_writer.get_metadata(group["labels"])
    assert labels_meta["labels"] == ["cells"]
    label_group = group["labels/cells"]
    assert label_group["s0"][:].tolist() == labels.tolist()
    assert label_group["s1"].shape == (4, 4)
