from __future__ import annotations

import copy
import importlib
import posixpath
import sys
from pathlib import Path

import dask.array as da
import numpy as np

from tests._outcomes import err, ok

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_format = importlib.import_module("ome_zarr.format")
_py_reader = importlib.import_module("ome_zarr.reader")
_cpp_reader = importlib.import_module("ome_zarr_c.reader")


class FakeZarr:
    def __init__(
        self,
        hierarchy: FakeHierarchy,
        path: str,
        *,
        root_attrs: dict | None = None,
        zgroup: dict | None = None,
        zarray: dict | None = None,
        exists: bool = True,
        fmt=None,
    ) -> None:
        self._hierarchy = hierarchy
        self._path = hierarchy.normalize(path)
        self._root_attrs = copy.deepcopy(root_attrs or {})
        self.zgroup = copy.deepcopy(zgroup or {})
        self.zarray = copy.deepcopy(zarray or {})
        self._exists = exists
        self.fmt = fmt if fmt is not None else _py_format.FormatV05()

    @property
    def root_attrs(self) -> dict:
        return copy.deepcopy(self._root_attrs)

    def exists(self) -> bool:
        return self._exists

    def basename(self) -> str:
        return self._path.rstrip("/").split("/")[-1]

    def create(self, path: str) -> FakeZarr:
        child_path = self._hierarchy.normalize(posixpath.join(self._path, path))
        return self._hierarchy.nodes.get(
            child_path,
            FakeZarr(self._hierarchy, child_path, exists=False, fmt=self.fmt),
        )

    def load(self, subpath: str = ""):
        full_path = self._path
        if subpath:
            full_path = self._hierarchy.normalize(posixpath.join(self._path, subpath))
        try:
            return self._hierarchy.arrays[full_path]
        except KeyError as exc:
            raise ValueError(full_path) from exc

    def __repr__(self) -> str:
        suffix = ""
        if self.zgroup:
            suffix += " [zgroup]"
        if self.zarray:
            suffix += " [zarray]"
        return f"{self._path}{suffix}"

    def __eq__(self, rhs: object) -> bool:
        return (
            type(self) is type(rhs)
            and isinstance(rhs, FakeZarr)
            and self._path == rhs._path
        )


class FakeHierarchy:
    def __init__(self) -> None:
        self.nodes: dict[str, FakeZarr] = {}
        self.arrays: dict[str, da.Array] = {}

    @staticmethod
    def normalize(path: str) -> str:
        return posixpath.normpath(path.replace("\\", "/"))

    def add(
        self,
        path: str,
        *,
        root_attrs: dict | None = None,
        zgroup: dict | None = None,
        zarray: dict | None = None,
        arrays: dict[str, da.Array] | None = None,
        fmt=None,
    ) -> FakeZarr:
        node = FakeZarr(
            self,
            path,
            root_attrs=root_attrs,
            zgroup=zgroup,
            zarray=zarray,
            fmt=fmt,
        )
        self.nodes[node._path] = node
        for relpath, array in (arrays or {}).items():
            full_path = node._path
            if relpath:
                full_path = self.normalize(posixpath.join(node._path, relpath))
            self.arrays[full_path] = array
        return node


def _normalize(value):
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, dict):
        return {key: _normalize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_normalize(item) for item in value)
    return value


def _array_signature(array: da.Array) -> dict:
    return {
        "shape": tuple(int(dim) for dim in array.shape),
        "dtype": str(array.dtype),
        "chunks": tuple(tuple(int(v) for v in axis) for axis in array.chunks),
        "values": array.compute().tolist(),
    }


def _spec_signature(spec) -> dict:
    signature = {"type": type(spec).__name__}
    for attr in (
        "datasets",
        "row_names",
        "col_names",
        "well_paths",
        "row_count",
        "column_count",
        "first_field_path",
        "img_paths",
    ):
        if hasattr(spec, attr):
            signature[attr] = _normalize(getattr(spec, attr))
    return signature


def _node_signature(node, module) -> dict:
    firsts = {}
    for name in ("Labels", "Label", "Multiscales", "OMERO", "Plate", "Well"):
        spectype = getattr(module, name)
        first = node.first(spectype)
        loaded = node.load(spectype)
        firsts[name] = (
            None if first is None else type(first).__name__,
            None if loaded is None else type(loaded).__name__,
        )

    return {
        "repr": repr(node),
        "visible": node.visible,
        "specs": [_spec_signature(spec) for spec in node.specs],
        "firsts": firsts,
        "metadata": _normalize(node.metadata),
        "data": [_array_signature(array) for array in node.data],
        "pre_nodes": [repr(child) for child in node.pre_nodes],
        "post_nodes": [repr(child) for child in node.post_nodes],
    }


def _reader_signature(module, zarr) -> list[dict]:
    reader = module.Reader(zarr)
    return [_node_signature(node, module) for node in reader()]


def _call(func, *args, **kwargs):
    try:
        return ok(value=func(*args, **kwargs))
    except Exception as exc:  # noqa: BLE001
        return err(exc)


def _build_image_tree() -> FakeHierarchy:
    hierarchy = FakeHierarchy()
    image0 = da.from_array(
        np.arange(12, dtype=np.uint16).reshape(3, 2, 2),
        chunks=(1, 2, 2),
    )
    image1 = da.from_array(
        np.arange(3, dtype=np.uint16).reshape(3, 1, 1),
        chunks=(1, 1, 1),
    )
    label0 = da.from_array(np.array([[0, 1], [2, 3]], dtype=np.uint8), chunks=(2, 2))
    label1 = da.from_array(np.array([[3]], dtype=np.uint8), chunks=(1, 1))

    hierarchy.add(
        "/dataset",
        root_attrs={
            "multiscales": [
                {
                    "version": "0.5",
                    "name": "main-image",
                    "axes": [
                        {"name": "c", "type": "channel"},
                        {"name": "y", "type": "space"},
                        {"name": "x", "type": "space"},
                    ],
                    "datasets": [
                        {
                            "path": "0",
                            "coordinateTransformations": [
                                {"type": "scale", "scale": [1, 1, 1]}
                            ],
                        },
                        {
                            "path": "1",
                            "coordinateTransformations": [
                                {"type": "scale", "scale": [1, 2, 2]}
                            ],
                        },
                    ],
                }
            ],
            "omero": {
                "rdefs": {"model": "greyscale"},
                "channels": [
                    {
                        "color": "FF0000",
                        "label": "red",
                        "active": 1,
                        "window": {"start": 0, "end": 255},
                    },
                    {
                        "color": "00FF00",
                        "label": "green",
                        "active": 0,
                        "window": {"start": 5, "end": 10},
                    },
                ],
            },
        },
        zgroup={"version": "0.5"},
        arrays={"0": image0, "1": image1},
    )
    hierarchy.add(
        "/dataset/labels",
        root_attrs={"labels": ["coins"]},
        zgroup={"version": "0.5"},
    )
    hierarchy.add(
        "/dataset/labels/coins",
        root_attrs={
            "image-label": {
                "source": {"image": "../../"},
                "colors": [
                    {"label-value": 1, "rgba": [255, 0, 0, 255]},
                    {"label-value": True, "rgba": [0, 255, 0, 255]},
                    {"label-value": "bad", "rgba": [0, 0, 0, 0]},
                ],
                "properties": [
                    {"label-value": 1, "name": "cell"},
                    {"label-value": 2, "name": "background"},
                ],
            },
            "image": {"source": "synthetic"},
            "multiscales": [
                {
                    "version": "0.5",
                    "name": "coins",
                    "axes": ["y", "x"],
                    "datasets": [{"path": "0"}, {"path": "1"}],
                }
            ],
        },
        zgroup={"version": "0.5"},
        arrays={"0": label0, "1": label1},
    )
    return hierarchy


def _build_hcs_tree() -> FakeHierarchy:
    hierarchy = FakeHierarchy()
    image0 = da.from_array(np.arange(6, dtype=np.uint16).reshape(2, 3), chunks=(2, 3))
    image1 = da.from_array(np.arange(2, dtype=np.uint16).reshape(1, 2), chunks=(1, 2))

    hierarchy.add(
        "/plate",
        root_attrs={
            "plate": {
                "rows": [{"name": "A"}],
                "columns": [{"name": "1"}],
                "wells": [{"path": "A/1"}],
            }
        },
        zgroup={"version": "0.4"},
    )
    hierarchy.add(
        "/plate/A/1",
        root_attrs={"well": {"images": [{"path": "0"}]}},
        zgroup={"version": "0.4"},
    )
    hierarchy.add(
        "/plate/A/1/0",
        root_attrs={
            "multiscales": [
                {
                    "version": "0.4",
                    "name": "field-0",
                    "axes": ["y", "x"],
                    "datasets": [{"path": "0"}, {"path": "1"}],
                }
            ]
        },
        zgroup={"version": "0.4"},
        arrays={"0": image0, "1": image1},
    )
    return hierarchy


def _build_raw_and_ignored_tree() -> tuple[FakeHierarchy, FakeZarr, FakeZarr]:
    hierarchy = FakeHierarchy()
    raw = hierarchy.add(
        "/raw-array",
        zarray={"shape": [2, 2]},
        arrays={
            "": da.from_array(
                np.arange(4, dtype=np.uint8).reshape(2, 2),
                chunks=(2, 2),
            )
        },
    )
    ignored = hierarchy.add("/ignored", zgroup={"version": "0.5"})
    return hierarchy, raw, ignored


def test_reader_matches_upstream_for_image_label_tree() -> None:
    hierarchy = _build_image_tree()
    zarr = hierarchy.nodes["/dataset"]

    expected = _call(_reader_signature, _py_reader, zarr)
    actual = _call(_reader_signature, _cpp_reader, zarr)
    assert expected == actual


def test_node_matches_upstream_for_visibility_and_duplicate_add() -> None:
    hierarchy = _build_image_tree()
    zarr = hierarchy.nodes["/dataset"]

    def exercise(module):
        node = module.Node(zarr, [])
        before = _node_signature(node, module)
        node.visible = False
        hidden = _node_signature(node, module)
        node.visible = True
        shown = _node_signature(node, module)
        duplicate = node.add(hierarchy.nodes["/dataset/labels"])
        return {
            "before": before,
            "hidden": hidden,
            "shown": shown,
            "duplicate_add": None if duplicate is None else repr(duplicate),
        }

    expected = _call(exercise, _py_reader)
    actual = _call(exercise, _cpp_reader)
    assert expected == actual


def test_reader_matches_upstream_for_omero_edge_cases() -> None:
    cases = [
        {"omero": {"channels": None}},
        {"omero": {"channels": object()}},
        {"omero": {"channels": [{"color": "GG0000"}]}},
        {
            "omero": {
                "rdefs": {"model": "color"},
                "channels": [
                    {"active": "", "window": {"start": 1, "end": 2}},
                    {"active": 0, "window": {"start": 3}},
                ],
            }
        },
    ]

    for index, root_attrs in enumerate(cases):
        hierarchy = FakeHierarchy()
        zarr = hierarchy.add(
            f"/edge-{index}",
            root_attrs=root_attrs,
            zgroup={"version": "0.5"},
        )

        expected = _call(_node_signature, _py_reader.Node(zarr, []), _py_reader)
        actual = _call(_node_signature, _cpp_reader.Node(zarr, []), _cpp_reader)
        assert expected == actual


def test_reader_matches_upstream_for_fake_hcs_tree() -> None:
    hierarchy = _build_hcs_tree()
    plate = hierarchy.nodes["/plate"]
    well = hierarchy.nodes["/plate/A/1"]

    expected_plate = _call(_node_signature, _py_reader.Node(plate, []), _py_reader)
    actual_plate = _call(_node_signature, _cpp_reader.Node(plate, []), _cpp_reader)
    assert expected_plate == actual_plate

    expected_well = _call(_node_signature, _py_reader.Node(well, []), _py_reader)
    actual_well = _call(_node_signature, _cpp_reader.Node(well, []), _cpp_reader)
    assert expected_well == actual_well


def test_reader_matches_upstream_for_raw_zarray_and_ignored_nodes() -> None:
    _, raw, ignored = _build_raw_and_ignored_tree()

    expected_raw = _call(_reader_signature, _py_reader, raw)
    actual_raw = _call(_reader_signature, _cpp_reader, raw)
    assert expected_raw == actual_raw

    expected_ignored = _call(_reader_signature, _py_reader, ignored)
    actual_ignored = _call(_reader_signature, _cpp_reader, ignored)
    assert expected_ignored == actual_ignored
