"""C++-backed port of selected reader helpers from ome-zarr-py."""

from __future__ import annotations

import importlib
import logging
import math
from collections.abc import Iterator
from typing import Any, cast, overload

import dask.array as da
import numpy as np

from . import _core
from ._frozen_upstream import ensure_frozen_upstream_importable
from .axes import Axes
from .format import format_from_version

ensure_frozen_upstream_importable()

ZarrLocation = importlib.import_module("ome_zarr.io").ZarrLocation
JSONDict = importlib.import_module("ome_zarr.types").JSONDict

LOGGER = logging.getLogger("ome_zarr.reader")


class Node:
    """Container for binary data somewhere in the data hierarchy."""

    def __init__(
        self,
        zarr: ZarrLocation,
        root: Node | Reader | list[ZarrLocation],
        visibility: bool = True,
        plate_labels: bool = False,
    ):
        self.zarr = zarr
        self.root = root
        self.seen: list[ZarrLocation] = []
        if isinstance(root, (Node, Reader)):
            self.seen = root.seen
        else:
            self.seen = cast(list[ZarrLocation], root)
        self.__visible = visibility

        self.metadata: JSONDict = dict()
        self.data: list[da.core.Array] = list()
        self.specs: list[Spec] = []
        self.pre_nodes: list[Node] = []
        self.post_nodes: list[Node] = []

        spec_types = {
            "Labels": Labels,
            "Label": Label,
            "Multiscales": Multiscales,
            "OMERO": OMERO,
            "Plate": Plate,
            "Well": Well,
        }
        for spec_name in _core.reader_matching_specs(zarr):
            self.specs.append(spec_types[str(spec_name)](self))

    @overload
    def first(self, spectype: type[Well]) -> Well | None: ...

    @overload
    def first(self, spectype: type[Plate]) -> Plate | None: ...

    def first(self, spectype: type[Spec]) -> Spec | None:
        for spec in self.specs:
            if isinstance(spec, spectype):
                return spec
        return None

    @property
    def visible(self) -> bool:
        return self.__visible

    @visible.setter
    def visible(self, visibility: bool) -> bool:
        old = self.__visible
        if old != visibility:
            self.__visible = visibility
            for node in self.pre_nodes + self.post_nodes:
                node.visible = visibility
        return old

    def load(self, spec_type: type[Spec]) -> Spec | None:
        for spec in self.specs:
            if isinstance(spec, spec_type):
                return spec
        return None

    def add(
        self,
        zarr: ZarrLocation,
        prepend: bool = False,
        visibility: bool | None = None,
        plate_labels: bool = False,
    ) -> Node | None:
        if zarr in self.seen and not plate_labels:
            LOGGER.debug("already seen  %s; stopping recursion", zarr)
            return None

        if visibility is None:
            visibility = self.visible

        self.seen.append(zarr)
        node = Node(zarr, self, visibility=visibility, plate_labels=plate_labels)
        if prepend:
            self.pre_nodes.append(node)
        else:
            self.post_nodes.append(node)

        return node

    def write_metadata(self, metadata: JSONDict) -> None:
        for _spec in self.specs:
            metadata.update(self.zarr.root_attrs)

    def __repr__(self) -> str:
        return str(_core.reader_node_repr(self.zarr, self.visible))


class Spec:
    """Base class for specifications that can be implemented by groups or arrays."""

    @staticmethod
    def matches(zarr: ZarrLocation) -> bool:
        raise NotImplementedError()

    def __init__(self, node: Node) -> None:
        self.node = node
        self.zarr = node.zarr
        LOGGER.debug("treating %s as %s", self.zarr, self.__class__.__name__)
        for k, v in self.zarr.root_attrs.items():
            LOGGER.info("root_attr: %s", k)
            LOGGER.debug(v)

    def lookup(self, key: str, default: Any) -> Any:
        return self.zarr.root_attrs.get(key, default)


class Labels(Spec):
    @staticmethod
    def matches(zarr: ZarrLocation) -> bool:
        return bool("labels" in zarr.root_attrs)

    def __init__(self, node: Node) -> None:
        super().__init__(node)
        for name in _core.reader_labels_names(self.zarr.root_attrs):
            child_zarr = self.zarr.create(name)
            if child_zarr.exists():
                node.add(child_zarr)


class Label(Spec):
    @staticmethod
    def matches(zarr: ZarrLocation) -> bool:
        return bool("image-label" in zarr.root_attrs)

    def __init__(self, node: Node) -> None:
        super().__init__(node)

        payload = dict(
            _core.reader_label_payload(
                self.zarr.root_attrs, self.zarr.basename(), node.visible
            )
        )
        image = payload["parent_image"]
        parent_zarr = None
        if image:
            parent_zarr = self.zarr.create(image)
            if parent_zarr.exists():
                LOGGER.debug("delegating to parent image: %s", parent_zarr)
                node.add(parent_zarr, prepend=True, visibility=False)
            else:
                parent_zarr = None
        if parent_zarr is None:
            LOGGER.warning("no parent found for %s: %s", self, image)

        node.metadata.update(payload["metadata"])
        if payload["properties"]:
            node.metadata.update({"properties": payload["properties"]})


class Multiscales(Spec):
    @staticmethod
    def matches(zarr: ZarrLocation) -> bool:
        return bool(zarr.zgroup) and "multiscales" in zarr.root_attrs

    def __init__(self, node: Node) -> None:
        super().__init__(node)

        payload = dict(_core.reader_multiscales_payload(self.zarr.root_attrs))
        version = payload["version"]
        datasets = payload["datasets"]
        axes = payload["axes"]
        fmt = format_from_version(version)
        axes_obj = Axes(axes, fmt)
        node.metadata["axes"] = axes_obj.to_list()
        node.metadata["name"] = payload["name"]
        self.datasets = list(payload["paths"])
        if "coordinateTransformations" in payload:
            node.metadata["coordinateTransformations"] = payload[
                "coordinateTransformations"
            ]
        LOGGER.info("datasets %s", datasets)

        for resolution in self.datasets:
            data: da.core.Array = self.array(resolution)
            chunk_sizes = [
                str(c[0]) + (f" (+ {c[-1]})" if c[-1] != c[0] else "")
                for c in data.chunks
            ]
            LOGGER.info("resolution: %s", resolution)
            axes_names = None
            if axes is not None:
                axes_names = tuple(
                    axis if isinstance(axis, str) else axis["name"] for axis in axes
                )
            LOGGER.info(" - shape %s = %s", axes_names, data.shape)
            LOGGER.info(" - chunks =  %s", chunk_sizes)
            LOGGER.info(" - dtype = %s", data.dtype)
            node.data.append(data)

        child_zarr = self.zarr.create("labels")
        if child_zarr.exists():
            node.add(child_zarr, visibility=False)

    def array(self, resolution: str) -> da.core.Array:
        return self.zarr.load(resolution)


class OMERO(Spec):
    @staticmethod
    def matches(zarr: ZarrLocation) -> bool:
        return bool("omero" in zarr.root_attrs)

    def __init__(self, node: Node) -> None:
        super().__init__(node)
        self.image_data = self.lookup("omero", {})

        try:
            channels = self.image_data.get("channels", None)
            if channels is None:
                return

            try:
                len(channels)
            except TypeError:
                LOGGER.warning("error counting channels: %s", channels)
                return

            node.metadata.update(
                _core.reader_omero_payload(self.image_data, node.visible)
            )
        except Exception:
            LOGGER.exception("Failed to parse metadata")


class Well(Spec):
    @staticmethod
    def matches(zarr: ZarrLocation) -> bool:
        return bool("well" in zarr.root_attrs)

    def __init__(self, node: Node) -> None:
        super().__init__(node)
        self.well_data = self.lookup("well", {})
        LOGGER.info("well_data: %s", self.well_data)

        image_paths = [image["path"] for image in self.well_data.get("images")]

        field_count = len(image_paths)
        column_count = math.ceil(math.sqrt(field_count))
        row_count = math.ceil(field_count / column_count)

        image_zarr = self.zarr.create(image_paths[0])
        image_node = Node(image_zarr, node)
        self.ds_paths = [
            d["path"] for d in image_zarr.root_attrs["multiscales"][0]["datasets"]
        ]
        x_index = len(image_node.metadata["axes"]) - 1
        y_index = len(image_node.metadata["axes"]) - 2
        self.numpy_type = image_node.data[0].dtype
        self.img_shape = image_node.data[0].shape
        self.img_metadata = image_node.metadata
        self.img_pyramid_shapes = [d.shape for d in image_node.data]

        def get_field(row: int, col: int, level: int) -> da.core.Array:
            field_index = (column_count * row) + col
            data = None
            try:
                if field_index < len(image_paths):
                    image_path = image_paths[field_index]
                    path = f"{image_path}/{self.ds_paths[level]}"
                    data = self.zarr.load(path)
            except ValueError:
                LOGGER.error("Failed to load %s", path)
            if data is None:
                data = da.zeros(self.img_pyramid_shapes[level], dtype=self.numpy_type)
            return data

        def get_lazy_well(level: int, tile_shape: tuple) -> da.Array:
            del tile_shape
            lazy_rows = []
            for row in range(row_count):
                lazy_row: list[da.Array] = []
                for col in range(column_count):
                    LOGGER.debug(
                        "creating lazy_reader. row: %s col: %s level: %s",
                        row,
                        col,
                        level,
                    )
                    lazy_tile = get_field(row, col, level)
                    lazy_row.append(lazy_tile)
                lazy_rows.append(da.concatenate(lazy_row, axis=x_index))
            return da.concatenate(lazy_rows, axis=y_index)

        pyramid = []
        for level, tile_shape in enumerate(self.img_pyramid_shapes):
            lazy_well = get_lazy_well(level, tile_shape)
            pyramid.append(lazy_well)

        node.data = pyramid
        node.metadata = image_node.metadata


class Plate(Spec):
    @staticmethod
    def matches(zarr: ZarrLocation) -> bool:
        return bool("plate" in zarr.root_attrs)

    def __init__(self, node: Node) -> None:
        super().__init__(node)
        LOGGER.debug("Plate created with ZarrLocation fmt: %s", self.zarr.fmt)
        self.get_pyramid_lazy(node)

    def get_pyramid_lazy(self, node: Node) -> None:
        self.plate_data = self.lookup("plate", {})
        LOGGER.info("plate_data: %s", self.plate_data)
        self.rows = self.plate_data.get("rows")
        self.columns = self.plate_data.get("columns")
        self.row_names = [row["name"] for row in self.rows]
        self.col_names = [col["name"] for col in self.columns]

        self.well_paths = [well["path"] for well in self.plate_data.get("wells")]
        self.well_paths.sort()

        self.row_count = len(self.rows)
        self.column_count = len(self.columns)

        well_zarr = self.zarr.create(self.well_paths[0])
        well_node = Node(well_zarr, node)
        well_spec: Well | None = well_node.first(Well)
        if well_spec is None:
            raise Exception("Could not find first well")
        self.first_field_path = well_spec.well_data["images"][0]["path"]
        img0 = self.zarr.create(f"{self.well_paths[0]}/{self.first_field_path}")
        self.img_paths = [
            d["path"] for d in img0.root_attrs["multiscales"][0]["datasets"]
        ]

        self.numpy_type = well_spec.numpy_type
        LOGGER.debug("img_pyramid_shapes: %s", well_spec.img_pyramid_shapes)
        self.axes = well_spec.img_metadata["axes"]

        pyramid = []
        for level, tile_shape in enumerate(well_spec.img_pyramid_shapes):
            lazy_plate = self.get_stitched_grid(level, tile_shape)
            pyramid.append(lazy_plate)

        node.data = pyramid
        node.metadata = well_spec.img_metadata
        node.metadata.update({"metadata": {"plate": self.plate_data}})

    def get_numpy_type(self, image_node: Node) -> np.dtype:
        return image_node.data[0].dtype

    def get_tile_path(self, level: int, row: int, col: int) -> str:
        return (
            f"{self.row_names[row]}/"
            f"{self.col_names[col]}/{self.first_field_path}/{self.img_paths[level]}"
        )

    def get_stitched_grid(self, level: int, tile_shape: tuple) -> da.core.Array:
        LOGGER.debug("get_stitched_grid() level: %s, tile_shape: %s", level, tile_shape)

        def get_tile(row: int, col: int) -> da.core.Array:
            well_path = f"{self.row_names[row]}/{self.col_names[col]}"
            if well_path not in self.well_paths:
                LOGGER.debug("empty well: %s", well_path)
                return np.zeros(tile_shape, dtype=self.numpy_type)

            path = self.get_tile_path(level, row, col)
            LOGGER.debug("creating tile... %s with shape: %s", path, tile_shape)

            try:
                data = self.zarr.load(path)
            except ValueError:
                LOGGER.exception("Failed to load %s", path)
                data = da.zeros(tile_shape, dtype=self.numpy_type)
            return data

        lazy_rows = []
        for row in range(self.row_count):
            lazy_row: list[da.Array] = [
                get_tile(row, col) for col in range(self.column_count)
            ]
            lazy_rows.append(da.concatenate(lazy_row, axis=len(self.axes) - 1))
        return da.concatenate(lazy_rows, axis=len(self.axes) - 2)


class Reader:
    """Parses the given Zarr instance into a collection of Nodes."""

    def __init__(self, zarr: ZarrLocation) -> None:
        assert zarr.exists()
        self.zarr = zarr
        self.seen: list[ZarrLocation] = [zarr]

    def __call__(self) -> Iterator[Node]:
        node = Node(self.zarr, self)
        if node.specs:
            LOGGER.debug("treating %s as ome-zarr", self.zarr)
            yield from self.descend(node)
        elif self.zarr.zarray:
            LOGGER.debug("treating %s as raw zarr", self.zarr)
            node.data.append(self.zarr.load())
            yield node
        else:
            LOGGER.debug("ignoring %s", self.zarr)

    def descend(self, node: Node, depth: int = 0) -> Iterator[Node]:
        for pre_node in node.pre_nodes:
            yield from self.descend(pre_node, depth + 1)

        LOGGER.debug("returning %s", node)
        yield node

        for post_node in node.post_nodes:
            yield from self.descend(post_node, depth + 1)


__all__ = [
    "Label",
    "Labels",
    "Multiscales",
    "Node",
    "OMERO",
    "Plate",
    "Reader",
    "Spec",
    "Well",
]
