"""C++-backed port of selected format helpers from ome-zarr-py."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from typing import Any

from zarr.storage import FsspecStore, LocalStore

from . import _core

LOGGER = logging.getLogger("ome_zarr_c.format")

_FORMAT_CLASS_BY_VERSION: dict[str, type[Format]] = {}


def format_from_version(version: str | float) -> Format:
    if isinstance(version, float):
        version = str(version)
    try:
        return _format_class_for_version(str(version))()
    except KeyError as exc:
        raise ValueError(f"Version {version} not recognized") from exc


def format_implementations() -> Iterator[Format]:
    for cls in (FormatV05, FormatV04, FormatV03, FormatV02, FormatV01):
        yield cls()


def detect_format(metadata: dict, default: Format) -> Format:
    if metadata:
        detected = _core.get_metadata_version(metadata)
        if isinstance(detected, str):
            cls = _FORMAT_CLASS_BY_VERSION.get(detected)
            if cls is not None:
                return cls()
    return default


class Format(ABC):
    @property
    @abstractmethod
    def version(self) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def zarr_format(self) -> int:
        raise NotImplementedError()

    @property
    @abstractmethod
    def chunk_key_encoding(self) -> dict[str, str]:
        raise NotImplementedError()

    @abstractmethod
    def matches(self, metadata: dict) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def init_store(self, path: str, mode: str = "r") -> FsspecStore | LocalStore:
        raise NotImplementedError()

    def init_channels(self) -> None:
        raise NotImplementedError()

    def _get_metadata_version(self, metadata: dict):
        return _core.get_metadata_version(metadata)

    def __repr__(self) -> str:
        return self.__class__.__name__

    def __eq__(self, other: object) -> bool:
        return self.__class__ == other.__class__

    @abstractmethod
    def generate_well_dict(
        self, well: str, rows: list[str], columns: list[str]
    ) -> dict:
        raise NotImplementedError()

    @abstractmethod
    def validate_well_dict(
        self, well: dict, rows: list[str], columns: list[str]
    ) -> None:
        raise NotImplementedError()

    @abstractmethod
    def generate_coordinate_transformations(
        self, shapes: list[tuple]
    ) -> list[list[dict[str, Any]]] | None:
        raise NotImplementedError()

    @abstractmethod
    def validate_coordinate_transformations(
        self,
        ndim: int,
        nlevels: int,
        coordinate_transformations: list[list[dict[str, Any]]] | None = None,
    ) -> list[list[dict[str, Any]]] | None:
        raise NotImplementedError()


class _VersionedFormat(Format):
    _VERSION: str

    @property
    def version(self) -> str:
        return self._VERSION

    @property
    def zarr_format(self) -> int:
        return int(_core.format_zarr_format(self._VERSION))

    @property
    def chunk_key_encoding(self) -> dict[str, str]:
        return dict(_core.format_chunk_key_encoding(self._VERSION))

    def matches(self, metadata: dict) -> bool:
        version = self._get_metadata_version(metadata)
        LOGGER.debug("%s matches %s?", self.version, version)
        return version == self.version


class FormatV01(_VersionedFormat):
    REQUIRED_PLATE_WELL_KEYS: Mapping[str, type] = {"path": str}
    _VERSION = "0.1"

    def init_store(self, path: str, mode: str = "r") -> FsspecStore | LocalStore:
        return _core.format_init_store(path, mode)

    def generate_well_dict(
        self, well: str, rows: list[str], columns: list[str]
    ) -> dict:
        del rows, columns
        return {"path": str(well)}

    def validate_well_dict(
        self, well: dict, rows: list[str], columns: list[str]
    ) -> None:
        del rows, columns
        _core.validate_well_dict_v01(well)

    def generate_coordinate_transformations(
        self, shapes: list[tuple]
    ) -> list[list[dict[str, Any]]] | None:
        del shapes
        return None

    def validate_coordinate_transformations(
        self,
        ndim: int,
        nlevels: int,
        coordinate_transformations: list[list[dict[str, Any]]] | None = None,
    ) -> None:
        del ndim, nlevels, coordinate_transformations
        return None


class FormatV02(FormatV01):
    _VERSION = "0.2"


class FormatV03(FormatV02):
    _VERSION = "0.3"


class FormatV04(FormatV03):
    REQUIRED_PLATE_WELL_KEYS: Mapping[str, type] = {
        "path": str,
        "rowIndex": int,
        "columnIndex": int,
    }
    _VERSION = "0.4"

    def generate_well_dict(
        self, well: str, rows: list[str], columns: list[str]
    ) -> dict:
        return _core.generate_well_dict_v04(well, rows, columns)

    def validate_well_dict(
        self, well: dict, rows: list[str], columns: list[str]
    ) -> None:
        _core.validate_well_dict_v04(well, rows, columns)

    def generate_coordinate_transformations(
        self, shapes: list[tuple]
    ) -> list[list[dict[str, Any]]] | None:
        return _core.generate_coordinate_transformations(shapes)

    def validate_coordinate_transformations(
        self,
        ndim: int,
        nlevels: int,
        coordinate_transformations: list[list[dict[str, Any]]] | None = None,
    ) -> None:
        _core.validate_coordinate_transformations(
            ndim, nlevels, coordinate_transformations
        )


class FormatV05(FormatV04):
    _VERSION = "0.5"


def _format_class_for_version(version: str) -> type[Format]:
    return _FORMAT_CLASS_BY_VERSION[version]


_FORMAT_CLASS_BY_VERSION.update(
    {
        "0.1": FormatV01,
        "0.2": FormatV02,
        "0.3": FormatV03,
        "0.4": FormatV04,
        "0.5": FormatV05,
    }
)


CurrentFormat = FormatV05

__all__ = [
    "CurrentFormat",
    "Format",
    "FormatV01",
    "FormatV02",
    "FormatV03",
    "FormatV04",
    "FormatV05",
    "detect_format",
    "format_from_version",
    "format_implementations",
]
