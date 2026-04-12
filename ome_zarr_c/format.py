"""C++-backed port of selected format helpers from ome-zarr-py."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterator, Mapping
from typing import Any

from zarr.storage import FsspecStore, LocalStore

from . import _core

LOGGER = logging.getLogger("ome_zarr_c.format")


def format_from_version(version: str | float) -> Format:
    if isinstance(version, float):
        version = str(version)
    for fmt in format_implementations():
        if fmt.version == version:
            return fmt
    raise ValueError(f"Version {version} not recognized")


def format_implementations() -> Iterator[Format]:
    yield FormatV05()
    yield FormatV04()
    yield FormatV03()
    yield FormatV02()
    yield FormatV01()


def detect_format(metadata: dict, default: Format) -> Format:
    if metadata:
        for fmt in format_implementations():
            if fmt.matches(metadata):
                return fmt
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

    def _get_metadata_version(self, metadata: dict) -> str | None:
        result = _core.get_metadata_version(metadata)
        return None if result is None else str(result)

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


class FormatV01(Format):
    REQUIRED_PLATE_WELL_KEYS: Mapping[str, type] = {"path": str}

    @property
    def version(self) -> str:
        return "0.1"

    @property
    def zarr_format(self) -> int:
        return 2

    @property
    def chunk_key_encoding(self) -> dict[str, str]:
        return {"name": "v2", "separator": "."}

    def matches(self, metadata: dict) -> bool:
        version = self._get_metadata_version(metadata)
        LOGGER.debug("%s matches %s?", self.version, version)
        return version == self.version

    def init_store(self, path: str, mode: str = "r") -> FsspecStore | LocalStore:
        read_only = mode == "r"
        if path.startswith(("http", "s3")):
            return FsspecStore.from_url(
                path,
                storage_options=None,
                read_only=read_only,
            )
        return LocalStore(path, read_only=read_only)

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
    @property
    def version(self) -> str:
        return "0.2"

    @property
    def chunk_key_encoding(self) -> dict[str, str]:
        return {"name": "v2", "separator": "/"}


class FormatV03(FormatV02):
    @property
    def version(self) -> str:
        return "0.3"


class FormatV04(FormatV03):
    REQUIRED_PLATE_WELL_KEYS: Mapping[str, type] = {
        "path": str,
        "rowIndex": int,
        "columnIndex": int,
    }

    @property
    def version(self) -> str:
        return "0.4"

    def generate_well_dict(
        self, well: str, rows: list[str], columns: list[str]
    ) -> dict:
        return dict(_core.generate_well_dict_v04(well, rows, columns))

    def validate_well_dict(
        self, well: dict, rows: list[str], columns: list[str]
    ) -> None:
        _core.validate_well_dict_v04(well, rows, columns)

    def generate_coordinate_transformations(
        self, shapes: list[tuple]
    ) -> list[list[dict[str, Any]]] | None:
        return list(_core.generate_coordinate_transformations(shapes))

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
    @property
    def version(self) -> str:
        return "0.5"

    @property
    def zarr_format(self) -> int:
        return 3

    @property
    def chunk_key_encoding(self) -> dict[str, str]:
        return {"name": "default", "separator": "/"}


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
