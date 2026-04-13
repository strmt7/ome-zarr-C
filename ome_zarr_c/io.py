"""C++-backed port of selected I/O helpers from ome-zarr-py."""

from __future__ import annotations

import logging
from pathlib import Path

import dask.array as da
import zarr
from zarr.storage import FsspecStore, LocalStore, StoreLike

from . import _core
from .format import CurrentFormat, Format, detect_format

LOGGER = logging.getLogger("ome_zarr.io")
_DEFAULT_FORMAT = CurrentFormat()


class ZarrLocation:
    """IO primitive for reading and writing Zarr data."""

    def __init__(
        self,
        path: StoreLike,
        mode: str = "r",
        fmt: Format = _DEFAULT_FORMAT,
    ) -> None:
        LOGGER.debug("ZarrLocation.__init__ path: %s, fmt: %s", path, fmt.version)
        self.__fmt = fmt
        self.__mode = mode
        if isinstance(path, Path):
            self.__path = str(path.resolve())
        elif isinstance(path, str):
            self.__path = path
        elif isinstance(path, FsspecStore):
            self.__path = path.path
        elif isinstance(path, LocalStore):
            self.__path = str(path.root)
        else:
            raise TypeError(f"not expecting: {type(path)}")

        loader = fmt
        self.__store: FsspecStore | LocalStore = (
            path
            if isinstance(path, (FsspecStore, LocalStore))
            else loader.init_store(self.__path, mode)
        )
        self.__init_metadata()
        detected = detect_format(self.__metadata, loader)
        LOGGER.debug("ZarrLocation.__init__ %s detected: %s", path, detected)
        if detected != fmt:
            LOGGER.warning(
                "version mismatch: detected: %s, requested: %s", detected, fmt
            )
            self.__fmt = detected
            self.__store = detected.init_store(self.__path, mode)
            self.__init_metadata()

    def __init_metadata(self) -> None:
        self.zgroup: dict = {}
        self.zarray: dict = {}
        self.__metadata: dict = {}
        self.__exists = True
        zarr_format = None
        try:
            group = zarr.open_group(
                store=self.__store, path="/", mode="r", zarr_format=zarr_format
            )
            self.zgroup = group.attrs.asdict()
            if "ome" in self.zgroup:
                self.zgroup = self.zgroup["ome"]
            self.__metadata = self.zgroup
        except (ValueError, FileNotFoundError):
            if self.__mode == "w":
                zarr_format = self.__fmt.zarr_format
                zarr.open_group(
                    store=self.__store, path="/", mode="w", zarr_format=zarr_format
                )
            else:
                self.__exists = False

    def __repr__(self) -> str:
        return str(
            _core.io_repr(self.subpath(""), bool(self.zgroup), bool(self.zarray))
        )

    def exists(self) -> bool:
        return self.__exists

    @property
    def fmt(self) -> Format:
        return self.__fmt

    @property
    def mode(self) -> str:
        return self.__mode

    @property
    def version(self) -> str:
        return self.__fmt.version

    @property
    def path(self) -> str:
        return self.__path

    @property
    def store(self) -> FsspecStore | LocalStore:
        return self.__store

    @property
    def root_attrs(self) -> dict:
        return dict(self.__metadata)

    def load(self, subpath: str = "") -> da.core.Array:
        return da.from_zarr(self.__store, subpath)

    def __eq__(self, rhs: object) -> bool:
        if type(self) is not type(rhs):
            return False
        if not isinstance(rhs, ZarrLocation):
            return False
        return self.subpath() == rhs.subpath()

    def basename(self) -> str:
        return str(_core.io_basename(self.__path))

    def create(self, path: str) -> ZarrLocation:
        subpath = self.subpath(path)
        LOGGER.debug("open(%s(%s))", self.__class__.__name__, subpath)
        return self.__class__(subpath, mode=self.__mode, fmt=self.__fmt)

    def parts(self) -> list[str]:
        return list(_core.io_parts(self.__path, self._isfile()))

    def subpath(self, subpath: str = "") -> str:
        return str(
            _core.io_subpath(
                self.__path,
                subpath,
                self._isfile(),
                self._ishttp(),
            )
        )

    def _isfile(self) -> bool:
        return isinstance(self.__store, LocalStore)

    def _ishttp(self) -> bool:
        if isinstance(self.__store, LocalStore) or not hasattr(self.__store, "fs"):
            return False
        return bool(_core.io_protocol_is_http(self.__store.fs.protocol))


def parse_url(
    path: Path | str,
    mode: str = "r",
    fmt: Format = _DEFAULT_FORMAT,
) -> ZarrLocation | None:
    """Convert a path string or URL to a ZarrLocation subclass."""

    loc = ZarrLocation(path, mode=mode, fmt=fmt)
    if "r" in mode and not loc.exists():
        return None
    return loc


__all__ = ["ZarrLocation", "parse_url"]
