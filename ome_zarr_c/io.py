"""C++-backed port of selected I/O helpers from ome-zarr-py."""

from __future__ import annotations

import logging
from pathlib import Path

import dask.array as da
from zarr.storage import FsspecStore, LocalStore, StoreLike

from . import _core
from .format import CurrentFormat, Format

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
        state = dict(_core.io_location_state(path, mode, fmt))
        self.__fmt = state["fmt"]
        self.__mode = str(state["mode"])
        self.__path = str(state["path"])
        self.__store = state["store"]
        self.zgroup = dict(state["zgroup"])
        self.zarray = dict(state["zarray"])
        self.__metadata = dict(state["metadata"])
        self.__exists = bool(state["exists"])
        LOGGER.debug("ZarrLocation.__init__ %s detected: %s", path, self.__fmt)

    def __init_metadata(self) -> None:
        state = dict(_core.io_location_state(self.__store, self.__mode, self.__fmt))
        self.zgroup = dict(state["zgroup"])
        self.zarray = dict(state["zarray"])
        self.__metadata = dict(state["metadata"])
        self.__exists = bool(state["exists"])

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
        return bool(_core.io_is_local_store(self.__store))

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
    if bool(_core.io_parse_url_returns_none(mode, loc.exists())):
        return None
    return loc


__all__ = ["ZarrLocation", "parse_url"]
