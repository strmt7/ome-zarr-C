"""C++-backed port of selected synthetic data helpers from ome-zarr-py."""

from collections.abc import Callable

import zarr

from ._core import data_astronaut as astronaut
from ._core import data_coins as coins
from ._core import data_create_zarr as _data_create_zarr
from ._core import data_make_circle as make_circle
from ._core import data_rgb_to_5d as rgb_to_5d
from .format import CurrentFormat, Format

CHANNEL_DIMENSION = 1
_DEFAULT_FORMAT = CurrentFormat()


def create_zarr(
    zarr_directory: str,
    method: Callable[..., tuple[list, list]] = coins,
    label_name: str = "coins",
    fmt: Format = _DEFAULT_FORMAT,
    chunks: tuple | list | None = None,
) -> zarr.Group:
    return _data_create_zarr(zarr_directory, method, label_name, fmt, chunks)


__all__ = [
    "CHANNEL_DIMENSION",
    "astronaut",
    "coins",
    "create_zarr",
    "make_circle",
    "rgb_to_5d",
]
