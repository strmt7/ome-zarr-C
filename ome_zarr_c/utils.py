"""C++-backed port of selected utility helpers from ome-zarr-py."""

from ._core import splitall, strip_common_prefix

__all__ = ["splitall", "strip_common_prefix"]
