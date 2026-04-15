"""C++-backed port of selected non-runtime utility helpers from ome-zarr-py."""

from __future__ import annotations

import importlib

from ._frozen_upstream import ensure_frozen_upstream_importable

ensure_frozen_upstream_importable()
_core = importlib.import_module("ome_zarr_c._core")

find_multiscales = _core.find_multiscales
splitall = _core.splitall
strip_common_prefix = _core.strip_common_prefix


__all__ = [
    "find_multiscales",
    "splitall",
    "strip_common_prefix",
]
