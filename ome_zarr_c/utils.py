"""C++-backed port of selected utility helpers from ome-zarr-py."""

from __future__ import annotations

import importlib

from ._frozen_upstream import ensure_frozen_upstream_importable

ensure_frozen_upstream_importable()
_core = importlib.import_module("ome_zarr_c._core")
_io = importlib.import_module("ome_zarr.io")
_reader = importlib.import_module("ome_zarr.reader")

find_multiscales = _core.find_multiscales
finder = _core.finder
splitall = _core.splitall
strip_common_prefix = _core.strip_common_prefix
view = _core.view


def info(path: str, stats: bool = False):
    zarr = _io.parse_url(path)
    assert zarr, f"not a zarr: {zarr}"
    reader = _reader.Reader(zarr)
    for node in reader():
        if not node.specs:
            print(f"not an ome-zarr node: {node}")
            continue

        for line in _core.info_lines(node, stats):
            print(line)
        yield node


__all__ = [
    "find_multiscales",
    "finder",
    "info",
    "splitall",
    "strip_common_prefix",
    "view",
]
