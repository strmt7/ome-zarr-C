"""C++-backed port of selected utility helpers from ome-zarr-py."""

from __future__ import annotations

import csv
import http.server as http_server
import importlib
import logging
import webbrowser
from pathlib import Path

import dask.array as da
import zarr
from dask.diagnostics import ProgressBar
from RangeHTTPServer import RangeRequestHandler

from . import io as _io
from ._frozen_upstream import ensure_frozen_upstream_importable
from .format import format_from_version

ensure_frozen_upstream_importable()
_core = importlib.import_module("ome_zarr_c._core")
_reader = importlib.import_module("ome_zarr_c.reader")

LOGGER = logging.getLogger("ome_zarr.utils")

find_multiscales = _core.find_multiscales
splitall = _core.splitall
strip_common_prefix = _core.strip_common_prefix


def info(path: str, stats: bool = False):
    zarr = _io.parse_url(path)
    assert zarr, f"not a zarr: {zarr}"
    reader = _reader.Reader(zarr)
    for node in reader():
        if not node.specs:
            print(str(_core.utils_info_not_ome_zarr_line(node)))
            continue

        for line in _core.info_lines(node, stats):
            print(line)
        yield node


def view(
    input_path: str, port: int = 8000, dry_run: bool = False, force: bool = False
) -> None:
    plan = dict(_core.utils_view_plan(input_path, port, force))
    if bool(plan["should_warn"]):
        print(str(plan["warning_message"]))
        return

    parent_dir = str(plan["parent_dir"])
    url = str(plan["url"])

    class CORSRequestHandler(RangeRequestHandler):
        def end_headers(self) -> None:
            self.send_header("Access-Control-Allow-Origin", "*")
            http_server.SimpleHTTPRequestHandler.end_headers(self)

        def translate_path(self, path: str) -> str:
            self.directory = parent_dir
            super_path = super().translate_path(path)
            return super_path

    if dry_run:
        return

    webbrowser.open(url)
    http_server.test(CORSRequestHandler, http_server.HTTPServer, port=port)


def finder(input_path: str, port: int = 8000, dry_run: bool = False) -> None:
    zarrs = list(_core.utils_finder_discover_images(input_path))
    if len(zarrs) == 0:
        print("No OME-Zarr files found in", input_path)
        return

    plan = dict(_core.utils_finder_plan(input_path, port))
    parent_path = str(plan["parent_path"])
    server_dir = str(plan["server_dir"])
    col_names = ["File Path", "File Name", "Folders", "Uploaded"]
    rows = list(_core.utils_finder_rows(input_path, port, zarrs, server_dir))
    bff_csv = str(plan["csv_path"])
    with open(bff_csv, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=",")
        writer.writerow(col_names)
        writer.writerows(rows)

    url = str(plan["url"])

    class CORSRequestHandler(RangeRequestHandler):
        def end_headers(self) -> None:
            self.send_header("Access-Control-Allow-Origin", "*")
            http_server.SimpleHTTPRequestHandler.end_headers(self)

        def translate_path(self, path: str) -> str:
            self.directory = parent_path
            super_path = super().translate_path(path)
            return super_path

    if dry_run:
        return

    webbrowser.open(url)
    http_server.test(CORSRequestHandler, http_server.HTTPServer, port=port)


def download(input_path: str, output_dir: str = ".") -> None:
    """Download an OME-Zarr from the given path."""

    location = _io.parse_url(input_path)
    assert location, f"not a zarr: {location}"

    reader = _reader.Reader(location)
    nodes = []
    paths = []
    for node in reader():
        nodes.append(node)
        paths.append(node.zarr.parts())

    download_plan = dict(_core.utils_download_plan(paths))
    common = str(download_plan["common"])
    paths = [
        [str(part) for part in path_parts]
        for path_parts in download_plan["stripped_parts"]
    ]
    output_path = Path(output_dir)
    root_path = output_path / common

    assert not root_path.exists(), f"{root_path} already exists!"
    print("downloading...")
    for path in paths:
        print("  ", Path(*path))
    print(f"to {output_dir}")

    for path, node in sorted(zip(paths, nodes, strict=False)):
        target_path = output_path / Path(*path)
        target_path.mkdir(parents=True)

        version = node.zarr.version
        fmt = format_from_version(version)
        node_plan = dict(
            _core.utils_download_node_plan(
                int(fmt.zarr_format), "axes" in node.metadata
            )
        )

        metadata = {}
        node.write_metadata(metadata)
        if bool(node_plan["wrap_ome_metadata"]):
            metadata = {"ome": metadata}

        root = zarr.open_group(
            target_path, mode="w", zarr_format=fmt.zarr_format, attributes=metadata
        )

        resolutions = []
        datasets = []

        for spec in node.specs:
            if isinstance(spec, _reader.Multiscales):
                datasets = spec.datasets
                resolutions = node.data
                zarr_array_kwargs = {}
                if bool(node_plan["use_v2_chunk_key_encoding"]):
                    zarr_array_kwargs["chunk_key_encoding"] = {
                        "name": "v2",
                        "separator": "/",
                    }
                else:
                    zarr_array_kwargs["chunk_key_encoding"] = fmt.chunk_key_encoding
                    zarr_array_kwargs["dimension_names"] = [
                        axis["name"] for axis in node.metadata["axes"]
                    ]
                if datasets and resolutions:
                    pbar = ProgressBar()
                    for dataset, data in reversed(
                        list(zip(datasets, resolutions, strict=False))
                    ):
                        LOGGER.info("resolution %s...", dataset)
                        with pbar:
                            da.to_zarr(
                                arr=data,
                                url=root.store,
                                component=dataset,
                                zarr_array_kwargs=zarr_array_kwargs,
                            )
            else:
                zarr.group(str(target_path))


__all__ = [
    "download",
    "find_multiscales",
    "finder",
    "info",
    "splitall",
    "strip_common_prefix",
    "view",
]
