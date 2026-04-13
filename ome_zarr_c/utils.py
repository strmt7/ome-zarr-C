"""C++-backed port of selected utility helpers from ome-zarr-py."""

from __future__ import annotations

import csv
import http.server as http_server
import importlib
import json
import logging
import os
import urllib
import webbrowser
from datetime import datetime
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
            print(f"not an ome-zarr node: {node}")
            continue

        for line in _core.info_lines(node, stats):
            print(line)
        yield node


def view(
    input_path: str, port: int = 8000, dry_run: bool = False, force: bool = False
) -> None:
    if not force:
        zarrs = []
        if (Path(input_path) / ".zattrs").exists() or (
            Path(input_path) / "zarr.json"
        ).exists():
            zarrs = find_multiscales(Path(input_path))
        if len(zarrs) == 0:
            print(
                f"No OME-Zarr images found in {input_path}. "
                f"Try $ ome_zarr finder {input_path} "
                "or use -f to force open in browser."
            )
            return

    parent_dir, image_name = os.path.split(input_path)
    if len(image_name) == 0:
        parent_dir, image_name = os.path.split(parent_dir)
    parent_dir = str(parent_dir)

    url = (
        f"https://ome.github.io/ome-ngff-validator/"
        f"?source=http://localhost:{port}/{image_name}"
    )

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
    parent_path, server_dir = os.path.split(input_path)
    if len(server_dir) == 0:
        parent_path, server_dir = os.path.split(parent_path)

    def walk(path: Path):
        if (path / ".zattrs").exists() or (path / "zarr.json").exists():
            yield from find_multiscales(path)
        else:
            for child in path.iterdir():
                if (child / ".zattrs").exists() or (child / "zarr.json").exists():
                    yield from find_multiscales(child)
                elif child.is_dir():
                    yield from walk(child)

    zarrs = list(walk(Path(input_path)))
    if len(zarrs) == 0:
        print("No OME-Zarr files found in", input_path)
        return

    col_names = ["File Path", "File Name", "Folders", "Uploaded"]
    bff_csv = os.path.join(input_path, "biofile_finder.csv")
    with open(bff_csv, "w", newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter=",")
        writer.writerow(col_names)
        for zarr_img in zarrs:
            relpath = os.path.relpath(zarr_img[0], input_path)
            rel_url = "/".join(splitall(relpath))
            file_path = f"http://localhost:{port}/{server_dir}/{rel_url}"
            name = zarr_img[1] or os.path.basename(zarr_img[0])
            folders_path = os.path.relpath(zarr_img[2], input_path)
            folders = ",".join(splitall(folders_path))
            timestamp = ""
            try:
                mtime = os.path.getmtime(zarr_img[0])
                timestamp = datetime.fromtimestamp(mtime).strftime(
                    "%Y-%m-%d %H:%M:%S.%Z"
                )
            except OSError:
                pass
            writer.writerow([file_path, name, folders, timestamp])

    source = {
        "uri": f"http://localhost:{port}/{server_dir}/biofile_finder.csv",
        "type": "csv",
        "name": "biofile_finder.csv",
    }
    url = (
        f"https://bff.allencell.org/app?source={urllib.parse.quote(json.dumps(source))}"
    )
    url += "&v=2"

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

    common = strip_common_prefix(paths)
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

        metadata = {}
        node.write_metadata(metadata)
        if fmt.zarr_format == 3:
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
                zarr_array_kwargs = {"zarr_format": fmt.zarr_format}
                if fmt.zarr_format == 2:
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
                                **zarr_array_kwargs,
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
