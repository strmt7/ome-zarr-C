"""C++-backed port of selected utility helpers from ome-zarr-py."""

from __future__ import annotations

import csv
import http.server as http_server
import importlib
import json
import os
import urllib
import webbrowser
from datetime import datetime
from pathlib import Path

from RangeHTTPServer import RangeRequestHandler

from ._frozen_upstream import ensure_frozen_upstream_importable

ensure_frozen_upstream_importable()
_core = importlib.import_module("ome_zarr_c._core")
_io = importlib.import_module("ome_zarr.io")
_reader = importlib.import_module("ome_zarr.reader")

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


__all__ = [
    "find_multiscales",
    "finder",
    "info",
    "splitall",
    "strip_common_prefix",
    "view",
]
