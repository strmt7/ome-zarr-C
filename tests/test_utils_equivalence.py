from __future__ import annotations

import importlib
import io
import json
import logging
import os
import random
import sys
import xml.etree.ElementTree as ET
from collections import deque
from contextlib import redirect_stdout
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_utils = importlib.import_module("ome_zarr.utils")
_cpp_utils = importlib.import_module("ome_zarr_c.utils")


def _run_strip_common_prefix(func, parts):
    payload = [list(path) for path in parts]
    try:
        result = func(payload)
        return ("ok", result, payload)
    except Exception as exc:  # noqa: BLE001
        return ("err", type(exc), str(exc), payload)


def _run_splitall(func, path):
    try:
        return ("ok", func(path))
    except Exception as exc:  # noqa: BLE001
        return ("err", type(exc), str(exc))


def _run_find_multiscales(func, path):
    stream = io.StringIO()
    records = []
    logger = logging.getLogger("ome_zarr.utils")
    previous_level = logger.level
    previous_propagate = logger.propagate

    class _Capture(logging.Handler):
        def emit(self, record) -> None:
            records.append((record.levelno, record.getMessage()))

    handler = _Capture()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    try:
        with redirect_stdout(stream):
            try:
                return ("ok", func(path), stream.getvalue(), records)
            except Exception as exc:  # noqa: BLE001
                return ("err", type(exc), str(exc), stream.getvalue(), records)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
        logger.propagate = previous_propagate


def _assert_strip_case(parts) -> None:
    expected = _run_strip_common_prefix(_py_utils.strip_common_prefix, parts)
    actual = _run_strip_common_prefix(_cpp_utils.strip_common_prefix, parts)
    assert expected == actual


def _assert_splitall_case(path) -> None:
    expected = _run_splitall(_py_utils.splitall, path)
    actual = _run_splitall(_cpp_utils.splitall, path)
    assert expected == actual


def _assert_find_multiscales_case(path) -> None:
    expected = _run_find_multiscales(_py_utils.find_multiscales, path)
    actual = _run_find_multiscales(_cpp_utils.find_multiscales, path)
    assert expected == actual


def test_strip_common_prefix_matches_upstream_known_cases() -> None:
    relative = [["d"], ["d", "e"], ["d", "e", "f"]]
    absolute = [
        ["/", "a", "b", "c", "d"],
        ["/", "a", "b", "c", "d", "e"],
        ["/", "a", "b", "c", "d", "e", "f"],
    ]

    for hierarchy in (relative, absolute):
        items = deque(hierarchy)
        for _ in range(len(items)):
            items.rotate(1)
            _assert_strip_case(list(items))

        items.reverse()
        for _ in range(len(items)):
            items.rotate(1)
            _assert_strip_case(list(items))


def test_strip_common_prefix_pair_matrix_matches_upstream() -> None:
    paths = []
    tokens = ["a", "b"]
    for length in range(1, 4):
        if length == 1:
            for a in tokens:
                paths.append([a])
        elif length == 2:
            for a in tokens:
                for b in tokens:
                    paths.append([a, b])
        else:
            for a in tokens:
                for b in tokens:
                    for c in tokens:
                        paths.append([a, b, c])

    for left in paths:
        for right in paths:
            _assert_strip_case([left, right])


def test_strip_common_prefix_random_triples_match_upstream() -> None:
    rng = random.Random(0)
    tokens = ["a", "b", "c", "d"]
    for _ in range(2000):
        parts = []
        for _inner in range(3):
            length = rng.randint(1, 5)
            parts.append([rng.choice(tokens) for _token in range(length)])
        _assert_strip_case(parts)


def test_splitall_known_cases_match_upstream() -> None:
    known_cases = [
        "",
        ".",
        "..",
        "/",
        "a",
        "a/",
        "a/b",
        "a/b/",
        "./a/b",
        "../a/b",
        "/a",
        "/a/",
        "/a/b",
        "/a/b/",
        "a//b",
        "//a/b",
    ]
    for path in known_cases:
        _assert_splitall_case(path)


def test_splitall_pathlike_cases_match_upstream() -> None:
    cases = [
        Path("."),
        Path("a"),
        Path("a/b"),
        Path("/"),
        Path("/a/b"),
    ]
    for path in cases:
        _assert_splitall_case(path)


def test_splitall_random_joined_paths_match_upstream() -> None:
    rng = random.Random(0)
    tokens = ["a", "b", "c", "d", ".", ".."]
    for _ in range(2000):
        length = rng.randint(0, 5)
        parts = [rng.choice(tokens) for _part in range(length)]

        relative = os.path.join(*parts) if parts else ""
        _assert_splitall_case(relative)
        if relative:
            _assert_splitall_case(relative + os.sep)

        absolute = os.path.join(os.sep, *parts) if parts else os.sep
        _assert_splitall_case(absolute)
        if absolute != os.sep:
            _assert_splitall_case(absolute + os.sep)


def test_find_multiscales_matches_upstream_when_metadata_is_missing(tmp_path) -> None:
    _assert_find_multiscales_case(tmp_path / "missing.zarr")


def test_find_multiscales_matches_upstream_for_multiscales_zattrs(tmp_path) -> None:
    path = tmp_path / "image.zarr"
    path.mkdir()
    (path / ".zattrs").write_text(json.dumps({"multiscales": [{}]}))
    _assert_find_multiscales_case(path)


def test_find_multiscales_matches_upstream_for_multiscales_nested_in_zarr_json(
    tmp_path,
) -> None:
    path = tmp_path / "image.zarr"
    path.mkdir()
    (path / "zarr.json").write_text(
        json.dumps({"attributes": {"ome": {"multiscales": [{"version": "0.4"}]}}})
    )
    _assert_find_multiscales_case(path)


def test_find_multiscales_prefers_dot_zattrs_over_zarr_json(tmp_path) -> None:
    path = tmp_path / "image.zarr"
    path.mkdir()
    (path / ".zattrs").write_text(json.dumps({"multiscales": [{}]}))
    (path / "zarr.json").write_text(
        json.dumps({"attributes": {"ome": {"plate": {"wells": [{"path": "A/1"}]}}}})
    )
    _assert_find_multiscales_case(path)


def test_find_multiscales_matches_upstream_for_plate_wells(tmp_path) -> None:
    path = tmp_path / "plate.zarr"
    path.mkdir()
    (path / ".zattrs").write_text(
        json.dumps({"plate": {"wells": [{"path": "A/1"}, {"path": "B/2"}]}})
    )
    _assert_find_multiscales_case(path)


def test_find_multiscales_matches_upstream_for_empty_plate_wells(tmp_path) -> None:
    path = tmp_path / "plate.zarr"
    path.mkdir()
    (path / ".zattrs").write_text(json.dumps({"plate": {"wells": []}}))
    _assert_find_multiscales_case(path)


def test_find_multiscales_matches_upstream_for_bioformats_layout(tmp_path) -> None:
    path = tmp_path / "series.zarr"
    ome_dir = path / "OME"
    ome_dir.mkdir(parents=True)
    (path / ".zattrs").write_text(json.dumps({"bioformats2raw.layout": 3}))

    root = ET.Element("{http://www.openmicroscopy.org/Schemas/OME/2016-06}OME")
    ET.SubElement(
        root,
        "{http://www.openmicroscopy.org/Schemas/OME/2016-06}Image",
        Name="Named Image",
    )
    ET.SubElement(root, "{http://www.openmicroscopy.org/Schemas/OME/2016-06}Image")
    ET.SubElement(root, "{http://www.openmicroscopy.org/Schemas/OME/2016-06}Other")
    ET.ElementTree(root).write(ome_dir / "METADATA.ome.xml", encoding="unicode")

    _assert_find_multiscales_case(path)


def test_find_multiscales_matches_upstream_when_bioformats_xml_is_missing(
    tmp_path,
) -> None:
    path = tmp_path / "series.zarr"
    path.mkdir()
    (path / ".zattrs").write_text(json.dumps({"bioformats2raw.layout": 3}))
    _assert_find_multiscales_case(path)


def test_find_multiscales_matches_upstream_for_string_paths_with_existing_metadata(
    tmp_path,
) -> None:
    path = tmp_path / "string-image.zarr"
    path.mkdir()
    (path / ".zattrs").write_text(json.dumps({"multiscales": [{}]}))
    _assert_find_multiscales_case(str(path))
