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
from contextlib import ExitStack, redirect_stdout
from pathlib import Path
from unittest.mock import patch

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


def _snapshot_tree(root: Path):
    if not root.exists():
        return None

    snapshot = []
    for path in sorted(root.rglob("*")):
        rel_path = path.relative_to(root).as_posix()
        if path.is_file():
            snapshot.append(("file", rel_path, path.read_bytes()))
        else:
            snapshot.append(("dir", rel_path, None))
    return snapshot


def _build_simple_finder_tree(root: Path) -> None:
    image = root / "image.zarr"
    image.mkdir(parents=True)
    (image / ".zattrs").write_text(json.dumps({"multiscales": [{}]}))
    os.utime(image, (1_700_000_000, 1_700_000_000))


def _build_nested_finder_tree(root: Path) -> None:
    top_image = root / "top-image.zarr"
    top_image.mkdir(parents=True)
    (top_image / ".zattrs").write_text(json.dumps({"multiscales": [{}]}))

    plate = root / "sub" / "plate.zarr"
    field = plate / "A" / "1" / "0"
    field.mkdir(parents=True)
    (plate / ".zattrs").write_text(json.dumps({"plate": {"wells": [{"path": "A/1"}]}}))

    (root / "sub" / "notes.txt").write_text("ignore me")
    os.utime(top_image, (1_700_000_000, 1_700_000_000))
    os.utime(field, (1_700_000_123, 1_700_000_123))


def _build_simple_view_tree(root: Path) -> None:
    image = root / "image.zarr"
    image.mkdir(parents=True)
    (image / ".zattrs").write_text(json.dumps({"multiscales": [{}]}))


def _run_finder(
    func,
    path,
    *,
    port: int = 8000,
    dry_run: bool = False,
    patch_runtime: bool = False,
    patch_getmtime_error: bool = False,
):
    stream = io.StringIO()
    browser_calls = []
    test_calls = []
    parent_path, server_dir = os.path.split(path)
    if len(server_dir) == 0:
        parent_path, server_dir = os.path.split(parent_path)
    expected_directory = str(parent_path)

    def fake_open(url):
        browser_calls.append(url)
        return None

    def fake_test(handler_cls, server_cls, port=8000):
        translate_info = None
        headers = []
        end_headers_calls = []

        def fake_translate(self, path):
            return f"{self.directory}|{path}"

        def fake_end_headers(self):
            end_headers_calls.append("base")
            return None

        with (
            patch(
                "RangeHTTPServer.RangeRequestHandler.translate_path", new=fake_translate
            ),
            patch(
                "http.server.SimpleHTTPRequestHandler.end_headers", new=fake_end_headers
            ),
        ):
            translate_instance = handler_cls.__new__(handler_cls)
            translate_result = handler_cls.translate_path(translate_instance, "/demo")
            translate_info = (
                getattr(translate_instance, "directory", None) == expected_directory,
                translate_result == f"{expected_directory}|/demo",
            )

            header_instance = handler_cls.__new__(handler_cls)

            def send_header(name, value):
                headers.append((name, value))

            header_instance.send_header = send_header
            handler_cls.end_headers(header_instance)

        test_calls.append(
            (
                handler_cls.__name__,
                handler_cls.__mro__[1].__name__,
                server_cls.__name__,
                port,
                translate_info,
                headers,
                end_headers_calls,
            )
        )
        return None

    with ExitStack() as stack:
        if patch_runtime:
            stack.enter_context(patch("webbrowser.open", side_effect=fake_open))
            if func is _py_utils.finder:
                stack.enter_context(
                    patch.object(_py_utils, "test", side_effect=fake_test)
                )
            else:
                stack.enter_context(patch("http.server.test", side_effect=fake_test))
        if patch_getmtime_error:
            stack.enter_context(
                patch("os.path.getmtime", side_effect=OSError("mtime blocked"))
            )

        with redirect_stdout(stream):
            try:
                func(path, port=port, dry_run=dry_run)
                return (
                    "ok",
                    stream.getvalue(),
                    _snapshot_tree(Path(path)),
                    browser_calls,
                    test_calls,
                )
            except Exception as exc:  # noqa: BLE001
                return (
                    "err",
                    type(exc),
                    str(exc),
                    stream.getvalue(),
                    _snapshot_tree(Path(path)),
                    browser_calls,
                    test_calls,
                )


def _run_view(
    func,
    path,
    *,
    port: int = 8000,
    dry_run: bool = False,
    force: bool = False,
    patch_runtime: bool = False,
):
    stream = io.StringIO()
    browser_calls = []
    test_calls = []
    parent_dir, image_name = os.path.split(path)
    if len(image_name) == 0:
        parent_dir, image_name = os.path.split(parent_dir)
    expected_directory = str(parent_dir)

    def fake_open(url):
        browser_calls.append(url)
        return None

    def fake_test(handler_cls, server_cls, port=8000):
        translate_info = None
        headers = []
        end_headers_calls = []

        def fake_translate(self, path):
            return f"{self.directory}|{path}"

        def fake_end_headers(self):
            end_headers_calls.append("base")
            return None

        with (
            patch(
                "RangeHTTPServer.RangeRequestHandler.translate_path", new=fake_translate
            ),
            patch(
                "http.server.SimpleHTTPRequestHandler.end_headers", new=fake_end_headers
            ),
        ):
            translate_instance = handler_cls.__new__(handler_cls)
            translate_result = handler_cls.translate_path(translate_instance, "/demo")
            translate_info = (
                getattr(translate_instance, "directory", None) == expected_directory,
                translate_result == f"{expected_directory}|/demo",
            )

            header_instance = handler_cls.__new__(handler_cls)

            def send_header(name, value):
                headers.append((name, value))

            header_instance.send_header = send_header
            handler_cls.end_headers(header_instance)

        test_calls.append(
            (
                handler_cls.__name__,
                handler_cls.__mro__[1].__name__,
                server_cls.__name__,
                port,
                translate_info,
                headers,
                end_headers_calls,
            )
        )
        return None

    with ExitStack() as stack:
        if patch_runtime:
            stack.enter_context(patch("webbrowser.open", side_effect=fake_open))
            if func is _py_utils.view:
                stack.enter_context(
                    patch.object(_py_utils, "test", side_effect=fake_test)
                )
            else:
                stack.enter_context(patch("http.server.test", side_effect=fake_test))

        with redirect_stdout(stream):
            try:
                func(path, port=port, dry_run=dry_run, force=force)
                return ("ok", stream.getvalue(), browser_calls, test_calls)
            except Exception as exc:  # noqa: BLE001
                return (
                    "err",
                    type(exc),
                    str(exc),
                    stream.getvalue(),
                    browser_calls,
                    test_calls,
                )


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


def _assert_finder_case(path, **kwargs) -> None:
    expected = _run_finder(_py_utils.finder, path, **kwargs)
    actual = _run_finder(_cpp_utils.finder, path, **kwargs)
    assert expected == actual


def _assert_view_case(path, **kwargs) -> None:
    expected = _run_view(_py_utils.view, path, **kwargs)
    actual = _run_view(_cpp_utils.view, path, **kwargs)
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


def test_finder_matches_upstream_for_missing_input_path(tmp_path) -> None:
    missing = tmp_path / "missing-data"
    _assert_finder_case(missing, dry_run=True)


def test_finder_matches_upstream_when_no_zarr_is_found(tmp_path) -> None:
    data = tmp_path / "data"
    (data / "sub").mkdir(parents=True)
    (data / "sub" / "notes.txt").write_text("plain file")
    _assert_finder_case(data, dry_run=False, patch_runtime=True)


def test_finder_matches_upstream_for_nested_tree_and_trailing_slash(tmp_path) -> None:
    py_data = tmp_path / "py-case" / "data"
    cpp_data = tmp_path / "cpp-case" / "data"
    _build_nested_finder_tree(py_data)
    _build_nested_finder_tree(cpp_data)

    expected = _run_finder(
        _py_utils.finder, f"{py_data}{os.sep}", port=8123, dry_run=True
    )
    actual = _run_finder(
        _cpp_utils.finder, f"{cpp_data}{os.sep}", port=8123, dry_run=True
    )
    assert expected == actual


def test_finder_matches_upstream_when_getmtime_raises(tmp_path) -> None:
    py_data = tmp_path / "py-case" / "data"
    cpp_data = tmp_path / "cpp-case" / "data"
    _build_simple_finder_tree(py_data)
    _build_simple_finder_tree(cpp_data)

    expected = _run_finder(
        _py_utils.finder,
        py_data,
        dry_run=True,
        patch_getmtime_error=True,
    )
    actual = _run_finder(
        _cpp_utils.finder,
        cpp_data,
        dry_run=True,
        patch_getmtime_error=True,
    )
    assert expected == actual


def test_finder_matches_upstream_for_non_dry_run_side_effects(tmp_path) -> None:
    py_data = tmp_path / "py-case" / "data"
    cpp_data = tmp_path / "cpp-case" / "data"
    _build_simple_finder_tree(py_data)
    _build_simple_finder_tree(cpp_data)

    expected = _run_finder(
        _py_utils.finder,
        py_data,
        port=8234,
        dry_run=False,
        patch_runtime=True,
    )
    actual = _run_finder(
        _cpp_utils.finder,
        cpp_data,
        port=8234,
        dry_run=False,
        patch_runtime=True,
    )
    assert expected == actual


def test_view_matches_upstream_for_missing_input_path(tmp_path) -> None:
    missing = tmp_path / "missing-image.zarr"
    _assert_view_case(missing, dry_run=False, force=False, patch_runtime=True)


def test_view_matches_upstream_for_valid_image_dry_run(tmp_path) -> None:
    path = tmp_path / "image.zarr"
    path.mkdir()
    (path / ".zattrs").write_text(json.dumps({"multiscales": [{}]}))
    _assert_view_case(path, port=8124, dry_run=True, force=False, patch_runtime=True)


def test_view_matches_upstream_for_non_dry_run_side_effects(tmp_path) -> None:
    py_root = tmp_path / "py-case" / "data"
    cpp_root = tmp_path / "cpp-case" / "data"
    _build_simple_view_tree(py_root)
    _build_simple_view_tree(cpp_root)

    expected = _run_view(
        _py_utils.view,
        py_root / "image.zarr",
        port=8125,
        dry_run=False,
        force=False,
        patch_runtime=True,
    )
    actual = _run_view(
        _cpp_utils.view,
        cpp_root / "image.zarr",
        port=8125,
        dry_run=False,
        force=False,
        patch_runtime=True,
    )
    assert expected == actual


def test_view_matches_upstream_for_force_true_without_metadata(tmp_path) -> None:
    py_root = tmp_path / "py-case" / "raw"
    cpp_root = tmp_path / "cpp-case" / "raw"
    py_root.mkdir(parents=True)
    cpp_root.mkdir(parents=True)

    expected = _run_view(
        _py_utils.view,
        py_root / "not-zarr",
        port=8126,
        dry_run=False,
        force=True,
        patch_runtime=True,
    )
    actual = _run_view(
        _cpp_utils.view,
        cpp_root / "not-zarr",
        port=8126,
        dry_run=False,
        force=True,
        patch_runtime=True,
    )
    assert expected == actual


def test_view_matches_upstream_for_trailing_slash_image_path(tmp_path) -> None:
    py_root = tmp_path / "py-case" / "data"
    cpp_root = tmp_path / "cpp-case" / "data"
    _build_simple_view_tree(py_root)
    _build_simple_view_tree(cpp_root)

    expected = _run_view(
        _py_utils.view,
        f"{py_root / 'image.zarr'}{os.sep}",
        port=8127,
        dry_run=False,
        force=False,
        patch_runtime=True,
    )
    actual = _run_view(
        _cpp_utils.view,
        f"{cpp_root / 'image.zarr'}{os.sep}",
        port=8127,
        dry_run=False,
        force=False,
        patch_runtime=True,
    )
    assert expected == actual
