from __future__ import annotations

import builtins
import http.client
import importlib
import io
import json
import logging
import os
import random
import shutil
import socket
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from collections import deque
from contextlib import ExitStack, redirect_stdout
from functools import lru_cache
from pathlib import Path
from unittest.mock import patch

import pytest
import zarr

from benchmarks.runtime_support import (
    run_download,
    run_info,
    write_minimal_v2_image,
    write_minimal_v3_image,
)
from benchmarks.runtime_support import (
    snapshot_tree as snapshot_json_tree,
)
from tests._outcomes import err, ok

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_utils = importlib.import_module("ome_zarr.utils")
_py_csv = importlib.import_module("ome_zarr.csv")


@lru_cache(maxsize=1)
def _native_cli_path() -> Path:
    cmake = shutil.which("cmake")
    if cmake is None:
        pytest.skip("cmake is required for standalone native CLI tests")

    build_dir = ROOT / "build-cpp-tests"
    configure_cmd = [
        cmake,
        "-S",
        str(ROOT),
        "-B",
        str(build_dir),
        "-DCMAKE_BUILD_TYPE=Release",
    ]
    if shutil.which("ninja") is not None:
        configure_cmd[1:1] = ["-G", "Ninja"]

    try:
        subprocess.run(configure_cmd, check=True, capture_output=True, text=True)
        subprocess.run(
            [cmake, "--build", str(build_dir), "-j2"],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        failure_text = f"{exc.stdout}\n{exc.stderr}"
        if (
            "Could not find BLOSC_LIBRARY" in failure_text
            or "Could not find ZSTD_LIBRARY" in failure_text
            or "blosc.h" in failure_text
            or "zstd.h" in failure_text
            or "ome-zarr-C native builds require" in failure_text
            or "CMake 4.3 or higher is required" in failure_text
        ):
            pytest.skip(
                "standalone native CLI tests require the latest pinned "
                "native host toolchain"
            )
        raise

    cli_path = build_dir / "ome_zarr_native_cli"
    if not cli_path.exists():
        cli_path = cli_path.with_suffix(".exe")
    assert cli_path.exists(), "standalone native CLI binary was not built"
    return cli_path


def _run_native_cli(args: list[str]):
    completed = subprocess.run(
        [str(_native_cli_path()), *args],
        check=False,
        capture_output=True,
        text=True,
    )
    payload = {
        "returncode": completed.returncode,
        "stderr": completed.stderr,
    }
    if completed.returncode == 0:
        return ok(stdout=completed.stdout, payload=payload)
    return err(
        RuntimeError(completed.stderr.strip() or completed.stdout.strip()),
        stdout=completed.stdout,
        payload=payload,
    )


@lru_cache(maxsize=1)
def _native_probe_path() -> Path:
    probe_path = _native_cli_path().with_name("ome_zarr_native_probe")
    if not probe_path.exists():
        probe_path = probe_path.with_suffix(".exe")
    assert probe_path.exists(), "standalone native probe binary was not built"
    return probe_path


def _run_native_probe(args: list[str]):
    completed = subprocess.run(
        [str(_native_probe_path()), *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return err(
            RuntimeError(completed.stderr.strip() or completed.stdout.strip()),
            stdout=completed.stdout,
            payload={"returncode": completed.returncode, "stderr": completed.stderr},
        )

    payload = json.loads(completed.stdout)
    raw_records = payload.get("records")
    records = None if raw_records is None else [tuple(record) for record in raw_records]
    stdout_text = payload.get("stdout", "")
    if payload.get("status") == "ok":
        return ok(
            value=payload.get("value"),
            payload=payload.get("payload"),
            stdout=stdout_text,
            records=records,
        )

    error_type_name = payload.get("error_type", "Exception")
    error_type = getattr(builtins, error_type_name, Exception)
    return err(
        error_type(payload.get("error_message", "")),
        stdout=stdout_text,
        records=records,
    )


def _spawn_native_cli(args: list[str], *, env: dict[str, str] | None = None):
    return subprocess.Popen(
        [str(_native_cli_path()), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )


def _find_free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_path(path: Path, *, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            return
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {path}")


def _wait_for_http(port: int, target: str, *, timeout: float = 5.0) -> bytes:
    last_error = None
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            conn = http.client.HTTPConnection("localhost", port, timeout=1)
            conn.request("GET", target)
            response = conn.getresponse()
            body = response.read()
            conn.close()
            if response.status == 200:
                return body
        except OSError as exc:
            last_error = exc
        time.sleep(0.05)
    raise AssertionError(
        f"Timed out waiting for HTTP server on port {port}: {last_error}"
    )


def _normalize_download_stdout(text: str, replacements: dict[str, str]) -> str:
    normalized = text
    for original, replacement in replacements.items():
        normalized = normalized.replace(original, replacement)
    lines = []
    for line in normalized.splitlines():
        stripped = line.strip()
        if stripped.startswith("[") and "Completed" in stripped:
            continue
        lines.append(line)
    return "\n".join(lines).strip()


def _run_strip_common_prefix(func, parts):
    payload = [list(path) for path in parts]
    try:
        result = func(payload)
        return ok(value=result, payload=payload)
    except Exception as exc:  # noqa: BLE001
        return err(exc, payload=payload)


def _run_splitall(func, path):
    try:
        return ok(value=func(path))
    except Exception as exc:  # noqa: BLE001
        return err(exc)


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
                return ok(value=func(path), stdout=stream.getvalue(), records=records)
            except Exception as exc:  # noqa: BLE001
                return err(exc, stdout=stream.getvalue(), records=records)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
        logger.propagate = previous_propagate


def _run_native_strip_common_prefix(parts):
    payload = json.dumps([list(path) for path in parts])
    result = _run_native_probe(["strip-common-prefix", "--parts-json", payload])
    if result.status == "ok":
        return ok(value=result.value, payload=result.payload)
    return err(
        result.error_type(result.error_message),
        payload=[list(path) for path in parts],
    )


def _run_native_splitall(path):
    return _run_native_probe(["splitall", "--path", os.fspath(path)])


def _normalize_find_multiscales_rows(rows):
    normalized = []
    for row in rows:
        normalized.append([os.fspath(row[0]), str(row[1]), str(row[2])])
    return normalized


def _normalize_outcome_records(records):
    if records is None:
        return None
    return [(int(level), str(message)) for level, message in records]


def _run_native_find_multiscales(path):
    outcome = _run_native_probe(["find-multiscales", "--path", os.fspath(path)])
    if outcome.status == "ok":
        return ok(
            value=_normalize_find_multiscales_rows(outcome.value),
            stdout=outcome.stdout,
            records=_normalize_outcome_records(outcome.records),
        )
    return err(
        outcome.error_type(outcome.error_message),
        stdout=outcome.stdout,
        records=_normalize_outcome_records(outcome.records),
    )


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


def _write_csv_rows(path: Path, rows: list[list[str]]) -> None:
    lines = []
    for row in rows:
        lines.append(",".join(row))
    path.write_text("\n".join(lines) + "\n")


def _build_csv_labels_tree(
    root: Path,
    root_attrs: dict,
    subgroup_attrs: dict[str, dict] | None = None,
) -> None:
    root_group = zarr.open_group(str(root), mode="w")
    root_group.attrs.update(json.loads(json.dumps(root_attrs)))
    for rel_path, attrs in (subgroup_attrs or {}).items():
        subgroup = zarr.open_group(str(root / rel_path), mode="w")
        if attrs:
            subgroup.attrs.update(json.loads(json.dumps(attrs)))


def _run_py_csv_to_zarr(
    csv_path: Path,
    csv_id: str,
    csv_keys: str,
    zarr_path: Path,
    zarr_id: str,
):
    try:
        _py_csv.csv_to_zarr(str(csv_path), csv_id, csv_keys, str(zarr_path), zarr_id)
        return ok(tree=snapshot_json_tree(zarr_path))
    except Exception as exc:  # noqa: BLE001
        return err(exc, tree=snapshot_json_tree(zarr_path))


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
                return ok(
                    stdout=stream.getvalue(),
                    tree=_snapshot_tree(Path(path)),
                    browser_calls=browser_calls,
                    test_calls=test_calls,
                )
            except Exception as exc:  # noqa: BLE001
                return err(
                    exc,
                    stdout=stream.getvalue(),
                    tree=_snapshot_tree(Path(path)),
                    browser_calls=browser_calls,
                    test_calls=test_calls,
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
                return ok(
                    stdout=stream.getvalue(),
                    browser_calls=browser_calls,
                    test_calls=test_calls,
                )
            except Exception as exc:  # noqa: BLE001
                return err(
                    exc,
                    stdout=stream.getvalue(),
                    browser_calls=browser_calls,
                    test_calls=test_calls,
                )


def _assert_strip_case(parts) -> None:
    expected = _run_strip_common_prefix(_py_utils.strip_common_prefix, parts)
    actual = _run_native_strip_common_prefix(parts)
    assert expected == actual


def _assert_splitall_case(path) -> None:
    expected = _run_splitall(_py_utils.splitall, os.fspath(path))
    actual = _run_native_splitall(path)
    assert expected == actual


def _assert_find_multiscales_case(path) -> None:
    oracle_path = Path(path) if isinstance(path, str) else path
    expected = _run_find_multiscales(_py_utils.find_multiscales, oracle_path)
    expected = (
        ok(
            value=_normalize_find_multiscales_rows(expected.value)
            if expected.status == "ok"
            else expected.value,
            stdout=expected.stdout,
            records=_normalize_outcome_records(expected.records),
        )
        if expected.status == "ok"
        else err(
            expected.error_type(expected.error_message),
            stdout=expected.stdout,
            records=_normalize_outcome_records(expected.records),
        )
    )
    actual = _run_native_find_multiscales(path)
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


def test_native_cli_info_matches_upstream(tmp_path) -> None:
    image = tmp_path / "image.zarr"
    write_minimal_v2_image(image)

    expected = run_info(_py_utils.info, image)
    actual = _run_native_cli(["info", str(image)])

    assert expected.status == actual.status == "ok"
    assert actual.payload["stderr"] == ""
    assert actual.stdout == expected.stdout


def test_native_cli_info_stats_matches_upstream(tmp_path) -> None:
    image = tmp_path / "image.zarr"
    write_minimal_v3_image(image)

    expected = run_info(_py_utils.info, image, stats=True)
    actual = _run_native_cli(["info", str(image), "--stats"])

    assert expected.status == actual.status == "ok"
    assert actual.payload["stderr"] == ""
    assert actual.stdout == expected.stdout


def test_native_cli_finder_matches_upstream_dry_run_outputs(tmp_path) -> None:
    py_data = tmp_path / "py-case" / "data"
    cpp_data = tmp_path / "cpp-case" / "data"
    _build_nested_finder_tree(py_data)
    _build_nested_finder_tree(cpp_data)

    expected = _run_finder(_py_utils.finder, py_data, port=8012, dry_run=True)
    actual = _run_native_cli(["finder", str(cpp_data), "--port", "8012"])

    assert expected.status == actual.status == "ok"
    assert actual.payload["stderr"] == ""
    assert actual.stdout == expected.stdout
    assert _snapshot_tree(py_data) == _snapshot_tree(cpp_data)


def test_native_cli_download_matches_upstream_for_v2_and_v3(tmp_path) -> None:
    for writer, name in (
        (write_minimal_v2_image, "image-v2.zarr"),
        (write_minimal_v3_image, "image-v3.zarr"),
    ):
        source = tmp_path / name
        py_output = tmp_path / f"py-{name}"
        cpp_output = tmp_path / f"cpp-{name}"
        writer(source)

        expected = run_download(_py_utils.download, source, py_output)
        actual = _run_native_cli(["download", str(source), f"--output={cpp_output}"])
        replacements = {
            str(py_output): "<OUT>",
            str(cpp_output): "<OUT>",
        }

        assert expected.status == actual.status == "ok"
        assert actual.payload["stderr"] == ""
        assert _normalize_download_stdout(actual.stdout, replacements) == (
            _normalize_download_stdout(expected.stdout, replacements)
        )
        assert expected.tree == snapshot_json_tree(cpp_output / source.name)


def test_native_cli_view_warns_like_upstream_when_image_missing(tmp_path) -> None:
    missing = tmp_path / "missing-image.zarr"
    expected = _run_view(
        _py_utils.view,
        missing,
        port=8014,
        dry_run=False,
        force=False,
        patch_runtime=True,
    )
    actual = _run_native_cli(["view", str(missing), "--port", "8014"])

    assert expected.status == actual.status == "ok"
    assert actual.payload["stderr"] == ""
    assert actual.stdout == expected.stdout


def test_native_cli_view_serves_validator_target_and_records_browser_url(
    tmp_path,
) -> None:
    py_root = tmp_path / "py-case" / "data"
    cpp_root = tmp_path / "cpp-case" / "data"
    _build_simple_view_tree(py_root)
    _build_simple_view_tree(cpp_root)

    port = _find_free_port()
    expected = _run_view(
        _py_utils.view,
        py_root / "image.zarr",
        port=port,
        dry_run=False,
        force=False,
        patch_runtime=True,
    )
    assert expected.status == "ok"
    expected_url = expected.browser_calls[0]

    browser_log = tmp_path / "browser-url.txt"
    browser_script = tmp_path / "browser-recorder.sh"
    browser_script.write_text('#!/bin/sh\nprintf \'%s\' "$1" > "$BROWSER_LOG_PATH"\n')
    browser_script.chmod(0o755)

    env = os.environ.copy()
    env["BROWSER"] = str(browser_script)
    env["BROWSER_LOG_PATH"] = str(browser_log)

    process = _spawn_native_cli(
        ["view", str(cpp_root / "image.zarr"), "--port", str(port)],
        env=env,
    )
    try:
        _wait_for_path(browser_log)
        assert browser_log.read_text() == expected_url

        expected_bytes = (cpp_root / "image.zarr" / ".zattrs").read_bytes()
        body = _wait_for_http(port, "/image.zarr/.zattrs")
        assert body == expected_bytes

        conn = http.client.HTTPConnection("localhost", port, timeout=2)
        conn.request(
            "GET",
            "/image.zarr/.zattrs",
            headers={"Range": "bytes=0-7"},
        )
        response = conn.getresponse()
        ranged_body = response.read()
        headers = {key.lower(): value for key, value in response.getheaders()}
        conn.close()

        assert response.status == 206
        assert headers["access-control-allow-origin"] == "*"
        assert headers["content-range"].startswith("bytes 0-7/")
        assert ranged_body == expected_bytes[:8]
        assert process.poll() is None
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

    stdout, stderr = process.communicate()
    assert stdout == ""
    assert stderr == ""


def test_native_cli_csv_to_labels_matches_upstream_for_image_root(tmp_path) -> None:
    py_root = tmp_path / "py-case" / "image.zarr"
    cpp_root = tmp_path / "cpp-case" / "image.zarr"
    py_csv = py_root.parent / "props.csv"
    cpp_csv = cpp_root.parent / "props.csv"

    root_attrs = {"multiscales": [{"version": "0.4"}]}
    subgroup_attrs = {
        "labels/0": {
            "image-label": {
                "properties": [
                    {"cell_id": 1, "name": "before"},
                    {"cell_id": "2"},
                ]
            }
        }
    }

    _build_csv_labels_tree(py_root, root_attrs, subgroup_attrs)
    _build_csv_labels_tree(cpp_root, root_attrs, subgroup_attrs)
    _write_csv_rows(py_csv, [["cell_id", "score", "alive"], ["1", "4.5", "1"]])
    _write_csv_rows(cpp_csv, [["cell_id", "score", "alive"], ["1", "4.5", "1"]])

    expected = _run_py_csv_to_zarr(
        py_csv, "cell_id", "score#d,alive#b", py_root, "cell_id"
    )
    actual = _run_native_cli(
        [
            "csv_to_labels",
            str(cpp_csv),
            "cell_id",
            "score#d,alive#b",
            str(cpp_root),
            "cell_id",
        ]
    )

    assert expected.status == actual.status == "ok"
    assert actual.payload["stderr"] == ""
    assert actual.stdout == f"csv_to_labels {cpp_csv} {cpp_root}\n"
    assert expected.tree == snapshot_json_tree(cpp_root)


def test_native_cli_csv_to_labels_matches_upstream_for_plate_missing_label_group(
    tmp_path,
) -> None:
    py_root = tmp_path / "py-case" / "plate.zarr"
    cpp_root = tmp_path / "cpp-case" / "plate.zarr"
    py_csv = py_root.parent / "props.csv"
    cpp_csv = cpp_root.parent / "props.csv"

    root_attrs = {"plate": {"wells": [{"path": "A/1"}, {"path": "B/2"}]}}
    subgroup_attrs = {
        "A/1/0/labels/0": {
            "image-label": {"properties": [{"well_id": "A1", "seed": 1}]}
        }
    }

    _build_csv_labels_tree(py_root, root_attrs, subgroup_attrs)
    _build_csv_labels_tree(cpp_root, root_attrs, subgroup_attrs)
    _write_csv_rows(py_csv, [["well_id", "gene"], ["A1", "TP53"]])
    _write_csv_rows(cpp_csv, [["well_id", "gene"], ["A1", "TP53"]])

    expected = _run_py_csv_to_zarr(py_csv, "well_id", "gene", py_root, "well_id")
    actual = _run_native_cli(
        [
            "csv_to_labels",
            str(cpp_csv),
            "well_id",
            "gene",
            str(cpp_root),
            "well_id",
        ]
    )

    assert expected.status == actual.status == "ok"
    assert actual.payload["stderr"] == ""
    assert actual.stdout == f"csv_to_labels {cpp_csv} {cpp_root}\n"
    assert expected.tree == snapshot_json_tree(cpp_root)


def test_native_cli_csv_to_labels_missing_csv_id_message_matches_upstream(
    tmp_path,
) -> None:
    py_root = tmp_path / "py-case" / "image.zarr"
    cpp_root = tmp_path / "cpp-case" / "image.zarr"
    py_csv = py_root.parent / "props.csv"
    cpp_csv = cpp_root.parent / "props.csv"

    _build_csv_labels_tree(
        py_root,
        {"multiscales": [{"version": "0.4"}]},
        {"labels/0": {"image-label": {"properties": [{"cell_id": 1}]}}},
    )
    _build_csv_labels_tree(
        cpp_root,
        {"multiscales": [{"version": "0.4"}]},
        {"labels/0": {"image-label": {"properties": [{"cell_id": 1}]}}},
    )
    _write_csv_rows(py_csv, [["wrong_id", "score"], ["1", "4.5"]])
    _write_csv_rows(cpp_csv, [["wrong_id", "score"], ["1", "4.5"]])

    expected = _run_py_csv_to_zarr(py_csv, "cell_id", "score#d", py_root, "cell_id")
    actual = _run_native_cli(
        [
            "csv_to_labels",
            str(cpp_csv),
            "cell_id",
            "score#d",
            str(cpp_root),
            "cell_id",
        ]
    )

    assert expected.status == "err"
    assert actual.status == "err"
    assert actual.stdout == f"csv_to_labels {cpp_csv} {cpp_root}\n"
    assert actual.payload["stderr"].strip() == expected.error_message
    assert expected.tree == snapshot_json_tree(cpp_root)
