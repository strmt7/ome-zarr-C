from __future__ import annotations

import importlib
import sys
from pathlib import Path

from tests._outcomes import err, ok

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_format = importlib.import_module("ome_zarr.format")
_cpp_format = importlib.import_module("ome_zarr_c.format")

PY_FORMAT_TYPES = [
    _py_format.FormatV01,
    _py_format.FormatV02,
    _py_format.FormatV03,
    _py_format.FormatV04,
    _py_format.FormatV05,
]
CPP_FORMAT_TYPES = [
    _cpp_format.FormatV01,
    _cpp_format.FormatV02,
    _cpp_format.FormatV03,
    _cpp_format.FormatV04,
    _cpp_format.FormatV05,
]


def _call(func, *args, **kwargs):
    try:
        return ok(value=func(*args, **kwargs))
    except Exception as exc:  # noqa: BLE001
        return err(exc)


def _format_signature(fmt) -> tuple[str, int, dict[str, str], str]:
    return (fmt.version, fmt.zarr_format, fmt.chunk_key_encoding, repr(fmt))


def _assert_result_match(py_func, cpp_func, *args, **kwargs) -> None:
    expected = _call(py_func, *args, **kwargs)
    actual = _call(cpp_func, *args, **kwargs)

    if expected.status == "err" or actual.status == "err":
        assert expected.status == actual.status
        assert expected.error_type is actual.error_type
        assert expected.error_message == actual.error_message
        return

    assert expected == actual


def test_format_from_version_matches_upstream() -> None:
    for version in ("0.1", "0.2", "0.3", "0.4", "0.5", 0.2, 0.5):
        py_result = _call(_py_format.format_from_version, version)
        cpp_result = _call(_cpp_format.format_from_version, version)

        assert py_result.status == cpp_result.status == "ok"
        assert _format_signature(py_result.value) == _format_signature(cpp_result.value)

    _assert_result_match(
        _py_format.format_from_version,
        _cpp_format.format_from_version,
        "1.0",
    )


def test_format_implementations_order_matches_upstream() -> None:
    expected = [_format_signature(fmt) for fmt in _py_format.format_implementations()]
    actual = [_format_signature(fmt) for fmt in _cpp_format.format_implementations()]
    assert expected == actual


def test_detect_format_matches_upstream() -> None:
    default_py = _py_format.FormatV03()
    default_cpp = _cpp_format.FormatV03()
    metadata_cases = [
        {},
        {"multiscales": [{"version": "0.5"}]},
        {"multiscales": [{"version": 0.5}]},
        {"plate": {"version": "0.4"}},
        {"well": {"version": "0.3"}},
        {"image-label": {"version": "0.2"}},
    ]

    for metadata in metadata_cases:
        py_result = _py_format.detect_format(metadata, default_py)
        cpp_result = _cpp_format.detect_format(metadata, default_cpp)
        assert _format_signature(py_result) == _format_signature(cpp_result)


def test_get_metadata_version_matches_upstream() -> None:
    metadata_cases = [
        {},
        {"multiscales": [{"version": "0.5"}]},
        {"multiscales": [{"version": 0.5}]},
        {"multiscales": [{"version": None}]},
        {"plate": {"version": "0.4"}},
        {"plate": {"version": 0.4}},
        {"plate": {}},
        {"well": {"version": "0.3"}},
        {"image-label": {"version": "0.2"}},
    ]

    for metadata in metadata_cases:
        assert _py_format.FormatV05()._get_metadata_version(
            metadata
        ) == _cpp_format.FormatV05()._get_metadata_version(metadata)


def test_format_properties_and_matches_align() -> None:
    metadata_cases = [
        {"multiscales": [{"version": "0.1"}]},
        {"multiscales": [{"version": "0.2"}]},
        {"multiscales": [{"version": "0.3"}]},
        {"multiscales": [{"version": "0.4"}]},
        {"multiscales": [{"version": "0.5"}]},
        {"plate": {"version": "0.4"}},
        {"well": {"version": "0.3"}},
        {"image-label": {"version": "0.2"}},
        {"plate": {}},
    ]

    for py_cls, cpp_cls in zip(PY_FORMAT_TYPES, CPP_FORMAT_TYPES, strict=True):
        py_fmt = py_cls()
        cpp_fmt = cpp_cls()
        assert _format_signature(py_fmt) == _format_signature(cpp_fmt)
        for metadata in metadata_cases:
            assert py_fmt.matches(metadata) == cpp_fmt.matches(metadata)


def test_format_matches_logging_matches_upstream(monkeypatch) -> None:
    metadata_cases = [
        {"multiscales": [{"version": "0.4"}]},
        {"multiscales": [{"version": 0.4}]},
        {"plate": {}},
    ]

    for py_cls, cpp_cls in zip(PY_FORMAT_TYPES, CPP_FORMAT_TYPES, strict=True):
        for metadata in metadata_cases:
            py_calls: list[tuple[object, ...]] = []
            cpp_calls: list[tuple[object, ...]] = []

            monkeypatch.setattr(
                _py_format.LOGGER,
                "debug",
                lambda *args, py_calls=py_calls: py_calls.append(args),
            )
            monkeypatch.setattr(
                _cpp_format.LOGGER,
                "debug",
                lambda *args, cpp_calls=cpp_calls: cpp_calls.append(args),
            )

            py_result = py_cls().matches(metadata)
            cpp_result = cpp_cls().matches(metadata)

            assert py_result == cpp_result
            assert py_calls == cpp_calls


def test_generate_well_dict_matches_upstream() -> None:
    py_fmt = _py_format.FormatV04()
    cpp_fmt = _cpp_format.FormatV04()
    rows = ["A", "B", "C"]
    columns = ["1", "2", "3"]

    for well in ("A/1", "B/3", "D/1", "A/9", "A", "A/1/2"):
        _assert_result_match(
            py_fmt.generate_well_dict,
            cpp_fmt.generate_well_dict,
            well,
            rows,
            columns,
        )


def test_validate_well_dict_matches_upstream() -> None:
    rows = ["A", "B", "C"]
    columns = ["1", "2", "3"]
    v01_cases = [
        {"path": "A/1"},
        {"path": "A/1", "extra": 1},
        {},
        {"path": 3},
    ]
    v04_cases = [
        {"path": "A/1", "rowIndex": 0, "columnIndex": 0},
        {"path": "A/1", "rowIndex": True, "columnIndex": False},
        {"path": "A/1"},
        {"path": "A/1", "rowIndex": 0},
        {"path": "A/1", "columnIndex": 0},
        {"path": 3, "rowIndex": 0, "columnIndex": 0},
        {"path": "A/1/2", "rowIndex": 0, "columnIndex": 0},
        {"path": "D/1", "rowIndex": 0, "columnIndex": 0},
        {"path": "A/9", "rowIndex": 0, "columnIndex": 0},
        {"path": "A/1", "rowIndex": 1, "columnIndex": 0},
        {"path": "A/1", "rowIndex": 0, "columnIndex": 1},
    ]

    py_v01 = _py_format.FormatV01()
    cpp_v01 = _cpp_format.FormatV01()
    for well in v01_cases:
        _assert_result_match(
            py_v01.validate_well_dict,
            cpp_v01.validate_well_dict,
            dict(well),
            rows,
            columns,
        )

    py_v04 = _py_format.FormatV04()
    cpp_v04 = _cpp_format.FormatV04()
    for well in v04_cases:
        _assert_result_match(
            py_v04.validate_well_dict,
            cpp_v04.validate_well_dict,
            dict(well),
            rows,
            columns,
        )


def test_format_v01_init_store_matches_upstream_local_and_remote(
    tmp_path, monkeypatch
) -> None:
    py_fmt = _py_format.FormatV01()
    cpp_fmt = _cpp_format.FormatV01()

    py_local = py_fmt.init_store(str(tmp_path / "py-local.zarr"), mode="w")
    cpp_local = cpp_fmt.init_store(str(tmp_path / "cpp-local.zarr"), mode="w")
    assert type(py_local) is type(cpp_local)
    assert not py_local.read_only
    assert not cpp_local.read_only

    calls: list[tuple[str, bool]] = []

    class _SentinelStore:
        def __init__(self, path: str, read_only: bool) -> None:
            self.path = path
            self.read_only = read_only

    def fake_from_url(path: str, storage_options=None, read_only=False):
        assert storage_options is None
        calls.append((path, read_only))
        return _SentinelStore(path, read_only)

    monkeypatch.setattr(
        _py_format.FsspecStore,
        "from_url",
        staticmethod(fake_from_url),
    )
    monkeypatch.setattr(
        _cpp_format.FsspecStore,
        "from_url",
        staticmethod(fake_from_url),
    )

    py_remote = py_fmt.init_store("https://example.invalid/image.zarr", mode="r")
    cpp_remote = cpp_fmt.init_store("https://example.invalid/image.zarr", mode="r")
    assert isinstance(py_remote, _SentinelStore)
    assert isinstance(cpp_remote, _SentinelStore)
    assert calls == [
        ("https://example.invalid/image.zarr", True),
        ("https://example.invalid/image.zarr", True),
    ]


def test_generate_coordinate_transformations_match_upstream() -> None:
    py_fmt = _py_format.FormatV04()
    cpp_fmt = _cpp_format.FormatV04()

    for shapes in (
        [(256, 256), (128, 128), (64, 64)],
        [(1, 3, 512, 512), (1, 3, 256, 256), (1, 3, 128, 128)],
        [(1, 1, 16, 32, 64), (1, 1, 8, 16, 32)],
    ):
        assert py_fmt.generate_coordinate_transformations(
            shapes
        ) == cpp_fmt.generate_coordinate_transformations(shapes)


def test_validate_coordinate_transformations_match_upstream() -> None:
    transformations = [
        [{"type": "scale", "scale": (1, 1)}],
        [{"type": "scale", "scale": (0.5, 0.5)}],
    ]
    translate = [{"type": "translation", "translation": (1, 1)}]
    scale_then_trans = [item + translate for item in transformations]
    double_translate = [item + translate for item in scale_then_trans]

    cases = [
        (2, 2, transformations),
        (2, 1, transformations),
        (2, 1, [[{"type": "scale", "scale": ("1", 1)}]]),
        (2, 1, [[{"type": "foo", "scale": (1, 1)}]]),
        (3, 1, [[{"type": "scale", "scale": (1, 1)}]]),
        (2, 2, scale_then_trans),
        (2, 2, [translate + item for item in transformations]),
        (2, 2, double_translate),
        (2, 1, [[{"scale": (1, 1)}]]),
        (2, 1, [[{"type": "scale"}]]),
        (2, 1, [[{"type": "scale", "scale": (1, 1)}, {"type": "translation"}]]),
        (
            2,
            1,
            [
                [
                    {"type": "scale", "scale": (1, 1)},
                    {"type": "translation", "translation": ("1", 1)},
                ]
            ],
        ),
    ]

    for py_cls, cpp_cls in (
        (_py_format.FormatV04, _cpp_format.FormatV04),
        (_py_format.FormatV05, _cpp_format.FormatV05),
    ):
        py_fmt = py_cls()
        cpp_fmt = cpp_cls()
        for ndim, nlevels, payload in cases:
            _assert_result_match(
                py_fmt.validate_coordinate_transformations,
                cpp_fmt.validate_coordinate_transformations,
                ndim,
                nlevels,
                payload,
            )


def test_format_repr_and_equality_match_upstream() -> None:
    for py_cls, cpp_cls in zip(PY_FORMAT_TYPES, CPP_FORMAT_TYPES, strict=True):
        py_fmt = py_cls()
        cpp_fmt = cpp_cls()
        assert repr(py_fmt) == repr(cpp_fmt)
        assert (py_fmt == py_cls()) == (cpp_fmt == cpp_cls())
        assert (py_fmt == object()) == (cpp_fmt == object())

    for py_left, cpp_left in zip(PY_FORMAT_TYPES, CPP_FORMAT_TYPES, strict=True):
        for py_right, cpp_right in zip(PY_FORMAT_TYPES, CPP_FORMAT_TYPES, strict=True):
            assert (py_left() == py_right()) == (cpp_left() == cpp_right())
