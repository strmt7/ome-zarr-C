from __future__ import annotations

import copy
import csv
import importlib
import math
import random
import sys
from pathlib import Path

import zarr

from tests._outcomes import err, ok

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_csv = importlib.import_module("ome_zarr.csv")
_cpp_csv = importlib.import_module("ome_zarr_c.csv")


def _assert_same_value(left, right) -> None:
    assert type(left) is type(right)
    if isinstance(left, float) and math.isnan(left):
        assert math.isnan(right)
        return
    assert left == right


def _snapshot_tree(root: Path):
    snapshot = []
    for path in sorted(root.rglob("*")):
        rel_path = path.relative_to(root).as_posix()
        if path.is_file():
            snapshot.append(("file", rel_path, path.read_bytes()))
        else:
            snapshot.append(("dir", rel_path, None))
    return snapshot


def _write_zarr_tree(
    root: Path,
    root_attrs: dict,
    subgroup_attrs: dict[str, dict] | None = None,
) -> None:
    root_group = zarr.open_group(str(root), mode="w")
    root_group.attrs.update(copy.deepcopy(root_attrs))
    for rel_path, attrs in (subgroup_attrs or {}).items():
        subgroup = zarr.open_group(str(root / rel_path), mode="w")
        if attrs:
            subgroup.attrs.update(copy.deepcopy(attrs))


def _write_csv(path: Path, rows: list[list[str]]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerows(rows)


def _run_dict_to_zarr(func, props_to_add, zarr_path: Path, zarr_id: str):
    payload = copy.deepcopy(props_to_add)
    try:
        func(payload, str(zarr_path), zarr_id)
        return ok(tree=_snapshot_tree(zarr_path))
    except Exception as exc:  # noqa: BLE001
        return err(exc, tree=_snapshot_tree(zarr_path))


def _run_csv_to_zarr(
    func,
    csv_path: Path,
    csv_id: str,
    csv_keys: str,
    zarr_path: Path,
    zarr_id: str,
):
    try:
        func(str(csv_path), csv_id, csv_keys, str(zarr_path), zarr_id)
        return ok(tree=_snapshot_tree(zarr_path))
    except Exception as exc:  # noqa: BLE001
        return err(exc, tree=_snapshot_tree(zarr_path))


def _assert_parse_case(value: str, col_type: str) -> None:
    expected_value = None
    actual_value = None
    expected_exc = None
    actual_exc = None

    try:
        expected_value = _py_csv.parse_csv_value(value, col_type)
    except Exception as exc:  # noqa: BLE001
        expected_exc = exc

    try:
        actual_value = _cpp_csv.parse_csv_value(value, col_type)
    except Exception as exc:  # noqa: BLE001
        actual_exc = exc

    if expected_exc or actual_exc:
        assert type(expected_exc) is type(actual_exc)
        assert str(expected_exc) == str(actual_exc)
        return

    _assert_same_value(expected_value, actual_value)


def test_column_types_match_upstream() -> None:
    assert _cpp_csv.COLUMN_TYPES == _py_csv.COLUMN_TYPES


def test_parse_csv_value_examples_match_upstream() -> None:
    values = [
        "",
        "0",
        "1",
        "-1",
        "1.5",
        "2.5",
        "-2.5",
        "abc",
        "True",
        "False",
        " 3 ",
        "1e3",
        "nan",
        "inf",
        "-inf",
    ]
    col_types = ["d", "l", "s", "b", "x"]

    for value in values:
        for col_type in col_types:
            _assert_parse_case(value, col_type)


def test_parse_csv_value_random_numeric_strings_match_upstream() -> None:
    rng = random.Random(0)
    for _ in range(5000):
        whole = rng.randint(-1_000_000, 1_000_000)
        frac = rng.randint(0, 9999)
        exponent = rng.randint(-5, 5)
        value = f"{whole}.{frac:04d}e{exponent:+d}"
        for col_type in ("d", "l"):
            _assert_parse_case(value, col_type)


def test_dict_to_zarr_matches_upstream_for_image_properties(tmp_path) -> None:
    root_attrs = {"multiscales": [{"version": "0.4"}], "extra": {"keep": True}}
    subgroup_attrs = {
        "labels/0": {
            "image-label": {
                "properties": [
                    {"cell_id": 1, "name": "before"},
                    {"cell_id": "2"},
                    {"other": "missing-id"},
                ]
            },
            "other": "value",
        }
    }
    props_to_add = {
        "1": {"score": 1.5, "alive": True},
        "2": {"name": "after", "count": 7},
        "None": {"fallback": "applied"},
    }

    py_root = tmp_path / "py-image.zarr"
    cpp_root = tmp_path / "cpp-image.zarr"
    _write_zarr_tree(py_root, root_attrs, subgroup_attrs)
    _write_zarr_tree(cpp_root, root_attrs, subgroup_attrs)

    expected = _run_dict_to_zarr(_py_csv.dict_to_zarr, props_to_add, py_root, "cell_id")
    actual = _run_dict_to_zarr(_cpp_csv.dict_to_zarr, props_to_add, cpp_root, "cell_id")
    assert expected == actual


def test_dict_to_zarr_matches_upstream_for_plate_and_missing_label_group(
    tmp_path,
) -> None:
    root_attrs = {
        "plate": {
            "wells": [
                {"path": "A/1"},
                {"path": "B/2"},
            ]
        }
    }
    subgroup_attrs = {
        "A/1/0/labels/0": {
            "image-label": {"properties": [{"well_id": "A1", "seed": 1}]}
        }
    }
    props_to_add = {"A1": {"gene": "TP53"}}

    py_root = tmp_path / "py-plate.zarr"
    cpp_root = tmp_path / "cpp-plate.zarr"
    _write_zarr_tree(py_root, root_attrs, subgroup_attrs)
    _write_zarr_tree(cpp_root, root_attrs, subgroup_attrs)

    expected = _run_dict_to_zarr(_py_csv.dict_to_zarr, props_to_add, py_root, "well_id")
    actual = _run_dict_to_zarr(_cpp_csv.dict_to_zarr, props_to_add, cpp_root, "well_id")
    assert expected == actual


def test_dict_to_zarr_matches_upstream_for_integer_mapping_key_quirk(
    tmp_path,
) -> None:
    root_attrs = {"multiscales": [{}]}
    subgroup_attrs = {
        "labels/0": {"image-label": {"properties": [{"cell_id": 7, "name": "cell"}]}}
    }
    props_to_add = {7: {"should_not_match": "int-key-only"}}

    py_root = tmp_path / "py-int-key.zarr"
    cpp_root = tmp_path / "cpp-int-key.zarr"
    _write_zarr_tree(py_root, root_attrs, subgroup_attrs)
    _write_zarr_tree(cpp_root, root_attrs, subgroup_attrs)

    expected = _run_dict_to_zarr(_py_csv.dict_to_zarr, props_to_add, py_root, "cell_id")
    actual = _run_dict_to_zarr(_cpp_csv.dict_to_zarr, props_to_add, cpp_root, "cell_id")
    assert expected == actual


def test_dict_to_zarr_matches_upstream_for_label_group_without_properties(
    tmp_path,
) -> None:
    root_attrs = {"multiscales": [{}]}
    subgroup_attrs = {"labels/0": {"image-label": {"color": "green"}, "other": 1}}
    props_to_add = {"1": {"unused": "value"}}

    py_root = tmp_path / "py-no-props.zarr"
    cpp_root = tmp_path / "cpp-no-props.zarr"
    _write_zarr_tree(py_root, root_attrs, subgroup_attrs)
    _write_zarr_tree(cpp_root, root_attrs, subgroup_attrs)

    expected = _run_dict_to_zarr(_py_csv.dict_to_zarr, props_to_add, py_root, "cell_id")
    actual = _run_dict_to_zarr(_cpp_csv.dict_to_zarr, props_to_add, cpp_root, "cell_id")
    assert expected == actual


def test_dict_to_zarr_matches_upstream_for_invalid_root(tmp_path) -> None:
    root_attrs = {"name": "not-a-plate-or-image"}

    py_root = tmp_path / "py-invalid.zarr"
    cpp_root = tmp_path / "cpp-invalid.zarr"
    _write_zarr_tree(py_root, root_attrs)
    _write_zarr_tree(cpp_root, root_attrs)

    expected = _run_dict_to_zarr(
        _py_csv.dict_to_zarr, {"1": {"x": 1}}, py_root, "cell_id"
    )
    actual = _run_dict_to_zarr(
        _cpp_csv.dict_to_zarr, {"1": {"x": 1}}, cpp_root, "cell_id"
    )
    assert expected == actual


def test_dict_to_zarr_random_image_cases_match_upstream(tmp_path) -> None:
    rng = random.Random(0)
    scalar_values = [None, True, False, 0, 1, 2, "0", "1", "A", "B"]

    for case in range(25):
        properties = []
        for _ in range(rng.randint(0, 5)):
            props = {}
            if rng.choice([True, False]):
                props["cell_id"] = rng.choice(scalar_values)
            if rng.choice([True, False]):
                props["name"] = rng.choice(["alpha", "beta", "gamma"])
            if rng.choice([True, False]):
                props["count"] = rng.randint(0, 10)
            properties.append(props)

        props_to_add = {}
        for _ in range(rng.randint(0, 5)):
            if rng.choice([True, False]):
                key = str(rng.choice(scalar_values))
            else:
                key = rng.randint(0, 5)
            updates = {}
            for field in ("score", "flag", "label"):
                if rng.choice([True, False]):
                    if field == "score":
                        updates[field] = round(rng.random() * 10, 3)
                    elif field == "flag":
                        updates[field] = rng.choice([True, False])
                    else:
                        updates[field] = rng.choice(["x", "y", "z"])
            props_to_add[key] = updates

        root_attrs = {"multiscales": [{"version": "0.4"}]}
        subgroup_attrs = {"labels/0": {"image-label": {"properties": properties}}}

        py_root = tmp_path / f"py-random-{case}.zarr"
        cpp_root = tmp_path / f"cpp-random-{case}.zarr"
        _write_zarr_tree(py_root, root_attrs, subgroup_attrs)
        _write_zarr_tree(cpp_root, root_attrs, subgroup_attrs)

        expected = _run_dict_to_zarr(
            _py_csv.dict_to_zarr, props_to_add, py_root, "cell_id"
        )
        actual = _run_dict_to_zarr(
            _cpp_csv.dict_to_zarr, props_to_add, cpp_root, "cell_id"
        )
        assert expected == actual
