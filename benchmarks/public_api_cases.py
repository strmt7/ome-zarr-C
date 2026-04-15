from __future__ import annotations

import argparse
import copy
import io
import json
import math
import os
import random
import shutil
import subprocess
import tempfile
import warnings
from contextlib import nullcontext, redirect_stdout
from dataclasses import fields, is_dataclass
from pathlib import Path
from unittest.mock import patch

import dask
import dask.array as da
import numpy as np
import zarr

from benchmarks import cases as core_cases
from benchmarks.runtime_support import (
    rewrite_snapshot_prefix,
    snapshot_tree,
    write_minimal_v2_image,
)
from tests import test_cli_equivalence as cli_eq
from tests import test_conversions_equivalence as conv_eq
from tests import test_csv_equivalence as csv_eq
from tests import test_data_equivalence as data_eq
from tests import test_format_equivalence as format_eq
from tests import test_io_equivalence as io_eq
from tests import test_reader_equivalence as reader_eq
from tests import test_scaler_equivalence as scaler_eq
from tests import test_utils_equivalence as utils_eq
from tests import test_writer_image_equivalence as writer_img_eq
from tests import test_writer_runtime_equivalence as writer_rt
from tests._outcomes import err, ok


def _temp_dir(prefix: str) -> Path:
    return Path(tempfile.mkdtemp(prefix=f"ome-zarr-c-bench-{prefix}-"))


def _assert_equal(case_name: str, left, right) -> None:
    if not _values_equal(left, right):
        raise AssertionError(
            f"Benchmark case {case_name} lost parity.\nleft={left!r}\nright={right!r}"
        )


def _values_equal(left, right) -> bool:
    if isinstance(left, float) and isinstance(right, float):
        if math.isnan(left) and math.isnan(right):
            return True
    if is_dataclass(left) and is_dataclass(right) and type(left) is type(right):
        return all(
            _values_equal(getattr(left, field.name), getattr(right, field.name))
            for field in fields(left)
        )
    if isinstance(left, dict) and isinstance(right, dict):
        if left.keys() != right.keys():
            return False
        return all(_values_equal(left[key], right[key]) for key in left)
    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        if len(left) != len(right):
            return False
        return all(
            _values_equal(item_left, item_right)
            for item_left, item_right in zip(left, right, strict=False)
        )
    return left == right


def _touch_outcome(value) -> float:
    return core_cases._touch_outcome(value)


def _touch_value(value) -> float:
    return core_cases._touch(value)


def _run_cli_namespace(func, args, replacements: dict[str, str]):
    stream = io.StringIO()
    patched = (
        patch.object(cli_eq._py_writer.da, "to_zarr", cli_eq._compat_to_zarr)
        if func in {cli_eq._py_cli.create, cli_eq._py_cli.scale}
        else nullcontext()
    )
    try:
        with patched, redirect_stdout(stream):
            func(args)
        return ok(stdout=cli_eq._normalize_output(stream.getvalue(), replacements))
    except Exception as exc:  # noqa: BLE001
        return err(
            exc, stdout=cli_eq._normalize_output(stream.getvalue(), replacements)
        )


def _run_native_cli(args: list[str], replacements: dict[str, str] | None = None):
    outcome = cli_eq._run_native_cli(args, replacements or {})
    if outcome.status == "ok":
        return ok(stdout=outcome.stdout)
    return err(RuntimeError(outcome.error_message), stdout=outcome.stdout)


def _native_bench_timer(
    *,
    case_id: str,
    verify,
    native_match: str,
):
    def timer(loops: int) -> float:
        core_cases._verify_once(case_id, verify)
        bench_path = utils_eq._native_cli_path().with_name("ome_zarr_native_bench_core")
        if not bench_path.exists():
            bench_path = bench_path.with_suffix(".exe")
        if not bench_path.exists():
            raise AssertionError("standalone native benchmark binary was not built")

        with tempfile.TemporaryDirectory(prefix="ome-zarr-c-native-bench-") as temp_dir:
            json_path = Path(temp_dir) / "native.json"
            completed = subprocess.run(
                [
                    str(bench_path),
                    "--match",
                    native_match,
                    "--rounds",
                    "1",
                    "--iterations",
                    str(max(1, loops)),
                    "--json-output",
                    str(json_path),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            del completed
            payload = json.loads(json_path.read_text(encoding="utf-8"))

        results = payload["results"]
        if len(results) != 1:
            raise AssertionError(
                "Expected one native benchmark result for "
                f"{native_match!r}, got {results!r}"
            )
        item = results[0]
        return float(item["median_us_per_op"]) * float(item["iterations"]) / 1_000_000.0

    return timer


def _run_utils_download(func, source: Path, output_dir: Path):
    stream = io.StringIO()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            with redirect_stdout(stream):
                func(str(source), str(output_dir))
            return ok(
                tree=snapshot_tree(output_dir),
                stdout=stream.getvalue(),
                records=[
                    (type(item.message).__name__, str(item.message)) for item in caught
                ],
            )
        except Exception as exc:  # noqa: BLE001
            tree = snapshot_tree(output_dir) if output_dir.exists() else []
            return err(
                exc,
                tree=tree,
                stdout=stream.getvalue(),
                records=[
                    (type(item.message).__name__, str(item.message)) for item in caught
                ],
            )


def _run_python_view(path: Path, *, port: int, force: bool):
    outcome = utils_eq._run_view(
        utils_eq._py_utils.view,
        str(path),
        port=port,
        dry_run=False,
        force=force,
        patch_runtime=True,
    )
    if outcome.status != "ok":
        return outcome
    return ok(
        value={
            "url": outcome.browser_calls[0],
            "body": (path / ".zattrs").read_bytes(),
        }
    )


def _run_native_view(path: Path, *, port: int, force: bool):
    browser_log = path.parent / "browser-url.txt"
    browser_script = path.parent / "browser-recorder.sh"
    browser_script.write_text('#!/bin/sh\nprintf \'%s\' "$1" > "$BROWSER_LOG_PATH"\n')
    browser_script.chmod(0o755)

    env = os.environ.copy()
    env["BROWSER"] = str(browser_script)
    env["BROWSER_LOG_PATH"] = str(browser_log)
    args = ["view", str(path), "--port", str(port)]
    if force:
        args.append("--force")
    process = utils_eq._spawn_native_cli(args, env=env)
    try:
        utils_eq._wait_for_path(browser_log)
        body = utils_eq._wait_for_http(port, f"/{path.name}/.zattrs")
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            return err(
                RuntimeError(stderr.strip() or stdout.strip()),
                stdout=stdout,
                payload={"stderr": stderr},
            )
        return ok(value={"url": browser_log.read_text(), "body": body})
    except Exception as exc:  # noqa: BLE001
        return err(exc)
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def _run_writer_multiscale(writer_module, format_module, root: Path):
    group = zarr.open_group(str(root), mode="w", zarr_format=3)
    pyramid = [
        np.arange(16, dtype=np.uint16).reshape(4, 4),
        np.arange(4, dtype=np.uint16).reshape(2, 2),
    ]
    patched = (
        patch.object(
            writer_img_eq._py_writer.da, "to_zarr", writer_img_eq._compat_to_zarr
        )
        if writer_module is writer_img_eq._py_writer
        else nullcontext()
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            with patched:
                delayed = writer_module.write_multiscale(
                    copy.deepcopy(pyramid),
                    group,
                    fmt=format_module.FormatV05(),
                    axes=["y", "x"],
                    coordinate_transformations=[
                        [{"type": "scale", "scale": [1, 1]}],
                        [{"type": "scale", "scale": [2, 2]}],
                    ],
                    compute=True,
                )
                if delayed:
                    dask.compute(*delayed)
            return ok(
                tree=snapshot_tree(root),
                value=writer_module.get_metadata(group),
                records=[
                    (type(item.message).__name__, str(item.message)) for item in caught
                ],
            )
        except Exception as exc:  # noqa: BLE001
            return err(
                exc,
                tree=snapshot_tree(root),
                records=[
                    (type(item.message).__name__, str(item.message)) for item in caught
                ],
            )


def _run_writer_multiscale_labels(writer_module, format_module, root: Path):
    group = zarr.open_group(str(root), mode="w", zarr_format=3)
    pyramid = [
        np.array([[0, 1], [2, 3]], dtype=np.uint8),
        np.array([[3]], dtype=np.uint8),
    ]
    patched = (
        patch.object(
            writer_img_eq._py_writer.da, "to_zarr", writer_img_eq._compat_to_zarr
        )
        if writer_module is writer_img_eq._py_writer
        else nullcontext()
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            with patched:
                delayed = writer_module.write_multiscale_labels(
                    copy.deepcopy(pyramid),
                    group,
                    "cells",
                    fmt=format_module.FormatV05(),
                    axes=["y", "x"],
                    coordinate_transformations=[
                        [{"type": "scale", "scale": [1, 1]}],
                        [{"type": "scale", "scale": [2, 2]}],
                    ],
                    label_metadata={
                        "colors": [{"label-value": 1, "rgba": [255, 0, 0, 255]}]
                    },
                    compute=True,
                )
                if delayed:
                    dask.compute(*delayed)
            return ok(
                tree=snapshot_tree(root),
                value=writer_module.get_metadata(group["labels"]),
                records=[
                    (type(item.message).__name__, str(item.message)) for item in caught
                ],
            )
        except Exception as exc:  # noqa: BLE001
            return err(
                exc,
                tree=snapshot_tree(root),
                records=[
                    (type(item.message).__name__, str(item.message)) for item in caught
                ],
            )


def _run_writer_labels(writer_module, format_module, root: Path):
    group = zarr.open_group(str(root), mode="w", zarr_format=3)
    labels = np.arange(64, dtype=np.uint8).reshape(8, 8)
    patched = (
        patch.object(
            writer_img_eq._py_writer.da, "to_zarr", writer_img_eq._compat_to_zarr
        )
        if writer_module is writer_img_eq._py_writer
        else nullcontext()
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        try:
            with patched:
                random.seed(0)
                delayed = writer_module.write_labels(
                    labels,
                    group,
                    "cells",
                    scaler=None,
                    scale_factors=(2,),
                    method=writer_module.Methods.NEAREST,
                    fmt=format_module.FormatV05(),
                    axes=["y", "x"],
                    label_metadata={
                        "properties": [{"label-value": 1, "class": "cell"}]
                    },
                    compute=True,
                )
                if delayed:
                    dask.compute(*delayed)
            return ok(
                tree=snapshot_tree(root),
                value=writer_module.get_metadata(group["labels"]),
                records=[
                    (type(item.message).__name__, str(item.message)) for item in caught
                ],
            )
        except Exception as exc:  # noqa: BLE001
            return err(
                exc,
                tree=snapshot_tree(root),
                records=[
                    (type(item.message).__name__, str(item.message)) for item in caught
                ],
            )


def _verify_cli_create_wrapper() -> None:
    py_root = _temp_dir("py-cli-create") / "image.zarr"
    cpp_root = _temp_dir("cpp-cli-create") / "image.zarr"
    replacements = {str(py_root): "<ROOT>", str(cpp_root): "<ROOT>"}
    try:
        random.seed(0)
        py_outcome = _run_cli_namespace(
            cli_eq._py_cli.create,
            argparse.Namespace(
                method="coins",
                path=str(py_root),
                format="0.5",
                verbose=0,
                quiet=0,
            ),
            replacements,
        )
        random.seed(0)
        cpp_outcome = _run_native_cli(
            ["create", "--method=coins", str(cpp_root), "--format", "0.5"],
            replacements,
        )
        _assert_equal(
            "cli.create_wrapper",
            py_outcome,
            cpp_outcome,
        )
        _assert_equal(
            "cli.create_wrapper.tree",
            snapshot_tree(py_root),
            snapshot_tree(cpp_root),
        )
    finally:
        shutil.rmtree(py_root.parent, ignore_errors=True)
        shutil.rmtree(cpp_root.parent, ignore_errors=True)


def _bench_cli_create_wrapper(py_like: bool) -> float:
    root = _temp_dir("cli-create") / "image.zarr"
    replacements = {str(root): "<ROOT>"}
    try:
        random.seed(0)
        if py_like:
            outcome = _run_cli_namespace(
                cli_eq._py_cli.create,
                argparse.Namespace(
                    method="coins",
                    path=str(root),
                    format="0.5",
                    verbose=0,
                    quiet=0,
                ),
                replacements,
            )
        else:
            outcome = _run_native_cli(
                ["create", "--method=coins", str(root), "--format", "0.5"],
                replacements,
            )
        return _touch_outcome(outcome) + _touch_value(snapshot_tree(root))
    finally:
        shutil.rmtree(root.parent, ignore_errors=True)


def _verify_cli_info_wrapper() -> None:
    py_root = _temp_dir("py-cli-info") / "image.zarr"
    cpp_root = _temp_dir("cpp-cli-info") / "image.zarr"
    replacements = {str(py_root): "<ROOT>", str(cpp_root): "<ROOT>"}
    try:
        write_minimal_v2_image(py_root)
        write_minimal_v2_image(cpp_root)
        _assert_equal(
            "cli.info_wrapper",
            _run_cli_namespace(
                cli_eq._py_cli.info,
                argparse.Namespace(
                    path=str(py_root),
                    stats=True,
                    verbose=0,
                    quiet=0,
                ),
                replacements,
            ),
            _run_native_cli(["info", str(cpp_root), "--stats"], replacements),
        )
    finally:
        shutil.rmtree(py_root.parent, ignore_errors=True)
        shutil.rmtree(cpp_root.parent, ignore_errors=True)


def _bench_cli_info_wrapper(py_like: bool) -> float:
    root = _temp_dir("cli-info") / "image.zarr"
    replacements = {str(root): "<ROOT>"}
    try:
        write_minimal_v2_image(root)
        if py_like:
            outcome = _run_cli_namespace(
                cli_eq._py_cli.info,
                argparse.Namespace(path=str(root), stats=True, verbose=0, quiet=0),
                replacements,
            )
        else:
            outcome = _run_native_cli(["info", str(root), "--stats"], replacements)
        return _touch_outcome(outcome)
    finally:
        shutil.rmtree(root.parent, ignore_errors=True)


def _verify_cli_download_wrapper() -> None:
    py_source = _temp_dir("py-cli-download-wrap") / "source.zarr"
    cpp_source = _temp_dir("cpp-cli-download-wrap") / "source.zarr"
    py_output = py_source.parent / "downloads"
    cpp_output = cpp_source.parent / "downloads"
    replacements = {
        str(py_source): "<SOURCE>",
        str(cpp_source): "<SOURCE>",
        str(py_output): "<OUT>",
        str(cpp_output): "<OUT>",
    }
    try:
        write_minimal_v2_image(py_source)
        write_minimal_v2_image(cpp_source)
        expected = _run_cli_namespace(
            cli_eq._py_cli.download,
            argparse.Namespace(
                path=str(py_source),
                output=str(py_output),
                verbose=0,
                quiet=0,
            ),
            replacements,
        )
        actual = _run_native_cli(
            ["download", str(cpp_source), f"--output={cpp_output}"],
            replacements,
        )
        if expected.status != actual.status or expected.status != "ok":
            _assert_equal("cli.download_wrapper.status", expected, actual)
        if utils_eq._normalize_download_stdout(
            expected.stdout, replacements
        ) != utils_eq._normalize_download_stdout(actual.stdout, replacements):
            raise AssertionError("Benchmark case cli.download_wrapper lost parity")
        _assert_equal(
            "cli.download_wrapper.tree",
            rewrite_snapshot_prefix(
                snapshot_tree(py_output),
                py_source.name,
                "<SOURCE_ROOT>",
            ),
            rewrite_snapshot_prefix(
                snapshot_tree(cpp_output),
                cpp_source.name,
                "<SOURCE_ROOT>",
            ),
        )
    finally:
        shutil.rmtree(py_source.parent, ignore_errors=True)
        shutil.rmtree(cpp_source.parent, ignore_errors=True)


def _bench_cli_download_wrapper(py_like: bool) -> float:
    source = _temp_dir("cli-download-wrap") / "source.zarr"
    output = source.parent / "downloads"
    replacements = {
        str(source): "<SOURCE>",
        str(output): "<OUT>",
    }
    try:
        write_minimal_v2_image(source)
        if py_like:
            outcome = _run_cli_namespace(
                cli_eq._py_cli.download,
                argparse.Namespace(
                    path=str(source),
                    output=str(output),
                    verbose=0,
                    quiet=0,
                ),
                replacements,
            )
        else:
            outcome = _run_native_cli(
                ["download", str(source), f"--output={output}"],
                replacements,
            )
        return _touch_outcome(outcome) + _touch_value(snapshot_tree(output))
    finally:
        shutil.rmtree(source.parent, ignore_errors=True)


def _write_scale_input(path: Path) -> None:
    array = zarr.open_array(
        str(path),
        mode="w",
        shape=(8, 8),
        chunks=(8, 8),
        dtype=np.uint16,
    )
    array[:] = np.arange(64, dtype=np.uint16).reshape(8, 8)
    array.attrs.update({"alpha": 1})


def _verify_cli_scale_wrapper() -> None:
    py_input = _temp_dir("py-cli-scale") / "input.zarr"
    cpp_input = _temp_dir("cpp-cli-scale") / "input.zarr"
    py_output = py_input.parent / "output.zarr"
    cpp_output = cpp_input.parent / "output.zarr"
    replacements = {
        str(py_input): "<INPUT>",
        str(cpp_input): "<INPUT>",
        str(py_output): "<OUT>",
        str(cpp_output): "<OUT>",
    }
    try:
        _write_scale_input(py_input)
        _write_scale_input(cpp_input)
        _assert_equal(
            "cli.scale_wrapper",
            _run_cli_namespace(
                cli_eq._py_cli.scale,
                argparse.Namespace(
                    input_array=str(py_input),
                    output_directory=str(py_output),
                    axes="yx",
                    copy_metadata=True,
                    method="nearest",
                    in_place=False,
                    downscale=2,
                    max_layer=2,
                ),
                replacements,
            ),
            _run_native_cli(
                [
                    "scale",
                    str(cpp_input),
                    str(cpp_output),
                    "yx",
                    "--copy-metadata",
                    "--method=nearest",
                    "--max_layer=2",
                ],
                replacements,
            ),
        )
        _assert_equal(
            "cli.scale_wrapper.tree",
            snapshot_tree(py_output),
            snapshot_tree(cpp_output),
        )
    finally:
        shutil.rmtree(py_input.parent, ignore_errors=True)
        shutil.rmtree(cpp_input.parent, ignore_errors=True)


def _bench_cli_scale_wrapper(py_like: bool) -> float:
    input_path = _temp_dir("cli-scale") / "input.zarr"
    output_path = input_path.parent / "output.zarr"
    replacements = {
        str(input_path): "<INPUT>",
        str(output_path): "<OUT>",
    }
    try:
        _write_scale_input(input_path)
        if py_like:
            outcome = _run_cli_namespace(
                cli_eq._py_cli.scale,
                argparse.Namespace(
                    input_array=str(input_path),
                    output_directory=str(output_path),
                    axes="yx",
                    copy_metadata=True,
                    method="nearest",
                    in_place=False,
                    downscale=2,
                    max_layer=2,
                ),
                replacements,
            )
        else:
            outcome = _run_native_cli(
                [
                    "scale",
                    str(input_path),
                    str(output_path),
                    "yx",
                    "--copy-metadata",
                    "--method=nearest",
                    "--max_layer=2",
                ],
                replacements,
            )
        return _touch_outcome(outcome) + _touch_value(snapshot_tree(output_path))
    finally:
        shutil.rmtree(input_path.parent, ignore_errors=True)


def _verify_csv_parse() -> None:
    values = []
    for literal in ("", "0", "1", "-1", "1.5", "abc", "True", "nan", "inf", "-inf"):
        for column_type in ("d", "l", "s", "b", "x"):
            values.append(
                (
                    literal,
                    column_type,
                    format_eq._call(
                        csv_eq._py_csv.parse_csv_value, literal, column_type
                    ),
                    format_eq._call(
                        csv_eq._run_native_parse_csv_value, literal, column_type
                    ),
                )
            )
    for literal, column_type, left, right in values:
        _assert_equal(f"csv.parse_csv_value[{literal!r},{column_type!r}]", left, right)


def _bench_csv_parse_python() -> float:
    total = 0.0
    for literal in ("", "0", "1", "-1", "1.5", "abc", "True", "nan", "inf", "-inf"):
        for column_type in ("d", "l", "s", "b", "x"):
            total += _touch_value(
                format_eq._call(csv_eq._py_csv.parse_csv_value, literal, column_type)
            )
    return total


def _verify_conversions_int_to_rgba() -> None:
    for value in core_cases._INT_RGBA_VALUES:
        _assert_equal(
            f"conversions.int_to_rgba[{value}]",
            conv_eq._py_conversions.int_to_rgba(value),
            conv_eq._run_native_int_to_rgba(value),
        )


def _bench_conversions_int_to_rgba_python() -> float:
    total = 0.0
    for value in core_cases._INT_RGBA_VALUES:
        rgba = conv_eq._py_conversions.int_to_rgba(value)
        total += float(rgba[0]) + float(rgba[3])
    return total


def _verify_conversions_rgba_to_int() -> None:
    for rgba in core_cases._RGBA_VALUES:
        _assert_equal(
            f"conversions.rgba_to_int[{rgba!r}]",
            conv_eq._py_conversions.rgba_to_int(*rgba),
            conv_eq._run_native_rgba_to_int(rgba),
        )


def _bench_conversions_rgba_to_int_python() -> float:
    total = 0
    for rgba in core_cases._RGBA_VALUES:
        total += conv_eq._py_conversions.rgba_to_int(*rgba)
    return float(total)


def _verify_csv_dict_to_zarr() -> None:
    root_attrs = {"multiscales": [{"version": "0.4"}], "extra": {"keep": True}}
    subgroup_attrs = {
        "labels/0": {
            "image-label": {"properties": [{"cell_id": 1, "name": "before"}]},
            "other": "value",
        }
    }
    props = {"1": {"score": 1.5, "alive": True}}
    py_root = _temp_dir("py-dict-to-zarr") / "image.zarr"
    cpp_root = _temp_dir("cpp-dict-to-zarr") / "image.zarr"
    try:
        csv_eq._write_zarr_tree(py_root, root_attrs, subgroup_attrs)
        csv_eq._write_zarr_tree(cpp_root, root_attrs, subgroup_attrs)
        _assert_equal(
            "csv.dict_to_zarr",
            csv_eq._run_dict_to_zarr(
                csv_eq._py_csv.dict_to_zarr, props, py_root, "cell_id"
            ),
            csv_eq._run_native_dict_to_zarr(props, cpp_root, "cell_id"),
        )
    finally:
        shutil.rmtree(py_root.parent, ignore_errors=True)
        shutil.rmtree(cpp_root.parent, ignore_errors=True)


def _bench_csv_dict_to_zarr_python() -> float:
    root = _temp_dir("csv-dict-to-zarr") / "image.zarr"
    try:
        csv_eq._write_zarr_tree(
            root,
            {"multiscales": [{"version": "0.4"}]},
            {"labels/0": {"image-label": {"properties": [{"cell_id": 1}]}}},
        )
        outcome = csv_eq._run_dict_to_zarr(
            csv_eq._py_csv.dict_to_zarr, {"1": {"score": 1}}, root, "cell_id"
        )
        return _touch_outcome(outcome)
    finally:
        shutil.rmtree(root.parent, ignore_errors=True)


def _verify_csv_csv_to_zarr() -> None:
    py_root = _temp_dir("py-csv-to-zarr") / "image.zarr"
    cpp_root = _temp_dir("cpp-csv-to-zarr") / "image.zarr"
    py_csv_path = py_root.parent / "props.csv"
    cpp_csv_path = cpp_root.parent / "props.csv"
    try:
        csv_eq._write_zarr_tree(
            py_root,
            {"multiscales": [{"version": "0.4"}]},
            {"labels/0": {"image-label": {"properties": [{"cell_id": 1}]}}},
        )
        csv_eq._write_zarr_tree(
            cpp_root,
            {"multiscales": [{"version": "0.4"}]},
            {"labels/0": {"image-label": {"properties": [{"cell_id": 1}]}}},
        )
        csv_eq._write_csv(py_csv_path, [["cell_id", "score#d"], ["1", "4.5"]])
        csv_eq._write_csv(cpp_csv_path, [["cell_id", "score#d"], ["1", "4.5"]])
        expected = csv_eq._run_csv_to_zarr(
            csv_eq._py_csv.csv_to_zarr,
            py_csv_path,
            "cell_id",
            "score#d",
            py_root,
            "cell_id",
        )
        actual = _run_native_cli(
            [
                "csv_to_labels",
                str(cpp_csv_path),
                "cell_id",
                "score#d",
                str(cpp_root),
                "cell_id",
            ]
        )
        assert expected.status == actual.status == "ok"
        assert snapshot_tree(py_root) == snapshot_tree(cpp_root)
    finally:
        shutil.rmtree(py_root.parent, ignore_errors=True)
        shutil.rmtree(cpp_root.parent, ignore_errors=True)


def _bench_csv_csv_to_zarr(py_like: bool) -> float:
    root = _temp_dir("csv-csv-to-zarr") / "image.zarr"
    csv_path = root.parent / "props.csv"
    try:
        csv_eq._write_zarr_tree(
            root,
            {"multiscales": [{"version": "0.4"}]},
            {"labels/0": {"image-label": {"properties": [{"cell_id": 1}]}}},
        )
        csv_eq._write_csv(csv_path, [["cell_id", "score#d"], ["1", "4.5"]])
        if py_like:
            outcome = csv_eq._run_csv_to_zarr(
                csv_eq._py_csv.csv_to_zarr,
                csv_path,
                "cell_id",
                "score#d",
                root,
                "cell_id",
            )
        else:
            outcome = _run_native_cli(
                [
                    "csv_to_labels",
                    str(csv_path),
                    "cell_id",
                    "score#d",
                    str(root),
                    "cell_id",
                ]
            )
        return _touch_outcome(outcome)
    finally:
        shutil.rmtree(root.parent, ignore_errors=True)


def _verify_data_make_circle() -> None:
    cases = [
        (8, 8, 1, np.float64),
        (9, 5, 7, np.int16),
        (12, 16, 2, np.uint8),
    ]
    for h, w, value, dtype in cases:
        _assert_equal(
            f"data.make_circle[{h},{w},{value},{dtype}]",
            data_eq._run_make_circle(data_eq._py_data.make_circle, h, w, value, dtype),
            data_eq._run_native_make_circle(
                target_shape=(h, w),
                circle_shape=(h, w),
                offset=(0, 0),
                value=value,
                dtype=dtype,
            ),
        )


def _bench_data_make_circle_python() -> float:
    total = 0.0
    for h, w, value, dtype in [
        (8, 8, 1, np.float64),
        (9, 5, 7, np.int16),
        (12, 16, 2, np.uint8),
    ]:
        total += _touch_outcome(
            data_eq._run_make_circle(data_eq._py_data.make_circle, h, w, value, dtype)
        )
    return total


def _verify_data_rgb_to_5d() -> None:
    for pixels in [
        np.arange(16, dtype=np.uint8).reshape(4, 4),
        np.arange(4 * 5 * 3, dtype=np.uint8).reshape(4, 5, 3),
        np.arange(2 * 3 * 4 * 5, dtype=np.uint8).reshape(2, 3, 4, 5),
    ]:
        _assert_equal(
            f"data.rgb_to_5d[{pixels.shape}]",
            data_eq._run_rgb_to_5d(data_eq._py_data.rgb_to_5d, pixels),
            data_eq._run_native_rgb_to_5d(pixels),
        )


def _bench_data_rgb_to_5d_python() -> float:
    total = 0.0
    for pixels in [
        np.arange(16, dtype=np.uint8).reshape(4, 4),
        np.arange(4 * 5 * 3, dtype=np.uint8).reshape(4, 5, 3),
        np.arange(2 * 3 * 4 * 5, dtype=np.uint8).reshape(2, 3, 4, 5),
    ]:
        total += _touch_outcome(
            data_eq._run_rgb_to_5d(data_eq._py_data.rgb_to_5d, pixels)
        )
    return total


def _verify_format_dispatch() -> None:
    versions = ("0.1", "0.2", "0.3", "0.4", "0.5", 0.2, 0.5)
    py_results = [
        format_eq._call(format_eq._py_format.format_from_version, version)
        for version in versions
    ]
    cpp_results = [
        format_eq._call(format_eq._cpp_format.format_from_version, version)
        for version in versions
    ]
    py_dispatch = [
        format_eq._format_signature(
            format_eq._py_format.detect_format(
                metadata, format_eq._py_format.FormatV03()
            )
        )
        for metadata in (
            {},
            {"multiscales": [{"version": "0.5"}]},
            {"plate": {"version": "0.4"}},
            {"well": {"version": "0.3"}},
            {"image-label": {"version": "0.2"}},
        )
    ]
    cpp_dispatch = [
        format_eq._format_signature(
            format_eq._cpp_format.detect_format(
                metadata, format_eq._cpp_format.FormatV03()
            )
        )
        for metadata in (
            {},
            {"multiscales": [{"version": "0.5"}]},
            {"plate": {"version": "0.4"}},
            {"well": {"version": "0.3"}},
            {"image-label": {"version": "0.2"}},
        )
    ]
    _assert_equal(
        "format.dispatch",
        {
            "from_version": [
                (
                    item.status,
                    None
                    if item.status == "err"
                    else format_eq._format_signature(item.value),
                    item.error_message,
                )
                for item in py_results
            ],
            "implementations": [
                format_eq._format_signature(fmt)
                for fmt in format_eq._py_format.format_implementations()
            ],
            "detect": py_dispatch,
        },
        {
            "from_version": [
                (
                    item.status,
                    None
                    if item.status == "err"
                    else format_eq._format_signature(item.value),
                    item.error_message,
                )
                for item in cpp_results
            ],
            "implementations": [
                format_eq._format_signature(fmt)
                for fmt in format_eq._cpp_format.format_implementations()
            ],
            "detect": cpp_dispatch,
        },
    )


def _bench_format_dispatch(module) -> float:
    versions = ("0.1", "0.2", "0.3", "0.4", "0.5", 0.2, 0.5)
    total = 0.0
    for version in versions:
        total += _touch_outcome(format_eq._call(module.format_from_version, version))
    total += _touch_value(
        [format_eq._format_signature(fmt) for fmt in module.format_implementations()]
    )
    default = module.FormatV03()
    for metadata in (
        {},
        {"multiscales": [{"version": "0.5"}]},
        {"plate": {"version": "0.4"}},
        {"well": {"version": "0.3"}},
        {"image-label": {"version": "0.2"}},
    ):
        total += _touch_value(
            format_eq._format_signature(module.detect_format(metadata, default))
        )
    return total


def _verify_format_matches() -> None:
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
    left = []
    right = []
    for py_cls, cpp_cls in zip(
        format_eq.PY_FORMAT_TYPES, format_eq.CPP_FORMAT_TYPES, strict=True
    ):
        py_fmt = py_cls()
        cpp_fmt = cpp_cls()
        left.append(
            (
                format_eq._format_signature(py_fmt),
                [py_fmt.matches(metadata) for metadata in metadata_cases],
            )
        )
        right.append(
            (
                format_eq._format_signature(cpp_fmt),
                [cpp_fmt.matches(metadata) for metadata in metadata_cases],
            )
        )
    _assert_equal("format.matches", left, right)


def _bench_format_matches(module) -> float:
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
    total = 0.0
    for cls in (
        module.FormatV01,
        module.FormatV02,
        module.FormatV03,
        module.FormatV04,
        module.FormatV05,
    ):
        fmt = cls()
        total += _touch_value(format_eq._format_signature(fmt))
        total += _touch_value([fmt.matches(metadata) for metadata in metadata_cases])
    return total


def _run_format_v01_init_store(format_module) -> dict[str, object]:
    local_root = _temp_dir("format-v01-store")
    try:
        fmt = format_module.FormatV01()
        local = fmt.init_store(str(local_root / "local.zarr"), mode="w")
        calls: list[tuple[str, bool]] = []

        class SentinelStore:
            def __init__(self, path: str, read_only: bool) -> None:
                self.path = path
                self.read_only = read_only

        def fake_from_url(path: str, storage_options=None, read_only=False):
            calls.append((path, read_only))
            return SentinelStore(path, read_only)

        with patch.object(
            format_module.FsspecStore, "from_url", staticmethod(fake_from_url)
        ):
            remote = fmt.init_store("https://example.invalid/image.zarr", mode="r")
        return {
            "local_type": type(local).__name__,
            "local_read_only": local.read_only,
            "remote_type": type(remote).__name__,
            "remote_read_only": remote.read_only,
            "calls": calls,
        }
    finally:
        shutil.rmtree(local_root, ignore_errors=True)


def _verify_format_v01_init_store() -> None:
    _assert_equal(
        "format.v01_init_store",
        _run_format_v01_init_store(format_eq._py_format),
        _run_format_v01_init_store(format_eq._cpp_format),
    )


def _bench_format_v01_init_store(module) -> float:
    return _touch_value(_run_format_v01_init_store(module))


def _verify_format_well_and_coord() -> None:
    rows = ["A", "B", "C"]
    columns = ["1", "2", "3"]
    coord_shapes = [
        (256, 256),
        (128, 128),
        (64, 64),
    ]
    py_generate_coord = (
        format_eq._py_format.FormatV04().generate_coordinate_transformations(
            coord_shapes
        )
    )
    py_generate_coord_v01 = (
        format_eq._py_format.FormatV01().generate_coordinate_transformations(
            coord_shapes
        )
    )
    cpp_generate_coord = (
        format_eq._cpp_format.FormatV04().generate_coordinate_transformations(
            coord_shapes
        )
    )
    cpp_generate_coord_v01 = (
        format_eq._cpp_format.FormatV01().generate_coordinate_transformations(
            coord_shapes
        )
    )
    left = {
        "generate_well_v01": [
            format_eq._call(
                format_eq._py_format.FormatV01().generate_well_dict,
                well,
                rows,
                columns,
            )
            for well in ("A/1", "B/3")
        ],
        "generate_well": [
            format_eq._call(
                format_eq._py_format.FormatV04().generate_well_dict, well, rows, columns
            )
            for well in ("A/1", "B/3", "D/1", "A/9", "A", "A/1/2")
        ],
        "validate_well_v01": [
            format_eq._call(
                format_eq._py_format.FormatV01().validate_well_dict,
                dict(well),
                rows,
                columns,
            )
            for well in ({"path": "A/1"}, {"path": "A/1", "extra": 1}, {}, {"path": 3})
        ],
        "validate_well_v04": [
            format_eq._call(
                format_eq._py_format.FormatV04().validate_well_dict,
                dict(well),
                rows,
                columns,
            )
            for well in (
                {"path": "A/1", "rowIndex": 0, "columnIndex": 0},
                {"path": "A/1"},
                {"path": "D/1", "rowIndex": 0, "columnIndex": 0},
                {"path": "A/1", "rowIndex": 1, "columnIndex": 0},
            )
        ],
        "generate_coord": py_generate_coord,
        "generate_coord_v01": py_generate_coord_v01,
        "validate_coord_v01": format_eq._call(
            format_eq._py_format.FormatV01().validate_coordinate_transformations,
            2,
            2,
            None,
        ),
        "validate_coord": format_eq._call(
            format_eq._py_format.FormatV04().validate_coordinate_transformations,
            2,
            2,
            [
                [{"type": "scale", "scale": (1, 1)}],
                [{"type": "scale", "scale": (0.5, 0.5)}],
            ],
        ),
    }
    right = {
        "generate_well_v01": [
            format_eq._call(
                format_eq._cpp_format.FormatV01().generate_well_dict,
                well,
                rows,
                columns,
            )
            for well in ("A/1", "B/3")
        ],
        "generate_well": [
            format_eq._call(
                format_eq._cpp_format.FormatV04().generate_well_dict,
                well,
                rows,
                columns,
            )
            for well in ("A/1", "B/3", "D/1", "A/9", "A", "A/1/2")
        ],
        "validate_well_v01": [
            format_eq._call(
                format_eq._cpp_format.FormatV01().validate_well_dict,
                dict(well),
                rows,
                columns,
            )
            for well in ({"path": "A/1"}, {"path": "A/1", "extra": 1}, {}, {"path": 3})
        ],
        "validate_well_v04": [
            format_eq._call(
                format_eq._cpp_format.FormatV04().validate_well_dict,
                dict(well),
                rows,
                columns,
            )
            for well in (
                {"path": "A/1", "rowIndex": 0, "columnIndex": 0},
                {"path": "A/1"},
                {"path": "D/1", "rowIndex": 0, "columnIndex": 0},
                {"path": "A/1", "rowIndex": 1, "columnIndex": 0},
            )
        ],
        "generate_coord": cpp_generate_coord,
        "generate_coord_v01": cpp_generate_coord_v01,
        "validate_coord_v01": format_eq._call(
            format_eq._cpp_format.FormatV01().validate_coordinate_transformations,
            2,
            2,
            None,
        ),
        "validate_coord": format_eq._call(
            format_eq._cpp_format.FormatV04().validate_coordinate_transformations,
            2,
            2,
            [
                [{"type": "scale", "scale": (1, 1)}],
                [{"type": "scale", "scale": (0.5, 0.5)}],
            ],
        ),
    }
    _assert_equal("format.well_and_coord", left, right)


def _bench_format_well_and_coord(module) -> float:
    rows = ["A", "B", "C"]
    columns = ["1", "2", "3"]
    coord_shapes = [(256, 256), (128, 128), (64, 64)]
    fmt01 = module.FormatV01()
    fmt04 = module.FormatV04()
    total = 0.0
    for well in ("A/1", "B/3"):
        total += _touch_outcome(
            format_eq._call(fmt01.generate_well_dict, well, rows, columns)
        )
    for well in ("A/1", "B/3", "D/1", "A/9", "A", "A/1/2"):
        total += _touch_outcome(
            format_eq._call(fmt04.generate_well_dict, well, rows, columns)
        )
    for well in ({"path": "A/1"}, {"path": "A/1", "extra": 1}, {}, {"path": 3}):
        total += _touch_outcome(
            format_eq._call(fmt01.validate_well_dict, dict(well), rows, columns)
        )
    for well in (
        {"path": "A/1", "rowIndex": 0, "columnIndex": 0},
        {"path": "A/1"},
        {"path": "D/1", "rowIndex": 0, "columnIndex": 0},
        {"path": "A/1", "rowIndex": 1, "columnIndex": 0},
    ):
        total += _touch_outcome(
            format_eq._call(fmt04.validate_well_dict, dict(well), rows, columns)
        )
    total += _touch_value(fmt01.generate_coordinate_transformations(coord_shapes))
    total += _touch_outcome(
        format_eq._call(fmt01.validate_coordinate_transformations, 2, 2, None)
    )
    total += _touch_value(fmt04.generate_coordinate_transformations(coord_shapes))
    total += _touch_outcome(
        format_eq._call(
            fmt04.validate_coordinate_transformations,
            2,
            2,
            [
                [{"type": "scale", "scale": (1, 1)}],
                [{"type": "scale", "scale": (0.5, 0.5)}],
            ],
        )
    )
    return total


def _verify_io_location_methods() -> None:
    v2_root = _temp_dir("io-v2") / "image.zarr"
    v3_root = _temp_dir("io-v3") / "image.zarr"
    try:
        io_eq._write_minimal_v2_image(v2_root)
        io_eq._write_minimal_v3_image(v3_root)
        _assert_equal(
            "io.location_methods",
            {
                "v2": io_eq._run_parse_url(
                    io_eq._py_io.parse_url, str(v2_root), mode="r"
                ),
                "v3": io_eq._run_parse_url(
                    io_eq._py_io.parse_url, str(v3_root), mode="r"
                ),
            },
            {
                "v2": io_eq._run_parse_url(
                    io_eq._cpp_io.parse_url, str(v2_root), mode="r"
                ),
                "v3": io_eq._run_parse_url(
                    io_eq._cpp_io.parse_url, str(v3_root), mode="r"
                ),
            },
        )
    finally:
        shutil.rmtree(v2_root.parent, ignore_errors=True)
        shutil.rmtree(v3_root.parent, ignore_errors=True)


def _bench_io_location_methods(module) -> float:
    v2_root = _temp_dir("io-location") / "image.zarr"
    try:
        io_eq._write_minimal_v2_image(v2_root)
        return _touch_outcome(
            io_eq._run_parse_url(module.parse_url, str(v2_root), mode="r")
        )
    finally:
        shutil.rmtree(v2_root.parent, ignore_errors=True)


def _run_io_create_load(io_module, root: Path):
    fmt = (
        io_eq._py_format.FormatV05()
        if io_module is io_eq._py_io
        else io_eq._cpp_format.FormatV05()
    )
    write_location = io_module.parse_url(root, mode="w", fmt=fmt)
    if write_location is None:
        raise AssertionError("parse_url unexpectedly returned None in write mode")
    group = zarr.open_group(str(root), mode="a", zarr_format=3)
    array = group.create_array(name="s0", shape=(2, 2), chunks=(2, 2), dtype="i4")
    array[:] = [[1, 2], [3, 4]]
    read_location = io_module.parse_url(root, mode="r", fmt=fmt)
    if read_location is None:
        raise AssertionError("parse_url unexpectedly returned None in read mode")
    child = read_location.create("s0")
    return {
        "child_path": Path(child.path).relative_to(root.parent).as_posix(),
        "child_exists": child.exists(),
        "loaded": read_location.load("s0").compute().tolist(),
    }


def _verify_io_create_load() -> None:
    py_root = _temp_dir("py-io-create-load") / "image.zarr"
    cpp_root = _temp_dir("cpp-io-create-load") / "image.zarr"
    try:
        _assert_equal(
            "io.create_load",
            _run_io_create_load(io_eq._py_io, py_root),
            _run_io_create_load(io_eq._cpp_io, cpp_root),
        )
    finally:
        shutil.rmtree(py_root.parent, ignore_errors=True)
        shutil.rmtree(cpp_root.parent, ignore_errors=True)


def _bench_io_create_load(io_module) -> float:
    root = _temp_dir("io-create-load") / "image.zarr"
    try:
        return _touch_value(_run_io_create_load(io_module, root))
    finally:
        shutil.rmtree(root.parent, ignore_errors=True)


def _verify_utils_path_helpers() -> None:
    parts_cases = [
        [["root", "a", "b"], ["root", "a", "c"]],
        [["only"]],
        [[], []],
    ]
    paths = ["alpha/beta/gamma", "/tmp/demo/image.zarr", "C:/data/demo.zarr"]
    left = {
        "strip": [
            utils_eq._run_strip_common_prefix(
                utils_eq._py_utils.strip_common_prefix, parts
            )
            for parts in parts_cases
        ],
        "splitall": [
            utils_eq._run_splitall(utils_eq._py_utils.splitall, path) for path in paths
        ],
    }
    right = {
        "strip": [
            utils_eq._run_native_strip_common_prefix(parts) for parts in parts_cases
        ],
        "splitall": [utils_eq._run_native_splitall(path) for path in paths],
    }
    _assert_equal("utils.path_helpers", left, right)


def _bench_utils_path_helpers_python() -> float:
    total = 0.0
    for parts in (
        [["root", "a", "b"], ["root", "a", "c"]],
        [["only"]],
        [[], []],
    ):
        total += _touch_outcome(
            utils_eq._run_strip_common_prefix(
                utils_eq._py_utils.strip_common_prefix, parts
            )
        )
    for path in ("alpha/beta/gamma", "/tmp/demo/image.zarr", "C:/data/demo.zarr"):
        total += _touch_outcome(
            utils_eq._run_splitall(utils_eq._py_utils.splitall, path)
        )
    return total


def _verify_utils_find_multiscales() -> None:
    _assert_equal(
        "utils.find_multiscales",
        utils_eq._run_find_multiscales(
            utils_eq._py_utils.find_multiscales, "/tmp/demo/image.zarr"
        ),
        utils_eq._run_native_find_multiscales("/tmp/demo/image.zarr"),
    )


def _bench_utils_find_multiscales_python() -> float:
    return _touch_outcome(
        utils_eq._run_find_multiscales(
            utils_eq._py_utils.find_multiscales, "/tmp/demo/image.zarr"
        )
    )


def _verify_utils_finder_and_view() -> None:
    finder_root = _temp_dir("finder-tree")
    view_root = _temp_dir("view-tree")
    try:
        utils_eq._build_nested_finder_tree(finder_root)
        utils_eq._build_simple_view_tree(view_root)
        expected_finder = utils_eq._run_finder(
            utils_eq._py_utils.finder, str(finder_root), port=8012, dry_run=True
        )
        actual_finder = _run_native_cli(["finder", str(finder_root), "--port", "8012"])
        assert expected_finder.status == actual_finder.status == "ok"
        assert expected_finder.stdout == actual_finder.stdout

        expected_view = _run_python_view(
            view_root / "image.zarr", port=8013, force=False
        )
        actual_view = _run_native_view(view_root / "image.zarr", port=8013, force=False)
        assert expected_view.status == actual_view.status == "ok"
        assert expected_view.value["url"] == actual_view.value["url"]
        assert expected_view.value["body"] == actual_view.value["body"]
    finally:
        shutil.rmtree(finder_root, ignore_errors=True)
        shutil.rmtree(view_root, ignore_errors=True)


def _bench_utils_finder(py_like: bool) -> float:
    finder_root = _temp_dir("finder-bench")
    try:
        utils_eq._build_nested_finder_tree(finder_root)
        if py_like:
            outcome = utils_eq._run_finder(
                utils_eq._py_utils.finder, str(finder_root), port=8012, dry_run=True
            )
        else:
            outcome = _run_native_cli(["finder", str(finder_root), "--port", "8012"])
        return _touch_outcome(outcome)
    finally:
        shutil.rmtree(finder_root, ignore_errors=True)


def _bench_utils_view(py_like: bool) -> float:
    view_root = _temp_dir("view-bench")
    try:
        utils_eq._build_simple_view_tree(view_root)
        if py_like:
            outcome = _run_python_view(view_root / "image.zarr", port=8013, force=False)
        else:
            outcome = _run_native_view(view_root / "image.zarr", port=8013, force=False)
        return _touch_outcome(outcome)
    finally:
        shutil.rmtree(view_root, ignore_errors=True)


def _verify_utils_download() -> None:
    py_source = _temp_dir("py-utils-download") / "source-v2.zarr"
    cpp_source = _temp_dir("cpp-utils-download") / "source-v2.zarr"
    py_output = py_source.parent / "downloads"
    cpp_output = cpp_source.parent / "downloads"
    try:
        write_minimal_v2_image(py_source)
        write_minimal_v2_image(cpp_source)
        left = _run_utils_download(utils_eq._py_utils.download, py_source, py_output)
        right = _run_native_cli(["download", str(cpp_source), f"--output={cpp_output}"])
        if left.status != right.status or left.status != "ok":
            _assert_equal("utils.download.status", left, right)
        left_replacements = {
            str(py_source): "<SOURCE>",
            str(py_output): "<OUTPUT>",
        }
        right_replacements = {
            str(cpp_source): "<SOURCE>",
            str(cpp_output): "<OUTPUT>",
        }
        if utils_eq._normalize_download_stdout(
            left.stdout, left_replacements
        ) != utils_eq._normalize_download_stdout(right.stdout, right_replacements):
            raise AssertionError("Benchmark case utils.download lost parity")
        _assert_equal(
            "utils.download.tree",
            left.tree,
            snapshot_tree(cpp_output),
        )
    finally:
        shutil.rmtree(py_source.parent, ignore_errors=True)
        shutil.rmtree(cpp_source.parent, ignore_errors=True)


def _bench_utils_download(py_like: bool) -> float:
    source = _temp_dir("utils-download") / "source-v2.zarr"
    output = source.parent / "downloads"
    try:
        write_minimal_v2_image(source)
        if py_like:
            outcome = _run_utils_download(utils_eq._py_utils.download, source, output)
        else:
            outcome = _run_native_cli(["download", str(source), f"--output={output}"])
        return _touch_outcome(outcome)
    finally:
        shutil.rmtree(source.parent, ignore_errors=True)


def _run_reader_matches(module) -> dict[str, object]:
    image_hierarchy = reader_eq._build_image_tree()
    hcs_hierarchy = reader_eq._build_hcs_tree()
    return {
        "Labels.matches": module.Labels.matches(
            image_hierarchy.nodes["/dataset/labels"]
        ),
        "Label.matches": module.Label.matches(
            image_hierarchy.nodes["/dataset/labels/coins"]
        ),
        "Multiscales.matches": module.Multiscales.matches(
            image_hierarchy.nodes["/dataset"]
        ),
        "OMERO.matches": module.OMERO.matches(image_hierarchy.nodes["/dataset"]),
        "Plate.matches": module.Plate.matches(hcs_hierarchy.nodes["/plate"]),
        "Well.matches": module.Well.matches(hcs_hierarchy.nodes["/plate/A/1"]),
    }


def _verify_reader_matches() -> None:
    _assert_equal(
        "reader.matches",
        _run_reader_matches(reader_eq._py_reader),
        _run_reader_matches(reader_eq._cpp_reader),
    )


def _bench_reader_matches(module) -> float:
    return _touch_value(_run_reader_matches(module))


def _run_reader_node_ops(module) -> dict[str, object]:
    hierarchy = reader_eq._build_image_tree()
    zarr = hierarchy.nodes["/dataset"]
    node = module.Node(zarr, [])
    before = reader_eq._node_signature(node, module)
    node.visible = False
    hidden = reader_eq._node_signature(node, module)
    node.visible = True
    shown = reader_eq._node_signature(node, module)
    labels_spec = node.first(module.Labels)
    loaded = node.load(module.Multiscales)
    metadata = {}
    node.write_metadata(metadata)
    duplicate = node.add(hierarchy.nodes["/dataset/labels"])
    return {
        "before": before,
        "hidden": hidden,
        "shown": shown,
        "first_labels": None if labels_spec is None else type(labels_spec).__name__,
        "load_multiscales": None if loaded is None else type(loaded).__name__,
        "metadata": metadata,
        "duplicate_add": None if duplicate is None else repr(duplicate),
    }


def _verify_reader_node_ops() -> None:
    _assert_equal(
        "reader.node_ops",
        _run_reader_node_ops(reader_eq._py_reader),
        _run_reader_node_ops(reader_eq._cpp_reader),
    )


def _bench_reader_node_ops(module) -> float:
    return _touch_value(_run_reader_node_ops(module))


def _run_reader_image_surface(module):
    hierarchy = reader_eq._build_image_tree()
    zarr = hierarchy.nodes["/dataset"]
    node = module.Node(zarr, [])
    multiscales = node.first(module.Multiscales)
    assert multiscales is not None
    reader = module.Reader(zarr)
    return {
        "array": reader_eq._array_signature(multiscales.array("0")),
        "descend": [
            reader_eq._node_signature(item, module) for item in reader.descend(node)
        ],
        "lookup": multiscales.lookup("multiscales", []),
    }


def _verify_reader_image_surface() -> None:
    _assert_equal(
        "reader.image_surface",
        _run_reader_image_surface(reader_eq._py_reader),
        _run_reader_image_surface(reader_eq._cpp_reader),
    )


def _bench_reader_image_surface(module) -> float:
    return _touch_value(_run_reader_image_surface(module))


def _run_reader_plate_surface(module):
    hierarchy = reader_eq._build_hcs_tree()
    plate_zarr = hierarchy.nodes["/plate"]
    plate_node = module.Node(plate_zarr, [])
    plate = plate_node.first(module.Plate)
    assert plate is not None
    plate.get_pyramid_lazy(plate_node)
    well_node = module.Node(hierarchy.nodes["/plate/A/1"], [])
    return {
        "get_numpy_type": str(plate.get_numpy_type(well_node)),
        "get_tile_path": plate.get_tile_path(0, 0, 0),
        "get_stitched_grid": reader_eq._array_signature(
            plate.get_stitched_grid(0, (2, 3))
        ),
        "after_get_pyramid_lazy": {
            "metadata": plate_node.metadata,
            "data": [reader_eq._array_signature(item) for item in plate_node.data],
        },
        "reader_signature": reader_eq._reader_signature(module, plate_zarr),
    }


def _verify_reader_plate_surface() -> None:
    _assert_equal(
        "reader.plate_surface",
        _run_reader_plate_surface(reader_eq._py_reader),
        _run_reader_plate_surface(reader_eq._cpp_reader),
    )


def _bench_reader_plate_surface(module) -> float:
    return _touch_value(_run_reader_plate_surface(module))


def _verify_scaler_methods() -> None:
    _assert_equal(
        "scale.scaler_methods",
        list(scaler_eq._py_scale.Scaler.methods()),
        list(scaler_eq._cpp_scale.Scaler.methods()),
    )


def _bench_scaler_methods(module) -> float:
    return _touch_value(list(module.Scaler.methods()))


def _verify_scaler_method(method_name: str, inputs: list[object]) -> None:
    for item in inputs:
        _assert_equal(
            f"scale.{method_name}[{getattr(item, 'shape', None)}]",
            scaler_eq._run_scaler_method(scaler_eq._py_scale.Scaler, method_name, item),
            scaler_eq._run_scaler_method(
                scaler_eq._cpp_scale.Scaler, method_name, item
            ),
        )


def _bench_scaler_method(module, method_name: str, inputs: list[object]) -> float:
    total = 0.0
    for item in inputs:
        total += _touch_outcome(
            scaler_eq._run_scaler_method(module.Scaler, method_name, item)
        )
    return total


def _verify_writer_metadata_helpers() -> None:
    py_fmt, cpp_fmt, zarr_format = writer_rt._fmt_pair("0.5")
    py_root = _temp_dir("py-writer-meta") / "meta.zarr"
    cpp_root = _temp_dir("cpp-writer-meta") / "meta.zarr"
    try:
        py_group = zarr.open_group(str(py_root), mode="w", zarr_format=zarr_format)
        cpp_group = zarr.open_group(str(cpp_root), mode="w", zarr_format=zarr_format)
        metadata = {"alpha": {"one": 1}, "beta": ["x", "y"]}
        writer_rt._py_writer.add_metadata(py_group, copy.deepcopy(metadata), py_fmt)
        writer_rt._cpp_writer.add_metadata(cpp_group, copy.deepcopy(metadata), cpp_fmt)
        _assert_equal(
            "writer.metadata_helpers",
            writer_rt._py_writer.get_metadata(py_group),
            writer_rt._cpp_writer.get_metadata(cpp_group),
        )
        _assert_equal(
            "writer.metadata_helpers.tree",
            snapshot_tree(py_root),
            snapshot_tree(cpp_root),
        )
    finally:
        shutil.rmtree(py_root.parent, ignore_errors=True)
        shutil.rmtree(cpp_root.parent, ignore_errors=True)


def _bench_writer_metadata_helpers(writer_module, format_module) -> float:
    root = _temp_dir("writer-meta") / "meta.zarr"
    try:
        group = zarr.open_group(str(root), mode="w", zarr_format=3)
        writer_module.add_metadata(
            group, {"alpha": {"one": 1}, "beta": ["x", "y"]}, format_module.FormatV05()
        )
        return _touch_value(writer_module.get_metadata(group)) + _touch_value(
            snapshot_tree(root)
        )
    finally:
        shutil.rmtree(root.parent, ignore_errors=True)


def _verify_writer_group_helpers() -> None:
    py_fmt, cpp_fmt, _ = writer_rt._fmt_pair("0.5")
    py_root = _temp_dir("py-check-group") / "image.zarr"
    cpp_root = _temp_dir("cpp-check-group") / "image.zarr"
    try:
        _assert_equal(
            "writer.group_helpers",
            {
                "check_group_fmt": writer_rt._run_tree_call(
                    writer_rt._py_writer.check_group_fmt,
                    py_root,
                    str(py_root),
                    py_fmt,
                    signature=writer_rt._signature_group_fmt,
                ),
                "check_format": writer_rt._py_writer.check_format(
                    zarr.open_group(
                        str(py_root.parent / "check.zarr"), mode="w", zarr_format=3
                    ),
                    py_fmt,
                ).version,
            },
            {
                "check_group_fmt": writer_rt._run_tree_call(
                    writer_rt._cpp_writer.check_group_fmt,
                    cpp_root,
                    str(cpp_root),
                    cpp_fmt,
                    signature=writer_rt._signature_group_fmt,
                ),
                "check_format": writer_rt._cpp_writer.check_format(
                    zarr.open_group(
                        str(cpp_root.parent / "check.zarr"), mode="w", zarr_format=3
                    ),
                    cpp_fmt,
                ).version,
            },
        )
    finally:
        shutil.rmtree(py_root.parent, ignore_errors=True)
        shutil.rmtree(cpp_root.parent, ignore_errors=True)


def _bench_writer_group_helpers(writer_module, format_module) -> float:
    root = _temp_dir("writer-group") / "image.zarr"
    try:
        outcome = writer_rt._run_tree_call(
            writer_module.check_group_fmt,
            root,
            str(root),
            format_module.FormatV05(),
            signature=writer_rt._signature_group_fmt,
        )
        return _touch_outcome(outcome)
    finally:
        shutil.rmtree(root.parent, ignore_errors=True)


def _verify_writer_metadata_writers() -> None:
    colors = [{"label-value": 1, "rgba": [255, 0, 0, 255]}]
    properties = [{"label-value": 1, "class": "cell"}]
    datasets = [
        {
            "path": "s0",
            "coordinateTransformations": [{"type": "scale", "scale": [1, 1]}],
        },
        {
            "path": "s1",
            "coordinateTransformations": [{"type": "scale", "scale": [2, 2]}],
        },
    ]
    py_fmt, cpp_fmt, zarr_format = writer_rt._fmt_pair("0.5")
    py_root = _temp_dir("py-writer-meta-writers") / "image.zarr"
    cpp_root = _temp_dir("cpp-writer-meta-writers") / "image.zarr"
    try:
        py_group = zarr.open_group(str(py_root), mode="w", zarr_format=zarr_format)
        cpp_group = zarr.open_group(str(cpp_root), mode="w", zarr_format=zarr_format)
        py_labels = py_group.require_group("labels")
        cpp_labels = cpp_group.require_group("labels")
        py_labels.require_group("cells")
        cpp_labels.require_group("cells")
        _assert_equal(
            "writer.metadata_writers",
            {
                "multiscales": writer_rt._run_tree_call(
                    writer_rt._py_writer.write_multiscales_metadata,
                    py_root,
                    py_group,
                    copy.deepcopy(datasets),
                    py_fmt,
                    ["y", "x"],
                    "sample",
                    metadata={"omero": {"channels": []}},
                ),
                "plate": writer_rt._run_tree_call(
                    writer_rt._py_writer.write_plate_metadata,
                    py_root,
                    py_group,
                    ["A", "B"],
                    ["1", "2"],
                    ["A/1", "B/2"],
                    py_fmt,
                    [{"id": 0, "name": "scan"}],
                    3,
                    "plate-a",
                ),
                "well": writer_rt._run_tree_call(
                    writer_rt._py_writer.write_well_metadata,
                    py_root,
                    py_group,
                    [{"path": "0"}, {"path": "1", "acquisition": 3}],
                    py_fmt,
                ),
                "label": writer_rt._run_tree_call(
                    writer_rt._py_writer.write_label_metadata,
                    py_root,
                    py_labels,
                    "cells",
                    copy.deepcopy(colors),
                    copy.deepcopy(properties),
                    py_fmt,
                ),
            },
            {
                "multiscales": writer_rt._run_tree_call(
                    writer_rt._cpp_writer.write_multiscales_metadata,
                    cpp_root,
                    cpp_group,
                    copy.deepcopy(datasets),
                    cpp_fmt,
                    ["y", "x"],
                    "sample",
                    metadata={"omero": {"channels": []}},
                ),
                "plate": writer_rt._run_tree_call(
                    writer_rt._cpp_writer.write_plate_metadata,
                    cpp_root,
                    cpp_group,
                    ["A", "B"],
                    ["1", "2"],
                    ["A/1", "B/2"],
                    cpp_fmt,
                    [{"id": 0, "name": "scan"}],
                    3,
                    "plate-a",
                ),
                "well": writer_rt._run_tree_call(
                    writer_rt._cpp_writer.write_well_metadata,
                    cpp_root,
                    cpp_group,
                    [{"path": "0"}, {"path": "1", "acquisition": 3}],
                    cpp_fmt,
                ),
                "label": writer_rt._run_tree_call(
                    writer_rt._cpp_writer.write_label_metadata,
                    cpp_root,
                    cpp_labels,
                    "cells",
                    copy.deepcopy(colors),
                    copy.deepcopy(properties),
                    cpp_fmt,
                ),
            },
        )
    finally:
        shutil.rmtree(py_root.parent, ignore_errors=True)
        shutil.rmtree(cpp_root.parent, ignore_errors=True)


def _bench_writer_metadata_writers(writer_module, format_module) -> float:
    root = _temp_dir("writer-meta-writers") / "image.zarr"
    try:
        group = zarr.open_group(str(root), mode="w", zarr_format=3)
        labels = group.require_group("labels")
        labels.require_group("cells")
        fmt = format_module.FormatV05()
        total = 0.0
        total += _touch_outcome(
            writer_rt._run_tree_call(
                writer_module.write_multiscales_metadata,
                root,
                group,
                [
                    {
                        "path": "s0",
                        "coordinateTransformations": [
                            {"type": "scale", "scale": [1, 1]}
                        ],
                    },
                    {
                        "path": "s1",
                        "coordinateTransformations": [
                            {"type": "scale", "scale": [2, 2]}
                        ],
                    },
                ],
                fmt,
                ["y", "x"],
                "sample",
                metadata={"omero": {"channels": []}},
            )
        )
        total += _touch_outcome(
            writer_rt._run_tree_call(
                writer_module.write_plate_metadata,
                root,
                group,
                ["A", "B"],
                ["1", "2"],
                ["A/1", "B/2"],
                fmt,
                [{"id": 0, "name": "scan"}],
                3,
                "plate-a",
            )
        )
        total += _touch_outcome(
            writer_rt._run_tree_call(
                writer_module.write_well_metadata,
                root,
                group,
                [{"path": "0"}, {"path": "1", "acquisition": 3}],
                fmt,
            )
        )
        total += _touch_outcome(
            writer_rt._run_tree_call(
                writer_module.write_label_metadata,
                root,
                labels,
                "cells",
                [{"label-value": 1, "rgba": [255, 0, 0, 255]}],
                [{"label-value": 1, "class": "cell"}],
                fmt,
            )
        )
        return total
    finally:
        shutil.rmtree(root.parent, ignore_errors=True)


def _verify_writer_runtime_writers() -> None:
    py_root = _temp_dir("py-writer-runtime") / "image.zarr"
    cpp_root = _temp_dir("cpp-writer-runtime") / "image.zarr"
    try:
        _assert_equal(
            "writer.runtime_writers",
            {
                "write_multiscale": _run_writer_multiscale(
                    writer_rt._py_writer, format_eq._py_format, py_root
                ),
                "write_multiscale_labels": _run_writer_multiscale_labels(
                    writer_rt._py_writer,
                    format_eq._py_format,
                    py_root.parent / "labels.zarr",
                ),
                "write_labels": _run_writer_labels(
                    writer_rt._py_writer,
                    format_eq._py_format,
                    py_root.parent / "labels-only.zarr",
                ),
            },
            {
                "write_multiscale": _run_writer_multiscale(
                    writer_rt._cpp_writer, format_eq._cpp_format, cpp_root
                ),
                "write_multiscale_labels": _run_writer_multiscale_labels(
                    writer_rt._cpp_writer,
                    format_eq._cpp_format,
                    cpp_root.parent / "labels.zarr",
                ),
                "write_labels": _run_writer_labels(
                    writer_rt._cpp_writer,
                    format_eq._cpp_format,
                    cpp_root.parent / "labels-only.zarr",
                ),
            },
        )
    finally:
        shutil.rmtree(py_root.parent, ignore_errors=True)
        shutil.rmtree(cpp_root.parent, ignore_errors=True)


def _bench_writer_multiscale(writer_module, format_module) -> float:
    root = _temp_dir("writer-multiscale") / "image.zarr"
    try:
        return _touch_outcome(
            _run_writer_multiscale(writer_module, format_module, root)
        )
    finally:
        shutil.rmtree(root.parent, ignore_errors=True)


def _bench_writer_multiscale_labels(writer_module, format_module) -> float:
    root = _temp_dir("writer-multiscale-labels") / "image.zarr"
    try:
        return _touch_outcome(
            _run_writer_multiscale_labels(writer_module, format_module, root)
        )
    finally:
        shutil.rmtree(root.parent, ignore_errors=True)


def _bench_writer_labels(writer_module, format_module) -> float:
    root = _temp_dir("writer-labels") / "image.zarr"
    try:
        return _touch_outcome(_run_writer_labels(writer_module, format_module, root))
    finally:
        shutil.rmtree(root.parent, ignore_errors=True)


PUBLIC_API_CASES = (
    core_cases._make_case(
        "cli",
        "create_wrapper",
        "Native CLI create command versus upstream cli.create semantics.",
        _verify_cli_create_wrapper,
        lambda: _bench_cli_create_wrapper(True),
        lambda: _bench_cli_create_wrapper(False),
    ),
    core_cases._make_case(
        "cli",
        "info_wrapper",
        "Native CLI info command versus upstream cli.info semantics.",
        _verify_cli_info_wrapper,
        lambda: _bench_cli_info_wrapper(True),
        lambda: _bench_cli_info_wrapper(False),
    ),
    core_cases._make_case(
        "cli",
        "download_wrapper",
        "Native CLI download command versus upstream cli.download semantics.",
        _verify_cli_download_wrapper,
        lambda: _bench_cli_download_wrapper(True),
        lambda: _bench_cli_download_wrapper(False),
    ),
    core_cases._make_case(
        "cli",
        "scale_wrapper",
        "Native CLI scale command versus upstream cli.scale semantics.",
        _verify_cli_scale_wrapper,
        lambda: _bench_cli_scale_wrapper(True),
        lambda: _bench_cli_scale_wrapper(False),
    ),
    core_cases.BenchmarkCase(
        group="conversions",
        name="int_to_rgba",
        description="Signed 32-bit integer to normalized RGBA conversion.",
        verify=_verify_conversions_int_to_rgba,
        python_timer=core_cases._make_timer(
            "conversions.int_to_rgba",
            _verify_conversions_int_to_rgba,
            _bench_conversions_int_to_rgba_python,
        ),
        cpp_timer=_native_bench_timer(
            case_id="conversions.int_to_rgba",
            verify=_verify_conversions_int_to_rgba,
            native_match="conversions.int_to_rgba",
        ),
    ),
    core_cases.BenchmarkCase(
        group="conversions",
        name="rgba_to_int",
        description="RGBA byte tuple to signed integer conversion.",
        verify=_verify_conversions_rgba_to_int,
        python_timer=core_cases._make_timer(
            "conversions.rgba_to_int",
            _verify_conversions_rgba_to_int,
            _bench_conversions_rgba_to_int_python,
        ),
        cpp_timer=_native_bench_timer(
            case_id="conversions.rgba_to_int",
            verify=_verify_conversions_rgba_to_int,
            native_match="conversions.rgba_to_int",
        ),
    ),
    core_cases.BenchmarkCase(
        group="csv",
        name="parse_csv_value",
        description="Typed CSV literal parsing across representative scalar cases.",
        verify=_verify_csv_parse,
        python_timer=core_cases._make_timer(
            "csv.parse_csv_value",
            _verify_csv_parse,
            _bench_csv_parse_python,
        ),
        cpp_timer=_native_bench_timer(
            case_id="csv.parse_csv_value",
            verify=_verify_csv_parse,
            native_match="csv.parse_csv_value",
        ),
    ),
    core_cases.BenchmarkCase(
        group="csv",
        name="dict_to_zarr",
        description=(
            "Metadata injection from pre-built property dicts into label properties."
        ),
        verify=_verify_csv_dict_to_zarr,
        python_timer=core_cases._make_timer(
            "csv.dict_to_zarr",
            _verify_csv_dict_to_zarr,
            _bench_csv_dict_to_zarr_python,
        ),
        cpp_timer=_native_bench_timer(
            case_id="csv.dict_to_zarr",
            verify=_verify_csv_dict_to_zarr,
            native_match="local.dict_to_zarr",
        ),
    ),
    core_cases._make_case(
        "csv",
        "csv_to_zarr",
        "CSV-driven metadata injection versus the native csv_to_labels runtime.",
        _verify_csv_csv_to_zarr,
        lambda: _bench_csv_csv_to_zarr(True),
        lambda: _bench_csv_csv_to_zarr(False),
    ),
    core_cases.BenchmarkCase(
        group="data",
        name="make_circle",
        description="Synthetic disk painting into preallocated arrays.",
        verify=_verify_data_make_circle,
        python_timer=core_cases._make_timer(
            "data.make_circle",
            _verify_data_make_circle,
            _bench_data_make_circle_python,
        ),
        cpp_timer=_native_bench_timer(
            case_id="data.make_circle",
            verify=_verify_data_make_circle,
            native_match="data.make_circle_batch",
        ),
    ),
    core_cases.BenchmarkCase(
        group="data",
        name="rgb_to_5d",
        description="RGB-like array normalization into 5D OME-Zarr layout.",
        verify=_verify_data_rgb_to_5d,
        python_timer=core_cases._make_timer(
            "data.rgb_to_5d",
            _verify_data_rgb_to_5d,
            _bench_data_rgb_to_5d_python,
        ),
        cpp_timer=_native_bench_timer(
            case_id="data.rgb_to_5d",
            verify=_verify_data_rgb_to_5d,
            native_match="data.rgb_to_5d_batch",
        ),
    ),
    core_cases._make_case(
        "format",
        "dispatch",
        "Format dispatch helpers and implementation iteration.",
        _verify_format_dispatch,
        lambda: _bench_format_dispatch(format_eq._py_format),
        lambda: _bench_format_dispatch(format_eq._cpp_format),
    ),
    core_cases._make_case(
        "format",
        "matches",
        "Concrete format property and metadata ownership checks.",
        _verify_format_matches,
        lambda: _bench_format_matches(format_eq._py_format),
        lambda: _bench_format_matches(format_eq._cpp_format),
    ),
    core_cases._make_case(
        "format",
        "v01_init_store",
        "FormatV01 local and remote store initialization behavior.",
        _verify_format_v01_init_store,
        lambda: _bench_format_v01_init_store(format_eq._py_format),
        lambda: _bench_format_v01_init_store(format_eq._cpp_format),
    ),
    core_cases._make_case(
        "format",
        "well_and_coord",
        "Concrete well-dict and coordinate-transformation helpers.",
        _verify_format_well_and_coord,
        lambda: _bench_format_well_and_coord(format_eq._py_format),
        lambda: _bench_format_well_and_coord(format_eq._cpp_format),
    ),
    core_cases._make_case(
        "io",
        "location_methods",
        "ZarrLocation method surface through parse_url on v2 and v3 fixtures.",
        _verify_io_location_methods,
        lambda: _bench_io_location_methods(io_eq._py_io),
        lambda: _bench_io_location_methods(io_eq._cpp_io),
    ),
    core_cases._make_case(
        "io",
        "create_load",
        "Explicit ZarrLocation.create and ZarrLocation.load behavior in write mode.",
        _verify_io_create_load,
        lambda: _bench_io_create_load(io_eq._py_io),
        lambda: _bench_io_create_load(io_eq._cpp_io),
    ),
    core_cases._make_case(
        "reader",
        "matches",
        "Reader spec predicate surface on synthetic image and plate trees.",
        _verify_reader_matches,
        lambda: _bench_reader_matches(reader_eq._py_reader),
        lambda: _bench_reader_matches(reader_eq._cpp_reader),
    ),
    core_cases._make_case(
        "reader",
        "node_ops",
        "Node add/load/first/write_metadata behavior on synthetic image trees.",
        _verify_reader_node_ops,
        lambda: _bench_reader_node_ops(reader_eq._py_reader),
        lambda: _bench_reader_node_ops(reader_eq._cpp_reader),
    ),
    core_cases._make_case(
        "reader",
        "image_surface",
        "Reader image traversal, multiscale array access, and Spec.lookup.",
        _verify_reader_image_surface,
        lambda: _bench_reader_image_surface(reader_eq._py_reader),
        lambda: _bench_reader_image_surface(reader_eq._cpp_reader),
    ),
    core_cases._make_case(
        "reader",
        "plate_surface",
        "Reader plate traversal and Plate tile-grid helpers on a fake HCS tree.",
        _verify_reader_plate_surface,
        lambda: _bench_reader_plate_surface(reader_eq._py_reader),
        lambda: _bench_reader_plate_surface(reader_eq._cpp_reader),
    ),
    core_cases._make_case(
        "scale",
        "scaler_methods",
        "Deprecated Scaler.methods enumeration parity.",
        _verify_scaler_methods,
        lambda: _bench_scaler_methods(scaler_eq._py_scale),
        lambda: _bench_scaler_methods(scaler_eq._cpp_scale),
    ),
    core_cases._make_case(
        "scale",
        "scaler_resize_image",
        "Deprecated Scaler.resize_image across NumPy and Dask inputs.",
        lambda: _verify_scaler_method(
            "resize_image",
            [
                np.arange(64, dtype=np.uint16).reshape(8, 8),
                np.arange(3 * 32 * 32, dtype=np.uint8).reshape(3, 32, 32),
                da.from_array(
                    np.arange(64, dtype=np.uint16).reshape(8, 8), chunks=(5, 5)
                ),
            ],
        ),
        lambda: _bench_scaler_method(
            scaler_eq._py_scale,
            "resize_image",
            [
                np.arange(64, dtype=np.uint16).reshape(8, 8),
                np.arange(3 * 32 * 32, dtype=np.uint8).reshape(3, 32, 32),
                da.from_array(
                    np.arange(64, dtype=np.uint16).reshape(8, 8), chunks=(5, 5)
                ),
            ],
        ),
        lambda: _bench_scaler_method(
            scaler_eq._cpp_scale,
            "resize_image",
            [
                np.arange(64, dtype=np.uint16).reshape(8, 8),
                np.arange(3 * 32 * 32, dtype=np.uint8).reshape(3, 32, 32),
                da.from_array(
                    np.arange(64, dtype=np.uint16).reshape(8, 8), chunks=(5, 5)
                ),
            ],
        ),
    ),
    core_cases._make_case(
        "scale",
        "scaler_gaussian",
        "Deprecated Scaler.gaussian across representative array shapes.",
        lambda: _verify_scaler_method(
            "gaussian",
            [
                np.arange(64, dtype=np.uint16).reshape(8, 8),
                np.arange(3 * 32 * 32, dtype=np.uint8).reshape(3, 32, 32),
            ],
        ),
        lambda: _bench_scaler_method(
            scaler_eq._py_scale,
            "gaussian",
            [
                np.arange(64, dtype=np.uint16).reshape(8, 8),
                np.arange(3 * 32 * 32, dtype=np.uint8).reshape(3, 32, 32),
            ],
        ),
        lambda: _bench_scaler_method(
            scaler_eq._cpp_scale,
            "gaussian",
            [
                np.arange(64, dtype=np.uint16).reshape(8, 8),
                np.arange(3 * 32 * 32, dtype=np.uint8).reshape(3, 32, 32),
            ],
        ),
    ),
    core_cases._make_case(
        "scale",
        "scaler_laplacian",
        "Deprecated Scaler.laplacian across representative array shapes.",
        lambda: _verify_scaler_method(
            "laplacian",
            [
                np.arange(64, dtype=np.uint16).reshape(8, 8),
                np.arange(3 * 32 * 32, dtype=np.uint8).reshape(3, 32, 32),
            ],
        ),
        lambda: _bench_scaler_method(
            scaler_eq._py_scale,
            "laplacian",
            [
                np.arange(64, dtype=np.uint16).reshape(8, 8),
                np.arange(3 * 32 * 32, dtype=np.uint8).reshape(3, 32, 32),
            ],
        ),
        lambda: _bench_scaler_method(
            scaler_eq._cpp_scale,
            "laplacian",
            [
                np.arange(64, dtype=np.uint16).reshape(8, 8),
                np.arange(3 * 32 * 32, dtype=np.uint8).reshape(3, 32, 32),
            ],
        ),
    ),
    core_cases._make_case(
        "scale",
        "scaler_zoom",
        "Deprecated Scaler.zoom on a representative 2D image.",
        lambda: _verify_scaler_method(
            "zoom",
            [np.arange(64, dtype=np.uint16).reshape(8, 8)],
        ),
        lambda: _bench_scaler_method(
            scaler_eq._py_scale,
            "zoom",
            [np.arange(64, dtype=np.uint16).reshape(8, 8)],
        ),
        lambda: _bench_scaler_method(
            scaler_eq._cpp_scale,
            "zoom",
            [np.arange(64, dtype=np.uint16).reshape(8, 8)],
        ),
    ),
    core_cases._make_case(
        "writer",
        "metadata_helpers",
        "add_metadata and get_metadata parity on a v0.5 group.",
        _verify_writer_metadata_helpers,
        lambda: _bench_writer_metadata_helpers(
            writer_rt._py_writer, format_eq._py_format
        ),
        lambda: _bench_writer_metadata_helpers(
            writer_rt._cpp_writer, format_eq._cpp_format
        ),
    ),
    core_cases._make_case(
        "writer",
        "group_helpers",
        "check_format and check_group_fmt parity on path and group inputs.",
        _verify_writer_group_helpers,
        lambda: _bench_writer_group_helpers(writer_rt._py_writer, format_eq._py_format),
        lambda: _bench_writer_group_helpers(
            writer_rt._cpp_writer, format_eq._cpp_format
        ),
    ),
    core_cases._make_case(
        "writer",
        "metadata_writers",
        "Metadata-only writer helpers for multiscales, plate, well, and labels.",
        _verify_writer_metadata_writers,
        lambda: _bench_writer_metadata_writers(
            writer_rt._py_writer, format_eq._py_format
        ),
        lambda: _bench_writer_metadata_writers(
            writer_rt._cpp_writer, format_eq._cpp_format
        ),
    ),
    core_cases._make_case(
        "writer",
        "write_multiscale",
        "Runtime write_multiscale parity on a small v0.5 pyramid.",
        _verify_writer_runtime_writers,
        lambda: _bench_writer_multiscale(writer_rt._py_writer, format_eq._py_format),
        lambda: _bench_writer_multiscale(writer_rt._cpp_writer, format_eq._cpp_format),
    ),
    core_cases._make_case(
        "writer",
        "write_multiscale_labels",
        "Runtime write_multiscale_labels parity on a small v0.5 label pyramid.",
        _verify_writer_runtime_writers,
        lambda: _bench_writer_multiscale_labels(
            writer_rt._py_writer, format_eq._py_format
        ),
        lambda: _bench_writer_multiscale_labels(
            writer_rt._cpp_writer, format_eq._cpp_format
        ),
    ),
    core_cases._make_case(
        "writer",
        "write_labels",
        "Runtime write_labels parity on a small v0.5 labels image.",
        _verify_writer_runtime_writers,
        lambda: _bench_writer_labels(writer_rt._py_writer, format_eq._py_format),
        lambda: _bench_writer_labels(writer_rt._cpp_writer, format_eq._cpp_format),
    ),
    core_cases.BenchmarkCase(
        group="utils",
        name="path_helpers",
        description="Path splitting and common-prefix stripping helpers.",
        verify=_verify_utils_path_helpers,
        python_timer=core_cases._make_timer(
            "utils.path_helpers",
            _verify_utils_path_helpers,
            _bench_utils_path_helpers_python,
        ),
        cpp_timer=_native_bench_timer(
            case_id="utils.path_helpers",
            verify=_verify_utils_path_helpers,
            native_match="utils.path_helpers",
        ),
    ),
    core_cases.BenchmarkCase(
        group="utils",
        name="find_multiscales",
        description="Multiscale path inference helper.",
        verify=_verify_utils_find_multiscales,
        python_timer=core_cases._make_timer(
            "utils.find_multiscales",
            _verify_utils_find_multiscales,
            _bench_utils_find_multiscales_python,
        ),
        cpp_timer=_native_bench_timer(
            case_id="utils.find_multiscales",
            verify=_verify_utils_find_multiscales,
            native_match="local.find_multiscales",
        ),
    ),
    core_cases._make_case(
        "utils",
        "finder",
        "Directory discovery and CSV planning for BioFile Finder output.",
        _verify_utils_finder_and_view,
        lambda: _bench_utils_finder(True),
        lambda: _bench_utils_finder(False),
    ),
    core_cases._make_case(
        "utils",
        "view",
        "Validator-view serving plan on a local OME-Zarr path.",
        _verify_utils_finder_and_view,
        lambda: _bench_utils_view(True),
        lambda: _bench_utils_view(False),
    ),
    core_cases._make_case(
        "utils",
        "download",
        "Local OME-Zarr download roundtrip on a minimal v2 image.",
        _verify_utils_download,
        lambda: _bench_utils_download(True),
        lambda: _bench_utils_download(False),
    ),
)


def iter_public_api_cases() -> tuple[core_cases.BenchmarkCase, ...]:
    return PUBLIC_API_CASES


__all__ = ["PUBLIC_API_CASES", "iter_public_api_cases"]
