"""Entrypoint for the `ome_zarr_c` command-line tool."""

from __future__ import annotations

import argparse
import logging
import sys

from . import _core
from .csv import csv_to_zarr
from .data import create_zarr
from .format import CurrentFormat, Format, format_from_version
from .utils import download as zarr_download
from .utils import finder as bff_finder
from .utils import info as zarr_info
from .utils import view as zarr_view


def _apply_argument(parser: argparse.ArgumentParser, spec: dict) -> None:
    flags = [str(flag) for flag in spec["flags"]]
    kwargs: dict[str, object] = {}
    help_text = str(spec["help"])
    if help_text:
        kwargs["help"] = help_text
    action = str(spec["action"])
    if action:
        kwargs["action"] = action
    type_name = str(spec["type_name"])
    if type_name == "int":
        kwargs["type"] = int
    elif type_name == "str":
        kwargs["type"] = str
    if bool(spec["has_default"]):
        default_value = str(spec["default_value"])
        if action == "count" or type_name == "int":
            kwargs["default"] = int(default_value)
        else:
            kwargs["default"] = default_value
    choices = [str(choice) for choice in spec["choices"]]
    if choices:
        kwargs["choices"] = tuple(choices)
    parser.add_argument(*flags, **kwargs)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    spec = dict(_core.cli_parser_spec())
    for arg_spec in spec["global_arguments"]:
        _apply_argument(parser, dict(arg_spec))
    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    for command in spec["commands"]:
        command_spec = dict(command)
        subparser = subparsers.add_parser(str(command_spec["name"]))
        for arg_spec in command_spec["arguments"]:
            _apply_argument(subparser, dict(arg_spec))
        subparser.set_defaults(func=globals()[str(command_spec["handler_name"])])
    return parser


def config_logging(loglevel: int, args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=int(_core.cli_resolved_log_level(loglevel, args.verbose, args.quiet))
    )
    logging.getLogger("s3fs").setLevel(logging.DEBUG)


def info(args: argparse.Namespace) -> None:
    config_logging(logging.WARNING, args)
    list(zarr_info(args.path, stats=args.stats))


def view(args: argparse.Namespace) -> None:
    config_logging(logging.WARNING, args)
    zarr_view(args.path, args.port, force=args.force)


def finder(args: argparse.Namespace) -> None:
    config_logging(logging.WARNING, args)
    bff_finder(args.path, args.port)


def download(args: argparse.Namespace) -> None:
    config_logging(logging.WARNING, args)
    zarr_download(args.path, args.output)


def create(args: argparse.Namespace) -> None:
    from .data import astronaut, coins

    config_logging(logging.WARNING, args)
    plan = dict(_core.cli_create_plan(args.method))
    method_map = {
        "coins": coins,
        "astronaut": astronaut,
    }
    fmt: Format = CurrentFormat()
    if args.format:
        fmt = format_from_version(args.format)

    create_zarr(
        args.path,
        method=method_map[str(plan["method_name"])],
        label_name=str(plan["label_name"]),
        fmt=fmt,
    )


def scale(args: argparse.Namespace) -> None:
    import dask.array as da
    import zarr

    from .writer import write_image

    base = zarr.open_array(args.input_array, mode="r")
    scale_factors = _core.cli_scale_factors(args.downscale, args.max_layer)

    data = da.from_zarr(args.input_array)

    write_image(
        data,
        args.output_directory,
        axes=str(args.axes),
        method=args.method,
        scale_factors=tuple(int(value) for value in scale_factors),
    )

    grp = zarr.open_group(args.output_directory, mode="a")

    if args.copy_metadata:
        print(f"copying attribute keys: {list(base.attrs.keys())}")
        grp.attrs.update(base.attrs)


def csv_to_labels(args: argparse.Namespace) -> None:
    print("csv_to_labels", args.csv_path, args.zarr_path)
    csv_to_zarr(args.csv_path, args.csv_id, args.csv_keys, args.zarr_path, args.zarr_id)


def main(args: list[str] | None = None) -> None:
    parser = _build_parser()

    if args is None:
        ns = parser.parse_args(sys.argv[1:])
    else:
        ns = parser.parse_args(args)

    try:
        ns.func(ns)
    except AssertionError as error:
        logging.getLogger("ome_zarr.cli").error(error)
        sys.exit(2)


__all__ = [
    "config_logging",
    "create",
    "csv_to_labels",
    "download",
    "finder",
    "info",
    "main",
    "scale",
    "view",
]
