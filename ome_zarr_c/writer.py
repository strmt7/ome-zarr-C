"""C++-backed port of selected writer helpers from ome-zarr-py."""

from __future__ import annotations

import importlib
from typing import Any

from .format import CurrentFormat, Format

_core = importlib.import_module("ome_zarr_c._core")

AxesType = str | list[str] | list[dict[str, str]] | None
_DEFAULT_FORMAT = CurrentFormat()


def _get_valid_axes(
    ndim: int | None = None,
    axes: AxesType = None,
    fmt: Format = _DEFAULT_FORMAT,
) -> list[str] | list[dict[str, str]] | None:
    return _core._get_valid_axes(ndim, axes, fmt)


def _extract_dims_from_axes(
    axes: list[str] | list[dict[str, str]] | None,
):
    return _core._extract_dims_from_axes(axes)


def _retuple(chunks: tuple[Any, ...] | int, shape: tuple[Any, ...]) -> tuple[Any, ...]:
    return _core._retuple(chunks, shape)


def _validate_well_images(
    images: list[str | dict],
    fmt: Format = _DEFAULT_FORMAT,
) -> list[dict]:
    return _core._validate_well_images(images, fmt)


def _validate_plate_acquisitions(
    acquisitions: list[dict],
    fmt: Format = _DEFAULT_FORMAT,
) -> list[dict]:
    return _core._validate_plate_acquisitions(acquisitions, fmt)


def _validate_plate_rows_columns(
    rows_or_columns: list[str],
    fmt: Format = _DEFAULT_FORMAT,
) -> list[dict]:
    return _core._validate_plate_rows_columns(rows_or_columns, fmt)


def _validate_datasets(
    datasets: list[dict],
    dims: int,
    fmt: Format = _DEFAULT_FORMAT,
) -> list[dict]:
    return _core._validate_datasets(datasets, dims, fmt)


def _validate_plate_wells(
    wells: list[str | dict],
    rows: list[str],
    columns: list[str],
    fmt: Format = _DEFAULT_FORMAT,
) -> list[dict]:
    return _core._validate_plate_wells(wells, rows, columns, fmt)


def _blosc_compressor():
    return _core._blosc_compressor()


def _resolve_storage_options(
    storage_options: dict[str, Any] | list[dict[str, Any]] | None,
    path: int,
) -> dict[str, Any]:
    return _core._resolve_storage_options(storage_options, path)


__all__ = [
    "_blosc_compressor",
    "_extract_dims_from_axes",
    "_get_valid_axes",
    "_retuple",
    "_resolve_storage_options",
    "_validate_datasets",
    "_validate_plate_acquisitions",
    "_validate_plate_rows_columns",
    "_validate_plate_wells",
    "_validate_well_images",
]
