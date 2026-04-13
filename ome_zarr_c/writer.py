"""C++-backed writer helpers and writer surface from ome-zarr-py."""

from __future__ import annotations

import importlib
import logging
import warnings
from pathlib import Path
from typing import Any

import dask
import dask.array as da
import numpy as np
import zarr
from dask.graph_manipulation import bind

from .format import CurrentFormat, Format, FormatV01, FormatV02, FormatV03, FormatV04
from .scale import Methods, Scaler

_core = importlib.import_module("ome_zarr_c._core")

LOGGER = logging.getLogger("ome_zarr.writer")

ListOfArrayLike = list[da.Array] | list[np.ndarray]
type ArrayLike = da.Array | np.ndarray
AxesType = str | list[str] | list[dict[str, str]] | None

SPATIAL_DIMS = ("x", "y", "z")
_DEFAULT_FORMAT = CurrentFormat()
_DEFAULT_LABEL_SCALER = object()


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


def check_group_fmt(
    group: zarr.Group | str,
    fmt: Format | None = None,
    mode: str = "a",
) -> tuple[zarr.Group, Format]:
    """Create/check a zarr group against the requested OME-Zarr format."""

    if isinstance(group, str):
        if fmt is None:
            fmt = CurrentFormat()
        group = zarr.open_group(group, mode=mode, zarr_format=fmt.zarr_format)
    else:
        fmt = check_format(group, fmt)
    return group, fmt


def check_format(
    group: zarr.Group,
    fmt: Format | None = None,
) -> Format:
    """Check if the format is valid for the given group."""

    zarr_format = group.info._zarr_format
    if fmt is not None:
        if fmt.zarr_format != zarr_format:
            raise ValueError(
                "Group is zarr_format: "
                f"{zarr_format} but OME-Zarr {fmt.version} "
                f"is {fmt.zarr_format}"
            )
    elif zarr_format == 2:
        fmt = FormatV04()
    elif zarr_format == 3:
        fmt = CurrentFormat()
    assert fmt is not None
    return fmt


def write_multiscale(
    pyramid: ListOfArrayLike,
    group: zarr.Group | str,
    fmt: Format | None = None,
    axes: AxesType = None,
    coordinate_transformations: list[list[dict[str, Any]]] | None = None,
    storage_options: dict[str, Any] | list[dict[str, Any]] | None = None,
    name: str | None = None,
    compute: bool | None = True,
    **metadata: str | dict[str, Any] | list[dict[str, Any]],
) -> list:
    group, fmt = check_group_fmt(group, fmt)
    dims = len(pyramid[0].shape)
    axes = _get_valid_axes(dims, axes, fmt)

    if type(fmt) in (FormatV01, FormatV02, FormatV03):
        raise DeprecationWarning(
            "Writing ome-zarr "
            f"v{fmt.version} is deprecated and has been removed in version 0.15.0."
        )

    pyramid = [
        da.from_array(level) if not isinstance(level, da.Array) else level
        for level in pyramid
    ]
    return _write_pyramid_to_zarr(
        pyramid,
        group,
        fmt=fmt,
        axes=axes,
        coordinate_transformations=coordinate_transformations,
        storage_options=storage_options,
        name=name,
        compute=compute,
        **metadata,
    )


def write_multiscales_metadata(
    group: zarr.Group | str,
    datasets: list[dict],
    fmt: Format | None = None,
    axes: AxesType = None,
    name: str | None = None,
    **metadata: str | dict[str, Any] | list[dict[str, Any]],
) -> None:
    group, fmt = check_group_fmt(group, fmt)
    ndim = -1
    if axes is not None:
        if fmt.version in ("0.1", "0.2"):
            LOGGER.info("axes ignored for version 0.1 or 0.2")
            axes = None
        else:
            axes = _get_valid_axes(axes=axes, fmt=fmt)
            if axes is not None:
                ndim = len(axes)

    if (
        isinstance(metadata, dict)
        and metadata.get("metadata")
        and isinstance(metadata["metadata"], dict)
        and "omero" in metadata["metadata"]
    ):
        omero_metadata = metadata["metadata"].pop("omero")
        if omero_metadata is None:
            raise KeyError("If `'omero'` is present, value cannot be `None`.")
        for channel in omero_metadata["channels"]:
            if "color" in channel and (
                not isinstance(channel["color"], str) or len(channel["color"]) != 6
            ):
                raise TypeError("`'color'` must be a hex code string.")
            if "window" in channel:
                if not isinstance(channel["window"], dict):
                    raise TypeError("`'window'` must be a dict.")
                for key in ["min", "max", "start", "end"]:
                    if key not in channel["window"]:
                        raise KeyError(f"`'{key}'` not found in `'window'`.")
                    if not isinstance(channel["window"][key], (int, float)):
                        raise TypeError(f"`'{key}'` must be an int or float.")

        add_metadata(group, {"omero": omero_metadata})

    multiscales = [
        dict(datasets=_validate_datasets(datasets, ndim, fmt), name=name or group.name)
    ]
    if len(metadata.get("metadata", {})) > 0:
        multiscales[0]["metadata"] = metadata["metadata"]
    if axes is not None:
        multiscales[0]["axes"] = axes

    if fmt.version in ("0.1", "0.2", "0.3", "0.4"):
        multiscales[0]["version"] = fmt.version
    else:
        add_metadata(group, {"version": fmt.version})

    add_metadata(group, {"multiscales": multiscales})


def write_plate_metadata(
    group: zarr.Group | str,
    rows: list[str],
    columns: list[str],
    wells: list[str | dict],
    fmt: Format | None = None,
    acquisitions: list[dict] | None = None,
    field_count: int | None = None,
    name: str | None = None,
) -> None:
    group, fmt = check_group_fmt(group, fmt)
    plate: dict[str, str | int | list[dict]] = {
        "columns": _validate_plate_rows_columns(columns),
        "rows": _validate_plate_rows_columns(rows),
        "wells": _validate_plate_wells(wells, rows, columns, fmt=fmt),
    }
    if name is not None:
        plate["name"] = name
    if field_count is not None:
        plate["field_count"] = field_count
    if acquisitions is not None:
        plate["acquisitions"] = _validate_plate_acquisitions(acquisitions)

    if fmt.version in ("0.1", "0.2", "0.3", "0.4"):
        plate["version"] = fmt.version
        group.attrs["plate"] = plate
    else:
        if fmt.version == "0.5":
            plate["version"] = fmt.version
        group.attrs["ome"] = {"version": fmt.version, "plate": plate}


def write_well_metadata(
    group: zarr.Group | str,
    images: list[str | dict],
    fmt: Format | None = None,
) -> None:
    group, fmt = check_group_fmt(group, fmt)
    well: dict[str, Any] = {
        "images": _validate_well_images(images),
    }

    if fmt.version in ("0.1", "0.2", "0.3", "0.4"):
        well["version"] = fmt.version
        group.attrs["well"] = well
    else:
        group.attrs["ome"] = {"version": fmt.version, "well": well}


def _write_pyramid_to_zarr(
    pyramid: list[da.Array],
    group: zarr.Group,
    fmt: Format,
    axes: AxesType = None,
    coordinate_transformations: list[list[dict[str, Any]]] | None = None,
    storage_options: dict[str, Any] | list[dict[str, Any]] | None = None,
    name: str | None = None,
    compute: bool | None = True,
    **metadata: str | dict[str, Any] | list[dict[str, Any]],
) -> list:
    group, fmt = check_group_fmt(group, fmt)

    zarr_array_kwargs: dict[str, Any] = {}
    zarr_format = fmt.zarr_format
    options = _resolve_storage_options(storage_options, 0)
    zarr_array_kwargs["zarr_format"] = zarr_format

    if zarr_format == 2:
        zarr_array_kwargs["chunk_key_encoding"] = {"name": "v2", "separator": "/"}
    else:
        if axes is not None:
            zarr_array_kwargs["dimension_names"] = [
                a["name"] for a in axes if isinstance(a, dict)
            ]
    if "compressor" in options:
        zarr_array_kwargs["compressors"] = [options.pop("compressor")]

    shapes = []
    datasets: list[dict] = []
    delayed = []

    for idx, level in enumerate(pyramid):
        options = _resolve_storage_options(storage_options, idx)

        chunks_opt = None
        if isinstance(storage_options, list) and isinstance(storage_options[idx], dict):
            if "chunks" in storage_options[idx]:
                chunks_opt = options.pop("chunks", None)
        elif isinstance(storage_options, dict) and "chunks" in storage_options:
            chunks_opt = options.pop("chunks", None)

        if chunks_opt is not None:
            chunks_opt = _retuple(chunks_opt, level.shape)
            zarr_array_kwargs["chunks"] = chunks_opt
            level_image = da.array(level).rechunk(chunks=chunks_opt)
        else:
            level_image = level

        shapes.append(level_image.shape)

        LOGGER.debug(
            "write dask.array to_zarr shape: %s, dtype: %s",
            level_image.shape,
            level_image.dtype,
        )

        delayed.append(
            da.to_zarr(
                arr=level_image,
                url=group.store,
                component=str(Path(group.path, f"s{idx}")),
                compute=False,
                **zarr_array_kwargs,
            )
        )
        datasets.append({"path": f"s{idx}"})

    if compute:
        da.compute(*delayed)
        delayed = []

    if coordinate_transformations is None:
        coordinate_transformations = fmt.generate_coordinate_transformations(shapes)

    fmt.validate_coordinate_transformations(
        len(pyramid[0].shape), len(datasets), coordinate_transformations
    )
    if coordinate_transformations is not None:
        for dataset, transform in zip(
            datasets, coordinate_transformations, strict=False
        ):
            dataset["coordinateTransformations"] = transform
    if not compute:
        write_multiscales_metadata_delayed = dask.delayed(write_multiscales_metadata)
        return delayed + [
            bind(write_multiscales_metadata_delayed, delayed)(
                group, datasets, fmt, axes, name, **metadata
            )
        ]

    write_multiscales_metadata(group, datasets, fmt, axes, name, **metadata)
    return delayed


def write_label_metadata(
    group: zarr.Group | str,
    name: str,
    colors: list[dict[str, Any]] | None = None,
    properties: list[dict[str, Any]] | None = None,
    fmt: Format | None = None,
    **metadata: list[dict[str, Any]] | dict[str, Any] | str,
) -> None:
    group, fmt = check_group_fmt(group, fmt)
    label_group = group[name]
    image_label_metadata = {**metadata}
    if colors is not None:
        image_label_metadata["colors"] = colors
    if properties is not None:
        image_label_metadata["properties"] = properties
    image_label_metadata["version"] = fmt.version

    label_list = get_metadata(group).get("labels", [])
    label_list.append(name)

    add_metadata(group, {"labels": label_list}, fmt=fmt)
    add_metadata(label_group, {"image-label": image_label_metadata}, fmt=fmt)


def get_metadata(group: zarr.Group | str) -> dict:
    if isinstance(group, str):
        group = zarr.open_group(group, mode="r")
    attrs = group.attrs
    if group.info._zarr_format == 3:
        attrs = attrs.get("ome", {})
    else:
        attrs = dict(attrs)
    return attrs


def add_metadata(
    group: zarr.Group | str, metadata: dict[str, Any], fmt: Format | None = None
) -> None:
    group, fmt = check_group_fmt(group, fmt)
    attrs = group.attrs
    if fmt.version not in ("0.1", "0.2", "0.3", "0.4"):
        attrs = attrs.get("ome", {})

    for key, value in metadata.items():
        if isinstance(value, dict) and isinstance(attrs.get(key), dict):
            attrs[key].update(value)
        else:
            attrs[key] = value

    if fmt.version in ("0.1", "0.2", "0.3", "0.4"):
        for key, value in attrs.items():
            group.attrs[key] = value
    else:
        group.attrs["ome"] = attrs


def write_multiscale_labels(
    pyramid: list,
    group: zarr.Group | str,
    name: str,
    fmt: Format | None = None,
    axes: AxesType = None,
    coordinate_transformations: list[list[dict[str, Any]]] | None = None,
    storage_options: dict[str, Any] | list[dict[str, Any]] | None = None,
    label_metadata: dict[str, Any] | None = None,
    compute: bool | None = True,
    **metadata: dict[str, Any],
) -> list:
    group, fmt = check_group_fmt(group, fmt)
    sub_group = group.require_group(f"labels/{name}")

    pyramid = [
        da.from_array(level) if not isinstance(level, da.Array) else level
        for level in pyramid
    ]

    dask_delayed_jobs = _write_pyramid_to_zarr(
        pyramid,
        sub_group,
        fmt=fmt,
        axes=axes,
        coordinate_transformations=coordinate_transformations,
        storage_options=storage_options,
        name=name,
        compute=compute,
        **metadata,
    )
    write_label_metadata(
        group["labels"],
        name,
        fmt=fmt,
        **({} if label_metadata is None else label_metadata),
    )

    return dask_delayed_jobs


def write_labels(
    labels: np.ndarray | da.Array,
    group: zarr.Group | str,
    name: str,
    scaler: Scaler | None | object = _DEFAULT_LABEL_SCALER,
    scale_factors: list[int] | tuple[int, ...] | list[dict[str, int]] = (2, 4, 8, 16),
    method: Methods = Methods.NEAREST,
    fmt: Format | None = None,
    axes: AxesType = None,
    coordinate_transformations: list[list[dict[str, Any]]] | None = None,
    storage_options: dict[str, Any] | list[dict[str, Any]] | None = None,
    label_metadata: dict[str, Any] | None = None,
    compute: bool | None = True,
    **metadata: dict[str, Any],
) -> list:
    from .scale import _build_pyramid

    group, fmt = check_group_fmt(group, fmt)
    sub_group = group.require_group(f"labels/{name}")

    if scaler is _DEFAULT_LABEL_SCALER:
        scaler = Scaler(order=0)

    if type(fmt) in (FormatV01, FormatV02, FormatV03):
        raise DeprecationWarning(
            "Writing ome-zarr "
            f"v{fmt.version} is deprecated and has been removed in version 0.15.0."
        )

    axes = _get_valid_axes(len(labels.shape), axes, fmt)
    dims = _extract_dims_from_axes(axes)

    if scaler is not None:
        msg = """
        The 'scaler' argument is deprecated and will be removed in version 0.13.0.
        Please use the 'scale_factors' argument instead.
        """
        scale_factors = [
            {d: 2**i if d in SPATIAL_DIMS else 1 for d in dims}
            for i in range(1, scaler.max_layer + 1)
        ]
        warnings.warn(msg, DeprecationWarning, stacklevel=2)

    if method is None:
        method = Methods.NEAREST

    if not isinstance(labels, da.Array):
        labels = da.from_array(labels)

    pyramid = _build_pyramid(
        labels,
        scale_factors,
        dims=dims,
        method=method,
    )

    dask_delayed_jobs = _write_pyramid_to_zarr(
        pyramid,
        sub_group,
        fmt=fmt,
        axes=axes,
        coordinate_transformations=coordinate_transformations,
        storage_options=storage_options,
        name=name,
        compute=compute,
        **metadata,
    )

    write_label_metadata(
        group=group["labels"],
        name=name,
        fmt=fmt,
        **({} if label_metadata is None else label_metadata),
    )

    return dask_delayed_jobs


__all__ = [
    "ArrayLike",
    "AxesType",
    "ListOfArrayLike",
    "SPATIAL_DIMS",
    "_blosc_compressor",
    "_extract_dims_from_axes",
    "_get_valid_axes",
    "_resolve_storage_options",
    "_retuple",
    "_validate_datasets",
    "_validate_plate_acquisitions",
    "_validate_plate_rows_columns",
    "_validate_plate_wells",
    "_validate_well_images",
    "_write_pyramid_to_zarr",
    "add_metadata",
    "check_format",
    "check_group_fmt",
    "get_metadata",
    "write_label_metadata",
    "write_labels",
    "write_multiscale",
    "write_multiscale_labels",
    "write_multiscales_metadata",
    "write_plate_metadata",
    "write_well_metadata",
]
