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
from numcodecs import Blosc

from .format import (
    CurrentFormat,
    Format,
    FormatV01,
    FormatV02,
    FormatV03,
    format_from_version,
)
from .scale import Methods, Scaler

_core = importlib.import_module("ome_zarr_c._core")

LOGGER = logging.getLogger("ome_zarr.writer")

ListOfArrayLike = list[da.Array] | list[np.ndarray]
type ArrayLike = da.Array | np.ndarray
AxesType = str | list[str] | list[dict[str, str]] | None

SPATIAL_DIMS = ("x", "y", "z")
_DEFAULT_FORMAT = CurrentFormat()
_DEFAULT_LABEL_SCALER = object()
_KNOWN_AXES = {"x": "space", "y": "space", "z": "space", "c": "channel", "t": "time"}


def _axes_to_dicts(axes: list[str] | list[dict[str, str]]) -> list[dict[str, str]]:
    axes_dicts: list[dict[str, str]] = []
    for axis in axes:
        if isinstance(axis, str):
            axis_dict = {"name": axis}
            if axis in _KNOWN_AXES:
                axis_dict["type"] = _KNOWN_AXES[axis]
            axes_dicts.append(axis_dict)
        else:
            axes_dicts.append(axis)
    return axes_dicts


def _axes_names(axes: list[dict[str, str]]) -> list[str]:
    names: list[str] = []
    for axis in axes:
        if "name" not in axis:
            raise ValueError(f"Axis Dict {axis} has no 'name'")
        names.append(str(axis["name"]))
    return names


def _validate_axes_03(axes: list[dict[str, str]]) -> None:
    val_axes = tuple(_axes_names(axes))
    if len(val_axes) == 2:
        if val_axes != ("y", "x"):
            raise ValueError(f"2D data must have axes ('y', 'x') {val_axes}")
        return
    if len(val_axes) == 3:
        if val_axes not in [("z", "y", "x"), ("c", "y", "x"), ("t", "y", "x")]:
            raise ValueError(
                "3D data must have axes ('z', 'y', 'x') or ('c', 'y', 'x')"
                f" or ('t', 'y', 'x'), not {val_axes}"
            )
        return
    if len(val_axes) == 4:
        if val_axes not in [
            ("t", "z", "y", "x"),
            ("c", "z", "y", "x"),
            ("t", "c", "y", "x"),
        ]:
            raise ValueError("4D data must have axes tzyx or czyx or tcyx")
        return
    if val_axes != ("t", "c", "z", "y", "x"):
        raise ValueError("5D data must have axes ('t', 'c', 'z', 'y', 'x')")


def _validate_axes_types(axes: list[dict[str, str]]) -> None:
    axes_types = [axis.get("type") for axis in axes]
    known_types = list(_KNOWN_AXES.values())
    unknown_types = [atype for atype in axes_types if atype not in known_types]
    if len(unknown_types) > 1:
        raise ValueError(
            f"Too many unknown axes types. 1 allowed, found: {unknown_types}"
        )

    def _last_index(item: str, item_list: list[Any]) -> int:
        return max(loc for loc, val in enumerate(item_list) if val == item)

    if "time" in axes_types and _last_index("time", axes_types) > 0:
        raise ValueError("'time' axis must be first dimension only")

    if axes_types.count("channel") > 1:
        raise ValueError("Only 1 axis can be type 'channel'")

    if "channel" in axes_types and _last_index(
        "channel", axes_types
    ) > axes_types.index("space"):
        raise ValueError("'space' axes must come after 'channel'")


def _get_valid_axes(
    ndim: int | None = None,
    axes: AxesType = None,
    fmt: Format = _DEFAULT_FORMAT,
) -> list[str] | list[dict[str, str]] | None:
    if fmt.version in ("0.1", "0.2"):
        if axes is not None:
            LOGGER.info("axes ignored for version 0.1 or 0.2")
        return None

    if axes is None:
        if ndim == 2:
            axes = ["y", "x"]
            LOGGER.info("Auto using axes %s for 2D data", axes)
        elif ndim == 5:
            axes = ["t", "c", "z", "y", "x"]
            LOGGER.info("Auto using axes %s for 5D data", axes)
        else:
            raise ValueError(
                "axes must be provided. Can't be guessed for 3D or 4D data"
            )

    if isinstance(axes, str):
        axes = list(axes)

    if ndim is not None and len(axes) != ndim:
        raise ValueError(
            f"axes length ({len(axes)}) must match number of dimensions ({ndim})"
        )

    axes_dicts = _axes_to_dicts(axes)
    if fmt.version == "0.3":
        _validate_axes_03(axes_dicts)
        return _axes_names(axes_dicts)

    _validate_axes_types(axes_dicts)
    return axes_dicts


def _extract_dims_from_axes(
    axes: list[str] | list[dict[str, str]] | None,
):
    if axes is None:
        return ("t", "c", "z", "y", "x")

    if all(isinstance(s, str) for s in axes):
        return tuple(str(s) for s in axes)

    if all(isinstance(s, dict) and "name" in s for s in axes):
        names: list[str] = []
        for s in axes:
            if not isinstance(s, dict) or "name" not in s:
                raise TypeError("`axes` must be a list of dicts containing 'name'")
            names.append(str(s["name"]))
        return tuple(names)

    raise TypeError(
        "`axes` must be a list of strings or a list of dicts containing 'name'"
    )


def _retuple(chunks: tuple[Any, ...] | int, shape: tuple[Any, ...]) -> tuple[Any, ...]:
    if isinstance(chunks, int):
        return tuple([chunks] * len(shape))

    dims_to_add = len(shape) - len(chunks)
    return (*shape[:dims_to_add], *chunks)


def _validate_well_images(
    images: list[str | dict],
    fmt: Format = _DEFAULT_FORMAT,
) -> list[dict]:
    del fmt
    valid_keys = ["acquisition", "path"]
    validated_images: list[dict] = []
    for image in images:
        if isinstance(image, str):
            validated_images.append({"path": str(image)})
        elif isinstance(image, dict):
            if any(element not in valid_keys for element in image):
                LOGGER.debug("%s contains unspecified keys", image)
            if "path" not in image:
                raise ValueError(f"{image} must contain a path key")
            if not isinstance(image["path"], str):
                raise ValueError(f"{image} path must be of string type")
            if "acquisition" in image and not isinstance(image["acquisition"], int):
                raise ValueError(f"{image} acquisition must be of int type")
            validated_images.append(image)
        else:
            raise ValueError(f"Unrecognized type for {image}")
    return validated_images


def _validate_plate_acquisitions(
    acquisitions: list[dict],
    fmt: Format = _DEFAULT_FORMAT,
) -> list[dict]:
    del fmt
    valid_keys = [
        "id",
        "name",
        "maximumfieldcount",
        "description",
        "starttime",
        "endtime",
    ]
    for acquisition in acquisitions:
        if not isinstance(acquisition, dict):
            raise ValueError(f"{acquisition} must be a dictionary")
        if any(element not in valid_keys for element in acquisition):
            LOGGER.debug("%s contains unspecified keys", acquisition)
        if "id" not in acquisition:
            raise ValueError(f"{acquisition} must contain an id key")
        if not isinstance(acquisition["id"], int):
            raise ValueError(f"{acquisition} id must be of int type")
    return acquisitions


def _validate_plate_rows_columns(
    rows_or_columns: list[str],
    fmt: Format = _DEFAULT_FORMAT,
) -> list[dict]:
    del fmt
    if len(set(rows_or_columns)) != len(rows_or_columns):
        raise ValueError(f"{rows_or_columns} must contain unique elements")
    validated_list: list[dict] = []
    for element in rows_or_columns:
        if not element.isalnum():
            raise ValueError(f"{element} must contain alphanumeric characters")
        validated_list.append({"name": str(element)})
    return validated_list


def _validate_datasets(
    datasets: list[dict],
    dims: int,
    fmt: Format = _DEFAULT_FORMAT,
) -> list[dict]:
    if datasets is None or len(datasets) == 0:
        raise ValueError("Empty datasets list")

    transformations = []
    for dataset in datasets:
        if isinstance(dataset, dict):
            if not dataset.get("path"):
                raise ValueError("no 'path' in dataset")
            transformation = dataset.get("coordinateTransformations")
            if transformation is not None:
                transformations.append(transformation)
        else:
            raise ValueError(f"Unrecognized type for {dataset}")

    fmt.validate_coordinate_transformations(dims, len(datasets), transformations)
    return datasets


def _validate_plate_wells(
    wells: list[str | dict],
    rows: list[str],
    columns: list[str],
    fmt: Format = _DEFAULT_FORMAT,
) -> list[dict]:
    validated_wells: list[dict] = []
    if wells is None or len(wells) == 0:
        raise ValueError("Empty wells list")
    for well in wells:
        if isinstance(well, str):
            well_dict = fmt.generate_well_dict(well, rows, columns)
            fmt.validate_well_dict(well_dict, rows, columns)
            validated_wells.append(well_dict)
        elif isinstance(well, dict):
            fmt.validate_well_dict(well, rows, columns)
            validated_wells.append(well)
        else:
            raise ValueError(f"Unrecognized type for {well}")
    return validated_wells


def _blosc_compressor():
    return Blosc(cname="zstd", clevel=5, shuffle=Blosc.SHUFFLE)


def _resolve_storage_options(
    storage_options: dict[str, Any] | list[dict[str, Any]] | None,
    path: int,
) -> dict[str, Any]:
    options: dict[str, Any] = {}
    if storage_options:
        options = (
            storage_options.copy()
            if not isinstance(storage_options, list)
            else storage_options[path]
        )
    return options


def check_group_fmt(
    group: zarr.Group | str,
    fmt: Format | None = None,
    mode: str = "a",
) -> tuple[zarr.Group, Format]:
    """Create/check a zarr group against the requested OME-Zarr format."""
    checked_group, version = _core.writer_check_group_fmt(
        group,
        None if fmt is None else fmt.version,
        mode,
    )
    return checked_group, format_from_version(str(version))


def check_format(
    group: zarr.Group,
    fmt: Format | None = None,
) -> Format:
    """Check if the format is valid for the given group."""
    return format_from_version(
        str(_core.writer_check_format(group, None if fmt is None else fmt.version))
    )


def write_image(
    image: ArrayLike,
    group: zarr.Group | str,
    scale_factors: list[int] | tuple[int, ...] | list[dict[str, int]] = (2, 4, 8, 16),
    method: Methods | str | None = Methods.RESIZE,
    scaler: Scaler | None = None,
    fmt: Format | None = None,
    axes: AxesType = None,
    coordinate_transformations: list[list[dict[str, Any]]] | None = None,
    storage_options: dict[str, Any] | list[dict[str, Any]] | None = None,
    compute: bool | None = True,
    **metadata: str | dict[str, Any] | list[dict[str, Any]],
) -> list:
    from .scale import _build_pyramid

    if method is None:
        method = Methods.RESIZE

    group, fmt = check_group_fmt(group, fmt)

    if not isinstance(image, da.Array):
        image = da.from_array(image)

    if type(fmt) in (FormatV01, FormatV02, FormatV03):
        raise DeprecationWarning(
            "Writing ome-zarr "
            f"v{fmt.version} is deprecated and has been removed in version 0.15.0."
        )

    axes = _get_valid_axes(len(image.shape), axes, fmt)
    dims = _extract_dims_from_axes(axes)

    runtime_plan = dict(
        _core.writer_image_plan(
            dims,
            scaler is not None,
            0 if scaler is None else int(scaler.max_layer),
            "" if scaler is None else str(scaler.method),
            method,
        )
    )

    if bool(runtime_plan["warn_scaler_deprecated"]):
        msg = """
            The 'scaler' argument is deprecated and will be removed in a future version.
            Please use the 'scale_factors' argument instead.
            """
        warnings.warn(msg, DeprecationWarning, stacklevel=2)
        scale_factors = list(runtime_plan["scale_factors"])
        method = str(runtime_plan["resolved_method"])

    if bool(runtime_plan["warn_laplacian_fallback"]):
        warnings.warn(
            "Laplacian downsampling is not supported anymore.Falling back to `resize`",
            UserWarning,
            stacklevel=2,
        )

    if method is None:
        method = Methods.RESIZE

    pyramid = _build_pyramid(
        image,
        scale_factors,
        dims=dims,
        method=method,
    )

    name = metadata.pop("name", None)
    name = str(name) if name is not None else None

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

    _core.writer_write_multiscales_metadata(
        group,
        _validate_datasets(datasets, ndim, fmt),
        fmt.version,
        axes,
        name,
        metadata,
    )


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
    _core.writer_write_plate_metadata(
        group,
        _validate_plate_rows_columns(rows),
        _validate_plate_rows_columns(columns),
        _validate_plate_wells(wells, rows, columns, fmt=fmt),
        fmt.version,
        None if acquisitions is None else _validate_plate_acquisitions(acquisitions),
        field_count,
        name,
    )


def write_well_metadata(
    group: zarr.Group | str,
    images: list[str | dict],
    fmt: Format | None = None,
) -> None:
    group, fmt = check_group_fmt(group, fmt)
    _core.writer_write_well_metadata(
        group,
        _validate_well_images(images),
        fmt.version,
    )


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

    runtime_plan = dict(
        _core.writer_pyramid_plan(pyramid, int(fmt.zarr_format), axes, storage_options)
    )
    zarr_array_kwargs: dict[str, Any] = {}
    options = _resolve_storage_options(storage_options, 0)

    if bool(runtime_plan["use_v2_chunk_key_encoding"]):
        zarr_array_kwargs["chunk_key_encoding"] = {"name": "v2", "separator": "/"}
        zarr_array_kwargs["compressor"] = options.pop("compressor", _blosc_compressor())
    else:
        dimension_names = list(runtime_plan["dimension_names"])
        if len(dimension_names) > 0:
            zarr_array_kwargs["dimension_names"] = dimension_names
    if not bool(runtime_plan["use_v2_chunk_key_encoding"]) and "compressor" in options:
        zarr_array_kwargs["compressors"] = [options.pop("compressor")]

    shapes = []
    datasets: list[dict] = []
    delayed = []

    for idx, (level, level_plan) in enumerate(
        zip(pyramid, list(runtime_plan["levels"]), strict=False)
    ):
        options = _resolve_storage_options(storage_options, idx)

        chunks_opt = None
        if bool(level_plan["has_chunks"]):
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
        component = str(Path(group.path, str(level_plan["component"])))
        if bool(runtime_plan["use_v2_chunk_key_encoding"]):
            compressor = zarr_array_kwargs["compressor"]
            chunk_key_encoding = dict(zarr_array_kwargs["chunk_key_encoding"])
            chunks = zarr_array_kwargs.get("chunks", level_image.chunksize)
            target = zarr.open_array(
                store=group.store,
                path=component,
                mode="w",
                shape=level_image.shape,
                chunks=chunks,
                dtype=level_image.dtype,
                zarr_format=2,
                dimension_separator=str(chunk_key_encoding["separator"]),
                compressor=compressor,
            )
            delayed.append(
                da.to_zarr(
                    arr=level_image,
                    url=target,
                    compute=False,
                )
            )
        else:
            delayed.append(
                da.to_zarr(
                    arr=level_image,
                    url=group.store,
                    component=component,
                    compute=False,
                    zarr_array_kwargs=zarr_array_kwargs,
                )
            )
        datasets.append({"path": str(level_plan["component"])})

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
    _core.writer_write_label_metadata(
        group,
        name,
        colors,
        properties,
        fmt.version,
        metadata,
    )


def get_metadata(group: zarr.Group | str) -> dict:
    return dict(_core.writer_get_metadata(group))


def add_metadata(
    group: zarr.Group | str, metadata: dict[str, Any], fmt: Format | None = None
) -> None:
    _core.writer_add_metadata(
        group,
        metadata,
        None if fmt is None else fmt.version,
    )


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

    if type(fmt) in (FormatV01, FormatV02, FormatV03):
        raise DeprecationWarning(
            "Writing ome-zarr "
            f"v{fmt.version} is deprecated and has been removed in version 0.15.0."
        )

    axes = _get_valid_axes(len(labels.shape), axes, fmt)
    dims = _extract_dims_from_axes(axes)
    labels_plan = dict(
        _core.writer_labels_plan(
            dims,
            scaler is _DEFAULT_LABEL_SCALER,
            scaler is None,
            0 if scaler in (None, _DEFAULT_LABEL_SCALER) else int(scaler.max_layer),
            method,
        )
    )

    if bool(labels_plan["warn_scaler_deprecated"]):
        msg = """
        The 'scaler' argument is deprecated and will be removed in version 0.13.0.
        Please use the 'scale_factors' argument instead.
        """
        warnings.warn(msg, DeprecationWarning, stacklevel=2)
        scale_factors = list(labels_plan["scale_factors"])

    if method is None or scaler is not None or scaler is _DEFAULT_LABEL_SCALER:
        method = str(labels_plan["resolved_method"])

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
    "write_image",
    "write_labels",
    "write_multiscale",
    "write_multiscale_labels",
    "write_multiscales_metadata",
    "write_plate_metadata",
    "write_well_metadata",
]
