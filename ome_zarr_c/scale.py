"""C++-backed port of selected scale helpers from ome-zarr-py."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Union, cast

import dask.array as da
import numpy as np
import zarr
from deprecated import deprecated
from scipy import __version__ as scipy_version
from skimage import __version__ as skimage_version

from . import _core
from .dask_utils import local_mean as dask_local_mean
from .dask_utils import resize as dask_resize
from .dask_utils import zoom as dask_zoom

SPATIAL_DIMS = ("z", "y", "x")
LOGGER = logging.getLogger("ome_zarr.scale")
ListOfArrayLike = Union[list[da.Array], list[np.ndarray]]  # noqa: UP007
ArrayLike = Union[da.Array, np.ndarray]  # noqa: UP007


class Methods(Enum):
    RESIZE = "resize"
    NEAREST = "nearest"
    LOCAL_MEAN = "local_mean"
    ZOOM = "zoom"


method_dispatch = {
    Methods.RESIZE: {
        "func": dask_resize,
        "kwargs": {
            "order": 1,
            "mode": "reflect",
            "anti_aliasing": True,
            "preserve_range": True,
        },
        "used_function": "skimage.transform.resize",
        "version": skimage_version,
    },
    Methods.NEAREST: {
        "func": dask_resize,
        "kwargs": {
            "order": 0,
            "mode": "reflect",
            "anti_aliasing": False,
            "preserve_range": True,
        },
        "used_function": "skimage.transform.resize",
        "version": skimage_version,
    },
    Methods.LOCAL_MEAN: {
        "func": dask_local_mean,
        "kwargs": {},
        "used_function": "skimage.transform.downscale_local_mean",
        "version": skimage_version,
    },
    Methods.ZOOM: {
        "func": dask_zoom,
        "kwargs": {},
        "used_function": "scipy.ndimage.zoom",
        "version": scipy_version,
    },
}


@deprecated(
    reason=(
        "Downsampling via the `Scaler` class has been deprecated. "
        "Please use the `scale_factors` argument instead."
    ),
    version="0.14.0",
)
@dataclass
class Scaler:
    copy_metadata: bool = False
    downscale: int = 2
    in_place: bool = False
    labeled: bool = False
    max_layer: int = 4
    method: str = "nearest"
    order: int = 1

    @staticmethod
    def methods():
        yield from _core.scaler_methods()

    def scale(self, input_array: str, output_directory: str) -> None:
        func = self.func

        base = zarr.open_array(input_array)
        pyramid = func(base)

        if self.labeled:
            self.__assert_values(pyramid)

        grp = self.__create_group(output_directory, base, pyramid)

        if self.copy_metadata:
            print(f"copying attribute keys: {list(base.attrs.keys())}")
            grp.attrs.update(base.attrs)

    @property
    def func(self):
        if self.method not in set(_core.scaler_methods()):
            raise Exception
        return getattr(self, self.method)

    def __assert_values(self, pyramid: list[np.ndarray]) -> None:
        expected = set(np.unique(pyramid[0]))
        print(f"level 0 {pyramid[0].shape} = {len(expected)} labels")
        for i in range(1, len(pyramid)):
            level = pyramid[i]
            print(f"level {i}", pyramid[i].shape, len(expected))
            found = set(np.unique(level))
            if not expected.issuperset(found):
                raise Exception(
                    f"{len(found)} found values are not "
                    "a subset of {len(expected)} values"
                )

    def __create_group(
        self, dir_path: str, base: np.ndarray, pyramid: list[np.ndarray]
    ) -> zarr.Group:
        grp = zarr.open_group(dir_path, mode="w")
        grp.create_dataset("base", data=base)
        series = []
        for i in range(len(pyramid)):
            if i == 0:
                path = "base"
            else:
                path = str(i)
                grp.create_dataset(path, data=pyramid[i])
            series.append({"path": path})
        return grp

    def resize_image(self, image: ArrayLike) -> ArrayLike:
        return _core.scaler_resize_image(image, self.downscale, self.order)

    def nearest(self, base: np.ndarray) -> list[np.ndarray]:
        return cast(
            list[np.ndarray], _core.scaler_nearest(base, self.downscale, self.max_layer)
        )

    def gaussian(self, base: np.ndarray) -> list[np.ndarray]:
        return cast(
            list[np.ndarray],
            _core.scaler_gaussian(base, self.downscale, self.max_layer),
        )

    def laplacian(self, base: np.ndarray) -> list[np.ndarray]:
        return cast(
            list[np.ndarray],
            _core.scaler_laplacian(base, self.downscale, self.max_layer),
        )

    def local_mean(self, base: np.ndarray) -> list[np.ndarray]:
        return cast(
            list[np.ndarray],
            _core.scaler_local_mean(base, self.downscale, self.max_layer),
        )

    def zoom(self, base: np.ndarray) -> list[np.ndarray]:
        return cast(
            list[np.ndarray],
            _core.scaler_zoom(base, self.downscale, self.max_layer),
        )


def _build_pyramid(
    image,
    scale_factors,
    dims,
    method: str | Methods = "nearest",
    chunks=None,
):
    if isinstance(method, str):
        method = Methods(method)
    return _core._build_pyramid(image, scale_factors, dims, method, chunks)


__all__ = [
    "ArrayLike",
    "ListOfArrayLike",
    "Methods",
    "SPATIAL_DIMS",
    "Scaler",
    "_build_pyramid",
    "method_dispatch",
]
