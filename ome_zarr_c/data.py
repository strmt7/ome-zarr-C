"""C++-backed port of selected synthetic data helpers from ome-zarr-py."""

from ._core import data_astronaut as astronaut
from ._core import data_coins as coins
from ._core import data_make_circle as make_circle
from ._core import data_rgb_to_5d as rgb_to_5d

CHANNEL_DIMENSION = 1

__all__ = [
    "CHANNEL_DIMENSION",
    "astronaut",
    "coins",
    "make_circle",
    "rgb_to_5d",
]
