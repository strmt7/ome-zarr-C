"""Native C++ conversion workspace for ome-zarr-py."""

try:
    from ._version import version as __version__
except ImportError:
    __version__ = "0+unknown"

from .axes import KNOWN_AXES, Axes
from .conversions import int_to_rgba, int_to_rgba_255, rgba_to_int
from .csv import COLUMN_TYPES, dict_to_zarr, parse_csv_value
from .data import CHANNEL_DIMENSION, astronaut, coins, make_circle, rgb_to_5d
from .format import (
    CurrentFormat,
    Format,
    FormatV01,
    FormatV02,
    FormatV03,
    FormatV04,
    FormatV05,
    detect_format,
    format_from_version,
    format_implementations,
)
from .io import ZarrLocation, parse_url
from .reader import (
    OMERO,
    Label,
    Labels,
    Multiscales,
    Node,
    Plate,
    Reader,
    Spec,
    Well,
)
from .utils import find_multiscales, splitall, strip_common_prefix

__all__ = [
    "Axes",
    "COLUMN_TYPES",
    "CHANNEL_DIMENSION",
    "CurrentFormat",
    "Format",
    "FormatV01",
    "FormatV02",
    "FormatV03",
    "FormatV04",
    "FormatV05",
    "KNOWN_AXES",
    "Label",
    "Labels",
    "Multiscales",
    "Node",
    "OMERO",
    "Plate",
    "Reader",
    "Spec",
    "Well",
    "__version__",
    "astronaut",
    "coins",
    "detect_format",
    "dict_to_zarr",
    "find_multiscales",
    "format_from_version",
    "format_implementations",
    "int_to_rgba",
    "int_to_rgba_255",
    "make_circle",
    "parse_csv_value",
    "parse_url",
    "rgba_to_int",
    "rgb_to_5d",
    "splitall",
    "strip_common_prefix",
    "ZarrLocation",
]
