"""Native C++ conversion workspace for ome-zarr-py."""

try:
    from ._version import version as __version__
except ImportError:
    __version__ = "0+unknown"

from .axes import KNOWN_AXES, Axes
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

__all__ = [
    "Axes",
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
    "detect_format",
    "format_from_version",
    "format_implementations",
    "parse_url",
    "ZarrLocation",
]
