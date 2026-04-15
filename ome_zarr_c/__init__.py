"""Native C++ conversion workspace for ome-zarr-py."""

try:
    from ._version import version as __version__
except ImportError:
    __version__ = "0+unknown"

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

__all__ = [
    "CurrentFormat",
    "Format",
    "FormatV01",
    "FormatV02",
    "FormatV03",
    "FormatV04",
    "FormatV05",
    "__version__",
    "detect_format",
    "format_from_version",
    "format_implementations",
]
