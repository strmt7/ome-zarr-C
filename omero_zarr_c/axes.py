"""C++-backed port of selected axes logic from ome-zarr-py."""

from __future__ import annotations

from . import _core

KNOWN_AXES = {"x": "space", "y": "space", "z": "space", "c": "channel", "t": "time"}


def _format_version(fmt: object | None) -> str:
    if fmt is None:
        return "0.5"
    if isinstance(fmt, str):
        return fmt
    if hasattr(fmt, "version"):
        return str(fmt.version)
    raise TypeError(f"Unsupported fmt value: {fmt!r}")


class Axes:
    def __init__(
        self,
        axes: list[str] | list[dict[str, str]],
        fmt: object = "0.5",
    ) -> None:
        """
        Constructor, transforms axes and validates

        Raises ValueError if not valid
        """
        if axes is not None:
            self.axes = self._axes_to_dicts(axes)
        elif _format_version(fmt) in ("0.1", "0.2"):
            self.axes = self._axes_to_dicts(["t", "c", "z", "y", "x"])
        self.fmt = fmt
        self.validate()

    def validate(self) -> None:
        """Raises ValueError if not valid."""
        version = _format_version(self.fmt)
        if version in ("0.1", "0.2"):
            return

        if version == "0.3":
            self._validate_03()
            return

        _core.validate_axes_types(self.axes)

    def to_list(self, fmt: object = "0.5") -> list[str] | list[dict[str, str]]:
        if _format_version(fmt) == "0.3":
            return self._get_names()
        return self.axes

    @staticmethod
    def _axes_to_dicts(axes: list[str] | list[dict[str, str]]) -> list[dict[str, str]]:
        """Returns a list of axis dicts with name and type."""
        return list(_core.axes_to_dicts(axes))

    def _get_names(self) -> list[str]:
        """Returns a list of axis names."""
        return list(_core.get_names(self.axes))

    def _validate_03(self) -> None:
        _core.validate_03(self.axes)
