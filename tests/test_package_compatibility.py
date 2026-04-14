from __future__ import annotations

import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "source_code_v.0.15.0"))

_py_pkg = importlib.import_module("ome_zarr")
_cpp_pkg = importlib.import_module("ome_zarr_c")
_py_types = importlib.import_module("ome_zarr.types")
_cpp_types = importlib.import_module("ome_zarr_c.types")


def test_package_version_fallback_matches_upstream() -> None:
    assert _py_pkg.__version__ == _cpp_pkg.__version__
    assert "__version__" in _cpp_pkg.__all__


def test_types_module_surface_is_importable() -> None:
    for name in ("JSONDict", "LayerData", "PathLike", "ReaderFunction"):
        assert hasattr(_py_types, name)
        assert hasattr(_cpp_types, name)
    assert _cpp_types.__all__ == ["JSONDict", "LayerData", "PathLike", "ReaderFunction"]
