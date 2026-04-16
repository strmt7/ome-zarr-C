import os
import sys

from pybind11.setup_helpers import Pybind11Extension, build_ext
from setuptools import find_packages, setup

define_macros = []
if sys.version_info >= (3, 14):
    define_macros.append(("OME_ZARR_C_PY_LIST_INDEX_GENERIC_NOT_FOUND", "1"))
define_macros.append(
    (
        "OME_ZARR_C_SOURCE_ROOT",
        '"' + os.path.abspath(os.path.dirname(__file__)).replace("\\", "\\\\") + '"',
    )
)


def _build_flags() -> tuple[list[str], list[str]]:
    compile_args: list[str] = []
    link_args: list[str] = []

    if sys.platform == "win32":
        compile_args.append("/O2")
        if os.environ.get("OME_ZARR_C_ENABLE_LTO") == "1":
            compile_args.append("/GL")
            link_args.append("/LTCG")
    else:
        compile_args.extend(["-O3", "-fvisibility=hidden"])
        if os.environ.get("OME_ZARR_C_ENABLE_LTO") == "1":
            compile_args.append("-flto=auto")
            link_args.append("-flto=auto")
        if os.environ.get("OME_ZARR_C_MARCH_NATIVE") == "1":
            compile_args.extend(["-march=native", "-mtune=native"])

    return compile_args, link_args


extra_compile_args, extra_link_args = _build_flags()

ext_modules = [
    Pybind11Extension(
        "ome_zarr_c._core",
        [
            "cpp/bindings/dask_utils_bindings.cpp",
            "cpp/bindings/format_bindings.cpp",
            "cpp/bindings/module.cpp",
            "cpp/bindings/scale_bindings.cpp",
            "cpp/bindings/writer_bindings.cpp",
            "cpp/native/axes.cpp",
            "cpp/native/cli.cpp",
            "cpp/native/conversions.cpp",
            "cpp/native/csv.cpp",
            "cpp/native/data.cpp",
            "cpp/native/dask_utils.cpp",
            "cpp/native/format.cpp",
            "cpp/native/io.cpp",
            "cpp/native/scale.cpp",
            "cpp/native/utils.cpp",
            "cpp/native/writer.cpp",
        ],
        cxx_std=17,
        define_macros=define_macros,
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args,
    ),
]


setup(
    packages=find_packages(),
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
    zip_safe=False,
)
