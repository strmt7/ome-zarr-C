from pybind11.setup_helpers import Pybind11Extension, build_ext
from setuptools import find_packages, setup

ext_modules = [
    Pybind11Extension(
        "ome_zarr_c._core",
        [
            "cpp/bindings/format_bindings.cpp",
            "cpp/core.cpp",
            "cpp/native/conversions.cpp",
            "cpp/native/csv.cpp",
            "cpp/native/dask_utils.cpp",
            "cpp/native/format.cpp",
        ],
        cxx_std=17,
    ),
]


setup(
    packages=find_packages(),
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
    zip_safe=False,
)
