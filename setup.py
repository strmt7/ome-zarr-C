from pybind11.setup_helpers import Pybind11Extension, build_ext
from setuptools import find_packages, setup

ext_modules = [
    Pybind11Extension(
        "omero_zarr_c._core",
        ["cpp/core.cpp"],
        cxx_std=17,
    ),
]


setup(
    packages=find_packages(),
    ext_modules=ext_modules,
    cmdclass={"build_ext": build_ext},
    zip_safe=False,
)
