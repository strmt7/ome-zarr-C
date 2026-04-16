#include <pybind11/pybind11.h>

namespace py = pybind11;

void register_dask_utils_bindings(py::module_& m);
void register_format_bindings(py::module_& m);
void register_scale_bindings(py::module_& m);

PYBIND11_MODULE(_core, m) {
    register_dask_utils_bindings(m);
    register_format_bindings(m);
    register_scale_bindings(m);
}
