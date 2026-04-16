#include <pybind11/pybind11.h>

namespace py = pybind11;

void register_format_bindings(py::module_& m);

PYBIND11_MODULE(_core, m) {
    register_format_bindings(m);
}
