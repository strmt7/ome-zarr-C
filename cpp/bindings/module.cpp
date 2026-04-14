#include <pybind11/pybind11.h>

namespace py = pybind11;

void register_basic_bindings(py::module_& m);
void register_cli_bindings(py::module_& m);
void register_csv_bindings(py::module_& m);
void register_data_bindings(py::module_& m);
void register_dask_utils_bindings(py::module_& m);
void register_format_bindings(py::module_& m);
void register_io_bindings(py::module_& m);
void register_reader_bindings(py::module_& m);
void register_scale_bindings(py::module_& m);
void register_utils_bindings(py::module_& m);
void register_writer_bindings(py::module_& m);

PYBIND11_MODULE(_core, m) {
    register_basic_bindings(m);
    register_cli_bindings(m);
    register_csv_bindings(m);
    register_data_bindings(m);
    register_dask_utils_bindings(m);
    register_format_bindings(m);
    register_io_bindings(m);
    register_reader_bindings(m);
    register_scale_bindings(m);
    register_utils_bindings(m);
    register_writer_bindings(m);
}
