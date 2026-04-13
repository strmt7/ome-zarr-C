#include <pybind11/pybind11.h>

#include <string>
#include <vector>

#include "../native/io.hpp"

namespace py = pybind11;

namespace {

py::str io_basename(const std::string& path) {
    return py::str(ome_zarr_c::native_code::io_basename(path));
}

py::list io_parts(const std::string& path, bool is_file) {
    py::list parts;
    for (const auto& part : ome_zarr_c::native_code::io_parts(path, is_file)) {
        parts.append(py::str(part));
    }
    return parts;
}

py::str io_subpath(
    const std::string& path,
    const std::string& subpath,
    bool is_file,
    bool is_http) {
    return py::str(
        ome_zarr_c::native_code::io_subpath(path, subpath, is_file, is_http));
}

py::str io_repr(
    const std::string& subpath,
    bool has_zgroup,
    bool has_zarray) {
    return py::str(
        ome_zarr_c::native_code::io_repr(subpath, has_zgroup, has_zarray));
}

bool io_protocol_is_http(py::object protocol) {
    std::vector<std::string> protocols;
    if (py::isinstance<py::tuple>(protocol) || py::isinstance<py::list>(protocol)) {
        for (const py::handle& entry : py::iterable(protocol)) {
            protocols.push_back(py::cast<std::string>(entry));
        }
    } else {
        protocols.push_back(py::cast<std::string>(protocol));
    }
    return ome_zarr_c::native_code::io_protocol_is_http(protocols);
}

}  // namespace

void register_io_bindings(py::module_& m) {
    m.def("io_basename", &io_basename, py::arg("path"));
    m.def("io_parts", &io_parts, py::arg("path"), py::arg("is_file"));
    m.def(
        "io_subpath",
        &io_subpath,
        py::arg("path"),
        py::arg("subpath"),
        py::arg("is_file"),
        py::arg("is_http"));
    m.def(
        "io_repr",
        &io_repr,
        py::arg("subpath"),
        py::arg("has_zgroup"),
        py::arg("has_zarray"));
    m.def("io_protocol_is_http", &io_protocol_is_http, py::arg("protocol"));
}
