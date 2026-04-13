#include <pybind11/pybind11.h>

#include <string>
#include <vector>

#include "../native/writer.hpp"
#include "common.hpp"

namespace py = pybind11;

namespace {

py::list validate_plate_wells(
    py::object wells,
    py::object rows,
    py::object columns,
    py::object fmt = py::none()) {
    if (wells.is_none() || py::len(wells) == 0) {
        throw py::value_error("Empty wells list");
    }

    py::list validated_wells;
    for (const py::handle& well_handle : py::iterable(wells)) {
        py::object well = py::reinterpret_borrow<py::object>(well_handle);
        if (py::isinstance<py::str>(well)) {
            py::object well_dict = fmt.attr("generate_well_dict")(well, rows, columns);
            fmt.attr("validate_well_dict")(well_dict, rows, columns);
            validated_wells.append(well_dict);
        } else if (py::isinstance<py::dict>(well)) {
            fmt.attr("validate_well_dict")(well, rows, columns);
            validated_wells.append(well);
        } else {
            throw py::value_error(
                "Unrecognized type for " +
                py::cast<std::string>(py::str(well)));
        }
    }

    return validated_wells;
}

py::object blosc_compressor() {
    py::object blosc = py::module_::import("numcodecs").attr("Blosc");
    return blosc(
        py::arg("cname") = "zstd",
        py::arg("clevel") = 5,
        py::arg("shuffle") = blosc.attr("SHUFFLE"));
}

py::object resolve_storage_options(py::object storage_options, py::object path) {
    py::dict options;
    if (ome_zarr_c::bindings::object_truthy(storage_options)) {
        if (!py::isinstance<py::list>(storage_options)) {
            options = py::cast<py::dict>(storage_options.attr("copy")());
        } else {
            return storage_options.attr("__getitem__")(path);
        }
    }
    return options;
}

}  // namespace

void register_writer_bindings(py::module_& m) {
    m.def("_validate_plate_wells",
          &validate_plate_wells,
          py::arg("wells"),
          py::arg("rows"),
          py::arg("columns"),
          py::arg("fmt"));
    m.def("_blosc_compressor", &blosc_compressor);
    m.def("_resolve_storage_options", &resolve_storage_options, py::arg("storage_options"), py::arg("path"));
}
