#include <pybind11/pybind11.h>

#include <string>
#include <vector>

#include "../native/csv.hpp"
#include "common.hpp"

namespace py = pybind11;

namespace {

py::object py_from_csv_value(const ome_zarr_c::native_code::CsvValue& value) {
    if (const auto* text = std::get_if<std::string>(&value)) {
        return py::str(*text);
    }
    if (const auto* number = std::get_if<double>(&value)) {
        return py::float_(*number);
    }
    if (const auto* integer = std::get_if<std::int64_t>(&value)) {
        return py::int_(*integer);
    }
    return py::bool_(std::get<bool>(value));
}

py::object parse_csv_value(const std::string& value, const std::string& col_type) {
    try {
        return py_from_csv_value(
            ome_zarr_c::native_code::parse_csv_value(value, col_type));
    } catch (const std::overflow_error& exc) {
        ome_zarr_c::bindings::raise_overflow_error(exc.what());
    }
}

std::vector<std::vector<std::string>> read_csv_rows(const std::string& csv_path) {
    py::object csv_module = py::module_::import("csv");
    py::object path_cls = py::module_::import("pathlib").attr("Path");

    std::vector<std::vector<std::string>> rows;
    py::object csvfile = path_cls(py::str(csv_path))
                             .attr("open")(
                                 py::str("r"),
                                 py::int_(-1),
                                 py::none(),
                                 py::none(),
                                 py::str(""));
    try {
        py::object row_reader = csv_module.attr("reader")(csvfile);
        for (const py::handle& row_handle : row_reader) {
            py::list row = py::cast<py::list>(row_handle);
            std::vector<std::string> native_row;
            native_row.reserve(py::len(row));
            for (const py::handle& value : row) {
                native_row.push_back(py::cast<std::string>(value));
            }
            rows.push_back(std::move(native_row));
        }
        csvfile.attr("close")();
    } catch (...) {
        try {
            csvfile.attr("close")();
        } catch (...) {
        }
        throw;
    }
    return rows;
}

py::dict props_by_id_to_python(
    const ome_zarr_c::native_code::CsvPropsById& props_by_id) {
    py::dict result;
    for (const auto& [row_id, props] : props_by_id) {
        py::dict row_props;
        for (const auto& [key, value] : props) {
            row_props[py::str(key)] = py_from_csv_value(value);
        }
        result[py::str(row_id)] = row_props;
    }
    return result;
}

void dict_to_zarr(
    py::dict props_to_add,
    const std::string& zarr_path,
    const std::string& zarr_id) {
    py::object zarr = py::module_::import("zarr");
    py::object builtins = py::module_::import("builtins");

    py::object root = zarr.attr("open_group")(py::str(zarr_path));
    py::dict root_attrs = py::cast<py::dict>(root.attr("attrs").attr("asdict")());
    py::object plate_attrs = root_attrs.attr("get")(py::str("plate"), py::none());
    const bool has_plate = !plate_attrs.is_none();
    const bool has_multiscales =
        py::cast<bool>(root_attrs.attr("__contains__")(py::str("multiscales")));

    std::vector<std::string> well_paths;
    if (has_plate) {
        py::object wells = plate_attrs.attr("get")(py::str("wells"), py::list());
        for (const py::handle& well_handle : wells) {
            py::object well = py::reinterpret_borrow<py::object>(well_handle);
            well_paths.push_back(py::cast<std::string>(
                well.attr("get")(py::str("path"))));
        }
    }

    std::vector<std::string> label_paths;
    try {
        label_paths = ome_zarr_c::native_code::csv_label_paths(
            has_plate,
            has_multiscales,
            zarr_path,
            well_paths);
    } catch (const std::runtime_error& exc) {
        ome_zarr_c::bindings::raise_plain_exception(exc.what());
    }

    for (const auto& path : label_paths) {
        py::object label_group = zarr.attr("open_group")(py::str(path));
        py::dict attrs = py::cast<py::dict>(label_group.attr("attrs").attr("asdict")());
        py::object properties =
            attrs.attr("get")(py::str("image-label"), py::dict())
                .attr("get")(py::str("properties"));
        if (properties.is_none()) {
            continue;
        }

        for (const py::handle& props_handle : properties) {
            py::object props_dict = py::reinterpret_borrow<py::object>(props_handle);
            py::object props_id =
                builtins.attr("str")(props_dict.attr("get")(py::str(zarr_id)));
            if (props_to_add.contains(props_id)) {
                py::object values_to_add = props_to_add[props_id];
                for (const py::handle& item_handle : values_to_add.attr("items")()) {
                    py::tuple item = py::cast<py::tuple>(item_handle);
                    props_dict[item[0]] = item[1];
                }
            }
        }

        label_group.attr("attrs").attr("update")(attrs);
    }
}

void csv_to_zarr(
    const std::string& csv_path,
    const std::string& csv_id,
    const std::string& csv_keys,
    const std::string& zarr_path,
    const std::string& zarr_id) {
    const auto rows = read_csv_rows(csv_path);
    const auto specs = ome_zarr_c::native_code::parse_csv_key_specs(csv_keys);

    try {
        const auto props_by_id =
            ome_zarr_c::native_code::csv_props_by_id(rows, csv_id, specs);
        dict_to_zarr(props_by_id_to_python(props_by_id), zarr_path, zarr_id);
    } catch (const std::invalid_argument&) {
        py::list header;
        if (!rows.empty()) {
            for (const auto& value : rows.front()) {
                header.append(py::str(value));
            }
        }
        throw py::value_error(
            "csv_id '" + csv_id + "' should match acsv column name: " +
            ome_zarr_c::bindings::repr_object(header));
    } catch (const std::out_of_range& exc) {
        PyErr_SetString(PyExc_IndexError, exc.what());
        throw py::error_already_set();
    }
}

}  // namespace

void register_csv_bindings(py::module_& m) {
    m.def("csv_to_zarr", &csv_to_zarr);
    m.def("dict_to_zarr", &dict_to_zarr);
    m.def("parse_csv_value", &parse_csv_value);
}
