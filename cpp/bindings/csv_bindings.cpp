#include <pybind11/pybind11.h>

#include <algorithm>
#include <string>
#include <vector>

#include "../native/csv.hpp"
#include "common.hpp"

namespace py = pybind11;

namespace {

const std::vector<std::string> kColumnTypes = {"d", "l", "s", "b"};

py::object parse_csv_value(const std::string& value, const std::string& col_type) {
    try {
        const auto parsed = ome_zarr_c::native_code::parse_csv_value(value, col_type);
        if (const auto* text = std::get_if<std::string>(&parsed)) {
            return py::str(*text);
        }
        if (const auto* number = std::get_if<double>(&parsed)) {
            return py::float_(*number);
        }
        if (const auto* integer = std::get_if<std::int64_t>(&parsed)) {
            return py::int_(*integer);
        }
        return py::bool_(std::get<bool>(parsed));
    } catch (const std::overflow_error& exc) {
        ome_zarr_c::bindings::raise_overflow_error(exc.what());
    }
}

void dict_to_zarr(
    py::dict props_to_add,
    const std::string& zarr_path,
    const std::string& zarr_id) {
    py::object zarr = py::module_::import("zarr");
    py::object os_path = py::module_::import("os").attr("path");
    py::object builtins = py::module_::import("builtins");

    py::object root = zarr.attr("open_group")(py::str(zarr_path));
    py::dict root_attrs = py::cast<py::dict>(root.attr("attrs").attr("asdict")());
    py::object plate_attrs = root_attrs.attr("get")("plate", py::none());
    const bool multiscales =
        py::cast<bool>(root_attrs.attr("__contains__")("multiscales"));

    if (plate_attrs.is_none() && !multiscales) {
        ome_zarr_c::bindings::raise_plain_exception(
            "zarr_path must be to plate.zarr or image.zarr");
    }

    py::list labels_paths;
    if (!plate_attrs.is_none()) {
        py::object wells = plate_attrs.attr("get")("wells", py::list());
        for (const py::handle& well_handle : wells) {
            py::object well = py::reinterpret_borrow<py::object>(well_handle);
            labels_paths.append(os_path.attr("join")(
                py::str(zarr_path),
                well.attr("get")("path"),
                py::str("0"),
                py::str("labels"),
                py::str("0")));
        }
    } else {
        labels_paths.append(os_path.attr("join")(
            py::str(zarr_path), py::str("labels"), py::str("0")));
    }

    for (const py::handle& path_handle : labels_paths) {
        py::object label_group =
            zarr.attr("open_group")(py::reinterpret_borrow<py::object>(path_handle));
        py::dict attrs = py::cast<py::dict>(label_group.attr("attrs").attr("asdict")());
        py::object image_label = attrs.attr("get")("image-label", py::dict());
        py::object properties = image_label.attr("get")("properties");
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
    py::object builtins = py::module_::import("builtins");
    py::object csv_module = py::module_::import("csv");
    py::object path_cls = py::module_::import("pathlib").attr("Path");
    py::object zip_fn = builtins.attr("zip");

    py::dict cols_types_by_name;
    for (const py::handle& item_handle :
         py::str(csv_keys).attr("split")(py::str(","))) {
        const std::string col_name_type = py::cast<std::string>(item_handle);
        const std::size_t split_at = col_name_type.rfind('#');
        if (split_at != std::string::npos) {
            const std::string col_name = col_name_type.substr(0, split_at);
            std::string col_type = col_name_type.substr(split_at + 1);
            if (std::find(kColumnTypes.begin(), kColumnTypes.end(), col_type) ==
                kColumnTypes.end()) {
                col_type = "s";
            }
            cols_types_by_name[py::str(col_name)] = py::str(col_type);
        } else {
            cols_types_by_name[py::str(col_name_type)] = py::str("s");
        }
    }

    py::object csv_columns = py::none();
    int id_column = -1;
    py::dict props_by_id;

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
            if (csv_columns.is_none()) {
                csv_columns = row;
                py::list csv_columns_list = py::cast<py::list>(csv_columns);
                if (!csv_columns_list.contains(py::str(csv_id))) {
                    throw py::value_error(
                        "csv_id '" + csv_id + "' should match acsv column name: " +
                        ome_zarr_c::bindings::repr_object(csv_columns));
                }
                id_column =
                    py::cast<int>(csv_columns.attr("index")(py::str(csv_id)));
            } else {
                py::object row_id = row[id_column];
                py::dict row_props;
                for (const py::handle& pair_handle : zip_fn(csv_columns, row)) {
                    py::tuple pair = py::cast<py::tuple>(pair_handle);
                    py::str col_name = py::cast<py::str>(pair[0]);
                    if (cols_types_by_name.contains(col_name)) {
                        row_props[col_name] = parse_csv_value(
                            py::cast<std::string>(pair[1]),
                            py::cast<std::string>(cols_types_by_name[col_name]));
                    }
                }
                props_by_id[row_id] = row_props;
            }
        }
        csvfile.attr("close")();
    } catch (...) {
        try {
            csvfile.attr("close")();
        } catch (...) {
        }
        throw;
    }

    dict_to_zarr(props_by_id, zarr_path, zarr_id);
}

}  // namespace

void register_csv_bindings(py::module_& m) {
    m.def("csv_to_zarr", &csv_to_zarr);
    m.def("dict_to_zarr", &dict_to_zarr);
}
