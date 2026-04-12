#include <pybind11/pybind11.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "native/conversions.hpp"
#include "native/csv.hpp"
#include "native/data.hpp"
#include "native/dask_utils.hpp"
#include "native/axes.hpp"
#include "native/format.hpp"
#include "native/reader.hpp"
#include "native/scale.hpp"
#include "native/utils.hpp"
#include "native/writer.hpp"

namespace py = pybind11;

namespace {

const std::map<std::string, std::string> kKnownAxes = {
    {"x", "space"},
    {"y", "space"},
    {"z", "space"},
    {"c", "channel"},
    {"t", "time"},
};

const std::vector<std::string> kColumnTypes = {"d", "l", "s", "b"};

std::string repr_object(const py::handle& obj) {
    return py::cast<std::string>(py::repr(obj));
}

std::vector<std::string> axis_names(const py::sequence& axes) {
    std::vector<std::string> names;
    names.reserve(py::len(axes));

    for (const py::handle& axis_handle : axes) {
        py::dict axis = py::cast<py::dict>(axis_handle);
        if (!axis.contains("name")) {
            throw py::value_error(
                "Axis Dict " + repr_object(axis) + " has no 'name'");
        }
        names.push_back(py::cast<std::string>(axis["name"]));
    }

    return names;
}

std::vector<ome_zarr_c::native_code::AxisRecord> axis_records_from_sequence(
    const py::sequence& axes) {
    std::vector<ome_zarr_c::native_code::AxisRecord> records;
    records.reserve(py::len(axes));

    for (const py::handle& axis_handle : axes) {
        ome_zarr_c::native_code::AxisRecord record{};
        if (py::isinstance<py::str>(axis_handle)) {
            record.has_name = true;
            record.name = py::cast<std::string>(axis_handle);
            record.has_type = false;
            record.type = "";
            record.axis_repr = "";
            record.type_repr = "None";
        } else {
            py::dict axis = py::cast<py::dict>(axis_handle);
            record.has_name = axis.contains("name");
            if (record.has_name) {
                record.name = py::cast<std::string>(axis["name"]);
            }
            py::object axis_type = axis.attr("get")("type");
            record.has_type = !axis_type.is_none();
            if (record.has_type) {
                record.type = py::cast<std::string>(axis_type);
            }
            record.axis_repr = repr_object(axis);
            record.type_repr = repr_object(axis_type);
        }
        records.push_back(std::move(record));
    }

    return records;
}

[[noreturn]] void raise_plain_exception(const std::string& message) {
    PyErr_SetString(PyExc_Exception, message.c_str());
    throw py::error_already_set();
}

[[noreturn]] void raise_value_error_args(const py::tuple& args) {
    PyErr_SetObject(PyExc_ValueError, args.ptr());
    throw py::error_already_set();
}

[[noreturn]] void raise_overflow_error(const std::string& message) {
    PyErr_SetString(PyExc_OverflowError, message.c_str());
    throw py::error_already_set();
}

bool is_number_like(const py::handle& value) {
    return PyFloat_Check(value.ptr()) || PyLong_Check(value.ptr());
}

bool objects_equal(const py::handle& left, const py::handle& right) {
    const int result = PyObject_RichCompareBool(left.ptr(), right.ptr(), Py_EQ);
    if (result < 0) {
        throw py::error_already_set();
    }
    return result == 1;
}

bool object_truthy(const py::handle& obj) {
    const int result = PyObject_IsTrue(obj.ptr());
    if (result < 0) {
        throw py::error_already_set();
    }
    return result == 1;
}

bool rich_compare_bool(const py::handle& left, const py::handle& right, int op) {
    const int result = PyObject_RichCompareBool(left.ptr(), right.ptr(), op);
    if (result < 0) {
        throw py::error_already_set();
    }
    return result == 1;
}

py::object true_divide(const py::handle& left, const py::handle& right) {
    PyObject* result = PyNumber_TrueDivide(left.ptr(), right.ptr());
    if (result == nullptr) {
        throw py::error_already_set();
    }
    return py::reinterpret_steal<py::object>(result);
}

py::object floor_divide(const py::handle& left, const py::handle& right) {
    PyObject* result = PyNumber_FloorDivide(left.ptr(), right.ptr());
    if (result == nullptr) {
        throw py::error_already_set();
    }
    return py::reinterpret_steal<py::object>(result);
}

py::object call_callable(
    const py::handle& callable,
    const std::vector<py::object>& leading_args,
    const py::tuple& extra_args = py::tuple(),
    const py::dict& kwargs = py::dict()) {
    py::tuple call_args(leading_args.size() + extra_args.size());
    py::size_t index = 0;
    for (const py::object& arg : leading_args) {
        call_args[index++] = arg;
    }
    for (const py::handle& arg : extra_args) {
        call_args[index++] = py::reinterpret_borrow<py::object>(arg);
    }

    PyObject* result = PyObject_Call(callable.ptr(), call_args.ptr(), kwargs.ptr());
    if (result == nullptr) {
        throw py::error_already_set();
    }
    return py::reinterpret_steal<py::object>(result);
}

void set_item(const py::handle& obj, const py::handle& key, const py::handle& value) {
    if (PyObject_SetItem(obj.ptr(), key.ptr(), value.ptr()) < 0) {
        throw py::error_already_set();
    }
}

py::object read_text_with_open(const py::object& path) {
    py::object builtins = py::module_::import("builtins");
    py::object handle = builtins.attr("open")(path);
    try {
        py::object text = handle.attr("read")();
        handle.attr("close")();
        return text;
    } catch (...) {
        try {
            handle.attr("close")();
        } catch (...) {
        }
        throw;
    }
}

std::string repr_joined_lines(const py::list& parts) {
    std::string message = "No common prefix:\n";
    for (const py::handle& part_handle : parts) {
        message += repr_object(part_handle);
        message += "\n";
    }
    return message;
}

template <typename Func>
void run_with_context_manager(const py::object& manager, Func&& func) {
    py::object exit = manager.attr("__exit__");
    manager.attr("__enter__")();
    try {
        func();
        exit(py::none(), py::none(), py::none());
    } catch (py::error_already_set& ex) {
        py::object suppress = exit(ex.type(), ex.value(), ex.trace());
        if (!object_truthy(suppress)) {
            throw;
        }
    } catch (...) {
        exit(py::none(), py::none(), py::none());
        throw;
    }
}

py::tuple output_slices_for_shape(const py::handle& shape) {
    py::sequence shape_seq = py::cast<py::sequence>(shape);
    const py::size_t ndim = py::len(shape_seq);
    py::tuple slices(ndim);
    for (py::size_t index = 0; index < ndim; ++index) {
        slices[index] = py::slice(
            py::int_(0),
            py::reinterpret_borrow<py::object>(shape_seq[index]),
            py::int_(1));
    }
    return slices;
}

}  // namespace

void register_format_bindings(py::module_& m);

py::list axes_to_dicts(const py::sequence& axes) {
    py::list result;

    for (const py::handle& axis_handle : axes) {
        if (py::isinstance<py::str>(axis_handle)) {
            const std::string axis = py::cast<std::string>(axis_handle);
            py::dict axis_dict;
            axis_dict["name"] = axis;
            const auto it = kKnownAxes.find(axis);
            if (it != kKnownAxes.end()) {
                axis_dict["type"] = it->second;
            }
            result.append(axis_dict);
        } else {
            result.append(py::reinterpret_borrow<py::object>(axis_handle));
        }
    }

    return result;
}

py::list get_names(const py::sequence& axes) {
    py::list result;
    const auto names = ome_zarr_c::native_code::get_names(axis_records_from_sequence(axes));
    for (const auto& name : names) {
        result.append(py::str(name));
    }
    return result;
}

void validate_03(const py::sequence& axes) {
    try {
        const auto names =
            ome_zarr_c::native_code::get_names(axis_records_from_sequence(axes));
        ome_zarr_c::native_code::validate_03(names);
    } catch (const std::invalid_argument& exc) {
        throw py::value_error(exc.what());
    }
}

void validate_axes_types(const py::sequence& axes) {
    try {
        ome_zarr_c::native_code::validate_axes_types(axis_records_from_sequence(axes));
    } catch (const std::invalid_argument& exc) {
        throw py::value_error(exc.what());
    }
}

py::list int_to_rgba_255(std::int32_t value) {
    const auto bytes = ome_zarr_c::native_code::int_to_rgba_255_bytes(value);
    py::list result;
    for (const auto byte : bytes) {
        result.append(py::int_(byte));
    }
    return result;
}

py::list int_to_rgba(std::int32_t value) {
    const auto rgba = ome_zarr_c::native_code::int_to_rgba(value);
    py::list result;
    for (const auto channel : rgba) {
        result.append(py::float_(channel));
    }
    return result;
}

std::int32_t rgba_to_int(std::uint8_t r,
                         std::uint8_t g,
                         std::uint8_t b,
                         std::uint8_t a) {
    return ome_zarr_c::native_code::rgba_to_int(r, g, b, a);
}

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
        raise_overflow_error(exc.what());
    }
}

void dict_to_zarr(py::dict props_to_add,
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
        raise_plain_exception("zarr_path must be to plate.zarr or image.zarr");
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

void csv_to_zarr(const std::string& csv_path,
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
                        repr_object(csv_columns));
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

py::str strip_common_prefix(py::list parts) {
    std::vector<std::vector<std::string>> native_parts;
    native_parts.reserve(py::len(parts));
    for (const py::handle& part_handle : parts) {
        py::list part = py::cast<py::list>(part_handle);
        std::vector<std::string> native_part;
        native_part.reserve(py::len(part));
        for (const py::handle& token : part) {
            native_part.push_back(py::cast<std::string>(token));
        }
        native_parts.push_back(std::move(native_part));
    }

    try {
        const auto common = ome_zarr_c::native_code::strip_common_prefix(native_parts);
        for (std::size_t path_index = 0; path_index < native_parts.size(); ++path_index) {
            py::list trimmed;
            for (const auto& token : native_parts[path_index]) {
                trimmed.append(py::str(token));
            }
            parts[path_index] = trimmed;
        }
        return py::str(common);
    } catch (const std::runtime_error& exc) {
        const std::string message = exc.what();
        if (message.empty()) {
            raise_plain_exception(repr_joined_lines(parts));
        }
        raise_plain_exception(message);
    }
}

py::list splitall(py::object path) {
    py::object current = path;
    if (!py::isinstance<py::str>(path)) {
        py::object os_path = py::module_::import("os").attr("path");
        py::tuple parts = py::cast<py::tuple>(os_path.attr("split")(path));
        if (objects_equal(parts[0], path)) {
            py::list direct;
            direct.append(parts[0]);
            return direct;
        }
        if (objects_equal(parts[1], path)) {
            py::list direct;
            direct.append(parts[1]);
            return direct;
        }

        py::list result;
        const auto native_parts = ome_zarr_c::native_code::splitall(
            py::cast<std::string>(py::str(parts[0])));
        for (const auto& part : native_parts) {
            result.append(py::str(part));
        }
        result.append(py::str(parts[1]));
        return result;
    }

    py::list result;
    const auto native_parts =
        ome_zarr_c::native_code::splitall(py::cast<std::string>(py::str(current)));
    for (const auto& part : native_parts) {
        result.append(py::str(part));
    }
    return result;
}

py::list find_multiscales(py::object path_to_zattrs) {
    py::object builtins = py::module_::import("builtins");
    py::object json = py::module_::import("json");
    py::object logging = py::module_::import("logging");
    py::object os_path = py::module_::import("os").attr("path");
    py::object path_cls = py::module_::import("pathlib").attr("Path");
    py::object element_tree = py::module_::import("xml.etree.ElementTree");
    py::object logger = logging.attr("getLogger")(py::str("ome_zarr.utils"));

    py::object text = py::none();
    for (const char* name : {".zattrs", "zarr.json"}) {
        py::object candidate = true_divide(path_cls(path_to_zattrs), py::str(name));
        if (py::cast<bool>(candidate.attr("exists")())) {
            text = read_text_with_open(true_divide(path_to_zattrs, py::str(name)));
            break;
        }
    }

    if (text.is_none()) {
        builtins.attr("print")(
            py::str("No .zattrs or zarr.json found in {path_to_zattrs}"));
        return py::list();
    }

    py::dict zattrs = py::cast<py::dict>(json.attr("loads")(text));
    py::object attributes = zattrs.attr("get")("attributes", py::none());
    if (!attributes.is_none() &&
        py::cast<bool>(attributes.attr("__contains__")("ome"))) {
        zattrs = py::cast<py::dict>(attributes.attr("get")("ome"));
    }

    if (py::cast<bool>(zattrs.attr("__contains__")("plate"))) {
        py::dict plate = py::cast<py::dict>(zattrs.attr("get")("plate"));
        py::object wells = plate.attr("get")("wells");
        py::list wells_list = py::cast<py::list>(wells);
        if (py::len(wells_list) > 0) {
            py::dict first_well = py::cast<py::dict>(wells_list[0]);
            py::object path_to_zarr =
                true_divide(true_divide(path_to_zattrs, first_well.attr("get")("path")),
                            py::str("0"));
            py::list image;
            image.append(path_to_zarr);
            image.append(os_path.attr("basename")(path_to_zattrs));
            image.append(os_path.attr("dirname")(path_to_zattrs));

            py::list images;
            images.append(image);
            return images;
        }
        logger.attr("info")("No wells found in plate%s", path_to_zattrs);
        return py::list();
    }

    if (objects_equal(zattrs.attr("get")("bioformats2raw.layout"), py::int_(3))) {
        try {
            py::object metadata_xml =
                true_divide(true_divide(path_to_zattrs, py::str("OME")),
                            py::str("METADATA.ome.xml"));
            py::object tree = element_tree.attr("parse")(metadata_xml);
            py::object root = tree.attr("getroot")();

            py::list images;
            int series = 0;
            for (const py::handle& child_handle : root) {
                py::object child = py::reinterpret_borrow<py::object>(child_handle);
                if (!py::cast<bool>(child.attr("tag").attr("endswith")("Image"))) {
                    continue;
                }

                py::str default_name = py::str(
                    py::cast<std::string>(os_path.attr("basename")(path_to_zattrs)) +
                    " Series:" + std::to_string(series));
                py::object img_name = child.attr("attrib").attr("get")("Name", default_name);

                py::list image;
                image.append(true_divide(path_to_zattrs, py::str(std::to_string(series))));
                image.append(img_name);
                image.append(os_path.attr("dirname")(path_to_zattrs));
                images.append(image);
                series += 1;
            }
            return images;
        } catch (const py::error_already_set& ex) {
            builtins.attr("print")(ex.value());
        }
    }

    if (object_truthy(zattrs.attr("get")("multiscales"))) {
        py::list image;
        image.append(path_to_zattrs);
        image.append(os_path.attr("basename")(path_to_zattrs));
        image.append(os_path.attr("dirname")(path_to_zattrs));

        py::list images;
        images.append(image);
        return images;
    }

    return py::list();
}

py::list info_lines(py::object node, bool stats = false) {
    py::object dask = py::module_::import("dask");
    py::object logger =
        py::module_::import("logging").attr("getLogger")(py::str("ome_zarr.utils"));

    py::list lines;
    lines.append(py::str(node));

    py::object loc = node.attr("zarr");
    py::object zgroup = loc.attr("zgroup");
    py::object version = zgroup.attr("get")(py::str("version"));
    if (version.is_none()) {
        py::list fallback;
        fallback.append(py::dict());
        py::object multiscales = zgroup.attr("get")(py::str("multiscales"), fallback);
        py::object first = multiscales.attr("__getitem__")(0);
        version = first.attr("get")(py::str("version"), py::str(""));
    }
    lines.append(py::str(" - version: ") + py::str(version));
    lines.append(py::str(" - metadata"));

    for (const py::handle& spec_handle : node.attr("specs")) {
        py::object spec = py::reinterpret_borrow<py::object>(spec_handle);
        lines.append(py::str("   - ") +
                     py::str(spec.attr("__class__").attr("__name__")));
    }

    lines.append(py::str(" - data"));
    for (const py::handle& array_handle : node.attr("data")) {
        py::object array = py::reinterpret_borrow<py::object>(array_handle);
        py::str line = py::str("   - ") + py::str(array.attr("shape"));
        if (stats) {
            line = line + py::str(" minmax=") +
                   py::str(
                       dask.attr("compute")(array.attr("min")(), array.attr("max")()));
        }
        lines.append(line);
    }

    logger.attr("debug")(node.attr("data"));
    return lines;
}

py::list reader_matching_specs(const py::object& zarr) {
    py::dict root_attrs = py::cast<py::dict>(zarr.attr("root_attrs"));
    ome_zarr_c::native_code::ReaderSpecFlags flags{};
    flags.has_labels =
        py::cast<bool>(root_attrs.attr("__contains__")(py::str("labels")));
    flags.has_image_label =
        py::cast<bool>(root_attrs.attr("__contains__")(py::str("image-label")));
    flags.has_zgroup = object_truthy(zarr.attr("zgroup"));
    flags.has_multiscales =
        py::cast<bool>(root_attrs.attr("__contains__")(py::str("multiscales")));
    flags.has_omero =
        py::cast<bool>(root_attrs.attr("__contains__")(py::str("omero")));
    flags.has_plate =
        py::cast<bool>(root_attrs.attr("__contains__")(py::str("plate")));
    flags.has_well =
        py::cast<bool>(root_attrs.attr("__contains__")(py::str("well")));
    const auto native_matches = ome_zarr_c::native_code::reader_matching_specs(flags);

    py::list matches;
    for (const auto& match : native_matches) {
        matches.append(py::str(match));
    }
    return matches;
}

py::object reader_labels_names(const py::dict& root_attrs) {
    return root_attrs.attr("get")(py::str("labels"), py::list());
}

py::str reader_node_repr(const py::object& zarr, bool visible) {
    return py::str(ome_zarr_c::native_code::reader_node_repr(
        py::cast<std::string>(py::str(zarr)),
        visible));
}

py::dict reader_label_payload(const py::dict& root_attrs,
                              const py::object& name,
                              bool visible) {
    py::dict payload;
    py::object image_label = root_attrs.attr("get")(py::str("image-label"), py::dict());
    py::object source = image_label.attr("get")(py::str("source"), py::dict());
    payload["parent_image"] = source.attr("get")(py::str("image"), py::none());

    py::dict colors;
    py::object color_list = image_label.attr("get")(py::str("colors"), py::list());
    if (object_truthy(color_list)) {
        for (const py::handle& color_handle : color_list) {
            py::object color = py::reinterpret_borrow<py::object>(color_handle);
            try {
                py::object label_value = color.attr("__getitem__")(py::str("label-value"));
                py::object rgba = color.attr("get")(py::str("rgba"), py::none());
                if (!rgba.is_none() && object_truthy(rgba)) {
                    py::list normalized;
                    for (const py::handle& entry : rgba) {
                        normalized.append(true_divide(entry, py::int_(255)));
                    }
                    rgba = normalized;
                }

                if (PyBool_Check(label_value.ptr()) || PyLong_Check(label_value.ptr())) {
                    colors[label_value] = rgba;
                }
            } catch (...) {
            }
        }
    }

    py::dict properties;
    py::object props_list = image_label.attr("get")(py::str("properties"), py::list());
    if (object_truthy(props_list)) {
        for (const py::handle& props_handle : props_list) {
            py::object props = py::reinterpret_borrow<py::object>(props_handle);
            py::object label_value = props.attr("__getitem__")(py::str("label-value"));
            py::dict props_copy = py::dict(props);
            props_copy.attr("pop")(py::str("label-value"));
            properties[label_value] = props_copy;
        }
    }

    py::dict metadata;
    metadata["visible"] = py::bool_(visible);
    metadata["name"] = name;
    metadata["color"] = colors;

    py::dict nested_metadata;
    nested_metadata["image"] = root_attrs.attr("get")(py::str("image"), py::dict());
    nested_metadata["path"] = name;
    metadata["metadata"] = nested_metadata;

    payload["metadata"] = metadata;
    payload["properties"] = properties;
    return payload;
}

py::dict reader_multiscales_payload(const py::dict& root_attrs) {
    py::dict payload;
    py::list multiscales =
        py::cast<py::list>(root_attrs.attr("get")(py::str("multiscales"), py::list()));
    py::object first = multiscales[0];
    py::object datasets = first.attr("__getitem__")(py::str("datasets"));
    py::object axes = first.attr("get")(py::str("axes"), py::none());

    payload["version"] = first.attr("get")(py::str("version"), py::str("0.1"));
    payload["datasets"] = datasets;
    payload["axes"] = axes;
    payload["name"] = first.attr("get")(py::str("name"), py::none());

    std::vector<ome_zarr_c::native_code::ReaderMultiscalesDatasetInput> native_datasets;
    native_datasets.reserve(py::len(datasets));
    for (const py::handle& dataset_handle : datasets) {
        py::object dataset = py::reinterpret_borrow<py::object>(dataset_handle);
        py::object transform =
            dataset.attr("get")(py::str("coordinateTransformations"), py::none());
        ome_zarr_c::native_code::ReaderMultiscalesDatasetInput input{};
        input.path = py::cast<std::string>(dataset.attr("__getitem__")(py::str("path")));
        input.has_coordinate_transformations = !transform.is_none();
        native_datasets.push_back(std::move(input));
    }

    const auto plan = ome_zarr_c::native_code::reader_multiscales_plan(native_datasets);
    py::list paths;
    py::list transformations;
    for (const auto& dataset_handle : datasets) {
        py::object dataset = py::reinterpret_borrow<py::object>(dataset_handle);
        transformations.append(
            dataset.attr("get")(py::str("coordinateTransformations"), py::none()));
    }
    for (const auto& path : plan.paths) {
        paths.append(py::str(path));
    }
    payload["paths"] = paths;
    if (plan.any_coordinate_transformations) {
        payload["coordinateTransformations"] = transformations;
    }

    return payload;
}

py::dict reader_omero_payload(const py::dict& image_data, bool node_visible) {
    std::string model = "unknown";
    py::object rdefs = image_data.attr("get")(py::str("rdefs"), py::dict());
    if (object_truthy(rdefs)) {
        model = py::cast<std::string>(rdefs.attr("get")(py::str("model"), py::str("unset")));
    }

    py::object channels = image_data.attr("get")(py::str("channels"), py::none());
    const py::size_t channel_count = py::len(channels);

    py::list colormaps;
    py::list contrast_limits;
    py::list names;
    py::list visibles;
    for (py::size_t idx = 0; idx < channel_count; ++idx) {
        contrast_limits.append(py::none());
        names.append(py::str("channel_" + std::to_string(idx)));
        visibles.append(py::bool_(true));
    }

    py::object contrast_limits_value = contrast_limits;
    for (py::size_t idx = 0; idx < channel_count; ++idx) {
        py::object channel = channels.attr("__getitem__")(py::int_(idx));

        py::object color = channel.attr("get")(py::str("color"), py::none());
        py::object label = channel.attr("get")(py::str("label"), py::none());
        py::object active = channel.attr("get")(py::str("active"), py::none());
        py::object window = channel.attr("get")(py::str("window"), py::none());
        py::object start = py::none();
        py::object end = py::none();
        if (!window.is_none()) {
            start = window.attr("get")(py::str("start"), py::none());
            end = window.attr("get")(py::str("end"), py::none());
        }

        const auto plan = ome_zarr_c::native_code::reader_omero_channel_plan(
            model,
            !color.is_none(),
            color.is_none() ? "" : py::cast<std::string>(color),
            !label.is_none(),
            label.is_none() ? "" : py::cast<std::string>(label),
            !active.is_none(),
            !active.is_none() && object_truthy(active),
            !window.is_none(),
            !start.is_none(),
            !end.is_none());

        if (plan.has_color) {
            py::list rgb;
            if (plan.force_greyscale_rgb) {
                rgb.append(py::int_(1));
                rgb.append(py::int_(1));
                rgb.append(py::int_(1));
            } else {
                for (const double component : plan.rgb) {
                    rgb.append(py::float_(component));
                }
            }
            py::list colormap;
            py::list zero_rgb;
            zero_rgb.append(py::int_(0));
            zero_rgb.append(py::int_(0));
            zero_rgb.append(py::int_(0));
            colormap.append(zero_rgb);
            colormap.append(rgb);
            colormaps.append(colormap);
        }

        if (plan.has_label) {
            names[idx] = label;
        }

        if (!active.is_none()) {
            if (plan.visible_mode ==
                ome_zarr_c::native_code::ReaderVisibleMode::node_visible_if_active) {
                visibles[idx] = py::bool_(node_visible);
            } else if (
                plan.visible_mode ==
                ome_zarr_c::native_code::ReaderVisibleMode::keep_raw_active) {
                visibles[idx] = active;
            }
        }

        if (!window.is_none()) {
            if (!plan.has_complete_window) {
                contrast_limits_value = py::none();
            } else if (!contrast_limits_value.is_none()) {
                py::list limits;
                limits.append(start);
                limits.append(end);
                contrast_limits[idx] = limits;
            }
        }
    }

    py::dict metadata;
    metadata["channel_names"] = names;
    metadata["visible"] = visibles;
    metadata["contrast_limits"] = contrast_limits_value;
    metadata["colormap"] = colormaps;
    return metadata;
}

py::object get_valid_axes(
    py::object ndim = py::none(),
    py::object axes = py::none(),
    py::object fmt = py::none()) {
    py::object builtins = py::module_::import("builtins");
    py::object logger = py::module_::import("logging").attr("getLogger")(
        py::str("ome_zarr.writer"));

    const std::string version = py::cast<std::string>(fmt.attr("version"));
    std::optional<std::int64_t> native_ndim;
    if (!ndim.is_none()) {
        native_ndim = py::cast<std::int64_t>(ndim);
    }

    try {
        const auto plan = ome_zarr_c::native_code::get_valid_axes_plan(
            version,
            !axes.is_none(),
            native_ndim);
        if (plan.log_ignored_axes) {
            logger.attr("info")("axes ignored for version 0.1 or 0.2");
        }
        if (plan.return_none) {
            return py::none();
        }
        if (axes.is_none()) {
            py::list guessed_axes;
            for (const auto& axis_name : plan.axes) {
                guessed_axes.append(py::str(axis_name));
            }
            axes = guessed_axes;
        }
        if (plan.log_auto_axes) {
            logger.attr("info")(
                "Auto using axes %s for " + plan.auto_label + " data",
                axes);
        }
    } catch (const std::invalid_argument& exc) {
        throw py::value_error(exc.what());
    }

    if (py::isinstance<py::str>(axes)) {
        axes = builtins.attr("list")(axes);
    }

    if (native_ndim.has_value()) {
        try {
            ome_zarr_c::native_code::validate_axes_length(
                static_cast<std::size_t>(py::len(axes)),
                native_ndim.value());
        } catch (const std::invalid_argument& exc) {
            throw py::value_error(exc.what());
        }
    }

    py::object axes_cls = py::module_::import("ome_zarr_c.axes").attr("Axes");
    py::object axes_obj = axes_cls(axes, fmt);
    return axes_obj.attr("to_list")(fmt);
}

py::tuple extract_dims_from_axes(py::object axes = py::none()) {
    if (axes.is_none()) {
        return py::make_tuple("t", "c", "z", "y", "x");
    }

    bool all_strings = true;
    for (const py::handle& axis_handle : py::iterable(axes)) {
        if (!py::isinstance<py::str>(axis_handle)) {
            all_strings = false;
            break;
        }
    }
    if (all_strings) {
        py::list names;
        for (const py::handle& axis_handle : py::iterable(axes)) {
            names.append(py::str(axis_handle));
        }
        return py::tuple(names);
    }

    bool all_named_dicts = true;
    for (const py::handle& axis_handle : py::iterable(axes)) {
        if (!py::isinstance<py::dict>(axis_handle)) {
            all_named_dicts = false;
            break;
        }
        py::dict axis = py::cast<py::dict>(axis_handle);
        if (!axis.contains("name")) {
            all_named_dicts = false;
            break;
        }
    }
    if (all_named_dicts) {
        py::list names;
        const auto native_names = ome_zarr_c::native_code::extract_dims_from_axes(
            axis_records_from_sequence(py::cast<py::sequence>(axes)));
        for (const auto& name : native_names) {
            names.append(py::str(name));
        }
        return py::tuple(names);
    }

    throw py::type_error(
        "`axes` must be a list of strings or a list of dicts containing 'name'");
}

py::tuple retuple(py::object chunks, py::object shape) {
    if (PyLong_Check(chunks.ptr())) {
        const py::size_t shape_len = py::len(shape);
        py::tuple result(shape_len);
        for (py::size_t index = 0; index < shape_len; ++index) {
            result[index] = chunks;
        }
        return result;
    }

    const py::ssize_t dims_to_add = py::len(shape) - py::len(chunks);
    py::object prefix = py::reinterpret_borrow<py::object>(shape).attr("__getitem__")(
        py::slice(0, dims_to_add, 1));
    py::tuple chunk_tuple = py::tuple(
        py::module_::import("builtins").attr("tuple")(chunks));

    py::ssize_t prefix_size = 0;
    for (const py::handle& value : py::iterable(prefix)) {
        static_cast<void>(value);
        ++prefix_size;
    }

    py::tuple result(prefix_size + py::len(chunk_tuple));
    py::ssize_t index = 0;
    for (const py::handle& value : py::iterable(prefix)) {
        result[index++] = py::reinterpret_borrow<py::object>(value);
    }
    for (const py::handle& value : chunk_tuple) {
        result[index++] = py::reinterpret_borrow<py::object>(value);
    }
    return result;
}

py::list validate_well_images(py::object images, py::object fmt = py::none()) {
    static_cast<void>(fmt);
    py::object logger = py::module_::import("logging").attr("getLogger")(
        py::str("ome_zarr.writer"));
    py::list validated_images;

    for (const py::handle& image_handle : py::iterable(images)) {
        py::object image = py::reinterpret_borrow<py::object>(image_handle);
        ome_zarr_c::native_code::WellImageInput input{};
        input.is_string = py::isinstance<py::str>(image);
        input.is_dict = py::isinstance<py::dict>(image);
        input.repr = py::cast<std::string>(py::str(image));
        if (input.is_string) {
            input.path = input.repr;
        } else if (input.is_dict) {
            py::dict image_dict = py::cast<py::dict>(image);
            input.has_path = image_dict.contains("path");
            if (input.has_path) {
                input.path_is_string = py::isinstance<py::str>(image_dict["path"]);
                if (input.path_is_string) {
                    input.path = py::cast<std::string>(image_dict["path"]);
                }
            }
            input.has_acquisition = image_dict.contains("acquisition");
            if (input.has_acquisition) {
                input.acquisition_is_int = PyLong_Check(image_dict["acquisition"].ptr());
            }
            for (const auto& key_value : image_dict) {
                const py::handle key_handle = key_value.first;
                if (!objects_equal(key_handle, py::str("acquisition")) &&
                    !objects_equal(key_handle, py::str("path"))) {
                    input.has_unexpected_key = true;
                    break;
                }
            }
        }

        try {
            const auto validated = ome_zarr_c::native_code::validate_well_image(input);
            if (validated.has_unexpected_key) {
                logger.attr("debug")("%s contains unspecified keys", image);
            }
            if (validated.materialize) {
                py::dict validated_image;
                validated_image["path"] = py::str(validated.path);
                validated_images.append(validated_image);
            } else {
                validated_images.append(image);
            }
        } catch (const std::invalid_argument& exc) {
            throw py::value_error(exc.what());
        }
    }

    return validated_images;
}

py::object validate_plate_acquisitions(
    py::object acquisitions,
    py::object fmt = py::none()) {
    static_cast<void>(fmt);
    py::object logger = py::module_::import("logging").attr("getLogger")(
        py::str("ome_zarr.writer"));

    for (const py::handle& acquisition_handle : py::iterable(acquisitions)) {
        py::object acquisition = py::reinterpret_borrow<py::object>(acquisition_handle);
        ome_zarr_c::native_code::PlateAcquisitionInput input{};
        input.is_dict = py::isinstance<py::dict>(acquisition);
        input.repr = py::cast<std::string>(py::str(acquisition));
        if (input.is_dict) {
            py::dict acquisition_dict = py::cast<py::dict>(acquisition);
            input.has_id = acquisition_dict.contains("id");
            if (input.has_id) {
                input.id_is_int = PyLong_Check(acquisition_dict["id"].ptr());
            }
            for (const auto& key_value : acquisition_dict) {
                const py::handle key_handle = key_value.first;
                if (!objects_equal(key_handle, py::str("id")) &&
                    !objects_equal(key_handle, py::str("name")) &&
                    !objects_equal(key_handle, py::str("maximumfieldcount")) &&
                    !objects_equal(key_handle, py::str("description")) &&
                    !objects_equal(key_handle, py::str("starttime")) &&
                    !objects_equal(key_handle, py::str("endtime"))) {
                    input.has_unexpected_key = true;
                    break;
                }
            }
        }

        try {
            ome_zarr_c::native_code::validate_plate_acquisition(input);
            if (input.has_unexpected_key) {
                logger.attr("debug")("%s contains unspecified keys", acquisition);
            }
        } catch (const std::invalid_argument& exc) {
            throw py::value_error(exc.what());
        }
    }

    return acquisitions;
}

py::list validate_plate_rows_columns(
    py::object rows_or_columns,
    py::object fmt = py::none()) {
    static_cast<void>(fmt);
    std::vector<std::string> native_values;
    native_values.reserve(py::len(rows_or_columns));
    for (const py::handle& element_handle : py::iterable(rows_or_columns)) {
        py::object element = py::reinterpret_borrow<py::object>(element_handle);
        if (!py::isinstance<py::str>(element)) {
            static_cast<void>(element.attr("isalnum")());
        }
        native_values.push_back(py::cast<std::string>(element));
    }

    try {
        const auto validated = ome_zarr_c::native_code::validate_plate_rows_columns(
            native_values,
            py::cast<std::string>(py::str(rows_or_columns)));
        py::list validated_list;
        for (const auto& element : validated) {
            py::dict validated_element;
            validated_element["name"] = py::str(element);
            validated_list.append(validated_element);
        }
        return validated_list;
    } catch (const std::invalid_argument& exc) {
        throw py::value_error(exc.what());
    }
}

py::object validate_datasets(
    py::object datasets,
    py::object dims,
    py::object fmt = py::none()) {
    std::vector<ome_zarr_c::native_code::DatasetInput> native_datasets;
    if (!datasets.is_none()) {
        native_datasets.reserve(py::len(datasets));
        for (const py::handle& dataset_handle : py::iterable(datasets)) {
            py::object dataset = py::reinterpret_borrow<py::object>(dataset_handle);
            ome_zarr_c::native_code::DatasetInput input{};
            input.is_dict = py::isinstance<py::dict>(dataset);
            input.repr = py::cast<std::string>(py::str(dataset));
            if (input.is_dict) {
                py::dict dataset_dict = py::cast<py::dict>(dataset);
                py::object path = dataset_dict.attr("get")("path");
                input.path_truthy = object_truthy(path);
                input.has_transformation =
                    !dataset_dict.attr("get")("coordinateTransformations").is_none();
            }
            native_datasets.push_back(std::move(input));
        }
    }

    try {
        const auto transformation_indices =
            ome_zarr_c::native_code::validate_datasets(native_datasets);
        py::list transformations;
        for (const auto index : transformation_indices) {
            py::object dataset = datasets.attr("__getitem__")(py::int_(index));
            transformations.append(
                dataset.attr("get")(py::str("coordinateTransformations")));
        }

        fmt.attr("validate_coordinate_transformations")(
            dims, py::len(datasets), transformations);
        return datasets;
    } catch (const std::invalid_argument& exc) {
        throw py::value_error(exc.what());
    }
}

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
    if (object_truthy(storage_options)) {
        if (!py::isinstance<py::list>(storage_options)) {
            options = py::cast<py::dict>(storage_options.attr("copy")());
        } else {
            return storage_options.attr("__getitem__")(path);
        }
    }
    return options;
}

py::tuple better_chunksize(py::object image, py::object factors) {
    py::object numpy = py::module_::import("numpy");
    py::object numpy_int64 = numpy.attr("int64");
    std::vector<std::int64_t> native_chunksize;
    std::vector<double> native_factors;

    for (const py::handle& chunk : image.attr("chunksize")) {
        native_chunksize.push_back(py::cast<std::int64_t>(chunk));
    }
    for (const py::handle& factor : factors) {
        native_factors.push_back(py::cast<double>(factor));
    }

    const auto [better_chunks_native, block_output_native] =
        ome_zarr_c::native_code::better_chunksize(native_chunksize, native_factors);

    py::tuple better_chunks(better_chunks_native.size());
    py::tuple block_output(block_output_native.size());
    for (py::size_t index = 0; index < better_chunks_native.size(); ++index) {
        better_chunks[index] = numpy_int64(better_chunks_native[index]);
        block_output[index] = numpy_int64(block_output_native[index]);
    }
    return py::make_tuple(better_chunks, block_output);
}

py::object dask_resize(
    py::object image,
    py::object output_shape,
    py::args args,
    py::kwargs kwargs) {
    py::object numpy = py::module_::import("numpy");
    py::object dask_array = py::module_::import("dask.array");
    py::object skimage_transform = py::module_::import("skimage.transform");
    py::object float_type = py::module_::import("builtins").attr("float");

    py::object factors = numpy.attr("divide")(
        numpy.attr("array")(output_shape),
        numpy.attr("array")(image.attr("shape")).attr("astype")(float_type));
    py::tuple chunk_info = better_chunksize(image, factors);
    py::object image_prepared = image.attr("rechunk")(chunk_info[0]);
    py::tuple extra_args = py::reinterpret_borrow<py::tuple>(args);
    py::dict call_kwargs = py::reinterpret_borrow<py::dict>(kwargs);

    py::object resize_block = py::cpp_function(
        [factors, skimage_transform, extra_args, call_kwargs](
            py::object image_block,
            py::object block_info = py::none()) {
            static_cast<void>(block_info);
            py::object numpy_inner = py::module_::import("numpy");
            py::object int_type_inner = py::module_::import("builtins").attr("int");
            py::object chunk_output_shape = py::tuple(
                numpy_inner
                    .attr("ceil")(
                        numpy_inner.attr("multiply")(
                            numpy_inner.attr("array")(image_block.attr("shape")),
                            factors))
                    .attr("astype")(int_type_inner));
            py::object resized = call_callable(
                skimage_transform.attr("resize"),
                {image_block, chunk_output_shape},
                extra_args,
                call_kwargs);
            return resized.attr("astype")(image_block.attr("dtype"));
        },
        py::arg("image_block"),
        py::arg("block_info") = py::none());

    py::object output = dask_array
                            .attr("map_blocks")(
                                resize_block,
                                image_prepared,
                                py::arg("dtype") = image.attr("dtype"),
                                py::arg("chunks") = chunk_info[1])
                            .attr("__getitem__")(output_slices_for_shape(output_shape));
    return output.attr("rechunk")(image.attr("chunksize"))
        .attr("astype")(image.attr("dtype"));
}

py::object dask_local_mean(
    py::object image,
    py::object output_shape,
    py::args args,
    py::kwargs kwargs) {
    py::object numpy = py::module_::import("numpy");
    py::object dask_array = py::module_::import("dask.array");
    py::object float_type = py::module_::import("builtins").attr("float");
    py::object int_type = py::module_::import("builtins").attr("int");
    py::object downscale_local_mean =
        py::module_::import("skimage.transform").attr("downscale_local_mean");

    py::object factors = numpy.attr("divide")(
        numpy.attr("array")(image.attr("shape")).attr("astype")(float_type),
        numpy.attr("array")(output_shape));
    py::tuple chunk_info = better_chunksize(
        image, numpy.attr("divide")(1, factors));
    py::object image_prepared = image.attr("rechunk")(chunk_info[0]);
    py::tuple extra_args = py::reinterpret_borrow<py::tuple>(args);
    py::dict call_kwargs = py::reinterpret_borrow<py::dict>(kwargs);
    py::object factor_tuple = py::tuple(factors.attr("astype")(int_type));

    py::object local_mean_block = py::cpp_function(
        [downscale_local_mean, factor_tuple, extra_args, call_kwargs](
            py::object image_block) {
            py::object reduced = call_callable(
                downscale_local_mean,
                {image_block, factor_tuple},
                extra_args,
                call_kwargs);
            return reduced.attr("astype")(image_block.attr("dtype"));
        },
        py::arg("image_block"));

    py::object output = dask_array
                            .attr("map_blocks")(
                                local_mean_block,
                                image_prepared,
                                py::arg("dtype") = image.attr("dtype"),
                                py::arg("chunks") = chunk_info[1])
                            .attr("__getitem__")(output_slices_for_shape(output_shape));
    return output.attr("rechunk")(image.attr("chunksize"))
        .attr("astype")(image.attr("dtype"));
}

py::object dask_zoom(
    py::object image,
    py::object output_shape,
    py::args args,
    py::kwargs kwargs) {
    static_cast<void>(args);
    static_cast<void>(kwargs);
    py::object numpy = py::module_::import("numpy");
    py::object dask_array = py::module_::import("dask.array");
    py::object float_type = py::module_::import("builtins").attr("float");
    py::object scipy_zoom = py::module_::import("scipy.ndimage").attr("zoom");

    py::object factors = numpy.attr("divide")(
        numpy.attr("array")(image.attr("shape")).attr("astype")(float_type),
        numpy.attr("array")(output_shape));
    py::object inverse_factors = numpy.attr("divide")(1, factors);
    py::tuple chunk_info = better_chunksize(image, inverse_factors);
    py::object image_prepared = image.attr("rechunk")(chunk_info[0]);

    py::object zoom_block = py::cpp_function(
        [scipy_zoom, inverse_factors](py::object image_block) {
            py::object zoomed = scipy_zoom(
                image_block, inverse_factors, py::arg("order") = 1);
            return zoomed.attr("astype")(image_block.attr("dtype"));
        },
        py::arg("image_block"));

    py::tuple image_shape = py::cast<py::tuple>(image.attr("shape"));
    py::tuple factors_tuple = py::cast<py::tuple>(factors);
    py::tuple resized_output_shape(py::len(image_shape));
    for (py::size_t index = 0; index < py::len(image_shape); ++index) {
        resized_output_shape[index] =
            floor_divide(image_shape[index], factors_tuple[index]);
    }

    py::object output = dask_array
                            .attr("map_blocks")(
                                zoom_block,
                                image_prepared,
                                py::arg("dtype") = image.attr("dtype"),
                                py::arg("chunks") = chunk_info[1])
                            .attr("__getitem__")(
                                output_slices_for_shape(resized_output_shape));
    return output.attr("rechunk")(image.attr("chunksize"))
        .attr("astype")(image.attr("dtype"));
}

py::object downscale_nearest_dask(py::object image, py::object factors) {
    py::tuple factor_tuple = py::tuple(factors);
    py::tuple shape = py::cast<py::tuple>(image.attr("shape"));
    const py::ssize_t factor_count = py::len(factor_tuple);
    const py::ssize_t ndim = py::cast<py::ssize_t>(image.attr("ndim"));

    if (factor_count != ndim) {
        throw py::value_error(
            "Dimension mismatch: " + py::cast<std::string>(py::str(image.attr("ndim"))) +
            " image dimensions, " + std::to_string(factor_count) +
            " scale factors");
    }

    for (py::size_t index = 0; index < static_cast<py::size_t>(factor_count); ++index) {
        const py::handle factor = factor_tuple[index];
        const py::handle dim = shape[index];
        const bool valid = PyLong_Check(factor.ptr()) &&
                           rich_compare_bool(factor, py::int_(0), Py_GT) &&
                           rich_compare_bool(factor, dim, Py_LE);
        if (!valid) {
            throw py::value_error(
                "All scale factors must not be greater than the dimension length: ("
                + py::cast<std::string>(py::str(factor_tuple)) + ") <= (" +
                py::cast<std::string>(py::str(shape)) + ")");
        }
    }

    py::tuple slices(static_cast<py::size_t>(factor_count));
    for (py::size_t index = 0; index < static_cast<py::size_t>(factor_count); ++index) {
        slices[index] = py::slice(py::none(), py::none(), factor_tuple[index]);
    }
    return image.attr("__getitem__")(slices);
}

py::list build_pyramid(
    py::object image,
    py::object scale_factors,
    py::object dims,
    py::object method = py::str("nearest"),
    py::object chunks = py::none()) {
    py::object numpy = py::module_::import("numpy");
    py::object dask_array = py::module_::import("dask.array");
    py::object builtins = py::module_::import("builtins");
    py::object warnings = py::module_::import("warnings");
    py::object core = py::module_::import("ome_zarr_c._core");
    py::tuple dims_tuple = py::tuple(dims);
    std::vector<std::string> native_dims;
    native_dims.reserve(py::len(dims_tuple));
    for (const py::handle& dim_handle : dims_tuple) {
        native_dims.push_back(py::cast<std::string>(dim_handle));
    }

    if (py::isinstance(image, numpy.attr("ndarray"))) {
        if (!chunks.is_none()) {
            image = dask_array.attr("from_array")(image, py::arg("chunks") = chunks);
        } else {
            image = dask_array.attr("from_array")(image);
        }
    }

    std::string method_key;
    if (py::isinstance<py::str>(method)) {
        method_key = py::cast<std::string>(method);
    } else if (py::hasattr(method, "value") &&
               py::isinstance<py::str>(method.attr("value"))) {
        method_key = py::cast<std::string>(method.attr("value"));
    }

    bool all_int_scale_factors =
        py::isinstance<py::list>(scale_factors) || py::isinstance<py::tuple>(scale_factors);
    if (all_int_scale_factors) {
        for (const py::handle& scale_factor : py::iterable(scale_factors)) {
            if (!PyLong_Check(scale_factor.ptr())) {
                all_int_scale_factors = false;
                break;
            }
        }
    }

    std::vector<ome_zarr_c::native_code::ScaleLevel> native_scale_levels;
    if (all_int_scale_factors) {
        native_scale_levels = ome_zarr_c::native_code::scale_levels_from_ints(
            native_dims,
            static_cast<std::size_t>(py::len(scale_factors)));
    } else {
        std::vector<std::map<std::string, double>> input_levels;
        input_levels.reserve(py::len(scale_factors));
        for (py::ssize_t index = 0; index < py::len(scale_factors); ++index) {
            py::object level = scale_factors.attr("__getitem__")(index);
            py::dict reordered_level;
            std::map<std::string, double> native_level;
            for (const py::handle& dim_handle : dims_tuple) {
                py::object value = level.attr("get")(dim_handle, py::int_(1));
                reordered_level[dim_handle] = value;
                native_level[py::cast<std::string>(dim_handle)] = py::cast<double>(value);
            }
            set_item(scale_factors, py::int_(index), reordered_level);
            input_levels.push_back(std::move(native_level));
        }
        native_scale_levels = ome_zarr_c::native_code::reorder_scale_levels(
            native_dims,
            input_levels);
    }

    std::vector<std::int64_t> base_shape;
    py::tuple image_shape = py::cast<py::tuple>(image.attr("shape"));
    base_shape.reserve(py::len(image_shape));
    for (const py::handle& dim : image_shape) {
        base_shape.push_back(py::cast<std::int64_t>(dim));
    }
    const auto pyramid_plan = ome_zarr_c::native_code::build_pyramid_plan(
        base_shape,
        native_dims,
        native_scale_levels);

    py::list images;
    images.append(image);

    for (const auto& plan : pyramid_plan) {
        for (const auto& warning_dim : plan.warning_dims) {
            warnings.attr("warn")(
                "Dimension " + warning_dim + " is too small to downsample further.",
                builtins.attr("UserWarning"),
                py::arg("stacklevel") = 3);
        }

        py::tuple target_shape(plan.target_shape.size());
        for (py::size_t dim_index = 0; dim_index < plan.target_shape.size(); ++dim_index) {
            target_shape[dim_index] = py::int_(plan.target_shape[dim_index]);
        }

        py::object current_image = images[py::len(images) - 1];
        py::object new_image;
        if (method_key == "resize") {
            new_image = core.attr("resize")(
                current_image,
                target_shape,
                py::arg("order") = 1,
                py::arg("mode") = "reflect",
                py::arg("anti_aliasing") = true,
                py::arg("preserve_range") = true);
        } else if (method_key == "nearest") {
            new_image = core.attr("resize")(
                current_image,
                target_shape,
                py::arg("order") = 0,
                py::arg("mode") = "reflect",
                py::arg("anti_aliasing") = false,
                py::arg("preserve_range") = true);
        } else if (method_key == "local_mean") {
            new_image = core.attr("local_mean")(current_image, target_shape);
        } else if (method_key == "zoom") {
            new_image = core.attr("zoom")(current_image, target_shape);
        } else {
            throw py::value_error(
                "Unknown downsampling method: " +
                py::cast<std::string>(py::str(method)));
        }

        images.append(new_image);
    }

    return images;
}

py::object scaler_resize_image(
    py::object image,
    py::int_ downscale = py::int_(2),
    py::int_ order = py::int_(1)) {
    py::object dask_array = py::module_::import("dask.array");
    py::object skimage_transform = py::module_::import("skimage.transform");
    py::object builtins = py::module_::import("builtins");

    py::object resize_func = skimage_transform.attr("resize");
    if (py::isinstance(image, dask_array.attr("Array"))) {
        resize_func = py::module_::import("ome_zarr_c._core").attr("resize");
    }

    py::tuple image_shape = py::cast<py::tuple>(image.attr("shape"));
    py::tuple out_shape(py::len(image_shape));
    for (py::size_t index = 0; index < py::len(image_shape); ++index) {
        out_shape[index] = image_shape[index];
    }
    out_shape[py::len(out_shape) - 1] =
        floor_divide(image_shape[py::len(image_shape) - 1], downscale);
    out_shape[py::len(out_shape) - 2] =
        floor_divide(image_shape[py::len(image_shape) - 2], downscale);

    py::object dtype = image.attr("dtype");
    py::object resized = resize_func(
        image.attr("astype")(builtins.attr("float")),
        out_shape,
        py::arg("order") = order,
        py::arg("mode") = "reflect",
        py::arg("anti_aliasing") = false);
    return resized.attr("astype")(dtype);
}

py::list scaler_by_plane(
    py::object base,
    const std::function<py::object(py::object, py::ssize_t, py::ssize_t)>& transform,
    py::int_ max_layer = py::int_(4)) {
    py::object numpy = py::module_::import("numpy");
    py::list rv;
    rv.append(base);

    for (py::ssize_t level_index = 0; level_index < max_layer; ++level_index) {
        py::object stack_to_scale = rv[py::len(rv) - 1];
        const py::ssize_t stack_ndim = py::cast<py::ssize_t>(stack_to_scale.attr("ndim"));
        py::tuple stack_shape = py::cast<py::tuple>(stack_to_scale.attr("shape"));

        std::array<py::ssize_t, 5> shape_5d = {1, 1, 1, 1, 1};
        for (py::ssize_t dim_index = 0; dim_index < stack_ndim; ++dim_index) {
            shape_5d[5 - stack_ndim + dim_index] = py::cast<py::ssize_t>(stack_shape[dim_index]);
        }

        const py::ssize_t T = shape_5d[0];
        const py::ssize_t C = shape_5d[1];
        const py::ssize_t Z = shape_5d[2];
        const py::ssize_t Y = shape_5d[3];
        const py::ssize_t X = shape_5d[4];

        if (stack_ndim == 2) {
            rv.append(transform(stack_to_scale, Y, X));
            continue;
        }

        const py::ssize_t stack_dims = stack_ndim - 2;
        py::object new_stack = py::none();

        for (py::ssize_t t = 0; t < T; ++t) {
            for (py::ssize_t c = 0; c < C; ++c) {
                for (py::ssize_t z = 0; z < Z; ++z) {
                    const std::array<py::ssize_t, 3> indices = {t, c, z};
                    py::tuple dims_to_slice(stack_dims);
                    for (py::ssize_t dim_index = 0; dim_index < stack_dims; ++dim_index) {
                        dims_to_slice[dim_index] = py::int_(indices[3 - stack_dims + dim_index]);
                    }

                    py::object plane = stack_to_scale.attr("__getitem__")(dims_to_slice);
                    py::object out = transform(plane, Y, X);

                    if (new_stack.is_none()) {
                        py::tuple out_shape = py::cast<py::tuple>(out.attr("shape"));
                        py::tuple new_shape(stack_dims + 2);
                        for (py::ssize_t dim_index = 0; dim_index < stack_dims; ++dim_index) {
                            new_shape[dim_index] =
                                py::int_(shape_5d[3 - stack_dims + dim_index]);
                        }
                        new_shape[stack_dims] = out_shape[0];
                        new_shape[stack_dims + 1] = out_shape[1];
                        new_stack = numpy.attr("zeros")(
                            new_shape, py::arg("dtype") = base.attr("dtype"));
                    }

                    set_item(new_stack, dims_to_slice, out);
                }
            }
        }

        rv.append(new_stack);
    }

    return rv;
}

py::object scaler_nearest_plane(
    py::object plane,
    py::ssize_t size_y,
    py::ssize_t size_x,
    py::int_ downscale = py::int_(2)) {
    py::object dask_array = py::module_::import("dask.array");
    py::object resize_func = py::module_::import("skimage.transform").attr("resize");
    if (py::isinstance(plane, dask_array.attr("Array"))) {
        resize_func = py::module_::import("ome_zarr_c._core").attr("resize");
    }

    py::tuple output_shape(2);
    output_shape[0] = floor_divide(py::int_(size_y), downscale);
    output_shape[1] = floor_divide(py::int_(size_x), downscale);

    return resize_func(
               plane,
               output_shape,
               py::arg("order") = 0,
               py::arg("preserve_range") = true,
               py::arg("anti_aliasing") = false)
        .attr("astype")(plane.attr("dtype"));
}

py::list scaler_nearest(
    py::object base,
    py::int_ downscale = py::int_(2),
    py::int_ max_layer = py::int_(4)) {
    return scaler_by_plane(
        base,
        [downscale](py::object plane, py::ssize_t size_y, py::ssize_t size_x) {
            return scaler_nearest_plane(plane, size_y, size_x, downscale);
        },
        max_layer);
}

py::list scaler_gaussian(
    py::object base,
    py::int_ downscale = py::int_(2),
    py::int_ max_layer = py::int_(4)) {
    py::object pyramid = py::module_::import("skimage.transform").attr("pyramid_gaussian")(
        base,
        py::arg("downscale") = downscale,
        py::arg("max_layer") = max_layer,
        py::arg("channel_axis") = py::none());

    py::list result;
    py::object dtype = base.attr("dtype");
    for (const py::handle& level : py::iterable(pyramid)) {
        result.append(py::reinterpret_borrow<py::object>(level).attr("astype")(dtype));
    }
    return result;
}

py::list scaler_laplacian(
    py::object base,
    py::int_ downscale = py::int_(2),
    py::int_ max_layer = py::int_(4)) {
    py::object pyramid =
        py::module_::import("skimage.transform").attr("pyramid_laplacian")(
            base,
            py::arg("downscale") = downscale,
            py::arg("max_layer") = max_layer,
            py::arg("channel_axis") = py::none());

    py::list result;
    py::object dtype = base.attr("dtype");
    for (const py::handle& level : py::iterable(pyramid)) {
        result.append(py::reinterpret_borrow<py::object>(level).attr("astype")(dtype));
    }
    return result;
}

py::list scaler_local_mean(
    py::object base,
    py::int_ downscale = py::int_(2),
    py::int_ max_layer = py::int_(4)) {
    py::object downscale_local_mean =
        py::module_::import("skimage.transform").attr("downscale_local_mean");
    py::list rv;
    rv.append(base);

    const py::ssize_t stack_dims = py::cast<py::ssize_t>(base.attr("ndim")) - 2;
    py::tuple factors(stack_dims + 2);
    for (py::ssize_t index = 0; index < stack_dims; ++index) {
        factors[index] = py::int_(1);
    }
    factors[stack_dims] = downscale;
    factors[stack_dims + 1] = downscale;

    for (py::ssize_t level_index = 0; level_index < max_layer; ++level_index) {
        py::object next_level = downscale_local_mean(rv[py::len(rv) - 1], py::arg("factors") = factors)
                                    .attr("astype")(base.attr("dtype"));
        rv.append(next_level);
    }

    return rv;
}

py::list scaler_zoom(
    py::object base,
    py::int_ downscale = py::int_(2),
    py::int_ max_layer = py::int_(4)) {
    py::object scipy_zoom = py::module_::import("scipy.ndimage").attr("zoom");
    py::list rv;
    rv.append(base);
    py::print(base.attr("shape"));
    for (py::ssize_t level_index = 0; level_index < max_layer; ++level_index) {
        py::print(level_index, downscale);
        const long zoom_factor = static_cast<long>(
            std::pow(py::cast<long>(downscale), static_cast<long>(level_index)));
        rv.append(scipy_zoom(base, py::int_(zoom_factor)));
        py::print(rv[py::len(rv) - 1].attr("shape"));
    }

    py::list reversed_result;
    for (py::ssize_t index = py::len(rv) - 1; index >= 0; --index) {
        reversed_result.append(rv[index]);
        if (index == 0) {
            break;
        }
    }
    return reversed_result;
}

void data_make_circle(
    py::int_ height,
    py::int_ width,
    py::object value,
    py::object target) {
    const auto points = ome_zarr_c::native_code::circle_points(
        py::cast<std::size_t>(height),
        py::cast<std::size_t>(width));
    for (const auto& point : points) {
        set_item(target, py::make_tuple(point.y, point.x), value);
    }
}

py::object data_rgb_to_5d(py::object pixels) {
    py::object numpy = py::module_::import("numpy");
    py::tuple pixel_shape = py::cast<py::tuple>(pixels.attr("shape"));
    std::vector<std::size_t> native_shape;
    native_shape.reserve(py::len(pixel_shape));
    for (const py::handle& dim : pixel_shape) {
        native_shape.push_back(py::cast<std::size_t>(dim));
    }

    try {
        const auto channel_order = ome_zarr_c::native_code::rgb_channel_order(native_shape);
        if (native_shape.size() == 2) {
            py::object stack = numpy.attr("array")(py::make_tuple(pixels));
            py::object channels = numpy.attr("array")(py::make_tuple(stack));
            return numpy.attr("array")(py::make_tuple(channels));
        }

        py::list channels;
        for (const auto channel_index : channel_order) {
            py::object channel = pixels.attr("__getitem__")(
                py::make_tuple(py::slice(py::none(), py::none(), py::none()),
                               py::slice(py::none(), py::none(), py::none()),
                               py::int_(channel_index)));
            channels.append(numpy.attr("array")(py::make_tuple(channel)));
        }
        return numpy.attr("array")(py::make_tuple(channels));
    } catch (const std::invalid_argument&) {
        PyErr_SetString(
            PyExc_AssertionError,
            ("expecting 2 or 3d: (" + py::cast<std::string>(py::str(pixels.attr("shape"))) +
             ")")
                .c_str());
        throw py::error_already_set();
    }
}

py::tuple data_coins() {
    py::object scipy_zoom = py::module_::import("scipy.ndimage").attr("zoom");
    py::object skimage_data = py::module_::import("skimage.data");
    py::object threshold_otsu =
        py::module_::import("skimage.filters").attr("threshold_otsu");
    py::object label = py::module_::import("skimage.measure").attr("label");
    py::object clear_border =
        py::module_::import("skimage.segmentation").attr("clear_border");
    py::object morphology = py::module_::import("skimage.morphology");
    py::object closing = morphology.attr("closing");
    py::object footprint_rectangle = morphology.attr("footprint_rectangle");
    py::object remove_small_objects = morphology.attr("remove_small_objects");

    py::object image = skimage_data.attr("coins")().attr("__getitem__")(
        py::make_tuple(py::slice(50, -50, 1), py::slice(50, -50, 1)));
    py::object thresh = threshold_otsu(image);
    py::object bw = closing(
        py::reinterpret_borrow<py::object>(PyObject_RichCompare(
            image.ptr(), thresh.ptr(), Py_GT)),
        footprint_rectangle(py::make_tuple(4, 4)));
    py::object cleared = remove_small_objects(clear_border(bw), py::arg("max_size") = 20);
    py::object label_image = label(cleared);

    py::list pyramid;
    py::list labels;
    for (py::ssize_t index = 3; index >= 0; --index) {
        const long scale = static_cast<long>(std::pow(2, index));
        pyramid.append(scipy_zoom(image, py::int_(scale), py::arg("order") = 3));
        labels.append(scipy_zoom(label_image, py::int_(scale), py::arg("order") = 0));
    }

    return py::make_tuple(pyramid, labels);
}

py::tuple data_astronaut() {
    py::object numpy = py::module_::import("numpy");
    py::object skimage_data = py::module_::import("skimage.data");

    py::object astro = skimage_data.attr("astronaut")();
    py::object red = astro.attr("__getitem__")(
        py::make_tuple(py::slice(py::none(), py::none(), py::none()),
                       py::slice(py::none(), py::none(), py::none()),
                       py::int_(0)));
    py::object green = astro.attr("__getitem__")(
        py::make_tuple(py::slice(py::none(), py::none(), py::none()),
                       py::slice(py::none(), py::none(), py::none()),
                       py::int_(1)));
    py::object blue = astro.attr("__getitem__")(
        py::make_tuple(py::slice(py::none(), py::none(), py::none()),
                       py::slice(py::none(), py::none(), py::none()),
                       py::int_(2)));
    astro = numpy.attr("array")(py::make_tuple(red, green, blue));
    py::object pixels = numpy.attr("tile")(astro, py::make_tuple(1, 2, 2));
    py::list pyramid = scaler_nearest(pixels);

    py::list shape = py::cast<py::list>(pyramid[0].attr("shape"));
    py::ssize_t y = py::cast<py::ssize_t>(shape[1]);
    py::ssize_t x = py::cast<py::ssize_t>(shape[2]);
    py::object label = numpy.attr("zeros")(
        py::make_tuple(y, x), py::arg("dtype") = numpy.attr("int8"));

    py::object first_target = label.attr("__getitem__")(
        py::make_tuple(py::slice(200, 300, 1), py::slice(200, 300, 1)));
    data_make_circle(py::int_(100), py::int_(100), py::int_(1), first_target);

    py::object second_target = label.attr("__getitem__")(
        py::make_tuple(py::slice(250, 400, 1), py::slice(250, 400, 1)));
    data_make_circle(py::int_(150), py::int_(150), py::int_(2), second_target);

    py::list labels = scaler_nearest(label);
    return py::make_tuple(pyramid, labels);
}

PYBIND11_MODULE(_core, m) {
    m.def("axes_to_dicts", &axes_to_dicts);
    m.def("get_names", &get_names);
    m.def("validate_03", &validate_03);
    m.def("validate_axes_types", &validate_axes_types);
    m.def("int_to_rgba", &int_to_rgba);
    m.def("int_to_rgba_255", &int_to_rgba_255);
    m.def("rgba_to_int", &rgba_to_int);
    m.def("parse_csv_value", &parse_csv_value);
    m.def("dict_to_zarr", &dict_to_zarr);
    m.def("csv_to_zarr", &csv_to_zarr);
    m.def("strip_common_prefix", &strip_common_prefix);
    m.def("splitall", &splitall);
    m.def("find_multiscales", &find_multiscales);
    m.def("info_lines", &info_lines, py::arg("node"), py::arg("stats") = false);
    m.def("reader_matching_specs", &reader_matching_specs, py::arg("zarr"));
    m.def("reader_labels_names", &reader_labels_names, py::arg("root_attrs"));
    m.def("reader_node_repr", &reader_node_repr, py::arg("zarr"), py::arg("visible"));
    m.def("reader_label_payload", &reader_label_payload, py::arg("root_attrs"), py::arg("name"), py::arg("visible"));
    m.def("reader_multiscales_payload", &reader_multiscales_payload, py::arg("root_attrs"));
    m.def("reader_omero_payload", &reader_omero_payload, py::arg("image_data"), py::arg("node_visible"));
    m.def("_get_valid_axes", &get_valid_axes, py::arg("ndim") = py::none(), py::arg("axes") = py::none(), py::arg("fmt"));
    m.def("_extract_dims_from_axes", &extract_dims_from_axes, py::arg("axes") = py::none());
    m.def("_retuple", &retuple, py::arg("chunks"), py::arg("shape"));
    m.def("_validate_well_images", &validate_well_images, py::arg("images"), py::arg("fmt"));
    m.def("_validate_plate_acquisitions", &validate_plate_acquisitions, py::arg("acquisitions"), py::arg("fmt"));
    m.def("_validate_plate_rows_columns", &validate_plate_rows_columns, py::arg("rows_or_columns"), py::arg("fmt"));
    m.def("_validate_datasets", &validate_datasets, py::arg("datasets"), py::arg("dims"), py::arg("fmt"));
    m.def("_validate_plate_wells", &validate_plate_wells, py::arg("wells"), py::arg("rows"), py::arg("columns"), py::arg("fmt"));
    m.def("_blosc_compressor", &blosc_compressor);
    m.def("_resolve_storage_options", &resolve_storage_options, py::arg("storage_options"), py::arg("path"));
    m.def("_better_chunksize", &better_chunksize, py::arg("image"), py::arg("factors"));
    m.def("resize", &dask_resize, py::arg("image"), py::arg("output_shape"));
    m.def("local_mean", &dask_local_mean, py::arg("image"), py::arg("output_shape"));
    m.def("zoom", &dask_zoom, py::arg("image"), py::arg("output_shape"));
    m.def("downscale_nearest", &downscale_nearest_dask, py::arg("image"), py::arg("factors"));
    m.def("_build_pyramid", &build_pyramid, py::arg("image"), py::arg("scale_factors"), py::arg("dims"), py::arg("method") = py::str("nearest"), py::arg("chunks") = py::none());
    m.def("scaler_resize_image", &scaler_resize_image, py::arg("image"), py::arg("downscale") = 2, py::arg("order") = 1);
    m.def("scaler_nearest", &scaler_nearest, py::arg("base"), py::arg("downscale") = 2, py::arg("max_layer") = 4);
    m.def("scaler_gaussian", &scaler_gaussian, py::arg("base"), py::arg("downscale") = 2, py::arg("max_layer") = 4);
    m.def("scaler_laplacian", &scaler_laplacian, py::arg("base"), py::arg("downscale") = 2, py::arg("max_layer") = 4);
    m.def("scaler_local_mean", &scaler_local_mean, py::arg("base"), py::arg("downscale") = 2, py::arg("max_layer") = 4);
    m.def("scaler_zoom", &scaler_zoom, py::arg("base"), py::arg("downscale") = 2, py::arg("max_layer") = 4);
    m.def("data_make_circle", &data_make_circle, py::arg("h"), py::arg("w"), py::arg("value"), py::arg("target"));
    m.def("data_rgb_to_5d", &data_rgb_to_5d, py::arg("pixels"));
    m.def("data_coins", &data_coins);
    m.def("data_astronaut", &data_astronaut);
    register_format_bindings(m);
}
