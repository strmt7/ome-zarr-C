#include <pybind11/pybind11.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <functional>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

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
const std::array<const char*, 5> kFormatVersions = {"0.5", "0.4", "0.3", "0.2", "0.1"};

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

std::string metadata_version_from_key(const py::dict& metadata, const char* key) {
    py::object obj = metadata.attr("get")(py::str(key));
    if (obj.is_none()) {
        return "";
    }
    py::dict value = py::cast<py::dict>(obj);
    py::object version = value.attr("get")(py::str("version"), py::none());
    if (version.is_none()) {
        return "";
    }
    return py::cast<std::string>(version);
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

std::string normalize_known_format_version(const py::handle& version) {
    py::object candidate = py::reinterpret_borrow<py::object>(version);
    if (PyFloat_Check(version.ptr())) {
        candidate = py::str(version);
    }

    for (const char* known_version : kFormatVersions) {
        if (objects_equal(py::str(known_version), candidate)) {
            return known_version;
        }
    }

    throw py::value_error(
        "Version " + py::cast<std::string>(py::str(candidate)) + " not recognized");
}

bool is_known_format_version(const py::handle& version) {
    for (const char* known_version : kFormatVersions) {
        if (objects_equal(py::str(known_version), version)) {
            return true;
        }
    }
    return false;
}

}  // namespace

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
    for (const auto& name : axis_names(axes)) {
        result.append(py::str(name));
    }
    return result;
}

void validate_03(const py::sequence& axes) {
    const auto names = axis_names(axes);
    const auto len = names.size();

    auto tuple_repr = [&names]() {
        std::ostringstream os;
        os << "(";
        for (std::size_t i = 0; i < names.size(); ++i) {
            if (i > 0) {
                os << ", ";
            }
            os << "'" << names[i] << "'";
        }
        if (names.size() == 1) {
            os << ",";
        }
        os << ")";
        return os.str();
    };

    if (len == 2) {
        if (!(names[0] == "y" && names[1] == "x")) {
            throw py::value_error(
                "2D data must have axes ('y', 'x') " + tuple_repr());
        }
        return;
    }

    if (len == 3) {
        const bool valid =
            (names[0] == "z" && names[1] == "y" && names[2] == "x") ||
            (names[0] == "c" && names[1] == "y" && names[2] == "x") ||
            (names[0] == "t" && names[1] == "y" && names[2] == "x");
        if (!valid) {
            throw py::value_error(
                "3D data must have axes ('z', 'y', 'x') or ('c', 'y', 'x')"
                " or ('t', 'y', 'x'), not " +
                tuple_repr());
        }
        return;
    }

    if (len == 4) {
        const bool valid =
            (names[0] == "t" && names[1] == "z" && names[2] == "y" &&
             names[3] == "x") ||
            (names[0] == "c" && names[1] == "z" && names[2] == "y" &&
             names[3] == "x") ||
            (names[0] == "t" && names[1] == "c" && names[2] == "y" &&
             names[3] == "x");
        if (!valid) {
            throw py::value_error(
                "4D data must have axes tzyx or czyx or tcyx");
        }
        return;
    }

    const bool valid_5d = names == std::vector<std::string>{"t", "c", "z", "y", "x"};
    if (!valid_5d) {
        throw py::value_error(
            "5D data must have axes ('t', 'c', 'z', 'y', 'x')");
    }
}

void validate_axes_types(const py::sequence& axes) {
    std::vector<py::object> axes_types_objects;
    axes_types_objects.reserve(py::len(axes));

    std::vector<std::string> axes_types;
    axes_types.reserve(py::len(axes));

    for (const py::handle& axis_handle : axes) {
        py::dict axis = py::cast<py::dict>(axis_handle);
        py::object axis_type = axis.attr("get")("type");
        axes_types_objects.push_back(axis_type);
        if (axis_type.is_none()) {
            axes_types.emplace_back("__NONE__");
        } else {
            axes_types.push_back(py::cast<std::string>(axis_type));
        }
    }

    const std::vector<std::string> known_types = {"space", "channel", "time"};
    std::vector<std::string> unknown_types;
    for (std::size_t i = 0; i < axes_types.size(); ++i) {
        const auto& axis_type = axes_types[i];
        if (std::find(known_types.begin(), known_types.end(), axis_type) ==
            known_types.end()) {
            unknown_types.push_back(
                py::cast<std::string>(py::repr(axes_types_objects[i])));
        }
    }

    if (unknown_types.size() > 1) {
        std::ostringstream os;
        os << "[";
        for (std::size_t i = 0; i < unknown_types.size(); ++i) {
            if (i > 0) {
                os << ", ";
            }
            os << unknown_types[i];
        }
        os << "]";
        throw py::value_error(
            "Too many unknown axes types. 1 allowed, found: " + os.str());
    }

    auto last_index = [&axes_types](const std::string& needle) {
        for (std::size_t i = axes_types.size(); i-- > 0;) {
            if (axes_types[i] == needle) {
                return static_cast<int>(i);
            }
        }
        return -1;
    };

    if (last_index("time") > 0) {
        throw py::value_error("'time' axis must be first dimension only");
    }

    const auto channel_count =
        static_cast<int>(std::count(axes_types.begin(), axes_types.end(), "channel"));
    if (channel_count > 1) {
        throw py::value_error("Only 1 axis can be type 'channel'");
    }

    const int channel_last_index = last_index("channel");
    if (channel_last_index >= 0) {
        py::list axes_types_list;
        for (const auto& axis_type : axes_types) {
            axes_types_list.append(py::str(axis_type));
        }
        const int first_space_index =
            py::cast<int>(axes_types_list.attr("index")(py::str("space")));
        if (channel_last_index > first_space_index) {
            throw py::value_error("'space' axes must come after 'channel'");
        }
    }
}

std::array<std::uint8_t, 4> int_to_rgba_255_bytes(std::int32_t value) {
    const std::uint32_t raw = static_cast<std::uint32_t>(value);
    return {
        static_cast<std::uint8_t>((raw >> 24U) & 0xFFU),
        static_cast<std::uint8_t>((raw >> 16U) & 0xFFU),
        static_cast<std::uint8_t>((raw >> 8U) & 0xFFU),
        static_cast<std::uint8_t>(raw & 0xFFU),
    };
}

py::list int_to_rgba_255(std::int32_t value) {
    const auto bytes = int_to_rgba_255_bytes(value);
    py::list result;
    for (const auto byte : bytes) {
        result.append(py::int_(byte));
    }
    return result;
}

py::list int_to_rgba(std::int32_t value) {
    const auto bytes = int_to_rgba_255_bytes(value);
    py::list result;
    for (const auto byte : bytes) {
        result.append(py::float_(static_cast<double>(byte) / 255.0));
    }
    return result;
}

std::int32_t rgba_to_int(std::uint8_t r,
                         std::uint8_t g,
                         std::uint8_t b,
                         std::uint8_t a) {
    const std::uint32_t raw =
        (static_cast<std::uint32_t>(r) << 24U) |
        (static_cast<std::uint32_t>(g) << 16U) |
        (static_cast<std::uint32_t>(b) << 8U) |
        static_cast<std::uint32_t>(a);
    return static_cast<std::int32_t>(raw);
}

py::object parse_csv_value(const std::string& value, const std::string& col_type) {
    try {
        if (col_type == "d") {
            return py::float_(std::stod(value));
        }
        if (col_type == "l") {
            const double parsed = std::stod(value);
            if (std::isnan(parsed)) {
                return py::str(value);
            }
            if (std::isinf(parsed)) {
                raise_overflow_error("cannot convert float infinity to integer");
            }
            return py::int_(static_cast<long long>(std::nearbyint(parsed)));
        }
        if (col_type == "b") {
            return py::bool_(!value.empty());
        }
    } catch (const std::invalid_argument&) {
        return py::str(value);
    } catch (const std::out_of_range&) {
        return py::str(value);
    }

    return py::str(value);
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
    if (py::len(parts) == 0) {
        raise_plain_exception("No common prefix:\n");
    }

    std::size_t min_length = static_cast<std::size_t>(-1);
    for (const py::handle& part_handle : parts) {
        py::list part = py::cast<py::list>(part_handle);
        min_length = std::min(min_length, static_cast<std::size_t>(py::len(part)));
    }

    std::size_t first_mismatch = 0;
    for (std::size_t idx = 0; idx < min_length; ++idx) {
        std::string candidate;
        bool all_equal = true;
        for (std::size_t path_index = 0; path_index < static_cast<std::size_t>(py::len(parts));
             ++path_index) {
            py::list part = py::cast<py::list>(parts[path_index]);
            const std::string current = py::cast<std::string>(part[idx]);
            if (path_index == 0) {
                candidate = current;
            } else if (current != candidate) {
                all_equal = false;
                break;
            }
        }
        if (!all_equal) {
            break;
        }
        first_mismatch += 1;
    }

    if (first_mismatch == 0) {
        raise_plain_exception(repr_joined_lines(parts));
    }

    py::list first_path = py::cast<py::list>(parts[0]);
    py::str common = py::cast<py::str>(first_path[first_mismatch - 1]);

    for (std::size_t idx = 0; idx < static_cast<std::size_t>(py::len(parts)); ++idx) {
        py::list path = py::cast<py::list>(parts[idx]);
        py::list trimmed;
        for (std::size_t offset = first_mismatch - 1;
             offset < static_cast<std::size_t>(py::len(path));
             ++offset) {
            trimmed.append(path[offset]);
        }
        parts[idx] = trimmed;
    }

    return common;
}

py::list splitall(py::object path) {
    py::list allparts;
    py::object current = path;
    py::object os_path = py::module_::import("os").attr("path");

    while (true) {
        py::tuple parts = py::cast<py::tuple>(os_path.attr("split")(current));
        if (objects_equal(parts[0], current)) {
            allparts.attr("insert")(0, parts[0]);
            break;
        }
        if (objects_equal(parts[1], current)) {
            allparts.attr("insert")(0, parts[1]);
            break;
        }
        current = parts[0];
        allparts.attr("insert")(0, parts[1]);
    }

    return allparts;
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
    py::list matches;

    if (py::cast<bool>(root_attrs.attr("__contains__")(py::str("labels")))) {
        matches.append(py::str("Labels"));
    }
    if (py::cast<bool>(root_attrs.attr("__contains__")(py::str("image-label")))) {
        matches.append(py::str("Label"));
    }
    if (object_truthy(zarr.attr("zgroup")) &&
        py::cast<bool>(root_attrs.attr("__contains__")(py::str("multiscales")))) {
        matches.append(py::str("Multiscales"));
    }
    if (py::cast<bool>(root_attrs.attr("__contains__")(py::str("omero")))) {
        matches.append(py::str("OMERO"));
    }
    if (py::cast<bool>(root_attrs.attr("__contains__")(py::str("plate")))) {
        matches.append(py::str("Plate"));
    }
    if (py::cast<bool>(root_attrs.attr("__contains__")(py::str("well")))) {
        matches.append(py::str("Well"));
    }

    return matches;
}

py::object reader_labels_names(const py::dict& root_attrs) {
    return root_attrs.attr("get")(py::str("labels"), py::list());
}

py::str reader_node_repr(const py::object& zarr, bool visible) {
    std::string suffix;
    if (!visible) {
        suffix = " (hidden)";
    }
    return py::str(py::cast<std::string>(py::str(zarr)) + suffix);
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

    py::list paths;
    py::list transformations;
    bool any_transformations = false;
    for (const py::handle& dataset_handle : datasets) {
        py::object dataset = py::reinterpret_borrow<py::object>(dataset_handle);
        paths.append(dataset.attr("__getitem__")(py::str("path")));
        py::object transform =
            dataset.attr("get")(py::str("coordinateTransformations"), py::none());
        transformations.append(transform);
        any_transformations = any_transformations || !transform.is_none();
    }
    payload["paths"] = paths;
    if (any_transformations) {
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
        if (!color.is_none()) {
            const std::string color_hex = py::cast<std::string>(color);
            py::list rgb;
            for (int offset = 0; offset < 6; offset += 2) {
                rgb.append(py::float_(static_cast<double>(
                    std::stoi(color_hex.substr(offset, 2), nullptr, 16)) /
                                      255.0));
            }
            if (model == "greyscale") {
                rgb = py::list();
                rgb.append(py::int_(1));
                rgb.append(py::int_(1));
                rgb.append(py::int_(1));
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

        py::object label = channel.attr("get")(py::str("label"), py::none());
        if (!label.is_none()) {
            names[idx] = label;
        }

        py::object active = channel.attr("get")(py::str("active"), py::none());
        if (!active.is_none()) {
            if (object_truthy(active)) {
                visibles[idx] = py::bool_(node_visible);
            } else {
                visibles[idx] = active;
            }
        }

        py::object window = channel.attr("get")(py::str("window"), py::none());
        if (!window.is_none()) {
            py::object start = window.attr("get")(py::str("start"), py::none());
            py::object end = window.attr("get")(py::str("end"), py::none());
            if (start.is_none() || end.is_none()) {
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
    if (version == "0.1" || version == "0.2") {
        if (!axes.is_none()) {
            logger.attr("info")("axes ignored for version 0.1 or 0.2");
        }
        return py::none();
    }

    if (axes.is_none()) {
        if (!ndim.is_none() && objects_equal(ndim, py::int_(2))) {
            py::list guessed_axes;
            guessed_axes.append(py::str("y"));
            guessed_axes.append(py::str("x"));
            axes = guessed_axes;
            logger.attr("info")("Auto using axes %s for 2D data", axes);
        } else if (!ndim.is_none() && objects_equal(ndim, py::int_(5))) {
            py::list guessed_axes;
            guessed_axes.append(py::str("t"));
            guessed_axes.append(py::str("c"));
            guessed_axes.append(py::str("z"));
            guessed_axes.append(py::str("y"));
            guessed_axes.append(py::str("x"));
            axes = guessed_axes;
            logger.attr("info")("Auto using axes %s for 5D data", axes);
        } else {
            throw py::value_error(
                "axes must be provided. Can't be guessed for 3D or 4D data");
        }
    }

    if (py::isinstance<py::str>(axes)) {
        axes = builtins.attr("list")(axes);
    }

    if (!ndim.is_none()) {
        const py::int_ axes_len(py::len(axes));
        if (!objects_equal(axes_len, ndim)) {
            throw py::value_error(
                "axes length (" + py::cast<std::string>(py::str(axes_len)) +
                ") must match number of dimensions (" +
                py::cast<std::string>(py::str(ndim)) + ")");
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
        for (const py::handle& axis_handle : py::iterable(axes)) {
            py::object axis = py::reinterpret_borrow<py::object>(axis_handle);
            if (!py::isinstance<py::dict>(axis) ||
                !py::cast<py::dict>(axis).contains("name")) {
                throw py::type_error(
                    "`axes` must be a list of dicts containing 'name'");
            }
            names.append(py::str(py::cast<py::dict>(axis)["name"]));
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
        if (py::isinstance<py::str>(image)) {
            py::dict validated_image;
            validated_image["path"] = py::str(image);
            validated_images.append(validated_image);
        } else if (py::isinstance<py::dict>(image)) {
            py::dict image_dict = py::cast<py::dict>(image);

            bool has_unexpected_key = false;
            for (const auto& key_value : image_dict) {
                const py::handle key_handle = key_value.first;
                if (!objects_equal(key_handle, py::str("acquisition")) &&
                    !objects_equal(key_handle, py::str("path"))) {
                    has_unexpected_key = true;
                    break;
                }
            }
            if (has_unexpected_key) {
                logger.attr("debug")("%s contains unspecified keys", image);
            }

            if (!image_dict.contains("path")) {
                throw py::value_error(
                    py::cast<std::string>(py::str(image)) +
                    " must contain a path key");
            }
            if (!py::isinstance<py::str>(image_dict["path"])) {
                throw py::value_error(
                    py::cast<std::string>(py::str(image)) +
                    " path must be of string type");
            }
            if (image_dict.contains("acquisition") &&
                !PyLong_Check(image_dict["acquisition"].ptr())) {
                throw py::value_error(
                    py::cast<std::string>(py::str(image)) +
                    " acquisition must be of int type");
            }
            validated_images.append(image);
        } else {
            throw py::value_error(
                "Unrecognized type for " +
                py::cast<std::string>(py::str(image)));
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
        if (!py::isinstance<py::dict>(acquisition)) {
            throw py::value_error(
                py::cast<std::string>(py::str(acquisition)) +
                " must be a dictionary");
        }

        py::dict acquisition_dict = py::cast<py::dict>(acquisition);
        bool has_unexpected_key = false;
        for (const auto& key_value : acquisition_dict) {
            const py::handle key_handle = key_value.first;
            if (!objects_equal(key_handle, py::str("id")) &&
                !objects_equal(key_handle, py::str("name")) &&
                !objects_equal(key_handle, py::str("maximumfieldcount")) &&
                !objects_equal(key_handle, py::str("description")) &&
                !objects_equal(key_handle, py::str("starttime")) &&
                !objects_equal(key_handle, py::str("endtime"))) {
                has_unexpected_key = true;
                break;
            }
        }
        if (has_unexpected_key) {
            logger.attr("debug")("%s contains unspecified keys", acquisition);
        }

        if (!acquisition_dict.contains("id")) {
            throw py::value_error(
                py::cast<std::string>(py::str(acquisition)) +
                " must contain an id key");
        }
        if (!PyLong_Check(acquisition_dict["id"].ptr())) {
            throw py::value_error(
                py::cast<std::string>(py::str(acquisition)) +
                " id must be of int type");
        }
    }

    return acquisitions;
}

py::list validate_plate_rows_columns(
    py::object rows_or_columns,
    py::object fmt = py::none()) {
    static_cast<void>(fmt);
    py::object builtins = py::module_::import("builtins");

    if (py::len(builtins.attr("set")(rows_or_columns)) != py::len(rows_or_columns)) {
        throw py::value_error(
            py::cast<std::string>(py::str(rows_or_columns)) +
            " must contain unique elements");
    }

    py::list validated_list;
    for (const py::handle& element_handle : py::iterable(rows_or_columns)) {
        py::object element = py::reinterpret_borrow<py::object>(element_handle);
        if (!object_truthy(element.attr("isalnum")())) {
            throw py::value_error(
                py::cast<std::string>(py::str(element)) +
                " must contain alphanumeric characters");
        }
        py::dict validated_element;
        validated_element["name"] = py::str(element);
        validated_list.append(validated_element);
    }

    return validated_list;
}

py::object validate_datasets(
    py::object datasets,
    py::object dims,
    py::object fmt = py::none()) {
    if (datasets.is_none() || py::len(datasets) == 0) {
        throw py::value_error("Empty datasets list");
    }

    py::list transformations;
    for (const py::handle& dataset_handle : py::iterable(datasets)) {
        py::object dataset = py::reinterpret_borrow<py::object>(dataset_handle);
        if (!py::isinstance<py::dict>(dataset)) {
            throw py::value_error(
                "Unrecognized type for " +
                py::cast<std::string>(py::str(dataset)));
        }

        py::dict dataset_dict = py::cast<py::dict>(dataset);
        py::object path = dataset_dict.attr("get")("path");
        if (!object_truthy(path)) {
            throw py::value_error("no 'path' in dataset");
        }

        py::object transformation =
            dataset_dict.attr("get")("coordinateTransformations");
        if (!transformation.is_none()) {
            transformations.append(transformation);
        }
    }

    fmt.attr("validate_coordinate_transformations")(
        dims, py::len(datasets), transformations);
    return datasets;
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
    py::object int_type = py::module_::import("builtins").attr("int");

    py::object chunksize_array = numpy.attr("array")(image.attr("chunksize"));
    py::object better_chunks = py::tuple(
        numpy
            .attr("maximum")(
                1,
                numpy.attr("divide")(
                    numpy.attr("round")(numpy.attr("multiply")(chunksize_array, factors)),
                    factors))
            .attr("astype")(int_type));

    py::object block_output = py::tuple(
        numpy
            .attr("ceil")(
                numpy.attr("multiply")(numpy.attr("array")(better_chunks), factors))
            .attr("astype")(int_type));

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

    py::object normalized_scale_factors = scale_factors;
    if (all_int_scale_factors) {
        py::list scales;
        const bool contains_z = dims_tuple.attr("__contains__")("z").cast<bool>();
        for (py::ssize_t level_index = 1; level_index <= py::len(scale_factors); ++level_index) {
            py::dict scale;
            for (const py::handle& dim_handle : dims_tuple) {
                const std::string dim_name = py::cast<std::string>(dim_handle);
                const bool is_spatial =
                    dim_name == "z" || dim_name == "y" || dim_name == "x";
                scale[dim_handle] = is_spatial ? py::int_(1 << level_index) : py::int_(1);
            }
            if (contains_z) {
                scale["z"] = py::int_(1);
            }
            scales.append(scale);
        }
        normalized_scale_factors = scales;
    } else {
        for (py::ssize_t index = 0; index < py::len(scale_factors); ++index) {
            py::object level = scale_factors.attr("__getitem__")(index);
            py::dict reordered_level;
            for (const py::handle& dim_handle : dims_tuple) {
                reordered_level[dim_handle] = level.attr("get")(dim_handle, py::int_(1));
            }
            set_item(scale_factors, py::int_(index), reordered_level);
        }
    }

    py::list images;
    images.append(image);

    for (py::ssize_t index = 0; index < py::len(normalized_scale_factors); ++index) {
        py::object factor = normalized_scale_factors.attr("__getitem__")(index);
        py::object relative_factors;
        if (index == 0) {
            relative_factors = numpy.attr("asarray")(
                builtins.attr("list")(factor.attr("values")()));
        } else {
            py::object previous = normalized_scale_factors.attr("__getitem__")(index - 1);
            relative_factors = numpy.attr("divide")(
                numpy.attr("asarray")(builtins.attr("list")(factor.attr("values")())),
                numpy.attr("asarray")(builtins.attr("list")(previous.attr("values")())));
        }

        py::tuple previous_shape = py::cast<py::tuple>(images[py::len(images) - 1].attr("shape"));
        py::tuple relative_factor_tuple = py::tuple(relative_factors);
        py::list target_shape;

        for (py::size_t dim_index = 0; dim_index < py::len(previous_shape); ++dim_index) {
            const std::string dim_name = py::cast<std::string>(dims_tuple[dim_index]);
            py::object shape_value = previous_shape[dim_index];
            py::object factor_value = relative_factor_tuple[dim_index];
            const bool is_spatial =
                dim_name == "z" || dim_name == "y" || dim_name == "x";

            if (is_spatial) {
                py::object scaled = floor_divide(shape_value, factor_value);
                if (objects_equal(scaled, py::int_(0))) {
                    target_shape.append(py::int_(1));
                    warnings.attr("warn")(
                        "Dimension " + dim_name + " is too small to downsample further.",
                        builtins.attr("UserWarning"),
                        py::arg("stacklevel") = 3);
                } else {
                    target_shape.append(builtins.attr("int")(scaled));
                }
            } else {
                target_shape.append(builtins.attr("int")(shape_value));
            }
        }

        py::object current_image = images[py::len(images) - 1];
        py::object new_image;
        if (method_key == "resize") {
            new_image = core.attr("resize")(
                current_image,
                py::tuple(target_shape),
                py::arg("order") = 1,
                py::arg("mode") = "reflect",
                py::arg("anti_aliasing") = true,
                py::arg("preserve_range") = true);
        } else if (method_key == "nearest") {
            new_image = core.attr("resize")(
                current_image,
                py::tuple(target_shape),
                py::arg("order") = 0,
                py::arg("mode") = "reflect",
                py::arg("anti_aliasing") = false,
                py::arg("preserve_range") = true);
        } else if (method_key == "local_mean") {
            new_image = core.attr("local_mean")(current_image, py::tuple(target_shape));
        } else if (method_key == "zoom") {
            new_image = core.attr("zoom")(current_image, py::tuple(target_shape));
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
    const py::ssize_t h = py::cast<py::ssize_t>(height);
    const py::ssize_t w = py::cast<py::ssize_t>(width);
    const py::ssize_t cx = w / 2;
    const py::ssize_t cy = h / 2;
    const py::ssize_t radius = std::min(w, h) / 2;
    const py::ssize_t radius_squared = radius * radius;

    for (py::ssize_t y = 0; y < h; ++y) {
        for (py::ssize_t x = 0; x < w; ++x) {
            const py::ssize_t dx = x - cx;
            const py::ssize_t dy = y - cy;
            if (dx * dx + dy * dy < radius_squared) {
                set_item(target, py::make_tuple(y, x), value);
            }
        }
    }
}

py::object data_rgb_to_5d(py::object pixels) {
    py::object numpy = py::module_::import("numpy");
    const py::ssize_t ndim = py::cast<py::ssize_t>(pixels.attr("ndim"));
    if (ndim == 2) {
        py::object stack = numpy.attr("array")(py::make_tuple(pixels));
        py::object channels = numpy.attr("array")(py::make_tuple(stack));
        return numpy.attr("array")(py::make_tuple(channels));
    }
    if (ndim == 3) {
        py::tuple pixel_shape = py::cast<py::tuple>(pixels.attr("shape"));
        const py::ssize_t size_c = py::cast<py::ssize_t>(pixel_shape[2]);
        py::list channels;
        for (py::ssize_t channel_index = 0; channel_index < size_c; ++channel_index) {
            py::object channel = pixels.attr("__getitem__")(
                py::make_tuple(py::slice(py::none(), py::none(), py::none()),
                               py::slice(py::none(), py::none(), py::none()),
                               py::int_(channel_index)));
            channels.append(numpy.attr("array")(py::make_tuple(channel)));
        }
        return numpy.attr("array")(py::make_tuple(channels));
    }
    PyErr_SetString(
        PyExc_AssertionError,
        ("expecting 2 or 3d: (" + py::cast<std::string>(py::str(pixels.attr("shape"))) + ")")
            .c_str());
    throw py::error_already_set();
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

py::list format_versions() {
    py::list versions;
    for (const char* version : kFormatVersions) {
        versions.append(py::str(version));
    }
    return versions;
}

py::str resolve_format_version(const py::handle& version) {
    return py::str(normalize_known_format_version(version));
}

py::object get_metadata_version(py::dict metadata) {
    py::list multiscales = py::cast<py::list>(metadata.attr("get")("multiscales", py::list()));
    if (py::len(multiscales) > 0) {
        py::dict dataset = py::cast<py::dict>(multiscales[0]);
        py::object version = dataset.attr("get")("version", py::none());
        if (!version.is_none()) {
            return version;
        }
    }

    for (const char* key : {"plate", "well", "image-label"}) {
        const std::string version = metadata_version_from_key(metadata, key);
        if (!version.empty()) {
            return py::str(version);
        }
    }

    return py::none();
}

py::object detect_format_version(py::dict metadata) {
    if (py::len(metadata) == 0) {
        return py::none();
    }

    py::object version = get_metadata_version(metadata);
    if (version.is_none() || !is_known_format_version(version)) {
        return py::none();
    }

    return version;
}

bool format_matches(const std::string& version, py::dict metadata) {
    py::object metadata_version = get_metadata_version(metadata);
    if (metadata_version.is_none()) {
        return false;
    }
    return objects_equal(py::str(normalize_known_format_version(py::str(version))), metadata_version);
}

int format_zarr_format(const std::string& version) {
    const std::string normalized = normalize_known_format_version(py::str(version));
    if (normalized == "0.5") {
        return 3;
    }
    return 2;
}

py::dict format_chunk_key_encoding(const std::string& version) {
    const std::string normalized = normalize_known_format_version(py::str(version));
    py::dict encoding;
    if (normalized == "0.1") {
        encoding["name"] = py::str("v2");
        encoding["separator"] = py::str(".");
        return encoding;
    }
    if (normalized == "0.5") {
        encoding["name"] = py::str("default");
        encoding["separator"] = py::str("/");
        return encoding;
    }
    encoding["name"] = py::str("v2");
    encoding["separator"] = py::str("/");
    return encoding;
}

void validate_well_dict_v01(py::dict well) {
    if (!well.contains("path")) {
        py::tuple args(4);
        args[0] = py::str("%s must contain a %s key of type %s");
        args[1] = well;
        args[2] = py::str("path");
        args[3] = py::type::of(py::str(""));
        raise_value_error_args(args);
    }
    if (!PyUnicode_Check(well["path"].ptr())) {
        py::tuple args(3);
        args[0] = py::str("%s path must be of %s type");
        args[1] = well;
        args[2] = py::type::of(py::str(""));
        raise_value_error_args(args);
    }
}

void validate_well_dict_v04(py::dict well,
                            const py::sequence& rows,
                            const py::sequence& columns) {
    validate_well_dict_v01(well);

    if (!well.contains("rowIndex")) {
        py::tuple args(4);
        args[0] = py::str("%s must contain a %s key of type %s");
        args[1] = well;
        args[2] = py::str("rowIndex");
        args[3] = py::type::of(py::int_(0));
        raise_value_error_args(args);
    }
    if (!well.contains("columnIndex")) {
        py::tuple args(4);
        args[0] = py::str("%s must contain a %s key of type %s");
        args[1] = well;
        args[2] = py::str("columnIndex");
        args[3] = py::type::of(py::int_(0));
        raise_value_error_args(args);
    }
    if (!PyLong_Check(well["rowIndex"].ptr())) {
        py::tuple args(3);
        args[0] = py::str("%s path must be of %s type");
        args[1] = well;
        args[2] = py::type::of(py::int_(0));
        raise_value_error_args(args);
    }
    if (!PyLong_Check(well["columnIndex"].ptr())) {
        py::tuple args(3);
        args[0] = py::str("%s path must be of %s type");
        args[1] = well;
        args[2] = py::type::of(py::int_(0));
        raise_value_error_args(args);
    }

    const std::string path = py::cast<std::string>(well["path"]);
    py::list path_parts = py::module_::import("builtins").attr("str")(path).attr("split")("/");
    if (py::len(path_parts) != 2) {
        py::tuple args(2);
        args[0] = py::str("%s path must exactly be composed of 2 groups");
        args[1] = well;
        raise_value_error_args(args);
    }

    const std::string row = py::cast<std::string>(path_parts[0]);
    const std::string column = py::cast<std::string>(path_parts[1]);
    py::list rows_list = py::list(rows);
    py::list columns_list = py::list(columns);

    if (!rows_list.contains(py::str(row))) {
        py::tuple args(2);
        args[0] = py::str("%s is not defined in the plate rows");
        args[1] = py::str(row);
        raise_value_error_args(args);
    }
    if (py::cast<int>(well["rowIndex"]) != py::cast<int>(rows_list.attr("index")(py::str(row)))) {
        py::tuple args(2);
        args[0] = py::str("Mismatching row index for %s");
        args[1] = well;
        raise_value_error_args(args);
    }
    if (!columns_list.contains(py::str(column))) {
        py::tuple args(2);
        args[0] = py::str("%s is not defined in the plate columns");
        args[1] = py::str(column);
        raise_value_error_args(args);
    }
    if (py::cast<int>(well["columnIndex"]) !=
        py::cast<int>(columns_list.attr("index")(py::str(column)))) {
        py::tuple args(2);
        args[0] = py::str("Mismatching column index for %s");
        args[1] = well;
        raise_value_error_args(args);
    }
}

py::list generate_coordinate_transformations(py::sequence shapes) {
    py::list shapes_list = py::list(shapes);
    py::sequence data_shape = py::cast<py::sequence>(shapes_list[0]);
    py::list coordinate_transformations;

    for (const py::handle& shape_handle : shapes_list) {
        py::sequence shape = py::cast<py::sequence>(shape_handle);
        if (py::len(shape) != py::len(data_shape)) {
            throw py::value_error("Shape lengths must match");
        }
        const py::ssize_t shape_length = static_cast<py::ssize_t>(py::len(shape));
        py::list scale;
        for (py::ssize_t index = 0; index < shape_length; ++index) {
            const double full = py::cast<double>(data_shape[index]);
            const double level = py::cast<double>(shape[index]);
            scale.append(py::float_(full / level));
        }
        py::dict transform;
        transform["type"] = py::str("scale");
        transform["scale"] = scale;
        py::list level_transforms;
        level_transforms.append(transform);
        coordinate_transformations.append(level_transforms);
    }

    return coordinate_transformations;
}

void validate_coordinate_transformations(
    int ndim,
    int nlevels,
    py::object coordinate_transformations_obj = py::none()) {
    if (coordinate_transformations_obj.is_none()) {
        throw py::value_error("coordinate_transformations must be provided");
    }

    py::list coordinate_transformations = py::cast<py::list>(coordinate_transformations_obj);
    const int ct_count = static_cast<int>(py::len(coordinate_transformations));
    if (ct_count != nlevels) {
        throw py::value_error(
            "coordinate_transformations count: " + std::to_string(ct_count) +
            " must match datasets " + std::to_string(nlevels));
    }

    for (const py::handle& transformations_handle : coordinate_transformations) {
        py::list transformations = py::cast<py::list>(transformations_handle);
        py::list types;
        for (const py::handle& transformation_handle : transformations) {
            py::dict transformation = py::cast<py::dict>(transformation_handle);
            types.append(transformation.attr("get")("type", py::none()));
        }

        for (const py::handle& type_handle : types) {
            if (type_handle.is_none()) {
                throw py::value_error(
                    "Missing type in: " + repr_object(transformations));
            }
        }

        int scale_count = 0;
        for (const py::handle& type_handle : types) {
            if (py::cast<std::string>(type_handle) == "scale") {
                scale_count += 1;
            }
        }
        if (scale_count != 1) {
            throw py::value_error(
                "Must supply 1 'scale' item in coordinate_transformations");
        }
        if (py::cast<std::string>(types[0]) != "scale") {
            throw py::value_error("First coordinate_transformations must be 'scale'");
        }

        py::dict first = py::cast<py::dict>(transformations[0]);
        if (!first.contains("scale")) {
            throw py::value_error(
                "Missing scale argument in: " + repr_object(first));
        }
        py::object scale = first["scale"];
        py::sequence scale_values = py::cast<py::sequence>(scale);
        if (py::len(scale_values) != static_cast<py::size_t>(ndim)) {
            throw py::value_error(
                "'scale' list " + repr_object(scale) +
                " must match number of image dimensions: " + std::to_string(ndim));
        }
        for (const py::handle& value : scale_values) {
            if (!is_number_like(value)) {
                throw py::value_error(
                    "'scale' values must all be numbers: " + repr_object(scale));
            }
        }

        int translation_count = 0;
        int translation_index = -1;
        const py::ssize_t types_length = static_cast<py::ssize_t>(py::len(types));
        for (py::ssize_t index = 0; index < types_length; ++index) {
            if (py::cast<std::string>(types[index]) == "translation") {
                translation_count += 1;
                translation_index = static_cast<int>(index);
            }
        }
        if (translation_count > 1) {
            throw py::value_error(
                "Must supply 0 or 1 'translation' item incoordinate_transformations");
        }
        if (translation_count == 1) {
            py::dict transformation = py::cast<py::dict>(transformations[translation_index]);
            if (!transformation.contains("translation")) {
                throw py::value_error(
                    "Missing scale argument in: " + repr_object(first));
            }
            py::object translation = transformation["translation"];
            py::sequence translation_values = py::cast<py::sequence>(translation);
            if (py::len(translation_values) != static_cast<py::size_t>(ndim)) {
                throw py::value_error(
                    "'translation' list " + repr_object(translation) +
                    " must match image dimensions count: " + std::to_string(ndim));
            }
            for (const py::handle& value : translation_values) {
                if (!is_number_like(value)) {
                    throw py::value_error(
                        "'translation' values must all be numbers: " +
                        repr_object(translation));
                }
            }
        }
    }
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
    m.def("format_versions", &format_versions);
    m.def("resolve_format_version", &resolve_format_version, py::arg("version"));
    m.def("get_metadata_version", &get_metadata_version);
    m.def("detect_format_version", &detect_format_version);
    m.def("format_matches", &format_matches, py::arg("version"), py::arg("metadata"));
    m.def("format_zarr_format", &format_zarr_format, py::arg("version"));
    m.def("format_chunk_key_encoding", &format_chunk_key_encoding, py::arg("version"));
    m.def("validate_well_dict_v01", &validate_well_dict_v01);
    m.def("validate_well_dict_v04", &validate_well_dict_v04);
    m.def("generate_coordinate_transformations", &generate_coordinate_transformations);
    m.def("validate_coordinate_transformations", &validate_coordinate_transformations);
}
