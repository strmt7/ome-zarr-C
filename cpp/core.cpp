#include <pybind11/eval.h>
#include <pybind11/pybind11.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
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

py::object true_divide(const py::handle& left, const py::handle& right) {
    PyObject* result = PyNumber_TrueDivide(left.ptr(), right.ptr());
    if (result == nullptr) {
        throw py::error_already_set();
    }
    return py::reinterpret_steal<py::object>(result);
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

py::dict generate_well_dict_v04(const std::string& well,
                                const py::sequence& rows,
                                const py::sequence& columns) {
    py::dict locals;
    locals["well"] = py::str(well);
    py::exec("row, column = well.split('/')", py::globals(), locals);
    const std::string row = py::cast<std::string>(locals["row"]);
    const std::string column = py::cast<std::string>(locals["column"]);

    py::list rows_list = py::list(rows);
    py::list columns_list = py::list(columns);

    if (!rows_list.contains(py::str(row))) {
        py::tuple args(2);
        args[0] = py::str("%s is not defined in the list of rows");
        args[1] = py::str(row);
        raise_value_error_args(args);
    }
    if (!columns_list.contains(py::str(column))) {
        py::tuple args(2);
        args[0] = py::str("%s is not defined in the list of columns");
        args[1] = py::str(column);
        raise_value_error_args(args);
    }

    py::dict result;
    result["path"] = py::str(well);
    result["rowIndex"] = rows_list.attr("index")(py::str(row));
    result["columnIndex"] = columns_list.attr("index")(py::str(column));
    return result;
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
    m.def("strip_common_prefix", &strip_common_prefix);
    m.def("splitall", &splitall);
    m.def("find_multiscales", &find_multiscales);
    m.def("get_metadata_version", &get_metadata_version);
    m.def("validate_well_dict_v01", &validate_well_dict_v01);
    m.def("generate_well_dict_v04", &generate_well_dict_v04);
    m.def("validate_well_dict_v04", &validate_well_dict_v04);
    m.def("generate_coordinate_transformations", &generate_coordinate_transformations);
    m.def("validate_coordinate_transformations", &validate_coordinate_transformations);
}
