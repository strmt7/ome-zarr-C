#include <pybind11/pybind11.h>

#include <algorithm>
#include <array>
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

std::string bytes_to_string(const std::array<std::uint8_t, 4>& bytes) {
    return std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size());
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
        const auto space_first =
            std::find(axes_types.begin(), axes_types.end(), "space");
        if (space_first == axes_types.end()) {
            throw py::value_error("'space' is not in list");
        }
        const int first_space_index =
            static_cast<int>(std::distance(axes_types.begin(), space_first));
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

PYBIND11_MODULE(_core, m) {
    m.def("axes_to_dicts", &axes_to_dicts);
    m.def("get_names", &get_names);
    m.def("validate_03", &validate_03);
    m.def("validate_axes_types", &validate_axes_types);
    m.def("int_to_rgba", &int_to_rgba);
    m.def("int_to_rgba_255", &int_to_rgba_255);
    m.def("rgba_to_int", &rgba_to_int);
}
