#include "axes.hpp"

#include <algorithm>
#include <map>
#include <sstream>

namespace ome_zarr_c::native_code {

namespace {

const std::map<std::string, std::string> kKnownAxes = {
    {"x", "space"},
    {"y", "space"},
    {"z", "space"},
    {"c", "channel"},
    {"t", "time"},
};

std::string tuple_repr(const std::vector<std::string>& names) {
    std::ostringstream output;
    output << "(";
    for (std::size_t index = 0; index < names.size(); ++index) {
        if (index > 0) {
            output << ", ";
        }
        output << "'" << names[index] << "'";
    }
    if (names.size() == 1) {
        output << ",";
    }
    output << ")";
    return output.str();
}

int last_index(const std::vector<std::string>& values, const std::string& target) {
    for (std::size_t index = values.size(); index-- > 0;) {
        if (values[index] == target) {
            return static_cast<int>(index);
        }
    }
    return -1;
}

}  // namespace

std::vector<AxisRecord> axes_to_dicts(const std::vector<AxisRecord>& axes) {
    std::vector<AxisRecord> result;
    result.reserve(axes.size());

    for (const auto& axis : axes) {
        if (axis.has_name && axis.axis_repr.empty()) {
            AxisRecord materialized;
            materialized.has_name = true;
            materialized.name = axis.name;
            materialized.has_type = false;
            materialized.type = "";
            materialized.axis_repr = "";
            materialized.type_repr = "None";
            const auto known = kKnownAxes.find(axis.name);
            if (known != kKnownAxes.end()) {
                materialized.has_type = true;
                materialized.type = known->second;
                materialized.type_repr = "'" + known->second + "'";
            }
            result.push_back(materialized);
        } else {
            result.push_back(axis);
        }
    }

    return result;
}

std::vector<std::string> get_names(const std::vector<AxisRecord>& axes) {
    std::vector<std::string> names;
    names.reserve(axes.size());
    for (const auto& axis : axes) {
        if (!axis.has_name) {
            throw std::invalid_argument(
                "Axis Dict " + axis.axis_repr + " has no 'name'");
        }
        names.push_back(axis.name);
    }
    return names;
}

void validate_03(const std::vector<std::string>& names) {
    const auto count = names.size();
    if (count == 2) {
        if (!(names[0] == "y" && names[1] == "x")) {
            throw std::invalid_argument(
                "2D data must have axes ('y', 'x') " + tuple_repr(names));
        }
        return;
    }

    if (count == 3) {
        const bool valid =
            (names[0] == "z" && names[1] == "y" && names[2] == "x") ||
            (names[0] == "c" && names[1] == "y" && names[2] == "x") ||
            (names[0] == "t" && names[1] == "y" && names[2] == "x");
        if (!valid) {
            throw std::invalid_argument(
                "3D data must have axes ('z', 'y', 'x') or ('c', 'y', 'x')"
                " or ('t', 'y', 'x'), not " +
                tuple_repr(names));
        }
        return;
    }

    if (count == 4) {
        const bool valid =
            (names[0] == "t" && names[1] == "z" && names[2] == "y" &&
             names[3] == "x") ||
            (names[0] == "c" && names[1] == "z" && names[2] == "y" &&
             names[3] == "x") ||
            (names[0] == "t" && names[1] == "c" && names[2] == "y" &&
             names[3] == "x");
        if (!valid) {
            throw std::invalid_argument(
                "4D data must have axes tzyx or czyx or tcyx");
        }
        return;
    }

    if (names != std::vector<std::string>{"t", "c", "z", "y", "x"}) {
        throw std::invalid_argument(
            "5D data must have axes ('t', 'c', 'z', 'y', 'x')");
    }
}

void validate_axes_types(const std::vector<AxisRecord>& axes) {
    const std::vector<std::string> known_types = {"space", "channel", "time"};
    std::vector<std::string> axis_types;
    axis_types.reserve(axes.size());
    std::vector<std::string> unknown_types;

    for (const auto& axis : axes) {
        if (!axis.has_type) {
            axis_types.push_back("__NONE__");
            unknown_types.push_back(axis.type_repr.empty() ? "None" : axis.type_repr);
            continue;
        }

        axis_types.push_back(axis.type);
        if (std::find(known_types.begin(), known_types.end(), axis.type) ==
            known_types.end()) {
            unknown_types.push_back(axis.type_repr);
        }
    }

    if (unknown_types.size() > 1) {
        std::ostringstream output;
        output << "[";
        for (std::size_t index = 0; index < unknown_types.size(); ++index) {
            if (index > 0) {
                output << ", ";
            }
            output << unknown_types[index];
        }
        output << "]";
        throw std::invalid_argument(
            "Too many unknown axes types. 1 allowed, found: " + output.str());
    }

    if (last_index(axis_types, "time") > 0) {
        throw std::invalid_argument("'time' axis must be first dimension only");
    }

    const auto channel_count = static_cast<int>(
        std::count(axis_types.begin(), axis_types.end(), "channel"));
    if (channel_count > 1) {
        throw std::invalid_argument("Only 1 axis can be type 'channel'");
    }

    const int channel_index = last_index(axis_types, "channel");
    if (channel_index >= 0) {
        const auto first_space = std::find(axis_types.begin(), axis_types.end(), "space");
        if (first_space == axis_types.end()) {
            throw std::invalid_argument("'space' is not in list");
        }
        const int first_space_index =
            static_cast<int>(std::distance(axis_types.begin(), first_space));
        if (channel_index > first_space_index) {
            throw std::invalid_argument("'space' axes must come after 'channel'");
        }
    }
}

}  // namespace ome_zarr_c::native_code
