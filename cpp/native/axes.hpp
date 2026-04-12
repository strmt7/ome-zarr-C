#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace ome_zarr_c::native_code {

struct AxisRecord {
    bool has_name;
    std::string name;
    bool has_type;
    std::string type;
    std::string axis_repr;
    std::string type_repr;
};

std::vector<AxisRecord> axes_to_dicts(const std::vector<AxisRecord>& axes);

std::vector<std::string> get_names(const std::vector<AxisRecord>& axes);

void validate_03(const std::vector<std::string>& names);

void validate_axes_types(const std::vector<AxisRecord>& axes);

}  // namespace ome_zarr_c::native_code
