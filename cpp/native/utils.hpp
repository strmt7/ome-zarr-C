#pragma once

#include <stdexcept>
#include <string>
#include <vector>

namespace ome_zarr_c::native_code {

std::string strip_common_prefix(std::vector<std::vector<std::string>>& parts);

std::vector<std::string> splitall(const std::string& path);

}  // namespace ome_zarr_c::native_code
