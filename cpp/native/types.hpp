#pragma once

#include "../../third_party/nlohmann/json.hpp"

#include <filesystem>
#include <functional>
#include <string>
#include <vector>

namespace ome_zarr_c::native_code {

using JsonDict = nlohmann::ordered_json;
using PathLike = std::filesystem::path;

struct LayerData {
    JsonDict data = JsonDict::object();
    JsonDict metadata = JsonDict::object();
    std::string name;
    bool has_metadata = false;
    bool has_name = false;
};

using ReaderFunction = std::function<std::vector<LayerData>(const PathLike&)>;

}  // namespace ome_zarr_c::native_code
