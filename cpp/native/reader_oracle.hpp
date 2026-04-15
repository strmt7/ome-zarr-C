#pragma once

#include <string>

#include "../../third_party/nlohmann/json.hpp"

namespace ome_zarr_c::native_code {

using ordered_json = nlohmann::ordered_json;

ordered_json reader_probe_matches(const std::string& scenario);
ordered_json reader_probe_node_ops(const std::string& scenario);
ordered_json reader_probe_signature(const std::string& scenario);
ordered_json reader_probe_image_surface();
ordered_json reader_probe_plate_surface();

}  // namespace ome_zarr_c::native_code
