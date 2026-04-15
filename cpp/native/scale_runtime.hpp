#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace ome_zarr_c::native_code {

struct LocalScaleLevelSummary {
    std::vector<std::int64_t> shape;
};

struct LocalScaleResult {
    std::string output_root;
    std::vector<LocalScaleLevelSummary> levels;
    std::vector<std::string> copied_metadata_keys;
};

LocalScaleResult local_scale_array(
    const std::string& input_array,
    const std::string& output_directory,
    const std::string& axes,
    bool copy_metadata,
    const std::string& method,
    bool in_place,
    std::int64_t downscale,
    std::int64_t max_layer);

}  // namespace ome_zarr_c::native_code
