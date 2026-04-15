#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace ome_zarr_c::native_code {

enum class CreateColorMode {
    keep_asset_seed,
    native_random,
};

void local_create_sample(
    const std::string& zarr_directory,
    const std::string& method_name,
    const std::string& label_name,
    const std::string& version,
    CreateColorMode color_mode = CreateColorMode::native_random,
    std::optional<std::uint64_t> seed = std::nullopt);

}  // namespace ome_zarr_c::native_code
