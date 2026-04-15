#pragma once

#include <filesystem>
#include <string>

namespace ome_zarr_c::native_code {

struct CreateAssetSpec {
    std::string method_name;
    std::string version;
    std::string default_label_name;
    std::filesystem::path archive_path;
};

CreateAssetSpec create_asset_spec(
    const std::string& method_name,
    const std::string& version);

}  // namespace ome_zarr_c::native_code
