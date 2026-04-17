#include "create_assets.hpp"

#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>

namespace ome_zarr_c::native_code {

namespace {

namespace fs = std::filesystem;

fs::path repo_root() {
    if (const char* env = std::getenv("OME_ZARR_C_ASSET_ROOT");
        env != nullptr && *env != '\0') {
        return fs::path(env);
    }
#ifdef OME_ZARR_C_SOURCE_ROOT
    return fs::path(OME_ZARR_C_SOURCE_ROOT);
#else
    throw std::runtime_error(
        "OME_ZARR_C_SOURCE_ROOT is not defined and OME_ZARR_C_ASSET_ROOT is unset");
#endif
}

std::string normalized_version_tag(const std::string& version) {
    std::string tag;
    tag.reserve(version.size());
    for (const char ch : version) {
        if (ch != '.') {
            tag.push_back(ch);
        }
    }
    return tag;
}

}  // namespace

CreateAssetSpec create_asset_spec(
    const std::string& method_name,
    const std::string& version) {
    const std::string default_label_name =
        method_name == "astronaut" ? "circles" : "coins";
    if (method_name != "coins" && method_name != "astronaut") {
        throw std::invalid_argument("Unsupported create method: " + method_name);
    }
    if (version != "0.4" && version != "0.5") {
        throw std::invalid_argument("Unsupported create version: " + version);
    }

    const fs::path archive = repo_root() / "cpp" / "assets" / "create" /
        (method_name + "_v" + normalized_version_tag(version) + "_seed0.tar");
    if (!fs::exists(archive)) {
        throw std::runtime_error("Missing create asset archive: " + archive.string());
    }

    return CreateAssetSpec{
        method_name,
        version,
        default_label_name,
        archive,
    };
}

}  // namespace ome_zarr_c::native_code
