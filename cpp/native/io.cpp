#include "io.hpp"

#include <filesystem>
#include <sstream>

namespace ome_zarr_c::native_code {

std::string io_basename(const std::string& path) {
    std::string normalized = path;
    if (normalized.size() > 1 && normalized.back() == '/') {
        normalized.pop_back();
    }
    const auto slash = normalized.find_last_of('/');
    if (slash == std::string::npos) {
        return normalized;
    }
    return normalized.substr(slash + 1);
}

std::vector<std::string> io_parts(const std::string& path, bool is_file) {
    std::vector<std::string> parts;
    if (is_file) {
        for (const auto& part : std::filesystem::path(path)) {
            parts.push_back(part.string());
        }
        return parts;
    }

    std::stringstream stream(path);
    std::string token;
    while (std::getline(stream, token, '/')) {
        parts.push_back(token);
    }
    return parts;
}

std::string io_subpath(
    const std::string& path,
    const std::string& subpath,
    bool is_file,
    bool is_http) {
    if (is_file) {
        const auto target = subpath.empty()
            ? std::filesystem::path(path)
            : std::filesystem::path(path) / subpath;
        return std::filesystem::absolute(target).lexically_normal().string();
    }
    if (is_http) {
        std::string base = path;
        if (!base.empty() && base.back() != '/') {
            base += "/";
        }
        return base + subpath;
    }
    if (!path.empty() && path.back() == '/') {
        return path + subpath;
    }
    return path + "/" + subpath;
}

std::string io_repr(
    const std::string& subpath,
    bool has_zgroup,
    bool has_zarray) {
    std::string suffix;
    if (has_zgroup) {
        suffix += " [zgroup]";
    }
    if (has_zarray) {
        suffix += " [zarray]";
    }
    return subpath + suffix;
}

bool io_protocol_is_http(const std::vector<std::string>& protocols) {
    for (const auto& protocol : protocols) {
        if (protocol == "http" || protocol == "https") {
            return true;
        }
    }
    return false;
}

}  // namespace ome_zarr_c::native_code
