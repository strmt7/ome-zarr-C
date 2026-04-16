#include "io.hpp"

#include <filesystem>
#include <fstream>
#include <cstdint>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string_view>

namespace ome_zarr_c::native_code {

namespace {

using json = nlohmann::ordered_json;
namespace fs = std::filesystem;

json load_json_file(const fs::path& path) {
    std::ifstream stream(path);
    if (!stream) {
        throw std::runtime_error("Unable to open JSON file: " + path.string());
    }
    return json::parse(stream);
}

void write_json_file(const fs::path& path, const json& payload) {
    std::ofstream stream(path, std::ios::trunc);
    if (!stream) {
        throw std::runtime_error("Unable to open JSON file for write: " + path.string());
    }
    stream << payload.dump(2);
}

std::optional<fs::path> metadata_path(const fs::path& path) {
    const fs::path zarr_json = path / "zarr.json";
    if (fs::exists(zarr_json)) {
        return zarr_json;
    }
    const fs::path zattrs = path / ".zattrs";
    if (fs::exists(zattrs)) {
        return zattrs;
    }
    return std::nullopt;
}

std::string json_scalar_to_string(const json& value) {
    if (value.is_string()) {
        return value.get<std::string>();
    }
    if (value.is_number_integer()) {
        return std::to_string(value.get<std::int64_t>());
    }
    if (value.is_number_unsigned()) {
        return std::to_string(value.get<std::uint64_t>());
    }
    if (value.is_number_float()) {
        std::ostringstream output;
        output << value.get<double>();
        return output.str();
    }
    return "";
}

std::string detect_metadata_version(const json& metadata) {
    if (metadata.is_object() && metadata.contains("version")) {
        return json_scalar_to_string(metadata["version"]);
    }
    if (metadata.is_object() && metadata.contains("plate") &&
        metadata["plate"].is_object() &&
        metadata["plate"].contains("version")) {
        return json_scalar_to_string(metadata["plate"]["version"]);
    }
    if (metadata.is_object() && metadata.contains("multiscales") &&
        metadata["multiscales"].is_array() && !metadata["multiscales"].empty() &&
        metadata["multiscales"][0].is_object() &&
        metadata["multiscales"][0].contains("version")) {
        return json_scalar_to_string(metadata["multiscales"][0]["version"]);
    }
    return "";
}

bool path_has_zarray(const fs::path& path) {
    if (fs::exists(path / ".zarray")) {
        return true;
    }
    const fs::path zarr_json = path / "zarr.json";
    if (!fs::exists(zarr_json)) {
        return false;
    }
    const json payload = load_json_file(zarr_json);
    return payload.is_object() && payload.contains("node_type") &&
        payload["node_type"].is_string() &&
        payload["node_type"].get<std::string>() == "array";
}

json root_attrs_from_payload(const json& payload, const fs::path& path) {
    if (path.filename() == "zarr.json") {
        if (payload.is_object() && payload.contains("attributes") &&
            payload["attributes"].is_object()) {
            const auto& attrs = payload["attributes"];
            if (attrs.contains("ome")) {
                return attrs["ome"];
            }
            return attrs;
        }
        return json::object();
    }
    if (payload.is_object()) {
        return payload;
    }
    return json::object();
}

std::optional<std::string> detected_version_for_path(const fs::path& path) {
    const auto metadata = metadata_path(path);
    if (!metadata.has_value()) {
        return std::nullopt;
    }
    const json payload = load_json_file(metadata.value());
    const json root_attrs = root_attrs_from_payload(payload, metadata.value());
    const std::string version = detect_metadata_version(root_attrs);
    if (version.empty()) {
        return std::nullopt;
    }
    return version;
}

void ensure_group_exists_for_version(
    const fs::path& path,
    const std::string& requested_version) {
    fs::create_directories(path);
    const bool use_v3 = requested_version == "0.5";
    if (use_v3) {
        write_json_file(
            path / "zarr.json",
            json{
                {"attributes", json::object()},
                {"zarr_format", 3},
                {"node_type", "group"},
            });
        return;
    }
    write_json_file(path / ".zgroup", json{{"zarr_format", 2}});
    write_json_file(path / ".zattrs", json::object());
}

json build_location_signature(
    const std::string& normalized_path,
    const std::string& mode,
    const std::string& fmt_version,
    const json& root_attrs,
    const bool has_zgroup,
    const bool has_zarray) {
    const auto parts = io_parts(normalized_path, true);
    const auto subpath_empty = io_subpath(normalized_path, "", true, false);
    return json{
        {"type", "ZarrLocation"},
        {"exists", true},
        {"fmt_version", fmt_version},
        {"mode", mode},
        {"version", fmt_version},
        {"path", normalized_path},
        {"basename", io_basename(normalized_path)},
        {"parts", parts},
        {"subpath_empty", subpath_empty},
        {"subpath_nested", io_subpath(normalized_path, "nested/data", true, false)},
        {"repr", io_repr(subpath_empty, has_zgroup, has_zarray)},
        {"root_attrs", root_attrs},
    };
}

}  // namespace

IoConstructorPlan io_constructor_plan(
    IoPathKind path_kind,
    const std::string& raw_path) {
    IoConstructorPlan plan{};
    plan.use_input_store =
        path_kind == IoPathKind::fsspec_store ||
        path_kind == IoPathKind::local_store;
    plan.normalized_path = raw_path;
    return plan;
}

IoMetadataPlan io_metadata_plan(
    bool metadata_loaded,
    bool mode_is_write,
    bool has_ome_namespace) {
    IoMetadataPlan plan{};
    if (metadata_loaded) {
        plan.exists = true;
        plan.unwrap_ome_namespace = has_ome_namespace;
        return plan;
    }
    plan.create_group = mode_is_write;
    plan.exists = mode_is_write;
    plan.unwrap_ome_namespace = false;
    return plan;
}

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
        std::string normalized =
            std::filesystem::absolute(target).lexically_normal().string();
        while (normalized.size() > 1 && normalized.back() == '/') {
            normalized.pop_back();
        }
        return normalized;
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

bool io_is_local_store(IoPathKind path_kind) {
    return path_kind == IoPathKind::local_store;
}

bool io_parse_url_returns_none(const std::string& mode, bool exists) {
    return mode.find('r') != std::string::npos && !exists;
}

bool io_protocol_is_http(const std::vector<std::string>& protocols) {
    for (const auto& protocol : protocols) {
        if (protocol == "http" || protocol == "https") {
            return true;
        }
    }
    return false;
}

LocalIoSignature local_io_signature(
    const std::string& path,
    const std::string& mode,
    const std::string& requested_version,
    const std::string& create_subpath,
    const bool use_create_subpath) {
    const fs::path base_path(path);
    std::string normalized_path = path;
    if (use_create_subpath) {
        normalized_path = io_subpath(path, create_subpath, true, false);
    }
    const fs::path target_path(normalized_path);

    const bool read_mode = mode.find('r') != std::string::npos;
    const bool write_mode = mode.find('w') != std::string::npos;
    const bool path_exists = fs::exists(target_path);
    std::string fmt_version = requested_version.empty() ? "0.5" : requested_version;

    if (!use_create_subpath && read_mode && !path_exists) {
        return LocalIoSignature{true, nullptr};
    }

    if (!use_create_subpath && write_mode && !path_exists) {
        ensure_group_exists_for_version(target_path, fmt_version);
    }

    json root_attrs = json::object();
    bool has_zgroup = false;
    bool has_zarray = false;
    if (const auto existing_metadata = metadata_path(target_path); existing_metadata.has_value()) {
        const json payload = load_json_file(existing_metadata.value());
        root_attrs = root_attrs_from_payload(payload, existing_metadata.value());
        has_zgroup = !root_attrs.empty();
        has_zarray = path_has_zarray(target_path);
    }

    if (const std::string detected_version = detect_metadata_version(root_attrs);
        !detected_version.empty()) {
        fmt_version = detected_version;
    } else {
        const auto parent_version = detected_version_for_path(base_path);
        if (use_create_subpath && !path_exists && parent_version.has_value()) {
        fmt_version = *parent_version;
        }
    }

    if (!fs::exists(target_path)) {
        return LocalIoSignature{
            false,
            json{
                {"type", "ZarrLocation"},
                {"exists", false},
                {"fmt_version", fmt_version},
                {"mode", mode},
                {"version", fmt_version},
                {"path", normalized_path},
                {"basename", io_basename(normalized_path)},
                {"parts", io_parts(normalized_path, true)},
                {"subpath_empty", io_subpath(normalized_path, "", true, false)},
                {"subpath_nested", io_subpath(normalized_path, "nested/data", true, false)},
                {"repr", io_repr(io_subpath(normalized_path, "", true, false), false, false)},
                {"root_attrs", json::object()},
            },
        };
    }

    return LocalIoSignature{
        false,
        build_location_signature(
            normalized_path,
            mode,
            fmt_version,
            root_attrs,
            has_zgroup,
            has_zarray),
    };
}

}  // namespace ome_zarr_c::native_code
