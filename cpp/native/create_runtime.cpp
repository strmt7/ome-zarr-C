#include "create_runtime.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "../../third_party/nlohmann/json.hpp"
#include "create_assets.hpp"
#include "python_random.hpp"

namespace ome_zarr_c::native_code {

namespace {

namespace fs = std::filesystem;
using json = nlohmann::ordered_json;
constexpr std::uint64_t max_cached_tar_archive_bytes = 8U * 1024U * 1024U;

struct ArchiveMetadata {
    std::uint64_t byte_size;
    fs::file_time_type modified_time;
};

std::string trim_nul_terminated(std::string_view text) {
    const auto end = text.find('\0');
    if (end == std::string_view::npos) {
        return std::string(text);
    }
    return std::string(text.substr(0, end));
}

std::uint64_t parse_tar_octal(std::string_view text) {
    std::uint64_t value = 0U;
    bool saw_digit = false;
    for (const char ch : text) {
        if (ch == '\0' || ch == ' ') {
            continue;
        }
        if (ch < '0' || ch > '7') {
            break;
        }
        saw_digit = true;
        value = (value << 3U) + static_cast<std::uint64_t>(ch - '0');
    }
    return saw_digit ? value : 0U;
}

bool block_is_zeroed(const std::array<char, 512>& block) {
    return std::all_of(
        block.begin(),
        block.end(),
        [](const char value) { return value == '\0'; });
}

struct TarEntry {
    enum class Kind { directory, file };

    Kind kind;
    std::string path;
    std::vector<char> payload;
};

struct CachedTarArchive {
    std::uint64_t byte_size;
    fs::file_time_type modified_time;
    std::shared_ptr<const std::vector<TarEntry>> entries;
};

bool custom_asset_root_enabled() {
    const char* raw = std::getenv("OME_ZARR_C_ASSET_ROOT");
    return raw != nullptr && *raw != '\0';
}

void ensure_directory_cached(
    const fs::path& path,
    std::unordered_set<std::string>& created_directories) {
    const auto key = path.generic_string();
    if (!created_directories.insert(key).second) {
        return;
    }
    fs::create_directories(path);
}

void read_exact_bytes(
    std::istream& input,
    std::uint64_t byte_count,
    std::vector<char>& buffer,
    const fs::path& archive_path,
    std::ostream* output = nullptr) {
    while (byte_count > 0U) {
        const auto chunk_size = static_cast<std::size_t>(
            std::min<std::uint64_t>(byte_count, buffer.size()));
        input.read(buffer.data(), static_cast<std::streamsize>(chunk_size));
        if (input.gcount() != static_cast<std::streamsize>(chunk_size)) {
            throw std::runtime_error(
                "Truncated tar payload in " + archive_path.string());
        }
        if (output != nullptr) {
            output->write(buffer.data(), static_cast<std::streamsize>(chunk_size));
            if (!(*output)) {
                throw std::runtime_error(
                    "Unable to write extracted file payload from " +
                    archive_path.string());
            }
        }
        byte_count -= static_cast<std::uint64_t>(chunk_size);
    }
}

std::vector<char> read_payload_bytes(
    std::istream& input,
    const std::uint64_t byte_count,
    const fs::path& archive_path) {
    if (byte_count > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
        throw std::runtime_error("Tar payload is too large in " + archive_path.string());
    }
    if (byte_count > static_cast<std::uint64_t>(std::numeric_limits<std::streamsize>::max())) {
        throw std::runtime_error("Tar payload is too large in " + archive_path.string());
    }
    std::vector<char> payload(static_cast<std::size_t>(byte_count));
    if (!payload.empty()) {
        input.read(payload.data(), static_cast<std::streamsize>(payload.size()));
        if (input.gcount() != static_cast<std::streamsize>(payload.size())) {
            throw std::runtime_error(
                "Truncated tar payload in " + archive_path.string());
        }
    }
    return payload;
}

std::uint64_t tar_payload_span(
    const std::uint64_t file_size,
    const fs::path& archive_path) {
    const auto padding = (512U - (file_size % 512U)) % 512U;
    if (file_size > std::numeric_limits<std::uint64_t>::max() - padding) {
        throw std::runtime_error("Tar payload is too large in " + archive_path.string());
    }
    return file_size + padding;
}

std::vector<TarEntry> parse_tar_entries(
    const fs::path& archive_path,
    const std::uint64_t archive_size) {
    std::ifstream stream(archive_path, std::ios::binary);
    if (!stream) {
        throw std::runtime_error("Unable to open create archive: " + archive_path.string());
    }

    std::vector<TarEntry> entries;
    entries.reserve(1024U);
    std::vector<char> discard_buffer(4096U);
    std::uint64_t remaining_bytes = archive_size;

    while (true) {
        std::array<char, 512> header{};
        stream.read(header.data(), static_cast<std::streamsize>(header.size()));
        if (stream.gcount() == 0) {
            break;
        }
        if (stream.gcount() != static_cast<std::streamsize>(header.size())) {
            throw std::runtime_error("Truncated tar header in " + archive_path.string());
        }
        if (remaining_bytes < header.size()) {
            throw std::runtime_error("Truncated tar header in " + archive_path.string());
        }
        remaining_bytes -= static_cast<std::uint64_t>(header.size());
        if (block_is_zeroed(header)) {
            break;
        }

        const auto original_name =
            trim_nul_terminated(std::string_view(header.data(), 100));
        const char typeflag = header[156];
        const auto file_size =
            parse_tar_octal(std::string_view(header.data() + 124, 12));
        const auto payload_span = tar_payload_span(file_size, archive_path);

        if (typeflag == 'x' || typeflag == 'g') {
            if (payload_span > remaining_bytes) {
                throw std::runtime_error("Truncated tar payload in " + archive_path.string());
            }
            read_exact_bytes(stream, payload_span, discard_buffer, archive_path);
            remaining_bytes -= payload_span;
            continue;
        }
        if (typeflag == '5') {
            entries.push_back(TarEntry{TarEntry::Kind::directory, original_name, {}});
            continue;
        }
        if (typeflag == '0' || typeflag == '\0') {
            if (payload_span > remaining_bytes) {
                throw std::runtime_error("Truncated tar payload in " + archive_path.string());
            }
            auto payload = read_payload_bytes(stream, file_size, archive_path);
            const auto padding = payload_span - file_size;
            if (padding > 0U) {
                read_exact_bytes(stream, padding, discard_buffer, archive_path);
            }
            remaining_bytes -= payload_span;
            entries.push_back(
                TarEntry{TarEntry::Kind::file, original_name, std::move(payload)});
            continue;
        }
        throw std::runtime_error(
            "Unsupported tar entry type in create archive: " +
            std::string(1, typeflag));
    }

    return entries;
}

ArchiveMetadata archive_metadata(const fs::path& archive_path) {
    std::ifstream stream(archive_path, std::ios::binary | std::ios::ate);
    if (!stream) {
        throw std::runtime_error("Unable to open create archive: " + archive_path.string());
    }
    const auto size = stream.tellg();
    if (size < std::streampos{0}) {
        throw std::runtime_error("Unable to size create archive: " + archive_path.string());
    }
    std::error_code ec;
    const auto modified_time = fs::last_write_time(archive_path, ec);
    if (ec) {
        throw std::runtime_error("Unable to stat create archive: " + archive_path.string());
    }
    return ArchiveMetadata{static_cast<std::uint64_t>(size), modified_time};
}

std::mutex& tar_cache_mutex() {
    static std::mutex mutex;
    return mutex;
}

std::unordered_map<std::string, CachedTarArchive>& tar_cache() {
    static std::unordered_map<std::string, CachedTarArchive> cache;
    return cache;
}

std::shared_ptr<const std::vector<TarEntry>> cached_tar_entries(
    const fs::path& archive_path,
    const ArchiveMetadata metadata) {
    const auto cache_key = fs::absolute(archive_path).generic_string();
    {
        std::lock_guard<std::mutex> lock(tar_cache_mutex());
        const auto cached = tar_cache().find(cache_key);
        if (
            cached != tar_cache().end() &&
            cached->second.entries != nullptr &&
            cached->second.byte_size == metadata.byte_size &&
            cached->second.modified_time == metadata.modified_time) {
            return cached->second.entries;
        }
    }

    auto parsed = std::make_shared<const std::vector<TarEntry>>(
        parse_tar_entries(archive_path, metadata.byte_size));

    std::lock_guard<std::mutex> lock(tar_cache_mutex());
    auto& cached = tar_cache()[cache_key];
    if (
        cached.entries != nullptr &&
        cached.byte_size == metadata.byte_size &&
        cached.modified_time == metadata.modified_time) {
        return cached.entries;
    }
    cached = CachedTarArchive{metadata.byte_size, metadata.modified_time, parsed};
    return parsed;
}

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
        throw std::runtime_error("Unable to write JSON file: " + path.string());
    }
    stream << payload.dump(2);
}

std::optional<std::uint64_t> create_seed_from_env() {
    const char* raw = std::getenv("OME_ZARR_C_CREATE_SEED");
    if (raw == nullptr || *raw == '\0') {
        return std::nullopt;
    }
    std::string text(raw);
    const bool negative = !text.empty() && text.front() == '-';
    if (negative) {
        text.erase(text.begin());
    }
    if (text.empty() ||
        !std::all_of(text.begin(), text.end(), [](unsigned char ch) {
            return std::isdigit(ch) != 0;
        })) {
        throw std::invalid_argument(
            "OME_ZARR_C_CREATE_SEED must be a decimal integer");
    }
    const auto parsed = static_cast<std::uint64_t>(std::stoull(text));
    return parsed;
}

std::string labels_group_metadata_relpath(const std::string& version) {
    return version == "0.4" ? "labels/.zattrs" : "labels/zarr.json";
}

std::string label_group_metadata_relpath(
    const std::string& version,
    const std::string& label_name) {
    return version == "0.4"
        ? "labels/" + label_name + "/.zattrs"
        : "labels/" + label_name + "/zarr.json";
}

void patch_labels_group_name(
    const fs::path& root,
    const std::string& version,
    const std::string& label_name) {
    const fs::path metadata_path = root / labels_group_metadata_relpath(version);
    json payload = load_json_file(metadata_path);
    if (version == "0.4") {
        payload["labels"] = json::array({label_name});
    } else {
        payload["attributes"]["ome"]["labels"] = json::array({label_name});
    }
    write_json_file(metadata_path, payload);
}

void patch_label_group_metadata(
    const fs::path& root,
    const std::string& version,
    const std::string& label_name,
    const CreateColorMode color_mode,
    const std::optional<std::uint64_t> seed_override) {
    const fs::path metadata_path = root / label_group_metadata_relpath(version, label_name);
    json payload = load_json_file(metadata_path);

    json* image_label = nullptr;
    json* multiscales = nullptr;
    if (version == "0.4") {
        image_label = &payload["image-label"];
        multiscales = &payload["multiscales"];
    } else {
        image_label = &payload["attributes"]["ome"]["image-label"];
        multiscales = &payload["attributes"]["ome"]["multiscales"];
    }

    if (multiscales != nullptr && multiscales->is_array() && !multiscales->empty() &&
        (*multiscales)[0].is_object()) {
        (*multiscales)[0]["name"] = "/labels/" + label_name;
    }

    if (image_label == nullptr || !image_label->is_object()) {
        write_json_file(metadata_path, payload);
        return;
    }

    if (color_mode == CreateColorMode::native_random) {
        const auto seed = seed_override.has_value()
            ? seed_override
            : create_seed_from_env();
        PythonRandom generator = python_random_from_seed(seed);
        json colors = json::array();
        for (int label_value = 1; label_value <= 8; ++label_value) {
            json rgba = json::array();
            for (int component = 0; component < 4; ++component) {
                rgba.push_back(generator.randbelow(256U));
            }
            colors.push_back(
                json{
                    {"label-value", label_value},
                    {"rgba", rgba},
                });
        }
        (*image_label)["colors"] = colors;
    }

    write_json_file(metadata_path, payload);
}

std::string rewritten_tar_path(
    const std::string& original,
    const std::string& default_label_name,
    const std::string& label_name) {
    if (default_label_name == label_name) {
        return original;
    }
    const std::string from_prefix = "labels/" + default_label_name;
    if (original == from_prefix) {
        return "labels/" + label_name;
    }
    if (original.rfind(from_prefix + "/", 0) == 0) {
        return "labels/" + label_name + original.substr(from_prefix.size());
    }
    return original;
}

void extract_tar_archive(
    const fs::path& archive_path,
    const fs::path& destination_root,
    const std::string& default_label_name,
    const std::string& label_name) {
    const auto metadata =
        custom_asset_root_enabled()
            ? ArchiveMetadata{0U, fs::file_time_type{}}
            : archive_metadata(archive_path);
    const bool stream_without_cache =
        custom_asset_root_enabled() ||
        metadata.byte_size > max_cached_tar_archive_bytes;
    if (stream_without_cache) {
        std::ifstream stream(archive_path, std::ios::binary);
        if (!stream) {
            throw std::runtime_error(
                "Unable to open create archive: " + archive_path.string());
        }

        std::vector<char> copy_buffer(1024U * 1024U);
        std::unordered_set<std::string> created_directories;
        created_directories.reserve(256U);

        while (true) {
            std::array<char, 512> header{};
            stream.read(header.data(), static_cast<std::streamsize>(header.size()));
            if (stream.gcount() == 0) {
                break;
            }
            if (stream.gcount() != static_cast<std::streamsize>(header.size())) {
                throw std::runtime_error(
                    "Truncated tar header in " + archive_path.string());
            }
            if (block_is_zeroed(header)) {
                break;
            }

            const std::string original_name =
                trim_nul_terminated(std::string_view(header.data(), 100));
            const std::string path_name =
                rewritten_tar_path(original_name, default_label_name, label_name);
            const char typeflag = header[156];
            const auto file_size =
                parse_tar_octal(std::string_view(header.data() + 124, 12));
            const auto padding = (512U - (file_size % 512U)) % 512U;
            const fs::path destination = destination_root / path_name;

            if (typeflag == 'x' || typeflag == 'g') {
                read_exact_bytes(stream, file_size + padding, copy_buffer, archive_path);
            } else if (typeflag == '5') {
                ensure_directory_cached(destination, created_directories);
            } else if (typeflag == '0' || typeflag == '\0') {
                ensure_directory_cached(destination.parent_path(), created_directories);
                std::ofstream output(destination, std::ios::binary | std::ios::trunc);
                if (!output) {
                    throw std::runtime_error(
                        "Unable to write extracted file: " + destination.string());
                }
                read_exact_bytes(stream, file_size, copy_buffer, archive_path, &output);
                if (padding > 0U) {
                    read_exact_bytes(stream, padding, copy_buffer, archive_path);
                }
            } else {
                throw std::runtime_error(
                    "Unsupported tar entry type in create archive: " +
                    std::string(1, typeflag));
            }
        }
        return;
    }

    const auto entries = cached_tar_entries(archive_path, metadata);

    std::unordered_set<std::string> created_directories;
    created_directories.reserve(256U);

    for (const auto& entry : *entries) {
        const std::string path_name =
            rewritten_tar_path(entry.path, default_label_name, label_name);
        const fs::path destination = destination_root / path_name;

        if (entry.kind == TarEntry::Kind::directory) {
            ensure_directory_cached(destination, created_directories);
            continue;
        }

        ensure_directory_cached(destination.parent_path(), created_directories);
        std::ofstream output(destination, std::ios::binary | std::ios::trunc);
        if (!output) {
            throw std::runtime_error(
                "Unable to write extracted file: " + destination.string());
        }
        if (!entry.payload.empty()) {
            output.write(
                entry.payload.data(),
                static_cast<std::streamsize>(entry.payload.size()));
            if (!output) {
                throw std::runtime_error(
                    "Unable to write extracted file payload: " + destination.string());
            }
        }
    }
}

}  // namespace

void local_create_sample(
    const std::string& zarr_directory,
    const std::string& method_name,
    const std::string& label_name,
    const std::string& version,
    const CreateColorMode color_mode,
    const std::optional<std::uint64_t> seed) {
    const auto asset = create_asset_spec(method_name, version);
    const fs::path destination_root(zarr_directory);
    std::error_code ec;
    fs::remove_all(destination_root, ec);
    fs::create_directories(destination_root.parent_path(), ec);
    extract_tar_archive(
        asset.archive_path,
        destination_root,
        asset.default_label_name,
        label_name);
    patch_labels_group_name(destination_root, version, label_name);
    patch_label_group_metadata(destination_root, version, label_name, color_mode, seed);
}

}  // namespace ome_zarr_c::native_code
