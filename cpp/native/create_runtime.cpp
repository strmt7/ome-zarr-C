#include "create_runtime.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
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

std::uint64_t parse_tar_octal(
    std::string_view text,
    std::string_view field_name,
    const fs::path& archive_path) {
    std::uint64_t value = 0U;
    bool saw_digit = false;
    bool terminated = false;
    for (const char ch : text) {
        if (ch == '\0' || ch == ' ') {
            if (saw_digit) {
                terminated = true;
            }
            continue;
        }
        if (ch < '0' || ch > '7') {
            throw std::runtime_error(
                "Invalid tar " + std::string(field_name) + " field in " +
                archive_path.string());
        }
        if (terminated) {
            throw std::runtime_error(
                "Invalid tar " + std::string(field_name) + " field in " +
                archive_path.string());
        }
        saw_digit = true;
        const auto digit = static_cast<std::uint64_t>(ch - '0');
        if (value > (std::numeric_limits<std::uint64_t>::max() - digit) / 8U) {
            throw std::runtime_error(
                "Tar " + std::string(field_name) + " field is too large in " +
                archive_path.string());
        }
        value = (value << 3U) + digit;
    }
    if (!saw_digit) {
        throw std::runtime_error(
            "Missing tar " + std::string(field_name) + " field in " +
            archive_path.string());
    }
    return value;
}

bool block_is_zeroed(const std::array<char, 512>& block) {
    return std::all_of(
        block.begin(),
        block.end(),
        [](const char value) { return value == '\0'; });
}

void verify_tar_header_checksum(
    const std::array<char, 512>& header,
    const fs::path& archive_path) {
    const auto expected = parse_tar_octal(
        std::string_view(header.data() + 148, 8),
        "checksum",
        archive_path);
    std::uint64_t actual = 0U;
    for (std::size_t index = 0; index < header.size(); ++index) {
        if (index >= 148U && index < 156U) {
            actual += static_cast<unsigned char>(' ');
        } else {
            actual += static_cast<unsigned char>(header[index]);
        }
    }
    if (actual != expected) {
        throw std::runtime_error(
            "Invalid tar header checksum in " + archive_path.string());
    }
}

std::string tar_entry_name(
    const std::array<char, 512>& header,
    const fs::path& archive_path) {
    const auto name = trim_nul_terminated(std::string_view(header.data(), 100));
    if (name.empty()) {
        throw std::runtime_error("Missing tar entry path in " + archive_path.string());
    }
    const auto prefix =
        trim_nul_terminated(std::string_view(header.data() + 345, 155));
    if (prefix.empty()) {
        return name;
    }
    if (prefix.back() == '/') {
        return prefix + name;
    }
    return prefix + "/" + name;
}

bool has_windows_drive_root_name(std::string_view path_name) {
    return path_name.size() >= 2U &&
        std::isalpha(static_cast<unsigned char>(path_name[0])) != 0 &&
        path_name[1] == ':';
}

struct TarEntry {
    enum class Kind { directory, file };

    Kind kind;
    std::string path;
    std::vector<char> payload;
};

struct ParsedTarHeader {
    std::string path;
    char typeflag;
    std::uint64_t file_size;
    std::uint64_t payload_span;
};

fs::path validated_tar_relative_path(
    const std::string& path_name,
    const TarEntry::Kind kind,
    const fs::path& archive_path) {
    if (path_name.empty()) {
        throw std::runtime_error("Missing tar entry path in " + archive_path.string());
    }
    if (path_name.find('\\') != std::string::npos ||
        has_windows_drive_root_name(path_name)) {
        throw std::runtime_error(
            "Unsafe tar entry path in create archive: " + path_name);
    }
    const fs::path relative_path(path_name);
    if (
        relative_path.is_absolute() ||
        relative_path.has_root_name() ||
        relative_path.has_root_directory()) {
        throw std::runtime_error(
            "Unsafe tar entry path in create archive: " + path_name);
    }
    for (const auto& component : relative_path) {
        const auto component_text = component.generic_string();
        if (component_text == "." || component_text == "..") {
            throw std::runtime_error(
                "Unsafe tar entry path in create archive: " + path_name);
        }
    }
    if (kind == TarEntry::Kind::file && relative_path.filename().empty()) {
        throw std::runtime_error(
            "Malformed tar file entry path in create archive: " + path_name);
    }
    return relative_path;
}

fs::path destination_for_tar_entry(
    const fs::path& destination_root,
    const std::string& path_name,
    const TarEntry::Kind kind,
    const fs::path& archive_path) {
    return destination_root / validated_tar_relative_path(path_name, kind, archive_path);
}

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

void validate_tar_archive_size(
    const fs::path& archive_path,
    const std::uint64_t archive_size) {
    if (archive_size % 512U != 0U) {
        throw std::runtime_error(
            "Malformed tar archive size in " + archive_path.string());
    }
}

void verify_zero_tar_tail(
    std::istream& input,
    std::uint64_t byte_count,
    std::vector<char>& buffer,
    const fs::path& archive_path) {
    while (byte_count > 0U) {
        const auto chunk_size = static_cast<std::size_t>(
            std::min<std::uint64_t>(byte_count, buffer.size()));
        input.read(buffer.data(), static_cast<std::streamsize>(chunk_size));
        if (input.gcount() != static_cast<std::streamsize>(chunk_size)) {
            throw std::runtime_error(
                "Truncated tar zero padding in " + archive_path.string());
        }
        if (!std::all_of(
                buffer.begin(),
                buffer.begin() + static_cast<std::ptrdiff_t>(chunk_size),
                [](const char value) { return value == '\0'; })) {
            throw std::runtime_error(
                "Trailing nonzero tar data in " + archive_path.string());
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

ParsedTarHeader parse_tar_header_info(
    const std::array<char, 512>& header,
    const fs::path& archive_path) {
    verify_tar_header_checksum(header, archive_path);
    const auto file_size = parse_tar_octal(
        std::string_view(header.data() + 124, 12),
        "size",
        archive_path);
    return ParsedTarHeader{
        tar_entry_name(header, archive_path),
        header[156],
        file_size,
        tar_payload_span(file_size, archive_path)};
}

std::vector<TarEntry> parse_tar_entries(
    const fs::path& archive_path,
    const std::uint64_t archive_size) {
    validate_tar_archive_size(archive_path, archive_size);
    std::ifstream stream(archive_path, std::ios::binary);
    if (!stream) {
        throw std::runtime_error("Unable to open create archive: " + archive_path.string());
    }

    std::vector<TarEntry> entries;
    entries.reserve(1024U);
    std::vector<char> discard_buffer(4096U);
    std::uint64_t remaining_bytes = archive_size;
    bool saw_end_marker = false;

    while (remaining_bytes > 0U) {
        std::array<char, 512> header{};
        stream.read(header.data(), static_cast<std::streamsize>(header.size()));
        if (stream.gcount() != static_cast<std::streamsize>(header.size())) {
            throw std::runtime_error("Truncated tar header in " + archive_path.string());
        }
        if (remaining_bytes < header.size()) {
            throw std::runtime_error("Truncated tar header in " + archive_path.string());
        }
        remaining_bytes -= static_cast<std::uint64_t>(header.size());
        if (block_is_zeroed(header)) {
            verify_zero_tar_tail(stream, remaining_bytes, discard_buffer, archive_path);
            remaining_bytes = 0U;
            saw_end_marker = true;
            break;
        }
        const auto entry_header = parse_tar_header_info(header, archive_path);

        if (entry_header.typeflag == 'x' || entry_header.typeflag == 'g') {
            if (entry_header.payload_span > remaining_bytes) {
                throw std::runtime_error("Truncated tar payload in " + archive_path.string());
            }
            read_exact_bytes(stream, entry_header.payload_span, discard_buffer, archive_path);
            remaining_bytes -= entry_header.payload_span;
            continue;
        }
        if (entry_header.typeflag == '5') {
            if (entry_header.file_size != 0U) {
                throw std::runtime_error(
                    "Malformed tar directory entry in " + archive_path.string());
            }
            entries.push_back(TarEntry{TarEntry::Kind::directory, entry_header.path, {}});
            continue;
        }
        if (entry_header.typeflag == '0' || entry_header.typeflag == '\0') {
            if (entry_header.payload_span > remaining_bytes) {
                throw std::runtime_error("Truncated tar payload in " + archive_path.string());
            }
            auto payload = read_payload_bytes(stream, entry_header.file_size, archive_path);
            const auto padding = entry_header.payload_span - entry_header.file_size;
            if (padding > 0U) {
                read_exact_bytes(stream, padding, discard_buffer, archive_path);
            }
            remaining_bytes -= entry_header.payload_span;
            entries.push_back(
                TarEntry{TarEntry::Kind::file, entry_header.path, std::move(payload)});
            continue;
        }
        throw std::runtime_error(
            "Unsupported tar entry type in create archive: " +
            std::string(1, entry_header.typeflag));
    }

    if (!saw_end_marker) {
        throw std::runtime_error("Missing tar end marker in " + archive_path.string());
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
    const auto metadata = archive_metadata(archive_path);
    const bool stream_without_cache =
        custom_asset_root_enabled() ||
        metadata.byte_size > max_cached_tar_archive_bytes;
    if (stream_without_cache) {
        validate_tar_archive_size(archive_path, metadata.byte_size);
        std::ifstream stream(archive_path, std::ios::binary);
        if (!stream) {
            throw std::runtime_error(
                "Unable to open create archive: " + archive_path.string());
        }

        std::vector<char> copy_buffer(1024U * 1024U);
        std::unordered_set<std::string> created_directories;
        created_directories.reserve(256U);
        std::uint64_t remaining_bytes = metadata.byte_size;
        bool saw_end_marker = false;

        while (remaining_bytes > 0U) {
            std::array<char, 512> header{};
            stream.read(header.data(), static_cast<std::streamsize>(header.size()));
            if (stream.gcount() != static_cast<std::streamsize>(header.size())) {
                throw std::runtime_error(
                    "Truncated tar header in " + archive_path.string());
            }
            if (remaining_bytes < header.size()) {
                throw std::runtime_error(
                    "Truncated tar header in " + archive_path.string());
            }
            remaining_bytes -= static_cast<std::uint64_t>(header.size());
            if (block_is_zeroed(header)) {
                verify_zero_tar_tail(stream, remaining_bytes, copy_buffer, archive_path);
                remaining_bytes = 0U;
                saw_end_marker = true;
                break;
            }
            const auto entry_header = parse_tar_header_info(header, archive_path);
            const std::string path_name =
                rewritten_tar_path(entry_header.path, default_label_name, label_name);

            if (entry_header.typeflag == 'x' || entry_header.typeflag == 'g') {
                if (entry_header.payload_span > remaining_bytes) {
                    throw std::runtime_error(
                        "Truncated tar payload in " + archive_path.string());
                }
                read_exact_bytes(stream, entry_header.payload_span, copy_buffer, archive_path);
                remaining_bytes -= entry_header.payload_span;
            } else if (entry_header.typeflag == '5') {
                if (entry_header.file_size != 0U) {
                    throw std::runtime_error(
                        "Malformed tar directory entry in " + archive_path.string());
                }
                const fs::path destination = destination_for_tar_entry(
                    destination_root,
                    path_name,
                    TarEntry::Kind::directory,
                    archive_path);
                ensure_directory_cached(destination, created_directories);
            } else if (entry_header.typeflag == '0' || entry_header.typeflag == '\0') {
                if (entry_header.payload_span > remaining_bytes) {
                    throw std::runtime_error(
                        "Truncated tar payload in " + archive_path.string());
                }
                const fs::path destination = destination_for_tar_entry(
                    destination_root,
                    path_name,
                    TarEntry::Kind::file,
                    archive_path);
                ensure_directory_cached(destination.parent_path(), created_directories);
                std::ofstream output(destination, std::ios::binary | std::ios::trunc);
                if (!output) {
                    throw std::runtime_error(
                        "Unable to write extracted file: " + destination.string());
                }
                read_exact_bytes(
                    stream,
                    entry_header.file_size,
                    copy_buffer,
                    archive_path,
                    &output);
                const auto padding = entry_header.payload_span - entry_header.file_size;
                if (padding > 0U) {
                    read_exact_bytes(stream, padding, copy_buffer, archive_path);
                }
                remaining_bytes -= entry_header.payload_span;
            } else {
                throw std::runtime_error(
                    "Unsupported tar entry type in create archive: " +
                    std::string(1, entry_header.typeflag));
            }
        }
        if (!saw_end_marker) {
            throw std::runtime_error("Missing tar end marker in " + archive_path.string());
        }
        return;
    }

    const auto entries = cached_tar_entries(archive_path, metadata);

    std::unordered_set<std::string> created_directories;
    created_directories.reserve(256U);

    for (const auto& entry : *entries) {
        const std::string path_name =
            rewritten_tar_path(entry.path, default_label_name, label_name);

        if (entry.kind == TarEntry::Kind::directory) {
            const fs::path destination = destination_for_tar_entry(
                destination_root,
                path_name,
                TarEntry::Kind::directory,
                archive_path);
            ensure_directory_cached(destination, created_directories);
            continue;
        }

        const fs::path destination = destination_for_tar_entry(
            destination_root,
            path_name,
            TarEntry::Kind::file,
            archive_path);
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

fs::path destination_parent_or_current(const fs::path& destination_root) {
    const auto parent = destination_root.parent_path();
    return parent.empty() ? fs::path(".") : parent;
}

void validate_destination_root(const fs::path& destination_root) {
    if (destination_root.empty() || destination_root == destination_root.root_path()) {
        throw std::invalid_argument("Create destination must not be empty or a filesystem root");
    }
    const auto filename = destination_root.filename();
    if (filename == "." || filename == "..") {
        throw std::invalid_argument("Create destination must name an output directory");
    }
}

void remove_all_or_throw(const fs::path& path, std::string_view action) {
    std::error_code ec;
    fs::remove_all(path, ec);
    if (ec) {
        throw std::runtime_error(
            "Unable to " + std::string(action) + ": " + path.string() +
            ": " + ec.message());
    }
}

fs::path create_staging_directory(const fs::path& destination_root) {
    const auto parent = destination_parent_or_current(destination_root);
    std::error_code ec;
    fs::create_directories(parent, ec);
    if (ec) {
        throw std::runtime_error(
            "Unable to create destination parent directory: " +
            parent.string() + ": " + ec.message());
    }

    const auto base_name = destination_root.filename().empty()
        ? std::string("ome-zarr-create-output")
        : destination_root.filename().generic_string();
    for (std::uint32_t attempt = 0U; attempt < 1024U; ++attempt) {
        const fs::path candidate =
            parent / ("." + base_name + ".tmp-" + std::to_string(attempt));
        if (fs::create_directory(candidate, ec)) {
            return candidate;
        }
        if (ec) {
            throw std::runtime_error(
                "Unable to create staging directory: " + candidate.string() +
                ": " + ec.message());
        }
    }
    throw std::runtime_error(
        "Unable to allocate staging directory for " + destination_root.string());
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
    validate_destination_root(destination_root);
    const fs::path staging_root = create_staging_directory(destination_root);
    try {
        extract_tar_archive(
            asset.archive_path,
            staging_root,
            asset.default_label_name,
            label_name);
        patch_labels_group_name(staging_root, version, label_name);
        patch_label_group_metadata(staging_root, version, label_name, color_mode, seed);
    } catch (...) {
        remove_all_or_throw(staging_root, "remove failed staging directory");
        throw;
    }

    remove_all_or_throw(destination_root, "replace existing destination");
    std::error_code ec;
    fs::rename(staging_root, destination_root, ec);
    if (ec) {
        remove_all_or_throw(staging_root, "remove failed staging directory");
        throw std::runtime_error(
            "Unable to publish staged create output: " +
            destination_root.string() + ": " + ec.message());
    }
}

}  // namespace ome_zarr_c::native_code
