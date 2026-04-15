#include "local_runtime.hpp"

#include <algorithm>
#include <cstdint>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <optional>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>
#include <sys/stat.h>

#include <blosc.h>
#include <zstd.h>

#include "../../third_party/nlohmann/json.hpp"
#include "../../third_party/tinyxml2/tinyxml2.h"
#include "io.hpp"

namespace ome_zarr_c::native_code {

namespace {

using json = nlohmann::json;
namespace fs = std::filesystem;

bool json_truthy(const json& value) {
    if (value.is_null()) {
        return false;
    }
    if (value.is_boolean()) {
        return value.get<bool>();
    }
    if (value.is_number_integer()) {
        return value.get<std::int64_t>() != 0;
    }
    if (value.is_number_unsigned()) {
        return value.get<std::uint64_t>() != 0;
    }
    if (value.is_number_float()) {
        return value.get<double>() != 0.0;
    }
    if (value.is_string()) {
        return !value.get_ref<const std::string&>().empty();
    }
    if (value.is_array() || value.is_object()) {
        return !value.empty();
    }
    return false;
}

std::optional<fs::path> metadata_json_path(const fs::path& path) {
    const fs::path zattrs = path / ".zattrs";
    if (fs::exists(zattrs)) {
        return zattrs;
    }
    const fs::path zarr_json = path / "zarr.json";
    if (fs::exists(zarr_json)) {
        return zarr_json;
    }
    return std::nullopt;
}

bool is_local_zarr_root(const fs::path& path) {
    return fs::exists(path / ".zgroup") || fs::exists(path / ".zattrs") ||
           fs::exists(path / ".zarray") || fs::exists(path / "zarr.json");
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
        throw std::runtime_error("Unable to open JSON file for write: " + path.string());
    }
    stream << payload.dump(2);
    stream << "\n";
}

json load_json_or_empty(const fs::path& path) {
    const auto metadata_path = metadata_json_path(path);
    if (!metadata_path.has_value()) {
        return json::object();
    }
    return load_json_file(metadata_path.value());
}

json unwrap_ome_namespace(const json& metadata) {
    if (metadata.is_object() &&
        metadata.contains("attributes") &&
        metadata["attributes"].is_object() &&
        metadata["attributes"].contains("ome")) {
        return metadata["attributes"]["ome"];
    }
    return metadata;
}

std::string generic_path_string(const fs::path& path) {
    return path.generic_string();
}

std::string basename_string(const fs::path& path) {
    return path.filename().generic_string();
}

std::string dirname_string(const fs::path& path) {
    return path.parent_path().generic_string();
}

bool element_name_is_image(const char* name) {
    if (name == nullptr) {
        return false;
    }
    const std::string_view text(name);
    if (text == "Image") {
        return true;
    }
    if (text.size() > 6 && text.substr(text.size() - 6) == ":Image") {
        return true;
    }
    if (text.size() > 6 && text.substr(text.size() - 6) == "}Image") {
        return true;
    }
    return false;
}

std::vector<UtilsDiscoveredImage> bioformats_images(
    const fs::path& path_to_zattrs) {
    std::vector<UtilsDiscoveredImage> images;
    const fs::path xml_path = path_to_zattrs / "OME" / "METADATA.ome.xml";
    if (!fs::exists(xml_path)) {
        return images;
    }

    tinyxml2::XMLDocument doc;
    if (doc.LoadFile(xml_path.string().c_str()) != tinyxml2::XML_SUCCESS) {
        return images;
    }

    const auto* root = doc.RootElement();
    if (root == nullptr) {
        return images;
    }

    std::size_t series = 0;
    const std::string base_name = basename_string(path_to_zattrs);
    const std::string base_dir = dirname_string(path_to_zattrs);
    for (const auto* child = root->FirstChildElement();
         child != nullptr;
         child = child->NextSiblingElement()) {
        if (!element_name_is_image(child->Name())) {
            continue;
        }

        const char* xml_name = child->Attribute("Name");
        const std::string image_name =
            xml_name != nullptr
                ? std::string(xml_name)
                : base_name + " Series:" + std::to_string(series);
        images.push_back(
            UtilsDiscoveredImage{
                generic_path_string(path_to_zattrs / std::to_string(series)),
                image_name,
                base_dir,
            });
        series += 1;
    }
    return images;
}

std::vector<std::vector<std::int64_t>> dataset_shapes(const fs::path& root, const json& metadata) {
    std::vector<std::vector<std::int64_t>> shapes;
    if (!metadata.is_object() || !metadata.contains("multiscales") ||
        !metadata["multiscales"].is_array() || metadata["multiscales"].empty()) {
        return shapes;
    }

    const auto& first = metadata["multiscales"][0];
    if (!first.is_object() || !first.contains("datasets") || !first["datasets"].is_array()) {
        return shapes;
    }

    for (const auto& dataset : first["datasets"]) {
        if (!dataset.is_object() || !dataset.contains("path") || !dataset["path"].is_string()) {
            continue;
        }
        const fs::path dataset_path = root / dataset["path"].get<std::string>();
        const auto array_json = load_json_or_empty(dataset_path);
        if (!array_json.is_object() || !array_json.contains("shape") || !array_json["shape"].is_array()) {
            continue;
        }
        std::vector<std::int64_t> shape;
        for (const auto& dim : array_json["shape"]) {
            if (dim.is_number_integer() || dim.is_number_unsigned()) {
                shape.push_back(dim.get<std::int64_t>());
            }
        }
        if (!shape.empty()) {
            shapes.push_back(std::move(shape));
        }
    }
    return shapes;
}

std::string shape_repr(const std::vector<std::int64_t>& shape) {
    std::ostringstream output;
    output << "(";
    for (std::size_t index = 0; index < shape.size(); ++index) {
        if (index > 0) {
            output << ", ";
        }
        output << shape[index];
    }
    if (shape.size() == 1) {
        output << ",";
    }
    output << ")";
    return output.str();
}

std::vector<std::string> multiscales_axes_names(const json& metadata) {
    std::vector<std::string> axes;
    if (!metadata.is_object() || !metadata.contains("multiscales") ||
        !metadata["multiscales"].is_array() || metadata["multiscales"].empty()) {
        return axes;
    }
    const auto& first = metadata["multiscales"][0];
    if (!first.is_object() || !first.contains("axes") || !first["axes"].is_array()) {
        return axes;
    }
    for (const auto& axis : first["axes"]) {
        if (axis.is_string()) {
            axes.push_back(axis.get<std::string>());
        } else if (axis.is_object() && axis.contains("name") && axis["name"].is_string()) {
            axes.push_back(axis["name"].get<std::string>());
        }
    }
    return axes;
}

std::string metadata_version(const json& metadata) {
    if (metadata.is_object() && metadata.contains("version") && metadata["version"].is_string()) {
        return metadata["version"].get<std::string>();
    }
    if (metadata.is_object() && metadata.contains("multiscales") &&
        metadata["multiscales"].is_array() && !metadata["multiscales"].empty() &&
        metadata["multiscales"][0].is_object() &&
        metadata["multiscales"][0].contains("version") &&
        metadata["multiscales"][0]["version"].is_string()) {
        return metadata["multiscales"][0]["version"].get<std::string>();
    }
    return "";
}

int root_zarr_format(const fs::path& source_root) {
    const fs::path zgroup = source_root / ".zgroup";
    if (fs::exists(zgroup)) {
        const auto metadata = load_json_file(zgroup);
        if (metadata.is_object() && metadata.contains("zarr_format")) {
            return metadata["zarr_format"].get<int>();
        }
    }
    const fs::path zarr_json = source_root / "zarr.json";
    if (fs::exists(zarr_json)) {
        const auto metadata = load_json_file(zarr_json);
        if (metadata.is_object() && metadata.contains("zarr_format")) {
            return metadata["zarr_format"].get<int>();
        }
    }
    return 0;
}

int output_zarr_format_from_version(const std::string& version) {
    if (version == "0.1" || version == "0.2" || version == "0.3" || version == "0.4") {
        return 2;
    }
    if (version == "0.5") {
        return 3;
    }
    return 0;
}

std::string zarr_v2_dtype_from_data_type(const std::string& data_type) {
    if (data_type == "int8") return "|i1";
    if (data_type == "uint8") return "|u1";
    if (data_type == "int16") return "<i2";
    if (data_type == "uint16") return "<u2";
    if (data_type == "int32") return "<i4";
    if (data_type == "uint32") return "<u4";
    if (data_type == "int64") return "<i8";
    if (data_type == "uint64") return "<u8";
    if (data_type == "float32") return "<f4";
    if (data_type == "float64") return "<f8";
    if (data_type == "bool") return "|b1";
    if (!data_type.empty() && (data_type[0] == '<' || data_type[0] == '|' || data_type[0] == '>')) {
        return data_type;
    }
    throw std::invalid_argument("Unsupported data_type for v2 rewrite: " + data_type);
}

std::vector<fs::path> dataset_paths_from_metadata(const json& metadata) {
    std::vector<fs::path> dataset_paths;
    if (!metadata.is_object() || !metadata.contains("multiscales") ||
        !metadata["multiscales"].is_array() || metadata["multiscales"].empty()) {
        return dataset_paths;
    }
    const auto& first = metadata["multiscales"][0];
    if (!first.is_object() || !first.contains("datasets") || !first["datasets"].is_array()) {
        return dataset_paths;
    }
    for (const auto& dataset : first["datasets"]) {
        if (dataset.is_object() && dataset.contains("path") && dataset["path"].is_string()) {
            dataset_paths.emplace_back(dataset["path"].get<std::string>());
        }
    }
    return dataset_paths;
}

std::size_t data_type_size(const std::string& data_type) {
    if (data_type == "int8" || data_type == "uint8" || data_type == "bool") return 1;
    if (data_type == "int16" || data_type == "uint16") return 2;
    if (data_type == "int32" || data_type == "uint32" || data_type == "float32") return 4;
    if (data_type == "int64" || data_type == "uint64" || data_type == "float64") return 8;
    if (!data_type.empty() && (data_type[0] == '<' || data_type[0] == '|' || data_type[0] == '>')) {
        const auto digits = data_type.substr(2);
        return static_cast<std::size_t>(std::stoull(digits));
    }
    throw std::invalid_argument("Unsupported data_type size: " + data_type);
}

std::vector<std::int64_t> json_int_vector(const json& values) {
    std::vector<std::int64_t> result;
    for (const auto& value : values) {
        result.push_back(value.get<std::int64_t>());
    }
    return result;
}

std::size_t chunk_uncompressed_nbytes(const fs::path& relative, const json& source_metadata) {
    const auto shape = json_int_vector(source_metadata.at("shape"));
    const auto chunk_shape = json_int_vector(
        source_metadata.at("chunk_grid").at("configuration").at("chunk_shape"));
    const auto item_size = data_type_size(source_metadata.at("data_type").get<std::string>());
    std::vector<std::int64_t> indices;
    for (const auto& part : relative) {
        indices.push_back(std::stoll(part.generic_string()));
    }
    if (indices.size() != shape.size()) {
        throw std::invalid_argument("Chunk index rank mismatch for " + relative.generic_string());
    }
    std::size_t elements = 1;
    for (std::size_t axis = 0; axis < shape.size(); ++axis) {
        const auto start = indices[axis] * chunk_shape[axis];
        const auto extent = std::min<std::int64_t>(chunk_shape[axis], shape[axis] - start);
        elements *= static_cast<std::size_t>(extent);
    }
    return elements * item_size;
}

std::vector<char> read_binary_file(const fs::path& path) {
    std::ifstream stream(path, std::ios::binary);
    if (!stream) {
        throw std::runtime_error("Unable to open binary file: " + path.string());
    }
    return std::vector<char>(std::istreambuf_iterator<char>(stream), std::istreambuf_iterator<char>());
}

void write_binary_file(const fs::path& path, const std::vector<char>& payload) {
    std::ofstream stream(path, std::ios::binary | std::ios::trunc);
    if (!stream) {
        throw std::runtime_error("Unable to write binary file: " + path.string());
    }
    stream.write(payload.data(), static_cast<std::streamsize>(payload.size()));
}

std::vector<char> transcode_chunk_to_v2(
    const std::vector<char>& source_bytes,
    const json& source_metadata,
    const fs::path& relative) {
    const auto raw_size = chunk_uncompressed_nbytes(relative, source_metadata);
    std::vector<char> raw(raw_size);
    const auto decompressed = ZSTD_decompress(
        raw.data(),
        raw.size(),
        source_bytes.data(),
        source_bytes.size());
    if (ZSTD_isError(decompressed) != 0U) {
        throw std::runtime_error(
            "ZSTD decompress failed: " +
            std::string(ZSTD_getErrorName(decompressed)));
    }
    raw.resize(static_cast<std::size_t>(decompressed));

    static const bool blosc_initialized = [] {
        blosc_init();
        return true;
    }();
    (void)blosc_initialized;

    std::vector<char> encoded(raw.size() + BLOSC_MAX_OVERHEAD);
    const auto type_size = data_type_size(source_metadata.at("data_type").get<std::string>());
    const int compressed = blosc_compress_ctx(
        5,
        BLOSC_SHUFFLE,
        static_cast<int>(type_size),
        raw.size(),
        raw.data(),
        encoded.data(),
        encoded.size(),
        "lz4",
        0,
        1);
    if (compressed <= 0) {
        throw std::runtime_error("Blosc compression failed for v2 chunk rewrite");
    }
    encoded.resize(static_cast<std::size_t>(compressed));
    return encoded;
}

void copy_dataset_chunks(
    const fs::path& source_dataset,
    const fs::path& destination_dataset,
    const int output_zarr_format,
    const json& source_metadata) {
    const fs::path source_chunk_root =
        fs::exists(source_dataset / "c") ? source_dataset / "c" : source_dataset;
    if (!fs::exists(source_chunk_root)) {
        return;
    }
    for (const auto& entry : fs::recursive_directory_iterator(source_chunk_root)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        const auto filename = entry.path().filename().generic_string();
        if (source_chunk_root == source_dataset &&
            (filename == ".zarray" || filename == ".zattrs" || filename == ".zgroup" ||
             filename == "zarr.json")) {
            continue;
        }
        const auto relative = fs::relative(entry.path(), source_chunk_root);
        const fs::path target =
            output_zarr_format == 2 ? destination_dataset / relative : destination_dataset / "c" / relative;
        fs::create_directories(target.parent_path());
        if (output_zarr_format == 2) {
            write_binary_file(
                target,
                transcode_chunk_to_v2(read_binary_file(entry.path()), source_metadata, relative));
        } else {
            fs::copy_file(entry.path(), target, fs::copy_options::overwrite_existing);
        }
    }
}

void write_dataset_metadata(
    const fs::path& source_dataset,
    const fs::path& destination_dataset,
    const int output_zarr_format,
    const std::vector<std::string>& axis_names) {
    const auto source_metadata = load_json_file(source_dataset / "zarr.json");
    if (output_zarr_format == 2) {
        json payload = {
            {"shape", source_metadata.at("shape")},
            {"chunks", source_metadata.at("chunk_grid").at("configuration").at("chunk_shape")},
            {"dtype", zarr_v2_dtype_from_data_type(source_metadata.at("data_type").get<std::string>())},
            {"fill_value", source_metadata.contains("fill_value") ? source_metadata.at("fill_value") : json(0)},
            {"order", "C"},
            {"filters", nullptr},
            {"dimension_separator", "/"},
            {"compressor", {
                {"id", "blosc"},
                {"cname", "lz4"},
                {"clevel", 5},
                {"shuffle", 1},
                {"blocksize", 0},
            }},
            {"zarr_format", 2},
        };
        fs::create_directories(destination_dataset);
        write_json_file(destination_dataset / ".zarray", payload);
        write_json_file(destination_dataset / ".zattrs", json::object());
        return;
    }

    json payload = source_metadata;
    if (!axis_names.empty()) {
        payload["dimension_names"] = axis_names;
    }
    fs::create_directories(destination_dataset);
    write_json_file(destination_dataset / "zarr.json", payload);
}

void write_group_metadata(
    const fs::path& destination_root,
    const int output_zarr_format,
    const json& metadata) {
    fs::create_directories(destination_root);
    if (output_zarr_format == 2) {
        write_json_file(destination_root / ".zgroup", json{{"zarr_format", 2}});
        write_json_file(destination_root / ".zattrs", metadata);
        return;
    }

    write_json_file(
        destination_root / "zarr.json",
        json{
            {"attributes", {{"ome", metadata}}},
            {"zarr_format", 3},
            {"node_type", "group"},
        });
}

std::string percent_encode(std::string_view text) {
    std::ostringstream output;
    output << std::uppercase << std::hex;
    for (const unsigned char ch : text) {
        const bool unreserved =
            (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
            (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' || ch == '.' ||
            ch == '~';
        if (unreserved) {
            output << static_cast<char>(ch);
            continue;
        }
        output << '%' << std::setw(2) << std::setfill('0')
               << static_cast<int>(ch);
    }
    return output.str();
}

std::string safe_uploaded_timestamp(const fs::path& path) {
    try {
        struct stat status {};
        if (::stat(path.c_str(), &status) != 0) {
            return "";
        }
        std::time_t time = status.st_mtime;
        std::tm local_tm{};
#if defined(_WIN32)
        localtime_s(&local_tm, &time);
#else
        localtime_r(&time, &local_tm);
#endif
        std::ostringstream output;
        output << std::put_time(&local_tm, "%Y-%m-%d %H:%M:%S.");
        return output.str();
    } catch (const std::exception&) {
        return "";
    }
}

std::string csv_escape(std::string_view text) {
    const bool needs_quotes =
        text.find_first_of(",\"\n\r") != std::string_view::npos;
    if (!needs_quotes) {
        return std::string(text);
    }
    std::string escaped;
    escaped.reserve(text.size() + 2);
    escaped.push_back('"');
    for (const char ch : text) {
        if (ch == '"') {
            escaped.push_back('"');
        }
        escaped.push_back(ch);
    }
    escaped.push_back('"');
    return escaped;
}

}  // namespace

LocalFindMultiscalesResult local_find_multiscales(const std::string& input_path) {
    LocalFindMultiscalesResult result{};
    const fs::path path_to_zattrs(input_path);
    const auto metadata_path = metadata_json_path(path_to_zattrs);
    if (!metadata_path.has_value()) {
        result.metadata_missing = true;
        return result;
    }

    const json zattrs = unwrap_ome_namespace(load_json_file(metadata_path.value()));
    const std::string native_path = generic_path_string(path_to_zattrs);
    const std::string base_name = basename_string(path_to_zattrs);
    const std::string base_dir = dirname_string(path_to_zattrs);

    if (zattrs.contains("plate") && zattrs["plate"].is_object()) {
        const auto& plate = zattrs["plate"];
        if (plate.contains("wells") && plate["wells"].is_array() && !plate["wells"].empty()) {
            const auto& first_well = plate["wells"][0];
            if (first_well.is_object() && first_well.contains("path") && first_well["path"].is_string()) {
                result.images.push_back(
                    UtilsDiscoveredImage{
                        generic_path_string(
                            path_to_zattrs / first_well["path"].get<std::string>() / "0"),
                        base_name,
                        base_dir,
                    });
            }
            return result;
        }
        result.logged_no_wells = true;
        result.logged_no_wells_path = native_path;
        return result;
    }

    if (zattrs.contains("bioformats2raw.layout") &&
        zattrs["bioformats2raw.layout"].is_number_integer() &&
        zattrs["bioformats2raw.layout"].get<std::int64_t>() == 3) {
        result.images = bioformats_images(path_to_zattrs);
        return result;
    }

    if (zattrs.contains("multiscales") && json_truthy(zattrs["multiscales"])) {
        result.images.push_back(
            UtilsDiscoveredImage{native_path, base_name, base_dir});
    }

    return result;
}

std::vector<UtilsDiscoveredImage> local_walk_ome_zarr(const std::string& input_path) {
    const fs::path root(input_path);
    std::vector<UtilsDiscoveredImage> discovered;

    std::function<void(const fs::path&)> walk = [&](const fs::path& path) {
        const auto direct = local_find_multiscales(generic_path_string(path));
        if (direct.metadata_missing) {
            if (!fs::exists(path) || !fs::is_directory(path)) {
                return;
            }
            for (const auto& child : fs::directory_iterator(path)) {
                if (!child.is_directory()) {
                    continue;
                }
                const auto nested_direct = local_find_multiscales(generic_path_string(child.path()));
                if (!nested_direct.metadata_missing) {
                    discovered.insert(
                        discovered.end(),
                        nested_direct.images.begin(),
                        nested_direct.images.end());
                } else {
                    walk(child.path());
                }
            }
            return;
        }

        discovered.insert(
            discovered.end(),
            direct.images.begin(),
            direct.images.end());
    };

    walk(root);
    return discovered;
}

std::vector<std::string> local_info_lines(const std::string& input_path) {
    const fs::path root(input_path);
    if (!fs::exists(root)) {
        throw std::invalid_argument("not a zarr: None");
    }

    const auto metadata_path = metadata_json_path(root);
    if (!metadata_path.has_value()) {
        return {};
    }

    const json metadata = unwrap_ome_namespace(load_json_file(metadata_path.value()));
    if (!(metadata.contains("multiscales") && json_truthy(metadata["multiscales"]))) {
        return {};
    }

    std::string version;
    if (metadata.contains("version") && metadata["version"].is_string()) {
        version = metadata["version"].get<std::string>();
    } else if (metadata["multiscales"].is_array() &&
               !metadata["multiscales"].empty() &&
               metadata["multiscales"][0].is_object() &&
               metadata["multiscales"][0].contains("version") &&
               metadata["multiscales"][0]["version"].is_string()) {
        version = metadata["multiscales"][0]["version"].get<std::string>();
    }

    std::vector<std::string> lines = utils_info_header_lines(
        io_repr(generic_path_string(root), true, false),
        version,
        {"Multiscales"});

    for (const auto& shape : dataset_shapes(root, metadata)) {
        lines.push_back(utils_info_data_line(shape_repr(shape), std::nullopt));
    }

    return lines;
}

LocalFinderResult local_finder_csv(const std::string& input_path, const int port) {
    LocalFinderResult result{};
    const auto plan = utils_finder_plan(input_path, port);
    result.csv_path = plan.csv_path;
    result.source_uri = plan.source_uri;

    const auto discovered = local_walk_ome_zarr(input_path);
    result.found_any = !discovered.empty();
    if (!result.found_any) {
        return result;
    }

    const fs::path root(input_path);
    result.rows.reserve(discovered.size());
    for (const auto& image : discovered) {
        const fs::path image_path(image.path);
        const fs::path dirname(image.dirname);
        const auto relpath = generic_path_string(fs::relative(image_path, root));
        const auto folders_path = generic_path_string(fs::relative(dirname, root));
        result.rows.push_back(
            utils_finder_row(
                port,
                plan.server_dir,
                relpath,
                image.name.empty() ? basename_string(image_path) : image.name,
                folders_path,
                safe_uploaded_timestamp(image_path)));
    }

    std::ofstream csv(plan.csv_path, std::ios::trunc);
    if (!csv) {
        throw std::runtime_error("Unable to open CSV file: " + plan.csv_path);
    }
    csv << "File Path,File Name,Folders,Uploaded\r\n";
    for (const auto& row : result.rows) {
        csv << csv_escape(row.file_path) << "," << csv_escape(row.name) << ","
            << csv_escape(row.folders) << "," << csv_escape(row.uploaded) << "\r\n";
    }

    const json source = {
        {"uri", plan.source_uri},
        {"type", "csv"},
        {"name", "biofile_finder.csv"},
    };
    result.app_url = plan.url + "?source=" + percent_encode(source.dump()) + "&v=2";
    return result;
}

LocalDownloadResult local_download_copy(
    const std::string& input_path,
    const std::string& output_dir) {
    fs::path source(input_path);
    if (source.filename().empty()) {
        source = source.parent_path();
    }
    if (!fs::exists(source) || !fs::is_directory(source) || !is_local_zarr_root(source)) {
        throw std::invalid_argument("not a zarr: None");
    }

    const auto raw_root_metadata = load_json_or_empty(source);
    const auto metadata = unwrap_ome_namespace(raw_root_metadata);
    const auto version = metadata_version(metadata);
    const int output_zarr_format = output_zarr_format_from_version(version);
    if (output_zarr_format == 0) {
        throw std::invalid_argument("Unsupported or missing OME-Zarr version for download");
    }

    const fs::path destination_root = fs::path(output_dir) / source.filename();
    if (fs::exists(destination_root)) {
        throw std::invalid_argument(destination_root.string() + " already exists!");
    }

    fs::create_directories(fs::path(output_dir));
    write_group_metadata(destination_root, output_zarr_format, metadata);

    const auto axis_names = multiscales_axes_names(metadata);
    for (const auto& dataset_path : dataset_paths_from_metadata(metadata)) {
        const auto source_dataset = source / dataset_path;
        const auto destination_dataset = destination_root / dataset_path;
        const auto source_dataset_metadata = load_json_file(source_dataset / "zarr.json");
        write_dataset_metadata(
            source_dataset,
            destination_dataset,
            output_zarr_format,
            axis_names);
        copy_dataset_chunks(
            source_dataset,
            destination_dataset,
            output_zarr_format,
            source_dataset_metadata);
    }

    return LocalDownloadResult{
        generic_path_string(destination_root),
        {source.filename().generic_string()},
    };
}

}  // namespace ome_zarr_c::native_code
