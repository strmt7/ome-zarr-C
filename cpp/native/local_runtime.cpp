#include "local_runtime.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <chrono>
#include <ctime>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <limits>
#include <optional>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>
#include <sys/stat.h>

#include <blosc.h>
#include <zstd.h>

#include "../../third_party/cpp-httplib/httplib.h"
#include "../../third_party/nlohmann/json.hpp"
#include "../../third_party/tinyxml2/tinyxml2.h"
#include "csv.hpp"
#include "io.hpp"
#include "reader.hpp"

namespace ome_zarr_c::native_code {

namespace {

using json = nlohmann::ordered_json;
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
}

json load_json_or_empty(const fs::path& path) {
    const auto metadata_path = metadata_json_path(path);
    if (!metadata_path.has_value()) {
        return json::object();
    }
    return load_json_file(metadata_path.value());
}

std::optional<fs::path> dataset_metadata_path(const fs::path& path) {
    const fs::path zarr_json = path / "zarr.json";
    if (fs::exists(zarr_json)) {
        return zarr_json;
    }
    const fs::path zarray = path / ".zarray";
    if (fs::exists(zarray)) {
        return zarray;
    }
    return std::nullopt;
}

struct GroupAttrsState {
    fs::path metadata_path;
    bool uses_zarr_json = false;
    json payload = json::object();
    json attrs = json::object();
};

GroupAttrsState load_group_attrs_state(
    const fs::path& path,
    const bool create_if_missing) {
    const auto metadata_path = metadata_json_path(path);
    if (metadata_path.has_value()) {
        GroupAttrsState state{};
        state.metadata_path = metadata_path.value();
        state.uses_zarr_json = state.metadata_path.filename() == "zarr.json";
        state.payload = load_json_file(state.metadata_path);
        if (state.uses_zarr_json) {
            if (state.payload.contains("attributes") &&
                state.payload["attributes"].is_object()) {
                state.attrs = state.payload["attributes"];
            } else {
                state.attrs = json::object();
            }
        } else {
            state.attrs = state.payload;
        }
        return state;
    }

    if (!create_if_missing) {
        return {};
    }

    fs::create_directories(path);
    GroupAttrsState state{};
    state.metadata_path = path / "zarr.json";
    state.uses_zarr_json = true;
    state.payload = json{
        {"attributes", json::object()},
        {"zarr_format", 3},
        {"node_type", "group"},
    };
    state.attrs = json::object();
    return state;
}

void persist_group_attrs_state(GroupAttrsState& state) {
    if (state.metadata_path.empty()) {
        throw std::runtime_error("Group metadata path is not initialized");
    }
    if (state.uses_zarr_json) {
        state.payload["attributes"] = state.attrs;
    } else {
        state.payload = state.attrs;
    }
    write_json_file(state.metadata_path, state.payload);
}

std::vector<std::vector<std::string>> read_csv_rows_native(const fs::path& path) {
    std::ifstream stream(path, std::ios::binary);
    if (!stream) {
        throw std::runtime_error("Unable to open CSV file: " + path.string());
    }

    const std::string content{
        std::istreambuf_iterator<char>(stream),
        std::istreambuf_iterator<char>()};

    std::vector<std::vector<std::string>> rows;
    std::vector<std::string> row;
    std::string field;
    bool in_quotes = false;
    bool field_touched = false;

    const auto flush_row = [&]() {
        if (row.empty() && field.empty() && !field_touched) {
            rows.emplace_back();
        } else {
            row.push_back(field);
            rows.push_back(std::move(row));
        }
        row = {};
        field.clear();
        field_touched = false;
    };

    for (std::size_t index = 0; index < content.size(); ++index) {
        const char ch = content[index];
        if (ch == '"') {
            field_touched = true;
            if (in_quotes && index + 1 < content.size() && content[index + 1] == '"') {
                field.push_back('"');
                index += 1;
            } else {
                in_quotes = !in_quotes;
            }
            continue;
        }
        if (!in_quotes && ch == ',') {
            row.push_back(field);
            field.clear();
            field_touched = false;
            continue;
        }
        if (!in_quotes && (ch == '\n' || ch == '\r')) {
            flush_row();
            if (ch == '\r' && index + 1 < content.size() && content[index + 1] == '\n') {
                index += 1;
            }
            continue;
        }
        field_touched = true;
        field.push_back(ch);
    }

    if (!content.empty() &&
        (content.back() == '\n' || content.back() == '\r')) {
        return rows;
    }
    if (!row.empty() || !field.empty() || field_touched) {
        row.push_back(field);
        rows.push_back(std::move(row));
    }

    return rows;
}

json csv_value_to_json(const CsvValue& value) {
    if (const auto* text = std::get_if<std::string>(&value)) {
        return *text;
    }
    if (const auto* number = std::get_if<double>(&value)) {
        return *number;
    }
    if (const auto* integer = std::get_if<std::int64_t>(&value)) {
        return *integer;
    }
    return std::get<bool>(value);
}

std::string python_like_string(const json& value) {
    if (value.is_null()) {
        return "None";
    }
    if (value.is_string()) {
        return value.get<std::string>();
    }
    if (value.is_boolean()) {
        return value.get<bool>() ? "True" : "False";
    }
    if (value.is_number_integer()) {
        return std::to_string(value.get<std::int64_t>());
    }
    if (value.is_number_unsigned()) {
        return std::to_string(value.get<std::uint64_t>());
    }
    if (value.is_number_float()) {
        const double number = value.get<double>();
        if (std::isnan(number)) {
            return "nan";
        }
        if (std::isinf(number)) {
            return number > 0.0 ? "inf" : "-inf";
        }
        std::ostringstream output;
        output << number;
        return output.str();
    }
    return value.dump();
}

std::vector<std::string> label_paths_for_root(const fs::path& zarr_path) {
    auto root_state = load_group_attrs_state(zarr_path, true);
    const json root_attrs =
        root_state.attrs.is_object() ? root_state.attrs : json::object();
    const bool has_plate =
        root_attrs.contains("plate") && root_attrs["plate"].is_object();
    const bool has_multiscales = root_attrs.contains("multiscales");

    std::vector<std::string> well_paths;
    if (has_plate) {
        const auto wells_iter = root_attrs["plate"].find("wells");
        if (wells_iter != root_attrs["plate"].end() && wells_iter->is_array()) {
            for (const auto& well : *wells_iter) {
                if (well.is_object() && well.contains("path") && well["path"].is_string()) {
                    well_paths.push_back(well["path"].get<std::string>());
                }
            }
        }
    }

    return csv_label_paths(
        has_plate,
        has_multiscales,
        zarr_path.string(),
        well_paths);
}

std::optional<fs::path> array_metadata_path(const fs::path& path) {
    const fs::path v3_json = path / "zarr.json";
    if (fs::exists(v3_json)) {
        return v3_json;
    }
    const fs::path v2_array = path / ".zarray";
    if (fs::exists(v2_array)) {
        return v2_array;
    }
    return std::nullopt;
}

json load_array_metadata_or_empty(const fs::path& path) {
    const auto metadata_path = array_metadata_path(path);
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

enum class NumericKind {
    signed_integer,
    unsigned_integer,
    floating_point,
    boolean,
};

struct NumericFormat {
    NumericKind kind;
    std::size_t item_size;
    bool little_endian;
    std::string numpy_repr_name;
};

struct StatsStrings {
    std::string min_repr;
    std::string max_repr;
};

bool host_is_little_endian() {
    return std::endian::native == std::endian::little;
}

std::vector<std::string> split_string(std::string_view text, const char delimiter) {
    std::vector<std::string> parts;
    std::size_t start = 0;
    while (start <= text.size()) {
        const auto next = text.find(delimiter, start);
        if (next == std::string_view::npos) {
            parts.emplace_back(text.substr(start));
            break;
        }
        parts.emplace_back(text.substr(start, next - start));
        start = next + 1;
    }
    return parts;
}

NumericFormat v2_numeric_format(const std::string& dtype) {
    if (dtype.size() < 3) {
        throw std::invalid_argument("Unsupported v2 dtype: " + dtype);
    }

    NumericFormat format{};
    switch (dtype[0]) {
        case '<':
            format.little_endian = true;
            break;
        case '>':
            format.little_endian = false;
            break;
        case '|':
            format.little_endian = host_is_little_endian();
            break;
        default:
            throw std::invalid_argument("Unsupported v2 dtype endianness: " + dtype);
    }

    switch (dtype[1]) {
        case 'i':
            format.kind = NumericKind::signed_integer;
            break;
        case 'u':
            format.kind = NumericKind::unsigned_integer;
            break;
        case 'f':
            format.kind = NumericKind::floating_point;
            break;
        case 'b':
            format.kind = NumericKind::boolean;
            break;
        default:
            throw std::invalid_argument("Unsupported v2 dtype kind: " + dtype);
    }

    format.item_size = static_cast<std::size_t>(std::stoull(dtype.substr(2)));
    switch (format.kind) {
        case NumericKind::signed_integer:
            format.numpy_repr_name = "int" + std::to_string(format.item_size * 8);
            break;
        case NumericKind::unsigned_integer:
            format.numpy_repr_name = "uint" + std::to_string(format.item_size * 8);
            break;
        case NumericKind::floating_point:
            format.numpy_repr_name = "float" + std::to_string(format.item_size * 8);
            break;
        case NumericKind::boolean:
            format.numpy_repr_name = "bool_";
            format.item_size = 1;
            break;
    }
    return format;
}

NumericFormat v3_numeric_format(const json& metadata) {
    if (!metadata.is_object() || !metadata.contains("data_type") ||
        !metadata["data_type"].is_string()) {
        throw std::invalid_argument("Missing v3 data_type metadata");
    }

    NumericFormat format{};
    const auto data_type = metadata["data_type"].get<std::string>();
    if (data_type == "bool") {
        format.kind = NumericKind::boolean;
        format.item_size = 1;
        format.little_endian = host_is_little_endian();
        format.numpy_repr_name = "bool_";
        return format;
    }

    if (data_type.rfind("int", 0) == 0) {
        format.kind = NumericKind::signed_integer;
        format.numpy_repr_name = data_type;
    } else if (data_type.rfind("uint", 0) == 0) {
        format.kind = NumericKind::unsigned_integer;
        format.numpy_repr_name = data_type;
    } else if (data_type.rfind("float", 0) == 0) {
        format.kind = NumericKind::floating_point;
        format.numpy_repr_name = data_type;
    } else {
        throw std::invalid_argument("Unsupported v3 data_type: " + data_type);
    }

    const auto bit_count = static_cast<std::size_t>(
        std::stoull(data_type.substr(data_type.find_first_of("0123456789"))));
    format.item_size = bit_count / 8;
    format.little_endian = true;

    if (metadata.contains("codecs") && metadata["codecs"].is_array()) {
        for (const auto& codec : metadata["codecs"]) {
            if (!codec.is_object() || !codec.contains("name") ||
                !codec["name"].is_string() ||
                codec["name"].get<std::string>() != "bytes") {
                continue;
            }
            if (codec.contains("configuration") &&
                codec["configuration"].is_object() &&
                codec["configuration"].contains("endian") &&
                codec["configuration"]["endian"].is_string()) {
                const auto endian =
                    codec["configuration"]["endian"].get<std::string>();
                if (endian == "little") {
                    format.little_endian = true;
                } else if (endian == "big") {
                    format.little_endian = false;
                }
            }
        }
    }

    return format;
}

std::vector<std::int64_t> json_int_vector(const json& values) {
    std::vector<std::int64_t> result;
    for (const auto& value : values) {
        result.push_back(value.get<std::int64_t>());
    }
    return result;
}

std::vector<std::int64_t> dataset_shape_vector(const json& metadata) {
    if (!metadata.is_object()) {
        return {};
    }
    if (metadata.contains("shape") && metadata["shape"].is_array()) {
        return json_int_vector(metadata["shape"]);
    }
    return {};
}

std::vector<std::int64_t> dataset_chunk_shape_vector(const json& metadata) {
    if (!metadata.is_object()) {
        return {};
    }
    if (metadata.contains("chunks") && metadata["chunks"].is_array()) {
        return json_int_vector(metadata["chunks"]);
    }
    if (metadata.contains("chunk_grid") && metadata["chunk_grid"].is_object() &&
        metadata["chunk_grid"].contains("configuration") &&
        metadata["chunk_grid"]["configuration"].is_object() &&
        metadata["chunk_grid"]["configuration"].contains("chunk_shape") &&
        metadata["chunk_grid"]["configuration"]["chunk_shape"].is_array()) {
        return json_int_vector(metadata["chunk_grid"]["configuration"]["chunk_shape"]);
    }
    return {};
}

std::vector<std::int64_t> chunk_indices_from_relative(
    const fs::path& relative,
    const char separator) {
    std::vector<std::int64_t> indices;
    if (separator == '.') {
        const auto parts = split_string(relative.filename().generic_string(), '.');
        for (const auto& part : parts) {
            indices.push_back(std::stoll(part));
        }
        return indices;
    }
    for (const auto& part : relative) {
        indices.push_back(std::stoll(part.generic_string()));
    }
    return indices;
}

std::size_t chunk_uncompressed_nbytes(
    const std::vector<std::int64_t>& shape,
    const std::vector<std::int64_t>& chunk_shape,
    const std::vector<std::int64_t>& indices,
    const std::size_t item_size) {
    if (shape.size() != chunk_shape.size() || shape.size() != indices.size()) {
        throw std::invalid_argument("Chunk index rank mismatch");
    }
    std::size_t elements = 1;
    for (std::size_t axis = 0; axis < shape.size(); ++axis) {
        const auto start = indices[axis] * chunk_shape[axis];
        const auto extent = std::min<std::int64_t>(chunk_shape[axis], shape[axis] - start);
        elements *= static_cast<std::size_t>(extent);
    }
    return elements * item_size;
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

struct LocalArraySummary {
    std::vector<std::int64_t> shape;
    std::optional<std::string> minmax_repr;
};

std::vector<char> read_binary_file(const fs::path& path);

std::vector<char> decode_v2_chunk(
    const std::vector<char>& source_bytes,
    const json& source_metadata,
    const fs::path& relative);

std::vector<char> decode_v3_chunk(
    const std::vector<char>& source_bytes,
    const json& source_metadata,
    const fs::path& relative);

template <typename Value, typename Bits = Value>
Value decode_scalar(const char* data, const bool little_endian) {
    std::array<unsigned char, sizeof(Bits)> bytes{};
    std::memcpy(bytes.data(), data, sizeof(Bits));
    if (sizeof(Bits) > 1 && little_endian != host_is_little_endian()) {
        std::reverse(bytes.begin(), bytes.end());
    }
    Bits bits{};
    std::memcpy(&bits, bytes.data(), sizeof(Bits));
    if constexpr (std::is_same_v<Value, float> || std::is_same_v<Value, double>) {
        return std::bit_cast<Value>(bits);
    }
    return static_cast<Value>(bits);
}

template <typename Value>
std::string format_integer_scalar(const std::string& numpy_name, const Value value) {
    return "np." + numpy_name + "(" + std::to_string(value) + ")";
}

template <typename Value>
std::string format_float_scalar(const std::string& numpy_name, const Value value) {
    if (std::isnan(value)) {
        return "np." + numpy_name + "(nan)";
    }
    if (std::isinf(value)) {
        return "np." + numpy_name + (value > 0 ? "(inf)" : "(-inf)");
    }
    std::ostringstream output;
    output << "np." << numpy_name << "("
           << std::setprecision(std::numeric_limits<Value>::max_digits10)
           << value << ")";
    return output.str();
}

template <typename Value>
StatsStrings scalar_stats_strings(
    const std::vector<char>& bytes,
    const NumericFormat& format) {
    if (bytes.size() < sizeof(Value)) {
        throw std::invalid_argument("Chunk payload shorter than dtype item size");
    }
    const std::size_t item_count = bytes.size() / sizeof(Value);
    Value min_value = decode_scalar<Value>(
        bytes.data(),
        format.little_endian);
    Value max_value = min_value;
    bool saw_nan = false;
    for (std::size_t index = 1; index < item_count; ++index) {
        const Value value = decode_scalar<Value>(
            bytes.data() + (index * sizeof(Value)),
            format.little_endian);
        if constexpr (std::is_floating_point_v<Value>) {
            if (std::isnan(value)) {
                saw_nan = true;
                break;
            }
        }
        if (value < min_value) {
            min_value = value;
        }
        if (value > max_value) {
            max_value = value;
        }
    }
    if constexpr (std::is_same_v<Value, float> || std::is_same_v<Value, double>) {
        if (saw_nan || std::isnan(min_value) || std::isnan(max_value)) {
            return {
                "np." + format.numpy_repr_name + "(nan)",
                "np." + format.numpy_repr_name + "(nan)",
            };
        }
        return {
            format_float_scalar(format.numpy_repr_name, min_value),
            format_float_scalar(format.numpy_repr_name, max_value),
        };
    }
    if constexpr (std::is_same_v<Value, bool>) {
        return {
            min_value ? "np.True_" : "np.False_",
            max_value ? "np.True_" : "np.False_",
        };
    }
    return {
        format_integer_scalar(format.numpy_repr_name, min_value),
        format_integer_scalar(format.numpy_repr_name, max_value),
    };
}

template <typename Value>
void update_minmax(Value chunk_min, Value chunk_max, bool& has_value, Value& global_min, Value& global_max) {
    if (!has_value) {
        global_min = chunk_min;
        global_max = chunk_max;
        has_value = true;
        return;
    }
    if (chunk_min < global_min) {
        global_min = chunk_min;
    }
    if (chunk_max > global_max) {
        global_max = chunk_max;
    }
}

template <typename Value, typename Bits = Value>
std::pair<Value, Value> chunk_minmax_values(
    const std::vector<char>& bytes,
    const NumericFormat& format) {
    if (bytes.size() < sizeof(Value)) {
        throw std::invalid_argument("Chunk payload shorter than dtype item size");
    }
    const std::size_t item_count = bytes.size() / sizeof(Value);
    Value min_value = decode_scalar<Value, Bits>(bytes.data(), format.little_endian);
    Value max_value = min_value;
    for (std::size_t index = 1; index < item_count; ++index) {
        const Value value = decode_scalar<Value, Bits>(
            bytes.data() + (index * sizeof(Value)),
            format.little_endian);
        if constexpr (std::is_floating_point_v<Value>) {
            if (std::isnan(value)) {
                return {value, value};
            }
        }
        if (value < min_value) {
            min_value = value;
        }
        if (value > max_value) {
            max_value = value;
        }
    }
    return {min_value, max_value};
}

template <typename Value, typename Bits = Value>
std::optional<std::string> dataset_minmax_repr_typed(
    const fs::path& dataset_path,
    const fs::path& chunk_root,
    const json& metadata,
    const NumericFormat& format,
    const bool is_v3) {
    Value global_min{};
    Value global_max{};
    bool has_value = false;
    bool saw_nan = false;

    for (const auto& entry : fs::recursive_directory_iterator(chunk_root)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        const auto filename = entry.path().filename().generic_string();
        if (chunk_root == dataset_path &&
            (filename == ".zarray" || filename == ".zattrs" || filename == ".zgroup" ||
             filename == "zarr.json")) {
            continue;
        }
        const auto relative = fs::relative(entry.path(), chunk_root);
        const auto decoded = is_v3
            ? decode_v3_chunk(read_binary_file(entry.path()), metadata, relative)
            : decode_v2_chunk(read_binary_file(entry.path()), metadata, relative);
        const auto [chunk_min, chunk_max] =
            chunk_minmax_values<Value, Bits>(decoded, format);
        if constexpr (std::is_floating_point_v<Value>) {
            if (std::isnan(chunk_min) || std::isnan(chunk_max)) {
                saw_nan = true;
                break;
            }
        }
        update_minmax(chunk_min, chunk_max, has_value, global_min, global_max);
    }

    if (!has_value && !saw_nan) {
        return std::nullopt;
    }
    if constexpr (std::is_same_v<Value, bool>) {
        return std::string("(") +
            (saw_nan ? "np.False_" : (global_min ? "np.True_" : "np.False_")) +
            ", " +
            (saw_nan ? "np.False_" : (global_max ? "np.True_" : "np.False_")) +
            ")";
    }
    if constexpr (std::is_floating_point_v<Value>) {
        if (saw_nan) {
            return std::string("(np.") + format.numpy_repr_name + "(nan), np." +
                format.numpy_repr_name + "(nan))";
        }
        return std::string("(") +
            format_float_scalar(format.numpy_repr_name, global_min) +
            ", " +
            format_float_scalar(format.numpy_repr_name, global_max) +
            ")";
    }
    return std::string("(") +
        format_integer_scalar(format.numpy_repr_name, global_min) +
        ", " +
        format_integer_scalar(format.numpy_repr_name, global_max) +
        ")";
}

std::optional<std::string> dataset_minmax_repr(
    const fs::path& dataset_path,
    const json& metadata) {
    if (!metadata.is_object()) {
        return std::nullopt;
    }

    const bool is_v3 = metadata.contains("zarr_format") &&
        metadata["zarr_format"].is_number_integer() &&
        metadata["zarr_format"].get<int>() == 3;
    const fs::path chunk_root = is_v3 && fs::exists(dataset_path / "c")
        ? dataset_path / "c"
        : dataset_path;

    const auto format = is_v3
        ? v3_numeric_format(metadata)
        : v2_numeric_format(metadata.at("dtype").get<std::string>());

    switch (format.kind) {
        case NumericKind::signed_integer:
            switch (format.item_size) {
                case 1:
                    return dataset_minmax_repr_typed<std::int8_t>(
                        dataset_path, chunk_root, metadata, format, is_v3);
                case 2:
                    return dataset_minmax_repr_typed<std::int16_t>(
                        dataset_path, chunk_root, metadata, format, is_v3);
                case 4:
                    return dataset_minmax_repr_typed<std::int32_t>(
                        dataset_path, chunk_root, metadata, format, is_v3);
                case 8:
                    return dataset_minmax_repr_typed<std::int64_t>(
                        dataset_path, chunk_root, metadata, format, is_v3);
                default:
                    break;
            }
            break;
        case NumericKind::unsigned_integer:
            switch (format.item_size) {
                case 1:
                    return dataset_minmax_repr_typed<std::uint8_t>(
                        dataset_path, chunk_root, metadata, format, is_v3);
                case 2:
                    return dataset_minmax_repr_typed<std::uint16_t>(
                        dataset_path, chunk_root, metadata, format, is_v3);
                case 4:
                    return dataset_minmax_repr_typed<std::uint32_t>(
                        dataset_path, chunk_root, metadata, format, is_v3);
                case 8:
                    return dataset_minmax_repr_typed<std::uint64_t>(
                        dataset_path, chunk_root, metadata, format, is_v3);
                default:
                    break;
            }
            break;
        case NumericKind::floating_point:
            switch (format.item_size) {
                case 4:
                    return dataset_minmax_repr_typed<float, std::uint32_t>(
                        dataset_path, chunk_root, metadata, format, is_v3);
                case 8:
                    return dataset_minmax_repr_typed<double, std::uint64_t>(
                        dataset_path, chunk_root, metadata, format, is_v3);
                default:
                    break;
            }
            break;
        case NumericKind::boolean:
            return dataset_minmax_repr_typed<bool, std::uint8_t>(
                dataset_path, chunk_root, metadata, format, is_v3);
    }
    throw std::invalid_argument("Unsupported dataset dtype for stats");
}

std::vector<LocalArraySummary> dataset_summaries(
    const fs::path& root,
    const json& metadata,
    const bool stats) {
    std::vector<LocalArraySummary> summaries;
    if (!metadata.is_object() || !metadata.contains("multiscales") ||
        !metadata["multiscales"].is_array() || metadata["multiscales"].empty()) {
        return summaries;
    }

    const auto& first = metadata["multiscales"][0];
    if (!first.is_object() || !first.contains("datasets") || !first["datasets"].is_array()) {
        return summaries;
    }

    for (const auto& dataset : first["datasets"]) {
        if (!dataset.is_object() || !dataset.contains("path") || !dataset["path"].is_string()) {
            continue;
        }
        const fs::path dataset_path = root / dataset["path"].get<std::string>();
        const auto array_json = load_array_metadata_or_empty(dataset_path);
        const auto shape = dataset_shape_vector(array_json);
        if (shape.empty()) {
            continue;
        }
        LocalArraySummary summary{};
        summary.shape = shape;
        if (stats) {
            summary.minmax_repr = dataset_minmax_repr(dataset_path, array_json);
        }
        summaries.push_back(std::move(summary));
    }

    return summaries;
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

std::vector<std::string> labels_from_metadata(const json& metadata) {
    std::vector<std::string> labels;
    if (!metadata.is_object() || !metadata.contains("labels") ||
        !metadata["labels"].is_array()) {
        return labels;
    }
    for (const auto& label : metadata["labels"]) {
        if (label.is_string()) {
            labels.push_back(label.get<std::string>());
        }
    }
    return labels;
}

std::vector<std::string> info_spec_names(const json& metadata) {
    std::vector<std::string> specs;
    if (!metadata.is_object()) {
        return specs;
    }
    if (metadata.contains("image-label") && metadata["image-label"].is_object()) {
        specs.push_back("Label");
    }
    if (metadata.contains("labels") && metadata["labels"].is_array()) {
        specs.push_back("Labels");
    }
    if (metadata.contains("plate") && metadata["plate"].is_object()) {
        specs.push_back("Plate");
    }
    if (metadata.contains("multiscales") && json_truthy(metadata["multiscales"])) {
        specs.push_back("Multiscales");
    }
    if (metadata.contains("omero") && metadata["omero"].is_object()) {
        specs.push_back("OMERO");
    }
    return specs;
}

std::optional<fs::path> plate_first_image_root(
    const fs::path& root,
    const json& metadata) {
    if (!metadata.is_object() || !metadata.contains("plate") ||
        !metadata["plate"].is_object()) {
        return std::nullopt;
    }
    const auto wells_iter = metadata["plate"].find("wells");
    if (wells_iter == metadata["plate"].end() || !wells_iter->is_array() ||
        wells_iter->empty()) {
        return std::nullopt;
    }
    const auto& first_well = (*wells_iter)[0];
    if (!first_well.is_object() || !first_well.contains("path") ||
        !first_well["path"].is_string()) {
        return std::nullopt;
    }
    const fs::path well_root = root / first_well["path"].get<std::string>();
    const auto well_metadata_path = metadata_json_path(well_root);
    if (!well_metadata_path.has_value()) {
        return std::nullopt;
    }
    const json well_metadata =
        unwrap_ome_namespace(load_json_file(well_metadata_path.value()));
    if (!well_metadata.is_object() || !well_metadata.contains("well") ||
        !well_metadata["well"].is_object()) {
        return std::nullopt;
    }
    const auto images_iter = well_metadata["well"].find("images");
    if (images_iter == well_metadata["well"].end() || !images_iter->is_array() ||
        images_iter->empty()) {
        return std::nullopt;
    }
    const auto& first_image = (*images_iter)[0];
    if (!first_image.is_object() || !first_image.contains("path") ||
        !first_image["path"].is_string()) {
        return std::nullopt;
    }
    return well_root / first_image["path"].get<std::string>();
}

std::vector<LocalArraySummary> plate_dataset_summaries(
    const fs::path& root,
    const json& metadata,
    const bool stats) {
    std::vector<LocalArraySummary> summaries;
    if (!metadata.is_object() || !metadata.contains("plate") ||
        !metadata["plate"].is_object()) {
        return summaries;
    }
    const auto rows_iter = metadata["plate"].find("rows");
    const auto cols_iter = metadata["plate"].find("columns");
    if (rows_iter == metadata["plate"].end() || !rows_iter->is_array() ||
        cols_iter == metadata["plate"].end() || !cols_iter->is_array()) {
        return summaries;
    }
    const auto image_root = plate_first_image_root(root, metadata);
    if (!image_root.has_value()) {
        return summaries;
    }
    const auto image_metadata_path = metadata_json_path(image_root.value());
    if (!image_metadata_path.has_value()) {
        return summaries;
    }
    const json image_metadata =
        unwrap_ome_namespace(load_json_file(image_metadata_path.value()));
    summaries = dataset_summaries(image_root.value(), image_metadata, stats);

    const auto row_count = static_cast<std::int64_t>(rows_iter->size());
    const auto col_count = static_cast<std::int64_t>(cols_iter->size());
    for (auto& summary : summaries) {
        if (summary.shape.size() >= 2) {
            summary.shape[summary.shape.size() - 2] *= row_count;
            summary.shape[summary.shape.size() - 1] *= col_count;
        }
    }
    return summaries;
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

void ensure_blosc_initialized() {
    static const bool initialized = [] {
        blosc_init();
        return true;
    }();
    (void)initialized;
}

std::vector<char> decode_zstd_chunk(
    const std::vector<char>& source_bytes,
    const std::size_t raw_size) {
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
    return raw;
}

std::vector<char> decode_blosc_chunk(
    const std::vector<char>& source_bytes,
    const std::size_t raw_size) {
    ensure_blosc_initialized();
    std::vector<char> raw(raw_size);
    const int decoded = blosc_decompress_ctx(
        source_bytes.data(),
        raw.data(),
        static_cast<int>(raw.size()),
        1);
    if (decoded < 0) {
        throw std::runtime_error("Blosc decompression failed");
    }
    raw.resize(static_cast<std::size_t>(decoded));
    return raw;
}

std::vector<char> decode_v2_chunk(
    const std::vector<char>& source_bytes,
    const json& source_metadata,
    const fs::path& relative) {
    const auto shape = dataset_shape_vector(source_metadata);
    const auto chunk_shape = dataset_chunk_shape_vector(source_metadata);
    const auto indices = chunk_indices_from_relative(
        relative,
        source_metadata.value("dimension_separator", ".")[0]);
    const auto raw_size = chunk_uncompressed_nbytes(
        shape,
        chunk_shape,
        indices,
        data_type_size(source_metadata.at("dtype").get<std::string>()));
    if (!source_metadata.contains("compressor") || source_metadata["compressor"].is_null()) {
        return source_bytes;
    }
    if (!source_metadata["compressor"].is_object() ||
        !source_metadata["compressor"].contains("id") ||
        !source_metadata["compressor"]["id"].is_string()) {
        throw std::invalid_argument("Unsupported v2 compressor metadata");
    }
    const auto codec = source_metadata["compressor"]["id"].get<std::string>();
    if (codec == "zstd") {
        return decode_zstd_chunk(source_bytes, raw_size);
    }
    if (codec == "blosc") {
        return decode_blosc_chunk(source_bytes, raw_size);
    }
    throw std::invalid_argument("Unsupported v2 compressor: " + codec);
}

std::vector<char> decode_v3_chunk(
    const std::vector<char>& source_bytes,
    const json& source_metadata,
    const fs::path& relative) {
    const auto shape = dataset_shape_vector(source_metadata);
    const auto chunk_shape = dataset_chunk_shape_vector(source_metadata);
    const auto separator = source_metadata.at("chunk_key_encoding")
                               .at("configuration")
                               .at("separator")
                               .get<std::string>();
    const auto indices = chunk_indices_from_relative(
        relative,
        separator == "." ? '.' : '/');
    const auto raw_size = chunk_uncompressed_nbytes(
        shape,
        chunk_shape,
        indices,
        data_type_size(source_metadata.at("data_type").get<std::string>()));

    std::vector<char> decoded = source_bytes;
    if (!source_metadata.contains("codecs") || !source_metadata["codecs"].is_array()) {
        return decoded;
    }

    const auto& codecs = source_metadata["codecs"];
    for (auto it = codecs.rbegin(); it != codecs.rend(); ++it) {
        if (!it->is_object() || !it->contains("name") || !(*it)["name"].is_string()) {
            throw std::invalid_argument("Unsupported v3 codec metadata");
        }
        const auto codec = (*it)["name"].get<std::string>();
        if (codec == "bytes") {
            continue;
        }
        if (codec == "zstd") {
            decoded = decode_zstd_chunk(decoded, raw_size);
            continue;
        }
        if (codec == "blosc") {
            decoded = decode_blosc_chunk(decoded, raw_size);
            continue;
        }
        throw std::invalid_argument("Unsupported v3 codec: " + codec);
    }
    return decoded;
}

std::vector<char> decode_chunk(
    const std::vector<char>& source_bytes,
    const json& source_metadata,
    const fs::path& relative) {
    if (source_metadata.contains("dtype")) {
        return decode_v2_chunk(source_bytes, source_metadata, relative);
    }
    return decode_v3_chunk(source_bytes, source_metadata, relative);
}

std::vector<char> encode_zstd_chunk(const std::vector<char>& raw) {
    const auto bound = ZSTD_compressBound(raw.size());
    std::vector<char> encoded(bound);
    const auto compressed = ZSTD_compress(
        encoded.data(),
        encoded.size(),
        raw.data(),
        raw.size(),
        0);
    if (ZSTD_isError(compressed) != 0U) {
        throw std::runtime_error(
            "ZSTD compress failed: " +
            std::string(ZSTD_getErrorName(compressed)));
    }
    encoded.resize(compressed);
    return encoded;
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
    const auto raw = decode_chunk(source_bytes, source_metadata, relative);

    ensure_blosc_initialized();

    std::vector<char> encoded(raw.size() + BLOSC_MAX_OVERHEAD);
    const auto type_size = source_metadata.contains("dtype")
        ? data_type_size(source_metadata.at("dtype").get<std::string>())
        : data_type_size(source_metadata.at("data_type").get<std::string>());
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

std::vector<char> transcode_chunk_to_v3(
    const std::vector<char>& source_bytes,
    const json& source_metadata,
    const fs::path& relative) {
    return encode_zstd_chunk(decode_chunk(source_bytes, source_metadata, relative));
}

fs::path chunk_relative_as_slash_path(
    const json& source_metadata,
    const fs::path& relative) {
    char separator = '.';
    if (source_metadata.contains("chunk_key_encoding") &&
        source_metadata["chunk_key_encoding"].is_object() &&
        source_metadata["chunk_key_encoding"].contains("configuration") &&
        source_metadata["chunk_key_encoding"]["configuration"].is_object() &&
        source_metadata["chunk_key_encoding"]["configuration"].contains("separator") &&
        source_metadata["chunk_key_encoding"]["configuration"]["separator"].is_string()) {
        const auto configured = source_metadata["chunk_key_encoding"]["configuration"]["separator"]
                                    .get<std::string>();
        separator = configured == "." ? '.' : '/';
    } else if (source_metadata.contains("dimension_separator") &&
               source_metadata["dimension_separator"].is_string()) {
        const auto configured = source_metadata["dimension_separator"].get<std::string>();
        separator = configured == "/" ? '/' : '.';
    }

    const auto indices = chunk_indices_from_relative(relative, separator);
    fs::path target;
    for (const auto index : indices) {
        target /= std::to_string(index);
    }
    return target;
}

bool v3_chunks_already_use_download_codecs(const json& source_metadata) {
    if (source_metadata.contains("dtype") || !source_metadata.contains("codecs") ||
        !source_metadata["codecs"].is_array()) {
        return false;
    }
    const auto& codecs = source_metadata["codecs"];
    if (codecs.size() != 2U) {
        return false;
    }
    if (!codecs[0].is_object() || !codecs[0].contains("name") ||
        codecs[0]["name"] != "bytes") {
        return false;
    }
    if (!codecs[1].is_object() || !codecs[1].contains("name") ||
        codecs[1]["name"] != "zstd") {
        return false;
    }
    if (!codecs[1].contains("configuration") ||
        !codecs[1]["configuration"].is_object()) {
        return false;
    }
    const auto& config = codecs[1]["configuration"];
    return config.value("level", -1) == 0 &&
           config.value("checksum", true) == false;
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
            output_zarr_format == 2
                ? destination_dataset / chunk_relative_as_slash_path(source_metadata, relative)
                : destination_dataset / "c" / chunk_relative_as_slash_path(source_metadata, relative);
        fs::create_directories(target.parent_path());
        if (output_zarr_format == 2) {
            write_binary_file(
                target,
                transcode_chunk_to_v2(read_binary_file(entry.path()), source_metadata, relative));
        } else {
            if (v3_chunks_already_use_download_codecs(source_metadata)) {
                fs::copy_file(entry.path(), target, fs::copy_options::overwrite_existing);
            } else {
                write_binary_file(
                    target,
                    transcode_chunk_to_v3(read_binary_file(entry.path()), source_metadata, relative));
            }
        }
    }
}

std::string bytes_codec_endian(const json& source_metadata) {
    if (source_metadata.contains("codecs") && source_metadata["codecs"].is_array()) {
        for (const auto& codec : source_metadata["codecs"]) {
            if (!codec.is_object() || !codec.contains("name") ||
                !codec["name"].is_string() ||
                codec["name"].get<std::string>() != "bytes") {
                continue;
            }
            if (codec.contains("configuration") &&
                codec["configuration"].is_object() &&
                codec["configuration"].contains("endian") &&
                codec["configuration"]["endian"].is_string()) {
                return codec["configuration"]["endian"].get<std::string>();
            }
        }
    }
    if (source_metadata.contains("dtype") && source_metadata["dtype"].is_string()) {
        const auto dtype = source_metadata["dtype"].get<std::string>();
        if (!dtype.empty() && dtype[0] == '>') {
            return "big";
        }
    }
    return "little";
}

json output_bytes_codec(const json& source_metadata) {
    const auto type_name = source_metadata.contains("data_type")
        ? source_metadata.at("data_type").get<std::string>()
        : source_metadata.at("dtype").get<std::string>();
    if (data_type_size(type_name) <= 1U) {
        return json{{"name", "bytes"}};
    }
    return json{
        {"name", "bytes"},
        {"configuration", {{"endian", bytes_codec_endian(source_metadata)}}},
    };
}

void write_dataset_metadata(
    const fs::path& destination_dataset,
    const int output_zarr_format,
    const json& source_metadata,
    const std::vector<std::string>& axis_names) {
    if (output_zarr_format == 2) {
        const auto chunk_shape = source_metadata.contains("chunks")
            ? source_metadata.at("chunks")
            : source_metadata.at("chunk_grid").at("configuration").at("chunk_shape");
        const auto dtype = source_metadata.contains("dtype")
            ? source_metadata.at("dtype").get<std::string>()
            : zarr_v2_dtype_from_data_type(source_metadata.at("data_type").get<std::string>());
        json payload = {
            {"shape", source_metadata.at("shape")},
            {"chunks", chunk_shape},
            {"dtype", dtype},
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

    json payload = source_metadata.contains("data_type")
        ? source_metadata
        : json{
              {"shape", source_metadata.at("shape")},
              {"data_type", v2_numeric_format(source_metadata.at("dtype").get<std::string>()).numpy_repr_name},
              {"chunk_grid",
               {{"name", "regular"},
                {"configuration", {{"chunk_shape", source_metadata.at("chunks")}}}}},
              {"chunk_key_encoding",
               {{"name", "default"}, {"configuration", {{"separator", "/"}}}}},
              {"fill_value",
               source_metadata.contains("fill_value") ? source_metadata.at("fill_value") : json(0)},
              {"attributes", json::object()},
              {"zarr_format", 3},
              {"node_type", "array"},
              {"storage_transformers", json::array()},
          };
    payload["codecs"] = json::array({
        output_bytes_codec(source_metadata),
        json{
            {"name", "zstd"},
            {"configuration", {{"level", 0}, {"checksum", false}}},
        },
    });
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

void copy_multiscale_group(
    const fs::path& source_group,
    const fs::path& destination_group,
    const json& metadata,
    const int output_zarr_format) {
    write_group_metadata(destination_group, output_zarr_format, metadata);
    const auto axis_names = multiscales_axes_names(metadata);
    for (const auto& dataset_path : dataset_paths_from_metadata(metadata)) {
        const auto source_dataset = source_group / dataset_path;
        const auto destination_dataset = destination_group / dataset_path;
        const auto source_dataset_metadata_path = dataset_metadata_path(source_dataset);
        if (!source_dataset_metadata_path.has_value()) {
            throw std::runtime_error(
                "Unable to find dataset metadata under: " + source_dataset.string());
        }
        const auto source_dataset_metadata =
            load_json_file(source_dataset_metadata_path.value());
        write_dataset_metadata(
            destination_dataset,
            output_zarr_format,
            source_dataset_metadata,
            axis_names);
        copy_dataset_chunks(
            source_dataset,
            destination_dataset,
            output_zarr_format,
            source_dataset_metadata);
    }
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

std::string shell_quote_posix(const std::string& value) {
    std::string quoted = "'";
    for (const char ch : value) {
        if (ch == '\'') {
            quoted += "'\"'\"'";
        } else {
            quoted.push_back(ch);
        }
    }
    quoted.push_back('\'');
    return quoted;
}

std::string render_browser_command(const std::string& url) {
    const char* browser_env = std::getenv("BROWSER");
    if (browser_env != nullptr && browser_env[0] != '\0') {
        std::string command(browser_env);
        const auto placeholder = command.find("%s");
        if (placeholder != std::string::npos) {
            command.replace(placeholder, 2, shell_quote_posix(url));
            return command;
        }
        return command + " " + shell_quote_posix(url);
    }
#if defined(__APPLE__)
    return "open " + shell_quote_posix(url);
#elif defined(_WIN32)
    return "cmd /c start \"\" " + shell_quote_posix(url);
#else
    return "xdg-open " + shell_quote_posix(url);
#endif
}

void open_browser_noexcept(const std::string& url) {
    const auto command = render_browser_command(url);
    if (command.empty()) {
        return;
    }
    const int exit_code = std::system(command.c_str());
    static_cast<void>(exit_code);
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
        const fs::path xml_path = path_to_zattrs / "OME" / "METADATA.ome.xml";
        if (!fs::exists(xml_path)) {
            result.printed_messages.push_back(
                "[Errno 2] No such file or directory: '" +
                generic_path_string(xml_path) + "'");
            return result;
        }
        try {
            result.images = bioformats_images(path_to_zattrs);
        } catch (const std::exception& exc) {
            result.printed_messages.push_back(exc.what());
        }
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

std::vector<std::string> local_info_lines(const std::string& input_path, const bool stats) {
    const fs::path root(input_path);
    if (!fs::exists(root)) {
        throw std::invalid_argument("not a zarr: None");
    }

    const auto metadata_path = metadata_json_path(root);
    if (!metadata_path.has_value()) {
        return {};
    }

    const json metadata = unwrap_ome_namespace(load_json_file(metadata_path.value()));
    const auto root_specs = info_spec_names(metadata);
    if (root_specs.empty()) {
        return {};
    }

    std::vector<std::string> lines = utils_info_header_lines(
        io_repr(generic_path_string(root), true, false),
        metadata_version(metadata),
        root_specs);

    auto summaries = plate_dataset_summaries(root, metadata, stats);
    if (summaries.empty()) {
        summaries = dataset_summaries(root, metadata, stats);
    }

    for (const auto& summary : summaries) {
        lines.push_back(
            utils_info_data_line(shape_repr(summary.shape), summary.minmax_repr));
    }

    const fs::path labels_root = root / "labels";
    if (fs::exists(labels_root)) {
        const json labels_metadata = unwrap_ome_namespace(load_json_or_empty(labels_root));
        const auto labels_specs = info_spec_names(labels_metadata);
        const auto label_names = labels_from_metadata(labels_metadata);
        if (!labels_specs.empty()) {
            const auto labels_repr = reader_node_repr(
                io_repr(generic_path_string(labels_root), true, false),
                false);
            const auto label_lines = utils_info_header_lines(
                labels_repr,
                metadata_version(labels_metadata),
                labels_specs);
            lines.insert(lines.end(), label_lines.begin(), label_lines.end());
        }

        for (const auto& label_name : label_names) {
            const fs::path label_root = labels_root / label_name;
            if (!fs::exists(label_root)) {
                continue;
            }
            const json label_metadata =
                unwrap_ome_namespace(load_json_or_empty(label_root));
            const auto label_specs = info_spec_names(label_metadata);
            if (label_specs.empty()) {
                continue;
            }
            const auto label_repr = reader_node_repr(
                io_repr(generic_path_string(label_root), true, false),
                false);
            const auto label_lines = utils_info_header_lines(
                label_repr,
                metadata_version(label_metadata),
                label_specs);
            lines.insert(lines.end(), label_lines.begin(), label_lines.end());
            for (const auto& summary : dataset_summaries(label_root, label_metadata, stats)) {
                lines.push_back(
                    utils_info_data_line(shape_repr(summary.shape), summary.minmax_repr));
            }
        }
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

LocalViewPreparation local_view_prepare(
    const std::string& input_path,
    const int port,
    const bool force) {
    std::size_t discovered_count = 0;
    if (!force) {
        const fs::path input_root(input_path);
        if (metadata_json_path(input_root).has_value()) {
            discovered_count = local_find_multiscales(input_path).images.size();
        }
    }

    const auto plan = utils_view_plan(input_path, port, force, discovered_count);
    return LocalViewPreparation{
        plan.should_warn,
        plan.warning_message,
        plan.parent_dir,
        plan.image_name,
        plan.url,
    };
}

void local_view_run(const LocalViewPreparation& preparation, const int port) {
    httplib::Server server;
    server.set_post_routing_handler(
        [](const httplib::Request&, httplib::Response& response) {
            response.set_header("Access-Control-Allow-Origin", "*");
        });
    server.set_error_handler(
        [](const httplib::Request&, httplib::Response& response) {
            response.set_header("Access-Control-Allow-Origin", "*");
        });

    const auto serve_dir =
        preparation.parent_dir.empty() ? std::string(".") : preparation.parent_dir;
    if (!server.set_mount_point("/", serve_dir)) {
        throw std::runtime_error("Unable to mount directory for view server: " + serve_dir);
    }

    open_browser_noexcept(preparation.url);

    if (!server.listen("localhost", port)) {
        throw std::runtime_error(
            "Failed to start local view server on localhost:" + std::to_string(port));
    }
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
    copy_multiscale_group(source, destination_root, metadata, output_zarr_format);

    std::vector<std::string> listed_paths{source.filename().generic_string()};
    const fs::path labels_root = source / "labels";
    if (fs::exists(labels_root)) {
        const json labels_metadata = unwrap_ome_namespace(load_json_or_empty(labels_root));
        const auto label_names = labels_from_metadata(labels_metadata);
        if (!label_names.empty()) {
            write_group_metadata(
                destination_root / "labels",
                output_zarr_format,
                labels_metadata);
            listed_paths.push_back(source.filename().generic_string() + "/labels");

            for (const auto& label_name : label_names) {
                const fs::path source_label_group = labels_root / label_name;
                if (!fs::exists(source_label_group)) {
                    continue;
                }
                const json label_metadata =
                    unwrap_ome_namespace(load_json_or_empty(source_label_group));
                copy_multiscale_group(
                    source_label_group,
                    destination_root / "labels" / label_name,
                    label_metadata,
                    output_zarr_format);
                listed_paths.push_back(
                    source.filename().generic_string() + "/labels/" + label_name);
            }
        }
    }

    return LocalDownloadResult{
        generic_path_string(destination_root),
        std::move(listed_paths),
    };
}

LocalCsvToLabelsResult local_csv_to_labels(
    const std::string& csv_path,
    const std::string& csv_id,
    const std::string& csv_keys,
    const std::string& zarr_path,
    const std::string& zarr_id) {
    const auto rows = read_csv_rows_native(csv_path);
    const auto specs = parse_csv_key_specs(csv_keys);

    CsvPropsById props_by_id;
    try {
        props_by_id = csv_props_by_id(rows, csv_id, specs);
    } catch (const std::invalid_argument&) {
        std::ostringstream header_repr;
        header_repr << "[";
        if (!rows.empty()) {
            const auto& header = rows.front();
            for (std::size_t index = 0; index < header.size(); ++index) {
                if (index > 0) {
                    header_repr << ", ";
                }
                header_repr << "'" << header[index] << "'";
            }
        }
        header_repr << "]";
        throw std::invalid_argument(
            "csv_id '" + csv_id + "' should match acsv column name: " +
            header_repr.str());
    }

    const auto label_paths = label_paths_for_root(fs::path(zarr_path));

    LocalCsvToLabelsResult result{};
    for (const auto& label_path : label_paths) {
        auto label_state = load_group_attrs_state(fs::path(label_path), true);
        json& attrs = label_state.attrs;
        if (!attrs.is_object() || !attrs.contains("image-label") ||
            !attrs["image-label"].is_object() ||
            !attrs["image-label"].contains("properties") ||
            !attrs["image-label"]["properties"].is_array()) {
            persist_group_attrs_state(label_state);
            continue;
        }

        bool changed = false;
        for (auto& props_dict : attrs["image-label"]["properties"]) {
            if (!props_dict.is_object()) {
                continue;
            }
            const auto props_id = python_like_string(
                props_dict.contains(zarr_id) ? props_dict[zarr_id] : json(nullptr));
            const auto match = props_by_id.find(props_id);
            if (match == props_by_id.end()) {
                continue;
            }
            for (const auto& [key, value] : match->second) {
                props_dict[key] = csv_value_to_json(value);
            }
            result.updated_properties += 1U;
            changed = true;
        }
        persist_group_attrs_state(label_state);
        result.touched_label_groups += 1U;
        if (!changed) {
            continue;
        }
    }

    return result;
}

LocalDictToZarrResult local_dict_to_zarr(
    const std::vector<LocalDictToZarrEntry>& props_to_add,
    const std::string& zarr_path,
    const std::string& zarr_id) {
    const auto label_paths = label_paths_for_root(fs::path(zarr_path));

    LocalDictToZarrResult result{};
    for (const auto& label_path : label_paths) {
        auto label_state = load_group_attrs_state(fs::path(label_path), true);
        json& attrs = label_state.attrs;
        if (!attrs.is_object() || !attrs.contains("image-label") ||
            !attrs["image-label"].is_object() ||
            !attrs["image-label"].contains("properties") ||
            !attrs["image-label"]["properties"].is_array()) {
            persist_group_attrs_state(label_state);
            continue;
        }

        bool changed = false;
        for (auto& props_dict : attrs["image-label"]["properties"]) {
            if (!props_dict.is_object()) {
                continue;
            }
            const auto props_id = python_like_string(
                props_dict.contains(zarr_id) ? props_dict[zarr_id] : json(nullptr));
            for (const auto& entry : props_to_add) {
                if (!entry.key_is_string || entry.key_text != props_id) {
                    continue;
                }
                if (!entry.values.is_object()) {
                    throw std::invalid_argument(
                        "props_to_add values must be JSON objects");
                }
                for (const auto& item : entry.values.items()) {
                    props_dict[item.key()] = item.value();
                }
                result.updated_properties += 1U;
                changed = true;
                break;
            }
        }
        persist_group_attrs_state(label_state);
        result.touched_label_groups += 1U;
        if (!changed) {
            continue;
        }
    }

    return result;
}

}  // namespace ome_zarr_c::native_code
