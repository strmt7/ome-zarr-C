#include "scale_runtime.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <blosc.h>
#include <zstd.h>

#include "../../third_party/nlohmann/json.hpp"
#include "axes.hpp"
#include "cli.hpp"
#include "format.hpp"
#include "scale.hpp"
#include "writer.hpp"

namespace ome_zarr_c::native_code {

namespace {

using json = nlohmann::json;
namespace fs = std::filesystem;

constexpr double kGaussianTruncate = 4.0;

struct ArraySpec {
    bool is_v3 = false;
    std::vector<std::int64_t> shape;
    std::vector<std::int64_t> chunk_shape;
    std::string data_type;
    bool little_endian = true;
    json attrs = json::object();
    json metadata = json::object();
    char separator = '/';
};

bool host_is_little_endian() {
    return std::endian::native == std::endian::little;
}

std::vector<char> read_binary_file(const fs::path& path) {
    std::ifstream stream(path, std::ios::binary);
    if (!stream) {
        throw std::runtime_error("Unable to open binary file: " + path.string());
    }
    return {
        std::istreambuf_iterator<char>(stream),
        std::istreambuf_iterator<char>()};
}

void write_binary_file(const fs::path& path, const std::vector<char>& payload) {
    std::ofstream stream(path, std::ios::binary | std::ios::trunc);
    if (!stream) {
        throw std::runtime_error("Unable to write binary file: " + path.string());
    }
    stream.write(payload.data(), static_cast<std::streamsize>(payload.size()));
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
    stream << payload.dump(2) << "\n";
}

std::size_t shape_product(const std::vector<std::int64_t>& shape) {
    std::size_t total = 1;
    for (const auto dim : shape) {
        total *= static_cast<std::size_t>(dim);
    }
    return total;
}

std::vector<std::size_t> c_strides(const std::vector<std::int64_t>& shape) {
    std::vector<std::size_t> strides(shape.size(), 1);
    std::size_t stride = 1;
    for (std::size_t index = shape.size(); index-- > 0;) {
        strides[index] = stride;
        stride *= static_cast<std::size_t>(shape[index]);
    }
    return strides;
}

std::vector<std::int64_t> json_int_vector(const json& values) {
    std::vector<std::int64_t> result;
    result.reserve(values.size());
    for (const auto& value : values) {
        result.push_back(value.get<std::int64_t>());
    }
    return result;
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

std::size_t data_type_size(const std::string& data_type) {
    if (data_type == "bool") {
        return 1;
    }
    if (data_type == "int8" || data_type == "uint8") {
        return 1;
    }
    if (data_type == "int16" || data_type == "uint16") {
        return 2;
    }
    if (data_type == "int32" || data_type == "uint32" || data_type == "float32") {
        return 4;
    }
    if (data_type == "int64" || data_type == "uint64" || data_type == "float64") {
        return 8;
    }
    throw std::invalid_argument("Unsupported data_type: " + data_type);
}

std::pair<std::string, bool> data_type_from_v2_dtype(const std::string& dtype) {
    if (dtype.size() < 3) {
        throw std::invalid_argument("Unsupported v2 dtype: " + dtype);
    }

    bool little_endian = true;
    switch (dtype[0]) {
        case '<':
            little_endian = true;
            break;
        case '>':
            little_endian = false;
            break;
        case '|':
            little_endian = host_is_little_endian();
            break;
        default:
            throw std::invalid_argument("Unsupported v2 dtype endianness: " + dtype);
    }

    const auto bit_count = static_cast<std::size_t>(std::stoull(dtype.substr(2))) * 8U;
    std::string data_type;
    switch (dtype[1]) {
        case 'i':
            data_type = "int" + std::to_string(bit_count);
            break;
        case 'u':
            data_type = "uint" + std::to_string(bit_count);
            break;
        case 'f':
            data_type = "float" + std::to_string(bit_count);
            break;
        case 'b':
            data_type = "bool";
            break;
        default:
            throw std::invalid_argument("Unsupported v2 dtype kind: " + dtype);
    }
    return {data_type, little_endian};
}

ArraySpec load_array_spec(const fs::path& root) {
    ArraySpec spec{};

    const fs::path v3_json = root / "zarr.json";
    if (fs::exists(v3_json)) {
        spec.is_v3 = true;
        spec.metadata = load_json_file(v3_json);
        if (!spec.metadata.is_object() ||
            !spec.metadata.contains("node_type") ||
            !spec.metadata["node_type"].is_string() ||
            spec.metadata["node_type"].get<std::string>() != "array") {
            throw std::invalid_argument("not an array: None");
        }
        spec.shape = json_int_vector(spec.metadata.at("shape"));
        spec.chunk_shape = json_int_vector(
            spec.metadata.at("chunk_grid").at("configuration").at("chunk_shape"));
        spec.data_type = spec.metadata.at("data_type").get<std::string>();
        if (spec.metadata.contains("attributes") && spec.metadata["attributes"].is_object()) {
            spec.attrs = spec.metadata["attributes"];
        }
        if (spec.metadata.contains("chunk_key_encoding") &&
            spec.metadata["chunk_key_encoding"].is_object() &&
            spec.metadata["chunk_key_encoding"].contains("configuration") &&
            spec.metadata["chunk_key_encoding"]["configuration"].is_object() &&
            spec.metadata["chunk_key_encoding"]["configuration"].contains("separator") &&
            spec.metadata["chunk_key_encoding"]["configuration"]["separator"].is_string()) {
            const auto separator =
                spec.metadata["chunk_key_encoding"]["configuration"]["separator"]
                    .get<std::string>();
            if (!separator.empty()) {
                spec.separator = separator[0];
            }
        }
        if (spec.metadata.contains("codecs") && spec.metadata["codecs"].is_array()) {
            for (const auto& codec : spec.metadata["codecs"]) {
                if (!codec.is_object() ||
                    !codec.contains("name") ||
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
                    spec.little_endian = endian != "big";
                }
            }
        }
        return spec;
    }

    const fs::path v2_json = root / ".zarray";
    if (!fs::exists(v2_json)) {
        throw std::invalid_argument("not an array: None");
    }

    spec.metadata = load_json_file(v2_json);
    spec.shape = json_int_vector(spec.metadata.at("shape"));
    spec.chunk_shape = json_int_vector(spec.metadata.at("chunks"));
    const auto [data_type, little_endian] =
        data_type_from_v2_dtype(spec.metadata.at("dtype").get<std::string>());
    spec.data_type = data_type;
    spec.little_endian = little_endian;
    if (spec.metadata.contains("dimension_separator") &&
        spec.metadata["dimension_separator"].is_string()) {
        const auto separator = spec.metadata["dimension_separator"].get<std::string>();
        if (!separator.empty()) {
            spec.separator = separator[0];
        }
    }
    const fs::path zattrs = root / ".zattrs";
    if (fs::exists(zattrs)) {
        const auto attrs = load_json_file(zattrs);
        if (attrs.is_object()) {
            spec.attrs = attrs;
        }
    }
    return spec;
}

std::vector<std::int64_t> chunk_indices_from_relative(
    const fs::path& relative,
    const char separator) {
    std::vector<std::int64_t> indices;
    if (separator == '.') {
        const auto parts = split_string(relative.filename().generic_string(), '.');
        indices.reserve(parts.size());
        for (const auto& part : parts) {
            indices.push_back(std::stoll(part));
        }
        return indices;
    }
    indices.reserve(std::distance(relative.begin(), relative.end()));
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

void ensure_blosc_initialized() {
    static const bool initialized = [] {
        blosc_init();
        return true;
    }();
    static_cast<void>(initialized);
}

std::vector<char> decode_zstd_chunk(
    const std::vector<char>& compressed,
    const std::size_t raw_size) {
    std::vector<char> raw(raw_size);
    const auto decompressed = ZSTD_decompress(
        raw.data(),
        raw.size(),
        compressed.data(),
        compressed.size());
    if (ZSTD_isError(decompressed) != 0U) {
        throw std::runtime_error(
            "ZSTD decompress failed: " +
            std::string(ZSTD_getErrorName(decompressed)));
    }
    raw.resize(static_cast<std::size_t>(decompressed));
    return raw;
}

std::vector<char> decode_blosc_chunk(
    const std::vector<char>& compressed,
    const std::size_t raw_size) {
    ensure_blosc_initialized();
    std::vector<char> raw(raw_size);
    const int decoded = blosc_decompress_ctx(
        compressed.data(),
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
    const ArraySpec& spec,
    const fs::path& relative) {
    const auto indices = chunk_indices_from_relative(relative, spec.separator);
    const auto raw_size = chunk_uncompressed_nbytes(
        spec.shape,
        spec.chunk_shape,
        indices,
        data_type_size(spec.data_type));
    if (!spec.metadata.contains("compressor") || spec.metadata["compressor"].is_null()) {
        return source_bytes;
    }
    if (!spec.metadata["compressor"].is_object() ||
        !spec.metadata["compressor"].contains("id") ||
        !spec.metadata["compressor"]["id"].is_string()) {
        throw std::invalid_argument("Unsupported v2 compressor metadata");
    }
    const auto codec = spec.metadata["compressor"]["id"].get<std::string>();
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
    const ArraySpec& spec,
    const fs::path& relative) {
    const auto indices = chunk_indices_from_relative(relative, spec.separator);
    const auto raw_size = chunk_uncompressed_nbytes(
        spec.shape,
        spec.chunk_shape,
        indices,
        data_type_size(spec.data_type));
    std::vector<char> decoded = source_bytes;
    if (!spec.metadata.contains("codecs") || !spec.metadata["codecs"].is_array()) {
        return decoded;
    }
    const auto& codecs = spec.metadata["codecs"];
    for (auto it = codecs.rbegin(); it != codecs.rend(); ++it) {
        if (!it->is_object() || !it->contains("name") || !(*it)["name"].is_string()) {
            continue;
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

double decode_numeric_value(
    const char* data,
    const std::string& data_type,
    const bool little_endian) {
    if (data_type == "bool") {
        return static_cast<double>(decode_scalar<bool, std::uint8_t>(data, little_endian));
    }
    if (data_type == "int8") {
        return static_cast<double>(decode_scalar<std::int8_t>(data, little_endian));
    }
    if (data_type == "uint8") {
        return static_cast<double>(decode_scalar<std::uint8_t>(data, little_endian));
    }
    if (data_type == "int16") {
        return static_cast<double>(decode_scalar<std::int16_t>(data, little_endian));
    }
    if (data_type == "uint16") {
        return static_cast<double>(decode_scalar<std::uint16_t>(data, little_endian));
    }
    if (data_type == "int32") {
        return static_cast<double>(decode_scalar<std::int32_t>(data, little_endian));
    }
    if (data_type == "uint32") {
        return static_cast<double>(decode_scalar<std::uint32_t>(data, little_endian));
    }
    if (data_type == "int64") {
        return static_cast<double>(decode_scalar<std::int64_t>(data, little_endian));
    }
    if (data_type == "uint64") {
        return static_cast<double>(decode_scalar<std::uint64_t>(data, little_endian));
    }
    if (data_type == "float32") {
        return static_cast<double>(decode_scalar<float, std::uint32_t>(data, little_endian));
    }
    if (data_type == "float64") {
        return decode_scalar<double, std::uint64_t>(data, little_endian);
    }
    throw std::invalid_argument("Unsupported data_type: " + data_type);
}

template <typename Value>
void write_scalar_native(
    std::vector<char>& raw,
    const std::size_t offset,
    Value value) {
    std::memcpy(raw.data() + offset, &value, sizeof(Value));
}

void write_numeric_value(
    std::vector<char>& raw,
    const std::size_t offset,
    const std::string& data_type,
    const double value) {
    if (data_type == "bool") {
        write_scalar_native<std::uint8_t>(
            raw,
            offset,
            static_cast<std::uint8_t>(value != 0.0));
        return;
    }
    if (data_type == "int8") {
        write_scalar_native<std::int8_t>(raw, offset, static_cast<std::int8_t>(value));
        return;
    }
    if (data_type == "uint8") {
        write_scalar_native<std::uint8_t>(raw, offset, static_cast<std::uint8_t>(value));
        return;
    }
    if (data_type == "int16") {
        write_scalar_native<std::int16_t>(raw, offset, static_cast<std::int16_t>(value));
        return;
    }
    if (data_type == "uint16") {
        write_scalar_native<std::uint16_t>(raw, offset, static_cast<std::uint16_t>(value));
        return;
    }
    if (data_type == "int32") {
        write_scalar_native<std::int32_t>(raw, offset, static_cast<std::int32_t>(value));
        return;
    }
    if (data_type == "uint32") {
        write_scalar_native<std::uint32_t>(raw, offset, static_cast<std::uint32_t>(value));
        return;
    }
    if (data_type == "int64") {
        write_scalar_native<std::int64_t>(raw, offset, static_cast<std::int64_t>(value));
        return;
    }
    if (data_type == "uint64") {
        write_scalar_native<std::uint64_t>(raw, offset, static_cast<std::uint64_t>(value));
        return;
    }
    if (data_type == "float32") {
        write_scalar_native<float>(raw, offset, static_cast<float>(value));
        return;
    }
    if (data_type == "float64") {
        write_scalar_native<double>(raw, offset, value);
        return;
    }
    throw std::invalid_argument("Unsupported data_type: " + data_type);
}

void scatter_chunk_into_array(
    const std::vector<char>& decoded,
    const ArraySpec& spec,
    const std::vector<std::int64_t>& indices,
    std::vector<double>& out) {
    const auto extents = [&] {
        std::vector<std::int64_t> result;
        result.reserve(spec.shape.size());
        for (std::size_t axis = 0; axis < spec.shape.size(); ++axis) {
            const auto start = indices[axis] * spec.chunk_shape[axis];
            result.push_back(
                std::min<std::int64_t>(spec.chunk_shape[axis], spec.shape[axis] - start));
        }
        return result;
    }();
    const auto global_strides = c_strides(spec.shape);
    const auto local_strides = c_strides(extents);
    const auto item_size = data_type_size(spec.data_type);
    const auto local_elements = shape_product(extents);

    for (std::size_t local_index = 0; local_index < local_elements; ++local_index) {
        std::size_t remaining = local_index;
        std::size_t global_index = 0;
        for (std::size_t axis = 0; axis < extents.size(); ++axis) {
            const auto coord = static_cast<std::int64_t>(remaining / local_strides[axis]);
            remaining %= local_strides[axis];
            const auto global_coord = indices[axis] * spec.chunk_shape[axis] + coord;
            global_index +=
                static_cast<std::size_t>(global_coord) * global_strides[axis];
        }
        out[global_index] = decode_numeric_value(
            decoded.data() + (local_index * item_size),
            spec.data_type,
            spec.little_endian);
    }
}

std::vector<double> read_array_as_double(const fs::path& root, const ArraySpec& spec) {
    std::vector<double> out(shape_product(spec.shape));
    const fs::path chunk_root =
        spec.is_v3 && fs::exists(root / "c") ? root / "c" : root;
    if (!fs::exists(chunk_root)) {
        return out;
    }

    for (const auto& entry : fs::recursive_directory_iterator(chunk_root)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        const auto filename = entry.path().filename().generic_string();
        if (chunk_root == root &&
            (filename == ".zarray" || filename == ".zattrs" || filename == ".zgroup" ||
             filename == "zarr.json")) {
            continue;
        }
        const auto relative = fs::relative(entry.path(), chunk_root);
        const auto indices = chunk_indices_from_relative(relative, spec.separator);
        const auto decoded = spec.is_v3
            ? decode_v3_chunk(read_binary_file(entry.path()), spec, relative)
            : decode_v2_chunk(read_binary_file(entry.path()), spec, relative);
        scatter_chunk_into_array(decoded, spec, indices, out);
    }

    return out;
}

long long mirror_index(long long index, const long long size) {
    if (size <= 1) {
        return 0;
    }
    const long long period = 2 * size - 2;
    long long value = index % period;
    if (value < 0) {
        value += period;
    }
    if (value >= size) {
        value = period - value;
    }
    return value;
}

std::vector<double> gaussian_kernel(const double sigma) {
    if (!(sigma > 0.0)) {
        return {1.0};
    }
    const int radius = std::max(1, static_cast<int>(std::llround(kGaussianTruncate * sigma)));
    std::vector<double> kernel(static_cast<std::size_t>(radius * 2 + 1), 0.0);
    double total = 0.0;
    for (int offset = -radius; offset <= radius; ++offset) {
        const double exponent = -0.5 * std::pow(static_cast<double>(offset) / sigma, 2.0);
        const double weight = std::exp(exponent);
        kernel[static_cast<std::size_t>(offset + radius)] = weight;
        total += weight;
    }
    for (double& weight : kernel) {
        weight /= total;
    }
    return kernel;
}

std::vector<double> gaussian_filter_axis(
    const std::vector<double>& input,
    const std::vector<std::int64_t>& shape,
    const std::size_t axis,
    const double sigma) {
    if (!(sigma > 0.0)) {
        return input;
    }

    const auto kernel = gaussian_kernel(sigma);
    const int radius = static_cast<int>((kernel.size() - 1) / 2);
    const std::size_t outer = shape_product(
        std::vector<std::int64_t>(shape.begin(), shape.begin() + static_cast<std::ptrdiff_t>(axis)));
    const std::size_t inner = shape_product(
        std::vector<std::int64_t>(shape.begin() + static_cast<std::ptrdiff_t>(axis + 1), shape.end()));
    const std::size_t axis_len = static_cast<std::size_t>(shape[axis]);
    std::vector<double> output(input.size(), 0.0);

    for (std::size_t outer_index = 0; outer_index < outer; ++outer_index) {
        for (std::size_t inner_index = 0; inner_index < inner; ++inner_index) {
            const std::size_t input_base = outer_index * axis_len * inner + inner_index;
            for (std::size_t out_index = 0; out_index < axis_len; ++out_index) {
                double accum = 0.0;
                for (int offset = -radius; offset <= radius; ++offset) {
                    const auto sample_index = static_cast<std::size_t>(
                        mirror_index(
                            static_cast<long long>(out_index) + offset,
                            static_cast<long long>(axis_len)));
                    accum += kernel[static_cast<std::size_t>(offset + radius)] *
                        input[input_base + sample_index * inner];
                }
                output[input_base + out_index * inner] = accum;
            }
        }
    }

    return output;
}

double grid_mode_true_coord(
    const std::size_t out_index,
    const std::size_t in_len,
    const std::size_t out_len) {
    const double scale = static_cast<double>(in_len) / static_cast<double>(out_len);
    return (static_cast<double>(out_index) + 0.5) * scale - 0.5;
}

double grid_mode_false_coord(
    const std::size_t out_index,
    const std::size_t in_len,
    const std::size_t out_len) {
    if (out_len <= 1 || in_len <= 1) {
        return 0.0;
    }
    return static_cast<double>(out_index) *
        static_cast<double>(in_len - 1) /
        static_cast<double>(out_len - 1);
}

std::vector<double> resample_axis_nearest(
    const std::vector<double>& input,
    const std::vector<std::int64_t>& shape,
    const std::size_t axis,
    const std::size_t out_len) {
    auto out_shape = shape;
    out_shape[axis] = static_cast<std::int64_t>(out_len);
    std::vector<double> output(shape_product(out_shape), 0.0);

    const std::size_t outer = shape_product(
        std::vector<std::int64_t>(shape.begin(), shape.begin() + static_cast<std::ptrdiff_t>(axis)));
    const std::size_t inner = shape_product(
        std::vector<std::int64_t>(shape.begin() + static_cast<std::ptrdiff_t>(axis + 1), shape.end()));
    const std::size_t axis_len = static_cast<std::size_t>(shape[axis]);

    for (std::size_t outer_index = 0; outer_index < outer; ++outer_index) {
        for (std::size_t inner_index = 0; inner_index < inner; ++inner_index) {
            const std::size_t input_base = outer_index * axis_len * inner + inner_index;
            const std::size_t output_base = outer_index * out_len * inner + inner_index;
            for (std::size_t out_index = 0; out_index < out_len; ++out_index) {
                const auto coord = grid_mode_true_coord(out_index, axis_len, out_len);
                const auto nearest = static_cast<std::size_t>(
                    mirror_index(
                        static_cast<long long>(std::floor(coord + 0.5)),
                        static_cast<long long>(axis_len)));
                output[output_base + out_index * inner] =
                    input[input_base + nearest * inner];
            }
        }
    }

    return output;
}

std::vector<double> resample_axis_linear(
    const std::vector<double>& input,
    const std::vector<std::int64_t>& shape,
    const std::size_t axis,
    const std::size_t out_len,
    const bool grid_mode_true,
    const bool mirror_mode) {
    auto out_shape = shape;
    out_shape[axis] = static_cast<std::int64_t>(out_len);
    std::vector<double> output(shape_product(out_shape), 0.0);

    const std::size_t outer = shape_product(
        std::vector<std::int64_t>(shape.begin(), shape.begin() + static_cast<std::ptrdiff_t>(axis)));
    const std::size_t inner = shape_product(
        std::vector<std::int64_t>(shape.begin() + static_cast<std::ptrdiff_t>(axis + 1), shape.end()));
    const std::size_t axis_len = static_cast<std::size_t>(shape[axis]);

    auto sample = [&](const std::size_t input_base, long long index) -> double {
        if (mirror_mode) {
            index = mirror_index(index, static_cast<long long>(axis_len));
            return input[input_base + static_cast<std::size_t>(index) * inner];
        }
        if (index < 0 || index >= static_cast<long long>(axis_len)) {
            return 0.0;
        }
        return input[input_base + static_cast<std::size_t>(index) * inner];
    };

    for (std::size_t outer_index = 0; outer_index < outer; ++outer_index) {
        for (std::size_t inner_index = 0; inner_index < inner; ++inner_index) {
            const std::size_t input_base = outer_index * axis_len * inner + inner_index;
            const std::size_t output_base = outer_index * out_len * inner + inner_index;
            for (std::size_t out_index = 0; out_index < out_len; ++out_index) {
                const auto coord = grid_mode_true
                    ? grid_mode_true_coord(out_index, axis_len, out_len)
                    : grid_mode_false_coord(out_index, axis_len, out_len);
                const auto lower = static_cast<long long>(std::floor(coord));
                const auto upper = lower + 1;
                const double alpha = coord - static_cast<double>(lower);
                const double low_value = sample(input_base, lower);
                const double high_value = sample(input_base, upper);
                output[output_base + out_index * inner] =
                    ((1.0 - alpha) * low_value) + (alpha * high_value);
            }
        }
    }

    return output;
}

std::vector<double> downsample_axis_mean(
    const std::vector<double>& input,
    const std::vector<std::int64_t>& shape,
    const std::size_t axis,
    const std::size_t factor) {
    auto out_shape = shape;
    out_shape[axis] = shape[axis] / static_cast<std::int64_t>(factor);
    std::vector<double> output(shape_product(out_shape), 0.0);

    const std::size_t outer = shape_product(
        std::vector<std::int64_t>(shape.begin(), shape.begin() + static_cast<std::ptrdiff_t>(axis)));
    const std::size_t inner = shape_product(
        std::vector<std::int64_t>(shape.begin() + static_cast<std::ptrdiff_t>(axis + 1), shape.end()));
    const std::size_t axis_len = static_cast<std::size_t>(shape[axis]);
    const std::size_t out_len = static_cast<std::size_t>(out_shape[axis]);

    for (std::size_t outer_index = 0; outer_index < outer; ++outer_index) {
        for (std::size_t inner_index = 0; inner_index < inner; ++inner_index) {
            const std::size_t input_base = outer_index * axis_len * inner + inner_index;
            const std::size_t output_base = outer_index * out_len * inner + inner_index;
            for (std::size_t out_index = 0; out_index < out_len; ++out_index) {
                double sum = 0.0;
                for (std::size_t factor_index = 0; factor_index < factor; ++factor_index) {
                    sum += input[input_base + (out_index * factor + factor_index) * inner];
                }
                output[output_base + out_index * inner] =
                    sum / static_cast<double>(factor);
            }
        }
    }

    return output;
}

std::vector<double> scale_level_data(
    const std::vector<double>& input,
    const std::vector<std::int64_t>& current_shape,
    const std::vector<std::string>& dims,
    const PyramidLevelPlan& plan,
    const std::string& method) {
    std::vector<double> working = input;
    auto working_shape = current_shape;

    if (method == "nearest") {
        for (std::size_t axis = 0; axis < dims.size(); ++axis) {
            if (working_shape[axis] == plan.target_shape[axis]) {
                continue;
            }
            working = resample_axis_nearest(
                working,
                working_shape,
                axis,
                static_cast<std::size_t>(plan.target_shape[axis]));
            working_shape[axis] = plan.target_shape[axis];
        }
        return working;
    }

    if (method == "local_mean") {
        for (std::size_t axis = 0; axis < dims.size(); ++axis) {
            if (working_shape[axis] == plan.target_shape[axis]) {
                continue;
            }
            const auto factor = static_cast<std::size_t>(std::llround(
                plan.relative_factors.values[axis]));
            working = downsample_axis_mean(working, working_shape, axis, factor);
            working_shape[axis] = plan.target_shape[axis];
        }
        return working;
    }

    if (method == "resize") {
        for (std::size_t axis = 0; axis < dims.size(); ++axis) {
            const double relative = plan.relative_factors.values[axis];
            if (!(relative > 1.0) ||
                (dims[axis] != "x" && dims[axis] != "y" && dims[axis] != "z")) {
                continue;
            }
            working = gaussian_filter_axis(working, working_shape, axis, (relative - 1.0) / 2.0);
        }
        for (std::size_t axis = 0; axis < dims.size(); ++axis) {
            if (working_shape[axis] == plan.target_shape[axis]) {
                continue;
            }
            working = resample_axis_linear(
                working,
                working_shape,
                axis,
                static_cast<std::size_t>(plan.target_shape[axis]),
                true,
                true);
            working_shape[axis] = plan.target_shape[axis];
        }
        return working;
    }

    if (method == "zoom") {
        for (std::size_t axis = 0; axis < dims.size(); ++axis) {
            if (working_shape[axis] == plan.target_shape[axis]) {
                continue;
            }
            working = resample_axis_linear(
                working,
                working_shape,
                axis,
                static_cast<std::size_t>(plan.target_shape[axis]),
                false,
                false);
            working_shape[axis] = plan.target_shape[axis];
        }
        return working;
    }

    throw std::invalid_argument("'" + method + "' is not a valid Methods");
}

std::vector<AxisRecord> axis_records_from_string(const std::string& axes) {
    std::vector<AxisRecord> records;
    records.reserve(axes.size());
    for (const char axis : axes) {
        AxisRecord record{};
        record.has_name = true;
        record.name = std::string(1, axis);
        records.push_back(std::move(record));
    }
    return records;
}

json axes_json(const std::vector<AxisRecord>& axes) {
    json payload = json::array();
    for (const auto& axis : axes) {
        json item = {{"name", axis.name}};
        if (axis.has_type) {
            item["type"] = axis.type;
        }
        payload.push_back(std::move(item));
    }
    return payload;
}

std::string python_string_list_repr(const std::vector<std::string>& values) {
    std::string text = "[";
    for (std::size_t index = 0; index < values.size(); ++index) {
        if (index > 0) {
            text += ", ";
        }
        text += "'" + values[index] + "'";
    }
    text += "]";
    return text;
}

std::vector<char> encode_zstd_chunk(const std::vector<char>& raw) {
    std::vector<char> compressed(ZSTD_compressBound(raw.size()));
    const auto written = ZSTD_compress(
        compressed.data(),
        compressed.size(),
        raw.data(),
        raw.size(),
        0);
    if (ZSTD_isError(written) != 0U) {
        throw std::runtime_error(
            "ZSTD_compress failed: " + std::string(ZSTD_getErrorName(written)));
    }
    compressed.resize(static_cast<std::size_t>(written));
    return compressed;
}

std::vector<std::int64_t> min_chunk_shape(
    const std::vector<std::int64_t>& base_chunk_shape,
    const std::vector<std::int64_t>& shape) {
    std::vector<std::int64_t> result;
    result.reserve(shape.size());
    for (std::size_t axis = 0; axis < shape.size(); ++axis) {
        result.push_back(std::min(base_chunk_shape[axis], shape[axis]));
    }
    return result;
}

std::string chunk_component_path(const std::vector<std::int64_t>& indices) {
    std::string path;
    for (std::size_t axis = 0; axis < indices.size(); ++axis) {
        if (axis > 0) {
            path += "/";
        }
        path += std::to_string(indices[axis]);
    }
    return path;
}

void write_v3_dataset(
    const fs::path& root,
    const std::string& component,
    const std::vector<double>& data,
    const std::vector<std::int64_t>& shape,
    const std::vector<std::int64_t>& chunk_shape,
    const std::string& data_type,
    const std::vector<std::string>& axis_names) {
    const fs::path dataset_root = root / component;
    fs::create_directories(dataset_root);
    write_json_file(
        dataset_root / "zarr.json",
        json{
            {"shape", shape},
            {"data_type", data_type},
            {"chunk_grid",
             {{"name", "regular"},
              {"configuration", {{"chunk_shape", chunk_shape}}}}},
            {"chunk_key_encoding",
             {{"name", "default"},
              {"configuration", {{"separator", "/"}}}}},
            {"fill_value", 0},
            {"codecs",
             json::array({
                 json{
                     {"name", "bytes"},
                     {"configuration", {{"endian", "little"}}},
                 },
                 json{
                     {"name", "zstd"},
                     {"configuration", {{"level", 0}, {"checksum", false}}},
                 },
             })},
            {"attributes", json::object()},
            {"dimension_names", axis_names},
            {"zarr_format", 3},
            {"node_type", "array"},
            {"storage_transformers", json::array()},
        });

    const auto shape_strides = c_strides(shape);
    const auto chunk_strides = c_strides(chunk_shape);
    std::vector<std::int64_t> chunk_grid(shape.size(), 0);
    for (std::size_t axis = 0; axis < shape.size(); ++axis) {
        chunk_grid[axis] =
            (shape[axis] + chunk_shape[axis] - 1) / chunk_shape[axis];
    }
    const auto chunk_grid_elements = shape_product(chunk_grid);
    const auto item_size = data_type_size(data_type);

    for (std::size_t chunk_linear = 0; chunk_linear < chunk_grid_elements; ++chunk_linear) {
        std::size_t remaining = chunk_linear;
        std::vector<std::int64_t> chunk_indices(shape.size(), 0);
        for (std::size_t axis = 0; axis < chunk_grid.size(); ++axis) {
            const auto stride = c_strides(chunk_grid)[axis];
            chunk_indices[axis] = static_cast<std::int64_t>(remaining / stride);
            remaining %= stride;
        }

        std::vector<std::int64_t> extents;
        extents.reserve(shape.size());
        for (std::size_t axis = 0; axis < shape.size(); ++axis) {
            const auto start = chunk_indices[axis] * chunk_shape[axis];
            extents.push_back(std::min<std::int64_t>(chunk_shape[axis], shape[axis] - start));
        }
        const auto chunk_elements = shape_product(extents);
        std::vector<char> raw(chunk_elements * item_size);
        const auto extent_strides = c_strides(extents);

        for (std::size_t local_index = 0; local_index < chunk_elements; ++local_index) {
            std::size_t rem = local_index;
            std::size_t global_index = 0;
            for (std::size_t axis = 0; axis < extents.size(); ++axis) {
                const auto coord = static_cast<std::int64_t>(rem / extent_strides[axis]);
                rem %= extent_strides[axis];
                const auto global_coord = chunk_indices[axis] * chunk_shape[axis] + coord;
                global_index += static_cast<std::size_t>(global_coord) * shape_strides[axis];
            }
            write_numeric_value(raw, local_index * item_size, data_type, data[global_index]);
        }

        const auto compressed = encode_zstd_chunk(raw);
        const fs::path chunk_path =
            dataset_root / "c" / fs::path(chunk_component_path(chunk_indices));
        fs::create_directories(chunk_path.parent_path());
        write_binary_file(chunk_path, compressed);
    }
}

}  // namespace

LocalScaleResult local_scale_array(
    const std::string& input_array,
    const std::string& output_directory,
    const std::string& axes,
    const bool copy_metadata,
    const std::string& method,
    const bool in_place,
    const std::int64_t downscale,
    const std::int64_t max_layer) {
    static_cast<void>(in_place);

    if (method != "resize" && method != "nearest" && method != "local_mean" &&
        method != "zoom") {
        throw std::invalid_argument("'" + method + "' is not a valid Methods");
    }

    const fs::path input_root(input_array);
    const ArraySpec input_spec = load_array_spec(input_root);

    auto axes_records = axis_records_from_string(axes);
    validate_axes_length(axes_records.size(), static_cast<std::int64_t>(input_spec.shape.size()));
    axes_records = axes_to_dicts(axes_records);
    validate_axes_types(axes_records);
    const auto dims = extract_dims_from_axes(axes_records);
    const auto scale_levels = scale_levels_from_ints(
        dims,
        static_cast<std::size_t>(max_layer));
    const auto pyramid_plan = build_pyramid_plan(input_spec.shape, dims, scale_levels);

    LocalScaleResult result{};
    result.output_root = output_directory;

    if (copy_metadata) {
        for (auto it = input_spec.attrs.begin(); it != input_spec.attrs.end(); ++it) {
            result.copied_metadata_keys.push_back(it.key());
        }
    }

    auto base_chunk_shape = input_spec.chunk_shape;
    for (std::size_t axis = 0; axis < base_chunk_shape.size(); ++axis) {
        base_chunk_shape[axis] = std::min(base_chunk_shape[axis], input_spec.shape[axis]);
    }

    std::vector<std::vector<double>> levels;
    levels.reserve(static_cast<std::size_t>(max_layer) + 1U);
    std::vector<std::vector<std::int64_t>> shapes;
    shapes.reserve(static_cast<std::size_t>(max_layer) + 1U);

    levels.push_back(read_array_as_double(input_root, input_spec));
    shapes.push_back(input_spec.shape);
    result.levels.push_back(LocalScaleLevelSummary{input_spec.shape});

    for (const auto& level_plan : pyramid_plan) {
        levels.push_back(scale_level_data(
            levels.back(),
            shapes.back(),
            dims,
            level_plan,
            method));
        shapes.push_back(level_plan.target_shape);
        result.levels.push_back(LocalScaleLevelSummary{level_plan.target_shape});
    }

    const fs::path destination_root(output_directory);
    fs::create_directories(destination_root);

    json attributes = json::object();
    attributes["ome"] = {
        {"version", "0.5"},
        {"multiscales", json::array()},
    };
    if (copy_metadata) {
        for (auto it = input_spec.attrs.begin(); it != input_spec.attrs.end(); ++it) {
            attributes[it.key()] = it.value();
        }
    }

    std::vector<std::vector<double>> shape_doubles;
    shape_doubles.reserve(shapes.size());
    for (const auto& shape : shapes) {
        std::vector<double> values;
        values.reserve(shape.size());
        for (const auto dim : shape) {
            values.push_back(static_cast<double>(dim));
        }
        shape_doubles.push_back(std::move(values));
    }
    const auto transforms = generate_coordinate_transformations(shape_doubles);
    json datasets = json::array();
    for (std::size_t index = 0; index < shapes.size(); ++index) {
        json coordinate_transformations = json::array();
        for (const auto& transform : transforms[index]) {
            coordinate_transformations.push_back(
                json{{"type", transform.type}, {"scale", transform.values}});
        }
        datasets.push_back(
            json{
                {"path", "s" + std::to_string(index)},
                {"coordinateTransformations", std::move(coordinate_transformations)},
            });
    }
    attributes["ome"]["multiscales"].push_back(
        json{
            {"datasets", std::move(datasets)},
            {"name", "/"},
            {"axes", axes_json(axes_records)},
        });

    write_json_file(
        destination_root / "zarr.json",
        json{
            {"attributes", std::move(attributes)},
            {"zarr_format", 3},
            {"node_type", "group"},
        });

    const auto axis_names = get_names(axes_records);
    for (std::size_t index = 0; index < levels.size(); ++index) {
        write_v3_dataset(
            destination_root,
            "s" + std::to_string(index),
            levels[index],
            shapes[index],
            min_chunk_shape(base_chunk_shape, shapes[index]),
            input_spec.data_type,
            axis_names);
    }

    return result;
}

}  // namespace ome_zarr_c::native_code
