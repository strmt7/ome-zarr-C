#include "ome_zarr_native_api.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <limits>
#include <memory>
#include <new>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "../../third_party/nlohmann/json.hpp"

#include "../native/conversions.hpp"
#include "../native/csv.hpp"
#include "../native/io.hpp"
#include "../native/format.hpp"
#include "../native/scale.hpp"
#include "../native/version.hpp"

namespace {

using Json = nlohmann::json;

using ome_zarr_c::native_code::CsvValue;
using ome_zarr_c::native_code::format_chunk_key_encoding;
using ome_zarr_c::native_code::format_class_name;
using ome_zarr_c::native_code::format_zarr_format;
using ome_zarr_c::native_code::int_to_rgba;
using ome_zarr_c::native_code::int_to_rgba_255_bytes;
using ome_zarr_c::native_code::local_io_signature;
using ome_zarr_c::native_code::parse_csv_value;
using ome_zarr_c::native_code::rgba_to_int;
using ome_zarr_c::native_code::scaler_methods;
using ome_zarr_c::native_code::scaler_resize_image_shape;

char* duplicate_string(const std::string& value) {
    char* output = new char[value.size() + 1];
    std::memcpy(output, value.c_str(), value.size() + 1);
    return output;
}

OmeZarrNativeApiResult make_success_json(const Json& payload) {
    return OmeZarrNativeApiResult{
        1,
        duplicate_string(payload.dump()),
        nullptr,
        nullptr,
    };
}

OmeZarrNativeApiResult make_success_raw_json(const std::string& payload) {
    return OmeZarrNativeApiResult{1, duplicate_string(payload), nullptr, nullptr};
}

OmeZarrNativeApiResult make_error(
    const std::string& error_type,
    const std::string& message) {
    return OmeZarrNativeApiResult{
        0,
        nullptr,
        duplicate_string(error_type),
        duplicate_string(message),
    };
}

OmeZarrNativeApiU8ArrayResult make_u8_error(
    const std::string& error_type,
    const std::string& message) {
    return OmeZarrNativeApiU8ArrayResult{
        0,
        nullptr,
        0,
        nullptr,
        0,
        duplicate_string(error_type),
        duplicate_string(message),
    };
}

std::string exception_type_name(const std::exception& exc) {
    if (dynamic_cast<const std::invalid_argument*>(&exc) != nullptr) {
        return "ValueError";
    }
    if (dynamic_cast<const std::out_of_range*>(&exc) != nullptr) {
        return "IndexError";
    }
    if (dynamic_cast<const std::overflow_error*>(&exc) != nullptr) {
        return "OverflowError";
    }
    if (dynamic_cast<const std::bad_alloc*>(&exc) != nullptr) {
        return "MemoryError";
    }
    return "RuntimeError";
}

OmeZarrNativeApiResult exception_to_error(const std::exception& exc) {
    return make_error(exception_type_name(exc), exc.what());
}

std::int32_t json_i32(const Json& request, const char* key) {
    if (!request.contains(key) || !request.at(key).is_number_integer()) {
        throw std::invalid_argument(std::string("Expected integer field: ") + key);
    }
    const auto value = request.at(key).get<std::int64_t>();
    if (
        value < std::numeric_limits<std::int32_t>::min() ||
        value > std::numeric_limits<std::int32_t>::max()) {
        throw std::overflow_error(std::string("Integer out of int32 range: ") + key);
    }
    return static_cast<std::int32_t>(value);
}

std::uint8_t json_u8(const Json& value, const char* key) {
    if (!value.is_number_integer()) {
        throw std::invalid_argument(std::string("Expected byte value: ") + key);
    }
    const auto parsed = value.get<std::int64_t>();
    if (parsed < 0 || parsed > 255) {
        throw std::overflow_error(std::string("Byte out of range: ") + key);
    }
    return static_cast<std::uint8_t>(parsed);
}

std::string json_string(const Json& request, const char* key) {
    if (!request.contains(key) || !request.at(key).is_string()) {
        throw std::invalid_argument(std::string("Expected string field: ") + key);
    }
    return request.at(key).get<std::string>();
}

std::string json_optional_string(
    const Json& request,
    const char* key,
    const std::string& fallback = "") {
    if (!request.contains(key) || request.at(key).is_null()) {
        return fallback;
    }
    if (!request.at(key).is_string()) {
        throw std::invalid_argument(std::string("Expected string field: ") + key);
    }
    return request.at(key).get<std::string>();
}

bool json_optional_bool(const Json& request, const char* key, bool fallback = false) {
    if (!request.contains(key) || request.at(key).is_null()) {
        return fallback;
    }
    if (!request.at(key).is_boolean()) {
        throw std::invalid_argument(std::string("Expected boolean field: ") + key);
    }
    return request.at(key).get<bool>();
}

std::vector<std::int64_t> json_i64_vector(const Json& request, const char* key) {
    if (!request.contains(key) || !request.at(key).is_array()) {
        throw std::invalid_argument(std::string("Expected integer array field: ") + key);
    }
    std::vector<std::int64_t> values;
    values.reserve(request.at(key).size());
    for (const auto& item : request.at(key)) {
        if (!item.is_number_integer()) {
            throw std::invalid_argument(
                std::string("Expected integer array field: ") + key);
        }
        values.push_back(item.get<std::int64_t>());
    }
    return values;
}

Json rgba_json_from_bytes(const std::array<std::uint8_t, 4>& bytes) {
    return Json::array(
        {bytes[0], bytes[1], bytes[2], bytes[3]});
}

Json csv_value_to_json(const CsvValue& value) {
    return std::visit(
        [](const auto& parsed) -> Json {
            using Value = std::decay_t<decltype(parsed)>;
            if constexpr (std::is_same_v<Value, std::string>) {
                return Json{{"type", "str"}, {"value", parsed}};
            } else if constexpr (std::is_same_v<Value, double>) {
                if (std::isnan(parsed)) {
                    return Json{{"type", "float"}, {"repr", "nan"}};
                }
                if (std::isinf(parsed)) {
                    return Json{
                        {"type", "float"},
                        {"repr", parsed > 0.0 ? "inf" : "-inf"}};
                }
                return Json{{"type", "float"}, {"value", parsed}};
            } else if constexpr (std::is_same_v<Value, std::int64_t>) {
                return Json{{"type", "int"}, {"value", parsed}};
            } else {
                return Json{{"type", "bool"}, {"value", parsed}};
            }
        },
        value);
}

Json project_metadata_json() {
    return Json{
        {"abi_version", ome_zarr_native_api_abi_version()},
        {"project_version", OME_ZARR_C_NATIVE_PROJECT_VERSION},
        {"api_style", "c_abi"},
        {"operations",
         Json::array({
             "api.available_operations",
             "conversions.int_to_rgba",
             "conversions.int_to_rgba_255",
             "conversions.rgba_to_int",
             "csv.parse_csv_value",
             "format.format_from_version",
             "io.local_io_signature",
             "scale.resize_image_shape",
             "scale.scaler_methods",
         })},
        {"buffer_operations", Json::array({"data.rgb_to_5d_u8"})},
    };
}

Json format_from_version_json(const std::string& version) {
    const auto chunk_key_encoding = format_chunk_key_encoding(version);
    return Json{
        {"version", version},
        {"class_name", format_class_name(version)},
        {"zarr_format", format_zarr_format(version)},
        {"chunk_key_encoding",
         Json{
             {"name", chunk_key_encoding.name},
             {"separator", chunk_key_encoding.separator},
         }},
    };
}

Json parse_request_json(const char* request_json) {
    if (request_json == nullptr || std::strlen(request_json) == 0U) {
        return Json::object();
    }
    Json request = Json::parse(request_json);
    if (!request.is_object()) {
        throw std::invalid_argument("Request JSON must be an object");
    }
    return request;
}

OmeZarrNativeApiResult dispatch_call_json(
    const std::string& operation,
    const Json& request) {
    if (operation == "api.available_operations") {
        return make_success_json(project_metadata_json());
    }
    if (operation == "conversions.int_to_rgba") {
        return ome_zarr_native_api_int_to_rgba(json_i32(request, "value"));
    }
    if (operation == "conversions.int_to_rgba_255") {
        return ome_zarr_native_api_int_to_rgba_255(json_i32(request, "value"));
    }
    if (operation == "conversions.rgba_to_int") {
        if (!request.contains("rgba") || !request.at("rgba").is_array() ||
            request.at("rgba").size() != 4U) {
            throw std::invalid_argument("Expected rgba array with four byte values");
        }
        const auto& rgba = request.at("rgba");
        return ome_zarr_native_api_rgba_to_int(
            json_u8(rgba.at(0), "rgba[0]"),
            json_u8(rgba.at(1), "rgba[1]"),
            json_u8(rgba.at(2), "rgba[2]"),
            json_u8(rgba.at(3), "rgba[3]"));
    }
    if (operation == "csv.parse_csv_value") {
        return ome_zarr_native_api_parse_csv_value(
            json_string(request, "value").c_str(),
            json_string(request, "col_type").c_str());
    }
    if (operation == "format.format_from_version") {
        return make_success_json(format_from_version_json(
            json_string(request, "version")));
    }
    if (operation == "io.local_io_signature") {
        const auto signature = local_io_signature(
            json_string(request, "path"),
            json_optional_string(request, "mode", "r"),
            json_optional_string(request, "requested_version", ""),
            json_optional_string(request, "create_subpath", ""),
            json_optional_bool(request, "use_create_subpath", false));
        if (signature.is_none) {
            return make_success_raw_json("null");
        }
        return make_success_raw_json(signature.value.dump());
    }
    if (operation == "scale.scaler_methods") {
        return make_success_json(scaler_methods());
    }
    if (operation == "scale.resize_image_shape") {
        return make_success_json(scaler_resize_image_shape(
            json_i64_vector(request, "shape"),
            json_i32(request, "downscale")));
    }
    throw std::invalid_argument("Unknown operation: " + operation);
}

std::size_t checked_multiply(std::size_t left, std::size_t right) {
    if (left != 0U && right > std::numeric_limits<std::size_t>::max() / left) {
        throw std::overflow_error("array size overflow");
    }
    return left * right;
}

std::size_t element_count(const std::vector<std::size_t>& shape) {
    std::size_t count = 1;
    for (const auto extent : shape) {
        count = checked_multiply(count, extent);
    }
    return count;
}

std::string shape_repr(std::size_t ndim, const std::size_t* shape) {
    std::ostringstream output;
    output << "(";
    for (std::size_t index = 0; index < ndim; ++index) {
        if (index != 0U) {
            output << ", ";
        }
        output << shape[index];
    }
    if (ndim == 1U) {
        output << ",";
    }
    output << ")";
    return output.str();
}

OmeZarrNativeApiU8ArrayResult rgb_to_5d_u8_impl(
    const std::uint8_t* data,
    std::size_t ndim,
    const std::size_t* shape) {
    if (shape == nullptr) {
        return make_u8_error("ValueError", "shape must not be null");
    }
    if (ndim != 2U && ndim != 3U) {
        return make_u8_error(
            "AssertionError",
            "expecting 2 or 3d: (" + shape_repr(ndim, shape) + ")");
    }

    std::vector<std::size_t> input_shape(shape, shape + ndim);
    const std::size_t input_len = element_count(input_shape);
    if (input_len != 0U && data == nullptr) {
        return make_u8_error("ValueError", "data must not be null");
    }

    std::array<std::size_t, 5> output_shape{};
    if (ndim == 2U) {
        output_shape = {1U, 1U, 1U, shape[0], shape[1]};
    } else {
        output_shape = {1U, shape[2], 1U, shape[0], shape[1]};
    }
    const std::size_t output_len = checked_multiply(
        checked_multiply(
            checked_multiply(
                checked_multiply(output_shape[0], output_shape[1]),
                output_shape[2]),
            output_shape[3]),
        output_shape[4]);

    auto output_shape_ptr = std::make_unique<std::size_t[]>(output_shape.size());
    std::copy(
        output_shape.begin(),
        output_shape.end(),
        output_shape_ptr.get());

    std::unique_ptr<std::uint8_t[]> output_data;
    if (output_len != 0U) {
        output_data = std::make_unique<std::uint8_t[]>(output_len);
        if (ndim == 2U) {
            std::copy(data, data + input_len, output_data.get());
        } else {
            const std::size_t height = shape[0];
            const std::size_t width = shape[1];
            const std::size_t channels = shape[2];
            std::size_t output_index = 0;
            for (std::size_t channel = 0; channel < channels; ++channel) {
                for (std::size_t y = 0; y < height; ++y) {
                    for (std::size_t x = 0; x < width; ++x) {
                        const std::size_t input_index =
                            checked_multiply(
                                checked_multiply(y, width) + x,
                                channels) +
                            channel;
                        output_data[output_index] = data[input_index];
                        ++output_index;
                    }
                }
            }
        }
    }

    return OmeZarrNativeApiU8ArrayResult{
        1,
        output_data.release(),
        output_len,
        output_shape_ptr.release(),
        output_shape.size(),
        nullptr,
        nullptr,
    };
}

}  // namespace

extern "C" {

const char* ome_zarr_native_api_abi_version(void) {
    return "1";
}

OmeZarrNativeApiResult ome_zarr_native_api_project_metadata(void) {
    try {
        return make_success_json(project_metadata_json());
    } catch (const std::exception& exc) {
        return exception_to_error(exc);
    } catch (...) {
        return make_error("RuntimeError", "unknown native API error");
    }
}

OmeZarrNativeApiResult ome_zarr_native_api_call_json(
    const char* operation,
    const char* request_json) {
    try {
        if (operation == nullptr) {
            return make_error("ValueError", "operation must not be null");
        }
        return dispatch_call_json(operation, parse_request_json(request_json));
    } catch (const std::exception& exc) {
        return exception_to_error(exc);
    } catch (...) {
        return make_error("RuntimeError", "unknown native API error");
    }
}

OmeZarrNativeApiResult ome_zarr_native_api_int_to_rgba(std::int32_t value) {
    try {
        const auto rgba = int_to_rgba(value);
        return make_success_json(Json::array({rgba[0], rgba[1], rgba[2], rgba[3]}));
    } catch (const std::exception& exc) {
        return exception_to_error(exc);
    } catch (...) {
        return make_error("RuntimeError", "unknown native API error");
    }
}

OmeZarrNativeApiResult ome_zarr_native_api_int_to_rgba_255(std::int32_t value) {
    try {
        return make_success_json(rgba_json_from_bytes(int_to_rgba_255_bytes(value)));
    } catch (const std::exception& exc) {
        return exception_to_error(exc);
    } catch (...) {
        return make_error("RuntimeError", "unknown native API error");
    }
}

OmeZarrNativeApiResult ome_zarr_native_api_rgba_to_int(
    std::uint8_t r,
    std::uint8_t g,
    std::uint8_t b,
    std::uint8_t a) {
    try {
        return make_success_raw_json(std::to_string(rgba_to_int(r, g, b, a)));
    } catch (const std::exception& exc) {
        return exception_to_error(exc);
    } catch (...) {
        return make_error("RuntimeError", "unknown native API error");
    }
}

OmeZarrNativeApiResult ome_zarr_native_api_parse_csv_value(
    const char* value,
    const char* col_type) {
    try {
        if (value == nullptr) {
            return make_error("ValueError", "value must not be null");
        }
        if (col_type == nullptr) {
            return make_error("ValueError", "col_type must not be null");
        }
        return make_success_json(csv_value_to_json(parse_csv_value(value, col_type)));
    } catch (const std::exception& exc) {
        return exception_to_error(exc);
    } catch (...) {
        return make_error("RuntimeError", "unknown native API error");
    }
}

OmeZarrNativeApiU8ArrayResult ome_zarr_native_api_rgb_to_5d_u8(
    const std::uint8_t* data,
    std::size_t ndim,
    const std::size_t* shape) {
    try {
        return rgb_to_5d_u8_impl(data, ndim, shape);
    } catch (const std::exception& exc) {
        return make_u8_error(exception_type_name(exc), exc.what());
    } catch (...) {
        return make_u8_error("RuntimeError", "unknown native API error");
    }
}

void ome_zarr_native_api_free_result(OmeZarrNativeApiResult result) {
    delete[] result.json;
    delete[] result.error_type;
    delete[] result.error_message;
}

void ome_zarr_native_api_free_u8_array_result(
    OmeZarrNativeApiU8ArrayResult result) {
    delete[] result.data;
    delete[] result.shape;
    delete[] result.error_type;
    delete[] result.error_message;
}

}  // extern "C"
