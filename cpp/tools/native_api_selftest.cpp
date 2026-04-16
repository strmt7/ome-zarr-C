#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "../../third_party/nlohmann/json.hpp"

#include "../api/ome_zarr_native_api.h"

namespace {

using Json = nlohmann::json;

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

Json require_json(OmeZarrNativeApiResult result, const std::string& message) {
    if (result.ok == 0) {
        const std::string error_type =
            result.error_type == nullptr ? "" : result.error_type;
        const std::string error_message =
            result.error_message == nullptr ? "" : result.error_message;
        ome_zarr_native_api_free_result(result);
        throw std::runtime_error(message + ": " + error_type + ": " + error_message);
    }

    Json parsed = Json::parse(result.json);
    ome_zarr_native_api_free_result(result);
    return parsed;
}

std::string require_error_type(OmeZarrNativeApiU8ArrayResult result) {
    require(result.ok == 0, "expected native API array call to fail");
    const std::string error_type =
        result.error_type == nullptr ? "" : result.error_type;
    ome_zarr_native_api_free_u8_array_result(result);
    return error_type;
}

void test_json_api() {
    const Json metadata = require_json(
        ome_zarr_native_api_project_metadata(),
        "project metadata");
    require(metadata.at("abi_version") == "1", "ABI version");
    require(
        metadata.at("operations").get<std::vector<std::string>>().size() >= 5U,
        "operation list");

    const Json rgba = require_json(
        ome_zarr_native_api_int_to_rgba(255),
        "int_to_rgba");
    require(rgba == Json::array({0.0, 0.0, 0.0, 1.0}), "int_to_rgba payload");

    const Json bytes = require_json(
        ome_zarr_native_api_call_json(
            "conversions.int_to_rgba_255",
            R"({"value":-1})"),
        "call_json int_to_rgba_255");
    require(bytes == Json::array({255, 255, 255, 255}), "rgba byte payload");

    const Json integer = require_json(
        ome_zarr_native_api_rgba_to_int(255, 255, 255, 255),
        "rgba_to_int");
    require(integer == -1, "rgba_to_int signed payload");

    const Json csv_bool = require_json(
        ome_zarr_native_api_parse_csv_value("False", "b"),
        "parse_csv_value bool");
    require(csv_bool.at("type") == "bool", "csv bool type");
    require(csv_bool.at("value") == true, "csv bool truthiness");

    const Json format = require_json(
        ome_zarr_native_api_call_json(
            "format.format_from_version",
            R"({"version":"0.5"})"),
        "format_from_version");
    require(format.at("zarr_format") == 3, "format zarr version");
    require(
        format.at("chunk_key_encoding").at("separator") == "/",
        "format chunk separator");

    const Json methods = require_json(
        ome_zarr_native_api_call_json("scale.scaler_methods", "{}"),
        "scale methods");
    require(methods.is_array(), "scale methods array");
    require(methods.size() >= 5U, "scale method count");

    const Json resized_shape = require_json(
        ome_zarr_native_api_call_json(
            "scale.resize_image_shape",
            R"({"shape":[1,1,1,64,64],"downscale":2})"),
        "scale resize_image_shape");
    require(
        resized_shape == Json::array({1, 1, 1, 32, 32}),
        "scale resized shape");
}

void test_rgb_to_5d_u8() {
    const std::array<std::uint8_t, 6> gray = {0, 1, 2, 3, 4, 5};
    const std::array<std::size_t, 2> gray_shape = {2, 3};
    OmeZarrNativeApiU8ArrayResult gray_result =
        ome_zarr_native_api_rgb_to_5d_u8(
            gray.data(),
            gray_shape.size(),
            gray_shape.data());
    require(gray_result.ok == 1, "2D rgb_to_5d_u8 succeeded");
    require(gray_result.ndim == 5U, "2D output ndim");
    require(gray_result.data_len == gray.size(), "2D output data length");
    require(
        std::vector<std::size_t>(
            gray_result.shape,
            gray_result.shape + gray_result.ndim) ==
            std::vector<std::size_t>({1, 1, 1, 2, 3}),
        "2D output shape");
    require(
        std::vector<std::uint8_t>(
            gray_result.data,
            gray_result.data + gray_result.data_len) ==
            std::vector<std::uint8_t>(gray.begin(), gray.end()),
        "2D output payload");
    ome_zarr_native_api_free_u8_array_result(gray_result);

    const std::array<std::uint8_t, 24> rgb = {
        0,  1,  2,  3,  4,  5,  6,  7,
        8,  9,  10, 11, 12, 13, 14, 15,
        16, 17, 18, 19, 20, 21, 22, 23,
    };
    const std::array<std::size_t, 3> rgb_shape = {2, 4, 3};
    OmeZarrNativeApiU8ArrayResult rgb_result =
        ome_zarr_native_api_rgb_to_5d_u8(
            rgb.data(),
            rgb_shape.size(),
            rgb_shape.data());
    require(rgb_result.ok == 1, "3D rgb_to_5d_u8 succeeded");
    require(
        std::vector<std::size_t>(
            rgb_result.shape,
            rgb_result.shape + rgb_result.ndim) ==
            std::vector<std::size_t>({1, 3, 1, 2, 4}),
        "3D output shape");
    require(
        std::vector<std::uint8_t>(
            rgb_result.data,
            rgb_result.data + rgb_result.data_len) ==
            std::vector<std::uint8_t>({
                0,  3,  6,  9,  12, 15, 18, 21,
                1,  4,  7,  10, 13, 16, 19, 22,
                2,  5,  8,  11, 14, 17, 20, 23,
            }),
        "3D output payload");
    ome_zarr_native_api_free_u8_array_result(rgb_result);

    const std::array<std::size_t, 4> invalid_shape = {1, 2, 3, 4};
    require(
        require_error_type(ome_zarr_native_api_rgb_to_5d_u8(
            rgb.data(),
            invalid_shape.size(),
            invalid_shape.data())) == "AssertionError",
        "invalid dimension error type");
}

}  // namespace

int main() {
    test_json_api();
    test_rgb_to_5d_u8();
    std::cout << "[PASS] native C ABI self-test\n";
    return 0;
}
