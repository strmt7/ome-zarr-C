#include <cstdint>
#include <cerrno>
#include <cstdlib>
#include <cmath>
#include <exception>
#include <functional>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

#include "../../third_party/nlohmann/json.hpp"
#include "../native/axes.hpp"
#include "../native/data.hpp"
#include "../native/conversions.hpp"
#include "../native/csv.hpp"
#include "../native/io.hpp"
#include "../native/local_runtime.hpp"
#include "../native/utils.hpp"

namespace {

using json = nlohmann::ordered_json;
using namespace ome_zarr_c::native_code;

struct ExitError final : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

struct Options {
    std::string command;
    std::string path;
    std::string mode = "r";
    std::string value;
    std::string col_type;
    std::string parts_json;
    std::string entries_json;
    std::string zarr_id;
    std::string rgba_json;
    std::string dtype;
    std::string shape_json;
    std::string values_json;
    std::string target_shape_json;
    std::string circle_shape_json;
    std::string offset_json;
    std::string axes_json;
    std::string format_version;
    std::string create_subpath;
    std::string cases_json;
    bool has_path = false;
    bool has_value = false;
    bool has_col_type = false;
    bool has_parts_json = false;
    bool has_entries_json = false;
    bool has_zarr_id = false;
    bool has_rgba_json = false;
    bool has_dtype = false;
    bool has_shape_json = false;
    bool has_values_json = false;
    bool has_target_shape_json = false;
    bool has_circle_shape_json = false;
    bool has_offset_json = false;
    bool has_axes_json = false;
    bool has_format_version = false;
    bool has_create_subpath = false;
    bool has_cases_json = false;
    std::size_t loops = 1;
};

std::size_t parse_positive_integer(const char* text, const char* flag_name) {
    char* end = nullptr;
    const unsigned long long parsed = std::strtoull(text, &end, 10);
    if (end == text || *end != '\0' || parsed == 0ULL) {
        throw ExitError(
            std::string("Invalid positive integer for ") + flag_name + ": " + text);
    }
    return static_cast<std::size_t>(parsed);
}

[[noreturn]] void print_usage_and_exit(const int code) {
    std::ostream& stream = code == 0 ? std::cout : std::cerr;
    stream
        << "Usage: ome_zarr_native_probe <command> [options]\n"
        << "\n"
        << "Commands:\n"
        << "  splitall --path PATH [--loops N]\n"
        << "  strip-common-prefix --parts-json JSON [--loops N]\n"
        << "  find-multiscales --path PATH [--loops N]\n"
        << "  io-signature --path PATH [--mode MODE] [--format-version VERSION] "
           "[--create-subpath SUBPATH] [--loops N]\n"
        << "  int-to-rgba --value INT32 [--loops N]\n"
        << "  int-to-rgba-255 --value INT32 [--loops N]\n"
        << "  rgba-to-int --rgba-json JSON [--loops N]\n"
        << "  parse-csv-value --value TEXT --col-type TYPE [--loops N]\n"
        << "  dict-to-zarr --entries-json JSON --path PATH --zarr-id ID [--loops N]\n"
        << "  axes --axes-json JSON --format-version VERSION [--loops N]\n"
        << "  axes-batch --cases-json JSON\n"
        << "  make-circle --target-shape-json JSON --circle-shape-json JSON "
           "--offset-json JSON --value NUMBER --dtype DTYPE [--loops N]\n"
        << "  rgb-to-5d --shape-json JSON --values-json JSON --dtype DTYPE "
           "[--loops N]\n";
    std::exit(code);
}

Options parse_options(const int argc, char** argv) {
    if (argc < 2) {
        print_usage_and_exit(1);
    }
    if (std::string_view(argv[1]) == "--help" || std::string_view(argv[1]) == "-h") {
        print_usage_and_exit(0);
    }
    Options options{};
    options.command = argv[1];
    for (int index = 2; index < argc; ++index) {
        const std::string_view arg(argv[index]);
        if (arg == "--path") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --path");
            }
            options.path = argv[++index];
            options.has_path = true;
            continue;
        }
        if (arg == "--value") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --value");
            }
            options.value = argv[++index];
            options.has_value = true;
            continue;
        }
        if (arg == "--mode") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --mode");
            }
            options.mode = argv[++index];
            continue;
        }
        if (arg == "--col-type") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --col-type");
            }
            options.col_type = argv[++index];
            options.has_col_type = true;
            continue;
        }
        if (arg == "--parts-json") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --parts-json");
            }
            options.parts_json = argv[++index];
            options.has_parts_json = true;
            continue;
        }
        if (arg == "--rgba-json") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --rgba-json");
            }
            options.rgba_json = argv[++index];
            options.has_rgba_json = true;
            continue;
        }
        if (arg == "--entries-json") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --entries-json");
            }
            options.entries_json = argv[++index];
            options.has_entries_json = true;
            continue;
        }
        if (arg == "--zarr-id") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --zarr-id");
            }
            options.zarr_id = argv[++index];
            options.has_zarr_id = true;
            continue;
        }
        if (arg == "--loops") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --loops");
            }
            options.loops = parse_positive_integer(argv[++index], "--loops");
            continue;
        }
        if (arg == "--dtype") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --dtype");
            }
            options.dtype = argv[++index];
            options.has_dtype = true;
            continue;
        }
        if (arg == "--shape-json") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --shape-json");
            }
            options.shape_json = argv[++index];
            options.has_shape_json = true;
            continue;
        }
        if (arg == "--values-json") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --values-json");
            }
            options.values_json = argv[++index];
            options.has_values_json = true;
            continue;
        }
        if (arg == "--target-shape-json") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --target-shape-json");
            }
            options.target_shape_json = argv[++index];
            options.has_target_shape_json = true;
            continue;
        }
        if (arg == "--circle-shape-json") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --circle-shape-json");
            }
            options.circle_shape_json = argv[++index];
            options.has_circle_shape_json = true;
            continue;
        }
        if (arg == "--offset-json") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --offset-json");
            }
            options.offset_json = argv[++index];
            options.has_offset_json = true;
            continue;
        }
        if (arg == "--axes-json") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --axes-json");
            }
            options.axes_json = argv[++index];
            options.has_axes_json = true;
            continue;
        }
        if (arg == "--format-version") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --format-version");
            }
            options.format_version = argv[++index];
            options.has_format_version = true;
            continue;
        }
        if (arg == "--create-subpath") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --create-subpath");
            }
            options.create_subpath = argv[++index];
            options.has_create_subpath = true;
            continue;
        }
        if (arg == "--cases-json") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --cases-json");
            }
            options.cases_json = argv[++index];
            options.has_cases_json = true;
            continue;
        }
        if (arg == "--help" || arg == "-h") {
            print_usage_and_exit(0);
        }
        throw ExitError("Unknown argument: " + std::string(arg));
    }
    return options;
}

json make_ok_payload(
    const json& value,
    const std::string& stdout_text = "",
    json records = nullptr,
    json payload = nullptr) {
    json result = json{
        {"status", "ok"},
        {"value", value},
        {"stdout", stdout_text},
    };
    if (!records.is_null()) {
        result["records"] = std::move(records);
    }
    if (!payload.is_null()) {
        result["payload"] = std::move(payload);
    }
    return result;
}

json make_error_payload(
    std::string_view error_type,
    std::string_view error_message,
    const std::string& stdout_text = "",
    json records = nullptr) {
    json result = json{
        {"status", "err"},
        {"error_type", error_type},
        {"error_message", error_message},
        {"stdout", stdout_text},
    };
    if (!records.is_null()) {
        result["records"] = std::move(records);
    }
    return result;
}

json encode_csv_value(const CsvValue& value) {
    if (const auto* text = std::get_if<std::string>(&value)) {
        return json{{"kind", "string"}, {"value", *text}};
    }
    if (const auto* number = std::get_if<double>(&value)) {
        if (std::isnan(*number)) {
            return json{{"kind", "float"}, {"repr", "nan"}};
        }
        if (std::isinf(*number)) {
            return json{
                {"kind", "float"},
                {"repr", *number < 0.0 ? "-inf" : "inf"},
            };
        }
        return json{{"kind", "float"}, {"value", *number}};
    }
    if (const auto* integer = std::get_if<std::int64_t>(&value)) {
        return json{{"kind", "int"}, {"value", *integer}};
    }
    return json{{"kind", "bool"}, {"value", std::get<bool>(value)}};
}

std::string python_string_repr(std::string_view value) {
    std::string rendered;
    rendered.reserve(value.size() + 2U);
    rendered.push_back('\'');
    for (const char ch : value) {
        if (ch == '\\' || ch == '\'') {
            rendered.push_back('\\');
        }
        rendered.push_back(ch);
    }
    rendered.push_back('\'');
    return rendered;
}

std::string python_dict_repr(const json& object) {
    if (!object.is_object()) {
        throw ExitError("Expected JSON object when building python-style repr");
    }
    std::string rendered = "{";
    bool first = true;
    for (const auto& item : object.items()) {
        if (!first) {
            rendered += ", ";
        }
        first = false;
        rendered += python_string_repr(item.key());
        rendered += ": ";
        if (item.value().is_string()) {
            rendered += python_string_repr(item.value().get_ref<const std::string&>());
        } else if (item.value().is_null()) {
            rendered += "None";
        } else if (item.value().is_boolean()) {
            rendered += item.value().get<bool>() ? "True" : "False";
        } else {
            rendered += item.value().dump();
        }
    }
    rendered += "}";
    return rendered;
}

std::vector<AxisRecord> parse_axis_records(const json& parsed) {
    if (!parsed.is_array()) {
        throw ExitError("--axes-json must decode to null or a JSON array");
    }
    std::vector<AxisRecord> records;
    records.reserve(parsed.size());
    for (const auto& item : parsed) {
        if (item.is_string()) {
            records.push_back(AxisRecord{
                true,
                item.get<std::string>(),
                false,
                "",
                "",
                "None",
            });
            continue;
        }
        if (!item.is_object()) {
            throw ExitError("--axes-json entries must be strings or objects");
        }
        AxisRecord record{};
        record.has_name = false;
        record.name.clear();
        record.has_type = false;
        record.type.clear();
        record.axis_repr = python_dict_repr(item);
        record.type_repr = "None";
        if (const auto name_it = item.find("name"); name_it != item.end()) {
            if (!name_it->is_string()) {
                throw ExitError("--axes-json object 'name' values must be strings");
            }
            record.has_name = true;
            record.name = name_it->get<std::string>();
        }
        if (const auto type_it = item.find("type"); type_it != item.end()) {
            if (!type_it->is_string()) {
                throw ExitError("--axes-json object 'type' values must be strings");
            }
            record.has_type = true;
            record.type = type_it->get<std::string>();
            record.type_repr = python_string_repr(record.type);
        }
        records.push_back(std::move(record));
    }
    return records;
}

json encode_axis_records(const std::vector<AxisRecord>& records) {
    json payload = json::array();
    for (const auto& record : records) {
        json axis = json::object();
        if (record.has_name) {
            axis["name"] = record.name;
        }
        if (record.has_type) {
            axis["type"] = record.type;
        }
        payload.push_back(std::move(axis));
    }
    return payload;
}

json run_axes_payload(const json& parsed, const std::string& version) {

    bool has_axes_attr = false;
    std::vector<AxisRecord> axes;
    if (!parsed.is_null()) {
        has_axes_attr = true;
        axes = axes_to_dicts(parse_axis_records(parsed));
    } else if (version == "0.1" || version == "0.2") {
        has_axes_attr = true;
        axes = axes_to_dicts({
            AxisRecord{true, "t", false, "", "", "None"},
            AxisRecord{true, "c", false, "", "", "None"},
            AxisRecord{true, "z", false, "", "", "None"},
            AxisRecord{true, "y", false, "", "", "None"},
            AxisRecord{true, "x", false, "", "", "None"},
        });
    } else {
        return make_error_payload(
            "AttributeError",
            "'Axes' object has no attribute 'axes'");
    }

    try {
        if (version == "0.3") {
            validate_03(get_names(axes));
        } else if (version != "0.1" && version != "0.2") {
            validate_axes_types(axes);
        }
    } catch (const std::invalid_argument& exc) {
        return make_error_payload("ValueError", exc.what());
    }

    const auto names = get_names(axes);
    json payload = json::object();
    payload["axes"] = has_axes_attr ? encode_axis_records(axes) : json(nullptr);
    payload["to_list_03"] = names;
    payload["to_list_04"] = encode_axis_records(axes);
    return make_ok_payload(payload);
}

json run_axes(const Options& options) {
    if (!options.has_axes_json) {
        throw ExitError("axes requires --axes-json");
    }
    if (!options.has_format_version) {
        throw ExitError("axes requires --format-version");
    }

    return run_axes_payload(json::parse(options.axes_json), options.format_version);
}

json run_axes_batch(const Options& options) {
    if (!options.has_cases_json) {
        throw ExitError("axes-batch requires --cases-json");
    }
    const json parsed = json::parse(options.cases_json);
    if (!parsed.is_array()) {
        throw ExitError("--cases-json must decode to a JSON array");
    }

    json results = json::array();
    for (const auto& item : parsed) {
        if (!item.is_object()) {
            throw ExitError("--cases-json entries must be JSON objects");
        }
        const auto axes_it = item.find("axes");
        const auto version_it = item.find("format_version");
        if (axes_it == item.end()) {
            throw ExitError("--cases-json entries require an 'axes' key");
        }
        if (version_it == item.end() || !version_it->is_string()) {
            throw ExitError(
                "--cases-json entries require string 'format_version' values");
        }
        results.push_back(run_axes_payload(*axes_it, version_it->get<std::string>()));
    }
    return make_ok_payload(results);
}

std::vector<std::size_t> parse_shape_vector(
    const std::string& encoded,
    const char* flag_name) {
    const json parsed = json::parse(encoded);
    if (!parsed.is_array()) {
        throw ExitError(std::string(flag_name) + " must decode to a JSON array");
    }
    std::vector<std::size_t> shape;
    shape.reserve(parsed.size());
    for (const auto& item : parsed) {
        if (!item.is_number_integer() && !item.is_number_unsigned()) {
            throw ExitError(std::string(flag_name) + " entries must be integers");
        }
        const auto value = item.get<long long>();
        if (value < 0) {
            throw ExitError(std::string(flag_name) + " entries must be >= 0");
        }
        shape.push_back(static_cast<std::size_t>(value));
    }
    return shape;
}

std::size_t shape_product(const std::vector<std::size_t>& shape) {
    std::size_t total = 1U;
    for (const auto dim : shape) {
        total *= dim;
    }
    return total;
}

std::string python_tuple_repr(const std::vector<std::size_t>& values) {
    std::string rendered = "(";
    for (std::size_t index = 0; index < values.size(); ++index) {
        if (index != 0U) {
            rendered += ", ";
        }
        rendered += std::to_string(values[index]);
    }
    if (values.size() == 1U) {
        rendered += ",";
    }
    rendered += ")";
    return rendered;
}

json parse_flat_values_json(
    const std::string& encoded,
    const std::vector<std::size_t>& shape) {
    const json parsed = json::parse(encoded);
    if (!parsed.is_array()) {
        throw ExitError("--values-json must decode to a JSON array");
    }
    if (parsed.size() != shape_product(shape)) {
        throw ExitError("--values-json length does not match --shape-json");
    }
    return parsed;
}

template <typename T>
T parse_typed_scalar(const std::string& text, const char* dtype_name) {
    if constexpr (std::is_floating_point_v<T>) {
        char* end = nullptr;
        errno = 0;
        const double value = std::strtod(text.c_str(), &end);
        if (end == text.c_str() || *end != '\0' || errno == ERANGE) {
            throw ExitError(std::string("Invalid value for dtype ") + dtype_name + ": " + text);
        }
        return static_cast<T>(value);
    } else if constexpr (std::is_signed_v<T>) {
        char* end = nullptr;
        errno = 0;
        const long long raw = std::strtoll(text.c_str(), &end, 10);
        if (end == text.c_str() || *end != '\0' || errno == ERANGE ||
            raw < static_cast<long long>(std::numeric_limits<T>::min()) ||
            raw > static_cast<long long>(std::numeric_limits<T>::max())) {
            throw ExitError(std::string("Invalid value for dtype ") + dtype_name + ": " + text);
        }
        return static_cast<T>(raw);
    } else {
        char* end = nullptr;
        errno = 0;
        const unsigned long long raw = std::strtoull(text.c_str(), &end, 10);
        if (end == text.c_str() || *end != '\0' || errno == ERANGE ||
            raw > static_cast<unsigned long long>(std::numeric_limits<T>::max())) {
            throw ExitError(std::string("Invalid value for dtype ") + dtype_name + ": " + text);
        }
        return static_cast<T>(raw);
    }
}

template <typename T>
json encode_flat_array(
    const std::vector<T>& values,
    const std::vector<std::size_t>& shape,
    const std::string& dtype) {
    json flat = json::array();
    for (const auto& value : values) {
        flat.push_back(value);
    }
    return json{
        {"shape", shape},
        {"dtype", dtype},
        {"values", std::move(flat)},
    };
}

template <typename T>
json run_make_circle_typed(const Options& options) {
    const auto target_shape = parse_shape_vector(options.target_shape_json, "--target-shape-json");
    const auto circle_shape = parse_shape_vector(options.circle_shape_json, "--circle-shape-json");
    const auto offset = parse_shape_vector(options.offset_json, "--offset-json");
    if (target_shape.size() != 2U || circle_shape.size() != 2U || offset.size() != 2U) {
        throw ExitError("make-circle shape arguments must all be 2D");
    }
    const auto target_height = target_shape[0];
    const auto target_width = target_shape[1];
    const auto circle_height = circle_shape[0];
    const auto circle_width = circle_shape[1];
    const auto offset_y = offset[0];
    const auto offset_x = offset[1];
    if (offset_y + circle_height > target_height || offset_x + circle_width > target_width) {
        throw ExitError("make-circle slice exceeds target shape");
    }
    const T typed_value = parse_typed_scalar<T>(options.value, options.dtype.c_str());
    std::vector<T> target(target_height * target_width, T{});
    for (std::size_t iteration = 0; iteration < options.loops; ++iteration) {
        std::fill(target.begin(), target.end(), T{});
        const auto points = circle_points(circle_height, circle_width);
        for (const auto& point : points) {
            const auto y = offset_y + point.y;
            const auto x = offset_x + point.x;
            target[y * target_width + x] = typed_value;
        }
    }
    return make_ok_payload(encode_flat_array(target, target_shape, options.dtype));
}

json run_make_circle(const Options& options) {
    if (!options.has_target_shape_json) {
        throw ExitError("make-circle requires --target-shape-json");
    }
    if (!options.has_circle_shape_json) {
        throw ExitError("make-circle requires --circle-shape-json");
    }
    if (!options.has_offset_json) {
        throw ExitError("make-circle requires --offset-json");
    }
    if (!options.has_value) {
        throw ExitError("make-circle requires --value");
    }
    if (!options.has_dtype) {
        throw ExitError("make-circle requires --dtype");
    }

    if (options.dtype == "float64") {
        return run_make_circle_typed<double>(options);
    }
    if (options.dtype == "float32") {
        return run_make_circle_typed<float>(options);
    }
    if (options.dtype == "int16") {
        return run_make_circle_typed<std::int16_t>(options);
    }
    if (options.dtype == "int32") {
        return run_make_circle_typed<std::int32_t>(options);
    }
    if (options.dtype == "int64") {
        return run_make_circle_typed<std::int64_t>(options);
    }
    if (options.dtype == "uint8") {
        return run_make_circle_typed<std::uint8_t>(options);
    }
    if (options.dtype == "uint16") {
        return run_make_circle_typed<std::uint16_t>(options);
    }
    if (options.dtype == "uint32") {
        return run_make_circle_typed<std::uint32_t>(options);
    }
    if (options.dtype == "uint64") {
        return run_make_circle_typed<std::uint64_t>(options);
    }
    throw ExitError("Unsupported dtype for make-circle: " + options.dtype);
}

json encode_rgb_to_5d_payload(
    const json& values,
    const std::vector<std::size_t>& input_shape,
    const std::string& dtype) {
    if (input_shape.size() == 2U) {
        return json{
            {"shape", std::vector<std::size_t>{1U, 1U, 1U, input_shape[0], input_shape[1]}},
            {"dtype", dtype},
            {"values", values},
        };
    }

    const auto height = input_shape[0];
    const auto width = input_shape[1];
    const auto channels = input_shape[2];
    json reordered = json::array();
    for (std::size_t channel = 0; channel < channels; ++channel) {
        for (std::size_t y = 0; y < height; ++y) {
            for (std::size_t x = 0; x < width; ++x) {
                const auto source_index = (y * width * channels) + (x * channels) + channel;
                reordered.push_back(values[source_index]);
            }
        }
    }
    return json{
        {"shape", std::vector<std::size_t>{1U, channels, 1U, height, width}},
        {"dtype", dtype},
        {"values", std::move(reordered)},
    };
}

json run_rgb_to_5d(const Options& options) {
    if (!options.has_shape_json) {
        throw ExitError("rgb-to-5d requires --shape-json");
    }
    if (!options.has_values_json) {
        throw ExitError("rgb-to-5d requires --values-json");
    }
    if (!options.has_dtype) {
        throw ExitError("rgb-to-5d requires --dtype");
    }

    const auto shape = parse_shape_vector(options.shape_json, "--shape-json");
    const auto values = parse_flat_values_json(options.values_json, shape);
    if (shape.size() != 2U && shape.size() != 3U) {
        return make_error_payload(
            "AssertionError",
            "expecting 2 or 3d: (" + python_tuple_repr(shape) + ")");
    }

    json payload;
    for (std::size_t iteration = 0; iteration < options.loops; ++iteration) {
        payload = encode_rgb_to_5d_payload(values, shape, options.dtype);
    }
    return make_ok_payload(payload);
}

std::string python_like_parts_repr(
    const std::vector<std::vector<std::string>>& parts) {
    std::string rendered;
    bool first_row = true;
    for (const auto& row : parts) {
        if (!first_row) {
            rendered += "\n";
        }
        first_row = false;
        rendered += "[";
        for (std::size_t index = 0; index < row.size(); ++index) {
            if (index != 0U) {
                rendered += ", ";
            }
            rendered += "'";
            rendered += row[index];
            rendered += "'";
        }
        rendered += "]";
    }
    return rendered;
}

json run_splitall(const Options& options) {
    if (!options.has_path) {
        throw ExitError("splitall requires --path");
    }
    std::vector<std::string> result;
    for (std::size_t iteration = 0; iteration < options.loops; ++iteration) {
        result = splitall(options.path);
    }
    return make_ok_payload(result);
}

json run_strip_common_prefix(const Options& options) {
    if (!options.has_parts_json) {
        throw ExitError("strip-common-prefix requires --parts-json");
    }

    const json parsed = json::parse(options.parts_json);
    if (!parsed.is_array()) {
        throw ExitError("--parts-json must decode to a JSON array");
    }

    std::vector<std::vector<std::string>> original_parts;
    original_parts.reserve(parsed.size());
    for (const auto& row : parsed) {
        if (!row.is_array()) {
            throw ExitError("--parts-json rows must be JSON arrays");
        }
        std::vector<std::string> converted_row;
        converted_row.reserve(row.size());
        for (const auto& item : row) {
            if (!item.is_string()) {
                throw ExitError("--parts-json values must be strings");
            }
            converted_row.push_back(item.get<std::string>());
        }
        original_parts.push_back(std::move(converted_row));
    }

    std::string result;
    try {
        for (std::size_t iteration = 0; iteration < options.loops; ++iteration) {
            auto parts = original_parts;
            result = strip_common_prefix(parts);
            if (iteration + 1 == options.loops) {
                return make_ok_payload(result, "", nullptr, parts);
            }
        }
    } catch (const std::runtime_error&) {
        return make_error_payload(
            "Exception",
            "No common prefix:\n" + python_like_parts_repr(original_parts) + "\n");
    }
    return make_ok_payload(result);
}

json run_find_multiscales(const Options& options) {
    if (!options.has_path) {
        throw ExitError("find-multiscales requires --path");
    }

    LocalFindMultiscalesResult result{};
    for (std::size_t iteration = 0; iteration < options.loops; ++iteration) {
        result = local_find_multiscales(options.path);
    }

    std::string stdout_text;
    if (result.metadata_missing) {
        stdout_text = utils_missing_metadata_message();
        stdout_text.push_back('\n');
    }
    for (const auto& line : result.printed_messages) {
        stdout_text += line;
        stdout_text.push_back('\n');
    }

    json records = json::array();
    if (result.logged_no_wells) {
        records.push_back(
            json::array({20, "No wells found in plate" + result.logged_no_wells_path}));
    }

    json value = json::array();
    for (const auto& image : result.images) {
        value.push_back(json::array({image.path, image.name, image.dirname}));
    }
    return make_ok_payload(value, stdout_text, records);
}

json run_io_signature(const Options& options) {
    if (!options.has_path) {
        throw ExitError("io-signature requires --path");
    }
    const std::string requested_version =
        options.has_format_version ? options.format_version : "0.5";
    LocalIoSignature signature{};
    for (std::size_t iteration = 0; iteration < options.loops; ++iteration) {
        signature = local_io_signature(
            options.path,
            options.mode,
            requested_version,
            options.create_subpath,
            options.has_create_subpath);
    }
    if (signature.is_none) {
        return make_ok_payload(nullptr);
    }
    return make_ok_payload(signature.value);
}

std::int32_t parse_int32_value(const std::string& text) {
    char* end = nullptr;
    errno = 0;
    const long long raw = std::strtoll(text.c_str(), &end, 10);
    if (end == text.c_str() || *end != '\0' || errno == ERANGE ||
        raw < static_cast<long long>(std::numeric_limits<std::int32_t>::min()) ||
        raw > static_cast<long long>(std::numeric_limits<std::int32_t>::max())) {
        throw ExitError("Invalid int32 value: " + text);
    }
    return static_cast<std::int32_t>(raw);
}

json run_int_to_rgba(const Options& options) {
    if (!options.has_value) {
        throw ExitError("int-to-rgba requires --value");
    }
    std::array<double, 4> rgba{};
    const auto value = parse_int32_value(options.value);
    for (std::size_t iteration = 0; iteration < options.loops; ++iteration) {
        rgba = int_to_rgba(value);
    }
    return make_ok_payload(json::array({rgba[0], rgba[1], rgba[2], rgba[3]}));
}

json run_int_to_rgba_255(const Options& options) {
    if (!options.has_value) {
        throw ExitError("int-to-rgba-255 requires --value");
    }
    std::array<std::uint8_t, 4> rgba{};
    const auto value = parse_int32_value(options.value);
    for (std::size_t iteration = 0; iteration < options.loops; ++iteration) {
        rgba = int_to_rgba_255_bytes(value);
    }
    return make_ok_payload(json::array({rgba[0], rgba[1], rgba[2], rgba[3]}));
}

json run_rgba_to_int(const Options& options) {
    if (!options.has_rgba_json) {
        throw ExitError("rgba-to-int requires --rgba-json");
    }
    const json parsed = json::parse(options.rgba_json);
    if (!parsed.is_array() || parsed.size() != 4U) {
        throw ExitError("--rgba-json must decode to a JSON array of length 4");
    }
    std::array<std::uint8_t, 4> rgba{};
    for (std::size_t index = 0; index < 4U; ++index) {
        if (!parsed[index].is_number_integer() && !parsed[index].is_number_unsigned()) {
            throw ExitError("--rgba-json entries must be integers");
        }
        const auto value = parsed[index].get<int>();
        if (value < 0 || value > 255) {
            throw ExitError("--rgba-json entries must be in [0, 255]");
        }
        rgba[index] = static_cast<std::uint8_t>(value);
    }

    std::int32_t value = 0;
    for (std::size_t iteration = 0; iteration < options.loops; ++iteration) {
        value = rgba_to_int(rgba[0], rgba[1], rgba[2], rgba[3]);
    }
    return make_ok_payload(value);
}

json run_parse_csv_value(const Options& options) {
    if (!options.has_value) {
        throw ExitError("parse-csv-value requires --value");
    }
    if (!options.has_col_type) {
        throw ExitError("parse-csv-value requires --col-type");
    }

    CsvValue value{};
    try {
        for (std::size_t iteration = 0; iteration < options.loops; ++iteration) {
            value = parse_csv_value(options.value, options.col_type);
        }
    } catch (const std::overflow_error& exc) {
        return make_error_payload("OverflowError", exc.what());
    }
    return make_ok_payload(encode_csv_value(value));
}

json run_dict_to_zarr(const Options& options) {
    if (!options.has_entries_json) {
        throw ExitError("dict-to-zarr requires --entries-json");
    }
    if (!options.has_path) {
        throw ExitError("dict-to-zarr requires --path");
    }
    if (!options.has_zarr_id) {
        throw ExitError("dict-to-zarr requires --zarr-id");
    }

    const json parsed = json::parse(options.entries_json);
    if (!parsed.is_array()) {
        throw ExitError("--entries-json must decode to a JSON array");
    }

    std::vector<LocalDictToZarrEntry> entries;
    entries.reserve(parsed.size());
    for (const auto& item : parsed) {
        if (!item.is_object()) {
            throw ExitError("--entries-json items must be JSON objects");
        }
        const auto key_is_string_iter = item.find("key_is_string");
        const auto key_text_iter = item.find("key_text");
        const auto values_iter = item.find("values");
        if (key_is_string_iter == item.end() || !key_is_string_iter->is_boolean()) {
            throw ExitError("--entries-json items require boolean key_is_string");
        }
        if (key_text_iter == item.end() || !key_text_iter->is_string()) {
            throw ExitError("--entries-json items require string key_text");
        }
        if (values_iter == item.end() || !values_iter->is_object()) {
            throw ExitError("--entries-json items require object values");
        }
        entries.push_back(LocalDictToZarrEntry{
            key_is_string_iter->get<bool>(),
            key_text_iter->get<std::string>(),
            *values_iter,
        });
    }

    try {
        LocalDictToZarrResult result{};
        for (std::size_t iteration = 0; iteration < options.loops; ++iteration) {
            result = local_dict_to_zarr(entries, options.path, options.zarr_id);
        }
        return make_ok_payload(json{
            {"touched_label_groups", result.touched_label_groups},
            {"updated_properties", result.updated_properties},
        });
    } catch (const std::runtime_error& exc) {
        return make_error_payload("Exception", exc.what());
    }
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const Options options = parse_options(argc, argv);
        json payload;
        if (options.command == "splitall") {
            payload = run_splitall(options);
        } else if (options.command == "strip-common-prefix") {
            payload = run_strip_common_prefix(options);
        } else if (options.command == "find-multiscales") {
            payload = run_find_multiscales(options);
        } else if (options.command == "io-signature") {
            payload = run_io_signature(options);
        } else if (options.command == "int-to-rgba") {
            payload = run_int_to_rgba(options);
        } else if (options.command == "int-to-rgba-255") {
            payload = run_int_to_rgba_255(options);
        } else if (options.command == "rgba-to-int") {
            payload = run_rgba_to_int(options);
        } else if (options.command == "parse-csv-value") {
            payload = run_parse_csv_value(options);
        } else if (options.command == "dict-to-zarr") {
            payload = run_dict_to_zarr(options);
        } else if (options.command == "axes") {
            payload = run_axes(options);
        } else if (options.command == "axes-batch") {
            payload = run_axes_batch(options);
        } else if (options.command == "make-circle") {
            payload = run_make_circle(options);
        } else if (options.command == "rgb-to-5d") {
            payload = run_rgb_to_5d(options);
        } else {
            throw ExitError("Unknown command: " + options.command);
        }
        std::cout << payload.dump();
        std::cout << "\n";
        return 0;
    } catch (const ExitError& exc) {
        std::cerr << exc.what() << "\n";
        return 2;
    } catch (const std::exception& exc) {
        std::cerr << exc.what() << "\n";
        return 1;
    }
}
