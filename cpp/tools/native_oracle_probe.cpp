#include <cstdint>
#include <cerrno>
#include <cstdlib>
#include <cmath>
#include <exception>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "../../third_party/nlohmann/json.hpp"
#include "../native/conversions.hpp"
#include "../native/csv.hpp"
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
    std::string value;
    std::string col_type;
    std::string parts_json;
    std::string entries_json;
    std::string zarr_id;
    std::string rgba_json;
    bool has_path = false;
    bool has_value = false;
    bool has_col_type = false;
    bool has_parts_json = false;
    bool has_entries_json = false;
    bool has_zarr_id = false;
    bool has_rgba_json = false;
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
        << "  int-to-rgba --value INT32 [--loops N]\n"
        << "  int-to-rgba-255 --value INT32 [--loops N]\n"
        << "  rgba-to-int --rgba-json JSON [--loops N]\n"
        << "  parse-csv-value --value TEXT --col-type TYPE [--loops N]\n"
        << "  dict-to-zarr --entries-json JSON --path PATH --zarr-id ID [--loops N]\n";
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
