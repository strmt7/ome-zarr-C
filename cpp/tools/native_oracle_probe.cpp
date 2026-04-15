#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "../../third_party/nlohmann/json.hpp"
#include "../native/local_runtime.hpp"
#include "../native/utils.hpp"

namespace {

using json = nlohmann::json;
using namespace ome_zarr_c::native_code;

struct ExitError final : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

struct Options {
    std::string command;
    std::string path;
    std::string parts_json;
    bool has_path = false;
    bool has_parts_json = false;
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
        << "  find-multiscales --path PATH [--loops N]\n";
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
        if (arg == "--parts-json") {
            if (index + 1 >= argc) {
                throw ExitError("Missing value after --parts-json");
            }
            options.parts_json = argv[++index];
            options.has_parts_json = true;
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
