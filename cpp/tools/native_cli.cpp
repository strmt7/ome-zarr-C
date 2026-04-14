#include <cstdint>
#include <exception>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "../native/cli.hpp"
#include "../native/format.hpp"

namespace {

using namespace ome_zarr_c::native_code;

struct ExitError final : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

std::vector<std::string> split_csv(std::string_view text) {
    std::vector<std::string> values;
    std::string current;
    for (const char ch : text) {
        if (ch == ',') {
            values.push_back(current);
            current.clear();
            continue;
        }
        current.push_back(ch);
    }
    values.push_back(current);
    return values;
}

std::string join_strings(
    const std::vector<std::string>& values,
    std::string_view separator = ",") {
    std::ostringstream output;
    for (std::size_t index = 0; index < values.size(); ++index) {
        if (index > 0) {
            output << separator;
        }
        output << values[index];
    }
    return output.str();
}

std::vector<std::string> rest_args(int argc, char** argv, int start_index) {
    std::vector<std::string> args;
    for (int index = start_index; index < argc; ++index) {
        args.emplace_back(argv[index]);
    }
    return args;
}

std::string require_option_value(
    const std::vector<std::string>& args,
    std::size_t& index,
    std::string_view option_name) {
    if (index + 1 >= args.size()) {
        throw ExitError("Missing value after " + std::string(option_name));
    }
    return args[++index];
}

std::int64_t parse_int64(const std::string& text, std::string_view option_name) {
    std::size_t consumed = 0;
    const auto value = std::stoll(text, &consumed, 10);
    if (consumed != text.size()) {
        throw ExitError("Invalid integer for " + std::string(option_name) + ": " + text);
    }
    return value;
}

[[noreturn]] void print_usage_and_exit(int code) {
    std::ostream& stream = code == 0 ? std::cout : std::cerr;
    stream
        << "Usage: ome_zarr_native_cli <group> <command> [options]\n"
        << "\n"
        << "Groups and commands:\n"
        << "  cli create-plan --method <coins|astronaut>\n"
        << "  cli scale-factors --downscale <int> --max-layer <int>\n"
        << "  format detect [--empty] [--multiscales-version V] [--plate-version V]\n"
        << "  format matches --version V [--empty] [--multiscales-version V] [--plate-version V]\n"
        << "  format zarr-format --version V\n"
        << "  format chunk-key-encoding --version V\n"
        << "  format class-name --version V\n"
        << "  format generate-well --path ROW/COL --rows A,B,... --columns 1,2,...\n"
        << "  format validate-well --path ROW/COL --row-index N --column-index N"
           " --rows A,B,... --columns 1,2,...\n";
    std::exit(code);
}

MetadataSummary parse_metadata_summary(const std::vector<std::string>& args) {
    MetadataSummary metadata{};
    for (std::size_t index = 0; index < args.size(); ++index) {
        const auto& arg = args[index];
        if (arg == "--empty") {
            metadata.is_empty = true;
            continue;
        }
        if (arg == "--multiscales-version") {
            metadata.has_multiscales_version = true;
            metadata.multiscales_version_is_string = true;
            metadata.multiscales_version =
                require_option_value(args, index, "--multiscales-version");
            continue;
        }
        if (arg == "--plate-version") {
            metadata.has_plate_version = true;
            metadata.plate_version_is_string = true;
            metadata.plate_version =
                require_option_value(args, index, "--plate-version");
            continue;
        }
        if (arg == "--well-version") {
            metadata.has_well_version = true;
            metadata.well_version_is_string = true;
            metadata.well_version =
                require_option_value(args, index, "--well-version");
            continue;
        }
        if (arg == "--image-label-version") {
            metadata.has_image_label_version = true;
            metadata.image_label_version_is_string = true;
            metadata.image_label_version =
                require_option_value(args, index, "--image-label-version");
            continue;
        }
        throw ExitError("Unknown format metadata option: " + arg);
    }
    return metadata;
}

void handle_cli_create_plan(const std::vector<std::string>& args) {
    std::optional<std::string> method_name;
    for (std::size_t index = 0; index < args.size(); ++index) {
        const auto& arg = args[index];
        if (arg == "--method") {
            method_name = require_option_value(args, index, "--method");
            continue;
        }
        throw ExitError("Unknown cli create-plan option: " + arg);
    }
    if (!method_name.has_value()) {
        throw ExitError("Missing required option --method");
    }

    const auto plan = cli_create_plan(method_name.value());
    std::cout << "method_name=" << plan.method_name << "\n";
    std::cout << "label_name=" << plan.label_name << "\n";
}

void handle_cli_scale_factors(const std::vector<std::string>& args) {
    std::optional<std::int64_t> downscale;
    std::optional<std::int64_t> max_layer;
    for (std::size_t index = 0; index < args.size(); ++index) {
        const auto& arg = args[index];
        if (arg == "--downscale") {
            downscale = parse_int64(
                require_option_value(args, index, "--downscale"),
                "--downscale");
            continue;
        }
        if (arg == "--max-layer") {
            max_layer = parse_int64(
                require_option_value(args, index, "--max-layer"),
                "--max-layer");
            continue;
        }
        throw ExitError("Unknown cli scale-factors option: " + arg);
    }
    if (!downscale.has_value() || !max_layer.has_value()) {
        throw ExitError("cli scale-factors requires --downscale and --max-layer");
    }

    const auto factors = cli_scale_factors(downscale.value(), max_layer.value());
    std::cout << join_strings(
        std::vector<std::string>([&] {
            std::vector<std::string> text;
            text.reserve(factors.size());
            for (const auto factor : factors) {
                text.push_back(std::to_string(factor));
            }
            return text;
        }())) << "\n";
}

void handle_format_detect(const std::vector<std::string>& args) {
    const auto metadata = parse_metadata_summary(args);
    const auto detected = detect_format_version(metadata);
    if (!detected.has_value()) {
        std::cout << "NONE\n";
        return;
    }
    std::cout << detected.value() << "\n";
}

void handle_format_matches(const std::vector<std::string>& args) {
    std::optional<std::string> version;
    std::vector<std::string> metadata_args;
    for (std::size_t index = 0; index < args.size(); ++index) {
        const auto& arg = args[index];
        if (arg == "--version") {
            version = require_option_value(args, index, "--version");
            continue;
        }
        metadata_args.push_back(arg);
    }
    if (!version.has_value()) {
        throw ExitError("format matches requires --version");
    }
    const auto metadata = parse_metadata_summary(metadata_args);
    std::cout << (format_matches(version.value(), metadata) ? "true" : "false") << "\n";
}

void handle_format_zarr_format(const std::vector<std::string>& args) {
    if (args.size() != 2 || args[0] != "--version") {
        throw ExitError("format zarr-format requires --version V");
    }
    std::cout << format_zarr_format(args[1]) << "\n";
}

void handle_format_chunk_key_encoding(const std::vector<std::string>& args) {
    if (args.size() != 2 || args[0] != "--version") {
        throw ExitError("format chunk-key-encoding requires --version V");
    }
    const auto encoding = format_chunk_key_encoding(args[1]);
    std::cout << "name=" << encoding.name << "\n";
    std::cout << "separator=" << encoding.separator << "\n";
}

void handle_format_class_name(const std::vector<std::string>& args) {
    if (args.size() != 2 || args[0] != "--version") {
        throw ExitError("format class-name requires --version V");
    }
    std::cout << format_class_name(args[1]) << "\n";
}

void handle_format_generate_well(const std::vector<std::string>& args) {
    std::optional<std::string> path;
    std::vector<std::string> rows;
    std::vector<std::string> columns;

    for (std::size_t index = 0; index < args.size(); ++index) {
        const auto& arg = args[index];
        if (arg == "--path") {
            path = require_option_value(args, index, "--path");
            continue;
        }
        if (arg == "--rows") {
            rows = split_csv(require_option_value(args, index, "--rows"));
            continue;
        }
        if (arg == "--columns") {
            columns = split_csv(require_option_value(args, index, "--columns"));
            continue;
        }
        throw ExitError("Unknown format generate-well option: " + arg);
    }

    if (!path.has_value() || rows.empty() || columns.empty()) {
        throw ExitError(
            "format generate-well requires --path, --rows, and --columns");
    }

    const auto well = generate_well_v04(path.value(), rows, columns);
    std::cout << "path=" << well.path << "\n";
    std::cout << "row_index=" << well.row_index << "\n";
    std::cout << "column_index=" << well.column_index << "\n";
}

void handle_format_validate_well(const std::vector<std::string>& args) {
    std::optional<std::string> path;
    std::optional<std::int64_t> row_index;
    std::optional<std::int64_t> column_index;
    std::vector<std::string> rows;
    std::vector<std::string> columns;

    for (std::size_t index = 0; index < args.size(); ++index) {
        const auto& arg = args[index];
        if (arg == "--path") {
            path = require_option_value(args, index, "--path");
            continue;
        }
        if (arg == "--row-index") {
            row_index = parse_int64(
                require_option_value(args, index, "--row-index"),
                "--row-index");
            continue;
        }
        if (arg == "--column-index") {
            column_index = parse_int64(
                require_option_value(args, index, "--column-index"),
                "--column-index");
            continue;
        }
        if (arg == "--rows") {
            rows = split_csv(require_option_value(args, index, "--rows"));
            continue;
        }
        if (arg == "--columns") {
            columns = split_csv(require_option_value(args, index, "--columns"));
            continue;
        }
        throw ExitError("Unknown format validate-well option: " + arg);
    }

    if (!path.has_value() || !row_index.has_value() || !column_index.has_value() ||
        rows.empty() || columns.empty()) {
        throw ExitError(
            "format validate-well requires --path, --row-index, --column-index, --rows, and --columns");
    }

    validate_well_v04(
        path.value(),
        row_index.value(),
        column_index.value(),
        rows,
        columns);
    std::cout << "OK\n";
}

void dispatch(int argc, char** argv) {
    if (argc < 2) {
        print_usage_and_exit(2);
    }

    const std::string_view group(argv[1]);
    if (group == "--help" || group == "-h") {
        print_usage_and_exit(0);
    }
    if (argc < 3) {
        print_usage_and_exit(2);
    }

    const std::string_view command(argv[2]);
    const auto args = rest_args(argc, argv, 3);

    if (group == "cli" && command == "create-plan") {
        handle_cli_create_plan(args);
        return;
    }
    if (group == "cli" && command == "scale-factors") {
        handle_cli_scale_factors(args);
        return;
    }
    if (group == "format" && command == "detect") {
        handle_format_detect(args);
        return;
    }
    if (group == "format" && command == "matches") {
        handle_format_matches(args);
        return;
    }
    if (group == "format" && command == "zarr-format") {
        handle_format_zarr_format(args);
        return;
    }
    if (group == "format" && command == "chunk-key-encoding") {
        handle_format_chunk_key_encoding(args);
        return;
    }
    if (group == "format" && command == "class-name") {
        handle_format_class_name(args);
        return;
    }
    if (group == "format" && command == "generate-well") {
        handle_format_generate_well(args);
        return;
    }
    if (group == "format" && command == "validate-well") {
        handle_format_validate_well(args);
        return;
    }

    throw ExitError(
        "Unknown command group/command: " + std::string(group) + " " +
        std::string(command));
}

}  // namespace

int main(int argc, char** argv) {
    try {
        dispatch(argc, argv);
        return 0;
    } catch (const ExitError& exc) {
        std::cerr << exc.what() << "\n";
        return 2;
    } catch (const WellGenerationError& exc) {
        std::cerr << exc.what() << ": " << exc.detail() << "\n";
        return 3;
    } catch (const WellValidationError& exc) {
        std::cerr << exc.what() << ": " << exc.detail() << "\n";
        return 3;
    } catch (const std::exception& exc) {
        std::cerr << exc.what() << "\n";
        return 1;
    }
}
