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
#include "../native/data.hpp"
#include "../native/format.hpp"
#include "../native/io.hpp"
#include "../native/utils.hpp"
#include "../native/writer.hpp"

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

std::vector<std::size_t> parse_size_list(
    const std::string& text,
    std::string_view option_name) {
    const auto items = split_csv(text);
    std::vector<std::size_t> values;
    values.reserve(items.size());
    for (const auto& item : items) {
        const auto parsed = parse_int64(item, option_name);
        if (parsed < 0) {
            throw ExitError(
                "Invalid non-negative integer for " + std::string(option_name) + ": " + item);
        }
        values.push_back(static_cast<std::size_t>(parsed));
    }
    return values;
}

[[noreturn]] void print_usage_and_exit(int code) {
    std::ostream& stream = code == 0 ? std::cout : std::cerr;
    stream
        << "Usage: ome_zarr_native_cli <group> <command> [options]\n"
        << "\n"
        << "Groups and commands:\n"
        << "  cli create-plan --method <coins|astronaut>\n"
        << "  cli scale-factors --downscale <int> --max-layer <int>\n"
        << "  data create-plan --version V --base-shape N,N,... --smallest-shape N,N,... [--chunks N,N,...]\n"
        << "  format detect [--empty] [--multiscales-version V] [--plate-version V]\n"
        << "  format matches --version V [--empty] [--multiscales-version V] [--plate-version V]\n"
        << "  format zarr-format --version V\n"
        << "  format chunk-key-encoding --version V\n"
        << "  format class-name --version V\n"
        << "  format generate-well --path ROW/COL --rows A,B,... --columns 1,2,...\n"
        << "  format validate-well --path ROW/COL --row-index N --column-index N"
           " --rows A,B,... --columns 1,2,...\n"
        << "  io subpath --path PATH --subpath SUBPATH [--file] [--http]\n"
        << "  utils view-plan --path PATH --port N [--force] [--discovered-count N]\n"
        << "  utils finder-plan --path PATH --port N\n"
        << "  writer image-plan --axes A,B,... --scaler-max-layer N --scaler-method METHOD [--scaler-present] [--requested-method METHOD]\n";
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

void handle_data_create_plan(const std::vector<std::string>& args) {
    std::optional<std::string> version;
    std::vector<std::size_t> base_shape;
    std::vector<std::size_t> smallest_shape;
    std::vector<std::size_t> chunks;

    for (std::size_t index = 0; index < args.size(); ++index) {
        const auto& arg = args[index];
        if (arg == "--version") {
            version = require_option_value(args, index, "--version");
            continue;
        }
        if (arg == "--base-shape") {
            base_shape = parse_size_list(
                require_option_value(args, index, "--base-shape"),
                "--base-shape");
            continue;
        }
        if (arg == "--smallest-shape") {
            smallest_shape = parse_size_list(
                require_option_value(args, index, "--smallest-shape"),
                "--smallest-shape");
            continue;
        }
        if (arg == "--chunks") {
            chunks = parse_size_list(
                require_option_value(args, index, "--chunks"),
                "--chunks");
            continue;
        }
        throw ExitError("Unknown data create-plan option: " + arg);
    }
    if (!version.has_value() || base_shape.empty() || smallest_shape.empty()) {
        throw ExitError(
            "data create-plan requires --version, --base-shape, and --smallest-shape");
    }

    const auto plan = create_zarr_plan(
        version.value(),
        base_shape,
        smallest_shape,
        chunks);
    std::cout << "axes=" << plan.axes << "\n";
    std::cout << "axes_is_none=" << (plan.axes_is_none ? "true" : "false") << "\n";
    std::cout << "size_c=" << plan.size_c << "\n";
    std::vector<std::string> chunk_text;
    chunk_text.reserve(plan.chunks.size());
    for (const auto value : plan.chunks) {
        chunk_text.push_back(std::to_string(value));
    }
    std::cout << "chunks=" << join_strings(chunk_text) << "\n";
    std::cout << "color_image=" << (plan.color_image ? "true" : "false") << "\n";
    std::cout << "channel_model=" << plan.channel_model << "\n";
    std::cout << "labels_axes=" << plan.labels_axes << "\n";
    std::cout << "labels_axes_is_none="
              << (plan.labels_axes_is_none ? "true" : "false") << "\n";
    std::cout << "random_label_count=" << plan.random_label_count << "\n";
    std::cout << "source_image=" << plan.source_image << "\n";
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

void handle_io_subpath(const std::vector<std::string>& args) {
    std::optional<std::string> path;
    std::optional<std::string> subpath;
    bool is_file = false;
    bool is_http = false;
    for (std::size_t index = 0; index < args.size(); ++index) {
        const auto& arg = args[index];
        if (arg == "--path") {
            path = require_option_value(args, index, "--path");
            continue;
        }
        if (arg == "--subpath") {
            subpath = require_option_value(args, index, "--subpath");
            continue;
        }
        if (arg == "--file") {
            is_file = true;
            continue;
        }
        if (arg == "--http") {
            is_http = true;
            continue;
        }
        throw ExitError("Unknown io subpath option: " + arg);
    }
    if (!path.has_value() || !subpath.has_value()) {
        throw ExitError("io subpath requires --path and --subpath");
    }
    std::cout << io_subpath(path.value(), subpath.value(), is_file, is_http) << "\n";
}

void handle_utils_view_plan(const std::vector<std::string>& args) {
    std::optional<std::string> path;
    std::optional<std::int64_t> port;
    bool force = false;
    std::size_t discovered_count = 1;

    for (std::size_t index = 0; index < args.size(); ++index) {
        const auto& arg = args[index];
        if (arg == "--path") {
            path = require_option_value(args, index, "--path");
            continue;
        }
        if (arg == "--port") {
            port = parse_int64(require_option_value(args, index, "--port"), "--port");
            continue;
        }
        if (arg == "--force") {
            force = true;
            continue;
        }
        if (arg == "--discovered-count") {
            const auto parsed = parse_int64(
                require_option_value(args, index, "--discovered-count"),
                "--discovered-count");
            if (parsed < 0) {
                throw ExitError("Invalid non-negative integer for --discovered-count");
            }
            discovered_count = static_cast<std::size_t>(parsed);
            continue;
        }
        throw ExitError("Unknown utils view-plan option: " + arg);
    }
    if (!path.has_value() || !port.has_value()) {
        throw ExitError("utils view-plan requires --path and --port");
    }

    const auto plan = utils_view_plan(
        path.value(),
        static_cast<int>(port.value()),
        force,
        discovered_count);
    std::cout << "should_warn=" << (plan.should_warn ? "true" : "false") << "\n";
    if (plan.should_warn) {
        std::cout << "warning_message=" << plan.warning_message << "\n";
        return;
    }
    std::cout << "parent_dir=" << plan.parent_dir << "\n";
    std::cout << "image_name=" << plan.image_name << "\n";
    std::cout << "url=" << plan.url << "\n";
}

void handle_utils_finder_plan(const std::vector<std::string>& args) {
    std::optional<std::string> path;
    std::optional<std::int64_t> port;
    for (std::size_t index = 0; index < args.size(); ++index) {
        const auto& arg = args[index];
        if (arg == "--path") {
            path = require_option_value(args, index, "--path");
            continue;
        }
        if (arg == "--port") {
            port = parse_int64(require_option_value(args, index, "--port"), "--port");
            continue;
        }
        throw ExitError("Unknown utils finder-plan option: " + arg);
    }
    if (!path.has_value() || !port.has_value()) {
        throw ExitError("utils finder-plan requires --path and --port");
    }

    const auto plan = utils_finder_plan(path.value(), static_cast<int>(port.value()));
    std::cout << "parent_path=" << plan.parent_path << "\n";
    std::cout << "server_dir=" << plan.server_dir << "\n";
    std::cout << "csv_path=" << plan.csv_path << "\n";
    std::cout << "source_uri=" << plan.source_uri << "\n";
    std::cout << "url=" << plan.url << "\n";
}

void handle_writer_image_plan(const std::vector<std::string>& args) {
    std::vector<std::string> axes;
    bool scaler_present = false;
    std::optional<std::int64_t> scaler_max_layer;
    std::optional<std::string> scaler_method;
    std::optional<std::string> requested_method;

    for (std::size_t index = 0; index < args.size(); ++index) {
        const auto& arg = args[index];
        if (arg == "--axes") {
            axes = split_csv(require_option_value(args, index, "--axes"));
            continue;
        }
        if (arg == "--scaler-present") {
            scaler_present = true;
            continue;
        }
        if (arg == "--scaler-max-layer") {
            scaler_max_layer = parse_int64(
                require_option_value(args, index, "--scaler-max-layer"),
                "--scaler-max-layer");
            continue;
        }
        if (arg == "--scaler-method") {
            scaler_method = require_option_value(args, index, "--scaler-method");
            continue;
        }
        if (arg == "--requested-method") {
            requested_method = require_option_value(args, index, "--requested-method");
            continue;
        }
        throw ExitError("Unknown writer image-plan option: " + arg);
    }
    if (axes.empty() || !scaler_max_layer.has_value() || !scaler_method.has_value()) {
        throw ExitError(
            "writer image-plan requires --axes, --scaler-max-layer, and --scaler-method");
    }

    const auto plan = writer_image_plan(
        axes,
        scaler_present,
        scaler_max_layer.value(),
        scaler_method.value(),
        requested_method);
    std::cout << "resolved_method=" << plan.resolved_method << "\n";
    std::cout << "warn_scaler_deprecated="
              << (plan.warn_scaler_deprecated ? "true" : "false") << "\n";
    std::cout << "warn_laplacian_fallback="
              << (plan.warn_laplacian_fallback ? "true" : "false") << "\n";
    std::cout << "scale_factor_count=" << plan.scale_factors.size() << "\n";
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
    if (group == "data" && command == "create-plan") {
        handle_data_create_plan(args);
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
    if (group == "io" && command == "subpath") {
        handle_io_subpath(args);
        return;
    }
    if (group == "utils" && command == "view-plan") {
        handle_utils_view_plan(args);
        return;
    }
    if (group == "utils" && command == "finder-plan") {
        handle_utils_finder_plan(args);
        return;
    }
    if (group == "writer" && command == "image-plan") {
        handle_writer_image_plan(args);
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
