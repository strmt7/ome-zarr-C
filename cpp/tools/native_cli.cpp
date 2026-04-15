#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "../native/cli.hpp"
#include "../native/create_runtime.hpp"
#include "../native/local_runtime.hpp"
#include "../native/scale_runtime.hpp"

namespace {

using namespace ome_zarr_c::native_code;

struct ExitError final : public std::runtime_error {
    using std::runtime_error::runtime_error;
};

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

[[noreturn]] void print_usage_and_exit(int code) {
    std::ostream& stream = code == 0 ? std::cout : std::cerr;
    stream
        << "Usage: ome_zarr_native_cli <command> [options]\n"
        << "\n"
        << "Commands:\n"
        << "  info <path>\n"
        << "  create [--method coins|astronaut] <path> [--format 0.4|0.5]\n"
        << "  download <path> [--output DIR]\n"
        << "  finder <path> [--port PORT]\n"
        << "  view <path> [--port PORT] [--force|-f]\n"
        << "  scale <input_array> <output_directory> <axes> [--copy-metadata] [--method METHOD]\n"
        << "  csv_to_labels <csv_path> <csv_id> <csv_keys> <zarr_path> <zarr_id>\n";
    std::exit(code);
}

void handle_create(const std::vector<std::string>& args) {
    std::string method = "coins";
    std::string version = "0.5";
    std::string path;

    for (std::size_t index = 0; index < args.size(); ++index) {
        const auto& arg = args[index];
        if (arg == "--method") {
            method = require_option_value(args, index, "--method");
            continue;
        }
        if (arg.rfind("--method=", 0) == 0) {
            method = arg.substr(std::string("--method=").size());
            continue;
        }
        if (arg == "--format") {
            version = require_option_value(args, index, "--format");
            continue;
        }
        if (arg.rfind("--format=", 0) == 0) {
            version = arg.substr(std::string("--format=").size());
            continue;
        }
        if (!arg.empty() && arg[0] == '-') {
            throw ExitError("Unknown create option: " + arg);
        }
        if (!path.empty()) {
            throw ExitError("create accepts exactly one path argument");
        }
        path = arg;
    }

    if (path.empty()) {
        throw ExitError("create requires a path argument");
    }

    const auto plan = cli_create_plan(method);
    local_create_sample(
        path,
        plan.method_name,
        plan.label_name,
        version,
        CreateColorMode::native_random);
}

void handle_info(const std::vector<std::string>& args) {
    std::string path;
    bool stats = false;

    for (std::size_t index = 0; index < args.size(); ++index) {
        const auto& arg = args[index];
        if (arg == "--stats") {
            stats = true;
            continue;
        }
        if (!arg.empty() && arg[0] == '-') {
            throw ExitError("Unknown info option: " + arg);
        }
        if (!path.empty()) {
            throw ExitError("info accepts exactly one path argument");
        }
        path = arg;
    }

    if (path.empty()) {
        throw ExitError("info requires a path argument");
    }

    for (const auto& line : local_info_lines(path, stats)) {
        std::cout << line << "\n";
    }
}

void handle_finder(const std::vector<std::string>& args) {
    std::string path;
    int port = 8000;

    for (std::size_t index = 0; index < args.size(); ++index) {
        const auto& arg = args[index];
        if (arg == "--port") {
            const auto value = require_option_value(args, index, "--port");
            try {
                port = std::stoi(value);
            } catch (const std::exception&) {
                throw ExitError("Invalid integer for --port: " + value);
            }
            continue;
        }
        if (!arg.empty() && arg[0] == '-') {
            throw ExitError("Unknown finder option: " + arg);
        }
        if (!path.empty()) {
            throw ExitError("finder accepts exactly one path argument");
        }
        path = arg;
    }

    if (path.empty()) {
        throw ExitError("finder requires a path argument");
    }

    const auto result = local_finder_csv(path, port);
    if (!result.found_any) {
        std::cout << "No OME-Zarr files found in " << path << "\n";
    }
}

void handle_download(const std::vector<std::string>& args) {
    std::string path;
    std::string output_dir = ".";

    for (std::size_t index = 0; index < args.size(); ++index) {
        const auto& arg = args[index];
        if (arg == "--output") {
            output_dir = require_option_value(args, index, "--output");
            continue;
        }
        if (arg.rfind("--output=", 0) == 0) {
            output_dir = arg.substr(std::string("--output=").size());
            continue;
        }
        if (!arg.empty() && arg[0] == '-') {
            throw ExitError("Unknown download option: " + arg);
        }
        if (!path.empty()) {
            throw ExitError("download accepts exactly one path argument");
        }
        path = arg;
    }

    if (path.empty()) {
        throw ExitError("download requires a path argument");
    }

    const auto result = local_download_copy(path, output_dir);
    std::cout << "downloading...\n";
    for (const auto& listed_path : result.listed_paths) {
        std::cout << "   " << listed_path << "\n";
    }
    std::cout << "to " << output_dir << "\n";
}

void handle_view(const std::vector<std::string>& args) {
    std::string path;
    int port = 8000;
    bool force = false;

    for (std::size_t index = 0; index < args.size(); ++index) {
        const auto& arg = args[index];
        if (arg == "--port") {
            const auto value = require_option_value(args, index, "--port");
            try {
                port = std::stoi(value);
            } catch (const std::exception&) {
                throw ExitError("Invalid integer for --port: " + value);
            }
            continue;
        }
        if (arg == "--force" || arg == "-f") {
            force = true;
            continue;
        }
        if (!arg.empty() && arg[0] == '-') {
            throw ExitError("Unknown view option: " + arg);
        }
        if (!path.empty()) {
            throw ExitError("view accepts exactly one path argument");
        }
        path = arg;
    }

    if (path.empty()) {
        throw ExitError("view requires a path argument");
    }

    const auto preparation = local_view_prepare(path, port, force);
    if (preparation.should_warn) {
        std::cout << preparation.warning_message << "\n";
        return;
    }
    local_view_run(preparation, port);
}

void handle_csv_to_labels(const std::vector<std::string>& args) {
    if (args.size() != 5U) {
        throw ExitError(
            "csv_to_labels requires csv_path csv_id csv_keys zarr_path zarr_id");
    }

    const auto& csv_path = args[0];
    const auto& csv_id = args[1];
    const auto& csv_keys = args[2];
    const auto& zarr_path = args[3];
    const auto& zarr_id = args[4];

    std::cout << "csv_to_labels " << csv_path << " " << zarr_path << "\n";
    static_cast<void>(local_csv_to_labels(
        csv_path,
        csv_id,
        csv_keys,
        zarr_path,
        zarr_id));
}

void handle_scale(const std::vector<std::string>& args) {
    std::string input_array;
    std::string output_directory;
    std::string axes;
    bool copy_metadata = false;
    bool in_place = false;
    std::string method = "resize";
    int downscale = 2;
    int max_layer = 4;

    std::vector<std::string> positional;
    positional.reserve(3);
    for (std::size_t index = 0; index < args.size(); ++index) {
        const auto& arg = args[index];
        if (arg == "--copy-metadata") {
            copy_metadata = true;
            continue;
        }
        if (arg == "--in-place") {
            in_place = true;
            continue;
        }
        if (arg == "--method") {
            method = require_option_value(args, index, "--method");
            continue;
        }
        if (arg.rfind("--method=", 0) == 0) {
            method = arg.substr(std::string("--method=").size());
            continue;
        }
        if (arg == "--downscale") {
            const auto value = require_option_value(args, index, "--downscale");
            try {
                downscale = std::stoi(value);
            } catch (const std::exception&) {
                throw ExitError("Invalid integer for --downscale: " + value);
            }
            continue;
        }
        if (arg.rfind("--downscale=", 0) == 0) {
            const auto value = arg.substr(std::string("--downscale=").size());
            try {
                downscale = std::stoi(value);
            } catch (const std::exception&) {
                throw ExitError("Invalid integer for --downscale: " + value);
            }
            continue;
        }
        if (arg == "--max_layer") {
            const auto value = require_option_value(args, index, "--max_layer");
            try {
                max_layer = std::stoi(value);
            } catch (const std::exception&) {
                throw ExitError("Invalid integer for --max_layer: " + value);
            }
            continue;
        }
        if (arg.rfind("--max_layer=", 0) == 0) {
            const auto value = arg.substr(std::string("--max_layer=").size());
            try {
                max_layer = std::stoi(value);
            } catch (const std::exception&) {
                throw ExitError("Invalid integer for --max_layer: " + value);
            }
            continue;
        }
        if (!arg.empty() && arg[0] == '-') {
            throw ExitError("Unknown scale option: " + arg);
        }
        positional.push_back(arg);
    }

    if (positional.size() != 3U) {
        throw ExitError("scale requires input_array output_directory axes");
    }
    input_array = positional[0];
    output_directory = positional[1];
    axes = positional[2];

    if (copy_metadata) {
        const auto result = local_scale_array(
            input_array,
            output_directory,
            axes,
            copy_metadata,
            method,
            in_place,
            downscale,
            max_layer);
        std::cout << "copying attribute keys: ";
        std::cout << "[";
        for (std::size_t index = 0; index < result.copied_metadata_keys.size(); ++index) {
            if (index > 0) {
                std::cout << ", ";
            }
            std::cout << "'" << result.copied_metadata_keys[index] << "'";
        }
        std::cout << "]\n";
        return;
    }

    static_cast<void>(local_scale_array(
        input_array,
        output_directory,
        axes,
        copy_metadata,
        method,
        in_place,
        downscale,
        max_layer));
}

void dispatch(int argc, char** argv) {
    if (argc < 2) {
        print_usage_and_exit(2);
    }

    const std::string command = argv[1];
    if (command == "--help" || command == "-h") {
        print_usage_and_exit(0);
    }

    const auto args = rest_args(argc, argv, 2);
    if (command == "info") {
        handle_info(args);
        return;
    }
    if (command == "create") {
        handle_create(args);
        return;
    }
    if (command == "finder") {
        handle_finder(args);
        return;
    }
    if (command == "download") {
        handle_download(args);
        return;
    }
    if (command == "view") {
        handle_view(args);
        return;
    }
    if (command == "csv_to_labels") {
        handle_csv_to_labels(args);
        return;
    }
    if (command == "scale") {
        handle_scale(args);
        return;
    }

    throw ExitError("Unknown command: " + command);
}

}  // namespace

int main(int argc, char** argv) {
    try {
        dispatch(argc, argv);
        return 0;
    } catch (const ExitError& exc) {
        std::cerr << exc.what() << "\n";
        return 2;
    } catch (const std::exception& exc) {
        std::cerr << exc.what() << "\n";
        return 1;
    }
}
