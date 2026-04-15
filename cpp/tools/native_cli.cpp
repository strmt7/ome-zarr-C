#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "../native/local_runtime.hpp"

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
        << "  download <path> [--output DIR]\n"
        << "  finder <path> [--port PORT]\n"
        << "  view <path> [--port PORT] [--force|-f]\n"
        << "  csv_to_labels <csv_path> <csv_id> <csv_keys> <zarr_path> <zarr_id>\n";
    std::exit(code);
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
