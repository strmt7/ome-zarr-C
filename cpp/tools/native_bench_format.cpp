#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <string>
#include <string_view>
#include <vector>

#include "../native/format.hpp"

namespace {

using Clock = std::chrono::steady_clock;

struct CaseResult {
    std::string name;
    std::size_t iterations;
    std::vector<double> round_microseconds;
};

struct Options {
    std::size_t rounds = 7;
    std::size_t iterations = 200000;
    bool quick = false;
};

std::string_view bool_text(bool value) {
    return value ? "true" : "false";
}

std::size_t parse_positive_integer(const char* text, const char* flag_name) {
    char* end = nullptr;
    const unsigned long long parsed = std::strtoull(text, &end, 10);
    if (end == text || *end != '\0' || parsed == 0) {
        throw std::invalid_argument(
            std::string("Invalid positive integer for ") + flag_name + ": " + text);
    }
    return static_cast<std::size_t>(parsed);
}

Options parse_options(int argc, char** argv) {
    Options options{};
    for (int index = 1; index < argc; ++index) {
        const std::string_view arg(argv[index]);
        if (arg == "--quick") {
            options.quick = true;
            options.rounds = 5;
            options.iterations = 50000;
            continue;
        }
        if (arg == "--rounds") {
            if (index + 1 >= argc) {
                throw std::invalid_argument("Missing value after --rounds");
            }
            options.rounds = parse_positive_integer(argv[++index], "--rounds");
            continue;
        }
        if (arg == "--iterations") {
            if (index + 1 >= argc) {
                throw std::invalid_argument("Missing value after --iterations");
            }
            options.iterations = parse_positive_integer(argv[++index], "--iterations");
            continue;
        }
        if (arg == "--help" || arg == "-h") {
            std::cout
                << "Usage: ome_zarr_native_bench_format [--quick] [--rounds N] [--iterations N]\n";
            std::exit(0);
        }
        throw std::invalid_argument("Unknown argument: " + std::string(arg));
    }
    return options;
}

double median_microseconds(std::vector<double> values) {
    std::sort(values.begin(), values.end());
    const std::size_t midpoint = values.size() / 2;
    if (values.size() % 2 == 1) {
        return values[midpoint];
    }
    return (values[midpoint - 1] + values[midpoint]) / 2.0;
}

template <typename Callable>
CaseResult benchmark_case(
    const std::string& name,
    std::size_t rounds,
    std::size_t iterations,
    Callable&& callable) {
    CaseResult result{name, iterations, {}};
    result.round_microseconds.reserve(rounds);

    for (std::size_t round = 0; round < rounds; ++round) {
        const auto start = Clock::now();
        volatile std::uint64_t sink = 0;
        for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
            sink += callable(iteration);
        }
        const auto stop = Clock::now();
        const auto elapsed =
            std::chrono::duration_cast<std::chrono::duration<double, std::micro>>(
                stop - start);
        result.round_microseconds.push_back(elapsed.count() / static_cast<double>(iterations));
        if (sink == 0xFFFFFFFFFFFFFFFFull) {
            std::cerr << "unreachable sink\n";
        }
    }

    return result;
}

std::uint64_t bench_format_matches(std::size_t iteration) {
    ome_zarr_c::native_code::MetadataSummary metadata{};
    switch (iteration % 5U) {
        case 0:
            metadata.has_multiscales_version = true;
            metadata.multiscales_version_is_string = true;
            metadata.multiscales_version = "0.5";
            break;
        case 1:
            metadata.has_plate_version = true;
            metadata.plate_version_is_string = true;
            metadata.plate_version = "0.4";
            break;
        case 2:
            metadata.has_well_version = true;
            metadata.well_version_is_string = true;
            metadata.well_version = "0.3";
            break;
        case 3:
            metadata.has_image_label_version = true;
            metadata.image_label_version_is_string = true;
            metadata.image_label_version = "0.2";
            break;
        default:
            metadata.is_empty = true;
            break;
    }
    return ome_zarr_c::native_code::format_matches("0.5", metadata) ? 1U : 0U;
}

std::uint64_t bench_generate_well(std::size_t iteration) {
    static const std::vector<std::string> rows = {"A", "B", "C"};
    static const std::vector<std::string> columns = {"1", "2", "3"};
    static const std::vector<std::string> wells = {"A/1", "B/3"};
    const auto& well = wells[iteration % wells.size()];
    const auto generated =
        ome_zarr_c::native_code::generate_well_v04(well, rows, columns);
    return static_cast<std::uint64_t>(generated.row_index + generated.column_index);
}

std::uint64_t bench_validate_well(std::size_t iteration) {
    static const std::vector<std::string> rows = {"A", "B", "C"};
    static const std::vector<std::string> columns = {"1", "2", "3"};
    static const std::vector<ome_zarr_c::native_code::WellDictV04> wells = {
        {"A/1", 0, 0},
        {"B/3", 1, 2},
    };
    const auto& well = wells[iteration % wells.size()];
    ome_zarr_c::native_code::validate_well_v04(
        well.path, well.row_index, well.column_index, rows, columns);
    return static_cast<std::uint64_t>(well.row_index + well.column_index);
}

std::uint64_t bench_coordinate_transformations(std::size_t) {
    static const std::vector<std::vector<double>> shapes = {
        {256.0, 256.0},
        {128.0, 128.0},
        {64.0, 64.0},
    };
    const auto generated =
        ome_zarr_c::native_code::generate_coordinate_transformations(shapes);
    ome_zarr_c::native_code::validate_coordinate_transformations(
        2, static_cast<int>(generated.size()), generated);
    return static_cast<std::uint64_t>(generated.size());
}

void print_results(const Options& options, const std::vector<CaseResult>& results) {
    std::cout << "ome-zarr-C native format benchmark\n";
    std::cout << "rounds=" << options.rounds
              << " iterations=" << options.iterations
              << " quick=" << bool_text(options.quick) << "\n\n";
    std::cout << std::left << std::setw(28) << "case"
              << std::right << std::setw(18) << "median us/op"
              << std::setw(18) << "best us/op"
              << std::setw(18) << "worst us/op" << "\n";
    std::cout << std::string(82, '-') << "\n";
    for (const auto& result : results) {
        const auto median = median_microseconds(result.round_microseconds);
        const auto [min_it, max_it] = std::minmax_element(
            result.round_microseconds.begin(), result.round_microseconds.end());
        std::cout << std::left << std::setw(28) << result.name
                  << std::right << std::setw(18) << std::fixed << std::setprecision(3)
                  << median
                  << std::setw(18) << *min_it
                  << std::setw(18) << *max_it << "\n";
    }
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const Options options = parse_options(argc, argv);
        const std::vector<CaseResult> results = {
            benchmark_case(
                "format_matches",
                options.rounds,
                options.iterations,
                bench_format_matches),
            benchmark_case(
                "generate_well_v04",
                options.rounds,
                options.iterations,
                bench_generate_well),
            benchmark_case(
                "validate_well_v04",
                options.rounds,
                options.iterations,
                bench_validate_well),
            benchmark_case(
                "coordinate_transformations",
                options.rounds,
                options.iterations / 10,
                bench_coordinate_transformations),
        };
        print_results(options, results);
        return 0;
    } catch (const std::exception& exc) {
        std::cerr << exc.what() << "\n";
        return 2;
    }
}
