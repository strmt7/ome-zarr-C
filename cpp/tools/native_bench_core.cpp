#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "../native/axes.hpp"
#include "../native/cli.hpp"
#include "../native/conversions.hpp"
#include "../native/csv.hpp"
#include "../native/dask_utils.hpp"
#include "../native/data.hpp"
#include "../native/format.hpp"
#include "../native/io.hpp"
#include "../native/reader.hpp"
#include "../native/scale.hpp"
#include "../native/utils.hpp"
#include "../native/writer.hpp"

namespace {

using namespace ome_zarr_c::native_code;
using Clock = std::chrono::steady_clock;

struct CaseResult {
    std::string name;
    std::size_t iterations;
    std::vector<double> round_microseconds;
};

struct Options {
    std::size_t rounds = 5;
    std::size_t iterations = 5000;
    std::string match;
    bool quick = false;
};

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
            options.rounds = 4;
            options.iterations = 1000;
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
        if (arg == "--match") {
            if (index + 1 >= argc) {
                throw std::invalid_argument("Missing value after --match");
            }
            options.match = argv[++index];
            continue;
        }
        if (arg == "--help" || arg == "-h") {
            std::cout
                << "Usage: ome_zarr_native_bench_core [--quick] [--rounds N] [--iterations N] [--match text]\n";
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

struct CaseDefinition {
    std::string name;
    std::size_t divisor;
    std::function<std::uint64_t(std::size_t)> callable;
};

std::uint64_t bench_axes_validate_types(std::size_t) {
    validate_axes_types({
        AxisRecord{true, "t", true, "time", "", "'time'"},
        AxisRecord{true, "c", true, "channel", "", "'channel'"},
        AxisRecord{true, "y", true, "space", "", "'space'"},
        AxisRecord{true, "x", true, "space", "", "'space'"},
    });
    return 1U;
}

std::uint64_t bench_conversions_roundtrip(std::size_t iteration) {
    const auto value = rgba_to_int(
        static_cast<std::uint8_t>(iteration & 0xFFU),
        static_cast<std::uint8_t>((iteration + 17U) & 0xFFU),
        static_cast<std::uint8_t>((iteration + 37U) & 0xFFU),
        255U);
    const auto rgba = int_to_rgba_255_bytes(value);
    return static_cast<std::uint64_t>(rgba[0] + rgba[1] + rgba[2] + rgba[3]);
}

std::uint64_t bench_csv_props(std::size_t) {
    const auto props = csv_props_by_id(
        {
            {"cell_id", "score", "count", "flag"},
            {"a", "3.5", "2", "1"},
            {"b", "7.0", "4", ""},
            {"c", "11.0", "8", "1"},
        },
        "cell_id",
        parse_csv_key_specs("score#d,count#l,flag#b"));
    return static_cast<std::uint64_t>(props.size());
}

std::uint64_t bench_dask_zoom(std::size_t iteration) {
    const auto plan = zoom_plan(
        {2048, 2048},
        {256, 256},
        {1024 - static_cast<std::int64_t>(iteration % 8), 1024});
    return static_cast<std::uint64_t>(plan.resized_output_shape[0] + plan.resized_output_shape[1]);
}

std::uint64_t bench_data_circle_points(std::size_t) {
    return static_cast<std::uint64_t>(circle_points(256, 256).size());
}

std::uint64_t bench_format_matches(std::size_t iteration) {
    MetadataSummary metadata{};
    switch (iteration % 4U) {
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
        default:
            metadata.has_image_label_version = true;
            metadata.image_label_version_is_string = true;
            metadata.image_label_version = "0.2";
            break;
    }
    return format_matches("0.5", metadata) ? 1U : 0U;
}

std::uint64_t bench_io_subpath(std::size_t iteration) {
    const auto path = io_subpath(
        "http://example.org/data",
        "group/" + std::to_string(iteration % 16U),
        false,
        true);
    return static_cast<std::uint64_t>(path.size());
}

std::uint64_t bench_reader_plate_levels(std::size_t) {
    const auto plans = reader_plate_level_plans(
        {"A", "B", "C"},
        {"1", "2", "3"},
        {"A/1", "A/2", "B/3"},
        "0",
        {"0", "1", "2"});
    return static_cast<std::uint64_t>(plans.size() + plans.front().tile_paths.size());
}

std::uint64_t bench_scale_build_pyramid(std::size_t) {
    const auto levels = scale_levels_from_ints({"t", "c", "y", "x"}, 4);
    const auto plan = build_pyramid_plan({1, 3, 2048, 2048}, {"t", "c", "y", "x"}, levels);
    return static_cast<std::uint64_t>(plan.back().target_shape.back());
}

std::uint64_t bench_utils_view_plan(std::size_t iteration) {
    const auto plan = utils_view_plan(
        "/tmp/image_" + std::to_string(iteration % 8U) + ".zarr",
        8000,
        false,
        1);
    return static_cast<std::uint64_t>(plan.url.size());
}

std::uint64_t bench_writer_image_plan(std::size_t) {
    const auto plan = writer_image_plan(
        {"t", "c", "y", "x"},
        true,
        4,
        "local_mean",
        std::nullopt);
    return static_cast<std::uint64_t>(plan.scale_factors.size());
}

void print_results(const Options& options, const std::vector<CaseResult>& results) {
    std::cout << "ome-zarr-C native core benchmark\n";
    std::cout << "rounds=" << options.rounds
              << " iterations=" << options.iterations
              << " quick=" << (options.quick ? "true" : "false")
              << " match=" << (options.match.empty() ? "<all>" : options.match)
              << "\n\n";
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
        std::vector<CaseDefinition> cases = {
            {"axes_validate_types", 1, bench_axes_validate_types},
            {"conversions_roundtrip", 1, bench_conversions_roundtrip},
            {"csv_props_by_id", 8, bench_csv_props},
            {"dask_zoom_plan", 1, bench_dask_zoom},
            {"data_circle_points", 16, bench_data_circle_points},
            {"format_matches", 1, bench_format_matches},
            {"io_subpath", 1, bench_io_subpath},
            {"reader_plate_levels", 4, bench_reader_plate_levels},
            {"scale_build_pyramid", 8, bench_scale_build_pyramid},
            {"utils_view_plan", 1, bench_utils_view_plan},
            {"writer_image_plan", 1, bench_writer_image_plan},
        };

        std::vector<CaseResult> results;
        for (const auto& definition : cases) {
            if (!options.match.empty() &&
                definition.name.find(options.match) == std::string::npos) {
                continue;
            }
            const std::size_t case_iterations =
                std::max<std::size_t>(1, options.iterations / definition.divisor);
            results.push_back(benchmark_case(
                definition.name,
                options.rounds,
                case_iterations,
                definition.callable));
        }

        if (results.empty()) {
            throw std::invalid_argument("No benchmark cases matched the requested filter");
        }

        print_results(options, results);
        return 0;
    } catch (const std::exception& exc) {
        std::cerr << exc.what() << "\n";
        return 2;
    }
}
