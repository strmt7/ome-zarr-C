#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

#include <zstd.h>

#include "../native/axes.hpp"
#include "../native/cli.hpp"
#include "../native/conversions.hpp"
#include "../native/create_runtime.hpp"
#include "../native/csv.hpp"
#include "../native/dask_utils.hpp"
#include "../native/data.hpp"
#include "../native/format.hpp"
#include "../native/io.hpp"
#include "../native/local_runtime.hpp"
#include "../native/reader.hpp"
#include "../native/reader_oracle.hpp"
#include "../native/scale.hpp"
#include "../native/scale_runtime.hpp"
#include "../native/utils.hpp"
#include "../native/writer.hpp"

namespace {

using namespace ome_zarr_c::native_code;
using Clock = std::chrono::steady_clock;
namespace fs = std::filesystem;

std::vector<char> zstd_compress_bytes(const std::vector<char>& payload) {
    std::vector<char> compressed(ZSTD_compressBound(payload.size()));
    const auto written = ZSTD_compress(
        compressed.data(),
        compressed.size(),
        payload.data(),
        payload.size(),
        0);
    if (ZSTD_isError(written) != 0U) {
        throw std::runtime_error(
            std::string("ZSTD_compress failed: ") + ZSTD_getErrorName(written));
    }
    compressed.resize(static_cast<std::size_t>(written));
    return compressed;
}

struct CaseResult {
    std::string name;
    std::size_t iterations;
    std::vector<double> round_microseconds;
};

struct Options {
    std::size_t rounds = 5;
    std::size_t iterations = 5000;
    std::string match;
    std::string json_output;
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
        if (arg == "--json-output") {
            if (index + 1 >= argc) {
                throw std::invalid_argument("Missing value after --json-output");
            }
            options.json_output = argv[++index];
            continue;
        }
        if (arg == "--help" || arg == "-h") {
            std::cout
                << "Usage: ome_zarr_native_bench_core [--quick] [--rounds N] [--iterations N] [--match text] [--json-output path]\n";
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

std::string json_escape(std::string_view text) {
    std::ostringstream output;
    for (const char ch : text) {
        switch (ch) {
            case '\\':
                output << "\\\\";
                break;
            case '"':
                output << "\\\"";
                break;
            case '\n':
                output << "\\n";
                break;
            case '\r':
                output << "\\r";
                break;
            case '\t':
                output << "\\t";
                break;
            default:
                output << ch;
                break;
        }
    }
    return output.str();
}

void write_u16_v2_array_fixture(
    const fs::path& root,
    const std::vector<std::uint16_t>& values,
    const std::vector<std::int64_t>& shape,
    const std::vector<std::int64_t>& chunk_shape,
    const std::optional<int>& alpha_attr = std::nullopt) {
    fs::create_directories(root);
    {
        std::ofstream zarray(root / ".zarray");
        zarray
            << "{\"shape\":[" << shape[0] << "," << shape[1] << "],"
            << "\"chunks\":[" << chunk_shape[0] << "," << chunk_shape[1] << "],"
            << "\"dtype\":\"<u2\",\"compressor\":null,\"fill_value\":0,"
            << "\"filters\":null,\"order\":\"C\",\"dimension_separator\":\".\","
            << "\"zarr_format\":2}";
    }
    if (alpha_attr.has_value()) {
        std::ofstream zattrs(root / ".zattrs");
        zattrs << "{\"alpha\":" << alpha_attr.value() << "}";
    }
    for (std::int64_t chunk_y = 0; chunk_y < shape[0] / chunk_shape[0]; ++chunk_y) {
        for (std::int64_t chunk_x = 0; chunk_x < shape[1] / chunk_shape[1]; ++chunk_x) {
            std::ofstream chunk(
                root / (std::to_string(chunk_y) + "." + std::to_string(chunk_x)),
                std::ios::binary);
            for (std::int64_t y = 0; y < chunk_shape[0]; ++y) {
                for (std::int64_t x = 0; x < chunk_shape[1]; ++x) {
                    const auto global_y = chunk_y * chunk_shape[0] + y;
                    const auto global_x = chunk_x * chunk_shape[1] + x;
                    const auto value = values[static_cast<std::size_t>(
                        global_y * shape[1] + global_x)];
                    chunk.write(
                        reinterpret_cast<const char*>(&value),
                        static_cast<std::streamsize>(sizeof(value)));
                }
            }
        }
    }
}

const fs::path& bench_runtime_fixture_root() {
    static const fs::path root = [] {
        const fs::path root_path =
            fs::temp_directory_path() / "ome_zarr_c_native_bench_runtime";
        fs::create_directories(root_path);

        const fs::path info_v2 = root_path / "info_v2.zarr";
        fs::create_directories(info_v2 / "0");
        {
            std::ofstream zattrs(info_v2 / ".zattrs");
            zattrs << R"({"multiscales":[{"version":"0.4","axes":["y","x"],"datasets":[{"path":"0"}]}]})";
        }
        {
            std::ofstream zgroup(info_v2 / ".zgroup");
            zgroup << R"({"zarr_format":2})";
        }
        {
            std::ofstream array_json(info_v2 / "0" / "zarr.json");
            array_json << R"({"shape":[2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,2]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"attributes":{},"zarr_format":3,"node_type":"array","storage_transformers":[]})";
        }

        const fs::path info_v3 = root_path / "info_v3.zarr";
        fs::create_directories(info_v3 / "s0" / "c" / "0");
        {
            std::ofstream zarr_json(info_v3 / "zarr.json");
            zarr_json << R"({"attributes":{"ome":{"version":"0.5","multiscales":[{"axes":[{"name":"y","type":"space"},{"name":"x","type":"space"}],"datasets":[{"path":"s0","coordinateTransformations":[{"type":"scale","scale":[1,1]}]}]}]}},"zarr_format":3,"node_type":"group"})";
        }
        {
            std::ofstream array_json(info_v3 / "s0" / "zarr.json");
            array_json << R"({"shape":[2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,2]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}},{"name":"zstd","configuration":{"level":0,"checksum":false}}],"attributes":{},"zarr_format":3,"node_type":"array","storage_transformers":[]})";
        }
        {
            const std::vector<char> raw = {
                5, 0, 0, 0,
                6, 0, 0, 0,
                7, 0, 0, 0,
                8, 0, 0, 0,
            };
            const auto compressed = zstd_compress_bytes(raw);
            std::ofstream chunk(info_v3 / "s0" / "c" / "0" / "0", std::ios::binary);
            chunk.write(compressed.data(), static_cast<std::streamsize>(compressed.size()));
        }

        const fs::path finder_root = root_path / "finder_tree";
        fs::create_directories(finder_root / "image.zarr");
        {
            std::ofstream zattrs(finder_root / "image.zarr" / ".zattrs");
            zattrs << R"({"multiscales":[{}]})";
        }

        const fs::path csv_image = root_path / "csv_image.zarr";
        fs::create_directories(csv_image / "labels" / "0");
        {
            std::ofstream zarr_json(csv_image / "zarr.json");
            zarr_json << R"({"attributes":{"multiscales":[{"version":"0.4"}]},"zarr_format":3,"node_type":"group"})";
        }
        {
            std::ofstream zarr_json(csv_image / "labels" / "0" / "zarr.json");
            zarr_json << R"({"attributes":{"image-label":{"properties":[{"cell_id":"1"}]}},"zarr_format":3,"node_type":"group"})";
        }
        {
            std::ofstream csv_file(root_path / "csv_props.csv");
            csv_file << "cell_id,score\n";
            csv_file << "1,4.5\n";
        }

        std::vector<std::uint16_t> scale_values(64);
        for (std::size_t index = 0; index < scale_values.size(); ++index) {
            scale_values[index] = static_cast<std::uint16_t>(index);
        }
        write_u16_v2_array_fixture(
            root_path / "scale_input.zarr",
            scale_values,
            {8, 8},
            {2, 2},
            1);

        return root_path;
    }();
    return root;
}

std::uint64_t bench_axes_validate_types(std::size_t) {
    validate_axes_types({
        AxisRecord{true, "t", true, "time", "", "'time'"},
        AxisRecord{true, "c", true, "channel", "", "'channel'"},
        AxisRecord{true, "y", true, "space", "", "'space'"},
        AxisRecord{true, "x", true, "space", "", "'space'"},
    });
    return 1U;
}

std::uint64_t bench_axes_constructor(std::size_t iteration) {
    static const std::vector<std::pair<std::string, std::vector<AxisRecord>>> kCases = {
        {"0.1", {}},
        {"0.2", {}},
        {"0.3",
         {
             AxisRecord{true, "y", false, "", "", "None"},
             AxisRecord{true, "x", false, "", "", "None"},
         }},
        {"0.4",
         {
             AxisRecord{true, "y", false, "", "", "None"},
             AxisRecord{true, "x", false, "", "", "None"},
         }},
        {"0.5",
         {
             AxisRecord{true, "y", false, "", "", "None"},
             AxisRecord{true, "x", false, "", "", "None"},
         }},
        {"0.3",
         {
             AxisRecord{true, "z", false, "", "", "None"},
             AxisRecord{true, "y", false, "", "", "None"},
             AxisRecord{true, "x", false, "", "", "None"},
         }},
        {"0.4",
         {
             AxisRecord{true, "z", false, "", "", "None"},
             AxisRecord{true, "y", false, "", "", "None"},
             AxisRecord{true, "x", false, "", "", "None"},
         }},
        {"0.5",
         {
             AxisRecord{true, "t", false, "", "", "None"},
             AxisRecord{true, "c", false, "", "", "None"},
             AxisRecord{true, "z", false, "", "", "None"},
             AxisRecord{true, "y", false, "", "", "None"},
             AxisRecord{true, "x", false, "", "", "None"},
         }},
        {"0.4",
         {
             AxisRecord{
                 true,
                 "z",
                 true,
                 "space",
                 "{'name': 'z', 'type': 'space'}",
                 "'space'",
             },
             AxisRecord{
                 true,
                 "y",
                 true,
                 "space",
                 "{'name': 'y', 'type': 'space'}",
                 "'space'",
             },
             AxisRecord{
                 true,
                 "x",
                 true,
                 "space",
                 "{'name': 'x', 'type': 'space'}",
                 "'space'",
             },
         }},
        {"0.5",
         {
             AxisRecord{
                 true,
                 "t",
                 true,
                 "time",
                 "{'name': 't', 'type': 'time'}",
                 "'time'",
             },
             AxisRecord{
                 true,
                 "c",
                 true,
                 "channel",
                 "{'name': 'c', 'type': 'channel'}",
                 "'channel'",
             },
             AxisRecord{
                 true,
                 "y",
                 true,
                 "space",
                 "{'name': 'y', 'type': 'space'}",
                 "'space'",
             },
             AxisRecord{
                 true,
                 "x",
                 true,
                 "space",
                 "{'name': 'x', 'type': 'space'}",
                 "'space'",
             },
         }},
    };

    const auto& [version, input_axes] = kCases[iteration % kCases.size()];
    std::vector<AxisRecord> axes;
    if (input_axes.empty()) {
        axes = axes_to_dicts({
            AxisRecord{true, "t", false, "", "", "None"},
            AxisRecord{true, "c", false, "", "", "None"},
            AxisRecord{true, "z", false, "", "", "None"},
            AxisRecord{true, "y", false, "", "", "None"},
            AxisRecord{true, "x", false, "", "", "None"},
        });
    } else {
        axes = axes_to_dicts(input_axes);
    }

    if (version == "0.3") {
        const auto names = get_names(axes);
        validate_03(names);
        std::uint64_t total = 0U;
        for (const auto& name : names) {
            total += static_cast<std::uint64_t>(name.size());
        }
        return total;
    }
    if (version != "0.1" && version != "0.2") {
        validate_axes_types(axes);
    }

    std::uint64_t total = 0U;
    for (const auto& axis : axes) {
        total += static_cast<std::uint64_t>(axis.name.size());
        if (axis.has_type) {
            total += static_cast<std::uint64_t>(axis.type.size());
        }
    }
    return total;
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

std::uint64_t bench_conversions_int_to_rgba(std::size_t iteration) {
    const auto rgba = int_to_rgba(static_cast<std::int32_t>(iteration * 7919U));
    return static_cast<std::uint64_t>(
        rgba[0] * 255.0 + rgba[1] * 255.0 + rgba[2] * 255.0 + rgba[3] * 255.0);
}

std::uint64_t bench_conversions_rgba_to_int(std::size_t iteration) {
    return static_cast<std::uint64_t>(rgba_to_int(
        static_cast<std::uint8_t>(iteration & 0xFFU),
        static_cast<std::uint8_t>((iteration + 17U) & 0xFFU),
        static_cast<std::uint8_t>((iteration + 37U) & 0xFFU),
        static_cast<std::uint8_t>((iteration + 59U) & 0xFFU)));
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

std::uint64_t bench_csv_parse_value(std::size_t iteration) {
    static const std::vector<std::pair<std::string, std::string>> cases = {
        {"", "s"},
        {"0", "l"},
        {"1.5", "d"},
        {"abc", "s"},
        {"True", "b"},
        {"nan", "d"},
        {"inf", "d"},
        {"-inf", "d"},
    };
    const auto& current = cases.at(iteration % cases.size());
    const auto parsed = parse_csv_value(current.first, current.second);
    if (const auto* text = std::get_if<std::string>(&parsed)) {
        return static_cast<std::uint64_t>(text->size());
    }
    if (const auto* number = std::get_if<double>(&parsed)) {
        if (std::isnan(*number)) {
            return 1U;
        }
        if (std::isinf(*number)) {
            return 2U;
        }
        return static_cast<std::uint64_t>(std::abs(*number));
    }
    if (const auto* integer = std::get_if<std::int64_t>(&parsed)) {
        return static_cast<std::uint64_t>(*integer >= 0 ? *integer : -*integer);
    }
    return std::get<bool>(parsed) ? 1U : 0U;
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

std::uint64_t bench_data_make_circle(std::size_t iteration) {
    const std::array<std::tuple<std::size_t, std::size_t, std::uint16_t>, 3> cases = {{
        {8U, 8U, 1U},
        {9U, 5U, 7U},
        {12U, 16U, 2U},
    }};
    const auto& [height, width, value] = cases.at(iteration % cases.size());
    std::vector<std::uint16_t> target(height * width, 0U);
    const auto points = circle_points(height, width);
    for (const auto& point : points) {
        target[point.y * width + point.x] = value;
    }
    std::uint64_t total = 0U;
    for (const auto entry : target) {
        total += entry;
    }
    return total;
}

std::uint64_t bench_data_rgb_to_5d(std::size_t iteration) {
    if (iteration % 2U == 0U) {
        const std::size_t height = 4U;
        const std::size_t width = 4U;
        std::vector<std::uint16_t> source(height * width);
        for (std::size_t index = 0; index < source.size(); ++index) {
            source[index] = static_cast<std::uint16_t>(index);
        }
        const auto output_shape = std::vector<std::size_t>{1U, 1U, 1U, height, width};
        std::uint64_t checksum = static_cast<std::uint64_t>(output_shape.size());
        for (const auto value : source) {
            checksum += value;
        }
        return checksum;
    }

    const std::size_t height = 4U;
    const std::size_t width = 5U;
    const std::size_t channels = 3U;
    std::vector<std::uint16_t> source(height * width * channels);
    for (std::size_t index = 0; index < source.size(); ++index) {
        source[index] = static_cast<std::uint16_t>(index);
    }
    std::vector<std::uint16_t> target(height * width * channels);
    for (std::size_t channel = 0; channel < channels; ++channel) {
        for (std::size_t y = 0; y < height; ++y) {
            for (std::size_t x = 0; x < width; ++x) {
                const auto source_index = (y * width * channels) + (x * channels) + channel;
                const auto target_index = (channel * height * width) + (y * width) + x;
                target[target_index] = source[source_index];
            }
        }
    }
    std::uint64_t checksum = 0U;
    for (const auto value : target) {
        checksum += value;
    }
    return checksum;
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

std::uint64_t bench_format_dispatch(std::size_t iteration) {
    const std::string version = format_versions()[iteration % format_versions().size()];
    std::uint64_t total = 0U;
    total += static_cast<std::uint64_t>(normalize_known_format_version(version).size());
    total += static_cast<std::uint64_t>(format_zarr_format(version));
    const auto encoding = format_chunk_key_encoding(version);
    total += static_cast<std::uint64_t>(encoding.name.size() + encoding.separator.size());
    total += static_cast<std::uint64_t>(format_class_name(version).size());

    MetadataSummary metadata{};
    switch (iteration % 5U) {
        case 0:
            break;
        case 1:
            metadata.has_multiscales_version = true;
            metadata.multiscales_version_is_string = true;
            metadata.multiscales_version = "0.5";
            break;
        case 2:
            metadata.has_plate_version = true;
            metadata.plate_version_is_string = true;
            metadata.plate_version = "0.4";
            break;
        case 3:
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
    const auto detected = detect_format_version(metadata);
    total += static_cast<std::uint64_t>(detected.has_value() ? detected->size() : 1U);
    return total;
}

std::uint64_t bench_format_v01_init_store(std::size_t iteration) {
    const auto local = format_init_store_plan(
        "/tmp/native-bench-" + std::to_string(iteration % 8U) + ".zarr",
        "w");
    const auto remote = format_init_store_plan(
        "https://example.invalid/image.zarr",
        iteration % 2U == 0 ? "r" : "w");
    std::uint64_t total = 0U;
    total += local.use_fsspec ? 10U : 1U;
    total += local.read_only ? 100U : 10U;
    total += remote.use_fsspec ? 1000U : 100U;
    total += remote.read_only ? 10000U : 1000U;
    return total;
}

std::uint64_t bench_format_well_and_coord(std::size_t iteration) {
    const std::vector<std::string> rows = {"A", "B", "C"};
    const std::vector<std::string> columns = {"1", "2", "3"};
    const std::array<std::string, 2> valid_paths = {"A/1", "B/3"};
    const auto& valid_path = valid_paths[iteration % valid_paths.size()];
    const auto generated = generate_well_v04(valid_path, rows, columns);
    validate_well_v04(generated.path, generated.row_index, generated.column_index, rows, columns);

    const auto coord = generate_coordinate_transformations(
        {
            {256.0, 256.0},
            {128.0, 128.0},
            {64.0, 64.0},
        });
    validate_coordinate_transformations(2, 3, coord);

    return static_cast<std::uint64_t>(
        generated.path.size() +
        static_cast<std::size_t>(generated.row_index) +
        static_cast<std::size_t>(generated.column_index) +
        coord.size() +
        coord.front().front().values.size());
}

std::uint64_t bench_io_subpath(std::size_t iteration) {
    const auto path = io_subpath(
        "http://example.org/data",
        "group/" + std::to_string(iteration % 16U),
        false,
        true);
    return static_cast<std::uint64_t>(path.size());
}

std::uint64_t bench_utils_path_helpers(std::size_t iteration) {
    if (iteration % 3U == 0U) {
        auto parts = std::vector<std::vector<std::string>>{
            {"root", "a", "b"},
            {"root", "a", "c"},
        };
        const auto common = strip_common_prefix(parts);
        return static_cast<std::uint64_t>(common.size() + parts.front().size());
    }
    if (iteration % 3U == 1U) {
        auto parts = std::vector<std::vector<std::string>>{{"only"}};
        const auto common = strip_common_prefix(parts);
        return static_cast<std::uint64_t>(common.size() + parts.front().size());
    }
    auto parts = std::vector<std::vector<std::string>>{{}, {}};
    try {
        static_cast<void>(strip_common_prefix(parts));
    } catch (const std::runtime_error&) {
        return 1U;
    }
    return 0U;
}

std::uint64_t bench_utils_finder(std::size_t iteration) {
    const auto plan = utils_finder_plan(
        "/tmp/demo_" + std::to_string(iteration % 8U) + "/images",
        8012);
    const auto row = utils_finder_row(
        8012,
        plan.server_dir,
        "plate/A/1/0",
        "coins",
        "plate/A/1",
        "2026-04-15T00:00:00");
    return static_cast<std::uint64_t>(
        plan.parent_path.size() +
        plan.server_dir.size() +
        plan.csv_path.size() +
        plan.source_uri.size() +
        plan.url.size() +
        row.file_path.size() +
        row.name.size() +
        row.folders.size() +
        row.uploaded.size());
}

std::uint64_t bench_utils_view(std::size_t iteration) {
    const auto plan = utils_view_plan(
        "/tmp/image_" + std::to_string(iteration % 8U) + ".zarr",
        8013,
        false,
        1);
    return static_cast<std::uint64_t>(
        plan.parent_dir.size() + plan.image_name.size() + plan.url.size());
}

std::uint64_t bench_data_create_plan(std::size_t iteration) {
    const auto plan = create_zarr_plan(
        iteration % 2U == 0U ? "0.5" : "0.4",
        {1, 3, 512, 512},
        {1, 3, 64, 64},
        {});
    return static_cast<std::uint64_t>(
        plan.axes.size() +
        plan.chunks.size() +
        plan.channels.size() +
        plan.labels_axes.size() +
        plan.source_image.size() +
        plan.random_label_count +
        plan.size_c);
}

std::uint64_t bench_local_create_coins(std::size_t iteration) {
    const auto output_root =
        bench_runtime_fixture_root() /
        ("create_coins_" + std::to_string(iteration % 32U) + ".zarr");
    std::error_code error;
    std::filesystem::remove_all(output_root, error);
    local_create_sample(
        output_root.generic_string(),
        "coins",
        "coins",
        "0.5",
        CreateColorMode::native_random,
        std::uint64_t{0});
    const auto metadata_size =
        std::filesystem::file_size(output_root / "zarr.json") +
        std::filesystem::file_size(output_root / "labels" / "coins" / "zarr.json");
    std::filesystem::remove_all(output_root, error);
    return static_cast<std::uint64_t>(metadata_size);
}

std::uint64_t bench_local_create_astronaut(std::size_t iteration) {
    const auto output_root =
        bench_runtime_fixture_root() /
        ("create_astronaut_" + std::to_string(iteration % 32U) + ".zarr");
    std::error_code error;
    std::filesystem::remove_all(output_root, error);
    local_create_sample(
        output_root.generic_string(),
        "astronaut",
        "circles",
        "0.5",
        CreateColorMode::native_random,
        std::uint64_t{0});
    const auto metadata_size =
        std::filesystem::file_size(output_root / "zarr.json") +
        std::filesystem::file_size(output_root / "labels" / "circles" / "zarr.json");
    std::filesystem::remove_all(output_root, error);
    return static_cast<std::uint64_t>(metadata_size);
}

std::uint64_t bench_local_find_multiscales(std::size_t) {
    const auto images = local_find_multiscales(
        (bench_runtime_fixture_root() / "finder_tree" / "image.zarr").string());
    return static_cast<std::uint64_t>(images.images.size());
}

std::uint64_t bench_local_info(std::size_t) {
    const auto lines = local_info_lines(
        (bench_runtime_fixture_root() / "info_v2.zarr").string());
    return static_cast<std::uint64_t>(lines.size());
}

std::uint64_t bench_local_info_stats(std::size_t) {
    const auto lines = local_info_lines(
        (bench_runtime_fixture_root() / "info_v3.zarr").string(),
        true);
    return static_cast<std::uint64_t>(lines.size());
}

std::uint64_t bench_local_finder(std::size_t) {
    const auto result = local_finder_csv(
        (bench_runtime_fixture_root() / "finder_tree").string(),
        8012);
    return static_cast<std::uint64_t>(
        result.rows.size() + result.csv_path.size() + result.source_uri.size() +
        result.app_url.size());
}

std::uint64_t bench_local_view_prepare(std::size_t) {
    const auto preparation = local_view_prepare(
        (bench_runtime_fixture_root() / "finder_tree" / "image.zarr").string(),
        8013,
        false);
    return static_cast<std::uint64_t>(
        preparation.parent_dir.size() +
        preparation.image_name.size() +
        preparation.url.size() +
        (preparation.should_warn ? 1U : 0U));
}

std::uint64_t bench_local_download(std::size_t iteration) {
    const auto output_root =
        bench_runtime_fixture_root() / ("download_out_" + std::to_string(iteration % 32U));
    std::filesystem::remove_all(output_root);
    const auto result = local_download_copy(
        (bench_runtime_fixture_root() / "info_v2.zarr").string(),
        output_root.string());
    const auto copied_root = output_root / "info_v2.zarr";
    const auto exists = std::filesystem::exists(copied_root / ".zattrs") ? 1U : 0U;
    std::filesystem::remove_all(output_root);
    return static_cast<std::uint64_t>(
        result.copied_root.size() + result.listed_paths.size() + exists);
}

std::uint64_t bench_local_csv_to_labels(std::size_t iteration) {
    const auto seed_root = bench_runtime_fixture_root() / "csv_image.zarr";
    const auto working_root =
        bench_runtime_fixture_root() /
        ("csv_image_work_" + std::to_string(iteration % 32U) + ".zarr");
    std::error_code error;
    std::filesystem::remove_all(working_root, error);
    error.clear();
    std::filesystem::copy(
        seed_root,
        working_root,
        std::filesystem::copy_options::recursive,
        error);
    if (error) {
        throw std::runtime_error("Failed to prepare csv_to_labels benchmark fixture");
    }
    const auto result = local_csv_to_labels(
        (bench_runtime_fixture_root() / "csv_props.csv").string(),
        "cell_id",
        "score#d",
        working_root.string(),
        "cell_id");
    return static_cast<std::uint64_t>(
        result.touched_label_groups + result.updated_properties);
}

std::uint64_t bench_local_dict_to_zarr(std::size_t iteration) {
    const auto seed_root = bench_runtime_fixture_root() / "csv_image.zarr";
    const auto working_root =
        bench_runtime_fixture_root() /
        ("dict_image_work_" + std::to_string(iteration % 32U) + ".zarr");
    std::error_code error;
    std::filesystem::remove_all(working_root, error);
    error.clear();
    std::filesystem::copy(
        seed_root,
        working_root,
        std::filesystem::copy_options::recursive,
        error);
    if (error) {
        throw std::runtime_error("Failed to prepare dict_to_zarr benchmark fixture");
    }
    const auto result = local_dict_to_zarr(
        {LocalDictToZarrEntry{
            true,
            "1",
            nlohmann::ordered_json{{"score", 4.5}, {"alive", true}},
        }},
        working_root.string(),
        "cell_id");
    return static_cast<std::uint64_t>(
        result.touched_label_groups + result.updated_properties);
}

std::uint64_t bench_local_scale_nearest(std::size_t iteration) {
    const auto output_root =
        bench_runtime_fixture_root() /
        ("scale_out_" + std::to_string(iteration % 32U) + ".zarr");
    std::error_code error;
    std::filesystem::remove_all(output_root, error);
    const auto result = local_scale_array(
        (bench_runtime_fixture_root() / "scale_input.zarr").string(),
        output_root.string(),
        "yx",
        true,
        "nearest",
        false,
        2,
        2);
    const auto metadata_size =
        std::filesystem::file_size(output_root / "zarr.json") +
        std::filesystem::file_size(output_root / "s0" / "zarr.json") +
        std::filesystem::file_size(output_root / "s1" / "zarr.json") +
        std::filesystem::file_size(output_root / "s2" / "zarr.json");
    std::filesystem::remove_all(output_root, error);
    return static_cast<std::uint64_t>(metadata_size + result.levels.size());
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

std::uint64_t bench_reader_matches(std::size_t iteration) {
    const auto payload =
        reader_probe_matches(iteration % 2U == 0U ? "image" : "hcs").dump();
    return static_cast<std::uint64_t>(payload.size());
}

std::uint64_t bench_reader_node_ops(std::size_t) {
    const auto payload = reader_probe_node_ops("image").dump();
    return static_cast<std::uint64_t>(payload.size());
}

std::uint64_t bench_reader_image_surface(std::size_t) {
    const auto payload = reader_probe_image_surface().dump();
    return static_cast<std::uint64_t>(payload.size());
}

std::uint64_t bench_reader_plate_surface(std::size_t) {
    const auto payload = reader_probe_plate_surface().dump();
    return static_cast<std::uint64_t>(payload.size());
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

void write_json_output(
    const Options& options,
    const std::vector<CaseResult>& results) {
    if (options.json_output.empty()) {
        return;
    }

    std::ofstream output(options.json_output);
    if (!output) {
        throw std::runtime_error(
            "Failed to open JSON output path: " + options.json_output);
    }

    output << "{\n";
    output << "  \"rounds\": " << options.rounds << ",\n";
    output << "  \"iterations\": " << options.iterations << ",\n";
    output << "  \"quick\": " << (options.quick ? "true" : "false") << ",\n";
    output << "  \"match\": \"" << json_escape(options.match) << "\",\n";
    output << "  \"results\": [\n";
    for (std::size_t index = 0; index < results.size(); ++index) {
        const auto& result = results[index];
        const auto median = median_microseconds(result.round_microseconds);
        const auto [min_it, max_it] = std::minmax_element(
            result.round_microseconds.begin(), result.round_microseconds.end());
        output << "    {\n";
        output << "      \"name\": \"" << json_escape(result.name) << "\",\n";
        output << "      \"iterations\": " << result.iterations << ",\n";
        output << "      \"median_us_per_op\": " << std::fixed << std::setprecision(6)
               << median << ",\n";
        output << "      \"best_us_per_op\": " << *min_it << ",\n";
        output << "      \"worst_us_per_op\": " << *max_it << "\n";
        output << "    }";
        if (index + 1 != results.size()) {
            output << ",";
        }
        output << "\n";
    }
    output << "  ]\n";
    output << "}\n";
}

}  // namespace

int main(int argc, char** argv) {
    try {
        const Options options = parse_options(argc, argv);
        std::vector<CaseDefinition> cases = {
            {"axes_validate_types", 1, bench_axes_validate_types},
            {"axes.constructor_batch", 16, bench_axes_constructor},
            {"cli.create_plan", 1, [](std::size_t iteration) {
                 const auto& method = iteration % 2U == 0 ? std::string("coins") : std::string("astronaut");
                 const auto plan = cli_create_plan(method);
                 return static_cast<std::uint64_t>(plan.method_name.size() + plan.label_name.size());
             }},
            {"cli.scale_factors", 1, [](std::size_t iteration) {
                 const auto factors = cli_scale_factors(
                     iteration % 2U == 0 ? 2 : 3,
                     iteration % 3U == 0 ? 4 : 3);
                 std::uint64_t total = 0U;
                 for (const auto factor : factors) {
                     total += static_cast<std::uint64_t>(factor);
                 }
                 return total;
             }},
            {"csv.parse_csv_value", 2048, bench_csv_parse_value},
            {"conversions.int_to_rgba", 2048, bench_conversions_int_to_rgba},
            {"conversions.rgba_to_int", 2048, bench_conversions_rgba_to_int},
            {"conversions_roundtrip", 1, bench_conversions_roundtrip},
            {"csv_props_by_id", 8, bench_csv_props},
            {"data.create_plan", 1, bench_data_create_plan},
            {"dask_zoom_plan", 1, bench_dask_zoom},
            {"data.make_circle_batch", 16, bench_data_make_circle},
            {"data.rgb_to_5d_batch", 16, bench_data_rgb_to_5d},
            {"data_circle_points", 16, bench_data_circle_points},
            {"format.dispatch", 1, bench_format_dispatch},
            {"format.matches", 1, bench_format_matches},
            {"format.v01_init_store", 1, bench_format_v01_init_store},
            {"format.well_and_coord", 2, bench_format_well_and_coord},
            {"io_subpath", 1, bench_io_subpath},
            {"local.create_astronaut", 4096, bench_local_create_astronaut},
            {"local.create_coins", 4096, bench_local_create_coins},
            {"local.csv_to_labels", 1024, bench_local_csv_to_labels},
            {"local.dict_to_zarr", 1024, bench_local_dict_to_zarr},
            {"local.download", 1, bench_local_download},
            {"local.finder", 1, bench_local_finder},
            {"local.find_multiscales", 1, bench_local_find_multiscales},
            {"local.info", 1, bench_local_info},
            {"local.info_stats", 1, bench_local_info_stats},
            {"local.scale_nearest", 32, bench_local_scale_nearest},
            {"local.view_prepare", 1, bench_local_view_prepare},
            {"reader.image_surface", 1, bench_reader_image_surface},
            {"reader.matches", 1, bench_reader_matches},
            {"reader.node_ops", 1, bench_reader_node_ops},
            {"reader.plate_surface", 1, bench_reader_plate_surface},
            {"reader_plate_levels", 4, bench_reader_plate_levels},
            {"scale_build_pyramid", 8, bench_scale_build_pyramid},
            {"utils.finder", 1, bench_utils_finder},
            {"utils.path_helpers", 1, bench_utils_path_helpers},
            {"utils.view", 1, bench_utils_view},
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
        write_json_output(options, results);
        return 0;
    } catch (const std::exception& exc) {
        std::cerr << exc.what() << "\n";
        return 2;
    }
}
