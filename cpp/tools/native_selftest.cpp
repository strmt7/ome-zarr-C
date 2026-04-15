#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <fstream>
#include <iostream>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "../../third_party/nlohmann/json.hpp"

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
#include "../native/scale.hpp"
#include "../native/scale_runtime.hpp"
#include "../native/utils.hpp"
#include "../native/writer.hpp"

namespace {

using namespace ome_zarr_c::native_code;

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

void write_u16_v2_array_fixture(
    const std::filesystem::path& root,
    const std::vector<std::uint16_t>& values,
    const std::vector<std::int64_t>& shape,
    const std::vector<std::int64_t>& chunk_shape,
    const std::optional<int>& alpha_attr = std::nullopt) {
    std::filesystem::create_directories(root);
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
            const auto chunk_path =
                root / (std::to_string(chunk_y) + "." + std::to_string(chunk_x));
            std::ofstream chunk(chunk_path, std::ios::binary);
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

template <typename Left, typename Right>
void require_eq(const Left& left, const Right& right, std::string_view message) {
    if (!(left == right)) {
        std::ostringstream output;
        output << message;
        throw std::runtime_error(output.str());
    }
}

void require(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

void require_close(double left, double right, double tolerance, std::string_view message) {
    if (std::abs(left - right) > tolerance) {
        throw std::runtime_error(std::string(message));
    }
}

template <typename Value>
void require_vector_eq(
    const std::vector<Value>& left,
    const std::vector<Value>& right,
    std::string_view message) {
    require_eq(left.size(), right.size(), message);
    for (std::size_t index = 0; index < left.size(); ++index) {
        if (!(left[index] == right[index])) {
            throw std::runtime_error(std::string(message));
        }
    }
}

void require_double_vector_close(
    const std::vector<double>& left,
    const std::vector<double>& right,
    double tolerance,
    std::string_view message) {
    require_eq(left.size(), right.size(), message);
    for (std::size_t index = 0; index < left.size(); ++index) {
        require_close(left[index], right[index], tolerance, message);
    }
}

template <typename Exception, typename Callable, typename Check>
void require_throws(Callable&& callable, Check&& check, std::string_view message) {
    try {
        callable();
    } catch (const Exception& exc) {
        check(exc);
        return;
    }
    throw std::runtime_error(std::string(message));
}

template <typename Exception, typename Callable>
void require_throws(Callable&& callable, std::string_view message) {
    require_throws<Exception>(
        std::forward<Callable>(callable),
        [](const Exception&) {},
        message);
}

template <typename T>
const T& require_variant(const CsvValue& value, std::string_view message) {
    if (!std::holds_alternative<T>(value)) {
        throw std::runtime_error(std::string(message));
    }
    return std::get<T>(value);
}

void test_axes() {
    const std::vector<AxisRecord> axes = {
        AxisRecord{true, "x", false, "", "", ""},
        AxisRecord{true, "c", false, "", "", ""},
        AxisRecord{true, "custom", true, "other", "{'name': 'custom'}", "'other'"},
    };

    const auto materialized = axes_to_dicts(axes);
    require_eq(materialized[0].type, std::string("space"), "x axis type materialization");
    require_eq(materialized[1].type, std::string("channel"), "c axis type materialization");
    require_eq(materialized[2].type, std::string("other"), "custom axis preserved");

    require_vector_eq(
        get_names(materialized),
        std::vector<std::string>{"x", "c", "custom"},
        "axis names");

    require_throws<std::invalid_argument>(
        [] {
            get_names({AxisRecord{false, "", false, "", "{'type': 'space'}", "None"}});
        },
        [](const std::invalid_argument& exc) {
            require_eq(
                std::string(exc.what()),
                std::string("Axis Dict {'type': 'space'} has no 'name'"),
                "missing axis name message");
        },
        "missing axis name should fail");

    validate_03({"y", "x"});
    validate_03({"t", "c", "z", "y", "x"});
    require_throws<std::invalid_argument>(
        [] { validate_03({"x", "y"}); },
        "invalid 2D axes should fail");

    validate_axes_types({
        AxisRecord{true, "t", true, "time", "", "'time'"},
        AxisRecord{true, "c", true, "channel", "", "'channel'"},
        AxisRecord{true, "y", true, "space", "", "'space'"},
        AxisRecord{true, "x", true, "space", "", "'space'"},
    });
    require_throws<std::invalid_argument>(
        [] {
            validate_axes_types({
                AxisRecord{true, "y", true, "space", "", "'space'"},
                AxisRecord{true, "c", true, "channel", "", "'channel'"},
            });
        },
        [](const std::invalid_argument& exc) {
            require_eq(
                std::string(exc.what()),
                std::string("'space' axes must come after 'channel'"),
                "channel/space ordering message");
        },
        "invalid axis ordering should fail");
}

void test_cli() {
    const auto spec = cli_parser_spec();
    require_eq(spec.global_arguments.size(), std::size_t{2}, "global argument count");
    require_eq(spec.commands.size(), std::size_t{7}, "command count");
    require_eq(spec.commands.front().name, std::string("info"), "first command");
    require_eq(spec.commands.back().name, std::string("csv_to_labels"), "last command");

    require_eq(cli_resolved_log_level(20, 2, 1), std::int64_t{10}, "log level math");

    const auto coins = cli_create_plan("coins");
    require_eq(coins.label_name, std::string("coins"), "coins label");
    const auto astronaut = cli_create_plan("astronaut");
    require_eq(astronaut.label_name, std::string("circles"), "astronaut label");
    require_vector_eq(
        cli_scale_factors(2, 4),
        std::vector<std::int64_t>{2, 4, 8, 16},
        "cli scale factors");
    require_throws<std::invalid_argument>(
        [] { static_cast<void>(cli_create_plan("unknown")); },
        "unknown create method should fail");
}

void test_conversions_and_csv() {
    require_eq(
        int_to_rgba_255_bytes(0x11223344),
        std::array<std::uint8_t, 4>{0x11, 0x22, 0x33, 0x44},
        "RGBA byte split");
    require_eq(
        rgba_to_int(0x11, 0x22, 0x33, 0x44),
        std::int32_t{0x11223344},
        "RGBA roundtrip");

    const auto rgba = int_to_rgba(0xFF0000FF);
    require_close(rgba[0], 1.0, 1e-12, "red channel");
    require_close(rgba[1], 0.0, 1e-12, "green channel");
    require_close(rgba[2], 0.0, 1e-12, "blue channel");
    require_close(rgba[3], 1.0, 1e-12, "alpha channel");

    const auto specs = parse_csv_key_specs("score#d,count#l,name,flag#b,bad#x");
    require_eq(specs.size(), std::size_t{5}, "csv spec count");
    require_eq(specs[0].type, std::string("d"), "double spec");
    require_eq(specs[1].type, std::string("l"), "int spec");
    require_eq(specs[2].type, std::string("s"), "default string spec");
    require_eq(specs[3].type, std::string("b"), "bool spec");
    require_eq(specs[4].type, std::string("s"), "invalid spec fallback");

    require_close(
        require_variant<double>(parse_csv_value("3.25", "d"), "double variant"),
        3.25,
        1e-12,
        "double parse");
    require_eq(
        require_variant<std::int64_t>(parse_csv_value("4.8", "l"), "int variant"),
        std::int64_t{5},
        "int parse");
    require_eq(
        require_variant<bool>(parse_csv_value("x", "b"), "bool variant"),
        true,
        "bool parse");
    require_eq(
        require_variant<std::string>(parse_csv_value("NaN", "l"), "string fallback"),
        std::string("NaN"),
        "NaN fallback");

    const CsvPropsById by_id = csv_props_by_id(
        {
            {"cell_id", "score", "count", "flag"},
            {"a", "3.5", "2", "1"},
            {"b", "7.0", "4", ""},
        },
        "cell_id",
        parse_csv_key_specs("score#d,count#l,flag#b"));
    require_eq(by_id.size(), std::size_t{2}, "csv rows by id");
    require_close(
        require_variant<double>(by_id.at("a")[0].second, "score value"),
        3.5,
        1e-12,
        "score row a");
    require_eq(
        require_variant<std::int64_t>(by_id.at("b")[1].second, "count value"),
        std::int64_t{4},
        "count row b");
    require_eq(
        require_variant<bool>(by_id.at("b")[2].second, "flag value"),
        false,
        "flag row b");

    require_vector_eq(
        csv_label_paths(true, false, "plate.zarr", {"A/1", "B/2"}),
        std::vector<std::string>{
            "plate.zarr/A/1/0/labels/0",
            "plate.zarr/B/2/0/labels/0",
        },
        "plate label paths");
    require_vector_eq(
        csv_label_paths(false, true, "image.zarr", {}),
        std::vector<std::string>{"image.zarr/labels/0"},
        "image label path");
    require_throws<std::runtime_error>(
        [] { static_cast<void>(csv_label_paths(false, false, "bad.zarr", {})); },
        "invalid csv label path source should fail");
}

void test_dask_scale_and_data() {
    const auto chunk_plan = better_chunksize({64, 64}, {0.5, 0.25});
    require_vector_eq(
        chunk_plan.better_chunks,
        std::vector<std::int64_t>{64, 64},
        "better chunks");
    require_vector_eq(
        chunk_plan.block_output_shape,
        std::vector<std::int64_t>{32, 16},
        "block output");

    const auto resize = resize_plan({100, 200}, {64, 64}, {50, 100});
    require_double_vector_close(resize.factors, {0.5, 0.5}, 1e-12, "resize factors");

    const auto local_mean = local_mean_plan({100, 200}, {64, 64}, {50, 100});
    require_double_vector_close(local_mean.factors, {2.0, 2.0}, 1e-12, "local mean factors");
    require_vector_eq(
        local_mean.int_factors,
        std::vector<std::int64_t>{2, 2},
        "local mean int factors");

    const auto zoom = zoom_plan({99, 201}, {64, 64}, {50, 100});
    require_eq(zoom.resized_output_shape.size(), std::size_t{2}, "zoom resized shape rank");

    require_vector_eq(
        block_output_shape({7, 9}, {0.5, 0.25}),
        std::vector<std::int64_t>{4, 3},
        "block output helper");
    validate_downscale_nearest({8, 8}, {2, 4});
    require_throws<std::invalid_argument>(
        [] { validate_downscale_nearest({8, 8}, {0, 4}); },
        "invalid downscale factors should fail");

    require(scaler_has_method("zoom"), "zoom method present");
    require(!scaler_has_method("missing"), "missing scaler method absent");
    require_vector_eq(
        scaler_resize_image_shape({1, 3, 100, 200}, 2),
        std::vector<std::int64_t>{1, 3, 50, 100},
        "resize image shape");
    require_vector_eq(
        scaler_nearest_plane_shape(100, 200, 4),
        std::vector<std::int64_t>{25, 50},
        "nearest plane shape");
    require_eq(scaler_plane_indices({2, 3, 5, 7}).size(), std::size_t{6}, "plane index count");
    require_vector_eq(
        scaler_stack_shape({2, 3, 10, 20}, {5, 10}),
        std::vector<std::int64_t>{2, 3, 5, 10},
        "stack shape");
    require_vector_eq(
        scaler_local_mean_factors(4, 2),
        std::vector<std::int64_t>{1, 1, 2, 2},
        "local mean factors");
    require_eq(
        scaler_zoom_factors(2, 4),
        std::vector<long>({1, 2, 4, 8}),
        "zoom factors");
    require_vector_eq(
        scaler_group_dataset_paths(3),
        std::vector<std::string>{"base", "1", "2"},
        "dataset paths");

    const auto int_levels = scale_levels_from_ints({"t", "c", "y", "x"}, 2);
    require_double_vector_close(
        int_levels[0].values,
        {1.0, 1.0, 2.0, 2.0},
        1e-12,
        "first scale level");
    const auto reordered = reorder_scale_levels(
        {"t", "c", "y", "x"},
        {
            {{"x", 2.0}, {"y", 2.0}},
            {{"x", 4.0}, {"y", 4.0}},
        });
    require_double_vector_close(
        reordered[1].values,
        {1.0, 1.0, 4.0, 4.0},
        1e-12,
        "reordered scale level");
    const auto pyramid = build_pyramid_plan({1, 1, 100, 200}, {"t", "c", "y", "x"}, int_levels);
    require_vector_eq(
        pyramid[0].target_shape,
        std::vector<std::int64_t>{1, 1, 50, 100},
        "first pyramid level target");

    const auto points = circle_points(3, 3);
    require_eq(points.size(), std::size_t{1}, "circle point count");
    require_eq(points.front().y, std::size_t{1}, "circle point y");
    require_eq(points.front().x, std::size_t{1}, "circle point x");

    require_vector_eq(
        rgb_to_5d_shape({4, 5}),
        std::vector<std::size_t>{1, 1, 4, 5},
        "2D rgb_to_5d");
    require_vector_eq(
        rgb_to_5d_shape({4, 5, 3}),
        std::vector<std::size_t>{1, 3, 4, 5},
        "3D rgb_to_5d");
    require_vector_eq(
        rgb_channel_order({4, 5, 3}),
        std::vector<std::size_t>{0, 1, 2},
        "rgb channel order");
    require_throws<std::invalid_argument>(
        [] { static_cast<void>(rgb_to_5d_shape({1})); },
        "invalid rgb_to_5d shape should fail");

    const auto coins = coins_plan();
    require_eq(coins.crop_margin, std::size_t{50}, "coins crop margin");
    const auto astronaut = astronaut_plan();
    require_eq(astronaut.circles.size(), std::size_t{2}, "astronaut circles");

    const auto create_v04 = create_zarr_plan("0.4", {3, 128, 128}, {3, 16, 16}, {});
    require(!create_v04.legacy_five_d, "v0.4 not legacy");
    require_eq(create_v04.axes, std::string("cyx"), "v0.4 axes");
    require_eq(create_v04.channels.size(), std::size_t{3}, "v0.4 channels");

    const auto create_v01 = create_zarr_plan("0.1", {1, 1, 16, 16, 16}, {1, 1, 8, 8, 8}, {});
    require(create_v01.legacy_five_d, "v0.1 legacy");
    require(create_v01.axes_is_none, "v0.1 axes none");
}

void test_format() {
    require_eq(format_versions().size(), std::size_t{5}, "format version count");
    require_eq(normalize_known_format_version("0.4"), std::string("0.4"), "known version");
    require_throws<std::invalid_argument>(
        [] { static_cast<void>(normalize_known_format_version("9.9")); },
        "unknown format version should fail");

    const MetadataSummary metadata{
        false,
        true,
        true,
        "0.5",
        false,
        false,
        "",
        false,
        false,
        "",
        false,
        false,
        "",
    };
    require_eq(
        detect_format_version(metadata),
        std::optional<std::string>("0.5"),
        "detected format version");
    require(format_matches("0.5", metadata), "format match");
    require_eq(format_zarr_format("0.5"), 3, "zarr format for 0.5");
    require_eq(format_chunk_key_encoding("0.1").separator, std::string("."), "v0.1 separator");
    require_eq(format_class_name("0.5"), std::string("FormatV05"), "class name");
    require(
        format_class_matches("0.4", "ome_zarr.format", "ome_zarr.format", "FormatV04"),
        "class match");

    const auto init_plan = format_init_store_plan("https://example.org/demo.zarr", "r");
    require(init_plan.use_fsspec, "http init uses fsspec");
    require(init_plan.read_only, "read-only init plan");

    const auto generated = generate_coordinate_transformations(
        {{256.0, 256.0}, {128.0, 128.0}, {64.0, 64.0}});
    require_eq(generated.size(), std::size_t{3}, "coordinate transform levels");
    require_close(generated[1][0].values[0], 2.0, 1e-12, "coordinate transform scale");
    validate_coordinate_transformations(2, 3, generated);

    require_throws<CoordinateTransformationsValidationError>(
        [] {
            validate_coordinate_transformations(
                2,
                2,
                std::vector<CoordinateTransformationsValidationInput>{});
        },
        [](const CoordinateTransformationsValidationError& exc) {
            require_eq(
                exc.code(),
                CoordinateTransformationsValidationErrorCode::count_mismatch,
                "coordinate count mismatch code");
        },
        "coordinate count mismatch should fail");

    const auto split = split_well_path_for_validation("B/3");
    require_eq(split.row, std::string_view("B"), "split well row");
    require_eq(split.column, std::string_view("3"), "split well column");

    const auto well = generate_well_v04("B/3", {"A", "B", "C"}, {"1", "2", "3"});
    require_eq(well.row_index, std::int64_t{1}, "well row index");
    require_eq(well.column_index, std::int64_t{2}, "well column index");
    validate_well_v04("B/3", 1, 2, {"A", "B", "C"}, {"1", "2", "3"});
    validate_well_v01({true, true});

    require_throws<WellValidationError>(
        [] { validate_well_v04("B/3", 0, 2, {"A", "B", "C"}, {"1", "2", "3"}); },
        [](const WellValidationError& exc) {
            require_eq(exc.code(), WellValidationErrorCode::row_index_mismatch, "well row mismatch code");
            require_eq(exc.detail(), std::string("B"), "well row mismatch detail");
        },
        "well validation mismatch should fail");
    require_throws<WellGenerationError>(
        [] { static_cast<void>(generate_well_v04("B/3/4", {"A", "B"}, {"1", "2"})); },
        [](const WellGenerationError& exc) {
            require_eq(exc.code(), WellGenerationErrorCode::path_too_many_groups, "well generation error code");
        },
        "well generation group count should fail");
}

void test_io_and_utils() {
    const auto constructor_plan = io_constructor_plan(IoPathKind::fsspec_store, "s3://bucket/demo.zarr");
    require(constructor_plan.use_input_store, "fsspec store should use input store");
    require_eq(constructor_plan.normalized_path, std::string("s3://bucket/demo.zarr"), "normalized path");

    const auto metadata_plan = io_metadata_plan(false, true, false);
    require(metadata_plan.create_group, "write mode creates group");
    require(metadata_plan.exists, "write mode exists");
    require(!metadata_plan.unwrap_ome_namespace, "no ome unwrap");

    require_eq(io_basename("demo/image.zarr/"), std::string("image.zarr"), "basename");
    require_vector_eq(io_parts("demo/image.zarr", false), std::vector<std::string>{"demo", "image.zarr"}, "io parts");
    require_eq(io_subpath("http://example.org/root", "s0", false, true), std::string("http://example.org/root/s0"), "http subpath");
    require(io_subpath("demo/image.zarr", "s0", true, false).ends_with("demo/image.zarr/s0"), "file subpath suffix");
    require_eq(io_repr("plate", true, false), std::string("plate [zgroup]"), "io repr");
    require(io_is_local_store(IoPathKind::local_store), "local store detection");
    require(io_parse_url_returns_none("r", false), "read missing url returns none");
    require(io_protocol_is_http({"s3", "https"}), "http protocol detection");

    auto parts = std::vector<std::vector<std::string>>{{"root", "a"}, {"root", "b"}};
    require_eq(strip_common_prefix(parts), std::string("root"), "common prefix");
    require_vector_eq(parts[0], std::vector<std::string>{"root", "a"}, "prefix-stripped first path");
    require_throws<std::runtime_error>(
        [] {
            auto none = std::vector<std::vector<std::string>>{{"a"}, {"b"}};
            static_cast<void>(strip_common_prefix(none));
        },
        "missing common prefix should fail");

    require_vector_eq(splitall("/tmp/demo"), std::vector<std::string>{"/", "tmp", "demo"}, "splitall");
    require_eq(
        utils_missing_metadata_message(),
        std::string("No .zattrs or zarr.json found in {path_to_zattrs}"),
        "missing metadata message");

    const auto plate_images = utils_plate_images("plate.zarr", "plate", "dir", {"A/1", "B/2"});
    require_eq(plate_images.size(), std::size_t{1}, "plate image count");
    require_eq(plate_images[0].path, std::string("plate.zarr/A/1/0"), "plate image path");

    const auto bio_images = utils_bioformats_images(
        "bio.zarr",
        "demo",
        "dir",
        {std::optional<std::string>("Series 0"), std::nullopt});
    require_eq(bio_images[1].name, std::string("demo Series:1"), "fallback series name");

    const auto header = utils_info_header_lines("Node('x')", "0.5", {"Multiscales", "OMERO"});
    require_eq(header.size(), std::size_t{6}, "info header line count");
    require_eq(utils_info_data_line("(1, 2, 3)", std::optional<std::string>("(0, 1)")), std::string("   - (1, 2, 3) minmax=(0, 1)"), "info data line");

    const auto download_plan = utils_download_plan({{"root", "a"}, {"root", "b"}});
    require_eq(download_plan.common, std::string("root"), "download plan common");
    const auto node_plan = utils_download_node_plan(3, true);
    require(node_plan.wrap_ome_metadata, "zarr v3 wraps ome metadata");
    require(node_plan.use_dimension_names, "zarr v3 uses dimension names");

    const auto warning_view = utils_view_plan("/tmp/image.zarr", 8000, false, 0);
    require(warning_view.should_warn, "missing view images should warn");
    const auto ok_view = utils_view_plan("/tmp/image.zarr", 8000, false, 1);
    require_eq(ok_view.parent_dir, std::string("/tmp"), "view parent dir");
    require(ok_view.url.find("localhost:8000") != std::string::npos, "view url");

    const auto finder_plan = utils_finder_plan("/tmp/data/image.zarr", 8001);
    require_eq(finder_plan.parent_path, std::string("/tmp/data"), "finder parent path");
    require_eq(finder_plan.server_dir, std::string("image.zarr"), "finder server dir");
    const auto finder_row = utils_finder_row(8001, "image.zarr", "A/1/0", "field", "A/1", "today");
    require(finder_row.file_path.find("localhost:8001") != std::string::npos, "finder row file path");
    require_eq(finder_row.folders, std::string("A,1"), "finder row folders");

    const auto fixture_root =
        std::filesystem::temp_directory_path() / "ome_zarr_native_selftest_runtime";
    std::filesystem::remove_all(fixture_root);
    std::filesystem::create_directories(fixture_root / "image.zarr" / "0");
    {
        std::ofstream zattrs(fixture_root / "image.zarr" / ".zattrs");
        zattrs << R"({"multiscales":[{"version":"0.4","axes":["y","x"],"datasets":[{"path":"0"}]}]})";
    }
    {
        std::ofstream zgroup(fixture_root / "image.zarr" / ".zgroup");
        zgroup << R"({"zarr_format":2})";
    }
    {
        std::ofstream array_json(fixture_root / "image.zarr" / "0" / "zarr.json");
        array_json << R"({"shape":[2,2],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[2,2]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"attributes":{},"zarr_format":3,"node_type":"array","storage_transformers":[]})";
    }
    std::filesystem::create_directories(fixture_root / "image-v3.zarr" / "s0" / "c" / "0");
    {
        std::ofstream zarr_json(fixture_root / "image-v3.zarr" / "zarr.json");
        zarr_json << R"({"attributes":{"ome":{"version":"0.5","multiscales":[{"axes":[{"name":"y","type":"space"},{"name":"x","type":"space"}],"datasets":[{"path":"s0","coordinateTransformations":[{"type":"scale","scale":[1,1]}]}]}]}},"zarr_format":3,"node_type":"group"})";
    }
    {
        std::ofstream array_json(fixture_root / "image-v3.zarr" / "s0" / "zarr.json");
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
        std::ofstream chunk(
            fixture_root / "image-v3.zarr" / "s0" / "c" / "0" / "0",
            std::ios::binary);
        chunk.write(compressed.data(), static_cast<std::streamsize>(compressed.size()));
    }

    std::filesystem::create_directories(
        fixture_root / "finder-root" / "plate.zarr" / "A" / "1" / "0");
    {
        std::ofstream zattrs(fixture_root / "finder-root" / "plate.zarr" / ".zattrs");
        zattrs << R"({"plate":{"wells":[{"path":"A/1"}]}})";
    }

    const auto direct_images =
        local_find_multiscales((fixture_root / "image.zarr").generic_string());
    require(!direct_images.metadata_missing, "local direct image metadata found");
    require_eq(direct_images.images.size(), std::size_t{1}, "local direct image count");
    require_eq(
        direct_images.images.front().name,
        std::string("image.zarr"),
        "local direct image name");

    const auto walked_images =
        local_walk_ome_zarr((fixture_root / "finder-root").generic_string());
    require_eq(walked_images.size(), std::size_t{1}, "local walked image count");
    require_eq(
        walked_images.front().path,
        (fixture_root / "finder-root" / "plate.zarr" / "A" / "1" / "0").generic_string(),
        "local walked image path");

    const auto native_info = local_info_lines((fixture_root / "image.zarr").generic_string());
    require_eq(native_info.size(), std::size_t{6}, "native local info line count");
    require_eq(
        native_info.front(),
        (fixture_root / "image.zarr").generic_string() + " [zgroup]",
        "native local info repr");
    require_eq(native_info.back(), std::string("   - (2, 2)"), "native local info shape");

    const auto native_info_stats = local_info_lines(
        (fixture_root / "image-v3.zarr").generic_string(),
        true);
    require_eq(
        native_info_stats.back(),
        std::string("   - (2, 2) minmax=(np.int32(5), np.int32(8))"),
        "native local info stats");

    const auto finder_result =
        local_finder_csv((fixture_root / "finder-root").generic_string(), 8012);
    require(finder_result.found_any, "local finder should discover images");
    require_eq(finder_result.rows.size(), std::size_t{1}, "local finder row count");
    require(
        finder_result.rows.front().file_path.find("http://localhost:8012") !=
            std::string::npos,
        "local finder row url");
    require(
        std::filesystem::exists(finder_result.csv_path),
        "local finder should write csv");
    {
        std::ifstream csv(finder_result.csv_path);
        std::ostringstream buffer;
        buffer << csv.rdbuf();
        const auto text = buffer.str();
        require(
            text.find("File Path,File Name,Folders,Uploaded") != std::string::npos,
            "local finder csv header");
        require(
            text.find("http://localhost:8012") != std::string::npos,
            "local finder csv body");
    }

    const auto warning_preparation =
        local_view_prepare((fixture_root / "missing-image.zarr").generic_string(), 8013, false);
    require(warning_preparation.should_warn, "local view should warn without metadata");
    const auto ok_preparation =
        local_view_prepare((fixture_root / "image.zarr").generic_string(), 8013, false);
    require(!ok_preparation.should_warn, "local view should prepare valid image");
    require_eq(
        ok_preparation.parent_dir,
        fixture_root.generic_string(),
        "local view parent directory");
    require_eq(
        ok_preparation.image_name,
        std::string("image.zarr"),
        "local view image name");
    require(
        ok_preparation.url.find("http://localhost:8013/image.zarr") != std::string::npos,
        "local view validator url");

    const auto download_output = fixture_root / "downloads";
    const auto download_result = local_download_copy(
        (fixture_root / "image.zarr").generic_string(),
        download_output.generic_string());
    require_eq(
        download_result.listed_paths,
        std::vector<std::string>{"image.zarr"},
        "local download listed paths");
    require(
        std::filesystem::exists(download_output / "image.zarr" / ".zattrs"),
        "local download copied metadata");
    require(
        std::filesystem::exists(download_output / "image.zarr" / "0" / "zarr.json") ||
            std::filesystem::exists(download_output / "image.zarr" / "0" / ".zarray"),
        "local download copied array metadata");

    const auto download_v3_output = fixture_root / "downloads-v3";
    static_cast<void>(local_download_copy(
        (fixture_root / "image-v3.zarr").generic_string(),
        download_v3_output.generic_string()));
    require(
        std::filesystem::exists(download_v3_output / "image-v3.zarr" / "zarr.json"),
        "local download v3 copied group metadata");
    require(
        std::filesystem::exists(download_v3_output / "image-v3.zarr" / "s0" / "c" / "0" / "0"),
        "local download v3 copied chunks");

    const auto create_root = fixture_root / "native-create-v05.zarr";
    local_create_sample(
        create_root.generic_string(),
        "coins",
        "coins",
        "0.5",
        CreateColorMode::keep_asset_seed);
    require(
        std::filesystem::exists(create_root / "zarr.json"),
        "local create wrote root metadata");
    require(
        std::filesystem::exists(create_root / "labels" / "coins" / "zarr.json"),
        "local create wrote label metadata");
    {
        std::ifstream metadata(create_root / "labels" / "coins" / "zarr.json");
        std::ostringstream buffer;
        buffer << metadata.rdbuf();
        const auto text = buffer.str();
        require(
            text.find("\"name\": \"/labels/coins\"") != std::string::npos,
            "local create multiscales name");
        require(
            text.find("\"label-value\": 8") != std::string::npos,
            "local create colors preserved");
    }

    const auto scale_input = fixture_root / "scale-input.zarr";
    std::vector<std::uint16_t> scale_values(64);
    for (std::size_t index = 0; index < scale_values.size(); ++index) {
        scale_values[index] = static_cast<std::uint16_t>(index);
    }
    write_u16_v2_array_fixture(scale_input, scale_values, {8, 8}, {2, 2}, 1);
    const auto scale_output = fixture_root / "scale-output.zarr";
    const auto scale_result = local_scale_array(
        scale_input.generic_string(),
        scale_output.generic_string(),
        "yx",
        true,
        "nearest",
        false,
        2,
        2);
    require_eq(
        scale_result.copied_metadata_keys,
        std::vector<std::string>{"alpha"},
        "local scale copied metadata keys");
    require_eq(scale_result.levels.size(), std::size_t{3}, "local scale level count");
    require_eq(
        scale_result.levels[1].shape,
        std::vector<std::int64_t>{4, 4},
        "local scale level 1 shape");
    require(
        std::filesystem::exists(scale_output / "s2" / "c" / "0" / "0"),
        "local scale wrote level-2 chunk");
    {
        std::ifstream metadata(scale_output / "zarr.json");
        std::ostringstream buffer;
        buffer << metadata.rdbuf();
        const auto text = buffer.str();
        require(
            text.find("\"version\": \"0.5\"") != std::string::npos,
            "local scale root version");
        require(
            text.find("\"name\": \"/\"") != std::string::npos,
            "local scale multiscales name");
        require(
            text.find("\"alpha\": 1") != std::string::npos,
            "local scale copied root attrs");
    }
    require_throws<std::invalid_argument>(
        [&]() {
            static_cast<void>(local_scale_array(
                scale_input.generic_string(),
                (fixture_root / "scale-bad-output.zarr").generic_string(),
                "yx",
                false,
                "laplacian",
                false,
                2,
                2));
        },
        [](const std::invalid_argument& exc) {
            require_eq(
                std::string(exc.what()),
                std::string("'laplacian' is not a valid Methods"),
                "local scale invalid-method message");
        },
        "local scale invalid method should fail");

    const auto csv_runtime_root = fixture_root / "csv-image.zarr";
    std::filesystem::create_directories(csv_runtime_root / "labels" / "0");
    {
        std::ofstream zarr_json(csv_runtime_root / "zarr.json");
        zarr_json << R"({"attributes":{"multiscales":[{"version":"0.4"}]},"zarr_format":3,"node_type":"group"})";
    }
    {
        std::ofstream zarr_json(csv_runtime_root / "labels" / "0" / "zarr.json");
        zarr_json << R"({"attributes":{"image-label":{"properties":[{"cell_id":1}]}},"zarr_format":3,"node_type":"group"})";
    }
    const auto csv_runtime_path = fixture_root / "csv-props.csv";
    {
        std::ofstream csv_file(csv_runtime_path);
        csv_file << "cell_id,score,alive\n";
        csv_file << "1,4.5,1\n";
    }
    const auto csv_result = local_csv_to_labels(
        csv_runtime_path.generic_string(),
        "cell_id",
        "score#d,alive#b",
        csv_runtime_root.generic_string(),
        "cell_id");
    require_eq(csv_result.touched_label_groups, std::size_t{1}, "local csv touched label groups");
    require_eq(csv_result.updated_properties, std::size_t{1}, "local csv updated properties");
    {
        std::ifstream metadata(csv_runtime_root / "labels" / "0" / "zarr.json");
        std::ostringstream buffer;
        buffer << metadata.rdbuf();
        const auto text = buffer.str();
        require(text.find("\"score\": 4.5") != std::string::npos, "local csv wrote numeric property");
        require(text.find("\"alive\": true") != std::string::npos, "local csv wrote boolean property");
    }
    require_throws<std::invalid_argument>(
        [&]() {
            static_cast<void>(local_csv_to_labels(
                csv_runtime_path.generic_string(),
                "missing_id",
                "score#d",
                csv_runtime_root.generic_string(),
                "cell_id"));
        },
        [](const std::invalid_argument& exc) {
            require(
                std::string(exc.what()).find("missing_id") != std::string::npos,
                "local csv missing-id message");
        },
        "local csv missing id should fail");

    const auto dict_runtime_root = fixture_root / "dict-image.zarr";
    std::filesystem::create_directories(dict_runtime_root / "labels" / "0");
    {
        std::ofstream zarr_json(dict_runtime_root / "zarr.json");
        zarr_json << R"({"attributes":{"multiscales":[{"version":"0.4"}]},"zarr_format":3,"node_type":"group"})";
    }
    {
        std::ofstream zarr_json(dict_runtime_root / "labels" / "0" / "zarr.json");
        zarr_json << R"({"attributes":{"image-label":{"properties":[{"cell_id":1,"seed":7}]}},"zarr_format":3,"node_type":"group"})";
    }
    const auto dict_result = local_dict_to_zarr(
        {LocalDictToZarrEntry{
            true,
            "1",
            nlohmann::ordered_json{{"score", 9.5}, {"alive", true}},
        }},
        dict_runtime_root.generic_string(),
        "cell_id");
    require_eq(
        dict_result.touched_label_groups,
        std::size_t{1},
        "local dict touched label groups");
    require_eq(
        dict_result.updated_properties,
        std::size_t{1},
        "local dict updated properties");
    {
        std::ifstream metadata(dict_runtime_root / "labels" / "0" / "zarr.json");
        std::ostringstream buffer;
        buffer << metadata.rdbuf();
        const auto text = buffer.str();
        require(
            text.find("\"score\": 9.5") != std::string::npos,
            "local dict wrote numeric property");
        require(
            text.find("\"alive\": true") != std::string::npos,
            "local dict wrote boolean property");
    }
    require_throws<std::runtime_error>(
        [&]() {
            static_cast<void>(local_dict_to_zarr(
                {LocalDictToZarrEntry{
                    true,
                    "1",
                    nlohmann::ordered_json{{"score", 1}},
                }},
                (fixture_root / "dict-bad.zarr").generic_string(),
                "cell_id"));
        },
        [](const std::runtime_error& exc) {
            require_eq(
                std::string(exc.what()),
                std::string("zarr_path must be to plate.zarr or image.zarr"),
                "local dict invalid-root message");
        },
        "local dict invalid root should fail");
}

void test_reader_and_writer() {
    const auto specs = reader_matching_specs({true, false, true, true, true, false, true});
    require_vector_eq(
        specs,
        std::vector<std::string>{"Labels", "Multiscales", "OMERO", "Well"},
        "reader matching specs");
    require_eq(reader_node_repr("Node('/demo')", false), std::string("Node('/demo') (hidden)"), "reader hidden repr");
    require(reader_should_write_metadata(1), "reader writes metadata");
    require_eq(reader_multiscales_array_path("2"), std::string("2"), "multiscales array path");
    require_eq(reader_primary_level_index(), std::size_t{0}, "primary level index");

    const auto add_plan = reader_node_add_plan(true, false, true, false, true);
    require(!add_plan.should_add, "already seen node should not add");
    require(!add_plan.visibility, "explicit visibility false");

    const auto multiscales = reader_multiscales_summary(
        ReaderMultiscalesInput{
            false,
            "",
            false,
            "",
            {
                {"0", false},
                {"1", true},
            },
        });
    require_eq(multiscales.version, std::string("0.1"), "default multiscales version");
    require(multiscales.any_coordinate_transformations, "coordinate transformations flag");

    const auto label_color = reader_label_color_plan({true, false, false, 0, true, {255, 128, 0, 255}});
    require(label_color.keep, "keep label color");
    require_close(label_color.rgba[1], 128.0 / 255.0, 1e-12, "label color conversion");

    const auto label_property = reader_label_property_plan({false, false, true, 7});
    require(label_property.keep, "keep label property");
    require_eq(label_property.label_int, std::int64_t{7}, "label property int");

    const auto omero = reader_omero_plan(
        "greyscale",
        {
            ReaderOmeroChannelInput{true, "FFAA00", true, "Channel 1", true, true, true, true, true},
            ReaderOmeroChannelInput{false, "", false, "", false, false, false, false, false},
        });
    require_eq(omero.channels.size(), std::size_t{2}, "omero channel count");
    require(omero.channels[0].force_greyscale_rgb, "greyscale color enforcement");
    require_eq(
        omero.channels[0].visible_mode,
        ReaderVisibleMode::node_visible_if_active,
        "omero visible mode");

    const auto well_plan = reader_well_plan({"A/1/0", "A/1/1", "A/1/2"});
    require_eq(well_plan.column_count, std::size_t{2}, "well column count");
    require_eq(well_plan.row_count, std::size_t{2}, "well row count");
    const auto well_levels = reader_well_level_plans({"A/1/0", "A/1/1", "A/1/2"}, {"0", "1"}, 2, 2);
    require_eq(well_levels.size(), std::size_t{2}, "well level count");
    require_eq(well_levels[0].tile_paths[3], std::string(""), "missing well tile");

    const auto plate_plan = reader_plate_plan({"B", "A"}, {"2", "1"}, {"B/2", "A/1"});
    require_eq(plate_plan.well_paths[0], std::string("A/1"), "sorted well paths");
    const auto plate_levels = reader_plate_level_plans({"A", "B"}, {"1", "2"}, {"A/1"}, "0", {"0", "1"});
    require_eq(plate_levels[0].tile_paths[0], std::string("A/1/0/0"), "plate tile path");
    require_eq(plate_levels[0].tile_paths[1], std::string(""), "missing plate tile");
    require_eq(
        reader_plate_tile_path("B", "2", "0", "1"),
        std::string("B/2/0/1"),
        "reader plate tile path helper");

    const auto valid_axes = get_valid_axes_plan("0.4", false, std::int64_t{2});
    require(valid_axes.log_auto_axes, "auto axes logging");
    require_vector_eq(valid_axes.axes, std::vector<std::string>{"y", "x"}, "auto axes");
    const auto legacy_axes = get_valid_axes_plan("0.1", true, std::int64_t{5});
    require(legacy_axes.return_none, "legacy axes return none");
    require(legacy_axes.log_ignored_axes, "legacy axes logs ignored");

    validate_axes_length(2, 2);
    require_throws<std::invalid_argument>(
        [] { validate_axes_length(3, 2); },
        "axes length mismatch should fail");

    require_vector_eq(
        extract_dims_from_axes({
            AxisRecord{true, "t", true, "time", "", "'time'"},
            AxisRecord{true, "y", true, "space", "", "'space'"},
            AxisRecord{true, "x", true, "space", "", "'space'"},
        }),
        std::vector<std::string>{"t", "y", "x"},
        "extract dims");
    require_vector_eq(retuple_prefix(5, 3), std::vector<std::size_t>{0, 1}, "retuple prefix");

    const auto string_image = validate_well_image(
        WellImageInput{true, false, "'A/1/0'", true, true, "A/1/0", false, false, false});
    require(string_image.materialize, "string well image materialized");
    const auto dict_image = validate_well_image(
        WellImageInput{false, true, "{'path': 'A/1/0'}", true, true, "A/1/0", true, true, true});
    require(!dict_image.materialize, "dict well image stays dict");
    require(dict_image.has_unexpected_key, "unexpected key flag propagated");
    require_throws<std::invalid_argument>(
        [] {
            validate_plate_acquisition(
                PlateAcquisitionInput{true, "{'id': 'bad'}", true, false, false});
        },
        "invalid acquisition should fail");
    validate_plate_acquisition(PlateAcquisitionInput{true, "{'id': 1}", true, true, false});

    require_vector_eq(
        validate_plate_rows_columns({"A", "B2"}, "rows"),
        std::vector<std::string>{"A", "B2"},
        "valid plate rows");
    require_throws<std::invalid_argument>(
        [] { static_cast<void>(validate_plate_rows_columns({"A", "A"}, "rows")); },
        "duplicate rows should fail");

    require_throws<DatasetValidationError>(
        [] { validate_datasets({}); },
        [](const DatasetValidationError& exc) {
            require_eq(exc.code(), DatasetValidationErrorCode::empty_datasets, "empty dataset code");
        },
        "empty datasets should fail");
    validate_datasets({
        DatasetInput{true, true, false},
        DatasetInput{true, true, true},
    });

    const auto resolved = resolve_writer_format(2, std::nullopt);
    require_eq(resolved.resolved_version, std::string("0.4"), "resolved writer version");
    require_throws<std::invalid_argument>(
        [] { static_cast<void>(resolve_writer_format(2, std::optional<std::string>("0.5"))); },
        "mismatched writer version should fail");

    require(writer_uses_legacy_root_attrs("0.4"), "legacy root attrs");
    require(!writer_uses_legacy_root_attrs("0.5"), "v0.5 root attrs");
    const auto multiscales_plan = writer_multiscales_metadata_plan("0.5", "main");
    require(multiscales_plan.write_root_version, "multiscales writes root version");
    const auto plate_metadata = writer_plate_metadata_plan("0.4");
    require(plate_metadata.embed_plate_version, "plate embeds version");
    const auto well_metadata = writer_well_metadata_plan("0.5");
    require(well_metadata.write_root_version, "well writes root version");
    const auto label_metadata = writer_label_metadata_plan("0.4");
    require(label_metadata.legacy_root_attrs, "label legacy attrs");

    const auto pyramid_plan = writer_pyramid_plan(
        {{10, 20}, {5, 10}},
        3,
        {"y", "x"},
        {{2, 2}, {}});
    require_eq(pyramid_plan.levels[0].component, std::string("s0"), "pyramid level name");
    require(pyramid_plan.levels[0].has_chunks, "first pyramid level chunks");
    require(!pyramid_plan.levels[1].has_chunks, "second pyramid level chunks");

    const auto storage_plan = writer_storage_options_plan(true, false);
    require(storage_plan.return_copy, "storage options copy");
    const auto blosc = writer_blosc_plan();
    require_eq(blosc.cname, std::string("zstd"), "blosc cname");

    const auto labels_plan = writer_labels_plan({"t", "y", "x"}, true, true, 4, std::nullopt);
    require(labels_plan.warn_scaler_deprecated, "labels scaler warning");
    require_eq(labels_plan.scale_factors.size(), std::size_t{4}, "labels scale factor count");

    const auto image_plan = writer_image_plan(
        {"c", "y", "x"},
        true,
        3,
        "laplacian",
        std::nullopt);
    require_eq(image_plan.resolved_method, std::string("resize"), "laplacian fallback");
    require(image_plan.warn_laplacian_fallback, "laplacian fallback warning");
}

struct NamedTest {
    const char* name;
    void (*run)();
};

}  // namespace

int main() {
    const std::vector<NamedTest> tests = {
        {"axes", &test_axes},
        {"cli", &test_cli},
        {"conversions_and_csv", &test_conversions_and_csv},
        {"dask_scale_and_data", &test_dask_scale_and_data},
        {"format", &test_format},
        {"io_and_utils", &test_io_and_utils},
        {"reader_and_writer", &test_reader_and_writer},
    };

    std::size_t passed = 0;
    for (const auto& test : tests) {
        try {
            test.run();
            std::cout << "[PASS] " << test.name << "\n";
            passed += 1;
        } catch (const std::exception& exc) {
            std::cerr << "[FAIL] " << test.name << ": " << exc.what() << "\n";
            return 1;
        }
    }

    std::cout << "All native self-tests passed (" << passed << ").\n";
    return 0;
}
