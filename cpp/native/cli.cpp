#include "cli.hpp"

#include <stdexcept>

namespace ome_zarr_c::native_code {

CliParserSpec cli_parser_spec() {
    CliParserSpec spec{};
    spec.global_arguments = {
        CliArgumentSpec{
            false,
            {"-v", "--verbose"},
            "increase loglevel for each use, e.g. -vvv",
            "count",
            "",
            true,
            "0",
            {},
        },
        CliArgumentSpec{
            false,
            {"-q", "--quiet"},
            "decrease loglevel for each use, e.q. -qqq",
            "count",
            "",
            true,
            "0",
            {},
        },
    };

    spec.commands = {
        CliCommandSpec{
            "info",
            "info",
            {
                CliArgumentSpec{
                    true,
                    {"path"},
                    "Path to image.zarr",
                    "",
                    "",
                    false,
                    "",
                    {},
                },
                CliArgumentSpec{
                    false,
                    {"--stats"},
                    "",
                    "store_true",
                    "",
                    false,
                    "",
                    {},
                },
            },
        },
        CliCommandSpec{
            "download",
            "download",
            {
                CliArgumentSpec{
                    true,
                    {"path"},
                    "",
                    "",
                    "",
                    false,
                    "",
                    {},
                },
                CliArgumentSpec{
                    false,
                    {"--output"},
                    "",
                    "",
                    "",
                    true,
                    ".",
                    {},
                },
            },
        },
        CliCommandSpec{
            "view",
            "view",
            {
                CliArgumentSpec{
                    true,
                    {"path"},
                    "Path to image.zarr to open in ome-ngff-validator",
                    "",
                    "",
                    false,
                    "",
                    {},
                },
                CliArgumentSpec{
                    false,
                    {"--port"},
                    "Port to serve the data (default: 8000)",
                    "",
                    "int",
                    true,
                    "8000",
                    {},
                },
                CliArgumentSpec{
                    false,
                    {"--force", "-f"},
                    "Force open in browser. Don't check for OME-Zarr data first.",
                    "store_true",
                    "",
                    false,
                    "",
                    {},
                },
            },
        },
        CliCommandSpec{
            "finder",
            "finder",
            {
                CliArgumentSpec{
                    true,
                    {"path"},
                    "Directory to open in BioFile Finder",
                    "",
                    "",
                    false,
                    "",
                    {},
                },
                CliArgumentSpec{
                    false,
                    {"--port"},
                    "Port to serve the data (default: 8000)",
                    "",
                    "int",
                    true,
                    "8000",
                    {},
                },
            },
        },
        CliCommandSpec{
            "create",
            "create",
            {
                CliArgumentSpec{
                    false,
                    {"--method"},
                    "",
                    "",
                    "",
                    true,
                    "coins",
                    {"coins", "astronaut"},
                },
                CliArgumentSpec{
                    true,
                    {"path"},
                    "",
                    "",
                    "",
                    false,
                    "",
                    {},
                },
                CliArgumentSpec{
                    false,
                    {"--format"},
                    "OME-Zarr version to create. e.g. '0.4'",
                    "",
                    "",
                    false,
                    "",
                    {},
                },
            },
        },
        CliCommandSpec{
            "scale",
            "scale",
            {
                CliArgumentSpec{
                    true,
                    {"input_array"},
                    "",
                    "",
                    "",
                    false,
                    "",
                    {},
                },
                CliArgumentSpec{
                    true,
                    {"output_directory"},
                    "",
                    "",
                    "",
                    false,
                    "",
                    {},
                },
                CliArgumentSpec{
                    true,
                    {"axes"},
                    "Dimensions of input data, i.e. 'zyx' or 'tczyx'.",
                    "",
                    "str",
                    false,
                    "",
                    {},
                },
                CliArgumentSpec{
                    false,
                    {"--copy-metadata"},
                    "copies the array metadata to the new group",
                    "store_true",
                    "",
                    false,
                    "",
                    {},
                },
                CliArgumentSpec{
                    false,
                    {"--method"},
                    "",
                    "",
                    "",
                    true,
                    "resize",
                    {"nearest", "resize", "laplacian", "local_mean", "zoom"},
                },
                CliArgumentSpec{
                    false,
                    {"--in-place"},
                    "if true, don't write the base array",
                    "store_true",
                    "",
                    false,
                    "",
                    {},
                },
                CliArgumentSpec{
                    false,
                    {"--downscale"},
                    "",
                    "",
                    "int",
                    true,
                    "2",
                    {},
                },
                CliArgumentSpec{
                    false,
                    {"--max_layer"},
                    "",
                    "",
                    "int",
                    true,
                    "4",
                    {},
                },
            },
        },
        CliCommandSpec{
            "csv_to_labels",
            "csv_to_labels",
            {
                CliArgumentSpec{
                    true,
                    {"csv_path"},
                    "path to csv file",
                    "",
                    "",
                    false,
                    "",
                    {},
                },
                CliArgumentSpec{
                    true,
                    {"csv_id"},
                    "csv column name containing ID for identifying label properties to update",
                    "",
                    "",
                    false,
                    "",
                    {},
                },
                CliArgumentSpec{
                    true,
                    {"csv_keys"},
                    "Comma-separated list of columns to read from csv to zarr",
                    "",
                    "",
                    false,
                    "",
                    {},
                },
                CliArgumentSpec{
                    true,
                    {"zarr_path"},
                    "path to local zarr plate or image",
                    "",
                    "",
                    false,
                    "",
                    {},
                },
                CliArgumentSpec{
                    true,
                    {"zarr_id"},
                    "Labels properties key. Values should match csv_id column values",
                    "",
                    "",
                    false,
                    "",
                    {},
                },
            },
        },
    };
    return spec;
}

std::int64_t cli_resolved_log_level(
    const std::int64_t base_log_level,
    const std::int64_t verbose_count,
    const std::int64_t quiet_count) {
    return base_log_level - (10 * verbose_count) + (10 * quiet_count);
}

CliCreatePlan cli_create_plan(const std::string& method_name) {
    if (method_name == "coins") {
        return {"coins", "coins"};
    }
    if (method_name == "astronaut") {
        return {"astronaut", "circles"};
    }
    throw std::invalid_argument("unknown method: " + method_name);
}

std::vector<std::int64_t> cli_scale_factors(
    const std::int64_t downscale,
    const std::int64_t max_layer) {
    std::vector<std::int64_t> factors;
    factors.reserve(static_cast<std::size_t>(max_layer));
    std::int64_t current = downscale;
    for (std::int64_t index = 0; index < max_layer; ++index) {
        factors.push_back(current);
        current *= downscale;
    }
    return factors;
}

}  // namespace ome_zarr_c::native_code
