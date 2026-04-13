#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace ome_zarr_c::native_code {

struct CliArgumentSpec {
    bool positional;
    std::vector<std::string> flags;
    std::string help;
    std::string action;
    std::string type_name;
    bool has_default;
    std::string default_value;
    std::vector<std::string> choices;
};

struct CliCommandSpec {
    std::string name;
    std::string handler_name;
    std::vector<CliArgumentSpec> arguments;
};

struct CliParserSpec {
    std::vector<CliArgumentSpec> global_arguments;
    std::vector<CliCommandSpec> commands;
};

struct CliCreatePlan {
    std::string method_name;
    std::string label_name;
};

CliParserSpec cli_parser_spec();

std::int64_t cli_resolved_log_level(
    std::int64_t base_log_level,
    std::int64_t verbose_count,
    std::int64_t quiet_count);

CliCreatePlan cli_create_plan(const std::string& method_name);

std::vector<std::int64_t> cli_scale_factors(
    std::int64_t downscale,
    std::int64_t max_layer);

}  // namespace ome_zarr_c::native_code
