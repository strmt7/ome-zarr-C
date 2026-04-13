#include <pybind11/pybind11.h>

#include "../native/cli.hpp"

namespace py = pybind11;

namespace {

py::dict cli_parser_spec() {
    const auto spec = ome_zarr_c::native_code::cli_parser_spec();

    auto encode_argument = [](const ome_zarr_c::native_code::CliArgumentSpec& arg) {
        py::dict payload;
        payload["positional"] = py::bool_(arg.positional);
        py::list flags;
        for (const auto& flag : arg.flags) {
            flags.append(py::str(flag));
        }
        payload["flags"] = flags;
        payload["help"] = py::str(arg.help);
        payload["action"] = py::str(arg.action);
        payload["type_name"] = py::str(arg.type_name);
        payload["has_default"] = py::bool_(arg.has_default);
        payload["default_value"] = py::str(arg.default_value);
        py::list choices;
        for (const auto& choice : arg.choices) {
            choices.append(py::str(choice));
        }
        payload["choices"] = choices;
        return payload;
    };

    py::dict payload;
    py::list globals;
    for (const auto& arg : spec.global_arguments) {
        globals.append(encode_argument(arg));
    }
    payload["global_arguments"] = globals;

    py::list commands;
    for (const auto& command : spec.commands) {
        py::dict command_payload;
        command_payload["name"] = py::str(command.name);
        command_payload["handler_name"] = py::str(command.handler_name);
        py::list arguments;
        for (const auto& arg : command.arguments) {
            arguments.append(encode_argument(arg));
        }
        command_payload["arguments"] = arguments;
        commands.append(command_payload);
    }
    payload["commands"] = commands;
    return payload;
}

py::int_ cli_resolved_log_level(
    py::int_ base_log_level,
    py::int_ verbose_count,
    py::int_ quiet_count) {
    return py::int_(ome_zarr_c::native_code::cli_resolved_log_level(
        py::cast<std::int64_t>(base_log_level),
        py::cast<std::int64_t>(verbose_count),
        py::cast<std::int64_t>(quiet_count)));
}

py::dict cli_create_plan(const std::string& method_name) {
    const auto plan = ome_zarr_c::native_code::cli_create_plan(method_name);
    py::dict payload;
    payload["method_name"] = py::str(plan.method_name);
    payload["label_name"] = py::str(plan.label_name);
    return payload;
}

py::tuple cli_scale_factors(py::int_ downscale, py::int_ max_layer) {
    const auto factors = ome_zarr_c::native_code::cli_scale_factors(
        py::cast<std::int64_t>(downscale),
        py::cast<std::int64_t>(max_layer));
    py::tuple result(factors.size());
    for (py::size_t index = 0; index < factors.size(); ++index) {
        result[index] = py::int_(factors[index]);
    }
    return result;
}

}  // namespace

void register_cli_bindings(py::module_& m) {
    m.def("cli_parser_spec", &cli_parser_spec);
    m.def(
        "cli_resolved_log_level",
        &cli_resolved_log_level,
        py::arg("base_log_level"),
        py::arg("verbose_count"),
        py::arg("quiet_count"));
    m.def("cli_create_plan", &cli_create_plan, py::arg("method_name"));
    m.def(
        "cli_scale_factors",
        &cli_scale_factors,
        py::arg("downscale"),
        py::arg("max_layer"));
}
