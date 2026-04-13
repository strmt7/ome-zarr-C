#include "reader.hpp"

#include <algorithm>
#include <cmath>
#include <sstream>
#include <stdexcept>

namespace ome_zarr_c::native_code {

std::vector<std::string> reader_matching_specs(const ReaderSpecFlags& flags) {
    std::vector<std::string> matches;
    if (flags.has_labels) {
        matches.push_back("Labels");
    }
    if (flags.has_image_label) {
        matches.push_back("Label");
    }
    if (flags.has_zgroup && flags.has_multiscales) {
        matches.push_back("Multiscales");
    }
    if (flags.has_omero) {
        matches.push_back("OMERO");
    }
    if (flags.has_plate) {
        matches.push_back("Plate");
    }
    if (flags.has_well) {
        matches.push_back("Well");
    }
    return matches;
}

std::vector<std::string> reader_labels_names(const std::vector<std::string>& labels) {
    return labels;
}

std::string reader_node_repr(const std::string& zarr_repr, bool visible) {
    return visible ? zarr_repr : zarr_repr + " (hidden)";
}

ReaderNodeAddPlan reader_node_add_plan(
    bool already_seen,
    bool plate_labels,
    bool has_explicit_visibility,
    bool explicit_visibility,
    bool current_visibility) {
    ReaderNodeAddPlan plan{};
    plan.should_add = !(already_seen && !plate_labels);
    plan.visibility =
        has_explicit_visibility ? explicit_visibility : current_visibility;
    return plan;
}

ReaderMultiscalesPlan reader_multiscales_plan(
    const std::vector<ReaderMultiscalesDatasetInput>& datasets) {
    ReaderMultiscalesPlan plan{};
    for (const auto& dataset : datasets) {
        plan.paths.push_back(dataset.path);
        plan.any_coordinate_transformations =
            plan.any_coordinate_transformations || dataset.has_coordinate_transformations;
    }
    return plan;
}

ReaderMultiscalesSummary reader_multiscales_summary(
    const ReaderMultiscalesInput& input) {
    const auto plan = reader_multiscales_plan(input.datasets);
    ReaderMultiscalesSummary summary{};
    summary.version = input.has_version ? input.version : "0.1";
    summary.name = input.has_name ? input.name : "";
    summary.paths = plan.paths;
    summary.any_coordinate_transformations = plan.any_coordinate_transformations;
    return summary;
}

ReaderLabelColorPlan reader_label_color_plan(const ReaderLabelColorInput& input) {
    ReaderLabelColorPlan plan{};
    plan.keep = input.label_is_bool || input.label_is_int;
    plan.label_is_bool = input.label_is_bool;
    plan.label_bool = input.label_bool;
    plan.label_int = input.label_int;
    if (input.has_rgba) {
        for (const auto component : input.rgba) {
            plan.rgba.push_back(static_cast<double>(component) / 255.0);
        }
    }
    return plan;
}

ReaderLabelPropertyPlan reader_label_property_plan(
    const ReaderLabelPropertyInput& input) {
    ReaderLabelPropertyPlan plan{};
    plan.keep = input.label_is_bool || input.label_is_int;
    plan.label_is_bool = input.label_is_bool;
    plan.label_bool = input.label_bool;
    plan.label_int = input.label_int;
    return plan;
}

ReaderOmeroChannelPlan reader_omero_channel_plan(
    const std::string& model,
    bool has_color,
    const std::string& color,
    bool has_label,
    const std::string& label,
    bool has_active,
    bool active_truthy,
    bool has_window,
    bool has_window_start,
    bool has_window_end) {
    ReaderOmeroChannelPlan plan{};
    plan.has_color = has_color;
    if (has_color) {
        for (int offset = 0; offset < 3; ++offset) {
            const auto component = std::stoi(color.substr(offset * 2, 2), nullptr, 16);
            plan.rgb[offset] = static_cast<double>(component) / 255.0;
        }
        plan.force_greyscale_rgb = model == "greyscale";
    }
    plan.has_label = has_label;
    plan.label = label;
    if (!has_active) {
        plan.visible_mode = ReaderVisibleMode::default_true;
    } else if (active_truthy) {
        plan.visible_mode = ReaderVisibleMode::node_visible_if_active;
    } else {
        plan.visible_mode = ReaderVisibleMode::keep_raw_active;
    }
    plan.has_complete_window = has_window && has_window_start && has_window_end;
    return plan;
}

ReaderOmeroPlan reader_omero_plan(
    const std::string& model,
    const std::vector<ReaderOmeroChannelInput>& channels) {
    ReaderOmeroPlan plan{};
    for (const auto& channel : channels) {
        plan.channels.push_back(reader_omero_channel_plan(
            model,
            channel.has_color,
            channel.color,
            channel.has_label,
            channel.label,
            channel.has_active,
            channel.active_truthy,
            channel.has_window,
            channel.has_window_start,
            channel.has_window_end));
    }
    return plan;
}

ReaderWellPlan reader_well_plan(const std::vector<std::string>& image_paths) {
    ReaderWellPlan plan{};
    plan.image_paths = image_paths;
    const auto field_count = static_cast<double>(image_paths.size());
    plan.column_count = static_cast<std::size_t>(std::ceil(std::sqrt(field_count)));
    if (plan.column_count == 0) {
        plan.row_count = 0;
    } else {
        plan.row_count = static_cast<std::size_t>(
            std::ceil(field_count / static_cast<double>(plan.column_count)));
    }
    return plan;
}

ReaderPlatePlan reader_plate_plan(
    const std::vector<std::string>& row_names,
    const std::vector<std::string>& col_names,
    const std::vector<std::string>& well_paths) {
    ReaderPlatePlan plan{};
    plan.row_names = row_names;
    plan.col_names = col_names;
    plan.well_paths = well_paths;
    std::sort(plan.well_paths.begin(), plan.well_paths.end());
    plan.row_count = row_names.size();
    plan.column_count = col_names.size();
    return plan;
}

std::string reader_plate_tile_path(
    const std::string& row_name,
    const std::string& col_name,
    const std::string& first_field_path,
    const std::string& dataset_path) {
    return row_name + "/" + col_name + "/" + first_field_path + "/" + dataset_path;
}

}  // namespace ome_zarr_c::native_code
