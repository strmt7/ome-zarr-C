#pragma once

#include <array>
#include <string>
#include <vector>

namespace ome_zarr_c::native_code {

struct ReaderSpecFlags {
    bool has_labels;
    bool has_image_label;
    bool has_zgroup;
    bool has_multiscales;
    bool has_omero;
    bool has_plate;
    bool has_well;
};

std::vector<std::string> reader_matching_specs(const ReaderSpecFlags& flags);

std::string reader_node_repr(const std::string& zarr_repr, bool visible);

struct ReaderMultiscalesDatasetInput {
    std::string path;
    bool has_coordinate_transformations;
};

struct ReaderMultiscalesPlan {
    std::vector<std::string> paths;
    bool any_coordinate_transformations;
};

ReaderMultiscalesPlan reader_multiscales_plan(
    const std::vector<ReaderMultiscalesDatasetInput>& datasets);

enum class ReaderVisibleMode {
    default_true,
    node_visible_if_active,
    keep_raw_active,
};

struct ReaderOmeroChannelPlan {
    bool has_color;
    std::array<double, 3> rgb;
    bool force_greyscale_rgb;
    bool has_label;
    std::string label;
    ReaderVisibleMode visible_mode;
    bool has_complete_window;
};

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
    bool has_window_end);

}  // namespace ome_zarr_c::native_code
