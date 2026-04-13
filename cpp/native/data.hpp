#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace ome_zarr_c::native_code {

struct CirclePoint {
    std::size_t y;
    std::size_t x;
};

struct CircleSpec {
    std::size_t height;
    std::size_t width;
    std::size_t offset_y;
    std::size_t offset_x;
    std::int32_t value;
};

struct CoinsPlan {
    std::size_t crop_margin;
    std::size_t footprint_rows;
    std::size_t footprint_cols;
    std::size_t clear_border_max_size;
    std::vector<long> scales;
    std::int32_t image_order;
    std::int32_t label_order;
};

struct AstronautPlan {
    std::vector<std::size_t> channel_indices;
    std::vector<std::size_t> tile_repetitions;
    std::vector<CircleSpec> circles;
};

struct CreateZarrChannelPlan {
    std::string color;
    std::string label;
    bool has_label;
};

struct CreateZarrPlan {
    bool legacy_five_d;
    std::string axes;
    bool axes_is_none;
    std::size_t size_c;
    std::vector<std::size_t> chunks;
    bool color_image;
    std::string channel_model;
    std::vector<CreateZarrChannelPlan> channels;
    std::string labels_axes;
    bool labels_axes_is_none;
    std::size_t random_label_count;
    std::string source_image;
};

std::vector<CirclePoint> circle_points(std::size_t height, std::size_t width);

std::vector<std::size_t> rgb_to_5d_shape(const std::vector<std::size_t>& shape);

std::vector<std::size_t> rgb_channel_order(const std::vector<std::size_t>& shape);

CoinsPlan coins_plan();

AstronautPlan astronaut_plan();

CreateZarrPlan create_zarr_plan(
    const std::string& version,
    const std::vector<std::size_t>& base_shape,
    const std::vector<std::size_t>& smallest_shape,
    const std::vector<std::size_t>& chunks);

}  // namespace ome_zarr_c::native_code
