#include "data.hpp"

#include <algorithm>
#include <stdexcept>

namespace ome_zarr_c::native_code {

std::vector<CirclePoint> circle_points(std::size_t height, std::size_t width) {
    const std::size_t cx = width / 2;
    const std::size_t cy = height / 2;
    const std::size_t radius = std::min(width, height) / 2;
    const std::size_t radius_squared = radius * radius;

    std::vector<CirclePoint> points;
    for (std::size_t y = 0; y < height; ++y) {
        for (std::size_t x = 0; x < width; ++x) {
            const auto dx = static_cast<std::int64_t>(x) - static_cast<std::int64_t>(cx);
            const auto dy = static_cast<std::int64_t>(y) - static_cast<std::int64_t>(cy);
            if (dx * dx + dy * dy < static_cast<std::int64_t>(radius_squared)) {
                points.push_back(CirclePoint{y, x});
            }
        }
    }
    return points;
}

std::vector<std::size_t> rgb_to_5d_shape(const std::vector<std::size_t>& shape) {
    if (shape.size() == 2) {
        return {1, 1, shape[0], shape[1]};
    }
    if (shape.size() == 3) {
        return {1, shape[2], shape[0], shape[1]};
    }
    throw std::invalid_argument("rgb_to_5d expects 2D or 3D input");
}

std::vector<std::size_t> rgb_channel_order(const std::vector<std::size_t>& shape) {
    if (shape.size() == 2) {
        return {0};
    }
    if (shape.size() == 3) {
        std::vector<std::size_t> order;
        order.reserve(shape[2]);
        for (std::size_t index = 0; index < shape[2]; ++index) {
            order.push_back(index);
        }
        return order;
    }
    throw std::invalid_argument("rgb_to_5d expects 2D or 3D input");
}

CoinsPlan coins_plan() {
    CoinsPlan plan{};
    plan.crop_margin = 50;
    plan.footprint_rows = 4;
    plan.footprint_cols = 4;
    plan.clear_border_max_size = 20;
    plan.scales = {8, 4, 2, 1};
    plan.image_order = 3;
    plan.label_order = 0;
    return plan;
}

AstronautPlan astronaut_plan() {
    AstronautPlan plan{};
    plan.channel_indices = {0, 1, 2};
    plan.tile_repetitions = {1, 2, 2};
    plan.circles = {
        CircleSpec{100, 100, 200, 200, 1},
        CircleSpec{150, 150, 250, 250, 2},
    };
    return plan;
}

}  // namespace ome_zarr_c::native_code
