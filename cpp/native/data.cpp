#include "data.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>

namespace ome_zarr_c::native_code {

std::vector<CirclePoint> circle_points(std::size_t height, std::size_t width) {
    const std::size_t cx = width / 2;
    const std::size_t cy = height / 2;
    const std::size_t radius = std::min(width, height) / 2;
    const std::size_t radius_squared = radius * radius;
    const auto signed_cx = static_cast<std::int64_t>(cx);
    const auto signed_cy = static_cast<std::int64_t>(cy);
    const auto signed_radius_squared = static_cast<std::int64_t>(radius_squared);

    std::vector<CirclePoint> points;
    if (height != 0 && width <= std::numeric_limits<std::size_t>::max() / height) {
        const std::size_t total_pixels = height * width;
        std::size_t circle_bound = radius_squared;
        if (circle_bound <= std::numeric_limits<std::size_t>::max() / 4U) {
            circle_bound *= 4U;
        } else {
            circle_bound = total_pixels;
        }
        points.reserve(std::min(total_pixels, circle_bound));
    }
    for (std::size_t y = 0; y < height; ++y) {
        const auto dy = static_cast<std::int64_t>(y) - signed_cy;
        const auto dy_squared = dy * dy;
        if (dy_squared >= signed_radius_squared) {
            continue;
        }
        for (std::size_t x = 0; x < width; ++x) {
            const auto dx = static_cast<std::int64_t>(x) - signed_cx;
            if (dx * dx + dy_squared < signed_radius_squared) {
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

CreateZarrPlan create_zarr_plan(
    const std::string& version,
    const std::vector<std::size_t>& base_shape,
    const std::vector<std::size_t>& smallest_shape,
    const std::vector<std::size_t>& chunks) {
    CreateZarrPlan plan{};
    plan.legacy_five_d = version == "0.1" || version == "0.2";
    plan.axes_is_none = plan.legacy_five_d;

    if (plan.legacy_five_d) {
        plan.size_c = base_shape.size() > 1 ? base_shape[1] : 1;
        plan.labels_axes_is_none = true;
    } else {
        if (base_shape.size() == 3) {
            plan.axes = "cyx";
            plan.size_c = 3;
        } else {
            const std::string tczyx = "tczyx";
            plan.axes = tczyx.substr(tczyx.size() - base_shape.size());
            plan.size_c = 1;
        }
        plan.labels_axes = plan.axes;
        plan.labels_axes.erase(
            std::remove(plan.labels_axes.begin(), plan.labels_axes.end(), 'c'),
            plan.labels_axes.end());
        plan.labels_axes_is_none = plan.labels_axes.empty();
    }

    if (chunks.empty()) {
        plan.chunks = smallest_shape;
        for (std::size_t zct = 0; zct < 3; ++zct) {
            if (zct + 2 < plan.chunks.size()) {
                plan.chunks[zct] = 1;
            }
        }
    } else {
        plan.chunks = chunks;
    }

    plan.color_image = plan.size_c != 1;
    if (!plan.color_image) {
        plan.channel_model = "greyscale";
        plan.channels = {CreateZarrChannelPlan{"FF0000", "", false}};
    } else {
        plan.channel_model = "color";
        plan.channels = {
            CreateZarrChannelPlan{"FF0000", "Red", true},
            CreateZarrChannelPlan{"00FF00", "Green", true},
            CreateZarrChannelPlan{"0000FF", "Blue", true},
        };
    }
    plan.random_label_count = 8;
    plan.source_image = "../../";
    return plan;
}

}  // namespace ome_zarr_c::native_code
