#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace ome_zarr_c::native_code {

struct CirclePoint {
    std::size_t y;
    std::size_t x;
};

std::vector<CirclePoint> circle_points(std::size_t height, std::size_t width);

std::vector<std::size_t> rgb_to_5d_shape(const std::vector<std::size_t>& shape);

std::vector<std::size_t> rgb_channel_order(const std::vector<std::size_t>& shape);

}  // namespace ome_zarr_c::native_code
