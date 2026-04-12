#pragma once

#include <array>
#include <cstdint>

namespace ome_zarr_c::native_code {

std::array<std::uint8_t, 4> int_to_rgba_255_bytes(std::int32_t value);

std::array<double, 4> int_to_rgba(std::int32_t value);

std::int32_t rgba_to_int(
    std::uint8_t r,
    std::uint8_t g,
    std::uint8_t b,
    std::uint8_t a);

}  // namespace ome_zarr_c::native_code
