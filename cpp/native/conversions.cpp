#include "conversions.hpp"

namespace ome_zarr_c::native_code {

std::array<std::uint8_t, 4> int_to_rgba_255_bytes(std::int32_t value) {
    const std::uint32_t raw = static_cast<std::uint32_t>(value);
    return {
        static_cast<std::uint8_t>((raw >> 24U) & 0xFFU),
        static_cast<std::uint8_t>((raw >> 16U) & 0xFFU),
        static_cast<std::uint8_t>((raw >> 8U) & 0xFFU),
        static_cast<std::uint8_t>(raw & 0xFFU),
    };
}

std::array<double, 4> int_to_rgba(std::int32_t value) {
    const auto bytes = int_to_rgba_255_bytes(value);
    return {
        static_cast<double>(bytes[0]) / 255.0,
        static_cast<double>(bytes[1]) / 255.0,
        static_cast<double>(bytes[2]) / 255.0,
        static_cast<double>(bytes[3]) / 255.0,
    };
}

std::int32_t rgba_to_int(
    std::uint8_t r,
    std::uint8_t g,
    std::uint8_t b,
    std::uint8_t a) {
    const std::uint32_t raw =
        (static_cast<std::uint32_t>(r) << 24U) |
        (static_cast<std::uint32_t>(g) << 16U) |
        (static_cast<std::uint32_t>(b) << 8U) |
        static_cast<std::uint32_t>(a);
    return static_cast<std::int32_t>(raw);
}

}  // namespace ome_zarr_c::native_code
