#pragma once

#include <cstdint>
#include <utility>
#include <vector>

namespace ome_zarr_c::native_code {

std::pair<std::vector<std::int64_t>, std::vector<std::int64_t>> better_chunksize(
    const std::vector<std::int64_t>& chunksize,
    const std::vector<double>& factors);

}  // namespace ome_zarr_c::native_code
