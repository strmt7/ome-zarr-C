#include "dask_utils.hpp"

#include <cmath>
#include <cstddef>
#include <stdexcept>

namespace ome_zarr_c::native_code {

std::pair<std::vector<std::int64_t>, std::vector<std::int64_t>> better_chunksize(
    const std::vector<std::int64_t>& chunksize,
    const std::vector<double>& factors) {
    if (chunksize.size() != factors.size()) {
        throw std::invalid_argument("chunksize and factors length mismatch");
    }

    std::vector<std::int64_t> better_chunks;
    std::vector<std::int64_t> block_output;
    better_chunks.reserve(chunksize.size());
    block_output.reserve(chunksize.size());

    for (std::size_t index = 0; index < chunksize.size(); ++index) {
        const double scaled = std::nearbyint(
            static_cast<double>(chunksize[index]) * factors[index]);
        const auto better_chunk = std::max<std::int64_t>(
            1,
            static_cast<std::int64_t>(scaled / factors[index]));
        better_chunks.push_back(better_chunk);
        block_output.push_back(static_cast<std::int64_t>(
            std::ceil(static_cast<double>(better_chunk) * factors[index])));
    }

    return {better_chunks, block_output};
}

}  // namespace ome_zarr_c::native_code
