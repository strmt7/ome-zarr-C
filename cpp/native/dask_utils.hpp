#pragma once

#include <cstdint>
#include <utility>
#include <vector>

namespace ome_zarr_c::native_code {

struct DaskChunkPlan {
    std::vector<std::int64_t> better_chunks;
    std::vector<std::int64_t> block_output_shape;
};

struct DaskResizePlan {
    std::vector<double> factors;
    DaskChunkPlan chunk_plan;
};

struct DaskLocalMeanPlan {
    std::vector<double> factors;
    std::vector<std::int64_t> int_factors;
    DaskChunkPlan chunk_plan;
};

struct DaskZoomPlan {
    std::vector<double> factors;
    std::vector<double> inverse_factors;
    DaskChunkPlan chunk_plan;
    std::vector<std::int64_t> resized_output_shape;
};

DaskChunkPlan better_chunksize(
    const std::vector<std::int64_t>& chunksize,
    const std::vector<double>& factors);

DaskResizePlan resize_plan(
    const std::vector<std::int64_t>& image_shape,
    const std::vector<std::int64_t>& chunksize,
    const std::vector<std::int64_t>& output_shape);

DaskLocalMeanPlan local_mean_plan(
    const std::vector<std::int64_t>& image_shape,
    const std::vector<std::int64_t>& chunksize,
    const std::vector<std::int64_t>& output_shape);

DaskZoomPlan zoom_plan(
    const std::vector<std::int64_t>& image_shape,
    const std::vector<std::int64_t>& chunksize,
    const std::vector<std::int64_t>& output_shape);

std::vector<std::int64_t> block_output_shape(
    const std::vector<std::int64_t>& block_shape,
    const std::vector<double>& factors);

void validate_downscale_nearest(
    const std::vector<std::int64_t>& image_shape,
    const std::vector<std::int64_t>& factors);

}  // namespace ome_zarr_c::native_code
