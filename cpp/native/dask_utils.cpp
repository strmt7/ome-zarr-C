#include "dask_utils.hpp"

#include <cmath>
#include <cstddef>
#include <stdexcept>

namespace ome_zarr_c::native_code {

namespace {

std::vector<double> ratio(
    const std::vector<std::int64_t>& numerator,
    const std::vector<std::int64_t>& denominator) {
    if (numerator.size() != denominator.size()) {
        throw std::invalid_argument("shape length mismatch");
    }

    std::vector<double> values;
    values.reserve(numerator.size());
    for (std::size_t index = 0; index < numerator.size(); ++index) {
        values.push_back(
            static_cast<double>(numerator[index]) /
            static_cast<double>(denominator[index]));
    }
    return values;
}

std::vector<double> reciprocal(const std::vector<double>& values) {
    std::vector<double> inverted;
    inverted.reserve(values.size());
    for (const double value : values) {
        inverted.push_back(1.0 / value);
    }
    return inverted;
}

std::vector<std::int64_t> as_ints(const std::vector<double>& values) {
    std::vector<std::int64_t> ints;
    ints.reserve(values.size());
    for (const double value : values) {
        ints.push_back(static_cast<std::int64_t>(value));
    }
    return ints;
}

}  // namespace

DaskChunkPlan better_chunksize(
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

    DaskChunkPlan plan{};
    plan.better_chunks = std::move(better_chunks);
    plan.block_output_shape = std::move(block_output);
    return plan;
}

DaskResizePlan resize_plan(
    const std::vector<std::int64_t>& image_shape,
    const std::vector<std::int64_t>& chunksize,
    const std::vector<std::int64_t>& output_shape) {
    DaskResizePlan plan{};
    plan.factors = ratio(output_shape, image_shape);
    plan.chunk_plan = better_chunksize(chunksize, plan.factors);
    return plan;
}

DaskLocalMeanPlan local_mean_plan(
    const std::vector<std::int64_t>& image_shape,
    const std::vector<std::int64_t>& chunksize,
    const std::vector<std::int64_t>& output_shape) {
    DaskLocalMeanPlan plan{};
    plan.factors = ratio(image_shape, output_shape);
    plan.int_factors = as_ints(plan.factors);
    plan.chunk_plan = better_chunksize(chunksize, reciprocal(plan.factors));
    return plan;
}

DaskZoomPlan zoom_plan(
    const std::vector<std::int64_t>& image_shape,
    const std::vector<std::int64_t>& chunksize,
    const std::vector<std::int64_t>& output_shape) {
    DaskZoomPlan plan{};
    plan.factors = ratio(image_shape, output_shape);
    plan.inverse_factors = reciprocal(plan.factors);
    plan.chunk_plan = better_chunksize(chunksize, plan.inverse_factors);
    plan.resized_output_shape.reserve(image_shape.size());
    for (std::size_t index = 0; index < image_shape.size(); ++index) {
        plan.resized_output_shape.push_back(static_cast<std::int64_t>(
            std::floor(
                static_cast<double>(image_shape[index]) / plan.factors[index])));
    }
    return plan;
}

std::vector<std::int64_t> block_output_shape(
    const std::vector<std::int64_t>& block_shape,
    const std::vector<double>& factors) {
    if (block_shape.size() != factors.size()) {
        throw std::invalid_argument("block_shape and factors length mismatch");
    }

    std::vector<std::int64_t> shape;
    shape.reserve(block_shape.size());
    for (std::size_t index = 0; index < block_shape.size(); ++index) {
        shape.push_back(static_cast<std::int64_t>(
            std::ceil(static_cast<double>(block_shape[index]) * factors[index])));
    }
    return shape;
}

void validate_downscale_nearest(
    const std::vector<std::int64_t>& image_shape,
    const std::vector<std::int64_t>& factors) {
    if (image_shape.size() != factors.size()) {
        throw std::invalid_argument("Dimension mismatch");
    }

    for (std::size_t index = 0; index < factors.size(); ++index) {
        if (factors[index] <= 0 || factors[index] > image_shape[index]) {
            throw std::invalid_argument("All scale factors must be valid");
        }
    }
}

}  // namespace ome_zarr_c::native_code
