#include "scale.hpp"

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace ome_zarr_c::native_code {

namespace {

bool is_spatial_dim(const std::string& dim) {
    return dim == "z" || dim == "y" || dim == "x";
}

}  // namespace

std::vector<std::int64_t> scaler_resize_image_shape(
    const std::vector<std::int64_t>& shape,
    std::int64_t downscale) {
    std::vector<std::int64_t> out_shape = shape;
    out_shape[out_shape.size() - 1] = shape[out_shape.size() - 1] / downscale;
    out_shape[out_shape.size() - 2] = shape[out_shape.size() - 2] / downscale;
    return out_shape;
}

std::vector<std::int64_t> scaler_nearest_plane_shape(
    std::int64_t size_y,
    std::int64_t size_x,
    std::int64_t downscale) {
    return {size_y / downscale, size_x / downscale};
}

std::vector<std::vector<std::int64_t>> scaler_plane_indices(
    const std::vector<std::int64_t>& shape) {
    if (shape.size() <= 2) {
        return {};
    }

    std::vector<std::vector<std::int64_t>> indices(1);
    for (std::size_t dim = 0; dim < shape.size() - 2; ++dim) {
        std::vector<std::vector<std::int64_t>> next;
        for (const auto& prefix : indices) {
            for (std::int64_t value = 0; value < shape[dim]; ++value) {
                auto expanded = prefix;
                expanded.push_back(value);
                next.push_back(std::move(expanded));
            }
        }
        indices = std::move(next);
    }
    return indices;
}

std::vector<std::int64_t> scaler_stack_shape(
    const std::vector<std::int64_t>& input_shape,
    const std::vector<std::int64_t>& plane_shape) {
    std::vector<std::int64_t> shape;
    shape.reserve(input_shape.size());
    for (std::size_t index = 0; index + 2 < input_shape.size(); ++index) {
        shape.push_back(input_shape[index]);
    }
    shape.insert(shape.end(), plane_shape.begin(), plane_shape.end());
    return shape;
}

std::vector<std::int64_t> scaler_local_mean_factors(
    std::size_t ndim,
    std::int64_t downscale) {
    std::vector<std::int64_t> factors(ndim, 1);
    factors[ndim - 1] = downscale;
    factors[ndim - 2] = downscale;
    return factors;
}

std::vector<long> scaler_zoom_factors(long downscale, long max_layer) {
    std::vector<long> factors;
    factors.reserve(static_cast<std::size_t>(max_layer));
    for (long level_index = 0; level_index < max_layer; ++level_index) {
        factors.push_back(static_cast<long>(std::pow(downscale, level_index)));
    }
    return factors;
}

std::vector<ScaleLevel> scale_levels_from_ints(
    const std::vector<std::string>& dims,
    std::size_t level_count) {
    std::vector<ScaleLevel> levels;
    levels.reserve(level_count);

    const bool contains_z =
        std::find(dims.begin(), dims.end(), "z") != dims.end();
    for (std::size_t level_index = 1; level_index <= level_count; ++level_index) {
        ScaleLevel level;
        level.values.reserve(dims.size());
        for (const auto& dim : dims) {
            if (!is_spatial_dim(dim)) {
                level.values.push_back(1.0);
            } else if (dim == "z" && contains_z) {
                level.values.push_back(1.0);
            } else {
                level.values.push_back(static_cast<double>(std::int64_t{1} << level_index));
            }
        }
        levels.push_back(std::move(level));
    }

    return levels;
}

std::vector<ScaleLevel> reorder_scale_levels(
    const std::vector<std::string>& dims,
    const std::vector<std::map<std::string, double>>& levels) {
    std::vector<ScaleLevel> ordered_levels;
    ordered_levels.reserve(levels.size());

    for (const auto& input_level : levels) {
        ScaleLevel level;
        level.values.reserve(dims.size());
        for (const auto& dim : dims) {
            const auto found = input_level.find(dim);
            level.values.push_back(found == input_level.end() ? 1.0 : found->second);
        }
        ordered_levels.push_back(std::move(level));
    }

    return ordered_levels;
}

std::vector<PyramidLevelPlan> build_pyramid_plan(
    const std::vector<std::int64_t>& base_shape,
    const std::vector<std::string>& dims,
    const std::vector<ScaleLevel>& scale_levels) {
    if (base_shape.size() != dims.size()) {
        throw std::invalid_argument("shape/dims length mismatch");
    }

    std::vector<PyramidLevelPlan> plan;
    plan.reserve(scale_levels.size());
    std::vector<std::int64_t> current_shape = base_shape;

    for (std::size_t index = 0; index < scale_levels.size(); ++index) {
        PyramidLevelPlan level_plan;
        level_plan.absolute_factors = scale_levels[index];
        level_plan.relative_factors.values.reserve(dims.size());
        level_plan.target_shape.reserve(dims.size());

        for (std::size_t dim_index = 0; dim_index < dims.size(); ++dim_index) {
            const double absolute_factor = scale_levels[index].values[dim_index];
            const double previous_factor =
                index == 0 ? 1.0 : scale_levels[index - 1].values[dim_index];
            const double relative_factor = absolute_factor / previous_factor;
            level_plan.relative_factors.values.push_back(relative_factor);

            if (!is_spatial_dim(dims[dim_index])) {
                level_plan.target_shape.push_back(current_shape[dim_index]);
                continue;
            }

            const double scaled = std::floor(
                static_cast<double>(current_shape[dim_index]) / relative_factor);
            if (scaled == 0.0) {
                level_plan.target_shape.push_back(1);
                level_plan.warning_dims.push_back(dims[dim_index]);
            } else {
                level_plan.target_shape.push_back(
                    static_cast<std::int64_t>(scaled));
            }
        }

        current_shape = level_plan.target_shape;
        plan.push_back(std::move(level_plan));
    }

    return plan;
}

}  // namespace ome_zarr_c::native_code
