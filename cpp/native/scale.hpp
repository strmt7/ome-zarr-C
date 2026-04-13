#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace ome_zarr_c::native_code {

struct ScaleLevel {
    std::vector<double> values;
};

struct PyramidLevelPlan {
    ScaleLevel absolute_factors;
    ScaleLevel relative_factors;
    std::vector<std::int64_t> target_shape;
    std::vector<std::string> warning_dims;
};

std::vector<std::string> scaler_methods();

bool scaler_has_method(const std::string& method);

std::vector<std::int64_t> scaler_resize_image_shape(
    const std::vector<std::int64_t>& shape,
    std::int64_t downscale);

std::vector<std::int64_t> scaler_nearest_plane_shape(
    std::int64_t size_y,
    std::int64_t size_x,
    std::int64_t downscale);

std::vector<std::vector<std::int64_t>> scaler_plane_indices(
    const std::vector<std::int64_t>& shape);

std::vector<std::int64_t> scaler_stack_shape(
    const std::vector<std::int64_t>& input_shape,
    const std::vector<std::int64_t>& plane_shape);

std::vector<std::int64_t> scaler_local_mean_factors(
    std::size_t ndim,
    std::int64_t downscale);

std::vector<long> scaler_zoom_factors(long downscale, long max_layer);

std::vector<std::string> scaler_group_dataset_paths(std::size_t pyramid_size);

std::vector<ScaleLevel> scale_levels_from_ints(
    const std::vector<std::string>& dims,
    std::size_t level_count);

std::vector<ScaleLevel> reorder_scale_levels(
    const std::vector<std::string>& dims,
    const std::vector<std::map<std::string, double>>& levels);

std::vector<PyramidLevelPlan> build_pyramid_plan(
    const std::vector<std::int64_t>& base_shape,
    const std::vector<std::string>& dims,
    const std::vector<ScaleLevel>& scale_levels);

}  // namespace ome_zarr_c::native_code
