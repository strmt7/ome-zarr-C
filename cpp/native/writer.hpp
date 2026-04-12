#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "axes.hpp"

namespace ome_zarr_c::native_code {

struct ValidAxesPlan {
    bool return_none;
    bool log_ignored_axes;
    bool log_auto_axes;
    std::string auto_label;
    std::vector<std::string> axes;
};

ValidAxesPlan get_valid_axes_plan(
    const std::string& version,
    bool axes_provided,
    const std::optional<std::int64_t>& ndim);

void validate_axes_length(std::size_t axes_len, std::int64_t ndim);

std::vector<std::string> extract_dims_from_axes(
    const std::vector<AxisRecord>& axes);

struct WellImageInput {
    bool is_string;
    bool is_dict;
    std::string repr;
    bool has_path;
    bool path_is_string;
    std::string path;
    bool has_acquisition;
    bool acquisition_is_int;
    bool has_unexpected_key;
};

struct WellImageResult {
    bool materialize;
    std::string path;
    bool has_unexpected_key;
};

WellImageResult validate_well_image(const WellImageInput& image);

struct PlateAcquisitionInput {
    bool is_dict;
    std::string repr;
    bool has_id;
    bool id_is_int;
    bool has_unexpected_key;
};

void validate_plate_acquisition(const PlateAcquisitionInput& acquisition);

std::vector<std::string> validate_plate_rows_columns(
    const std::vector<std::string>& rows_or_columns,
    const std::string& rows_or_columns_repr);

struct DatasetInput {
    bool is_dict;
    std::string repr;
    bool path_truthy;
    bool has_transformation;
};

std::vector<std::size_t> validate_datasets(const std::vector<DatasetInput>& datasets);

}  // namespace ome_zarr_c::native_code
