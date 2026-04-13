#pragma once

#include <cstdint>
#include <optional>
#include <stdexcept>
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

std::vector<std::size_t> retuple_prefix(
    std::size_t shape_len,
    std::size_t chunks_len);

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

struct WriterFormatPlan {
    std::string resolved_version;
    int resolved_zarr_format;
};

WriterFormatPlan resolve_writer_format(
    int group_zarr_format,
    const std::optional<std::string>& requested_version);

bool writer_uses_legacy_root_attrs(const std::string& version);

struct WriterMultiscalesMetadataPlan {
    bool legacy_root_attrs;
    bool embed_version_in_multiscales;
    bool write_root_version;
    std::string group_name;
};

WriterMultiscalesMetadataPlan writer_multiscales_metadata_plan(
    const std::string& version,
    const std::string& group_name);

struct WriterPlateMetadataPlan {
    bool legacy_root_attrs;
    bool embed_plate_version;
    bool write_root_version;
};

WriterPlateMetadataPlan writer_plate_metadata_plan(const std::string& version);

struct WriterWellMetadataPlan {
    bool legacy_root_attrs;
    bool embed_well_version;
    bool write_root_version;
};

WriterWellMetadataPlan writer_well_metadata_plan(const std::string& version);

struct WriterLabelMetadataPlan {
    bool legacy_root_attrs;
};

WriterLabelMetadataPlan writer_label_metadata_plan(const std::string& version);

}  // namespace ome_zarr_c::native_code
