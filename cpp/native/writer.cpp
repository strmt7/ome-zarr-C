#include "writer.hpp"

#include <algorithm>
#include <cctype>
#include <set>
#include <stdexcept>

#include "format.hpp"

namespace ome_zarr_c::native_code {

ValidAxesPlan get_valid_axes_plan(
    const std::string& version,
    bool axes_provided,
    const std::optional<std::int64_t>& ndim) {
    if (version == "0.1" || version == "0.2") {
        ValidAxesPlan plan{};
        plan.return_none = true;
        plan.log_ignored_axes = axes_provided;
        return plan;
    }

    if (axes_provided) {
        return ValidAxesPlan{};
    }

    if (ndim.has_value() && ndim.value() == 2) {
        ValidAxesPlan plan{};
        plan.log_auto_axes = true;
        plan.auto_label = "2D";
        plan.axes = {"y", "x"};
        return plan;
    }
    if (ndim.has_value() && ndim.value() == 5) {
        ValidAxesPlan plan{};
        plan.log_auto_axes = true;
        plan.auto_label = "5D";
        plan.axes = {"t", "c", "z", "y", "x"};
        return plan;
    }

    throw std::invalid_argument(
        "axes must be provided. Can't be guessed for 3D or 4D data");
}

void validate_axes_length(std::size_t axes_len, std::int64_t ndim) {
    if (axes_len != static_cast<std::size_t>(ndim)) {
        throw std::invalid_argument(
            "axes length (" + std::to_string(axes_len) +
            ") must match number of dimensions (" + std::to_string(ndim) + ")");
    }
}

std::vector<std::string> extract_dims_from_axes(
    const std::vector<AxisRecord>& axes) {
    return get_names(axes);
}

std::vector<std::size_t> retuple_prefix(
    std::size_t shape_len,
    std::size_t chunks_len) {
    const long long dims_to_add = static_cast<long long>(shape_len) -
                                  static_cast<long long>(chunks_len);
    long long prefix_len = dims_to_add;
    if (prefix_len < 0) {
        prefix_len = static_cast<long long>(shape_len) + prefix_len;
    }
    if (prefix_len < 0) {
        prefix_len = 0;
    }
    std::vector<std::size_t> prefix;
    prefix.reserve(static_cast<std::size_t>(prefix_len));
    for (std::size_t index = 0; index < static_cast<std::size_t>(prefix_len); ++index) {
        prefix.push_back(index);
    }
    return prefix;
}

WellImageResult validate_well_image(const WellImageInput& image) {
    if (image.is_string) {
        WellImageResult result{};
        result.materialize = true;
        result.path = image.path;
        return result;
    }

    if (!image.is_dict) {
        throw std::invalid_argument("Unrecognized type for " + image.repr);
    }
    if (!image.has_path) {
        throw std::invalid_argument(image.repr + " must contain a path key");
    }
    if (!image.path_is_string) {
        throw std::invalid_argument(image.repr + " path must be of string type");
    }
    if (image.has_acquisition && !image.acquisition_is_int) {
        throw std::invalid_argument(
            image.repr + " acquisition must be of int type");
    }
    WellImageResult result{};
    result.materialize = false;
    result.path = image.path;
    result.has_unexpected_key = image.has_unexpected_key;
    return result;
}

void validate_plate_acquisition(const PlateAcquisitionInput& acquisition) {
    if (!acquisition.is_dict) {
        throw std::invalid_argument(
            acquisition.repr + " must be a dictionary");
    }
    if (!acquisition.has_id) {
        throw std::invalid_argument(
            acquisition.repr + " must contain an id key");
    }
    if (!acquisition.id_is_int) {
        throw std::invalid_argument(
            acquisition.repr + " id must be of int type");
    }
}

std::vector<std::string> validate_plate_rows_columns(
    const std::vector<std::string>& rows_or_columns,
    const std::string& rows_or_columns_repr) {
    std::set<std::string> unique(rows_or_columns.begin(), rows_or_columns.end());
    if (unique.size() != rows_or_columns.size()) {
        throw std::invalid_argument(
            rows_or_columns_repr + " must contain unique elements");
    }

    for (const auto& element : rows_or_columns) {
        const bool valid = !element.empty() &&
            std::all_of(
                element.begin(),
                element.end(),
                [](unsigned char value) { return std::isalnum(value) != 0; });
        if (!valid) {
            throw std::invalid_argument(
                element + " must contain alphanumeric characters");
        }
    }

    return rows_or_columns;
}

DatasetValidationError::DatasetValidationError(
    DatasetValidationErrorCode code,
    std::size_t dataset_index)
    : code_(code), dataset_index_(dataset_index) {}

const char* DatasetValidationError::what() const noexcept {
    return "DatasetValidationError";
}

DatasetValidationErrorCode DatasetValidationError::code() const noexcept {
    return code_;
}

std::size_t DatasetValidationError::dataset_index() const noexcept {
    return dataset_index_;
}

void validate_datasets(const std::vector<DatasetInput>& datasets) {
    if (datasets.empty()) {
        throw DatasetValidationError(DatasetValidationErrorCode::empty_datasets);
    }

    for (std::size_t index = 0; index < datasets.size(); ++index) {
        const auto& dataset = datasets[index];
        if (!dataset.is_dict) {
            throw DatasetValidationError(
                DatasetValidationErrorCode::unrecognized_type,
                index);
        }
        if (!dataset.path_truthy) {
            throw DatasetValidationError(
                DatasetValidationErrorCode::missing_path,
                index);
        }
    }
}

WriterFormatPlan resolve_writer_format(
    int group_zarr_format,
    const std::optional<std::string>& requested_version) {
    if (requested_version.has_value()) {
        const int requested_zarr_format = format_zarr_format(requested_version.value());
        if (requested_zarr_format != group_zarr_format) {
            throw std::invalid_argument(
                "Group is zarr_format: " + std::to_string(group_zarr_format) +
                " but OME-Zarr v" + requested_version.value() +
                " is " + std::to_string(requested_zarr_format));
        }
        return WriterFormatPlan{requested_version.value(), requested_zarr_format};
    }

    if (group_zarr_format == 2) {
        return WriterFormatPlan{"0.4", 2};
    }
    return WriterFormatPlan{"0.5", 3};
}

bool writer_uses_legacy_root_attrs(const std::string& version) {
    return version == "0.1" || version == "0.2" ||
           version == "0.3" || version == "0.4";
}

WriterMultiscalesMetadataPlan writer_multiscales_metadata_plan(
    const std::string& version,
    const std::string& group_name) {
    WriterMultiscalesMetadataPlan plan{};
    plan.legacy_root_attrs = writer_uses_legacy_root_attrs(version);
    plan.embed_version_in_multiscales = plan.legacy_root_attrs;
    plan.write_root_version = !plan.legacy_root_attrs;
    plan.group_name = group_name;
    return plan;
}

WriterPlateMetadataPlan writer_plate_metadata_plan(const std::string& version) {
    WriterPlateMetadataPlan plan{};
    plan.legacy_root_attrs = writer_uses_legacy_root_attrs(version);
    plan.embed_plate_version = plan.legacy_root_attrs || version == "0.5";
    plan.write_root_version = !plan.legacy_root_attrs;
    return plan;
}

WriterWellMetadataPlan writer_well_metadata_plan(const std::string& version) {
    WriterWellMetadataPlan plan{};
    plan.legacy_root_attrs = writer_uses_legacy_root_attrs(version);
    plan.embed_well_version = plan.legacy_root_attrs;
    plan.write_root_version = !plan.legacy_root_attrs;
    return plan;
}

WriterLabelMetadataPlan writer_label_metadata_plan(const std::string& version) {
    WriterLabelMetadataPlan plan{};
    plan.legacy_root_attrs = writer_uses_legacy_root_attrs(version);
    return plan;
}

WriterPyramidPlan writer_pyramid_plan(
    const std::vector<std::vector<std::int64_t>>& shapes,
    int zarr_format,
    const std::vector<std::string>& axis_names,
    const std::vector<std::vector<std::size_t>>& explicit_chunks) {
    WriterPyramidPlan plan{};
    plan.zarr_format = zarr_format;
    plan.use_v2_chunk_key_encoding = zarr_format == 2;
    plan.use_dimension_names = zarr_format != 2 && !axis_names.empty();
    plan.dimension_names = axis_names;
    plan.levels.reserve(shapes.size());

    for (std::size_t index = 0; index < shapes.size(); ++index) {
        WriterPyramidLevelPlan level{};
        level.component = "s" + std::to_string(index);
        if (index < explicit_chunks.size() && !explicit_chunks[index].empty()) {
            level.has_chunks = true;
            level.chunks = explicit_chunks[index];
        }
        plan.levels.push_back(std::move(level));
    }

    return plan;
}

WriterStorageOptionsPlan writer_storage_options_plan(
    bool has_storage_options,
    bool is_list) {
    WriterStorageOptionsPlan plan{};
    plan.return_copy = has_storage_options && !is_list;
    plan.return_item = has_storage_options && is_list;
    return plan;
}

WriterBloscPlan writer_blosc_plan() {
    return {"zstd", 5, "SHUFFLE"};
}

WriterLabelsPlan writer_labels_plan(
    const std::vector<std::string>& dims,
    bool use_default_scaler,
    bool scaler_is_none,
    std::int64_t scaler_max_layer,
    const std::optional<std::string>& requested_method) {
    WriterLabelsPlan plan{};
    plan.resolved_method = requested_method.value_or("nearest");
    if (use_default_scaler || !scaler_is_none) {
        plan.warn_scaler_deprecated = true;
        const std::int64_t level_count = use_default_scaler ? 4 : scaler_max_layer;
        for (std::int64_t level = 1; level <= level_count; ++level) {
            std::map<std::string, std::int64_t> level_plan;
            for (const auto& dim : dims) {
                const bool spatial = dim == "x" || dim == "y" || dim == "z";
                level_plan[dim] = spatial ? (std::int64_t{1} << level) : 1;
            }
            plan.scale_factors.push_back(std::move(level_plan));
        }
    }
    return plan;
}

WriterImagePlan writer_image_plan(
    const std::vector<std::string>& dims,
    bool scaler_present,
    std::int64_t scaler_max_layer,
    const std::string& scaler_method,
    const std::optional<std::string>& requested_method) {
    WriterImagePlan plan{};
    plan.resolved_method = requested_method.value_or("resize");

    if (!scaler_present) {
        return plan;
    }

    plan.warn_scaler_deprecated = true;
    for (std::int64_t level = 1; level <= scaler_max_layer; ++level) {
        std::map<std::string, std::int64_t> level_plan;
        for (const auto& dim : dims) {
            const bool spatial = dim == "x" || dim == "y" || dim == "z";
            level_plan[dim] = spatial ? (std::int64_t{1} << level) : 1;
        }
        plan.scale_factors.push_back(std::move(level_plan));
    }

    if (scaler_method == "local_mean") {
        plan.resolved_method = "local_mean";
    } else if (scaler_method == "nearest") {
        plan.resolved_method = "nearest";
    } else if (scaler_method == "resize_image") {
        plan.resolved_method = "resize";
    } else if (scaler_method == "laplacian") {
        plan.resolved_method = "resize";
        plan.warn_laplacian_fallback = true;
    } else if (scaler_method == "zoom") {
        plan.resolved_method = "zoom";
    } else {
        plan.resolved_method = "resize";
    }

    return plan;
}

}  // namespace ome_zarr_c::native_code
