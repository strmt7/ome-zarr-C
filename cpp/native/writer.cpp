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

std::vector<std::size_t> validate_datasets(const std::vector<DatasetInput>& datasets) {
    if (datasets.empty()) {
        throw std::invalid_argument("Empty datasets list");
    }

    std::vector<std::size_t> transformation_indices;
    for (std::size_t index = 0; index < datasets.size(); ++index) {
        const auto& dataset = datasets[index];
        if (!dataset.is_dict) {
            throw std::invalid_argument(
                "Unrecognized type for " + dataset.repr);
        }
        if (!dataset.path_truthy) {
            throw std::invalid_argument("no 'path' in dataset");
        }
        if (dataset.has_transformation) {
            transformation_indices.push_back(index);
        }
    }

    return transformation_indices;
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

}  // namespace ome_zarr_c::native_code
