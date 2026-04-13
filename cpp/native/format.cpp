#include "format.hpp"

#include <algorithm>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace ome_zarr_c::native_code {

namespace {

const std::array<std::string, 5> kFormatVersions = {"0.5", "0.4", "0.3", "0.2", "0.1"};

template <typename T>
bool any_false(const T& values) {
    return std::any_of(values.begin(), values.end(), [](bool value) { return !value; });
}

}  // namespace

WellValidationError::WellValidationError(
    WellValidationErrorCode code,
    std::string detail)
    : code_(code), detail_(std::move(detail)) {}

const char* WellValidationError::what() const noexcept {
    return "WellValidationError";
}

WellValidationErrorCode WellValidationError::code() const noexcept {
    return code_;
}

const std::string& WellValidationError::detail() const noexcept {
    return detail_;
}

const std::array<std::string, 5>& format_versions() {
    return kFormatVersions;
}

WellGenerationError::WellGenerationError(
    WellGenerationErrorCode code,
    std::string detail)
    : code_(code), detail_(std::move(detail)) {}

const char* WellGenerationError::what() const noexcept {
    return "WellGenerationError";
}

WellGenerationErrorCode WellGenerationError::code() const noexcept {
    return code_;
}

const std::string& WellGenerationError::detail() const noexcept {
    return detail_;
}

std::string normalize_known_format_version(const std::string& version) {
    for (const auto& known_version : kFormatVersions) {
        if (known_version == version) {
            return known_version;
        }
    }
    throw std::invalid_argument("Version " + version + " not recognized");
}

bool is_known_format_version_string(const std::string& version) {
    for (const auto& known_version : kFormatVersions) {
        if (known_version == version) {
            return true;
        }
    }
    return false;
}

std::optional<std::string> get_metadata_version(const MetadataSummary& metadata) {
    if (metadata.has_multiscales_version) {
        return metadata.multiscales_version;
    }
    if (metadata.has_plate_version) {
        return metadata.plate_version;
    }
    if (metadata.has_well_version) {
        return metadata.well_version;
    }
    if (metadata.has_image_label_version) {
        return metadata.image_label_version;
    }
    return std::nullopt;
}

std::optional<std::string> detect_format_version(const MetadataSummary& metadata) {
    if (metadata.is_empty) {
        return std::nullopt;
    }

    if (metadata.has_multiscales_version) {
        if (!metadata.multiscales_version_is_string ||
            !is_known_format_version_string(metadata.multiscales_version)) {
            return std::nullopt;
        }
        return metadata.multiscales_version;
    }
    if (metadata.has_plate_version) {
        if (!metadata.plate_version_is_string ||
            !is_known_format_version_string(metadata.plate_version)) {
            return std::nullopt;
        }
        return metadata.plate_version;
    }
    if (metadata.has_well_version) {
        if (!metadata.well_version_is_string ||
            !is_known_format_version_string(metadata.well_version)) {
            return std::nullopt;
        }
        return metadata.well_version;
    }
    if (metadata.has_image_label_version) {
        if (!metadata.image_label_version_is_string ||
            !is_known_format_version_string(metadata.image_label_version)) {
            return std::nullopt;
        }
        return metadata.image_label_version;
    }

    return std::nullopt;
}

bool format_matches(const std::string& version, const MetadataSummary& metadata) {
    const auto detected = detect_format_version(metadata);
    if (!detected.has_value()) {
        return false;
    }
    return normalize_known_format_version(version) == detected.value();
}

int format_zarr_format(const std::string& version) {
    const std::string normalized = normalize_known_format_version(version);
    if (normalized == "0.5") {
        return 3;
    }
    return 2;
}

ChunkKeyEncoding format_chunk_key_encoding(const std::string& version) {
    const std::string normalized = normalize_known_format_version(version);
    if (normalized == "0.1") {
        return {"v2", "."};
    }
    if (normalized == "0.5") {
        return {"default", "/"};
    }
    return {"v2", "/"};
}

FormatInitStorePlan format_init_store_plan(
    const std::string& path,
    const std::string& mode) {
    const bool read_only = mode == "r";
    const bool use_fsspec =
        path.rfind("http", 0) == 0 || path.rfind("s3", 0) == 0;
    return {use_fsspec, read_only};
}

std::vector<CoordinateTransformations> generate_coordinate_transformations(
    const std::vector<std::vector<double>>& shapes) {
    if (shapes.empty()) {
        return {};
    }

    const auto& data_shape = shapes.front();
    std::vector<CoordinateTransformations> result;
    result.reserve(shapes.size());

    for (const auto& shape : shapes) {
        if (shape.size() != data_shape.size()) {
            throw std::invalid_argument("Shape lengths must match");
        }
        CoordinateTransformation transform;
        transform.type = "scale";
        transform.values.reserve(shape.size());
        for (std::size_t index = 0; index < shape.size(); ++index) {
            transform.values.push_back(data_shape[index] / shape[index]);
        }
        result.push_back(CoordinateTransformations{transform});
    }

    return result;
}

void validate_coordinate_transformations(
    int ndim,
    int nlevels,
    const std::vector<CoordinateTransformations>& coordinate_transformations) {
    const int count = static_cast<int>(coordinate_transformations.size());
    if (count != nlevels) {
        throw std::invalid_argument(
            "coordinate_transformations count: " + std::to_string(count) +
            " must match datasets " + std::to_string(nlevels));
    }

    for (const auto& transformations : coordinate_transformations) {
        int scale_count = 0;
        int translation_count = 0;
        int translation_index = -1;

        for (std::size_t index = 0; index < transformations.size(); ++index) {
            const auto& transformation = transformations[index];
            if (transformation.type == "scale") {
                scale_count += 1;
            } else if (transformation.type == "translation") {
                translation_count += 1;
                translation_index = static_cast<int>(index);
            }
        }

        if (scale_count != 1) {
            throw std::invalid_argument(
                "Must supply 1 'scale' item in coordinate_transformations");
        }
        if (transformations.front().type != "scale") {
            throw std::invalid_argument(
                "First coordinate_transformations must be 'scale'");
        }

        const auto& scale = transformations.front().values;
        if (static_cast<int>(scale.size()) != ndim) {
            throw std::invalid_argument(
                "'scale' list must match number of image dimensions: " +
                std::to_string(ndim));
        }

        if (translation_count > 1) {
            throw std::invalid_argument(
                "Must supply 0 or 1 'translation' item incoordinate_transformations");
        }
        if (translation_count == 1) {
            const auto& translation = transformations[translation_index].values;
            if (static_cast<int>(translation.size()) != ndim) {
                throw std::invalid_argument(
                    "'translation' list must match image dimensions count: " +
                    std::to_string(ndim));
            }
        }
    }
}

void validate_coordinate_transformations(
    int ndim,
    int nlevels,
    const std::vector<CoordinateTransformationsValidationInput>& coordinate_transformations) {
    const int count = static_cast<int>(coordinate_transformations.size());
    if (count != nlevels) {
        throw std::invalid_argument(
            "coordinate_transformations count: " + std::to_string(count) +
            " must match datasets " + std::to_string(nlevels));
    }

    for (const auto& group : coordinate_transformations) {
        for (const auto& transformation : group.transformations) {
            if (!transformation.has_type) {
                throw std::invalid_argument(
                    "Missing type in: " + group.transformations_repr);
            }
        }

        int scale_count = 0;
        int translation_count = 0;
        int translation_index = -1;
        for (std::size_t index = 0; index < group.transformations.size(); ++index) {
            const auto& transformation = group.transformations[index];
            if (transformation.type == "scale") {
                scale_count += 1;
            } else if (transformation.type == "translation") {
                translation_count += 1;
                translation_index = static_cast<int>(index);
            }
        }

        if (scale_count != 1) {
            throw std::invalid_argument(
                "Must supply 1 'scale' item in coordinate_transformations");
        }
        if (group.transformations.front().type != "scale") {
            throw std::invalid_argument(
                "First coordinate_transformations must be 'scale'");
        }

        const auto& first = group.transformations.front();
        if (!first.has_scale) {
            throw std::invalid_argument(
                "Missing scale argument in: " + first.transformation_repr);
        }
        if (static_cast<int>(first.scale_length) != ndim) {
            throw std::invalid_argument(
                "'scale' list " + first.scale_repr +
                " must match number of image dimensions: " + std::to_string(ndim));
        }
        if (any_false(first.scale_numeric)) {
            throw std::invalid_argument(
                "'scale' values must all be numbers: " + first.scale_repr);
        }

        if (translation_count > 1) {
            throw std::invalid_argument(
                "Must supply 0 or 1 'translation' item incoordinate_transformations");
        }
        if (translation_count == 1) {
            const auto& translation = group.transformations[translation_index];
            if (!translation.has_translation) {
                throw std::invalid_argument(
                    "Missing scale argument in: " + first.transformation_repr);
            }
            if (static_cast<int>(translation.translation_length) != ndim) {
                throw std::invalid_argument(
                    "'translation' list " + translation.translation_repr +
                    " must match image dimensions count: " + std::to_string(ndim));
            }
            if (any_false(translation.translation_numeric)) {
                throw std::invalid_argument(
                    "'translation' values must all be numbers: " +
                    translation.translation_repr);
            }
        }
    }
}

void validate_well_v04(
    const std::string& path,
    std::int64_t row_index,
    std::int64_t column_index,
    const std::vector<std::string>& rows,
    const std::vector<std::string>& columns) {
    const auto slash_index = path.find('/');
    if (slash_index == std::string::npos ||
        slash_index != path.rfind('/') ||
        slash_index == 0 ||
        slash_index == path.size() - 1) {
        throw WellValidationError(
            WellValidationErrorCode::path_group_count, path);
    }

    const std::string row = path.substr(0, slash_index);
    const std::string column = path.substr(slash_index + 1);
    const auto row_it = std::find(rows.begin(), rows.end(), row);
    if (row_it == rows.end()) {
        throw WellValidationError(WellValidationErrorCode::row_missing, row);
    }
    if (row_index != std::distance(rows.begin(), row_it)) {
        throw WellValidationError(
            WellValidationErrorCode::row_index_mismatch, row);
    }

    const auto column_it = std::find(columns.begin(), columns.end(), column);
    if (column_it == columns.end()) {
        throw WellValidationError(WellValidationErrorCode::column_missing, column);
    }
    if (column_index != std::distance(columns.begin(), column_it)) {
        throw WellValidationError(
            WellValidationErrorCode::column_index_mismatch, column);
    }
}

WellDictV04 generate_well_v04(
    const std::string& path,
    const std::vector<std::string>& rows,
    const std::vector<std::string>& columns) {
    const auto slash_index = path.find('/');
    if (slash_index == std::string::npos) {
        throw WellGenerationError(
            WellGenerationErrorCode::path_not_enough_groups,
            "not enough values to unpack (expected 2, got 1)");
    }
    if (slash_index != path.rfind('/')) {
        throw WellGenerationError(
            WellGenerationErrorCode::path_too_many_groups,
            "too many values to unpack (expected 2)");
    }

    const std::string row = path.substr(0, slash_index);
    const std::string column = path.substr(slash_index + 1);

    const auto row_it = std::find(rows.begin(), rows.end(), row);
    if (row_it == rows.end()) {
        throw WellGenerationError(WellGenerationErrorCode::row_missing, row);
    }

    const auto column_it = std::find(columns.begin(), columns.end(), column);
    if (column_it == columns.end()) {
        throw WellGenerationError(WellGenerationErrorCode::column_missing, column);
    }

    return WellDictV04{
        path,
        std::distance(rows.begin(), row_it),
        std::distance(columns.begin(), column_it),
    };
}

void validate_well_v01(const WellDictV01Input& well) {
    if (!well.has_path || !well.path_is_string) {
        throw std::invalid_argument("invalid well");
    }
}

}  // namespace ome_zarr_c::native_code
