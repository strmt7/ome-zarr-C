#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace ome_zarr_c::native_code {

struct ChunkKeyEncoding {
    std::string name;
    std::string separator;
};

struct MetadataSummary {
    bool is_empty;
    bool has_multiscales_version;
    bool multiscales_version_is_string;
    std::string multiscales_version;
    bool has_plate_version;
    bool plate_version_is_string;
    std::string plate_version;
    bool has_well_version;
    bool well_version_is_string;
    std::string well_version;
    bool has_image_label_version;
    bool image_label_version_is_string;
    std::string image_label_version;
};

struct WellDictV01Input {
    bool has_path;
    bool path_is_string;
};

struct CoordinateTransformation {
    std::string type;
    std::vector<double> values;
};

using CoordinateTransformations = std::vector<CoordinateTransformation>;

struct CoordinateTransformationValidationInput {
    bool has_type;
    std::string type;
    std::string transformation_repr;
    bool has_scale;
    std::size_t scale_length;
    std::vector<bool> scale_numeric;
    std::string scale_repr;
    bool has_translation;
    std::size_t translation_length;
    std::vector<bool> translation_numeric;
    std::string translation_repr;
};

struct CoordinateTransformationsValidationInput {
    std::string transformations_repr;
    std::vector<CoordinateTransformationValidationInput> transformations;
};

struct WellDictV04 {
    std::string path;
    std::int64_t row_index;
    std::int64_t column_index;
};

enum class WellValidationErrorCode {
    path_group_count,
    row_missing,
    row_index_mismatch,
    column_missing,
    column_index_mismatch,
};

class WellValidationError final : public std::exception {
  public:
    WellValidationError(WellValidationErrorCode code, std::string detail);

    const char* what() const noexcept override;

    WellValidationErrorCode code() const noexcept;

    const std::string& detail() const noexcept;

  private:
    WellValidationErrorCode code_;
    std::string detail_;
};

enum class WellGenerationErrorCode {
    path_not_enough_groups,
    path_too_many_groups,
    row_missing,
    column_missing,
};

class WellGenerationError final : public std::exception {
  public:
    WellGenerationError(WellGenerationErrorCode code, std::string detail);

    const char* what() const noexcept override;

    WellGenerationErrorCode code() const noexcept;

    const std::string& detail() const noexcept;

  private:
    WellGenerationErrorCode code_;
    std::string detail_;
};

const std::array<std::string, 5>& format_versions();

std::string normalize_known_format_version(const std::string& version);

bool is_known_format_version_string(const std::string& version);

std::optional<std::string> get_metadata_version(const MetadataSummary& metadata);

std::optional<std::string> detect_format_version(const MetadataSummary& metadata);

bool format_matches(const std::string& version, const MetadataSummary& metadata);

int format_zarr_format(const std::string& version);

ChunkKeyEncoding format_chunk_key_encoding(const std::string& version);

std::vector<CoordinateTransformations> generate_coordinate_transformations(
    const std::vector<std::vector<double>>& shapes);

void validate_coordinate_transformations(
    int ndim,
    int nlevels,
    const std::vector<CoordinateTransformations>& coordinate_transformations);

void validate_coordinate_transformations(
    int ndim,
    int nlevels,
    const std::vector<CoordinateTransformationsValidationInput>& coordinate_transformations);

void validate_well_v04(
    const std::string& path,
    std::int64_t row_index,
    std::int64_t column_index,
    const std::vector<std::string>& rows,
    const std::vector<std::string>& columns);

WellDictV04 generate_well_v04(
    const std::string& path,
    const std::vector<std::string>& rows,
    const std::vector<std::string>& columns);

void validate_well_v01(const WellDictV01Input& well);

}  // namespace ome_zarr_c::native_code
