#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace ome_zarr_c::native_code {

struct ChunkKeyEncoding {
    std::string name;
    std::string separator;
};

struct FormatInitStorePlan {
    bool use_fsspec;
    bool read_only;
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

enum class CoordinateTransformationKind {
    other,
    scale,
    translation,
};

struct CoordinateTransformationValidationInput {
    bool has_type;
    CoordinateTransformationKind kind;
    bool has_scale;
    std::size_t scale_length;
    bool scale_all_numeric;
    bool has_translation;
    std::size_t translation_length;
    bool translation_all_numeric;
};

struct CoordinateTransformationsValidationInput {
    std::vector<CoordinateTransformationValidationInput> transformations;
};

enum class CoordinateTransformationsValidationErrorCode {
    count_mismatch,
    missing_type,
    invalid_scale_count,
    first_not_scale,
    missing_scale_argument,
    scale_length_mismatch,
    scale_non_numeric,
    invalid_translation_count,
    missing_translation_argument,
    translation_length_mismatch,
    translation_non_numeric,
};

class CoordinateTransformationsValidationError final : public std::exception {
  public:
    CoordinateTransformationsValidationError(
        CoordinateTransformationsValidationErrorCode code,
        std::size_t group_index = 0,
        std::size_t transformation_index = 0,
        int actual_count = 0);

    const char* what() const noexcept override;

    CoordinateTransformationsValidationErrorCode code() const noexcept;

    std::size_t group_index() const noexcept;

    std::size_t transformation_index() const noexcept;

    int actual_count() const noexcept;

  private:
    CoordinateTransformationsValidationErrorCode code_;
    std::size_t group_index_;
    std::size_t transformation_index_;
    int actual_count_;
};

struct WellDictV04 {
    std::string path;
    std::int64_t row_index;
    std::int64_t column_index;
};

struct WellPathParts {
    std::string row;
    std::string column;
};

struct WellPathPartsView {
    std::string_view row;
    std::string_view column;
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

std::string format_class_name(const std::string& version);
bool format_class_matches(
    const std::string& version,
    const std::string& self_module,
    const std::string& other_module,
    const std::string& other_name);

FormatInitStorePlan format_init_store_plan(
    const std::string& path,
    const std::string& mode);

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

WellPathPartsView split_well_path_for_validation(std::string_view path);

WellPathPartsView split_well_path_for_generation(std::string_view path);

WellDictV04 generate_well_v04(
    const std::string& path,
    const std::vector<std::string>& rows,
    const std::vector<std::string>& columns);

void validate_well_v01(const WellDictV01Input& well);

}  // namespace ome_zarr_c::native_code
