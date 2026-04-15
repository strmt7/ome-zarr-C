#pragma once

#include <string>
#include <vector>

#include "utils.hpp"

namespace ome_zarr_c::native_code {

struct LocalFindMultiscalesResult {
    bool metadata_missing;
    bool logged_no_wells;
    std::string logged_no_wells_path;
    std::vector<UtilsDiscoveredImage> images;
};

LocalFindMultiscalesResult local_find_multiscales(const std::string& input_path);

std::vector<UtilsDiscoveredImage> local_walk_ome_zarr(const std::string& input_path);

std::vector<std::string> local_info_lines(
    const std::string& input_path,
    bool stats = false);

struct LocalFinderResult {
    bool found_any;
    std::string csv_path;
    std::string source_uri;
    std::string app_url;
    std::vector<UtilsFinderRow> rows;
};

LocalFinderResult local_finder_csv(const std::string& input_path, int port);

struct LocalViewPreparation {
    bool should_warn;
    std::string warning_message;
    std::string parent_dir;
    std::string image_name;
    std::string url;
};

LocalViewPreparation local_view_prepare(
    const std::string& input_path,
    int port,
    bool force);

void local_view_run(const LocalViewPreparation& preparation, int port);

struct LocalDownloadResult {
    std::string copied_root;
    std::vector<std::string> listed_paths;
};

LocalDownloadResult local_download_copy(
    const std::string& input_path,
    const std::string& output_dir);

struct LocalCsvToLabelsResult {
    std::size_t touched_label_groups;
    std::size_t updated_properties;
};

LocalCsvToLabelsResult local_csv_to_labels(
    const std::string& csv_path,
    const std::string& csv_id,
    const std::string& csv_keys,
    const std::string& zarr_path,
    const std::string& zarr_id);

}  // namespace ome_zarr_c::native_code
