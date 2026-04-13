#pragma once

#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace ome_zarr_c::native_code {

std::string strip_common_prefix(std::vector<std::vector<std::string>>& parts);

std::vector<std::string> splitall(const std::string& path);

struct UtilsDiscoveredImage {
    std::string path;
    std::string name;
    std::string dirname;
};

std::string utils_missing_metadata_message();

std::vector<UtilsDiscoveredImage> utils_plate_images(
    const std::string& path_to_zattrs,
    const std::string& basename,
    const std::string& dirname,
    const std::vector<std::string>& wells);

std::vector<UtilsDiscoveredImage> utils_bioformats_images(
    const std::string& path_to_zattrs,
    const std::string& basename,
    const std::string& dirname,
    const std::vector<std::optional<std::string>>& image_names);

std::vector<UtilsDiscoveredImage> utils_single_multiscales_image(
    const std::string& path_to_zattrs,
    const std::string& basename,
    const std::string& dirname);

std::string utils_info_not_ome_zarr_line(const std::string& node_repr);

std::vector<std::string> utils_info_header_lines(
    const std::string& node_repr,
    const std::string& version,
    const std::vector<std::string>& spec_names);

std::string utils_info_data_line(
    const std::string& shape_repr,
    const std::optional<std::string>& minmax_repr);

struct UtilsDownloadPlan {
    std::string common;
    std::vector<std::vector<std::string>> stripped_parts;
};

UtilsDownloadPlan utils_download_plan(std::vector<std::vector<std::string>> parts);

struct UtilsDownloadNodePlan {
    bool wrap_ome_metadata;
    bool use_v2_chunk_key_encoding;
    bool use_dimension_names;
};

UtilsDownloadNodePlan utils_download_node_plan(int zarr_format, bool has_axes);

struct UtilsViewPlan {
    bool should_warn;
    std::string warning_message;
    std::string parent_dir;
    std::string image_name;
    std::string url;
};

UtilsViewPlan utils_view_plan(
    const std::string& input_path,
    int port,
    bool force,
    std::size_t discovered_count);

struct UtilsFinderPlan {
    std::string parent_path;
    std::string server_dir;
    std::string csv_path;
    std::string source_uri;
    std::string url;
};

UtilsFinderPlan utils_finder_plan(const std::string& input_path, int port);

struct UtilsFinderRow {
    std::string file_path;
    std::string name;
    std::string folders;
    std::string uploaded;
};

UtilsFinderRow utils_finder_row(
    int port,
    const std::string& server_dir,
    const std::string& relpath,
    const std::string& name,
    const std::string& folders_path,
    const std::string& uploaded);

}  // namespace ome_zarr_c::native_code
