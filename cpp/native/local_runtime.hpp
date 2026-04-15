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

std::vector<std::string> local_info_lines(const std::string& input_path);

struct LocalFinderResult {
    bool found_any;
    std::string csv_path;
    std::string source_uri;
    std::string app_url;
    std::vector<UtilsFinderRow> rows;
};

LocalFinderResult local_finder_csv(const std::string& input_path, int port);

}  // namespace ome_zarr_c::native_code
