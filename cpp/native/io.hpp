#pragma once

#include <string>
#include <vector>

namespace ome_zarr_c::native_code {

std::string io_basename(const std::string& path);

std::vector<std::string> io_parts(const std::string& path, bool is_file);

std::string io_subpath(
    const std::string& path,
    const std::string& subpath,
    bool is_file,
    bool is_http);

std::string io_repr(
    const std::string& subpath,
    bool has_zgroup,
    bool has_zarray);

bool io_protocol_is_http(const std::vector<std::string>& protocols);

}  // namespace ome_zarr_c::native_code
