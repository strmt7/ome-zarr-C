#pragma once

#include <string>
#include <vector>

namespace ome_zarr_c::native_code {

enum class IoPathKind {
    pathlib_path,
    string_path,
    fsspec_store,
    local_store,
};

struct IoConstructorPlan {
    std::string normalized_path;
    bool use_input_store;
};

IoConstructorPlan io_constructor_plan(
    IoPathKind path_kind,
    const std::string& raw_path);

struct IoMetadataPlan {
    bool exists;
    bool create_group;
    bool unwrap_ome_namespace;
};

IoMetadataPlan io_metadata_plan(
    bool metadata_loaded,
    bool mode_is_write,
    bool has_ome_namespace);

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

bool io_is_local_store(IoPathKind path_kind);

bool io_parse_url_returns_none(const std::string& mode, bool exists);

bool io_protocol_is_http(const std::vector<std::string>& protocols);

}  // namespace ome_zarr_c::native_code
