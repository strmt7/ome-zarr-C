#pragma once

#include <string_view>

#ifndef OME_ZARR_C_NATIVE_PROJECT_VERSION
#define OME_ZARR_C_NATIVE_PROJECT_VERSION "0.0.1"
#endif

namespace ome_zarr_c::native_code {

inline constexpr std::string_view native_project_name = "ome-zarr-C";
inline constexpr std::string_view native_project_version =
    OME_ZARR_C_NATIVE_PROJECT_VERSION;
inline constexpr std::string_view upstream_reference_version = "0.15.0";
inline constexpr std::string_view upstream_unknown_version = "0+unknown";

}  // namespace ome_zarr_c::native_code
